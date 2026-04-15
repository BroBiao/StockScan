#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

import us_ticker_filter

load_dotenv()

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DOWNLOAD_RETRIES = 3
DOWNLOAD_RETRY_WAIT_SECONDS = 5
DOWNLOAD_TIMEOUT = 20
DOWNLOAD_REQUEST_INTERVAL_SECONDS = 2.0

# 策略配置中心
SCAN_CONFIG = {
    "EXCLUDED_SECTORS": ["real-estate"],
    "MARKET_DATA": {
        "LOOKBACK_PERIOD": "18mo",
        "BATCH_SIZE": 100,
        "REQUEST_INTERVAL_SECONDS": DOWNLOAD_REQUEST_INTERVAL_SECONDS,
        "THREADS": False,
    },
    "VOLATILITY": {
        "WINDOW": 60,
        "THRESHOLD": 1.2,
    },
    "EMA": {
        "SHORT": 30,
        "MEDIUM": 60,
        "LONG": 120,
    },
    "PULLBACK": {
        "ENABLED": True,
    },
}


def _atomic_write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as temp_file:
            json.dump(data, temp_file, ensure_ascii=False, indent=2)
            temp_file.flush()
            os.fsync(temp_file.fileno())
            temp_path = Path(temp_file.name)
        os.replace(temp_path, path)
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink(missing_ok=True)


def _format_duration(total_seconds: float) -> str:
    total_seconds = max(0, int(total_seconds))
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}小时{minutes}分{seconds}秒"
    if minutes:
        return f"{minutes}分{seconds}秒"
    return f"{seconds}秒"


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-").replace("/", "-")


def _wait_for_request_slot(next_request_time: float, interval_seconds: float) -> float:
    now = time.monotonic()
    if now < next_request_time:
        time.sleep(next_request_time - now)
    return time.monotonic() + interval_seconds


def _download_history(symbols: List[str], period: str, use_threads: bool) -> Optional[pd.DataFrame]:
    if not symbols:
        return None

    tickers = " ".join(symbols)
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            data = yf.download(
                tickers=tickers,
                period=period,
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                actions=False,
                progress=False,
                threads=use_threads,
                timeout=DOWNLOAD_TIMEOUT,
            )
        except Exception as e:
            logger.warning(f"Yahoo Finance 下载失败 (第 {attempt}/{DOWNLOAD_RETRIES} 次): {e}")
            if attempt < DOWNLOAD_RETRIES:
                time.sleep(DOWNLOAD_RETRY_WAIT_SECONDS * attempt)
            continue

        if isinstance(data, pd.DataFrame) and not data.empty:
            return data

        logger.warning(
            f"Yahoo Finance 返回空数据 (第 {attempt}/{DOWNLOAD_RETRIES} 次)，请求股票数 {len(symbols)}"
        )
        if attempt < DOWNLOAD_RETRIES:
            time.sleep(DOWNLOAD_RETRY_WAIT_SECONDS * attempt)

    return None


def _extract_history(data: Optional[pd.DataFrame], symbol: str) -> Optional[pd.DataFrame]:
    if data is None or data.empty:
        return None

    if isinstance(data.columns, pd.MultiIndex):
        level_zero = set(data.columns.get_level_values(0))
        level_one = set(data.columns.get_level_values(1))
        if symbol in level_zero:
            hist = data[symbol].copy()
        elif symbol in level_one:
            hist = data.xs(symbol, axis=1, level=1).copy()
        else:
            return None
    else:
        hist = data.copy()

    required_columns = ["Close", "High", "Low"]
    if any(column not in hist.columns for column in required_columns):
        return None

    hist = hist[required_columns].dropna()
    if hist.empty:
        return None

    hist = hist[~hist.index.duplicated(keep="last")].sort_index()
    hist.index.name = "Date"
    return hist


class StockScanner:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or SCAN_CONFIG
        self.results: List[str] = []
        self.base_dir = Path(__file__).resolve().parent
        self.ticker_path = self.base_dir / "final_tickers.json"
        self.excluded_sector_keys = {
            value.lower().replace(" ", "-") for value in self.config["EXCLUDED_SECTORS"]
        }
        self.excluded_sector_names = {value.lower() for value in self.config["EXCLUDED_SECTORS"]}
        self.next_download_time = 0.0

    def calculate_ema(self, prices, period):
        return prices.ewm(span=period, adjust=False).mean()

    def is_volatile_enough(self, hist) -> bool:
        window = self.config["VOLATILITY"]["WINDOW"]
        threshold = self.config["VOLATILITY"]["THRESHOLD"]

        recent_hist = hist.tail(window)
        if len(recent_hist) < window:
            return False

        lowest_low = recent_hist["Low"].min()
        if lowest_low <= 0:
            return False

        ratio = recent_hist["High"].max() / lowest_low
        return ratio > threshold

    def check_ema_trend(self, close_prices) -> Tuple[bool, Tuple[float, float, float]]:
        ema_s = self.calculate_ema(close_prices, self.config["EMA"]["SHORT"]).iloc[-1]
        ema_m = self.calculate_ema(close_prices, self.config["EMA"]["MEDIUM"]).iloc[-1]
        ema_l = self.calculate_ema(close_prices, self.config["EMA"]["LONG"]).iloc[-1]
        return ema_s > ema_m > ema_l, (ema_s, ema_m, ema_l)

    def check_pullback(self, hist, emas: Tuple[float, float, float]) -> bool:
        if not self.config["PULLBACK"].get("ENABLED", True):
            return True

        ema_s, _, ema_l = emas
        last_low = hist["Low"].iloc[-1]
        last_high = hist["High"].iloc[-1]
        return last_low < ema_s and last_high > ema_l

    def load_symbols(self) -> List[str]:
        if not self.ticker_path.exists():
            raise FileNotFoundError(f"未找到股票池文件: {self.ticker_path}")

        with self.ticker_path.open("r", encoding="utf-8") as f:
            ticker_data = json.load(f)

        if isinstance(ticker_data, list):
            logger.warning("检测到旧版列表格式 final_tickers.json，将按股票代码直接扫描。")
            return sorted(
                {
                    _normalize_symbol(symbol)
                    for symbol in ticker_data
                    if isinstance(symbol, str) and symbol.strip()
                }
            )

        if not isinstance(ticker_data, dict):
            raise ValueError("final_tickers.json 格式错误，仅支持 list 或 dict。")

        symbols: List[str] = []
        for symbol, info in ticker_data.items():
            if not isinstance(symbol, str) or not symbol.strip():
                continue
            if not isinstance(info, dict):
                info = {}

            sector_key = str(info.get("sectorKey", "")).lower()
            sector_name = str(info.get("sector", "")).lower()

            if sector_key in self.excluded_sector_keys or sector_name in self.excluded_sector_names:
                continue

            symbols.append(_normalize_symbol(symbol))

        return sorted(set(symbols))

    def fetch_histories(self, symbols: List[str]) -> Tuple[Dict[str, pd.DataFrame], int]:
        market_data_config = self.config["MARKET_DATA"]
        self.next_download_time = _wait_for_request_slot(
            self.next_download_time,
            market_data_config["REQUEST_INTERVAL_SECONDS"],
        )
        batch_data = _download_history(
            symbols,
            market_data_config["LOOKBACK_PERIOD"],
            use_threads=market_data_config["THREADS"],
        )
        histories: Dict[str, pd.DataFrame] = {}
        missing_symbols: List[str] = []
        fallback_count = 0

        for symbol in symbols:
            hist = _extract_history(batch_data, symbol)
            if hist is None:
                missing_symbols.append(symbol)
                continue
            histories[symbol] = hist

        if missing_symbols:
            logger.info(f"批量行情缺失 {len(missing_symbols)} 只股票，开始逐只补拉...")

        for symbol in missing_symbols:
            self.next_download_time = _wait_for_request_slot(
                self.next_download_time,
                market_data_config["REQUEST_INTERVAL_SECONDS"],
            )
            hist = _extract_history(
                _download_history(
                    [symbol],
                    market_data_config["LOOKBACK_PERIOD"],
                    use_threads=market_data_config["THREADS"],
                ),
                symbol,
            )
            if hist is None:
                continue
            histories[symbol] = hist
            fallback_count += 1

        return histories, fallback_count

    def scan_stocks(self):
        logger.info("开始扫描股票...")
        symbols = self.load_symbols()
        logger.info(f"过滤后准备分析 {len(symbols)} 只股票")

        if not symbols:
            logger.warning("股票池为空，停止扫描。")
            return

        min_required = self.config["EMA"]["LONG"]
        batch_size = self.config["MARKET_DATA"]["BATCH_SIZE"]
        total = len(symbols)
        batch_total = (total + batch_size - 1) // batch_size
        data_shortage_count = 0
        analysis_error_count = 0
        fallback_count = 0
        processed = 0
        start_time = time.monotonic()

        for batch_index, start_idx in enumerate(range(0, total, batch_size), start=1):
            batch_symbols = symbols[start_idx:start_idx + batch_size]
            logger.info(f"下载批次 {batch_index}/{batch_total}，股票数 {len(batch_symbols)} ...")
            histories, batch_fallback_count = self.fetch_histories(batch_symbols)
            fallback_count += batch_fallback_count

            for symbol in batch_symbols:
                processed += 1
                try:
                    hist = histories.get(symbol)
                    if hist is None or len(hist) < min_required:
                        data_shortage_count += 1
                        continue

                    if not self.is_volatile_enough(hist):
                        continue

                    is_trend, emas = self.check_ema_trend(hist["Close"])
                    if not is_trend:
                        continue

                    if self.check_pullback(hist, emas):
                        logger.info(f"[{processed}/{total}] ✓ {symbol}: 符合所有条件")
                        self.results.append(symbol)

                except Exception as e:
                    analysis_error_count += 1
                    logger.warning(f"[{processed}/{total}] 分析 {symbol} 时出错: {e}")

            elapsed = time.monotonic() - start_time
            avg_seconds = elapsed / processed if processed else 0
            eta_seconds = avg_seconds * (total - processed)
            logger.info(
                "批次完成: %s/%s，已处理 %s/%s，命中 %s 只，数据不足/缺失 %s 只，逐只补拉成功 %s 只，异常 %s 只，已耗时 %s，预计剩余 %s",
                batch_index,
                batch_total,
                processed,
                total,
                len(self.results),
                data_shortage_count,
                fallback_count,
                analysis_error_count,
                _format_duration(elapsed),
                _format_duration(eta_seconds),
            )

        self.results = sorted(set(self.results))
        logger.info(
            "扫描完成，共找到 %s 只股票；数据不足/缺失 %s 只，逐只补拉成功 %s 只，异常 %s 只",
            len(self.results),
            data_shortage_count,
            fallback_count,
            analysis_error_count,
        )

    def save_results(self):
        full_path = self.base_dir / "full_scan_result.json"

        old_results: List[str] = []
        if full_path.is_file():
            try:
                with full_path.open("r", encoding="utf-8") as f:
                    old_data = json.load(f)
                if isinstance(old_data, list):
                    old_results = [v for v in old_data if isinstance(v, str)]
            except Exception as e:
                logger.warning(f"读取历史结果失败，将按空历史处理: {e}")

        _atomic_write_json(full_path, self.results)
        logger.info(f"完整结果已保存到 {full_path}")

        delta_path = self.base_dir / "delta_scan_result.json"
        delta_results = sorted(set(self.results) - set(old_results))
        _atomic_write_json(delta_path, delta_results)
        logger.info(f"今日新增结果已保存到 {delta_path}")

    def send_results(self) -> bool:
        import asyncio
        import telegram

        bot_token = os.getenv("BOT_TOKEN")
        chat_id = os.getenv("CHAT_ID")
        if not bot_token or not chat_id:
            logger.warning("未配置 BOT_TOKEN 或 CHAT_ID，跳过发送结果。")
            return False

        bot = telegram.Bot(bot_token)
        delta_path = self.base_dir / "delta_scan_result.json"
        if not delta_path.exists():
            raise FileNotFoundError(f"未找到增量结果文件: {delta_path}")

        with delta_path.open("r", encoding="utf-8") as f:
            delta_results = json.load(f)

        if not isinstance(delta_results, list):
            raise ValueError("delta_scan_result.json 格式错误，必须为列表。")

        if delta_results:
            message = "今日新增信号：\n" + "\n".join(str(symbol) for symbol in delta_results)
        else:
            message = "今日无新增信号。"

        if len(message) > 3500:
            message = message[:3400] + "... (truncated)"

        asyncio.run(bot.send_message(chat_id=chat_id, text=message))
        return True


def check_and_update_tickers(base_dir: Path):
    ticker_path = base_dir / "final_tickers.json"
    now = datetime.now()

    need_update = False
    if not ticker_path.exists():
        logger.info("未发现股票池文件，将进行首次初始化...")
        need_update = True
    else:
        mtime = ticker_path.stat().st_mtime
        last_update = datetime.fromtimestamp(mtime)
        if (now.year, now.month) != (last_update.year, last_update.month):
            logger.info(f"检测到新月份 ({now.year}-{now.month})，准备更新全市场股票池...")
            need_update = True

    if need_update:
        try:
            us_ticker_filter.main(output_path=str(ticker_path))
            logger.info("股票池更新完成！")
        except Exception as e:
            logger.error(f"更新股票池时出错: {e}")
            if not ticker_path.exists():
                raise RuntimeError("股票池更新失败，且本地没有可用的股票池文件。") from e
            logger.warning("将继续使用现有股票池文件。")


def main():
    scanner = StockScanner()

    try:
        check_and_update_tickers(scanner.base_dir)
        scanner.scan_stocks()
        scanner.save_results()
        logger.info("扫描并保存完毕！")

        if scanner.send_results():
            logger.info("结果发送成功！")
    except KeyboardInterrupt:
        logger.info("用户中断了扫描")
    except Exception as e:
        logger.error(f"扫描过程中出错: {e}")


if __name__ == "__main__":
    main()
