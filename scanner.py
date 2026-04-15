#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

import us_ticker_filter

load_dotenv()

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Finnhub K线接口
CANDLE_URL = "https://finnhub.io/api/v1/stock/candle"

# Finnhub 免费版限速：60次/分钟，间隔 1.1 秒可安全运行
CANDLE_REQUEST_INTERVAL_SECONDS = 1.1
CANDLE_RETRIES = 3
CANDLE_RETRY_WAIT_SECONDS = 15  # 遇到 429 时的等待时长
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}

# 策略配置中心
SCAN_CONFIG = {
    "EXCLUDED_SECTORS": ["Real Estate"],
    "VOLATILITY": {
        "WINDOW": 60,  # 检查过去 60 个交易日
        "THRESHOLD": 1.2,  # 最高价/最低价必须 > 1.2
    },
    "EMA": {
        "SHORT": 30,
        "MEDIUM": 60,
        "LONG": 120,
    },
    "PULLBACK": {
        "ENABLED": True,
        # 回踩逻辑：最低价低于短均线，最高价高于长均线
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


def _is_retryable_status(status: int) -> bool:
    return status in RETRYABLE_STATUS_CODES or 500 <= status < 600


def _fetch_candle(api_key: str, symbol: str, days: int = 365) -> Optional[pd.DataFrame]:
    """
    从 Finnhub 拉取指定股票的日线 K 线数据，返回包含 Close/High/Low 列的 DataFrame。
    失败或数据不足时返回 None。

    Finnhub candle API 文档:
      GET /stock/candle?symbol=<sym>&resolution=D&from=<unix>&to=<unix>&token=<key>
    响应字段: c(收盘), h(最高), l(最低), o(开盘), v(成交量), t(时间戳), s(状态)
    """
    to_ts = int(datetime.now().timestamp())
    from_ts = int((datetime.now() - timedelta(days=days)).timestamp())

    params = {
        "symbol": symbol,
        "resolution": "D",
        "from": str(from_ts),
        "to": str(to_ts),
        "token": api_key,
    }

    for attempt in range(1, CANDLE_RETRIES + 1):
        try:
            response = requests.get(CANDLE_URL, params=params, timeout=20)
        except requests.RequestException as e:
            logger.warning(f"{symbol} 请求失败 (第 {attempt}/{CANDLE_RETRIES} 次): {e}")
            if attempt < CANDLE_RETRIES:
                time.sleep(CANDLE_RETRY_WAIT_SECONDS * attempt)
            continue

        status = response.status_code

        if status == 429:
            wait = CANDLE_RETRY_WAIT_SECONDS * attempt
            logger.warning(f"{symbol} 触发 429 限速，等待 {wait} 秒后重试...")
            if attempt < CANDLE_RETRIES:
                time.sleep(wait)
            continue

        if _is_retryable_status(status):
            wait = CANDLE_RETRY_WAIT_SECONDS * attempt
            logger.warning(f"{symbol} HTTP {status} 临时错误，等待 {wait} 秒后重试...")
            if attempt < CANDLE_RETRIES:
                time.sleep(wait)
            continue

        if status != 200:
            logger.debug(f"{symbol} HTTP {status}，跳过。")
            return None

        try:
            payload = response.json()
        except ValueError:
            logger.warning(f"{symbol} 响应不是合法 JSON，跳过。")
            return None

        # Finnhub 在无数据时返回 {"s": "no_data"}
        if not isinstance(payload, dict) or payload.get("s") != "ok":
            logger.debug(f"{symbol} Finnhub 返回 s={payload.get('s', '?')}，跳过。")
            return None

        c = payload.get("c")
        h = payload.get("h")
        l = payload.get("l")
        t = payload.get("t")

        if not (c and h and l and t and len(c) == len(h) == len(l) == len(t)):
            logger.debug(f"{symbol} K线数组长度不一致或为空，跳过。")
            return None

        df = pd.DataFrame({
            "Close": c,
            "High":  h,
            "Low":   l,
        }, index=pd.to_datetime(t, unit="s", utc=True))
        df.index.name = "Date"
        df.sort_index(inplace=True)

        return df

    return None


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

        self.api_key = os.getenv("FINNHUB_API_KEY")
        if not self.api_key:
            raise ValueError("FINNHUB_API_KEY is required in .env")

    def calculate_ema(self, prices, period):
        return prices.ewm(span=period, adjust=False).mean()

    def is_volatile_enough(self, hist) -> bool:
        """检查最近波动率是否达标"""
        window = self.config["VOLATILITY"]["WINDOW"]
        threshold = self.config["VOLATILITY"]["THRESHOLD"]

        recent_hist = hist.tail(window)
        if len(recent_hist) < window:
            return False

        ratio = recent_hist["High"].max() / recent_hist["Low"].min()
        return ratio > threshold

    def check_ema_trend(self, close_prices) -> Tuple[bool, Tuple[float, float, float]]:
        """检查 EMA 是否多头排列"""
        ema_s = self.calculate_ema(close_prices, self.config["EMA"]["SHORT"]).iloc[-1]
        ema_m = self.calculate_ema(close_prices, self.config["EMA"]["MEDIUM"]).iloc[-1]
        ema_l = self.calculate_ema(close_prices, self.config["EMA"]["LONG"]).iloc[-1]

        return ema_s > ema_m > ema_l, (ema_s, ema_m, ema_l)

    def check_pullback(self, hist, emas: Tuple[float, float, float]) -> bool:
        """检查是否处于回踩/区间内"""
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
            symbols = [s for s in ticker_data if isinstance(s, str) and s]
            return sorted(set(symbols))

        if not isinstance(ticker_data, dict):
            raise ValueError("final_tickers.json 格式错误，仅支持 list 或 dict。")

        symbols: List[str] = []
        for symbol, info in ticker_data.items():
            if not isinstance(symbol, str) or not symbol:
                continue
            if not isinstance(info, dict):
                info = {}

            sector_key = str(info.get("sectorKey", "")).lower()
            sector_name = str(info.get("sector", "")).lower()

            if sector_key in self.excluded_sector_keys or sector_name in self.excluded_sector_names:
                continue
            symbols.append(symbol)

        return sorted(set(symbols))

    def scan_stocks(self):
        logger.info("开始扫描股票...")
        symbols = self.load_symbols()
        logger.info(f"过滤后准备分析 {len(symbols)} 只股票")

        if not symbols:
            logger.warning("股票池为空，停止扫描。")
            return

        min_required = self.config["EMA"]["LONG"]
        total = len(symbols)

        for idx, symbol in enumerate(symbols, start=1):
            try:
                hist = _fetch_candle(self.api_key, symbol)

                if hist is None or len(hist) < min_required:
                    logger.debug(f"[{idx}/{total}] {symbol} 数据不足，跳过。")
                    time.sleep(CANDLE_REQUEST_INTERVAL_SECONDS)
                    continue

                if not self.is_volatile_enough(hist):
                    time.sleep(CANDLE_REQUEST_INTERVAL_SECONDS)
                    continue

                is_trend, emas = self.check_ema_trend(hist["Close"])
                if not is_trend:
                    time.sleep(CANDLE_REQUEST_INTERVAL_SECONDS)
                    continue

                if self.check_pullback(hist, emas):
                    logger.info(f"[{idx}/{total}] ✓ {symbol}: 符合所有条件")
                    self.results.append(symbol)
                else:
                    logger.debug(f"[{idx}/{total}] {symbol} 未满足回踩条件，跳过。")

            except Exception as e:
                logger.debug(f"[{idx}/{total}] 分析 {symbol} 内部跳过: {e}")

            # 无论成功与否均保持限速间隔，确保不超过 60次/分钟
            time.sleep(CANDLE_REQUEST_INTERVAL_SECONDS)

        self.results = sorted(set(self.results))
        logger.info(f"扫描完成，共找到 {len(self.results)} 只股票")

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

    def send_results(self):
        import asyncio
        import telegram

        bot_token = os.getenv("BOT_TOKEN")
        chat_id = os.getenv("CHAT_ID")
        if not bot_token or not chat_id:
            logger.warning("未配置 BOT_TOKEN 或 CHAT_ID，跳过发送结果。")
            return

        bot = telegram.Bot(bot_token)

        delta_path = self.base_dir / "delta_scan_result.json"
        with delta_path.open("r", encoding="utf-8") as f:
            message = str(json.load(f))

        if len(message) > 3500:
            message = message[:3400] + "... (truncated)"

        asyncio.run(bot.send_message(chat_id=chat_id, text=message))


def check_and_update_tickers(base_dir: Path):
    """检查是否需要每月例行更新股票池"""
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


def main():
    scanner = StockScanner()

    # 1. 自动维护股票池
    check_and_update_tickers(scanner.base_dir)

    # 2. 运行扫描器
    try:
        scanner.scan_stocks()
        scanner.save_results()
        logger.info("扫描并保存完毕！")

        scanner.send_results()
        logger.info("结果发送成功！")
    except KeyboardInterrupt:
        logger.info("用户中断了扫描")
    except Exception as e:
        logger.error(f"扫描过程中出错: {e}")


if __name__ == "__main__":
    main()