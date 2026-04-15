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

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DOWNLOAD_RETRIES = 3
DOWNLOAD_RETRY_WAIT_SECONDS = 5
DOWNLOAD_TIMEOUT = 20
DOWNLOAD_REQUEST_INTERVAL_SECONDS = 2.0

# Strategy configuration
SCAN_CONFIG = {
    "EXCLUDED_SECTORS": ["Real Estate"],
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
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-").replace("/", "-")


def _normalize_sector_name(sector: str) -> str:
    return " ".join(sector.strip().lower().split())


def _normalize_sector_key(sector: str) -> str:
    return _normalize_sector_name(sector).replace(" ", "-")


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
            logger.warning(
                "Yahoo Finance download failed (attempt %s/%s): %s",
                attempt,
                DOWNLOAD_RETRIES,
                e,
            )
            if attempt < DOWNLOAD_RETRIES:
                time.sleep(DOWNLOAD_RETRY_WAIT_SECONDS * attempt)
            continue

        if isinstance(data, pd.DataFrame) and not data.empty:
            return data

        logger.warning(
            "Yahoo Finance returned empty data (attempt %s/%s) for %s requested symbols.",
            attempt,
            DOWNLOAD_RETRIES,
            len(symbols),
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
            _normalize_sector_key(value) for value in self.config["EXCLUDED_SECTORS"]
        }
        self.excluded_sector_names = {
            _normalize_sector_name(value) for value in self.config["EXCLUDED_SECTORS"]
        }
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
            raise FileNotFoundError(f"Ticker universe file not found: {self.ticker_path}")

        with self.ticker_path.open("r", encoding="utf-8") as f:
            ticker_data = json.load(f)

        if isinstance(ticker_data, list):
            logger.warning(
                "Detected legacy list format in final_tickers.json; scanning raw symbols directly."
            )
            return sorted(
                {
                    _normalize_symbol(symbol)
                    for symbol in ticker_data
                    if isinstance(symbol, str) and symbol.strip()
                }
            )

        if not isinstance(ticker_data, dict):
            raise ValueError("final_tickers.json must be either a list or a dict.")

        symbols: List[str] = []
        for symbol, info in ticker_data.items():
            if not isinstance(symbol, str) or not symbol.strip():
                continue
            if not isinstance(info, dict):
                info = {}

            sector_key = _normalize_sector_key(str(info.get("sectorKey", "")))
            sector_name = _normalize_sector_name(str(info.get("sector", "")))

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
            logger.info(
                "Batch download missed %s symbols; retrying them one by one.",
                len(missing_symbols),
            )

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
        logger.info("Starting stock scan...")
        symbols = self.load_symbols()
        logger.info("Prepared %s symbols after filtering.", len(symbols))

        if not symbols:
            logger.warning("Ticker universe is empty. Stopping scan.")
            return

        self.next_download_time = 0.0
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
            logger.info(
                "Downloading batch %s/%s (%s symbols)...",
                batch_index,
                batch_total,
                len(batch_symbols),
            )
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
                        logger.info("[%s/%s] ✓ %s matched all conditions", processed, total, symbol)
                        self.results.append(symbol)

                except Exception as e:
                    analysis_error_count += 1
                    logger.warning("[%s/%s] Failed to analyze %s: %s", processed, total, symbol, e)

            elapsed = time.monotonic() - start_time
            avg_seconds = elapsed / processed if processed else 0
            eta_seconds = avg_seconds * (total - processed)
            logger.info(
                "Batch complete: %s/%s, processed %s/%s, matches %s, missing/short data %s, single-symbol recoveries %s, errors %s, elapsed %s, ETA %s",
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
            "Scan complete. Found %s matches; missing/short data %s, single-symbol recoveries %s, errors %s.",
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
                logger.warning("Failed to read previous results; treating history as empty: %s", e)

        _atomic_write_json(full_path, self.results)
        logger.info("Saved full results to %s", full_path)

        delta_path = self.base_dir / "delta_scan_result.json"
        delta_results = sorted(set(self.results) - set(old_results))
        _atomic_write_json(delta_path, delta_results)
        logger.info("Saved delta results to %s", delta_path)

    def send_results(self) -> bool:
        import asyncio
        import telegram

        bot_token = os.getenv("BOT_TOKEN")
        chat_id = os.getenv("CHAT_ID")
        if not bot_token or not chat_id:
            logger.warning("BOT_TOKEN or CHAT_ID is missing; skipping Telegram notification.")
            return False

        bot = telegram.Bot(bot_token)
        delta_path = self.base_dir / "delta_scan_result.json"
        if not delta_path.exists():
            raise FileNotFoundError(f"Delta result file not found: {delta_path}")

        with delta_path.open("r", encoding="utf-8") as f:
            delta_results = json.load(f)

        if not isinstance(delta_results, list):
            raise ValueError("delta_scan_result.json must contain a list.")

        if delta_results:
            message = str(delta_results)
        else:
            message = "No new signals today."

        if len(message) > 3500:
            message = message[:3400] + "... (truncated)"

        asyncio.run(bot.send_message(chat_id=chat_id, text=message))
        return True


def check_and_update_tickers(base_dir: Path):
    ticker_path = base_dir / "final_tickers.json"
    now = datetime.now()

    need_update = False
    if not ticker_path.exists():
        logger.info("Ticker universe file not found. Running initial build...")
        need_update = True
    else:
        mtime = ticker_path.stat().st_mtime
        last_update = datetime.fromtimestamp(mtime)
        if (now.year, now.month) != (last_update.year, last_update.month):
            logger.info(
                "Detected a new month (%s-%s). Refreshing the ticker universe...",
                now.year,
                now.month,
            )
            need_update = True

    if need_update:
        try:
            us_ticker_filter.main(output_path=str(ticker_path))
            logger.info("Ticker universe refresh completed.")
        except Exception as e:
            logger.error("Ticker universe refresh failed: %s", e)
            if not ticker_path.exists():
                raise RuntimeError(
                    "Ticker universe refresh failed and no local fallback file is available."
                ) from e
            logger.warning("Continuing with the existing ticker universe file.")


def main():
    scanner = StockScanner()

    try:
        check_and_update_tickers(scanner.base_dir)
        scanner.scan_stocks()
        scanner.save_results()
        logger.info("Scan and persistence completed.")

        if scanner.send_results():
            logger.info("Telegram notification sent successfully.")
    except KeyboardInterrupt:
        logger.info("Scan interrupted by user.")
    except Exception:
        logger.exception("Scan failed.")


if __name__ == "__main__":
    main()
