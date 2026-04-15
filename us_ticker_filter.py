import csv
import io
import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 筛选配置
FILTER_CONFIG = {
    "MARKET_CAP_THRESHOLD": 1_000_000_000,  # 1B
    "EXCLUDED_SECTORS": ["real-estate"],
    "NASDAQ_LISTED_URL": "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
    "OTHER_LISTED_URL": "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
    "ALLOWED_OTHERLISTED_EXCHANGES": {"N", "A", "P"},
}

REQUEST_TIMEOUT = 20
LIST_RETRIES = 3
PROFILE_RETRIES = 3
PROFILE_REQUEST_INTERVAL_SECONDS = 1.5
SAVE_EVERY = 50
EXCLUDED_NAME_PATTERNS = (
    re.compile(r"\bwarrants?\b", re.IGNORECASE),
    re.compile(r"\brights?\b", re.IGNORECASE),
    re.compile(r"\bunits?\b", re.IGNORECASE),
    re.compile(r"\bpreferred\b", re.IGNORECASE),
    re.compile(r"\bdepositary\b", re.IGNORECASE),
    re.compile(r"\bnotes?\b", re.IGNORECASE),
    re.compile(r"\bbonds?\b", re.IGNORECASE),
    re.compile(r"\bdebentures?\b", re.IGNORECASE),
    re.compile(r"\betf\b", re.IGNORECASE),
    re.compile(r"\betn\b", re.IGNORECASE),
)


def _load_json_dict(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.warning(f"读取 {path} 失败，将按空数据处理: {e}")

    return {}


def _save_json(path: Path, data) -> None:
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


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-").replace("/", "-")


def _wait_for_request_slot(next_request_time: float, interval_seconds: float) -> float:
    now = time.monotonic()
    if now < next_request_time:
        time.sleep(next_request_time - now)
    return time.monotonic() + interval_seconds


def _looks_like_common_stock(security_name: str) -> bool:
    if not security_name:
        return False
    return not any(pattern.search(security_name) for pattern in EXCLUDED_NAME_PATTERNS)


def _request_text(url: str, retries: int) -> str:
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.warning(f"下载 {url} 失败 (第 {attempt}/{retries} 次): {e}")
            if attempt < retries:
                time.sleep(5 * attempt)

    raise RuntimeError(f"无法下载符号目录: {url}")


def _parse_directory_rows(raw_text: str) -> List[Dict[str, str]]:
    reader = csv.DictReader(io.StringIO(raw_text), delimiter="|")
    rows: List[Dict[str, str]] = []
    for row in reader:
        if not row:
            continue
        if any("File Creation Time" in str(value) for value in row.values()):
            continue
        rows.append({str(key): str(value).strip() for key, value in row.items() if key})
    return rows


def get_yahoo_compatible_us_tickers() -> List[str]:
    logger.info("从公开交易所目录获取美股代码...")

    nasdaq_rows = _parse_directory_rows(_request_text(FILTER_CONFIG["NASDAQ_LISTED_URL"], LIST_RETRIES))
    other_rows = _parse_directory_rows(_request_text(FILTER_CONFIG["OTHER_LISTED_URL"], LIST_RETRIES))

    symbols = set()

    for row in nasdaq_rows:
        if row.get("Test Issue") != "N":
            continue
        if row.get("ETF") != "N":
            continue
        if row.get("NextShares") == "Y":
            continue

        symbol = _normalize_symbol(row.get("Symbol", ""))
        security_name = row.get("Security Name", "")
        if not symbol or not _looks_like_common_stock(security_name):
            continue
        symbols.add(symbol)

    for row in other_rows:
        if row.get("Test Issue") != "N":
            continue
        if row.get("ETF") != "N":
            continue
        if row.get("Exchange") not in FILTER_CONFIG["ALLOWED_OTHERLISTED_EXCHANGES"]:
            continue

        symbol = _normalize_symbol(row.get("NASDAQ Symbol") or row.get("CQS Symbol") or row.get("ACT Symbol", ""))
        security_name = row.get("Security Name", "")
        if not symbol or not _looks_like_common_stock(security_name):
            continue
        symbols.add(symbol)

    sorted_symbols = sorted(symbols)
    logger.info(f"交易所目录初筛后股票数: {len(sorted_symbols)}")
    return sorted_symbols


def _get_company_profile(ticker: str, next_request_time: float) -> Tuple[Optional[dict], float]:
    for attempt in range(1, PROFILE_RETRIES + 1):
        next_request_time = _wait_for_request_slot(
            next_request_time,
            PROFILE_REQUEST_INTERVAL_SECONDS,
        )
        try:
            info = yf.Ticker(ticker).info
        except Exception as e:
            logger.warning(f"{ticker} 获取公司资料失败 (第 {attempt}/{PROFILE_RETRIES} 次): {e}")
            if attempt < PROFILE_RETRIES:
                time.sleep(3 * attempt)
            continue

        if isinstance(info, dict) and info:
            return info, next_request_time

        logger.warning(f"{ticker} 公司资料为空 (第 {attempt}/{PROFILE_RETRIES} 次)")
        if attempt < PROFILE_RETRIES:
            time.sleep(3 * attempt)

    return None, next_request_time


def main(output_path: Optional[str] = None):
    script_dir = Path(__file__).resolve().parent
    output_file = Path(output_path).resolve() if output_path else script_dir / "final_tickers.json"
    error_file = output_file.with_name("error_tickers.json")
    staging_output_file = output_file.with_suffix(output_file.suffix + ".updating")
    staging_error_file = error_file.with_suffix(error_file.suffix + ".updating")
    output_file.parent.mkdir(parents=True, exist_ok=True)

    symbols = get_yahoo_compatible_us_tickers()
    if not symbols:
        logger.error("股票列表为空，停止更新。")
        return

    previous_count = len(_load_json_dict(output_file))
    final_data: Dict[str, dict] = {}
    error_tickers = set()
    next_profile_request_time = 0.0
    logger.info(
        f"初步获取到 {len(symbols)} 只股票，本次将全量重算（上次入选 {previous_count} 只）。"
    )

    excluded_sectors = {sector.lower() for sector in FILTER_CONFIG["EXCLUDED_SECTORS"]}
    threshold = FILTER_CONFIG["MARKET_CAP_THRESHOLD"]

    for count, ticker in enumerate(symbols, start=1):
        try:
            data, next_profile_request_time = _get_company_profile(
                ticker,
                next_profile_request_time,
            )
            if not data:
                error_tickers.add(ticker)
                continue

            quote_type = str(data.get("quoteType") or "").upper()
            if quote_type and quote_type != "EQUITY":
                continue

            market_cap_raw = data.get("marketCap") or 0
            sector = str(data.get("sector") or "").strip()
            sector_key = sector.lower().replace(" ", "-")

            try:
                market_cap = float(market_cap_raw)
            except (TypeError, ValueError):
                market_cap = 0.0

            error_tickers.discard(ticker)

            if market_cap >= threshold and sector.lower() not in excluded_sectors:
                final_data[ticker] = {
                    "marketCap": market_cap,
                    "sector": sector,
                    "sectorKey": sector_key,
                }
                logger.info(f"[✓] {ticker} - {sector or 'Unknown'} - ${market_cap / 1e9:.2f}B")

            if count % SAVE_EVERY == 0:
                _save_json(staging_output_file, final_data)
                _save_json(staging_error_file, sorted(error_tickers))
                logger.info(
                    f"进度: {count}/{len(symbols)} - 已入选 {len(final_data)} 只 - 异常 {len(error_tickers)} 只"
                )
        except Exception as e:
            logger.error(f"处理 {ticker} 时出错: {e}")
            error_tickers.add(ticker)
            time.sleep(2)

    _save_json(staging_output_file, final_data)
    _save_json(staging_error_file, sorted(error_tickers))
    os.replace(staging_output_file, output_file)
    os.replace(staging_error_file, error_file)
    logger.info(f"股票池更新完成，共 {len(final_data)} 只股票，异常 {len(error_tickers)} 只。")


if __name__ == "__main__":
    main()
