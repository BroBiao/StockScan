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

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Filter configuration
FILTER_CONFIG = {
    "MARKET_CAP_THRESHOLD": 1_000_000_000,  # 1B
    "EXCLUDED_SECTORS": ["Real Estate"],
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
        logger.warning("Failed to read %s; treating it as empty data: %s", path, e)

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


def _normalize_sector_name(sector: str) -> str:
    return " ".join(sector.strip().lower().split())


def _normalize_sector_key(sector: str) -> str:
    return _normalize_sector_name(sector).replace(" ", "-")


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
            logger.warning("Failed to download %s (attempt %s/%s): %s", url, attempt, retries, e)
            if attempt < retries:
                time.sleep(5 * attempt)

    raise RuntimeError(f"Unable to download symbol directory: {url}")


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
    logger.info("Fetching U.S. ticker symbols from public exchange directories...")

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
    logger.info("Directory prefilter kept %s symbols.", len(sorted_symbols))
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
            logger.warning(
                "Failed to fetch company profile for %s (attempt %s/%s): %s",
                ticker,
                attempt,
                PROFILE_RETRIES,
                e,
            )
            if attempt < PROFILE_RETRIES:
                time.sleep(3 * attempt)
            continue

        if isinstance(info, dict) and info:
            return info, next_request_time

        logger.warning(
            "Company profile for %s was empty (attempt %s/%s)",
            ticker,
            attempt,
            PROFILE_RETRIES,
        )
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
        logger.error("Ticker list is empty. Stopping refresh.")
        return

    previous_count = len(_load_json_dict(output_file))
    final_data: Dict[str, dict] = {}
    error_tickers = set()
    next_profile_request_time = 0.0
    logger.info(
        "Loaded %s candidate symbols. Rebuilding the entire universe from scratch (previous universe size: %s).",
        len(symbols),
        previous_count,
    )

    excluded_sector_names = {
        _normalize_sector_name(sector) for sector in FILTER_CONFIG["EXCLUDED_SECTORS"]
    }
    excluded_sector_keys = {
        _normalize_sector_key(sector) for sector in FILTER_CONFIG["EXCLUDED_SECTORS"]
    }
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
            sector_key = _normalize_sector_key(sector)
            sector_name = _normalize_sector_name(sector)

            try:
                market_cap = float(market_cap_raw)
            except (TypeError, ValueError):
                market_cap = 0.0

            error_tickers.discard(ticker)

            if market_cap >= threshold and sector_name not in excluded_sector_names and sector_key not in excluded_sector_keys:
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
                    "Progress: %s/%s processed, %s selected, %s errors",
                    count,
                    len(symbols),
                    len(final_data),
                    len(error_tickers),
                )
        except Exception as e:
            logger.error("Failed to process %s: %s", ticker, e)
            error_tickers.add(ticker)
            time.sleep(2)

    _save_json(staging_output_file, final_data)
    _save_json(staging_error_file, sorted(error_tickers))
    os.replace(staging_output_file, output_file)
    os.replace(staging_error_file, error_file)
    logger.info(
        "Universe refresh completed. Selected %s symbols with %s errors.",
        len(final_data),
        len(error_tickers),
    )


if __name__ == "__main__":
    main()
