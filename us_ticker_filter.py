import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 筛选配置
FILTER_CONFIG = {
    "MARKET_CAP_THRESHOLD": 1_000_000_000,  # 1B
    "EXCLUDED_SECTORS": ["Financial Services", "Real Estate"],  # Finnhub 行业名称
    "API_URL": "https://finnhub.io/api/v1/stock/symbol",
    "PROFILE_URL": "https://finnhub.io/api/v1/stock/profile2",
    "ALLOWED_MIC": {"XNYS", "XNAS", "ARCX"},
}

REQUEST_TIMEOUT = 20
LIST_RETRIES = 3
PROFILE_RETRIES = 5
REQUEST_INTERVAL_SECONDS = 1.1
SAVE_EVERY = 50
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


def _is_retryable_status(status: int) -> bool:
    return status in RETRYABLE_STATUS_CODES or 500 <= status < 600


def _request_json(
    url: str,
    params: Dict[str, str],
    retries: int,
    retry_wait_seconds: int,
    log_non_200: bool = True,
) -> Tuple[Optional[object], Optional[int]]:
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            logger.warning(f"请求失败 (第 {attempt}/{retries} 次): {e}")
            if attempt < retries:
                time.sleep(retry_wait_seconds * attempt)
            continue

        status = response.status_code
        if status == 200:
            try:
                return response.json(), status
            except ValueError:
                logger.warning(f"响应不是合法 JSON (第 {attempt}/{retries} 次)")
                if attempt < retries:
                    time.sleep(retry_wait_seconds * attempt)
                continue

        if _is_retryable_status(status):
            if status == 429:
                wait_seconds = max(retry_wait_seconds * attempt, 15)
                logger.warning(f"触发 API 频率限制 (429)，等待 {wait_seconds} 秒后重试...")
            else:
                wait_seconds = retry_wait_seconds * attempt
                logger.warning(f"HTTP {status} 为临时错误，等待 {wait_seconds} 秒后重试...")

            if attempt < retries:
                time.sleep(wait_seconds)
                continue

        if log_non_200:
            logger.warning(f"HTTP {status}: {response.text[:200]}")
        return None, status

    return None, None


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


def get_finnhub_tickers(api_key: str) -> List[str]:
    logger.info("从 Finnhub 获取全美股列表...")
    payload, status = _request_json(
        FILTER_CONFIG["API_URL"],
        {"exchange": "US", "token": api_key},
        retries=LIST_RETRIES,
        retry_wait_seconds=5,
        log_non_200=True,
    )

    if status != 200 or not isinstance(payload, list):
        logger.error("无法获取股票列表，返回空结果。")
        return []

    # 基础过滤：仅保留普通股，排除 ETF 与其他 MIC
    symbols = [
        item["symbol"]
        for item in payload
        if isinstance(item, dict)
        and item.get("type") == "Common Stock"
        and item.get("mic") in FILTER_CONFIG["ALLOWED_MIC"]
        and isinstance(item.get("symbol"), str)
        and item.get("symbol")
    ]
    symbols = sorted(set(symbols))
    logger.info(f"全市场普通股数量: {len(symbols)}")
    return symbols


def _get_company_profile(api_key: str, ticker: str) -> Optional[dict]:
    payload, status = _request_json(
        FILTER_CONFIG["PROFILE_URL"],
        {"symbol": ticker, "token": api_key},
        retries=PROFILE_RETRIES,
        retry_wait_seconds=10,
        log_non_200=False,
    )

    if status != 200 or not isinstance(payload, dict):
        return None
    if not payload:
        return None
    return payload


def main(output_path: Optional[str] = None):
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY is required in .env")

    script_dir = Path(__file__).resolve().parent
    output_file = Path(output_path).resolve() if output_path else script_dir / "final_tickers.json"
    error_file = output_file.with_name("error_tickers.json")
    staging_output_file = output_file.with_suffix(output_file.suffix + ".updating")
    staging_error_file = error_file.with_suffix(error_file.suffix + ".updating")
    output_file.parent.mkdir(parents=True, exist_ok=True)

    symbols = get_finnhub_tickers(api_key)
    if not symbols:
        logger.error("股票列表为空，停止更新。")
        return

    previous_count = len(_load_json_dict(output_file))
    final_data: Dict[str, dict] = {}
    error_tickers = set()
    logger.info(
        f"初步获取到 {len(symbols)} 只普通股，本次将全量重算（上次入选 {previous_count} 只）。"
    )

    excluded_sectors = {sector.lower() for sector in FILTER_CONFIG["EXCLUDED_SECTORS"]}
    threshold = FILTER_CONFIG["MARKET_CAP_THRESHOLD"]

    for count, ticker in enumerate(symbols, start=1):
        try:
            data = _get_company_profile(api_key, ticker)
            if not data:
                error_tickers.add(ticker)
                time.sleep(REQUEST_INTERVAL_SECONDS)
                continue

            market_cap = float(data.get("marketCapitalization") or 0) * 1_000_000
            sector = str(data.get("finnhubIndustry") or "").strip()
            sector_key = sector.lower().replace(" ", "-")
            error_tickers.discard(ticker)

            if market_cap >= threshold and sector.lower() not in excluded_sectors:
                final_data[ticker] = {
                    "marketCap": market_cap,
                    "sector": sector,
                    "sectorKey": sector_key,
                }
                logger.info(f"[✓] {ticker} - {sector} - ${market_cap / 1e9:.2f}B")

            if count % SAVE_EVERY == 0:
                _save_json(staging_output_file, final_data)
                _save_json(staging_error_file, sorted(error_tickers))
                logger.info(
                    f"进度: {count}/{len(symbols)} - 已入选 {len(final_data)} 只 - 异常 {len(error_tickers)} 只"
                )

            time.sleep(REQUEST_INTERVAL_SECONDS)
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
