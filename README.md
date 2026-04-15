# StockScan

Daily US stock scanner with monthly universe refresh, technical signal filtering, and Telegram push notifications.

## What This Project Does

This project is designed to run **once per day** (typically by cron or another scheduler).

Each run of `scanner.py` does:

1. Checks whether `final_tickers.json` is missing or outdated by month.
2. If needed, refreshes the US stock universe via public exchange symbol directories + Yahoo Finance (`us_ticker_filter.py`).
3. Scans tickers using daily price data from Yahoo Finance.
4. Saves:
   - full results (`full_scan_result.json`)
   - newly added symbols vs previous run (`delta_scan_result.json`)
5. Sends the delta list to Telegram.

## Strategy Logic (Current Version)

A symbol is selected only if all conditions pass:

1. Volatility filter:
   - Lookback window: last 60 daily bars
   - Condition: `max(High) / min(Low) > 1.2`
2. EMA trend filter:
   - `EMA30 > EMA60 > EMA120`
3. Pullback filter:
   - Latest daily `Low < EMA30`
   - Latest daily `High > EMA120`

Sector exclusions are applied from universe metadata.

## Repository Files

- `scanner.py`: daily runner, signal scan, output persistence, Telegram sender
- `us_ticker_filter.py`: monthly universe builder from public exchange symbol directories and Yahoo Finance
- `requirements.txt`: Python dependencies
- `final_tickers.json`: filtered stock universe (ticker -> metadata)
- `error_tickers.json`: tickers that failed during universe refresh
- `full_scan_result.json`: all symbols matching today’s scan
- `delta_scan_result.json`: newly matched symbols vs previous full result

## Requirements

- Python 3.9+
- Network access to:
  - `nasdaqtrader.com` (US symbol directories)
  - `query1.finance.yahoo.com` (via `yfinance`)
  - Telegram Bot API (for notifications)

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment Variables

Create a `.env` file in project root:

```bash
BOT_TOKEN=your_telegram_bot_token
CHAT_ID=your_telegram_chat_id
```

Notes:
- `BOT_TOKEN` + `CHAT_ID` are required for sending messages.
- If Telegram vars are missing, scan still runs and saves files, but skips notification.

## How To Run

Run manually:

```bash
python3 scanner.py
```

Run only universe refresh:

```bash
python3 us_ticker_filter.py
```

## Scheduling (Daily Fixed-Time Run)

Use cron to run once per day (example):

```cron
0 7 * * 1-5 cd /path/to/StockScan && /usr/bin/python3 scanner.py >> scan.log 2>&1
```

Recommended:
- Run after US market close if you want stable end-of-day signals.
- Keep timezone behavior explicit in your scheduler host.

## Data Safety and Reliability

Current implementation includes:

- Atomic JSON writes (tmp file + replace) to reduce file corruption risk.
- Retry handling for temporary download failures.
- Full monthly universe recomputation (avoids stale carry-over symbols).
- Exchange-directory prefiltering for ETFs, test issues, NextShares, and obvious non-stock instruments before Yahoo enrichment.
- Yahoo-compatible symbol normalization when reading the stock universe.
- Fallback single-symbol fetch if a symbol is missing from a batch price download.
- Request pacing for Yahoo Finance batch downloads and per-symbol metadata fetches to reduce throttling risk.

## Quick Troubleshooting

- First universe refresh is slow
  - `us_ticker_filter.py` enriches symbols one by one from Yahoo Finance and intentionally throttles requests, so the initial build can take a long time.
- No Telegram message sent
  - Verify `BOT_TOKEN`, `CHAT_ID`, and bot permissions in target chat.
- Empty scan output
  - Check whether universe file is present and contains symbols, and verify market data connectivity.
