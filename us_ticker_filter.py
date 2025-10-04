import requests
import pandas as pd
from io import StringIO

# Download and parse nasdaqlisted.txt (NASDAQ-listed)
url1 = 'https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt'
response1 = requests.get(url1)
df1 = pd.read_csv(StringIO(response1.text), sep='|')  # header=0 by default

# Filter for active common stocks
nasdaq_filter = (df1['Test Issue'] == 'N') & (df1['ETF'] == 'N')
symbols1 = df1[nasdaq_filter]['Symbol'].unique().tolist()

# Download and parse otherlisted.txt (NYSE, AMEX, etc.)
url2 = 'https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt'
response2 = requests.get(url2)
df2 = pd.read_csv(StringIO(response2.text), sep='|')  # header=0 by default

# Filter for active non-ETFs
other_filter = (df2['Test Issue'] == 'N') & (df2['ETF'] == 'N')
symbols2 = df2[other_filter]['ACT Symbol'].unique().tolist()

# Combine unique tickers
us_tickers = set(symbols1 + symbols2)

import json
import time
import yfinance as yf

count = 0
final_tickers = []
error_tickers = []
for ticker in us_tickers:
    try:
        count += 1
        print(f'{count}/{len(us_tickers)}')
        dat = yf.Ticker(ticker)
        if dat.info['marketCap'] > 1000000000:
            final_tickers.append(ticker)
        time.sleep(2)
    except Exception as e:
        print(ticker)
        print(e)
        error_tickers.append(ticker)
        time.sleep(5)
        continue

print(f'Final Tickers: {len(final_tickers)}')
print(f'Error Tickers: {len(error_tickers)}')
with open('final_tickers.json', 'w') as f:
    json.dump(final_tickers, f)
with open('error_tickers.json', 'w') as f:
    json.dump(error_tickers, f)
