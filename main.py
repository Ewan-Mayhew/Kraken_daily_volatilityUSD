import requests
import pandas as pd
import numpy as np
import time

# Step 1: Fetch USD/ZUSD Asset Pairs
asset_pairs_url = "https://api.kraken.com/0/public/AssetPairs"
response = requests.get(asset_pairs_url)
asset_pairs_data = response.json()['result']

# Filter pairs that have USD or ZUSD as the quote currency
usd_pairs = [pair for pair, data in asset_pairs_data.items() if data['quote'] in ['ZUSD', 'USD']]

# Step 2: Fetch Volume for each pair and filter
ticker_url = "https://api.kraken.com/0/public/Ticker"
pairs_with_volume = []

# Fetch current timestamp (for 24-hour window)
current_timestamp = int(time.time())

print("Checking pairs:")

for pair in usd_pairs:
    response = requests.get(ticker_url, params={"pair": pair})
    ticker_info = response.json()['result'][pair]
    
    # Get 24hr trade volume in base currency
    volume = float(ticker_info['v'][1])
    close_price = float(ticker_info['c'][0])
    
    # Convert volume to USD
    volume_usd = volume * close_price
    
    if volume_usd > 10000:
        pairs_with_volume.append(pair)
        print(f"Checking {pair}: Volume = {volume_usd} USD")

# Step 3: Calculate 15-Minute Volatility over the Last 24 Hours
ohlc_url = "https://api.kraken.com/0/public/OHLC"

def get_15min_volatility(pair):
    # Fetch 1-minute OHLC data for the past 24 hours (1440 minutes in a day)
    since_timestamp = current_timestamp - 24 * 60 * 60
    response = requests.get(ohlc_url, params={"pair": pair, "interval": 1, "since": since_timestamp})
    
    ohlc_data = response.json()['result'][pair]
    
    # Convert to DataFrame
    df = pd.DataFrame(ohlc_data, columns=['time', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count'])
    
    # Ensure 'close' column is numeric
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    # Drop rows with missing or invalid 'close' prices
    df.dropna(subset=['close'], inplace=True)
    
    # Aggregate to 15-minute intervals
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)
    df = df.resample('15T').agg({'close': 'last'}).dropna()  # Resample to 15 minutes, use the last close price
    
    # Calculate returns
    df['return'] = df['close'].pct_change()
    
    # Calculate volatility (standard deviation of returns)
    volatility = np.std(df['return'].dropna())
    
    return volatility

pair_volatility = [(pair, get_15min_volatility(pair)) for pair in pairs_with_volume]

# Step 4: Sort pairs by volatility and display results
pair_volatility_sorted = sorted(pair_volatility, key=lambda x: x[1], reverse=True)

print("\nRanking pairs by 15-min volatility over the last 24 hours:")
for pair, vol in pair_volatility_sorted:
    print(f"{pair}: Volatility = {vol}")
