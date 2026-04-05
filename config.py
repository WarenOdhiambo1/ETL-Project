# All assets you want to track
 
# config.py — full file, replace everything

TICKERS = [
    "BTC-USD",
    "ETH-USD",
    "AAPL",
    "SPY",
    "GC=F",
    "EURUSD=X",
]

# Format: "key": (interval, period, table_suffix)
# 1h is capped at 700d — Yahoo Finance server limit, cannot be changed
TIMEFRAMES = {
    "1d":  ("1d",  "20y",  "daily"),
    "1wk": ("1wk", "20y",  "weekly"),
    "1h":  ("1h",  "700d", "hourly"),
}

TABLE_PREFIX = "ohlcv"