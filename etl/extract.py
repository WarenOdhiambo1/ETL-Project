# Professional parallel extraction — all tickers fetched simultaneously
# Sequential: 6 tickers × 3s each = 18s
# Parallel:   6 tickers at once   = ~3s   (6x faster immediately)
# 
# Professional parallel extraction — all tickers fetched simultaneously
# Sequential: 6 tickers × 3s each = 18s
# Parallel:   6 tickers at once   = ~3s   (6x faster immediately)
# ═══════════════════════════════════════════════════════════════════
 
import yfinance as yf
import polars as pl
import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
 
logger = logging.getLogger(__name__)
 
MAX_RETRIES  = 3
WAIT_SECONDS = 5
MAX_WORKERS  = 6   # one thread per ticker — safe for Yahoo Finance rate limits
 
 
def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten yfinance mixed column types after reset_index().
    Handles: plain strings, tuples, MultiIndex — all cases.
    """
    flat = []
    for col in df.columns:
        name = str(col[0]) if isinstance(col, tuple) else str(col)
        flat.append(name.lower().strip().replace(" ", "_"))
    df.columns = flat
    return df
 
 
def extract(ticker: str, period: str = "20y", interval: str = "1d") -> pl.DataFrame:
    """
    Download one ticker with automatic retry on timeout or empty response.
    Used directly and also called inside the parallel executor.
    """
    last_error = None
 
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            raw: pd.DataFrame = yf.download(
                ticker,
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False,
                threads=False,
                timeout=30,
            )
 
            if not raw.empty:
                raw = raw.reset_index()
                raw = _flatten_columns(raw)
                df  = pl.from_pandas(raw)
                logger.info(f"[{ticker}/{interval}] {len(df)} rows extracted")
                return df
 
            logger.warning(f"[{ticker}/{interval}] Empty response attempt {attempt}/{MAX_RETRIES}")
 
        except Exception as e:
            last_error = e
            logger.warning(f"[{ticker}/{interval}] Attempt {attempt} failed: {e}")
 
        if attempt < MAX_RETRIES:
            time.sleep(WAIT_SECONDS)
 
    raise ValueError(
        f"[{ticker}/{interval}] Failed after {MAX_RETRIES} attempts. "
        f"Last error: {last_error}"
    )
 
 
def extract_parallel(
    tickers: list[str],
    period:  str = "20y",
    interval: str = "1d",
) -> dict[str, pl.DataFrame]:
    """
    Fetch all tickers simultaneously using a thread pool.
 
    Sequential (old):  ticker1 → ticker2 → ticker3 → ...  ~18 seconds
    Parallel   (new):  all tickers at once               ~3 seconds
 
    ThreadPoolExecutor is safe here because each thread makes its own
    HTTP request to Yahoo Finance — no shared mutable state.
    """
    results  = {}
    failures = {}
 
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all download jobs at once
        future_to_ticker = {
            executor.submit(extract, ticker, period, interval): ticker
            for ticker in tickers
        }
 
        # Collect results as they complete (fastest first)
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                results[ticker] = future.result()
            except Exception as e:
                failures[ticker] = str(e)
                logger.error(f"[{ticker}/{interval}] Extract failed: {e}")
 
    if failures:
        logger.warning(f"Failed tickers: {list(failures.keys())}")
 
    return results
 
 
