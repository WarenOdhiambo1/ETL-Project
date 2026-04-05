# Two fixes applied:
#   1. Install pyarrow first: pip install pyarrow
#   2. yfinance now returns multi-level columns — flatten them
#      BEFORE calling pl.from_pandas()
# ═══════════════════════════════════════════════════════
import yfinance as yf
import polars as pl
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Robustly flatten yfinance column names after reset_index().

    The problem:
      yfinance returns columns as a MultiIndex: [("Open","BTC-USD"), ...]
      After reset_index(), the date index is injected as a plain string "Datetime"
      while the OHLCV columns remain tuples.
      The result is a MIXED index — not a pure MultiIndex, not pure strings.

      isinstance(raw.columns, pd.MultiIndex) → False  (mixed, not pure)
      str(("Open","BTC-USD"))               → "('open', 'btc-usd')"  (wrong)

    The fix:
      Iterate every column individually.
      If it is a tuple  → take the first element (the field name).
      If it is a string → keep it as-is.
      Then lowercase everything.

    This handles all four cases yfinance produces:
      "Datetime"           → "datetime"
      ("Open",  "BTC-USD") → "open"
      ("Open",  "")        → "open"
      "open"               → "open"   (already clean)
    """
    flat = []
    for col in df.columns:
        if isinstance(col, tuple):
            name = str(col[0])
        else:
            name = str(col)
        flat.append(name.lower().strip().replace(" ", "_"))
    df.columns = flat
    return df


def extract(
    ticker: str,
    period: str = "20y",
    interval: str = "1d"
) -> pl.DataFrame:
    """
    Download OHLCV data from Yahoo Finance.
    Returns a clean Polars DataFrame ready for transform().

    Args:
        ticker:   e.g. "BTC-USD", "AAPL", "SPY"
        period:   "20y" for daily/weekly, "700d" for hourly (Yahoo limit)
        interval: "1d", "1wk", "1h"
    """
    logger.info(f"Extracting {ticker} | period={period} | interval={interval}")

    raw: pd.DataFrame = yf.download(
        ticker,
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
        threads=False,
    )

    if raw.empty:
        raise ValueError(
            f"No data returned for {ticker}. "
            f"period={period}, interval={interval}."
        )

    # Bring date/datetime index into a regular column
    raw = raw.reset_index()

    # Flatten columns — handles mixed string/tuple columns from yfinance
    raw = _flatten_columns(raw)

    # At this point all column names are clean lowercase strings:
    #   daily/weekly: "date", "open", "high", "low", "close", "volume"
    #   hourly:       "datetime", "open", "high", "low", "close", "volume"
    # The transform() step handles the datetime→date rename.

    logger.info(f"Columns after flatten: {raw.columns.tolist()}")

    df = pl.from_pandas(raw)

    logger.info(f"Extracted {len(df)} rows for {ticker}")
    return df


def extract_multiple(
    tickers: list[str],
    period: str = "20y",
    interval: str = "1d"
) -> dict[str, pl.DataFrame]:
    """Extract multiple tickers. Returns {ticker: DataFrame}."""
    results = {}
    for ticker in tickers:
        try:
            results[ticker] = extract(ticker, period=period, interval=interval)
        except Exception as e:
            logger.error(f"Failed {ticker}: {e}")
    return results