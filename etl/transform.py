import polars as pl
import logging

logger = logging.getLogger(__name__)


def transform(df: pl.DataFrame, ticker: str) -> pl.DataFrame:
    """
    Full cleaning pipeline for raw yfinance data.

    Steps:
      1. Rename "datetime" to "date" (hourly data fix)
      2. Flatten column names
      3. Keep only OHLCV + date
      4. Drop rows with ANY missing values
      5. Round numbers to sensible precision
      6. Convert date to clean string (removes timezone noise)
      7. Add ticker column
      8. Sort by date ascending
      9. Remove duplicate dates (keep last)
    """

    # ── Step 1: Fix hourly column name ────────────────────────────
    # Daily/weekly data → index column is called "Date"  → "date" after lowercase
    # Hourly data       → index column is called "Datetime" → "datetime" after lowercase
    # We rename "datetime" to "date" so every timeframe works the same way
    if "datetime" in df.columns and "date" not in df.columns:
        df = df.rename({"datetime": "date"})

    # ── Step 2: Flatten column names ──────────────────────────────
    # yfinance sometimes returns columns like ("Close", "BTC-USD")
    # We flatten to simple lowercase strings: "close"
    flat_cols = []
    for col in df.columns:
        if isinstance(col, tuple):
            flat_cols.append(col[0].lower().replace(" ", "_"))
        else:
            flat_cols.append(col.lower().replace(" ", "_"))
    df.columns = flat_cols

    # ── Step 3: Keep only the columns we need ─────────────────────
    needed   = ["date", "open", "high", "low", "close", "volume"]
    existing = [c for c in needed if c in df.columns]
    df = df.select(existing)

    # ── Step 4: Drop rows with ANY missing value ───────────────────
    rows_before = len(df)
    df = df.drop_nulls()
    dropped = rows_before - len(df)
    if dropped > 0:
        logger.info(f"Dropped {dropped} rows with null values")

    # ── Step 5: Round numbers ──────────────────────────────────────
    df = df.with_columns([
        pl.col("open").cast(pl.Float64).round(4),
        pl.col("high").cast(pl.Float64).round(4),
        pl.col("low").cast(pl.Float64).round(4),
        pl.col("close").cast(pl.Float64).round(4),
        pl.col("volume").cast(pl.Int64),
    ])

    # ── Step 6: Convert date to clean string ──────────────────────
    # yfinance returns timezone-aware timestamps like:
    #   2024-01-15 00:00:00+00:00
    # We strip to "2024-01-15" — simple, consistent, timezone-free
    df = df.with_columns(
        pl.col("date")
        .cast(pl.Utf8)
        .str.slice(0, 10)
        .alias("date")
    )

    # ── Step 7: Add ticker column ──────────────────────────────────
    df = df.with_columns(
        pl.lit(ticker).alias("ticker")
    )

    # ── Step 8: Sort ascending by date ────────────────────────────
    df = df.sort("date")

    # ── Step 9: Remove duplicate dates ────────────────────────────
    df = df.unique(subset=["ticker", "date"], keep="last")

    logger.info(f"Transform complete: {len(df)} rows for {ticker}")
    return df