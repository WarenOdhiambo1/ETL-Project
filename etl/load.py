import os
import polars as pl
from supabase import create_client, Client
from dotenv import load_dotenv
import logging

load_dotenv()  # reads your .env file automatically
logger = logging.getLogger(__name__)

# Build the Supabase client once (reused across all loads)
supabase: Client = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)


def load(df: pl.DataFrame, table: str) -> int:
    """
    Upsert records into Supabase.
    Upsert = INSERT if new, UPDATE if already exists.
    This means you can safely re-run the pipeline — no duplicates.

    Returns number of records processed.
    """
    if len(df) == 0:
        logger.warning("Empty DataFrame — nothing to load")
        return 0

    # Convert Polars DataFrame to list of dicts
    # [{"ticker":"BTC-USD","date":"2024-01-15","open":42000,...}, ...]
    records = df.to_dicts()

    # Supabase upsert — conflict on (ticker, date) = update instead of error
    result = (
        supabase
        .table(table)
        .upsert(records, on_conflict="ticker,date")
        .execute()
    )

    logger.info(f"Loaded {len(records)} rows into {table}")
    return len(records)


def retrieve(
    table: str,
    ticker: str,
    start_date: str = None,
    end_date: str = None
) -> pl.DataFrame:
    """
    Retrieve OHLCV data from Supabase as a Polars DataFrame.

    Args:
        table:      "ohlcv_daily", "ohlcv_hourly", etc.
        ticker:     "BTC-USD"
        start_date: "2022-01-01" (optional)
        end_date:   "2024-12-31" (optional)
    """
    query = (
        supabase
        .table(table)
        .select("*")
        .eq("ticker", ticker)
        .order("date")
    )

    if start_date:
        query = query.gte("date", start_date)
    if end_date:
        query = query.lte("date", end_date)

    result = query.execute()

    if not result.data:
        raise ValueError(f"No data found for {ticker} in {table}")

    return pl.DataFrame(result.data)