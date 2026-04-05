# ═══════════════════════════════════════════════════════
# FILE 3 — etl/pipeline.py  (replace your existing pipeline.py)
# ═══════════════════════════════════════════════════════
#
# Fix: read per-timeframe period from TIMEFRAMES config
#      instead of using one global HISTORY_PERIOD for all intervals
# ═══════════════════════════════════════════════════════
 
from etl.extract import extract
from etl.transform import transform
from etl.load import load
from config import TICKERS, TIMEFRAMES, TABLE_PREFIX
import logging
 
logger = logging.getLogger(__name__)
 
 
def run_full_pipeline():
    """
    Run the complete ETL for all tickers and all timeframes.
 
    Each timeframe now uses its own correct period:
      1d  → 20y
      1wk → 20y
      1h  → 700d  (Yahoo Finance limits hourly to 730 days)
    """
    total_rows = 0
    failures   = []
 
    for ticker in TICKERS:
        for key, (interval, period, suffix) in TIMEFRAMES.items():
            table = f"{TABLE_PREFIX}_{suffix}"   # e.g. "ohlcv_daily"
 
            try:
                raw_df      = extract(ticker, period=period, interval=interval)
                clean_df    = transform(raw_df, ticker)
                rows_loaded = load(clean_df, table)
                total_rows += rows_loaded
                logger.info(f"✓ {ticker}/{interval} → {rows_loaded} rows → {table}")
 
            except Exception as e:
                msg = f"{ticker}/{interval}: {e}"
                failures.append(msg)
                logger.error(f"✗ {msg}")
                continue   # one failure doesn't stop the rest
 
    # Summary
    logger.info(f"\nPipeline complete.")
    logger.info(f"  Total rows loaded: {total_rows}")
    if failures:
        logger.warning(f"  Failures ({len(failures)}):")
        for f in failures:
            logger.warning(f"    - {f}")
 
    return {"rows": total_rows, "failures": failures}
 
 
def run_daily_update():
    """Lightweight daily refresh — recent data only."""
    total_rows = 0
    for ticker in TICKERS:
        for key, (interval, _, suffix) in TIMEFRAMES.items():
            # For daily updates, pull only 5 days regardless of timeframe
            update_period = "5d" if interval != "1h" else "5d"
            table = f"{TABLE_PREFIX}_{suffix}"
            try:
                raw_df      = extract(ticker, period=update_period, interval=interval)
                clean_df    = transform(raw_df, ticker)
                rows_loaded = load(clean_df, table)
                total_rows += rows_loaded
            except Exception as e:
                logger.error(f"Daily update failed {ticker}/{interval}: {e}")
    return total_rows