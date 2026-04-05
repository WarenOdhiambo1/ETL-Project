
# FILE 3 — etl/pipeline.py  (replace your existing pipeline.py)

#
# Fix: read per-timeframe period from TIMEFRAMES config
#      instead of using one global HISTORY_PERIOD for all intervals

 
# Parallel pipeline — all tickers extracted simultaneously per timeframe

 
from etl.extract   import extract_parallel
from etl.transform import transform
from etl.load      import load
from config        import TICKERS, TIMEFRAMES, TABLE_PREFIX
import logging
import time
 
logger = logging.getLogger(__name__)
 
 
def run_full_pipeline() -> dict:
    """
    Full ETL — parallel extraction, sequential transform + load.
 
    Architecture:
      EXTRACT:   all tickers fetched simultaneously (6x faster)
      TRANSFORM: CPU-bound Polars operations — fast enough sequentially
      LOAD:      Supabase upsert — sequential to avoid connection pool limits
 
    Returns summary dict with row counts and failure list.
    """
    total_rows = 0
    failures   = []
    start_time = time.time()
 
    for key, (interval, period, suffix) in TIMEFRAMES.items():
        table = f"{TABLE_PREFIX}_{suffix}"
        logger.info(f"\n── {interval} ({period}) → {table} ──────────────────")
 
        # ── EXTRACT: all tickers in parallel ──────────────────────
        raw_data = extract_parallel(TICKERS, period=period, interval=interval)
 
        # ── TRANSFORM + LOAD: sequential per ticker ───────────────
        for ticker in TICKERS:
            if ticker not in raw_data:
                failures.append(f"{ticker}/{interval}: extraction failed")
                continue
 
            try:
                clean_df    = transform(raw_data[ticker], ticker)
                rows_loaded = load(clean_df, table)
                total_rows += rows_loaded
                logger.info(f"✓ {ticker}/{interval} → {rows_loaded} rows → {table}")
 
            except Exception as e:
                msg = f"{ticker}/{interval}: {e}"
                failures.append(msg)
                logger.error(f"✗ {msg}")
 
    elapsed = time.time() - start_time
 
    # ── Summary ───────────────────────────────────────────────────
    logger.info(f"\n{'═'*50}")
    logger.info(f"Pipeline complete in {elapsed:.1f}s")
    logger.info(f"Total rows loaded: {total_rows:,}")
    if failures:
        logger.warning(f"Failures ({len(failures)}):")
        for f in failures:
            logger.warning(f"  - {f}")
    else:
        logger.info("All tickers loaded successfully ✓")
    logger.info(f"{'═'*50}")
 
    return {"rows": total_rows, "failures": failures, "elapsed_seconds": round(elapsed, 1)}
 
 
def run_daily_update() -> dict:
    """
    Lightweight daily refresh — last 5 days only.
    Called by Airflow DAG every morning at 02:00 UTC.
    """
    total_rows = 0
    failures   = []
 
    for key, (interval, _, suffix) in TIMEFRAMES.items():
        table = f"{TABLE_PREFIX}_{suffix}"
        raw_data = extract_parallel(TICKERS, period="5d", interval=interval)
 
        for ticker in TICKERS:
            if ticker not in raw_data:
                failures.append(f"{ticker}/{interval}")
                continue
            try:
                clean_df    = transform(raw_data[ticker], ticker)
                rows_loaded = load(clean_df, table)
                total_rows += rows_loaded
            except Exception as e:
                failures.append(f"{ticker}/{interval}: {e}")
 
    return {"rows": total_rows, "failures": failures}
 
 