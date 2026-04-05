from apscheduler.schedulers.blocking import BlockingScheduler
from etl.pipeline import run_daily_update
import logging

logging.basicConfig(level=logging.INFO)

scheduler = BlockingScheduler()

# Run every day at 02:00 UTC (after US markets close)
scheduler.add_job(
    run_daily_update,
    trigger='cron',
    hour=2,
    minute=0,
    id='daily_etl'
)

if __name__ == "__main__":
    print("Scheduler running. Daily update at 02:00 UTC.")
    print("Press Ctrl+C to stop.")
    scheduler.start()