# Apache Airflow DAG — replaces APScheduler entirely
#
# Why Airflow over APScheduler:
#   APScheduler: runs inside your Python process — if the process dies, schedule dies
#   Airflow:     independent scheduler with web UI, retry logic, alerting,
#                task dependency graphs, backfill capability, and execution history
#
# Install: pip install apache-airflow
# Start:   airflow standalone   (opens web UI at localhost:8080)
# ═══════════════════════════════════════════════════════════════════
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email  import EmailOperator
from airflow.utils.dates      import days_ago
from datetime                 import timedelta
import sys
import os
 
# Add project root to path so Airflow can find your modules
sys.path.insert(0, "/home/ubuntu/FINANCE_MACHINE_LEARNING")  # change to your path
 
DEFAULT_ARGS = {
    "owner":            "trading_system",
    "depends_on_past":  False,
    "email":            ["your@email.com"],
    "email_on_failure": True,          # email you when ETL fails
    "email_on_retry":   False,
    "retries":          3,             # retry failed tasks 3 times automatically
    "retry_delay":      timedelta(minutes=5),
}
 
 
def run_etl(**context):
    """Airflow task: run daily ETL update."""
    from etl.pipeline import run_daily_update
    result = run_daily_update()
 
    # Push result to XCom so downstream tasks can read it
    context["ti"].xcom_push(key="etl_result", value=result)
 
    if result["failures"]:
        raise ValueError(f"ETL had {len(result['failures'])} failures: {result['failures']}")
 
    return f"Loaded {result['rows']} rows successfully"
 
 
def run_feature_engineering(**context):
    """Airflow task: recompute features after ETL completes."""
    from features.engineer import update_features_for_all_tickers
    update_features_for_all_tickers()
    return "Features updated"
 
 
def run_ml_inference(**context):
    """Airflow task: generate LSTM predictions after features are ready."""
    from models.lstm_model import run_inference_all_tickers
    run_inference_all_tickers()
    return "Predictions generated"
 
 
# ── Define the DAG ────────────────────────────────────────────────
with DAG(
    dag_id="trading_ml_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily ETL → Features → ML Inference → Signals",
    schedule_interval="0 2 * * 1-5",  # 02:00 UTC, Monday–Friday only
    start_date=days_ago(1),
    catchup=False,
    tags=["trading", "ml", "etl"],
) as dag:
 
    # Task 1: Extract, Transform, Load
    etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )
 
    # Task 2: Feature engineering (runs AFTER ETL completes)
    features_task = PythonOperator(
        task_id="run_feature_engineering",
        python_callable=run_feature_engineering,
    )
 
    # Task 3: ML inference (runs AFTER features complete)
    inference_task = PythonOperator(
        task_id="run_ml_inference",
        python_callable=run_ml_inference,
    )
 
    # Task dependency chain: ETL → Features → Inference
    # If ETL fails, Features and Inference do not run
    # Airflow handles this automatically
    etl_task >> features_task >> inference_task
 
 