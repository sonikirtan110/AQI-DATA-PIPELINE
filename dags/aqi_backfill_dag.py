"""Backfill DAG: fetch historical OGD AQI data."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import sys

sys.path.insert(0, '/opt/airflow/include/scripts')
from fetch_ogd import fetch_and_upload_ogd

default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2026, 1, 1),
}

dag = DAG(
    "aqi_backfill_pipeline",
    default_args=default_args,
    description="Backfill historical OGD AQI data to S3 bronze",
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["aqi", "backfill", "ingestion"],
)


def fetch_ogd_backfill_task(limit=1000, offset=0, **context):
    """Fetch OGD data in batches for backfill."""
    result = fetch_and_upload_ogd(limit=limit, offset=offset)
    context["task_instance"].xcom_push(key="s3_key", value=result["s3_key"])
    context["task_instance"].xcom_push(key="record_count", value=result["records_uploaded"])
    return result


fetch_backfill = PythonOperator(
    task_id="fetch_ogd_backfill",
    python_callable=fetch_ogd_backfill_task,
    op_kwargs={
        "limit": 1000,  # Larger batch for backfill
        "offset": 0,
    },
    dag=dag,
)
