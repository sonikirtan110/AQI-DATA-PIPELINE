"""Backfill DAG: fetch historical OGD AQI data and build Medallion layers."""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
import sys

sys.path.insert(0, '/opt/airflow/include/scripts')
from fetch_ogd import fetch_and_upload_ogd

COPY_INTO_BRONZE_SQL = Path('/opt/airflow/include/sql/copy_into_bronze.sql').read_text(encoding='utf-8')

default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2026, 1, 1),
}

dag = DAG(
    "aqi_backfill_pipeline",
    default_args=default_args,
    description="Backfill historical OGD AQI data: API -> S3 -> Snowflake -> dbt (Silver -> Gold)",
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["aqi", "backfill", "ingestion"],
)


def fetch_ogd_backfill_task(limit=1000, offset=0, **context):
    """Fetch OGD data in batches for backfill."""
    result = fetch_and_upload_ogd(limit=limit, offset=offset, paginate=True)
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

copy_into_bronze = SQLExecuteQueryOperator(
    task_id='copy_into_bronze',
    conn_id='snowflake_default',
    sql=COPY_INTO_BRONZE_SQL,
    database='aqi_db',
    dag=dag,
)

dbt_run_silver = BashOperator(
    task_id='dbt_run_silver',
    bash_command='cd /opt/airflow/dbt && dbt run --select silver --profiles-dir .',
    dag=dag,
)

dbt_run_gold = BashOperator(
    task_id='dbt_run_gold',
    bash_command='cd /opt/airflow/dbt && dbt run --select gold --profiles-dir .',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
    dag=dag,
)

log_completion = BashOperator(
    task_id='log_completion',
    bash_command='echo "Backfill pipeline complete at $(date)."',
    dag=dag,
)

fetch_backfill >> copy_into_bronze >> dbt_run_silver >> dbt_run_gold >> dbt_test >> log_completion
