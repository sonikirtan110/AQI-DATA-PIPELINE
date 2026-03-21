"""Daily AQI pipeline: Medallion Architecture (Bronze -> Silver -> Gold).

Full 6-task pipeline:
  1. Fetch OGD API data -> upload to S3
  2. COPY INTO Snowflake Bronze
  3. dbt run Silver (incremental)
  4. dbt run Gold (aggregations)
  5. dbt test (data quality)
  6. Log completion
"""
from __future__ import annotations
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

# -- Default Arguments ------------------------------------
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email': ['your@email.com'],
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1),
    'start_date': datetime(2026, 1, 1),
}

# -- DAG Definition -------------------------------------
with DAG(
    dag_id='aqi_daily_pipeline',
    description='AQI Medallion Pipeline: API -> S3 -> Snowflake -> dbt (Silver -> Gold)',
    schedule='0 6 * * *',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['aqi', 'production', 'medallion'],
) as dag:

    def fetch_ogd_task(**context):
        """Fetch OGD data and upload to S3."""
        result = fetch_and_upload_ogd(limit=1000, offset=0)
        context['task_instance'].xcom_push(key='s3_key', value=result['s3_key'])
        context['task_instance'].xcom_push(key='record_count', value=result['records_uploaded'])
        return result

    t_fetch = PythonOperator(
        task_id='fetch_ogd_api',
        python_callable=fetch_ogd_task,
    )

    t_bronze = SQLExecuteQueryOperator(
        task_id='copy_into_bronze',
        conn_id='snowflake_default',
        sql=COPY_INTO_BRONZE_SQL,
        database='aqi_db',
    )

    t_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command='cd /opt/airflow/dbt && dbt run --select silver --profiles-dir .',
    )

    t_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /opt/airflow/dbt && dbt run --select gold --profiles-dir .',
    )

    t_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
    )

    t_done = BashOperator(
        task_id='log_completion',
        bash_command='echo "Pipeline complete at $(date). Records processed."',
    )

    t_fetch >> t_bronze >> t_silver >> t_gold >> t_test >> t_done
