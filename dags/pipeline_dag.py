from validate_staging import run_validation
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def run_scraper(**context):
    """
    Run the scraper directly via Python import rather than docker exec.
    Airflow workers share the same filesystem mounts as the other containers,
    so we can import and call the scraper module directly — no Docker socket
    needed, no privilege escalation, no brittle container name coupling.
    """
    import sys, os
    sys.path.insert(0, '/opt/airflow/dags')

    # The scraper module lives at /opt/spark-apps/scraper.py via volume mount.
    # We exec it as a subprocess so it runs in its own process and any crash
    # is captured cleanly rather than killing the Airflow worker.
    import subprocess
    result = subprocess.run(
        ['python', '/opt/airflow/include/scraper.py'],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            'MINIO_ENDPOINT':    os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            'MINIO_ROOT_USER':   os.getenv('MINIO_ROOT_USER', ''),
            'MINIO_ROOT_PASSWORD': os.getenv('MINIO_ROOT_PASSWORD', ''),
            'REDIS_PASSWORD':    os.getenv('REDIS_PASSWORD', ''),
            'POSTGRES_DSN':      os.getenv('POSTGRES_DSN', ''),
            'S3_BUCKET_RAW':     os.getenv('S3_BUCKET_RAW', 'realestate-raw-data'),
            'DATA_SOURCE_URLS':  os.getenv('DATA_SOURCE_URLS', ''),
            'SCRAPER_LOG_LEVEL': os.getenv('SCRAPER_LOG_LEVEL', 'INFO'),
        }
    )
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)
    if result.returncode != 0:
        raise Exception(f"Scraper failed (exit {result.returncode}):\n{result.stderr}")
    return result.stdout

def run_forecast(**context):
    """Run the forecast model as a subprocess."""
    import subprocess, os
    result = subprocess.run(
        ['python', '/opt/airflow/include/forecast.py'],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            'POSTGRES_DSN':                  os.getenv('POSTGRES_DSN', ''),
            'REDIS_PASSWORD':                os.getenv('REDIS_PASSWORD', ''),
            'FORECAST_HORIZON':              os.getenv('FORECAST_HORIZON', '90'),
            'FORECAST_CONFIDENCE_INTERVAL':  os.getenv('FORECAST_CONFIDENCE_INTERVAL', '0.95'),
            'FORECAST_ZIP_CODES':            os.getenv('FORECAST_ZIP_CODES', ''),
        }
    )
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)
    if result.returncode != 0:
        raise Exception(f"Forecast failed (exit {result.returncode}):\n{result.stderr}")
    return result.stdout


def refresh_materialized_views(**context):
    """
    Refresh the market_trends_30d materialized view so Grafana
    dashboards always show up-to-date data after each pipeline run.
    """
    import psycopg2, os
    conn = psycopg2.connect(dsn=os.getenv('POSTGRES_DSN'))
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY market_trends_30d;")
    cur.close()
    conn.close()
    logger.info("Refreshed materialized view: market_trends_30d")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='real_estate_pipeline_v2',
    default_args=default_args,
    description='Scrape → Spark Flatten → dbt Transform → Forecast',
    schedule='0 */6 * * *',    
    catchup=False,
    max_active_runs=1,
) as dag:

    # ---- 1. Health checks ------------------------------------------------
    check_postgres = BashOperator(
        task_id='check_postgres',
        bash_command=(
            'python -c "'
            'import psycopg2, os; '
            'psycopg2.connect(dsn=os.getenv(\'POSTGRES_DSN\')).close(); '
            'print(\'postgres ok\')'
            '"'
        ),
    )

    check_minio = BashOperator(
        task_id='check_minio',
        bash_command="curl -sf http://minio:9000/minio/health/live && echo 'minio ok'",
    )

    # 1. Scrape data to MinIO (Bronze)
    scrape = PythonOperator(
        task_id='extract_to_datalake',
        python_callable=run_scraper,
    )

    # 2. Spark Job: Flatten JSON to Postgres Staging (Silver)
    flatten_staging = BashOperator(
        task_id='spark_flatten_staging',
        bash_command='spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/airflow/include/etl.py',
    )

    # 3. The Bouncer: Validate the staging data before dbt touches it
    validate_staging = PythonOperator(
        task_id='gx_validate_staging',
        python_callable=run_validation, 
    )

    # 4. dbt: Run all models to build core tables (Gold)
    dbt_run = BashOperator(
        task_id='dbt_transform_models',
        bash_command='dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt',
    )
    
    # Optional: Run dbt tests immediately after building
    dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command='dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt',
    )

    # 5. Forecast future prices based on new Gold data
    forecast = PythonOperator(
        task_id='run_ml_forecast',
        python_callable=run_forecast,
    )

    # 6. Refresh Materialized Views for Grafana
    refresh_views = PythonOperator(
        task_id='refresh_grafana_views',
        python_callable=refresh_materialized_views,
    )    

    # ---- Pipeline order --------------------------------------------------
    [check_postgres, check_minio] >> scrape >> flatten_staging >> validate_staging >> dbt_run >> dbt_test >> forecast >> refresh_views
