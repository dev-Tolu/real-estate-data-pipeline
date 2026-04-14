from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
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
    Run the scraper directly via subprocess.
    The scraper writes raw JSON pages to MinIO (Bronze layer).
    """
    import subprocess, os
    result = subprocess.run(
        ['python', '/opt/airflow/include/scraper.py'],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            'MINIO_ENDPOINT':      os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            'MINIO_ROOT_USER':     os.getenv('MINIO_ROOT_USER', ''),
            'MINIO_ROOT_PASSWORD': os.getenv('MINIO_ROOT_PASSWORD', ''),
            'REDIS_PASSWORD':      os.getenv('REDIS_PASSWORD', ''),
            'POSTGRES_DSN':        os.getenv('POSTGRES_DSN', ''),
            'S3_BUCKET_RAW':       os.getenv('S3_BUCKET_RAW', 'realestate-raw-data'),
            'DATA_SOURCE_URLS':    os.getenv('DATA_SOURCE_URLS', ''),
            'SCRAPER_LOG_LEVEL':   os.getenv('SCRAPER_LOG_LEVEL', 'INFO'),
        }
    )
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)
    if result.returncode != 0:
        raise Exception(f"Scraper failed (exit {result.returncode}):\n{result.stderr}")
    return result.stdout


def submit_spark_and_wait(**context):
    """
    FIX: The original task used a BashOperator that fired the Spark REST
    submission and returned immediately when the curl response said
    'Driver successfully submitted'. The Spark job itself runs asynchronously
    on the cluster — the Airflow task was marking itself SUCCESS while the
    ETL job was still initialising, so the downstream GX validation always
    found an empty staging table.

    This PythonOperator:
    1. Submits the job via the REST API (same payload as before).
    2. Polls the /v1/submissions/status/<submissionId> endpoint every 15 s
       until the driver state is FINISHED (success) or FAILED/KILLED/ERROR.
    3. Raises an exception on failure so Airflow retries the task correctly.
    """
    import os, time, requests

    spark_master = os.getenv('SPARK_MASTER_URL', 'http://spark-master:6066')
    minio_user   = os.getenv('MINIO_ROOT_USER', 'minioadmin')
    minio_pass   = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')

    payload = {
        "action": "CreateSubmissionRequest",
        "appArgs": ["/opt/spark-apps/etl.py"],
        "appResource": "file:/opt/spark-apps/etl.py",
        "clientSparkVersion": "3.5.0",
        "mainClass": "org.apache.spark.deploy.PythonRunner",
        "environmentVariables": {
            "MINIO_ENDPOINT":      "minio:9000",
            "MINIO_ROOT_USER":     minio_user,
            "MINIO_ROOT_PASSWORD": minio_pass,
            "MINIO_ACCESS_KEY":    minio_user,
            "MINIO_SECRET_KEY":    minio_pass,
            "S3_BUCKET_RAW":       os.getenv('S3_BUCKET_RAW', 'realestate-raw-data'),
            "POSTGRES_DB":         os.getenv('POSTGRES_DB', 'airflow'),
            "POSTGRES_USER":       os.getenv('POSTGRES_USER', 'airflow'),
            "POSTGRES_PASSWORD":   os.getenv('POSTGRES_PASSWORD', 'airflow'),
        },
        "sparkProperties": {
            "spark.master":              "spark://spark-master:7077",
            "spark.app.name":            "realestate-etl",
            "spark.submit.deployMode":   "client",
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
        },
    }

    # 1. Submit
    resp = requests.post(
        f"{spark_master}/v1/submissions/create",
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    submission = resp.json()
    if not submission.get("success"):
        raise Exception(f"Spark submission failed: {submission}")

    submission_id = submission["submissionId"]
    logger.info(f"Spark job submitted: {submission_id}")

    # 2. Poll for completion
    terminal_states = {"FINISHED", "FAILED", "KILLED", "ERROR", "UNKNOWN"}
    poll_interval   = 15   # seconds
    timeout         = 900  # 15 minutes hard stop
    deadline        = time.time() + timeout

    while time.time() < deadline:
        time.sleep(poll_interval)
        status_resp = requests.get(
            f"{spark_master}/v1/submissions/status/{submission_id}",
            timeout=10,
        )
        if status_resp.status_code != 200:
            logger.warning(f"Status endpoint returned {status_resp.status_code}; retrying...")
            continue

        driver_state = status_resp.json().get("driverState", "UNKNOWN")
        logger.info(f"Spark driver state: {driver_state}")

        if driver_state in terminal_states:
            if driver_state == "FINISHED":
                logger.info("Spark ETL job completed successfully.")
                return
            else:
                raise Exception(
                    f"Spark ETL job ended in state '{driver_state}'. "
                    f"Check spark-master logs for submission {submission_id}."
                )

    raise Exception(
        f"Spark ETL job did not complete within {timeout}s. "
        f"Submission ID: {submission_id}"
    )


def execute_validation(**context):
    # Import inside the callable so Airflow's DAG parser ignores great_expectations.
    from validate_staging import run_validation
    run_validation()


def run_forecast(**context):
    """Run the forecast model as a subprocess."""
    import subprocess, os
    result = subprocess.run(
        ['python', '/opt/airflow/include/forecast.py'],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            'POSTGRES_DSN':                 os.getenv('POSTGRES_DSN', ''),
            'REDIS_URL':                    os.getenv('REDIS_URL', ''),
            'REDIS_PASSWORD':               os.getenv('REDIS_PASSWORD', ''),
            'FORECAST_HORIZON':             os.getenv('FORECAST_HORIZON', '90'),
            'FORECAST_CONFIDENCE_INTERVAL': os.getenv('FORECAST_CONFIDENCE_INTERVAL', '0.95'),
            'FORECAST_ZIP_CODES':           os.getenv('FORECAST_ZIP_CODES', ''),
        }
    )
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)
    if result.returncode != 0:
        raise Exception(f"Forecast failed (exit {result.returncode}):\n{result.stderr}")
    return result.stdout


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='real_estate_pipeline_v2',
    default_args=default_args,
    description='Scrape → Spark Flatten → GX Validate → dbt Transform → Forecast',
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

    # 2. Spark Job: Flatten JSON to Postgres Staging (Silver).
    # FIX: Changed from BashOperator (fire-and-forget) to PythonOperator that
    # submits the job AND polls until the driver reaches a terminal state.
    # This ensures the staging table is actually populated before GX runs.
    flatten_staging = PythonOperator(
        task_id='spark_flatten_staging',
        python_callable=submit_spark_and_wait,
        execution_timeout=timedelta(minutes=20),
    )

    # 3. The Bouncer: Validate the staging data before dbt touches it
    validate_staging = PythonOperator(
        task_id='gx_validate_staging',
        python_callable=execute_validation,
    )

    # 4. dbt: Run all models to build core tables (Gold)
    dbt_run = BashOperator(
        task_id='dbt_transform_models',
        bash_command='dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt',
    )

    # 5. Run dbt tests immediately after building
    dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command='dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt',
    )

    # 6. Forecast future prices based on new Gold data
    forecast = PythonOperator(
        task_id='run_ml_forecast',
        python_callable=run_forecast,
    )

    # ---- Pipeline order --------------------------------------------------
    [check_postgres, check_minio] >> scrape >> flatten_staging >> validate_staging >> dbt_run >> dbt_test >> forecast