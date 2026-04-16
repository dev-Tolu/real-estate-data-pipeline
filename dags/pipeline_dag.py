"""
Real Estate Pipeline DAG — Airflow 3.x

After any change to this file, delete dags/__pycache__/pipeline_dag.cpython-*.pyc
or restart the airflow-scheduler container so Airflow re-parses the DAG from source.
"""
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def run_scraper(**context):
    """Invoke the scraper and upload raw JSON pages to MinIO (Bronze layer)."""
    import subprocess, os

    result = subprocess.run(
        ["python", "/opt/airflow/include/scraper.py"],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "MINIO_ENDPOINT":      os.getenv("MINIO_ENDPOINT", "minio:9000"),
            "MINIO_ROOT_USER":     os.getenv("MINIO_ROOT_USER", ""),
            "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", ""),
            "REDIS_PASSWORD":      os.getenv("REDIS_PASSWORD", ""),
            "POSTGRES_DSN":        os.getenv("POSTGRES_DSN", ""),
            "S3_BUCKET_RAW":       os.getenv("S3_BUCKET_RAW", "realestate-raw-data"),
            "DATA_SOURCE_URLS":    os.getenv("DATA_SOURCE_URLS", ""),
            "SCRAPER_LOG_LEVEL":   os.getenv("SCRAPER_LOG_LEVEL", "INFO"),
        },
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
    Submit the ETL job to the Spark Standalone REST API and block until done.
    This function handles the entire lifecycle of the Spark job:
    1. Submits the job via REST API with all necessary configs and environment variables.
    2. Polls the job status every 15 seconds until it reaches a terminal state (FINISHED, FAILED, KILLED, ERROR, UNKNOWN).
    3. Logs the progress and final outcome, including where to find logs on failure.

    """
    import os, time, requests

    # The REST submission port is 6066 (not the WebUI port 8080/8082).
    # Ensure `- "6066:6066"` is in docker-compose.yml under spark-master ports.
    spark_master = os.getenv("SPARK_MASTER_URL", "http://spark-master:6066")
    minio_user   = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_pass   = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    # The Bitnami Python path. This is the interpreter that has pyspark,
    # beautifulsoup4, and our other deps installed.
    bitnami_python = "/opt/bitnami/python/bin/python3"

    # Spark 3 on Java 17 requires explicit memory API access
    java_17_opts = (
        "-XX:+IgnoreUnrecognizedVMOptions "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )

    payload = {
        "action":      "CreateSubmissionRequest",
        # args(0) = primary python script, args(1) = pyFiles (empty)
        "appArgs":     ["/opt/spark-apps/etl.py", ""],
        "appResource": "file:/opt/spark-apps/etl.py",
        "clientSparkVersion": "3.5.0",
        "mainClass":   "org.apache.spark.deploy.PythonRunner",
        "environmentVariables": {
            # Explicitly set the Python interpreter so PythonRunner never falls
            # back to a system python3 that lacks pyspark / beautifulsoup4.
            "PYSPARK_PYTHON":        bitnami_python,
            "PYSPARK_DRIVER_PYTHON": bitnami_python,
            # MinIO / S3A credentials
            "MINIO_ENDPOINT":        "minio:9000",
            "MINIO_ROOT_USER":       minio_user,
            "MINIO_ROOT_PASSWORD":   minio_pass,
            "MINIO_ACCESS_KEY":      minio_user,
            "MINIO_SECRET_KEY":      minio_pass,
            "S3_BUCKET_RAW":         os.getenv("S3_BUCKET_RAW", "realestate-raw-data"),
            # PostgreSQL
            "POSTGRES_DB":           os.getenv("POSTGRES_DB", "airflow"),
            "POSTGRES_USER":         os.getenv("POSTGRES_USER", "airflow"),
            "POSTGRES_PASSWORD":     os.getenv("POSTGRES_PASSWORD", "airflow"),
        },
        "sparkProperties": {
            "spark.master":            "spark://spark-master:7077",
            "spark.app.name":          "realestate-etl",
            "spark.submit.deployMode": "cluster",
            "spark.pyspark.python":        bitnami_python,
            "spark.pyspark.driver.python": bitnami_python,
            "spark.driver.extraJavaOptions":   java_17_opts,
            "spark.executor.extraJavaOptions": java_17_opts,
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
        raise Exception(f"Spark submission rejected: {submission}")

    submission_id = submission["submissionId"]
    logger.info("Spark job submitted: %s", submission_id)

    # 2. Poll until terminal state
    terminal_states = {"FINISHED", "FAILED", "KILLED", "ERROR", "UNKNOWN"}
    poll_interval   = 15    # seconds between polls
    timeout         = 900   # 15-minute hard stop
    deadline        = time.time() + timeout

    while time.time() < deadline:
        time.sleep(poll_interval)
        try:
            status_resp = requests.get(
                f"{spark_master}/v1/submissions/status/{submission_id}",
                timeout=10,
            )
        except requests.RequestException as exc:
            logger.warning("Status poll network error: %s — retrying", exc)
            continue

        if status_resp.status_code != 200:
            logger.warning("Status endpoint returned %s — retrying", status_resp.status_code)
            continue

        driver_state = status_resp.json().get("driverState", "UNKNOWN")
        logger.info("Spark driver state: %s", driver_state)

        if driver_state in terminal_states:
            if driver_state == "FINISHED":
                logger.info("Spark ETL job completed successfully.")
                return
            # On failure, log the stderr URL so the user knows where to look.
            raise Exception(
                f"Spark ETL job ended in state '{driver_state}'.\n"
                f"Check worker stderr: docker exec <spark-worker> "
                f"cat /opt/bitnami/spark/work/{submission_id}/stderr\n"
                f"Submission ID: {submission_id}"
            )

    raise Exception(
        f"Spark ETL job did not complete within {timeout}s. "
        f"Submission ID: {submission_id}"
    )


def execute_validation(**context):
    from validate_staging import run_validation
    run_validation()


def run_forecast(**context):
    import subprocess, os

    result = subprocess.run(
        ["python", "/opt/airflow/include/forecast.py"],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "POSTGRES_DSN":                 os.getenv("POSTGRES_DSN", ""),
            "REDIS_URL":                     os.getenv("REDIS_URL", ""),
            "REDIS_PASSWORD":               os.getenv("REDIS_PASSWORD", ""),
            "FORECAST_HORIZON":             os.getenv("FORECAST_HORIZON", "90"),
            "FORECAST_CONFIDENCE_INTERVAL": os.getenv("FORECAST_CONFIDENCE_INTERVAL", "0.95"),
            "FORECAST_ZIP_CODES":           os.getenv("FORECAST_ZIP_CODES", ""),
        },
    )
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)
    if result.returncode != 0:
        raise Exception(f"Forecast failed (exit {result.returncode}):\n{result.stderr}")
    return result.stdout


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="real_estate_pipeline_v2",
    default_args=default_args,
    description="Scrape → Spark Flatten → GX Validate → dbt Transform → Forecast",
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    check_postgres = BashOperator(
        task_id="check_postgres",
        bash_command=(
            "python -c \""
            "import psycopg2, os; "
            "psycopg2.connect(dsn=os.getenv('POSTGRES_DSN')).close(); "
            "print('postgres ok')"
            "\""
        ),
    )

    check_minio = BashOperator(
        task_id="check_minio",
        bash_command="curl -sf http://minio:9000/minio/health/live && echo 'minio ok'",
    )

    scrape = PythonOperator(
        task_id="extract_to_datalake",
        python_callable=run_scraper,
    )

    flatten_staging = PythonOperator(
        task_id="spark_flatten_staging",
        python_callable=submit_spark_and_wait,
        execution_timeout=timedelta(minutes=20),
    )

    validate_staging = PythonOperator(
        task_id="gx_validate_staging",
        python_callable=execute_validation,
    )

    dbt_run = BashOperator(
        task_id="dbt_transform_models",
        bash_command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_models",
        bash_command="dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
    )

    forecast = PythonOperator(
        task_id="run_ml_forecast",
        python_callable=run_forecast,
    )

    [check_postgres, check_minio] >> scrape >> flatten_staging >> validate_staging >> dbt_run >> dbt_test >> forecast
