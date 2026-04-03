from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['alerts@realestate.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_scraper():
    """Run scraper using Docker SDK or subprocess"""
    result = subprocess.run(
        ['docker', 'exec', 'realestate-scraper', 'python', 'scraper.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Scraper failed: {result.stderr}")
    return result.stdout

def run_spark_etl():
    """Run Spark ETL job"""
    result = subprocess.run(
        ['docker', 'exec', 'realestate-spark-master', 'spark-submit', 
         '--master', 'spark://spark-master:7077', 
         '/opt/spark-apps/etl.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Spark ETL failed: {result.stderr}")
    return result.stdout

def run_forecast():
    """Run forecast model"""
    result = subprocess.run(
        ['docker', 'exec', 'realestate-forecast', 'python', 'forecast.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Forecast failed: {result.stderr}")
    return result.stdout

with DAG(
    'real_estate_pipeline_local',
    default_args=default_args,
    description='Real Estate Pipeline - LocalExecutor',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['realestate', 'localexecutor']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    scrape = PythonOperator(
        task_id='scrape_data',
        python_callable=run_scraper
    )
    
    etl = PythonOperator(
        task_id='run_spark_etl',
        python_callable=run_spark_etl
    )
    
    forecast = PythonOperator(
        task_id='run_forecast',
        python_callable=run_forecast
    )
    
    end = DummyOperator(task_id='end')
    
    start >> scrape >> etl >> forecast >> end