from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from spotify_etl import run_spotify_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 17),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args = default_args,
    description = 'Spotify ETL process',
    schedule_interval = timedelta(days=1)
)

run_etl = PythonOperator(
    task_id='spotify_etl',
    python_callable = run_spotify_etl,
    dag = dag
)

run_etl