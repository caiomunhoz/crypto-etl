from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from scripts.extract import run as run_extract
from scripts.transform import run as run_transform

default_args = {
    'owner': 'Caio Munhoz',
    'email': 'caio.munhoz@email.com',
    'start_date': datetime(2025, 7, 18),
    'schedule': timedelta(hours=4),
    'catchup': False
}

with DAG(
    'crypto_etl',
    default_args=default_args
) as dag:
    
    extract = PythonOperator(
        task_id='extract_task',
        python_callable=run_extract
    )

    transform = PythonOperator(
        task_id='transform_task',
        python_callable=run_transform
    )

    extract >> transform 