from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from scripts.extract import run as run_extract
from scripts.transform import run as run_transform
from scripts.load import run as run_load

default_args = {
    'owner': 'Caio Munhoz',
    'email': 'caio.munhoz@email.com',
    'start_date': datetime(2025, 7, 18),
    'catchup': False
}

with DAG(
    'crypto_etl',
    schedule=timedelta(hours=4),
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
    load = PythonOperator(
        task_id='load_task',
        python_callable=run_load
    )

    extract >> transform >> load