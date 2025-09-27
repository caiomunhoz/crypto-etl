from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

from utils.coingecko_client import fetch_crypto_data
from utils.s3_loader import upload_to_s3

with DAG(
    'crypto_etl_2',
    schedule='@hourly',
    start_date=datetime(2025, 9, 1)
):
    task_fetch_crypto_data = PythonOperator(
        task_id='get_crypto_data',
        python_callable=fetch_crypto_data
    )
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )


    task_fetch_crypto_data >> task_upload_to_s3