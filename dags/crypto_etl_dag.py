from airflow.sdk.decorators import DAG

from airflow.operators.python import PythonOperator
from pendulum import datetime

from scripts.extract import run as run_extract
from scripts.transform import run as run_transform
from scripts.load import run as run_load

with DAG(
    'crypto_etl',
    schedule='@hourly',
    start_date=datetime(2025, 9, 1)
):
    get_crypto_data = HttpOperator(
        task_id='get_crypto_data',
        endpoint='get'
    )


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