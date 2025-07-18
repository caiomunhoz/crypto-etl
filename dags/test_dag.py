from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print('Hello, World!')

with DAG('test_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False) as dag:
    task = PythonOperator(
        task_id='hello_task',
        python_callable=hello
    )