from airflow.decorators import dag, task
from airflow.sdk import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

from utils.coingecko_client import fetch_raw_crypto_data
from utils.s3_loader import S3Loader
from utils.transform import normalize_raw_data

BUCKET_NAME = Variable.get('bucket_name')
AWS_CONN_ID = 'aws_conn'
PG_CONN_ID = 'postgres_conn'

COINS = ['bitcoin', 'ethereum', 'solana']

@dag(
    'crypto_etl',
    schedule='@hourly',
    start_date=datetime(2025, 9, 1)
)
def crypto_etl_dag():
    @task
    def set_dag_run_datetime_xcoms(ti, **kwargs):
        start_datetime = kwargs['dag_run'].start_date

        ti.xcom_push(key='dag_run_timestamp', value=start_datetime)
        ti.xcom_push(key='dag_run_date', value=start_datetime.strftime('%Y-%m-%d'))
        ti.xcom_push(key='dug_run_time', value=start_datetime.strftime('%H-%M'))

    @task
    def fetch_crypto_data(ti):
        strf_raw_crypto_data = fetch_raw_crypto_data(COINS)

        ti.xcom_push(key='strf_raw_crypto_data', value=strf_raw_crypto_data)
    
    @task
    def upload_raw_data_to_s3(ti):
        dag_run_date = ti.xcom_pull(key='dag_run_date', task_ids='set_dag_run_datetime_xcoms')
        dag_run_time = ti.xcom_pull(key='dag_run_time', task_ids='set_dag_run_datetime_xcoms')

        S3Loader(
            aws_conn_id=AWS_CONN_ID, 
            bucket_name=BUCKET_NAME,
            file_path=f'raw/{dag_run_date}/{dag_run_time}.json',
            data=ti.xcom_pull(key='strf_raw_crypto_data', task_ids='fetch_crypto_data')
        ).load_raw_data()
    
    @task
    def normalize_raw_crypto_data(ti):
        dag_run_timestamp = ti.xcom_pull(key='dag_run_timestamp', task_ids='set_dag_run_datetime_xcoms')

        strf_raw_crypto_data = ti.xcom_pull(key='strf_raw_crypto_data', task_ids='fetch_crypto_data')
        normalized_data = normalize_raw_data(strf_raw_crypto_data, dag_run_timestamp)

        ti.xcom_push(key='normalized_crypto_data', value=normalized_data)

    @task
    def upload_transformed_data_to_s3(ti):
        dag_run_date = ti.xcom_pull(key='dag_run_date', task_ids='set_dag_run_datetime_xcoms')
        dag_run_time = ti.xcom_pull(key='dag_run_time', task_ids='set_dag_run_datetime_xcoms')

        S3Loader(    
            aws_conn_id=AWS_CONN_ID,
            bucket_name=BUCKET_NAME,
            file_path=f'staging/{dag_run_date}/{dag_run_time}.parquet',
            data=ti.xcom_pull(key='normalized_crypto_data', task_ids='normalize_raw_crypto_data')  
        ).load_transformed_data()

    create_db_schema = SQLExecuteQueryOperator(
        task_id='create_db_schema',
        conn_id=PG_CONN_ID,
        sql='sql/create_db_schema.sql'
    )

    load_dimensions = SQLExecuteQueryOperator(
        task_id='load_dimensions',
        conn_id=PG_CONN_ID,
        sql='sql/load_dimensions.sql',
        parameters={
            'timestamp': '{{ ti.xcom_pull(key="dag_run_timestamp", task_ids="set_dag_run_datetime_xcoms") }}',
            'coins': ','.join(COINS)
        }
    )

    (   
        [set_dag_run_datetime_xcoms(), fetch_crypto_data()] >>
        upload_raw_data_to_s3() >>
        normalize_raw_crypto_data() >>
        upload_transformed_data_to_s3() >>
        create_db_schema >>
        load_dimensions
    )

crypto_etl_dag()