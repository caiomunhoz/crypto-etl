from airflow.decorators import dag, task
from airflow.sdk import Variable
from pendulum import datetime

from utils.coingecko_client import fetch_raw_crypto_data
from utils.s3_loader import S3Loader
from utils.transform import normalize_raw_data

BUCKET_NAME = Variable.get('bucket_name')
AWS_CONN_ID = 'aws_conn'

@dag(
    'crypto_etl',
    schedule='@hourly',
    start_date=datetime(2025, 9, 1)
)
def crypto_etl_dag():
    @task
    def fetch_crypto_data(ti):
       strf_raw_crypto_data = fetch_raw_crypto_data()
       ti.xcom_push(key='strf_raw_crypto_data', value=strf_raw_crypto_data)
    
    @task
    def upload_raw_data_to_s3(ti):
        start_date = ti.start_date.strftime('%Y-%m-%d')
        start_time = ti.start_date.strftime('%H-%M')

        S3Loader(
            aws_conn_id=AWS_CONN_ID, 
            bucket_name=BUCKET_NAME,
            file_path=f'raw/{start_date}/{start_time}.json',
            data=ti.xcom_pull(key='strf_raw_crypto_data', task_ids='fetch_crypto_data')
        ).load_raw_data()
    
    @task
    def normalize_raw_crypto_data(ti):
        strf_raw_crypto_data = ti.xcom_pull(key='strf_raw_crypto_data', task_ids='fetch_crypto_data')
        timestamp = ti.start_date.strftime('%Y-%m-%d %H-%M')

        normalized_data = normalize_raw_data(strf_raw_crypto_data, timestamp)
        ti.xcom_push(key='normalized_crypto_data', value=normalized_data)

    @task
    def upload_transformed_data_to_s3(ti):
        start_date = ti.start_date.strftime('%Y-%m-%d')
        start_time = ti.start_date.strftime('%H-%M')

        S3Loader(    
            aws_conn_id=AWS_CONN_ID,
            bucket_name=BUCKET_NAME,
            file_path=f'staging/{start_date}/{start_time}.parquet',
            data=ti.xcom_pull(key='normalized_crypto_data', task_ids='normalize_raw_crypto_data')  
        ).load_transformed_data()

    fetch_crypto_data() >> upload_raw_data_to_s3() >> normalize_raw_crypto_data() >> upload_transformed_data_to_s3()

crypto_etl_dag()