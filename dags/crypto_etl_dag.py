from airflow.decorators import dag, task
from airflow.sdk import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime

from utils.coingecko_client import fetch_raw_crypto_data
from utils.s3_loader import S3Loader
from utils.transform import normalize_raw_data, merge_with_dimensions
from utils.postgres_client import PostgresClient

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
    def get_run_datetime(ti, dag_run):
        run_date = dag_run.logical_date

        ti.xcom_push(
            key='dag_run_datetime',
            value={
                'timestamp': run_date.strftime('%Y-%m-%d %H:%M'),
                'date': run_date.strftime('%Y-%m-%d'),
                'time': run_date.strftime('%H-%M')
            }
        )

    @task
    def fetch_crypto_data(ti):
        strf_raw_crypto_data = fetch_raw_crypto_data(COINS)

        ti.xcom_push(key='strf_raw_crypto_data', value=strf_raw_crypto_data)
    
    @task
    def upload_raw_data_to_s3(ti):
        dag_run_datetime = ti.xcom_pull(key='dag_run_datetime', task_ids='get_run_datetime')

        S3Loader(
            aws_conn_id=AWS_CONN_ID, 
            bucket_name=BUCKET_NAME,
            file_path=f'raw/{dag_run_datetime['date']}/{dag_run_datetime['time']}.json',
            data=ti.xcom_pull(key='strf_raw_crypto_data', task_ids='fetch_crypto_data')
        ).load_raw_data()
    
    @task
    def normalize_raw_crypto_data(ti):
        strf_raw_crypto_data = ti.xcom_pull(key='strf_raw_crypto_data', task_ids='fetch_crypto_data')
        dag_run_datetime = ti.xcom_pull(key='dag_run_datetime', task_ids='get_run_datetime')

        normalized_data = normalize_raw_data(strf_raw_crypto_data, dag_run_datetime['timestamp'])

        ti.xcom_push(key='normalized_crypto_data', value=normalized_data)

    @task
    def upload_transformed_data_to_s3(ti):
        dag_run_datetime = ti.xcom_pull(key='dag_run_datetime', task_ids='get_run_datetime')

        S3Loader(    
            aws_conn_id=AWS_CONN_ID,
            bucket_name=BUCKET_NAME,
            file_path=f'staging/{dag_run_datetime['date']}/{dag_run_datetime['time']}.parquet',
            data=ti.xcom_pull(key='normalized_crypto_data', task_ids='normalize_raw_crypto_data')  
        ).load_transformed_data()

    create_db_schema = SQLExecuteQueryOperator(
        task_id='create_db_schema',
        conn_id=PG_CONN_ID,
        sql='sql/create_db_schema.sql',
        autocommit=True
    )

    load_dimensions = SQLExecuteQueryOperator(
        task_id='load_dimensions',
        conn_id=PG_CONN_ID,
        sql='sql/load_dimensions.sql',
        autocommit=True,
        params = {'coins': COINS},
        parameters = {'timestamp': '{{ ti.xcom_pull(key="dag_run_datetime", task_ids="get_run_datetime")["timestamp"] }}'}
    )

    @task
    def get_dimensions(ti):
        dag_run_datetime = ti.xcom_pull(key="dag_run_datetime", task_ids="get_run_datetime")

        pg_client = PostgresClient(postgres_conn_id=PG_CONN_ID)

        dim_coins = pg_client.get_records(
            'SELECT * FROM dim_coins WHERE NAME = ANY(%s)',
            parameters=(COINS,)
        )
        dim_times = pg_client.get_records(
            f"SELECT id, TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS timestamp FROM dim_times WHERE timestamp = '{dag_run_datetime['timestamp']}'",
            # parameters=DAG_RUN_START_DATETIME_STR
        )

        ti.xcom_push(key='dim_coins', value=dim_coins)
        ti.xcom_push(key='dim_times', value=dim_times)

    @task
    def merge_normalized_data_with_dimensions(ti):
        normalized_data = ti.xcom_pull(key='normalized_crypto_data', task_ids='normalize_raw_crypto_data')  
        dim_coins = ti.xcom_pull(key='dim_coins', task_ids='get_dimensions')
        dim_times = ti.xcom_pull(key='dim_times', task_ids='get_dimensions')

        market_data = merge_with_dimensions(normalized_data, dim_coins, dim_times)

        print(market_data)

        ti.xcom_push(key='market_data', value=market_data)

    @task
    def load_fact_market_data(ti):
        market_data = ti.xcom_pull(key='market_data', task_ids='merge_normalized_data_with_dimensions')

        PostgresClient(postgres_conn_id=PG_CONN_ID).insert(
            table='fact_market_data',
            records=market_data
        )

    (   
        [get_run_datetime(), fetch_crypto_data()] >>
        upload_raw_data_to_s3() >>
        normalize_raw_crypto_data() >>
        upload_transformed_data_to_s3() >>
        create_db_schema >>
        load_dimensions >>
        get_dimensions() >>
        merge_normalized_data_with_dimensions() >>
        load_fact_market_data()
    )

crypto_etl_dag()