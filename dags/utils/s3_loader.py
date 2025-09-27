from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_to_s3(ti):
    data = ti.xcom_pull(key='crypto_data', task_ids='get_crypto_data')

    s3_hook = S3Hook(aws_conn_id='aws_conn')

    s3_hook.load_string(
        string_data=data,
        key='raw/data.json',
        bucket_name='crypto-pipeline-caiomunhoz'
    )
    