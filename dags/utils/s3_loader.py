from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from .transform import serialize_as_parquet

class S3Loader():
    def __init__(self, aws_conn_id: str, bucket_name: str, file_path: str, data: str | bytes, replace: bool = True) -> None:
        self.bucket_name = bucket_name
        self.s3_hook: S3Hook = S3Hook(aws_conn_id=aws_conn_id)
        self.file_path = file_path
        self.data = data
        self.replace = replace
    
    def load_raw_data(self) -> None:
        self.s3_hook.load_string(
            string_data=self.data,
            key=self.file_path,
            bucket_name=self.bucket_name,
            replace=self.replace
        )

    def load_transformed_data(self) -> None:
        serialized_parquet = serialize_as_parquet(self.data)

        self.s3_hook.load_bytes(
            bytes_data=serialized_parquet,
            key=self.file_path,
            bucket_name=self.bucket_name,
            replace=self.replace
        )