import os
from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresClient:
    def __init__(self, postgres_conn_id: str):
        self.pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def get_records(self, sql, parameters=None):
        SQL_PATH = f'/opt/airflow/dags/{sql}'

        if os.path.isfile(SQL_PATH):
            sql_query = open(SQL_PATH, 'r').read()
        else:
            sql_query = sql

        return self.pg_hook.get_records(sql_query, parameters=parameters)

    def insert(self, table, records):
        self.pg_hook.insert_rows(
            table=table,
            rows=records
        )