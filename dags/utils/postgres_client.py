import os
from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresClient:
    def __init__(self, postgres_conn_id):
        self.pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def get_records(self, sql, parameters=None):
        if os.path.isfile(sql):
            with open(sql, 'r') as file:
                sql_query = file.read()
        else:
            sql_query = sql

        return self.pg_hook.get_records(sql_query, parameters=parameters)

    def insert(self, table, records):
        self.pg_hook.insert_rows(
            table=table,
            rows=records,
            target_fields=['time_id', 'coin_id', 'price_usd', 'volume_24h_usd', 'market_cap_usd']
        )