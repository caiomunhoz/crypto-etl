import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, MetaData, Table, Column, PrimaryKeyConstraint, DateTime, String, Numeric

def create_table(engine):
    metadata = MetaData()

    Table(
        'crypto_prices',
        metadata,
        Column('timestamp', DateTime, nullable=False),
        Column('name', String(32), nullable=False),
        Column('price_usd', Numeric(10, 2), nullable=False),
        Column('market_cap_usd', Numeric(20, 2), nullable=False),
        Column('24h_vol_usd', Numeric(20, 2), nullable=False),
        Column('volume_ratio', Numeric(10, 3), nullable=False),
        Column('price_delta_4h', Numeric(5, 3)),
        Column('market_cap_delta_4h', Numeric(5, 3)),
        PrimaryKeyConstraint('timestamp', 'name')
    )

    metadata.create_all(engine)


def get_transformed_data(parquet_file_path):
    return pd.read_parquet(parquet_file_path)

def load_to_db(df, engine):
    with engine.connect() as con:
        df.to_sql(
            'crypto_prices',
            con=con.connection,
            if_exists='append',
            index_label='name'
        )

def run(ti):
    parquet_file_path = ti.xcom_pull(task_ids='transform_task')
    transformed_df = get_transformed_data(parquet_file_path)

    Path('db').mkdir(parents=True, exist_ok=True)

    engine = create_engine('sqlite:///db/crypto_prices.sqlite')
    create_table(engine)

    load_to_db(transformed_df, engine)