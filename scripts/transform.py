import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def get_previous_df():
    folder_path = Path('data/processed')
    all_parquet_files = [datetime.strptime(file.stem, '%Y-%m-%d_%H-%M-%S') for file in folder_path.iterdir()]

    if not all_parquet_files: return None

    most_recent_file = folder_path / f'{max(all_parquet_files).strftime("%Y-%m-%d_%H-%M-%S")}.parquet'

    return pd.read_parquet(most_recent_file)

def get_extracted_data(json_file_path):
    return pd.read_json(json_file_path, orient='index')

def add_market_indicators_to_df(current_df, previous_df):
    if previous_df is not None:
        current_df['price_delta_4h'] = (current_df['usd'] - previous_df['usd']) / previous_df['usd'] * 100
        current_df['market_cap_delta_4h'] = (current_df['usd_market_cap'] - previous_df['usd_market_cap']) / previous_df['usd_market_cap'] * 100

    current_df['volume_ratio'] = current_df['usd_24h_vol'] / current_df['usd_market_cap']

    return current_df

def write_as_parquet(df, timestamp):
    parquet_file_path = Path(f'data/processed/{timestamp}.parquet')

    df.to_parquet(parquet_file_path, 'pyarrow')

    return str(parquet_file_path)

def run(ti):
    json_file_path = ti.xcom_pull(task_ids='extract_task')
    timestamp = Path(json_file_path).stem

    previous_df = get_previous_df()
    current_df = get_extracted_data(json_file_path)
    
    transformed_df = add_market_indicators_to_df(current_df, previous_df)
    parquet_file_path = write_as_parquet(transformed_df, timestamp)

    return parquet_file_path