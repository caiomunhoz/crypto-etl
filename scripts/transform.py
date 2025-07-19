import pandas as pd
from pathlib import Path
from .utils import format_timestamp_to_file_name, parse_custom_timestamp

def get_previous_df():
    folder_path = Path('data/processed')

    if not folder_path.exists(): return None

    all_parquet_files = [parse_custom_timestamp(file.stem) for file in folder_path.iterdir()]

    previous_timestamp = max(all_parquet_files)
    previous_file = folder_path / f'{format_timestamp_to_file_name(previous_timestamp)}.parquet'

    return pd.read_parquet(previous_file)

def get_extracted_data(json_file_path):
    return pd.read_json(json_file_path, orient='index')

def transform_df(current_df, previous_df, timestamp):
    current_df = current_df.rename(columns={
        'usd': 'price_usd',
        'usd_market_cap': 'market_cap_usd',
        'usd_24h_vol': '24h_vol_usd'
    })

    current_df['volume_ratio'] = current_df['24h_vol_usd'] / current_df['market_cap_usd']
    current_df['timestamp'] = timestamp

    if previous_df is not None:
        current_df['price_delta_4h'] = (current_df['price_usd'] - previous_df['price_usd']) / previous_df['price_usd'] * 100
        current_df['market_cap_delta_4h'] = (current_df['market_cap_usd'] - previous_df['market_cap_usd']) / previous_df['market_cap_usd'] * 100

    return current_df

def write_as_parquet(df, file_name):
    parquet_file_path = Path(f'data/processed/{file_name}.parquet')
    parquet_file_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(parquet_file_path, engine='pyarrow')

    return str(parquet_file_path)

def run(ti):
    json_file_path = ti.xcom_pull(task_ids='extract_task')
    file_name = Path(json_file_path).stem
    timestamp = parse_custom_timestamp(file_name)

    previous_df = get_previous_df()
    current_df = get_extracted_data(json_file_path)

    transformed_df = transform_df(current_df, previous_df, timestamp)
    parquet_file_path = write_as_parquet(transformed_df, file_name)

    return parquet_file_path