import json
import pandas as pd
from pendulum import DateTime
from io import BytesIO

def normalize_raw_data(strf_raw_crypto_data: str, timestamp: DateTime) -> list[dict]:
    raw_crypto_data = json.loads(strf_raw_crypto_data)

    normalized_data = []

    for coin_name, data in raw_crypto_data.items():
        normalized_data.append({
            'coin_name': coin_name,
            'price_usd': data.get('usd', 0),
            'volume_24h_usd': data.get('usd_24h_vol', 0),
            'market_cap_usd': data.get('usd_market_cap', 0),
            'timestamp': timestamp
        })

    return normalized_data

def serialize_as_parquet(normalized_crypto_data: list[dict]) -> bytes:
    buffer = BytesIO()

    pd.DataFrame(normalized_crypto_data).to_parquet(
        buffer,
        engine='pyarrow',
        index=False
    )

    buffer.seek(0)
    
    return buffer.read()

def merge_with_dimensions(normalized_data: list[dict], dim_coins: list[tuple], dim_times: list[tuple]) -> list[tuple]:
    df = pd.DataFrame(normalized_data).astype({
        'coin_name': 'string',
        'price_usd': 'float64',
        'volume_24h_usd': 'float64',
        'market_cap_usd': 'float64',
        'timestamp': 'string'
    })
    df_dim_coins = pd.DataFrame(dim_coins, columns=['id', 'coin_name']).astype({'id': 'int32', 'coin_name': 'string'})
    df_dim_times = pd.DataFrame(dim_times, columns=['id', 'timestamp']).astype({'id': 'int32', 'timestamp': 'string'})

    merged_df = (
        df.merge(df_dim_coins, on='coin_name')
        .merge(df_dim_times, on='timestamp', suffixes=['_coin', '_time'])
        .rename(columns={'id_coin': 'coin_id', 'id_time': 'time_id'})
    )
    merged_df = merged_df[['time_id', 'coin_id', 'price_usd', 'volume_24h_usd', 'market_cap_usd']]

    return [tuple(row) for row in merged_df.itertuples(index=False, name=None)]