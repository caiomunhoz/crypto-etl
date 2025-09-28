import pandas as pd
from io import BytesIO
import json

def normalize_raw_data(strf_raw_crypto_data, timestamp):
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

def serialize_as_parquet(normalized_crypto_data):
    buffer = BytesIO()

    pd.DataFrame(normalized_crypto_data).to_parquet(
        buffer,
        engine='pyarrow',
        index=False
    )

    buffer.seek(0)
    
    return buffer.read()