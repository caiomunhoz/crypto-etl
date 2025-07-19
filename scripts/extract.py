import requests
import json
import logging
from datetime import datetime
from pathlib import Path
from .utils import format_timestamp_to_file_name

def get_crypto_prices():
    headers = {
        'accept': 'application/json',
    }
    params = {
        'ids': 'bitcoin,ethereum,solana',
        'vs_currencies': 'usd',
        'include_market_cap': 'true',
        'include_24hr_vol': 'true',
        'precision': 2
    }
    URL = 'https://api.coingecko.com/api/v3/simple/price'

    response = requests.get(
        URL,
        headers=headers,
        params=params
    )

    try:
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as e:
        logging.error(f'API request failed. HTTP error: {e}')
        raise

def save_as_json_file(response):
    data = response.json()
    formatted_timestamp = format_timestamp_to_file_name(datetime.now())

    file_path = Path(f'data/raw/{formatted_timestamp}.json')
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, 'w') as json_file:
        json.dump(data, json_file)

    return str(file_path)

def run():
    crypto_prices = get_crypto_prices()
    json_file_path = save_as_json_file(crypto_prices)

    return json_file_path