import requests
import json
import logging
from datetime import datetime

def get_crypto_prices():
    headers = {
        'accept': 'application/json',
    }
    params = {
        'ids': 'bitcoin,ethereum,solana',
        'vs_currencies': 'usd',
        'precision': 2,
        'include_24h_change': True
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
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    with open(f'data/raw/{timestamp}.json', 'w') as json_file:
        json.dump(data, json_file)

crypto_prices = get_crypto_prices()
save_as_json_file(crypto_prices)