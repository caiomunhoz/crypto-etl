from airflow.sdk import Variable
import requests
import logging

def fetch_raw_crypto_data(coins) -> str | None:
    headers = {
        'accept': 'application/json',
    }

    params = {
        'vs_currencies': 'usd',
        'ids': ','.join(coins),
        'include_market_cap': 'true',
        'include_24hr_vol': 'true',
        'precision': 2
    }

    ENDPOINT = Variable.get('coingecko_endpoint')

    response = requests.get(
        ENDPOINT,
        headers=headers,
        params=params
    )

    try:
        response.raise_for_status()
        return response.text
    except requests.exceptions.HTTPError as e:
        logging.error(f'API request failed. HTTP error: {e}')
        raise