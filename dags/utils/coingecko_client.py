from airflow.sdk import Variable
import requests
import logging

def fetch_crypto_data(ti):
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

    endpoint = Variable.get('coingecko_endpoint')

    response = requests.get(
        endpoint,
        headers=headers,
        params=params
    )

    try:
        response.raise_for_status()
        ti.xcom_push(key='crypto_data', value=response.text)
    except requests.exceptions.HTTPError as e:
        logging.error(f'API request failed. HTTP error: {e}')
        raise