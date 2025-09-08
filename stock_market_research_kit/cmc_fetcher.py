import os
from datetime import datetime
import json
import requests
from requests.exceptions import HTTPError

from dotenv import load_dotenv

load_dotenv()
CMC_API_KEY = os.environ["CMC_API_KEY"]


def get_cmc100_latest():
    session = requests.Session()
    session.headers.update({
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': CMC_API_KEY,
    })

    try:
        response = session.get("https://pro-api.coinmarketcap.com/v3/index/cmc100-latest")
        response.raise_for_status()
    except HTTPError as http_err:
        return [], f"HTTP error occurred: {http_err}"
    except Exception as err:
        return [], f"Other error occurred: {err}"
    else:
        resp = json.loads(response.text)
        print(resp)


def get_cmc100_historical():
    session = requests.Session()
    session.headers.update({
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': CMC_API_KEY,
    })

    params = {
        'time_end': '2023-01-01T00:00:00.000Z',
        'count': 10,
        # 'interval': '15m',
        'interval': 'daily',
    }

    try:
        response = session.get("https://pro-api.coinmarketcap.com/v3/index/cmc100-historical", params=params)
        response.raise_for_status()
    except HTTPError as http_err:
        return [], f"HTTP error occurred: {http_err}"
    except Exception as err:
        return [], f"Other error occurred: {err}"
    else:
        resp = json.loads(response.text)
        print(resp)


def get_btc_eth_quotes_historical():
    session = requests.Session()
    session.headers.update({
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': CMC_API_KEY,
    })

    params = {
        'symbol': ','.join(['BTC', 'ETH']),
        'time_end': '2025-09-05T00:00:00.000Z',
        'count': 10,
        # 'interval': '15m',
        'interval': 'daily',
    }

    try:
        response = session.get("https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/historical", params=params)
        response.raise_for_status()
    except HTTPError as http_err:
        return [], f"HTTP error occurred: {http_err}"
    except Exception as err:
        return [], f"Other error occurred: {err}"
    else:
        resp = json.loads(response.text)
        print(resp)


def get_cmc100_historical_chart():
    session = requests.Session()
    # session.headers.update({
    #     'Accepts': 'application/json',
    #     'X-CMC_PRO_API_KEY': CMC_API_KEY,
    # })

    params = {
        'range': 'all',
    }

    try:
        response = session.get("https://api.coinmarketcap.com/data-api/v3/top100/historical/chart", params=params)
        response.raise_for_status()
    except HTTPError as http_err:
        return [], f"HTTP error occurred: {http_err}"
    except Exception as err:
        return [], f"Other error occurred: {err}"
    else:
        resp = json.loads(response.text)
        print(resp)


if __name__ == '__main__':
    try:
        # print(get_cmc100_latest())
        # print(get_cmc100_historical())
        # print(get_cmc100_historical_chart())
        print(get_btc_eth_quotes_historical())
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
