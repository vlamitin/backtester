import os
from datetime import datetime
import json
import requests
from requests.exceptions import HTTPError


from dotenv import load_dotenv

load_dotenv()
CD_API_KEY = os.environ["CD_API_KEY"]


def get_coin_supply():
    session = requests.Session()
    session.headers.update({
        'Accepts': 'application/json',
        'Content-Type': 'application/json',
        'authorization': f'Apikey {CD_API_KEY}',
    })

    params = {
        "asset": 'ETH',
        "asset_lookup_priority": 'SYMBOL',
        "to_ts": 1757161905,
        "fill": 'false',
        "limit": 100,
        "groups": 'SUPPLY',
        "response_format": 'JSON',
    }

    try:
        response = session.get("https://data-api.coindesk.com/onchain/v2/historical/supply/days", params=params)
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
        print(get_coin_supply())
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
