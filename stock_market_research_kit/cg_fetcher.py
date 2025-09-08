import os
from datetime import datetime
import json
import requests
from requests.exceptions import HTTPError


from dotenv import load_dotenv

load_dotenv()
CG_API_KEY = os.environ["CG_API_KEY"]


def get_coin_cap():
    session = requests.Session()
    # session.headers.update({
    #     'Accepts': 'application/json',
    #     'X-CMC_PRO_API_KEY': CMC_API_KEY,
    # })

    params = {
        'x_cg_demo_api_key': CG_API_KEY,
        'vs_currency': 'usd',
        'ids': 'bitcoin'
    }

    try:
        response = session.get("https://api.coingecko.com/api/v3/coins/markets", params=params)
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
        print(get_coin_cap())
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
