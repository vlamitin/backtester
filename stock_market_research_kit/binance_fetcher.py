import json
import requests


# https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
def fetch_15m_candles(symbol, start_time, end_time):
    # return mock_fetch_15m_candles()
    response = requests.get(
        f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=15m&startTime={start_time}&endTime={end_time}")
    response.raise_for_status()
    server_candles = json.loads(response.text)

    return server_candles


if __name__ == "__main__":
    try:
        pass
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
