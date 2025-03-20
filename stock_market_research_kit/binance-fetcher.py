import json
import os

import requests

def test(symbol, start_time, end_time):
    # [
    #   [
    #     1499040000000,      // Open time
    #     "0.01634790",       // Open
    #     "0.80000000",       // High
    #     "0.01575800",       // Low
    #     "0.01577100",       // Close
    #     "148976.11427815",  // Volume
    #     1499644799999,      // Close time
    #     "2434.19055334",    // Quote asset volume
    #     308,                // Number of trades
    #     "1756.87402397",    // Taker buy base asset volume
    #     "28.46694368",      // Taker buy quote asset volume
    #     "17928899.62484339" // Ignore.
    #   ]
    # ]
    response = requests.get(
        f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=15m&startTime={start_time}&endTime={end_time}")
    response.raise_for_status()
    server_candles = json.loads(response.text)

    return server_candles


if __name__ == "__main__":
    try:
        test("BTCUSDT", 1742421600000, 1742494500000)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)

# curl https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=15m&startTime=1742421600000&endTime=1742494500000
