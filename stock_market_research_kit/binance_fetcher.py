import json
import os
from typing import Optional

import requests

from scripts.run_day_markuper import get_actual_cme_open_time, to_timestamp, now_ts, get_previous_candle_15m_close, \
    as_1_candle, markup_days, today_candle_from_15m_candles
from scripts.run_series_loader import to_inner_candle
from stock_market_research_kit.binance_fetcher_mock import mock_fetch_15m_candles
from stock_market_research_kit.day import Day


# https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
def fetch_15m_candles(symbol, start_time, end_time):
    response = requests.get(
        f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=15m&startTime={start_time}&endTime={end_time}")
    response.raise_for_status()
    server_candles = json.loads(response.text)

    return server_candles


def get_today(symbol) -> Optional[Day]:
    start_time = to_timestamp(get_actual_cme_open_time())
    if start_time == -1:
        return None
    end_time = get_previous_candle_15m_close()
    candles_15m = [to_inner_candle(x) for x in fetch_15m_candles(symbol, start_time, end_time)]
    # candles_15m = [to_inner_candle(x) for x in mock_fetch_15m_candles()]
    candles_1d = [today_candle_from_15m_candles(candles_15m)]

    days = markup_days(candles_1d, candles_15m)

    if len(days) != 1:
        print("something wrong!!!!")
        return None

    return days[0]


if __name__ == "__main__":
    try:
        get_today("BTCUSDT")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
