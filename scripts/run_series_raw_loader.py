import csv
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from stock_market_research_kit.binance_fetcher import fetch_15m_candles
from stock_market_research_kit.db_layer import update_stock_data, last_candle_15m
from utils.date_utils import to_utc_datetime, to_date_str, to_timestamp, \
    get_all_days_between, end_of_day


def load_from_csv(symbol, period, year):
    # Folder path
    folder_path = f"./data/csv/{symbol}/{period}"

    # Get all CSV files and sort them alphabetically
    csv_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".csv") if str(year) in f])

    # Read each CSV file and store content in a list
    all_data = []

    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        with open(file_path, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            data = list(reader)  # Read all rows into a list
            all_data.append((file, data))  # Store filename and content
            print(f"Read {file}")

    timeseries_data = []
    for filename, data in all_data:
        for row in data:
            if not row[0].isdigit():
                continue
            timeseries_data.append(to_inner_candle(row))

    return timeseries_data


def to_inner_candle(binance_candle):
    date = to_date_str(datetime.fromtimestamp(int(str(binance_candle[0])[:10]), tz=ZoneInfo("UTC")))
    open_price = float(binance_candle[1])
    high = float(binance_candle[2])
    low = float(binance_candle[3])
    close = float(binance_candle[4])
    volume = float(binance_candle[5])

    return [open_price, high, low, close, volume, date]


def fill_year_from_csv(year, symbol):
    series_15m = load_from_csv(symbol, "15m", year)
    update_stock_data(year, series_15m, symbol, "15m")

    print(f"done loading {year} year for {symbol}")


def update_candle_from_binance(symbol, last_date_utc: datetime):
    now_year = last_date_utc.year  # TODO не будет работать при смене года

    last_15m_candle = last_candle_15m(now_year, symbol)
    if not last_15m_candle:
        print(f"Symbol {symbol} {now_year} year 15m not found in DB")
        return

    iteration_days = get_all_days_between(
        to_utc_datetime(last_15m_candle[5]) + timedelta(minutes=15),
        last_date_utc
    )

    if len(iteration_days) > 32:
        raise ValueError("More than a month passed! Download a CSV!")

    for i, date in enumerate(iteration_days):
        update_day_from_binance(symbol, date, last_date_utc)


def update_day_from_binance(symbol: str, from_date: datetime, to_date: datetime):
    start_time = to_timestamp(from_date)
    end_time = min(to_timestamp(end_of_day(from_date)), to_timestamp(to_date) - 1)

    if start_time >= end_time:
        return

    candles_15m = [to_inner_candle(x) for x in fetch_15m_candles(symbol, start_time, end_time)]
    update_stock_data(from_date.year, candles_15m, symbol, "15m")


if __name__ == "__main__":
    try:
        # update_candle_from_binance("BTCUSDT")
        for smb in [
            # "1INCHUSDT",
            # "AAVEUSDT",
            # "AVAXUSDT",
            # "BTCUSDT",
            # "COMPUSDT",
            # "CRVUSDT",
            "ETHUSDT",
            # "LINKUSDT",
            # "LTCUSDT",
            "SOLUSDT",
            # "SUSHIUSDT",
            # "UNIUSDT",
            # "XLMUSDT",
            # "XMRUSDT",
        ]:
            # update_candle_from_binance(smb, datetime.now(ZoneInfo("UTC")))
            update_day_from_binance(
                "SOLUSDT",
                to_utc_datetime('2022-02-28 00:00'),
                to_utc_datetime('2022-02-28 23:59')
            )
            # for series_year in [
            #     # 2021,
            #     2022,
            #     # 2023,
            #     # 2024,
            #     # 2025
            # ]:
            #     fill_year_from_csv(series_year, smb)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
