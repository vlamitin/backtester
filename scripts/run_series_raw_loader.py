import csv
import os
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from scripts.setup_db import connect_to_db
from stock_market_research_kit.binance_fetcher import fetch_15m_candles
from utils.date_utils import to_utc_datetime, to_date_str, to_timestamp, get_previous_candle_15m_close, start_of_day, \
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
        for row in data[1:]:
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


def to_db_format(symbol, period, inner_candle):
    return (symbol, inner_candle[5], period,
            inner_candle[0], inner_candle[1], inner_candle[2], inner_candle[3], inner_candle[4])


def update_stock_data(inner_candles, symbol, period, conn):
    rows = [to_db_format(symbol, period, candle) for candle in inner_candles]

    try:
        c = conn.cursor()
        c.executemany(
            """INSERT INTO raw_candles (symbol, date_ts, period, open, high, low, close, volume) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
                ON CONFLICT (symbol, date_ts, period) DO UPDATE
                SET open = excluded.open, 
                    high = excluded.high, 
                    low = excluded.low, 
                    close = excluded.close,
                    volume = excluded.volume""",
            rows
        )
        conn.commit()
        print(f"Success inserting {len(rows)} raw_candles {period} data for symbol {symbol}")
        return True
    except sqlite3.ProgrammingError as e:
        print(f"Error inserting raw_candles {period} data for symbol {symbol}: {e}")
        return False


def fill_year_from_csv(year, symbol):
    connection = connect_to_db(year)

    series_15m = load_from_csv(symbol, "15m", year)
    update_stock_data(series_15m, symbol, "15m", connection)

    print(f"done loading {year} year for {symbol}")

    connection.close()


def update_candle_from_binance(symbol):
    now_year = datetime.now(ZoneInfo("UTC")).year  # TODO не будет работать при смене года

    connection = connect_to_db(now_year)
    c = connection.cursor()

    c.execute("""SELECT open, high, low, close, volume, date_ts
            FROM raw_candles WHERE symbol = ? AND period = ? ORDER BY date_ts DESC LIMIT 1""", (symbol, "15m"))
    rows_15m = c.fetchall()

    if len(rows_15m) != 1:
        print(f"Symbol {symbol} {now_year} year 15m not found in DB")
        return

    last_15m_candle = [rows_15m[0][0], rows_15m[0][1], rows_15m[0][2], rows_15m[0][3], rows_15m[0][4], rows_15m[0][5]]

    iteration_days = get_all_days_between(
        to_utc_datetime(last_15m_candle[5]) + timedelta(minutes=15),
        get_previous_candle_15m_close()
    )

    if len(iteration_days) > 32:
        raise ValueError("More than a month passed! Download a CSV!")

    for i, date in enumerate(iteration_days):
        if i == len(iteration_days) - 1:
            start_time = to_timestamp(to_date_str(start_of_day(date)))
            end_time = to_timestamp(to_date_str(date))
        else:
            start_time = to_timestamp(to_date_str(date))
            end_time = int(end_of_day(date).timestamp() * 1000)

        candles_15m = [to_inner_candle(x) for x in fetch_15m_candles(symbol, start_time, end_time)]
        update_stock_data(candles_15m, symbol, "15m", connection)

    connection.close()


if __name__ == "__main__":
    try:
        # update_candle_from_binance("BTCUSDT")

        for smb in [
            "BTCUSDT",
            "AAVEUSDT",
            "CRVUSDT",
            "AVAXUSDT",
        ]:
            for series_year in [
                2021,
                2022,
                2023,
                2024,
                2025
            ]:
                fill_year_from_csv(series_year, smb)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
