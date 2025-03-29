import csv
import os
import sqlite3
from datetime import datetime

from scripts.setup_db import connect_to_db


def load_timeseries_data(symbol, period, year):
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
    date = datetime.fromtimestamp(int(str(binance_candle[0])[:10])).strftime("%Y-%m-%d %H:%M")
    open_price = float(binance_candle[1])
    high = float(binance_candle[2])
    low = float(binance_candle[3])
    close = float(binance_candle[4])
    volume = float(binance_candle[5])

    return [open_price, high, low, close, volume, date]


def to_db_format(symbol, period, inner_candle):
    return (symbol, datetime.strptime(inner_candle[5], "%Y-%m-%d %H:%M").timestamp(), period,
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


def load_series(year, symbol):
    connection = connect_to_db(year)

    series_1d = load_timeseries_data(symbol, "1d", year)
    update_stock_data(series_1d, symbol, "1d", connection)

    series_15m = load_timeseries_data(symbol, "15m", year)
    update_stock_data(series_15m, symbol, "15m", connection)

    print(f"done loading {year} year for {symbol}")

    connection.close()


if __name__ == "__main__":
    try:
        for smb in [
            "BTCUSDT",
            "AAVEUSDT"
        ]:
            for series_year in [
                2021,
                2022,
                2023,
                2024,
                2025
            ]:
                load_series(series_year, smb)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
