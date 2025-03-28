import csv
import os
import json
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


def update_stock_data(timeseries_data, symbol, period, conn):
    c = conn.cursor()
    c.execute(
        "INSERT OR IGNORE INTO stock_data (symbol, exchange, sector, industry, delisted) VALUES (?, ?, ?, ?, ?)",
        (
            symbol,
            "BINANCE",
            "CRYPTO",
            "CRYPTO",
            False,
        ),
    )
    if period == "1d":
        c.execute(
            "UPDATE stock_data SET daily = ? WHERE symbol = ?",
            (json.dumps(timeseries_data), symbol),
        )
    elif period == "15m":
        c.execute(
            "UPDATE stock_data SET fifteen_minutely = ? WHERE symbol = ?",
            (json.dumps(timeseries_data), symbol),
        )
    conn.commit()


def load_series(year, symbol):
    connection = connect_to_db(year)

    series_1d = load_timeseries_data(symbol, "1d", year)
    update_stock_data(series_1d, symbol, "1d", connection)

    series_15m = load_timeseries_data(symbol, "15m", year)
    update_stock_data(series_15m, symbol, "15m", connection)

    connection.close()


if __name__ == "__main__":
    try:
        load_series(2024, "AAVEUSDT")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
