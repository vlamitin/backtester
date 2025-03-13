import csv
import os
import json
import sqlite3
from datetime import datetime

DB_PATH = "stock_market_research.db"


def connect_to_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def load_timeseries_data(symbol, period):
    # Folder path
    folder_path = f"./data/csv/{symbol}/{period}"

    # Get all CSV files and sort them alphabetically
    csv_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".csv")])

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
            date = datetime.fromtimestamp(int(row[0][:10])).strftime("%Y-%m-%d %H:%M")
            open_price = float(row[1])
            high = float(row[2])
            low = float(row[3])
            close = float(row[4])
            volume = float(row[5])

            timeseries_data.append([open_price, high, low, close, volume, date])

    update_stock_data(timeseries_data, symbol, period)


def update_stock_data(timeseries_data, symbol, period):
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(
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
        cur.execute(
            "UPDATE stock_data SET daily = ? WHERE symbol = ?",
            (json.dumps(timeseries_data), symbol),
        )
    elif period == "1h":
        cur.execute(
            "UPDATE stock_data SET hourly = ? WHERE symbol = ?",
            (json.dumps(timeseries_data), symbol),
        )
    elif period == "15m":
        cur.execute(
            "UPDATE stock_data SET fifteen_minutely = ? WHERE symbol = ?",
            (json.dumps(timeseries_data), symbol),
        )
    conn.commit()


def main():
    load_timeseries_data("BTCUSDT", "1d")
    load_timeseries_data("BTCUSDT", "1h")
    load_timeseries_data("BTCUSDT", "15m")


if __name__ == "__main__":
    main()
