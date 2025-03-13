import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple
from zoneinfo import ZoneInfo

from stock_market_research_kit.day import Day, new_day

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import sqlite3
import json
from alive_progress import alive_bar

DATABASE_PATH = "stock_market_research.db"

# Connect to the SQLite database
conn = sqlite3.connect(DATABASE_PATH)
c = conn.cursor()


def markup_days(daily_data_str, hourly_data_str, fifteen_minutely_data_str):
    if daily_data_str is None or hourly_data_str is None or fifteen_minutely_data_str is None:
        print("no data")
        return

    candles_15m = json.loads(fifteen_minutely_data_str)
    candles_1h = json.loads(hourly_data_str)
    candles_1d = json.loads(daily_data_str)
    days: List[Day] = []

    last_i_1h = 0
    last_i_15m = 0

    with alive_bar(len(candles_1d), title=f"Marking up {len(candles_1d)} day candles ...") as bar:
        for i in range(len(candles_1d)):
            day = new_day()
            day.day_of_week = datetime.strptime(candles_1d[i][5], "%Y-%m-%d %H:%M").isoweekday()
            day.date_readable = candles_1d[i][5]

            day.candle_1d = candles_1d[i]

            for i_1h in range(last_i_1h, len(candles_1h)):
                candle = candles_1h[i_1h]
                comparison_result = is_same_day_or_past_or_future(candle[5], day.date_readable)
                if comparison_result == 1:
                    last_i_1h = i_1h
                    break
                elif comparison_result == -1:
                    continue

                day.candles_1h.append(candle)

            for i_15m in range(0 if last_i_15m - 8 < 0 else last_i_15m - 8, len(candles_15m)):
                candle = candles_15m[i_15m]
                comparison_result = is_same_day_or_past_or_future(candle[5], day.date_readable)
                if comparison_result == 1:
                    last_i_15m = i_15m
                    break
                elif comparison_result == -1:
                    if is_prev_day_cme_open_time(candle[5], day.date_readable):
                        day.cme_open_candles_15m.append(candle)
                    if is_asian_time(candle[5], day.date_readable):
                        day.asian_candles_15m.append(candle)
                    continue

                if is_london_time(candle[5], day.date_readable):
                    day.london_candles_15m.append(candle)
                if is_early_session_time(candle[5], day.date_readable):
                    day.early_session_candles_15m.append(candle)
                if is_premarket_time(candle[5], day.date_readable):
                    day.premarket_candles_15m.append(candle)
                if is_ny_am_open_time(candle[5], day.date_readable):
                    day.ny_am_open_candles_15m.append(candle)
                if is_ny_am_time(candle[5], day.date_readable):
                    day.ny_am_candles_15m.append(candle)
                if is_ny_lunch_time(candle[5], day.date_readable):
                    day.ny_lunch_candles_15m.append(candle)
                if is_ny_pm_time(candle[5], day.date_readable):
                    day.ny_pm_candles_15m.append(candle)
                if is_ny_pm_close_time(candle[5], day.date_readable):
                    day.ny_pm_close_candles_15m.append(candle)

                day.candles_15m.append(candle)

            day.cme_as_candle = as_1_candle(day.cme_open_candles_15m)
            day.asia_as_candle = as_1_candle(day.asian_candles_15m)
            day.london_as_candle = as_1_candle(day.london_candles_15m)
            day.early_session_as_candle = as_1_candle(day.early_session_candles_15m)
            day.premarket_as_candle = as_1_candle(day.premarket_candles_15m)
            day.ny_am_open_as_candle = as_1_candle(day.ny_am_open_candles_15m)
            day.ny_am_as_candle = as_1_candle(day.ny_am_candles_15m)
            day.ny_lunch_as_candle = as_1_candle(day.ny_lunch_candles_15m)
            day.ny_pm_as_candle = as_1_candle(day.ny_pm_candles_15m)
            day.ny_pm_close_as_candle = as_1_candle(day.ny_pm_close_candles_15m)

            days.append(day)
            bar()

    return days


# returns -1 if checked time is past, 0 if same day and +1 if future
def is_same_day_or_past_or_future(checked_time_string, current_day_string):
    current_day = datetime.strptime(current_day_string, "%Y-%m-%d %H:%M")
    next_day = current_day + timedelta(days=1)
    checked_time = datetime.strptime(checked_time_string, "%Y-%m-%d %H:%M")

    range_from = datetime(current_day.year, current_day.month, current_day.day, 0, 0)
    range_to = datetime(next_day.year, next_day.month, next_day.day, 0, 0)

    if range_from > checked_time:
        return -1
    elif checked_time >= range_to:
        return 1
    else:
        return 0


def is_prev_day_cme_open_time(checked_time_string, current_day_string):
    return is_some_prev_day_session(checked_time_string, current_day_string, "18:00", "19:00")


def is_asian_time(checked_time_string, current_day_string):
    return is_some_prev_day_session(checked_time_string, current_day_string, "19:00", "22:00")


def is_london_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "02:00", "05:00")


def is_early_session_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "07:00", "08:00")


def is_premarket_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "08:00", "09:30")


def is_ny_am_open_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "09:30", "10:00")


def is_ny_am_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "10:00", "12:00")


def is_ny_lunch_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "12:00", "13:00")


def is_ny_pm_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "13:00", "15:00")


def is_ny_pm_close_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "15:00", "16:00")


def is_some_prev_day_session(checked_time_string, current_day_string, from_str, to_str):
    current_day = datetime.strptime(current_day_string, "%Y-%m-%d %H:%M")
    prev_day = current_day - timedelta(days=1)

    if prev_day.isoweekday() == 5 or prev_day.isoweekday() == 6:
        return False

    checked_time = datetime.strptime(checked_time_string, "%Y-%m-%d %H:%M").astimezone(ZoneInfo("UTC"))

    from_h, from_m = [int(x) for x in from_str.split(":")]
    to_h, to_m = [int(x) for x in to_str.split(":")]

    range_from = datetime(
        prev_day.year, prev_day.month, prev_day.day, from_h, from_m, tzinfo=ZoneInfo("America/New_York")
    )
    range_to = datetime(
        prev_day.year, prev_day.month, prev_day.day, to_h, to_m, tzinfo=ZoneInfo("America/New_York")
    )

    return range_from <= checked_time < range_to


def is_some_same_day_session(checked_time_string, current_day_string, from_str, to_str):
    current_day = datetime.strptime(current_day_string, "%Y-%m-%d %H:%M")
    if current_day.isoweekday() == 6 or current_day.isoweekday() == 7:
        return False

    checked_time = datetime.strptime(checked_time_string, "%Y-%m-%d %H:%M").astimezone(ZoneInfo("UTC"))

    from_h, from_m = [int(x) for x in from_str.split(":")]
    to_h, to_m = [int(x) for x in to_str.split(":")]

    range_from = datetime(
        current_day.year, current_day.month, current_day.day, from_h, from_m, tzinfo=ZoneInfo("America/New_York")
    )
    range_to = datetime(
        current_day.year, current_day.month, current_day.day, to_h, to_m, tzinfo=ZoneInfo("America/New_York")
    )

    return range_from <= checked_time < range_to


def as_1_candle(candles: List[Tuple[float]]) -> Tuple[float, float, float, float, float, str]:
    if len(candles) == 0:
        return -1, -1, -1, -1, 0, ""

    open_, high, low, close, volume, date = candles[0][0], -1, -1, candles[-1][3], 0, candles[0][5]

    for _, h, l, _, v, _ in candles:
        volume += v
        if h >= high or high == -1:
            high = h
        if l <= low or low == -1:
            low = l

    return open_, high, low, close, volume, date


def insert_to_db(symbol: str, days: List[Day]):
    rows = [day.to_db_format(symbol) for day in days]

    try:
        c.executemany("INSERT INTO days (symbol, date_ts, data) VALUES (?, ?, ?)", rows)
        conn.commit()
        return True
    except sqlite3.ProgrammingError as e:
        print(f"Error inserting days data for symbol {symbol}: {e}")
        return False


def main(symbol):
    c.execute("SELECT symbol, daily, hourly, fifteen_minutely FROM stock_data WHERE symbol = ?", (symbol,))
    rows = c.fetchall()

    if len(rows) == 0:
        print(f"Symbol {symbol} not found in DB")
        return

    symbol, daily_data_str, hourly_data_str, fifteen_minutely_data_str = rows[0]
    print(f"Marking up {symbol}")
    days = markup_days(daily_data_str, hourly_data_str, fifteen_minutely_data_str)
    print(f"Done marking up {len(days)} days. Inserting results to db")
    result = insert_to_db(symbol, days)
    if result:
        print(f"Done with {symbol}")

    conn.close()


if __name__ == "__main__":
    try:
        main("BTCUSDT")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
