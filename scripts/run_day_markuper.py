import sqlite3
from datetime import datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo

from scripts.setup_db import connect_to_db
from stock_market_research_kit.candle import as_1_candle
from stock_market_research_kit.day import Day, new_day
from utils.date_utils import is_same_day_or_past_or_future, is_some_same_day_session, is_some_prev_day_session, \
    to_utc_datetime, now_utc_datetime, now_ny_datetime, to_date_str, start_of_day


def group_15m_by_days(candles_15m):
    if len(candles_15m) == 0:
        return []
    grouped_candles_15m = [[candles_15m[0]]]
    last_date = to_utc_datetime(candles_15m[0][5]).date()
    for candle in candles_15m[1:]:
        if to_utc_datetime(candle[5]).date() == last_date:
            grouped_candles_15m[-1].append(candle)
        else:
            grouped_candles_15m.append([candle])
        last_date = to_utc_datetime(candle[5]).date()

    return grouped_candles_15m


def markup_days(candles_15m):
    days: List[Day] = []

    if len(candles_15m) == 0:
        return []

    grouped_candles_15m = group_15m_by_days(candles_15m)

    for i, day_group in enumerate(grouped_candles_15m):
        if len(grouped_candles_15m) > 50 and (i % 50 == 0 or i == len(grouped_candles_15m) - 1):
            print(f"marking up {i + 1}/{len(grouped_candles_15m)} days")
        day = new_day()

        utc_start_of_day = start_of_day(to_utc_datetime(day_group[0][5]))
        day.day_of_week = utc_start_of_day.isoweekday()
        day.date_readable = to_date_str(utc_start_of_day)

        day.candle_1d = as_1_candle(day_group)
        candles_to_iterate = day_group if i == 0 else [*grouped_candles_15m[i - 1][-8:], *day_group]

        for candle in candles_to_iterate:
            comparison_result = is_same_day_or_past_or_future(candle[5], day.date_readable)
            if comparison_result == 1:
                break
            elif comparison_result == -1:
                if is_prev_day_cme_open_time(candle[5], day.date_readable):
                    day.cme_open_candles_15m.append(candle)
                elif is_asian_time(candle[5], day.date_readable):
                    day.asian_candles_15m.append(candle)
                continue

            if is_asian_time(candle[5], day.date_readable):  # check 2nd time because of DST
                day.asian_candles_15m.append(candle)
            elif is_london_time(candle[5], day.date_readable):
                day.london_candles_15m.append(candle)
            elif is_early_session_time(candle[5], day.date_readable):
                day.early_session_candles_15m.append(candle)
            elif is_premarket_time(candle[5], day.date_readable):
                day.premarket_candles_15m.append(candle)
            elif is_ny_am_open_time(candle[5], day.date_readable):
                day.ny_am_open_candles_15m.append(candle)
            elif is_ny_am_time(candle[5], day.date_readable):
                day.ny_am_candles_15m.append(candle)
            elif is_ny_lunch_time(candle[5], day.date_readable):
                day.ny_lunch_candles_15m.append(candle)
            elif is_ny_pm_time(candle[5], day.date_readable):
                day.ny_pm_candles_15m.append(candle)
            elif is_ny_pm_close_time(candle[5], day.date_readable):
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

    return days


def get_actual_cme_open_time():
    now_time = now_ny_datetime()

    if now_time.hour >= 18:
        if now_time.isoweekday() in [5, 6]:
            return ""
        res_time = datetime(
            now_time.year, now_time.month, now_time.day, 18, 0, tzinfo=ZoneInfo("America/New_York")
        ).astimezone(ZoneInfo("UTC"))

        return to_date_str(res_time)

    prev_day = now_time - timedelta(days=1)
    if prev_day.isoweekday() in [5, 6]:
        return ""
    res_time = datetime(
        prev_day.year, prev_day.month, prev_day.day, 18, 0, tzinfo=ZoneInfo("America/New_York")
    ).astimezone(ZoneInfo("UTC"))

    return to_date_str(res_time)


def today_candle_from_15m_candles(candles_15m):
    today_candles = [x for x in candles_15m
                     if to_utc_datetime(x[5]).date() == now_utc_datetime().date()]
    return as_1_candle(today_candles)


def cme_open_from_to(day_string) -> (str, str):
    return (
        to_date_str(to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")).replace(
            hour=18, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str(to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")).replace(
            hour=18, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_prev_day_cme_open_time(checked_time_string, current_day_string):
    return is_some_prev_day_session(checked_time_string, current_day_string, "18:00", "19:00")


def asia_from_to(day_string) -> (str, str):
    return (
        to_date_str(to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")).replace(
            hour=19, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str(to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")).replace(
            hour=21, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_asian_time(checked_time_string, current_day_string):
    return is_some_prev_day_session(checked_time_string, current_day_string, "19:00", "22:00")


def london_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=2, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=4, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_london_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "02:00", "05:00")


def early_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=7, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=7, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_early_session_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "07:00", "08:00")


def pre_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=8, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=9, minute=29, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_premarket_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "08:00", "09:30")


def open_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=9, minute=30, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=9, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_ny_am_open_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "09:30", "10:00")


def nyam_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=10, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=11, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_ny_am_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "10:00", "12:00")


def lunch_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=12, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=12, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_ny_lunch_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "12:00", "13:00")


def nypm_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=13, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=14, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_ny_pm_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "13:00", "15:00")


def close_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=15, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=15, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_ny_pm_close_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "15:00", "16:00")


def insert_to_db(symbol: str, days: List[Day], conn):
    rows = [day.to_db_format(symbol) for day in days]

    try:
        c = conn.cursor()
        c.executemany("""INSERT INTO
            days (symbol, date_ts, data) VALUES (?, ?, ?) ON CONFLICT (symbol, date_ts) DO UPDATE SET data = excluded.data""",
                      rows)
        conn.commit()
        print(f"Success inserting {len(rows)} days for symbol {symbol}")
        return True
    except sqlite3.ProgrammingError as e:
        print(f"Error inserting days {len(days)} symbol {symbol}: {e}")
        return False


def main(symbol, year):
    conn = connect_to_db(year)
    c = conn.cursor()

    c.execute("""WITH last_row_date_ts AS (
    SELECT date_ts last_row_date FROM raw_candles
    WHERE symbol = ? AND period = ?
    ORDER BY strftime('%s', date_ts) DESC
    LIMIT 1
),
     last_full_day AS (
         SELECT
             (strftime('%s', last_row_date) - 86400) / 86400 * 86400 + 24 * 3600 AS end_of_day
         FROM last_row_date_ts
     )
SELECT open, high, low, close, volume, date_ts FROM raw_candles
WHERE strftime('%s', date_ts) + 0 < (SELECT end_of_day FROM last_full_day)
  AND symbol = ? AND period = ?
  ORDER BY strftime('%s', date_ts)""", (symbol, "15m", symbol, "15m"))
    rows_15m = c.fetchall()

    if len(rows_15m) == 0:
        print(f"Symbol {symbol} 15m not found in DB")
        return

    candles_15m = [[x[0], x[1], x[2], x[3], x[4], x[5]] for x in rows_15m]

    days = markup_days(candles_15m)
    print(f"Done marking up {symbol} {len(days)} days. Inserting results to db")
    result = insert_to_db(symbol, days, conn)
    if result:
        print(f"Done with {year} year {symbol}")
    conn.close()

    return days


if __name__ == "__main__":
    try:
        # res1 = cme_open_from_to('2025-04-01 00:00')
        # res2 = asia_from_to('2025-04-01 00:00')
        # res3 = london_from_to('2025-04-01 00:00')
        # res4 = early_from_to('2025-04-01 00:00')
        # res5 = pre_from_to('2025-04-01 00:00')
        # res6 = open_from_to('2025-04-01 00:00')
        # res7 = nyam_from_to('2025-04-01 00:00')
        # res8 = lunch_from_to('2025-04-01 00:00')
        # res9 = nypm_from_to('2025-04-01 00:00')
        # res10 = close_from_to('2025-04-01 00:00')
        # cmet = get_actual_cme_open_time()
        for smb in [
            "BTCUSDT",
            "AAVEUSDT",
            "CRVUSDT",
            "AVAXUSDT",
        ]:
            for series_year in [
                # 2021,
                # 2022,
                # 2023,
                # 2024,
                2025
            ]:
                main(smb, series_year)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
