from typing import List

from stock_market_research_kit.candle import as_1_candle
from stock_market_research_kit.day import Day, new_day
from stock_market_research_kit.db_layer import upsert_days_to_db, select_full_days_candles_15m
from utils.date_utils import is_same_day_or_past_or_future, to_utc_datetime, to_date_str, start_of_day, \
    is_prev_day_cme_open_time, \
    is_asian_time, is_london_time, is_early_session_time, is_premarket_time, is_ny_am_open_time, is_ny_am_time, \
    is_ny_lunch_time, is_ny_pm_time, is_ny_pm_close_time


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


def main(symbol, year):
    candles_15m = select_full_days_candles_15m(year, symbol)

    days = markup_days(candles_15m)
    print(f"Done marking up {symbol} {len(days)} days. Inserting results to db")
    result = upsert_days_to_db(year, symbol, days)
    if result:
        print(f"Done with {year} year {symbol}, last day is {days[-1].date_readable}")

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
