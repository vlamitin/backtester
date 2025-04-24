from typing import List

from stock_market_research_kit.candle import as_1_candle
from stock_market_research_kit.day import Day, new_day
from stock_market_research_kit.db_layer import upsert_days_to_db, select_full_days_candles_15m
from stock_market_research_kit.session import new_spa, SessionName, SessionPriceAction
from utils.date_utils import is_same_day_or_past_or_future, to_utc_datetime, to_date_str, start_of_day, \
    is_prev_day_cme_open_time, \
    is_asian_time, is_london_time, is_early_session_time, is_premarket_time, is_ny_am_open_time, is_ny_am_time, \
    is_ny_lunch_time, is_ny_pm_time, is_ny_pm_close_time, is_some_same_day_session, get_second_monday, is_previous_week, \
    is_some_same_day


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

    true_do = (-1, "")
    wo = (-1, "")
    true_wo = (-1, "")
    mo = (-1, "")
    true_mo = (-1, "")
    yo = (-1, "")
    true_yo = (-1, "")
    prev_ny_close = (-1, "")
    prev_cme_close = (-1, "")
    week_high = (-1, "")
    week_range_75q = (-1, "")
    week_range_50q = (-1, "")
    week_range_25q = (-1, "")
    week_low = (-1, "")
    prev_week_high = (-1, "")
    prev_week_range_75q = (-1, "")
    prev_week_range_50q = (-1, "")
    prev_week_range_25q = (-1, "")
    prev_week_low = (-1, "")

    def set_week_ranges():
        nonlocal week_range_75q, week_range_50q, week_range_25q
        if week_high[1] == "" or week_low[1] == "":
            return
        week_range_75q = (week_low[0] + (week_high[0] - week_low[0]) * 0.75, candle[5])
        week_range_50q = (week_low[0] + (week_high[0] - week_low[0]) * 0.5, candle[5])
        week_range_25q = (week_low[0] + (week_high[0] - week_low[0]) * 0.25, candle[5])

    for i, day_group in enumerate(grouped_candles_15m):
        if len(grouped_candles_15m) > 100 and (i % 100 == 0 or i == len(grouped_candles_15m) - 1):
            print(f"marking up {i + 1}/{len(grouped_candles_15m)} days")
        day = new_day()

        utc_start_of_day = start_of_day(to_utc_datetime(day_group[0][5]))
        day.day_of_week = utc_start_of_day.isoweekday()
        day.date_readable = to_date_str(utc_start_of_day)

        if utc_start_of_day.isoweekday() == 1:
            wo = (day_group[0][0], day_group[0][5])

        if utc_start_of_day.day == 1:
            mo = (day_group[0][0], day_group[0][5])

        if get_second_monday(utc_start_of_day) == utc_start_of_day:
            true_mo = (day_group[0][0], day_group[0][5])

        if utc_start_of_day.day == 1 and utc_start_of_day.month == 1:
            yo = (day_group[0][0], day_group[0][5])

        if utc_start_of_day.day == 1 and utc_start_of_day.month == 4:
            true_yo = (day_group[0][0], day_group[0][5])

        # TODO могут ли быть случаи, когда только high или только low заполнены?
        if week_high[1] != "" and is_previous_week(to_utc_datetime(week_high[1]), utc_start_of_day):
            prev_week_high = week_high
            prev_week_range_75q = week_range_75q
            prev_week_range_50q = week_range_50q
            prev_week_range_25q = week_range_25q
            prev_week_low = week_low
            # TODO получается, в понедельники эти значения всегда будут пустые - это by design
            week_high = (-1, "")
            week_range_75q = (-1, "")
            week_range_50q = (-1, "")
            week_range_25q = (-1, "")
            week_low = (-1, "")

        day.do = (day_group[0][0], day_group[0][5])
        day.true_do = true_do
        day.wo = wo
        day.true_wo = true_wo
        day.mo = mo
        day.true_mo = true_mo
        day.yo = yo
        day.true_yo = true_yo
        day.prev_ny_close = prev_ny_close
        day.prev_cme_close = prev_cme_close
        day.week_high = week_high
        day.week_range_75q = week_range_75q
        day.week_range_50q = week_range_50q
        day.week_range_25q = week_range_25q
        day.week_low = week_low
        day.prev_week_high = prev_week_high
        day.prev_week_range_75q = prev_week_range_75q
        day.prev_week_range_50q = prev_week_range_50q
        day.prev_week_range_25q = prev_week_range_25q
        day.prev_week_low = prev_week_low

        day.candle_1d = as_1_candle(day_group)

        candles_to_iterate = day_group if i == 0 else [*grouped_candles_15m[i - 1][-8:], *day_group]
        for candle in candles_to_iterate:
            candle_datetime = to_utc_datetime(candle[5])
            comparison_result = is_same_day_or_past_or_future(candle[5], day.date_readable)
            if comparison_result == 1:
                break
            elif comparison_result == -1:
                if is_prev_day_cme_open_time(candle[5], day.date_readable):
                    if not day.cme:
                        day.cme = new_spa(SessionName.CME)

                        if len(days) > 0:
                            day.cme.do = days[-1].do
                        day.cme.true_do = true_do
                        if utc_start_of_day.isoweekday() == 1:
                            if len(days) > 0:
                                day.cme.wo = days[-1].wo
                                day.cme.true_wo = days[-1].true_wo
                        else:
                            day.cme.wo = wo
                            day.cme.true_wo = true_wo
                        if utc_start_of_day.day == 1:
                            if len(days) > 0:
                                day.cme.mo = days[-1].mo
                        else:
                            day.cme.mo = mo
                        if get_second_monday(utc_start_of_day) == utc_start_of_day:
                            if len(days) > 0:
                                day.cme.true_mo = days[-1].true_mo
                        else:
                            day.cme.true_mo = true_mo

                        if utc_start_of_day.day == 1 and utc_start_of_day.month == 1:
                            if len(days) > 0:
                                day.cme.yo = days[-1].yo
                        else:
                            day.cme.yo = yo

                        if utc_start_of_day.day == 1 and utc_start_of_day.month == 4:
                            if len(days) > 0:
                                day.cme.true_yo = days[-1].true_yo
                        else:
                            day.cme.true_yo = true_yo

                        day.cme.prev_ny_close = prev_ny_close
                        day.cme.prev_cme_close = prev_cme_close
                        if utc_start_of_day.isoweekday() == 1:
                            day.cme.week_high = prev_week_high
                            day.cme.week_range_75q = prev_week_range_75q
                            day.cme.week_range_50q = prev_week_range_50q
                            day.cme.week_range_25q = prev_week_range_25q
                            day.cme.week_low = prev_week_low
                        else:
                            day.cme.week_high = week_high
                            day.cme.week_range_75q = week_range_75q
                            day.cme.week_range_50q = week_range_50q
                            day.cme.week_range_25q = week_range_25q
                            day.cme.week_low = week_low
                    day.cme.candles_15m.append(candle)
                elif is_asian_time(candle[5], day.date_readable):  # когда азия открывается в 23:00
                    if not day.asia:
                        day.asia = new_spa(SessionName.ASIA)
                        if len(days) > 0:
                            day.asia.do = days[-1].do
                        day.asia.true_do = true_do
                        if utc_start_of_day.isoweekday() == 1:
                            if len(days) > 0:
                                day.asia.wo = days[-1].wo
                                day.asia.true_wo = days[-1].true_wo
                        else:
                            day.asia.wo = wo
                            day.asia.true_wo = true_wo
                        if utc_start_of_day.day == 1:
                            if len(days) > 0:
                                day.asia.mo = days[-1].mo
                        else:
                            day.asia.mo = mo
                        if get_second_monday(utc_start_of_day) == utc_start_of_day:
                            if len(days) > 0:
                                day.asia.true_mo = days[-1].true_mo
                        else:
                            day.asia.true_mo = true_mo

                        if utc_start_of_day.day == 1 and utc_start_of_day.month == 1:
                            if len(days) > 0:
                                day.asia.yo = days[-1].yo
                        else:
                            day.asia.yo = yo

                        if utc_start_of_day.day == 1 and utc_start_of_day.month == 4:
                            if len(days) > 0:
                                day.asia.true_yo = days[-1].true_yo
                        else:
                            day.asia.true_yo = true_yo

                        day.asia.prev_ny_close = prev_ny_close
                        day.asia.prev_cme_close = prev_cme_close
                        if utc_start_of_day.isoweekday() == 1:
                            day.asia.week_high = prev_week_high
                            day.asia.week_range_75q = prev_week_range_75q
                            day.asia.week_range_50q = prev_week_range_50q
                            day.asia.week_range_25q = prev_week_range_25q
                            day.asia.week_low = prev_week_low
                        else:
                            day.asia.week_high = week_high
                            day.asia.week_range_75q = week_range_75q
                            day.asia.week_range_50q = week_range_50q
                            day.asia.week_range_25q = week_range_25q
                            day.asia.week_low = week_low
                    day.asia.candles_15m.append(candle)
                continue

            def set_default_levels(spa: SessionPriceAction):
                spa.do = day.do
                spa.true_do = true_do
                spa.wo = wo
                if utc_start_of_day.isoweekday() == 1:
                    if len(days) > 0:
                        spa.true_wo = days[-1].true_wo
                else:
                    spa.true_wo = true_wo
                spa.mo = mo
                spa.true_mo = true_mo
                spa.yo = yo
                spa.true_yo = true_yo
                spa.prev_ny_close = prev_ny_close
                spa.prev_cme_close = prev_cme_close
                spa.week_high = week_high
                spa.week_range_75q = week_range_75q
                spa.week_range_50q = week_range_50q
                spa.week_range_25q = week_range_25q
                spa.week_low = week_low

            if is_asian_time(candle[5], day.date_readable):  # когда азия открывается в 00:00
                if not day.asia:
                    day.asia = new_spa(SessionName.ASIA)
                    day.asia.do = day.do
                    day.asia.true_do = true_do
                    day.asia.wo = day.wo
                    if utc_start_of_day.isoweekday() == 1:
                        if len(days) > 0:
                            day.asia.true_wo = days[-1].true_wo
                    else:
                        day.asia.true_wo = true_wo
                    day.asia.mo = mo
                    day.asia.true_mo = true_mo
                    day.asia.yo = yo
                    day.asia.true_yo = true_yo
                    day.asia.prev_ny_close = prev_ny_close
                    day.asia.prev_cme_close = prev_cme_close
                    if utc_start_of_day.isoweekday() == 1:
                        if len(days) > 0:
                            day.asia.week_high = prev_week_high
                            day.asia.week_range_75q = prev_week_range_75q
                            day.asia.week_range_50q = prev_week_range_50q
                            day.asia.week_range_25q = prev_week_range_25q
                            day.asia.week_low = prev_week_low
                    else:
                        day.asia.week_high = week_high
                        day.asia.week_range_75q = week_range_75q
                        day.asia.week_range_50q = week_range_50q
                        day.asia.week_range_25q = week_range_25q
                        day.asia.week_low = week_low

                day.asia.candles_15m.append(candle)
            elif is_london_time(candle[5], day.date_readable):
                if not day.london:
                    day.london = new_spa(SessionName.LONDON)
                    set_default_levels(day.london)
                day.london.candles_15m.append(candle)
            elif is_early_session_time(candle[5], day.date_readable):
                if not day.early_session:
                    day.early_session = new_spa(SessionName.EARLY)
                    set_default_levels(day.early_session)
                day.early_session.candles_15m.append(candle)
            elif is_premarket_time(candle[5], day.date_readable):
                if not day.premarket:
                    day.premarket = new_spa(SessionName.PRE)
                    set_default_levels(day.premarket)
                day.premarket.candles_15m.append(candle)
            elif is_ny_am_open_time(candle[5], day.date_readable):
                if not day.ny_am_open:
                    day.ny_am_open = new_spa(SessionName.NY_OPEN)
                    set_default_levels(day.ny_am_open)
                day.ny_am_open.candles_15m.append(candle)
            elif is_ny_am_time(candle[5], day.date_readable):
                if not day.ny_am:
                    day.ny_am = new_spa(SessionName.NY_AM)
                    set_default_levels(day.ny_am)
                day.ny_am.candles_15m.append(candle)
            elif is_ny_lunch_time(candle[5], day.date_readable):
                if not day.ny_lunch:
                    day.ny_lunch = new_spa(SessionName.NY_LUNCH)
                    set_default_levels(day.ny_lunch)
                day.ny_lunch.candles_15m.append(candle)
            elif is_ny_pm_time(candle[5], day.date_readable):
                if not day.ny_pm:
                    day.ny_pm = new_spa(SessionName.NY_PM)
                    set_default_levels(day.ny_pm)
                day.ny_pm.candles_15m.append(candle)
            elif is_ny_pm_close_time(candle[5], day.date_readable):
                if not day.ny_pm_close:
                    day.ny_pm_close = new_spa(SessionName.NY_CLOSE)
                    set_default_levels(day.ny_pm_close)
                day.ny_pm_close.candles_15m.append(candle)

            if is_some_same_day(candle[5], day_group[0][5], "00:00", "00:01"):
                true_do = (candle[0], candle[5])
            if candle_datetime.isoweekday() == 1 \
                    and is_some_same_day(candle[5], day_group[0][5], "18:00", "18:01"):
                true_wo = (candle[0], candle[5])
            if candle_datetime.isoweekday() not in [6, 7] \
                    and is_some_same_day(candle[5], day_group[0][5], "16:00", "16:01"):
                prev_ny_close = (candle[0], candle[5])
            if candle_datetime.isoweekday() not in [6, 7] \
                    and is_some_same_day(candle[5], day_group[0][5], "17:00", "17:01"):
                prev_cme_close = (candle[0], candle[5])
            if week_high[1] == "" or candle[1] > week_high[0]:
                week_high = (candle[0], candle[5])
                set_week_ranges()
            if week_low[1] == "" or candle[2] < week_low[0]:
                week_low = (candle[0], candle[5])
                set_week_ranges()

            day.candles_15m.append(candle)

        if day.cme:
            day.cme.session_candle = as_1_candle(day.cme.candles_15m)
        if day.asia:
            day.asia.session_candle = as_1_candle(day.asia.candles_15m)
        if day.london:
            day.london.session_candle = as_1_candle(day.london.candles_15m)
        if day.early_session:
            day.early_session.session_candle = as_1_candle(day.early_session.candles_15m)
        if day.premarket:
            day.premarket.session_candle = as_1_candle(day.premarket.candles_15m)
        if day.ny_am_open:
            day.ny_am_open.session_candle = as_1_candle(day.ny_am_open.candles_15m)
        if day.ny_am:
            day.ny_am.session_candle = as_1_candle(day.ny_am.candles_15m)
        if day.ny_lunch:
            day.ny_lunch.session_candle = as_1_candle(day.ny_lunch.candles_15m)
        if day.ny_pm:
            day.ny_pm.session_candle = as_1_candle(day.ny_pm.candles_15m)
        if day.ny_pm_close:
            day.ny_pm_close.session_candle = as_1_candle(day.ny_pm_close.candles_15m)

        days.append(day)

    return days


def main(symbol, years):
    candles_15m = []
    for year in years:
        candles_15m.extend(select_full_days_candles_15m(year, symbol))

    days = markup_days(candles_15m)
    print(f"Done marking up {symbol} {len(days)} days. Inserting results to db")

    grouped_days = []
    for day in days:
        if len(grouped_days) > 0 and to_utc_datetime(day.date_readable).year == to_utc_datetime(
                grouped_days[-1][-1].date_readable).year:
            grouped_days[-1].append(day)
        else:
            grouped_days.append([day])

    for day_group in grouped_days:
        year = to_utc_datetime(day_group[0].date_readable).year
        result = upsert_days_to_db(year, symbol, day_group)
        if result:
            print(f"Done with {year} year {symbol}, last day is {day_group[-1].date_readable}")

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
            series_years = [
                2021,
                2022,
                2023,
                2024,
                2025
            ]
            main(smb, series_years)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
