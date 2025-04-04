from datetime import timedelta
from typing import List, Tuple

from scripts.setup_db import connect_to_db
from stock_market_research_kit.day import Day, day_from_json
from stock_market_research_kit.session import Session, SessionType, SessionName
from stock_market_research_kit.session_thresholds import SessionThresholds, btc_universal_threshold
from utils.date_utils import to_utc_datetime, to_date_str


def typify_sessions(days: List[Day]) -> List[Session]:
    if len(days) == 0:
        print("no data")
        return

    sessions: List[Session] = []

    for i in range(len(days)):
        day = days[i]

        if day.cme_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.cme_as_candle[5],
                session_end_date=session_end_time(day.cme_as_candle, day.cme_open_candles_15m),
                name=SessionName.CME,
                type=typify_session(day.cme_as_candle, btc_universal_threshold),
                open=day.cme_as_candle[0],
                high=day.cme_as_candle[1],
                low=day.cme_as_candle[2],
                close=day.cme_as_candle[3]
            ))

        if day.asia_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.asia_as_candle[5],
                session_end_date=session_end_time(day.asia_as_candle, day.asian_candles_15m),
                name=SessionName.ASIA,
                type=typify_session(day.asia_as_candle, btc_universal_threshold),
                open=day.asia_as_candle[0],
                high=day.asia_as_candle[1],
                low=day.asia_as_candle[2],
                close=day.asia_as_candle[3]
            ))

        if day.london_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.london_as_candle[5],
                session_end_date=session_end_time(day.london_as_candle, day.london_candles_15m),
                name=SessionName.LONDON,
                type=typify_session(day.london_as_candle, btc_universal_threshold),
                open=day.london_as_candle[0],
                high=day.london_as_candle[1],
                low=day.london_as_candle[2],
                close=day.london_as_candle[3]
            ))

        if day.early_session_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.early_session_as_candle[5],
                session_end_date=session_end_time(day.early_session_as_candle, day.early_session_candles_15m),
                name=SessionName.EARLY,
                type=typify_session(day.early_session_as_candle, btc_universal_threshold),
                open=day.early_session_as_candle[0],
                high=day.early_session_as_candle[1],
                low=day.early_session_as_candle[2],
                close=day.early_session_as_candle[3]
            ))

        if day.premarket_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.premarket_as_candle[5],
                session_end_date=session_end_time(day.premarket_as_candle, day.premarket_candles_15m),
                name=SessionName.PRE,
                type=typify_session(day.premarket_as_candle, btc_universal_threshold),
                open=day.premarket_as_candle[0],
                high=day.premarket_as_candle[1],
                low=day.premarket_as_candle[2],
                close=day.premarket_as_candle[3]
            ))

        if day.ny_am_open_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.ny_am_open_as_candle[5],
                session_end_date=session_end_time(day.ny_am_open_as_candle, day.ny_am_open_candles_15m),
                name=SessionName.NY_OPEN,
                type=typify_session(day.ny_am_open_as_candle, btc_universal_threshold),
                open=day.ny_am_open_as_candle[0],
                high=day.ny_am_open_as_candle[1],
                low=day.ny_am_open_as_candle[2],
                close=day.ny_am_open_as_candle[3]
            ))

        if day.ny_am_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.ny_am_as_candle[5],
                session_end_date=session_end_time(day.ny_am_as_candle, day.ny_am_candles_15m),
                name=SessionName.NY_AM,
                type=typify_session(day.ny_am_as_candle, btc_universal_threshold),
                open=day.ny_am_as_candle[0],
                high=day.ny_am_as_candle[1],
                low=day.ny_am_as_candle[2],
                close=day.ny_am_as_candle[3]
            ))

        if day.ny_lunch_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.ny_lunch_as_candle[5],
                session_end_date=session_end_time(day.ny_lunch_as_candle, day.ny_lunch_candles_15m),
                name=SessionName.NY_LUNCH,
                type=typify_session(day.ny_lunch_as_candle, btc_universal_threshold),
                open=day.ny_lunch_as_candle[0],
                high=day.ny_lunch_as_candle[1],
                low=day.ny_lunch_as_candle[2],
                close=day.ny_lunch_as_candle[3]
            ))

        if day.ny_pm_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.ny_pm_as_candle[5],
                session_end_date=session_end_time(day.ny_pm_as_candle, day.ny_pm_candles_15m),
                name=SessionName.NY_PM,
                type=typify_session(day.ny_pm_as_candle, btc_universal_threshold),
                open=day.ny_pm_as_candle[0],
                high=day.ny_pm_as_candle[1],
                low=day.ny_pm_as_candle[2],
                close=day.ny_pm_as_candle[3]
            ))

        if day.ny_pm_close_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.ny_pm_close_as_candle[5],
                session_end_date=session_end_time(day.ny_pm_close_as_candle, day.ny_pm_close_candles_15m),
                name=SessionName.NY_CLOSE,
                type=typify_session(day.ny_pm_close_as_candle, btc_universal_threshold),
                open=day.ny_pm_close_as_candle[0],
                high=day.ny_pm_close_as_candle[1],
                low=day.ny_pm_close_as_candle[2],
                close=day.ny_pm_close_as_candle[3]
            ))

    return sessions


def session_end_time(as_candle, candles_15m) -> str:
    return to_date_str(to_utc_datetime(as_candle[5]) + timedelta(minutes=15 * len(candles_15m)) - timedelta(seconds=1))


def typify_session(candle: Tuple[float, float, float, float, float, str], thresholds: SessionThresholds) -> SessionType:
    perf = (candle[3] - candle[0]) / candle[0] * 100
    volat = (candle[1] - candle[2]) / candle[0] * 100

    wicks_fractions = (0, 0) if candle[1] - candle[2] == 0 \
        else (
        (candle[1] - max(candle[0], candle[3])) / (candle[1] - candle[2]),
        (min(candle[0], candle[3]) - candle[2]) / (candle[1] - candle[2])
    )
    body_fraction = 1 - wicks_fractions[0] - wicks_fractions[1]

    if volat < thresholds.slow_range[0]:  # COMPRESSION, DOJI
        return SessionType.COMPRESSION
    elif thresholds.slow_range[0] <= volat < thresholds.slow_range[1]:
        if body_fraction > thresholds.directional_body_min_fraction:
            if perf < 0:
                return SessionType.BEAR
            return SessionType.BULL

        if body_fraction < thresholds.doji_max_fraction:
            return SessionType.DOJI

        return SessionType.COMPRESSION
    elif thresholds.slow_range[1] <= volat < thresholds.fast_range[0]:
        if body_fraction > thresholds.directional_body_min_fraction:
            if perf < 0:
                return SessionType.BEAR
            return SessionType.BULL
        if body_fraction < thresholds.doji_max_fraction:
            return SessionType.DOJI
        return SessionType.INDECISION
    elif thresholds.fast_range[0] <= volat < thresholds.fast_range[1]:
        if body_fraction < thresholds.doji_max_fraction:
            if min(wicks_fractions[0], wicks_fractions[0]) < thresholds.hammer_wick_max_min_fraction:
                return SessionType.HAMMER
            return SessionType.DOJI
        if thresholds.doji_max_fraction <= body_fraction < thresholds.indecision_max_fraction:
            if min(wicks_fractions[0], wicks_fractions[0]) < thresholds.hammer_wick_max_min_fraction:
                return SessionType.HAMMER
            return SessionType.INDECISION
        if thresholds.indecision_max_fraction <= body_fraction < thresholds.directional_body_min_fraction:
            if perf < 0:
                if wicks_fractions[0] > wicks_fractions[1]:
                    return SessionType.BTS
                return SessionType.REJECTION_BEAR
            if wicks_fractions[1] > wicks_fractions[0]:
                return SessionType.STB
            return SessionType.REJECTION_BULL
        if body_fraction >= thresholds.directional_body_min_fraction:
            if perf < 0:
                return SessionType.BEAR
            return SessionType.BULL
    elif thresholds.fast_range[1] <= volat:
        if body_fraction > thresholds.directional_body_min_fraction:
            if perf < 0:
                return SessionType.FLASH_CRASH
            return SessionType.TO_THE_MOON
        if thresholds.indecision_max_fraction <= body_fraction < thresholds.directional_body_min_fraction:
            if perf < 0:
                if wicks_fractions[0] > wicks_fractions[1]:
                    return SessionType.BTS
                return SessionType.REJECTION_BEAR
            if wicks_fractions[1] > wicks_fractions[0]:
                return SessionType.STB
            return SessionType.REJECTION_BULL
        if wicks_fractions[0] > wicks_fractions[1]:
            return SessionType.PUMP_AND_DUMP
        return SessionType.V_SHAPE


YEAR = 2022
SYMBOL = "CRVUSDT"
if __name__ == "__main__":
    try:
        conn = connect_to_db(YEAR)
        c = conn.cursor()

        c.execute("""SELECT data FROM days WHERE symbol = ? AND date_ts = ?""", (SYMBOL, "2022-10-10 00:00"))
        rows = c.fetchall()

        if len(rows) == 0:
            print(f"Symbol {SYMBOL} not found in DB")
            quit(0)

        days = [day_from_json(x[0]) for x in rows]
        print(f"Typifying sessions of {len(rows)} {YEAR} days of {SYMBOL}")
        sessions = typify_sessions(days)
        print(f"Done typifying up {len(sessions)} sessions")

        conn.close()
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
