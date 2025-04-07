from datetime import timedelta
from typing import List, Tuple, Optional

from scripts.setup_db import connect_to_db
from stock_market_research_kit.candle import InnerCandle, as_1_candle
from stock_market_research_kit.day import Day, day_from_json
from stock_market_research_kit.session import Session, SessionType, SessionName, SessionImpact
from stock_market_research_kit.session_thresholds import SessionThresholds, btc_universal_threshold, impact_thresholds
from utils.date_utils import to_utc_datetime, to_date_str, start_of_day


def typify_sessions(days: List[Day]) -> List[Session]:
    if len(days) == 0:
        print("no data")
        return []

    sessions: List[Session] = []

    for day in days:
        if day.cme_as_candle[5] != "":
            sessions.append(Session(
                day_date=day.date_readable,
                session_date=day.cme_as_candle[5],
                session_end_date=session_end_time(day.cme_as_candle, day.cme_open_candles_15m),
                name=SessionName.CME,
                type=typify_session(day.cme_as_candle, btc_universal_threshold),
                impact=SessionImpact.UNSPECIFIED,
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
                impact=define_session_impact(day.asian_candles_15m, day.candles_15m, day.candle_1d),
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
                impact=define_session_impact(day.london_candles_15m, day.candles_15m, day.candle_1d),
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
                impact=define_session_impact(day.early_session_candles_15m, day.candles_15m, day.candle_1d),
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
                impact=define_session_impact(day.premarket_candles_15m, day.candles_15m, day.candle_1d),
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
                impact=define_session_impact(day.ny_am_open_candles_15m, day.candles_15m, day.candle_1d),
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
                impact=define_session_impact(day.ny_am_candles_15m, day.candles_15m, day.candle_1d),
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
                impact=define_session_impact(day.ny_lunch_candles_15m, day.candles_15m, day.candle_1d),
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
                impact=define_session_impact(day.ny_pm_candles_15m, day.candles_15m, day.candle_1d),
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
                impact=define_session_impact(day.ny_pm_close_candles_15m, day.candles_15m, day.candle_1d),
                open=day.ny_pm_close_as_candle[0],
                high=day.ny_pm_close_as_candle[1],
                low=day.ny_pm_close_as_candle[2],
                close=day.ny_pm_close_as_candle[3]
            ))

    return sessions


def session_end_time(as_candle, candles_15m) -> str:
    return to_date_str(to_utc_datetime(as_candle[5]) + timedelta(minutes=15 * len(candles_15m)) - timedelta(seconds=1))


def define_session_impact(session_candles: List[InnerCandle], day_candles: List[InnerCandle],
                          day_candle: InnerCandle) -> SessionImpact:
    session_day_candles = [x for x in session_candles
                           if start_of_day(to_utc_datetime(x[5])) == start_of_day(to_utc_datetime(day_candles[0][5]))]

    session_day_candle = as_1_candle(session_day_candles)
    if session_day_candle[1] == day_candle[1]:
        return SessionImpact.DAILY_HIGH
    if session_day_candle[2] == day_candle[2]:
        return SessionImpact.DAILY_LOW

    daily_high_time, daily_low_time = "", ""
    for candle in day_candles:
        if candle[1] == day_candle[1]:
            daily_high_time = candle[5]
        if candle[2] == day_candle[2]:
            daily_low_time = candle[5]

    if daily_high_time == "" or daily_low_time == "":
        print("hello there!")

    overlaps = session_overlaps(session_day_candle, day_candle)
    day_anatomy = candle_anatomy(day_candle)

    if day_anatomy[2] >= impact_thresholds['min_meaningful_wick'] and \
            to_utc_datetime(session_day_candles[-1][5]) < to_utc_datetime(daily_high_time) and \
            overlaps[0] > impact_thresholds['min_wick_overlap_percent']:
        return SessionImpact.HIGH_WICK_BUILDER

    if day_anatomy[4] >= impact_thresholds['min_meaningful_wick'] and \
            to_utc_datetime(session_day_candles[-1][5]) < to_utc_datetime(daily_low_time) and \
            overlaps[2] > impact_thresholds['min_wick_overlap_percent']:
        return SessionImpact.LOW_WICK_BUILDER

    min_body_adj_thr = min(day_candle[0], day_candle[3]) - impact_thresholds['body_adj_coef'] * (
                day_candle[1] - day_candle[2])
    max_body_adj_thr = max(day_candle[0], day_candle[3]) + impact_thresholds['body_adj_coef'] * (
                day_candle[1] - day_candle[2])

    if day_anatomy[2] >= impact_thresholds['min_meaningful_wick'] and \
            to_utc_datetime(session_day_candles[0][5]) > to_utc_datetime(daily_high_time) and \
            max(day_candle[0], day_candle[3]) < session_day_candles[0][0] < day_candle[1] and \
            min_body_adj_thr < session_day_candles[-1][3] < max_body_adj_thr:
        return SessionImpact.HIGH_TO_BODY_REVERSAL

    if day_anatomy[4] >= impact_thresholds['min_meaningful_wick'] and \
            to_utc_datetime(session_day_candles[0][5]) > to_utc_datetime(daily_low_time) and \
            day_candle[2] < session_day_candles[0][0] < min(day_candle[0], day_candle[3]) and \
            min_body_adj_thr < session_day_candles[-1][3] < max_body_adj_thr:
        return SessionImpact.LOW_TO_BODY_REVERSAL

    if day_anatomy[3] >= impact_thresholds['min_meaningful_body'] and overlaps[1] > impact_thresholds[
        'min_body_overlap_percent']:
        if (session_day_candle[0] - session_day_candle[3] > 0 and day_candle[0] - day_candle[3] > 0) or \
                (session_day_candle[0] - session_day_candle[3] < 0 and day_candle[0] - day_candle[3] < 0):
            return SessionImpact.FORWARD_BODY_BUILDER
        return SessionImpact.BACKWARD_BODY_BUILDER

    return SessionImpact.UNSPECIFIED


# returns session overlaps in percent with (upper wick, body, lower wick)
def session_overlaps(session: InnerCandle, day: InnerCandle) -> Tuple[float, float, float]:
    upper_wick_overlap = get_overlap((session[2], session[1]), (max(day[0], day[3]), day[1]))
    body_overlap = get_overlap((session[2], session[1]), (min(day[0], day[3]), max(day[0], day[3])))
    lower_wick_overlap = get_overlap((session[2], session[1]), (day[2], min(day[0], day[3])))

    return (
        0 if not upper_wick_overlap
        else (upper_wick_overlap[1] - upper_wick_overlap[0]) / (max(day[0], day[3]) - day[1]) * 100,
        0 if not body_overlap
        else (body_overlap[1] - body_overlap[0]) / (max(day[0], day[3]) - min(day[0], day[3])) * 100,
        0 if not lower_wick_overlap
        else (lower_wick_overlap[1] - lower_wick_overlap[0]) / (min(day[0], day[3]) - day[2]) * 100,
    )


def get_overlap(range1: Tuple[float, float], range2: Tuple[float, float]) -> Optional[Tuple[float, float]]:
    start = max(range1[0], range2[0])
    end = min(range1[1], range2[1])
    if start < end:
        return start, end
    return None


def typify_session(candle: InnerCandle, thresholds: SessionThresholds) -> SessionType:
    perf, volat, upper_wick_fraction, body_fraction, lower_wick_fraction = candle_anatomy(candle)

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
            if min(upper_wick_fraction, upper_wick_fraction) < thresholds.hammer_wick_max_min_fraction:
                return SessionType.HAMMER
            return SessionType.DOJI
        if thresholds.doji_max_fraction <= body_fraction < thresholds.indecision_max_fraction:
            if min(upper_wick_fraction, upper_wick_fraction) < thresholds.hammer_wick_max_min_fraction:
                return SessionType.HAMMER
            return SessionType.INDECISION
        if thresholds.indecision_max_fraction <= body_fraction < thresholds.directional_body_min_fraction:
            if perf < 0:
                if upper_wick_fraction > lower_wick_fraction:
                    return SessionType.BTS
                return SessionType.REJECTION_BEAR
            if lower_wick_fraction > upper_wick_fraction:
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
                if upper_wick_fraction > lower_wick_fraction:
                    return SessionType.BTS
                return SessionType.REJECTION_BEAR
            if lower_wick_fraction > upper_wick_fraction:
                return SessionType.STB
            return SessionType.REJECTION_BULL
        if upper_wick_fraction > lower_wick_fraction:
            return SessionType.PUMP_AND_DUMP
        return SessionType.V_SHAPE


def candle_anatomy(candle: InnerCandle) -> Tuple[float, float, float, float, float]:
    open_, high, low, close = candle[:4]

    perf = (close - open_) / open_ * 100
    volat = (high - low) / open_ * 100

    wicks_fractions = (0, 0) if high - low == 0 \
        else (
        (high - max(open_, close)) / (high - low),
        (min(open_, close) - low) / (high - low)
    )
    upper_wick_fraction, lower_wick_fraction = wicks_fractions
    body_fraction = 1 - upper_wick_fraction - lower_wick_fraction

    return perf, volat, upper_wick_fraction, body_fraction, lower_wick_fraction


if __name__ == "__main__":
    try:
        conn = connect_to_db(2024)
        c = conn.cursor()

        c.execute("""SELECT data FROM days WHERE symbol = ?""", ("BTCUSDT",))
        rows = c.fetchall()

        btc_days: List[Day] = [day_from_json(x[0]) for x in rows]
        sessions = typify_sessions(btc_days)
        print(f"Done typifying up {len(sessions)} sessions")

        conn.close()
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
