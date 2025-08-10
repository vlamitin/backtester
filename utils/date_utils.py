import math
from datetime import datetime, timedelta
from typing import Tuple, List
from zoneinfo import ZoneInfo

from stock_market_research_kit.quarter import YearQuarter, MonthWeek, WeekDay, DayQuarter, Quarter90m


# returns -1 if checked time is past, 0 if same day and +1 if future
def is_same_day_or_past_or_future(checked_time_string, current_day_string):
    current_day = to_utc_datetime(current_day_string)
    next_day = current_day + timedelta(days=1)
    checked_time = to_utc_datetime(checked_time_string)

    range_from = datetime(current_day.year, current_day.month, current_day.day, 0, 0, tzinfo=ZoneInfo("UTC"))
    range_to = datetime(next_day.year, next_day.month, next_day.day, 0, 0, tzinfo=ZoneInfo("UTC"))

    if range_from > checked_time:
        return -1
    elif checked_time >= range_to:
        return 1
    else:
        return 0


def session_end_time(candle, candles_15m) -> str:
    return to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15 * len(candles_15m)) - timedelta(seconds=1))


def start_of_day(date: datetime) -> datetime:
    return date.replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(date: datetime) -> datetime:
    return date.replace(hour=23, minute=59, second=59, microsecond=999000)


# it returns start_date, end_date and all days with 0:00 in between
def get_all_days_between(start_date: datetime, end_date: datetime):
    if start_date.date() == end_date.date():
        return [end_date]
    days = [start_date]
    current_date = start_of_day(start_date + timedelta(days=1))

    while current_date < end_date and current_date != start_of_day(end_date):
        days.append(current_date)
        current_date += timedelta(days=1)

    days.append(end_date)

    return days


def to_timestamp(date_str: str) -> int:
    if date_str == "":
        return -1

    return int(to_utc_datetime(date_str).timestamp() * 1000)


def get_previous_candle_15m_close() -> datetime:
    now = datetime.now(ZoneInfo("UTC"))
    rounded = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    return rounded


def now_ts() -> int:
    return int(now_utc_datetime().timestamp() * 1000)


def now_utc_datetime() -> datetime:
    return datetime.now(ZoneInfo("UTC"))


def now_ny_datetime() -> datetime:
    return datetime.now(ZoneInfo("America/New_York"))


def quarters_by_time(date_utc: str) -> Tuple[YearQuarter, MonthWeek, WeekDay, DayQuarter, Quarter90m]:
    ny_date = to_utc_datetime(date_utc).astimezone(ZoneInfo("America/New_York"))
    yq = YearQuarter.YQ1
    if ny_date.month in [4, 5, 6]:
        yq = YearQuarter.YQ2
    elif ny_date.month in [7, 8, 9]:
        yq = YearQuarter.YQ3
    elif ny_date.month in [10, 11, 12]:
        yq = YearQuarter.YQ4

    mw = MonthWeek(math.ceil((ny_date - timedelta(days=ny_date.weekday())).day / 7))
    wd = WeekDay(ny_date.isoweekday())

    dq = DayQuarter.DQ2_London
    if ny_date.hour in [6, 7, 8, 9, 10, 11]:
        dq = DayQuarter.DQ3_NYAM
    elif ny_date.hour in [12, 13, 14, 15, 16, 17]:
        dq = DayQuarter.DQ4_NYPM
    if ny_date.hour in [18, 19, 20, 21, 22, 23]:
        dq = DayQuarter.DQ1_Asia
        wd = WeekDay((wd.value + 1) % 7)
        mw = MonthWeek(math.ceil(
            (ny_date + timedelta(hours=6) - timedelta(days=(ny_date + timedelta(hours=6)).weekday())).day / 7))

    start_of_dq = ny_date.replace(hour=(ny_date.hour // 6) * 6, minute=0, second=0, microsecond=0)
    mins_from_dq_start = (ny_date - start_of_dq).total_seconds() / 60

    q90m = Quarter90m.Q1_90m
    if 90 <= mins_from_dq_start < 180:
        q90m = Quarter90m.Q2_90m
    elif 180 <= mins_from_dq_start < 270:
        q90m = Quarter90m.Q3_90m
    elif 270 <= mins_from_dq_start < 360:
        q90m = Quarter90m.Q4_90m

    return yq, mw, wd, dq, q90m


def quarters90m_ranges(date_utc: str) -> Tuple[List[Tuple[Quarter90m, datetime, datetime]], str]:  # ranges, t90m0
    res = []
    q90m = quarters_by_time(date_utc)[4]
    ny_date = to_utc_datetime(date_utc).astimezone(ZoneInfo("America/New_York"))

    seconds_since_midnight = ny_date.hour * 3600 + ny_date.minute * 60 + ny_date.second
    start_of_q90m = ny_date - timedelta(seconds=seconds_since_midnight % (90 * 60))

    t90m0 = None

    if q90m == Quarter90m.Q1_90m:
        res.append((
            Quarter90m.Q4_90m,
            (start_of_q90m - timedelta(minutes=90)).astimezone(ZoneInfo("UTC")),
            (start_of_q90m - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            Quarter90m.Q1_90m,
            start_of_q90m.astimezone(ZoneInfo("UTC")),
            (start_of_q90m + timedelta(minutes=90) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif q90m == Quarter90m.Q2_90m:
        t90m0 = start_of_q90m.astimezone(ZoneInfo("UTC"))
        res.append((
            Quarter90m.Q1_90m,
            (start_of_q90m - timedelta(minutes=90)).astimezone(ZoneInfo("UTC")),
            t90m0 - timedelta(seconds=1)
        ))
        res.append((
            Quarter90m.Q2_90m,
            t90m0,
            t90m0 + timedelta(minutes=90) - timedelta(seconds=1)
        ))
    elif q90m == Quarter90m.Q3_90m:
        t90m0 = (start_of_q90m - timedelta(minutes=90)).astimezone(ZoneInfo("UTC"))
        res.append((
            Quarter90m.Q1_90m,
            (start_of_q90m - timedelta(minutes=180)).astimezone(ZoneInfo("UTC")),
            (start_of_q90m - timedelta(minutes=90, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            Quarter90m.Q2_90m,
            t90m0,
            (start_of_q90m - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            Quarter90m.Q3_90m,
            start_of_q90m.astimezone(ZoneInfo("UTC")),
            (start_of_q90m + timedelta(minutes=90) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif q90m == Quarter90m.Q4_90m:
        t90m0 = (start_of_q90m - timedelta(minutes=180)).astimezone(ZoneInfo("UTC"))
        res.append((
            Quarter90m.Q1_90m,
            (start_of_q90m - timedelta(minutes=270)).astimezone(ZoneInfo("UTC")),
            (start_of_q90m - timedelta(minutes=180, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            Quarter90m.Q2_90m,
            t90m0,
            (start_of_q90m - timedelta(minutes=90, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            Quarter90m.Q3_90m,
            (start_of_q90m - timedelta(minutes=90)).astimezone(ZoneInfo("UTC")),
            (start_of_q90m - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            Quarter90m.Q4_90m,
            start_of_q90m.astimezone(ZoneInfo("UTC")),
            (start_of_q90m + timedelta(minutes=90) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))

    return res, "" if t90m0 == "" else to_date_str(t90m0)


def day_quarters_ranges(date_utc: str) -> Tuple[List[Tuple[DayQuarter, datetime, datetime]], str]:  # ranges, tdo
    res = []
    dq = quarters_by_time(date_utc)[3]
    ny_date = to_utc_datetime(date_utc).astimezone(ZoneInfo("America/New_York"))

    start_of_dq = ny_date.replace(hour=(ny_date.hour // 6) * 6, minute=0, second=0, microsecond=0)

    tdo = None

    if dq == DayQuarter.DQ1_Asia:
        res.append((
            DayQuarter.DQ4_NYPM,
            (start_of_dq - timedelta(hours=6)).astimezone(ZoneInfo("UTC")),
            (start_of_dq - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            DayQuarter.DQ1_Asia,
            start_of_dq.astimezone(ZoneInfo("UTC")),
            (start_of_dq + timedelta(hours=6) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif dq == DayQuarter.DQ2_London:
        tdo = start_of_dq.astimezone(ZoneInfo("UTC"))
        res.append((
            DayQuarter.DQ1_Asia,
            (start_of_dq - timedelta(hours=6)).astimezone(ZoneInfo("UTC")),
            tdo - timedelta(seconds=1)
        ))
        res.append((
            DayQuarter.DQ2_London,
            tdo,
            tdo + timedelta(hours=6) - timedelta(seconds=1)
        ))
    elif dq == DayQuarter.DQ3_NYAM:
        tdo = (start_of_dq - timedelta(hours=6)).astimezone(ZoneInfo("UTC"))
        res.append((
            DayQuarter.DQ1_Asia,
            (start_of_dq - timedelta(hours=12)).astimezone(ZoneInfo("UTC")),
            (start_of_dq - timedelta(hours=6, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            DayQuarter.DQ2_London,
            tdo,
            (start_of_dq - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            DayQuarter.DQ3_NYAM,
            start_of_dq.astimezone(ZoneInfo("UTC")),
            (start_of_dq + timedelta(hours=6) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif dq == DayQuarter.DQ4_NYPM:
        tdo = (start_of_dq - timedelta(hours=12)).astimezone(ZoneInfo("UTC"))
        res.append((
            DayQuarter.DQ1_Asia,
            (start_of_dq - timedelta(hours=18)).astimezone(ZoneInfo("UTC")),
            (start_of_dq - timedelta(hours=12, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            DayQuarter.DQ2_London,
            tdo,
            (start_of_dq - timedelta(hours=6, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            DayQuarter.DQ3_NYAM,
            (start_of_dq - timedelta(hours=6)).astimezone(ZoneInfo("UTC")),
            (start_of_dq - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            DayQuarter.DQ4_NYPM,
            start_of_dq.astimezone(ZoneInfo("UTC")),
            (start_of_dq + timedelta(hours=6) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))

    return res, "" if not tdo else to_date_str(tdo)


def weekday_ranges(date_utc: str) -> Tuple[List[Tuple[WeekDay, datetime, datetime]], str]:  # ranges, two
    res = []
    _, _, wd, dq, _ = quarters_by_time(date_utc)
    ny_date = to_utc_datetime(date_utc).astimezone(ZoneInfo("America/New_York"))

    start_of_wd = ny_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=6)
    if dq == DayQuarter.DQ1_Asia:
        start_of_wd = ny_date.replace(hour=(ny_date.hour // 6) * 6, minute=0, second=0, microsecond=0)

    two = None

    if wd == WeekDay.Mon:
        res.append((
            WeekDay.Thu,
            (start_of_wd - timedelta(days=4)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(days=3, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Fri,
            (start_of_wd - timedelta(days=3)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(days=2, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Mon,
            start_of_wd.astimezone(ZoneInfo("UTC")),
            (start_of_wd + timedelta(days=1) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif wd == WeekDay.Tue:
        two = start_of_wd.astimezone(ZoneInfo("UTC"))
        res.append((
            WeekDay.Mon,
            (start_of_wd - timedelta(days=1)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Tue,
            start_of_wd.astimezone(ZoneInfo("UTC")),
            (start_of_wd + timedelta(days=1) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif wd == WeekDay.Wed:
        two = (start_of_wd - timedelta(days=1)).astimezone(ZoneInfo("UTC"))
        res.append((
            WeekDay.Mon,
            (start_of_wd - timedelta(days=2)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(days=1, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Tue,
            two,
            (start_of_wd - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Wed,
            start_of_wd.astimezone(ZoneInfo("UTC")),
            (start_of_wd + timedelta(days=1) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif wd == WeekDay.Thu:
        two = (start_of_wd - timedelta(days=2)).astimezone(ZoneInfo("UTC"))
        res.append((
            WeekDay.Mon,
            (start_of_wd - timedelta(days=3)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(days=2, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Tue,
            two,
            (start_of_wd - timedelta(days=1, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Wed,
            (start_of_wd - timedelta(days=1)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Thu,
            start_of_wd.astimezone(ZoneInfo("UTC")),
            (start_of_wd + timedelta(days=1) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif wd == WeekDay.Fri:
        two = (start_of_wd - timedelta(days=3)).astimezone(ZoneInfo("UTC"))
        res.append((
            WeekDay.Mon,
            (start_of_wd - timedelta(days=4)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(days=3, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Tue,
            two,
            (start_of_wd - timedelta(days=2, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Wed,
            (start_of_wd - timedelta(days=2)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(days=1, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Thu,
            (start_of_wd - timedelta(days=1)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.MonThu,
            (start_of_wd - timedelta(days=4)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Fri,
            start_of_wd.astimezone(ZoneInfo("UTC")),
            (start_of_wd + timedelta(days=1) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif wd == WeekDay.Sat:
        two = (start_of_wd - timedelta(days=4)).astimezone(ZoneInfo("UTC"))
        res.append((
            WeekDay.MonThu,
            (start_of_wd - timedelta(days=5)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(days=1, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Fri,
            (start_of_wd - timedelta(days=1)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.MonFri,
            (start_of_wd - timedelta(days=5)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Sat,
            start_of_wd.astimezone(ZoneInfo("UTC")),
            (start_of_wd + timedelta(days=1) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif wd == WeekDay.Sun:
        two = (start_of_wd - timedelta(days=5)).astimezone(ZoneInfo("UTC"))
        res.append((
            WeekDay.MonThu,
            (start_of_wd - timedelta(days=6)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(days=2, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Fri,
            (start_of_wd - timedelta(days=2)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(days=1, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.MonFri,
            (start_of_wd - timedelta(days=6)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(days=1, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Sat,
            (start_of_wd - timedelta(days=1)).astimezone(ZoneInfo("UTC")),
            (start_of_wd - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            WeekDay.Sun,
            start_of_wd.astimezone(ZoneInfo("UTC")),
            (start_of_wd + timedelta(days=1) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))

    return res, "" if not two else to_date_str(two)


def month_week_quarters_ranges(date_utc: str) -> Tuple[List[Tuple[MonthWeek, datetime, datetime]], str]:
    res = []
    mw = quarters_by_time(date_utc)[1]
    ny_date = to_utc_datetime(date_utc).astimezone(ZoneInfo("America/New_York"))
    start_of_mw = (ny_date - timedelta(days=ny_date.weekday())).replace(hour=0, minute=0, second=0,
                                                                        microsecond=0) - timedelta(hours=6)

    tmo = None

    if mw == MonthWeek.MW1:
        prev_mw = quarters_by_time(to_date_str((start_of_mw - timedelta(days=7)).astimezone(ZoneInfo("UTC"))))[1]
        if prev_mw == MonthWeek.MW5:
            res.append((
                MonthWeek.MW4,
                (start_of_mw - timedelta(days=14)).astimezone(ZoneInfo("UTC")),
                (start_of_mw - timedelta(days=7, seconds=1)).astimezone(ZoneInfo("UTC"))
            ))
            res.append((
                MonthWeek.MW5,
                (start_of_mw - timedelta(days=7)).astimezone(ZoneInfo("UTC")),
                (start_of_mw - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
            ))
        else:
            res.append((
                MonthWeek.MW4,
                (start_of_mw - timedelta(days=7)).astimezone(ZoneInfo("UTC")),
                (start_of_mw - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
            ))
        res.append((
            MonthWeek.MW1,
            start_of_mw.astimezone(ZoneInfo("UTC")),
            (start_of_mw + timedelta(days=7) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif mw == MonthWeek.MW2:
        tmo = start_of_mw.astimezone(ZoneInfo("UTC"))
        res.append((
            MonthWeek.MW1,
            (start_of_mw - timedelta(days=7)).astimezone(ZoneInfo("UTC")),
            (start_of_mw - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            MonthWeek.MW2,
            start_of_mw.astimezone(ZoneInfo("UTC")),
            (start_of_mw + timedelta(days=7) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif mw == MonthWeek.MW3:
        tmo = (start_of_mw - timedelta(days=7)).astimezone(ZoneInfo("UTC"))
        res.append((
            MonthWeek.MW1,
            (start_of_mw - timedelta(days=14)).astimezone(ZoneInfo("UTC")),
            (start_of_mw - timedelta(days=7, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            MonthWeek.MW2,
            tmo,
            (start_of_mw - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            MonthWeek.MW3,
            start_of_mw.astimezone(ZoneInfo("UTC")),
            (start_of_mw + timedelta(days=7) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif mw == MonthWeek.MW4:
        tmo = (start_of_mw - timedelta(days=14)).astimezone(ZoneInfo("UTC"))
        res.append((
            MonthWeek.MW1,
            (start_of_mw - timedelta(days=21)).astimezone(ZoneInfo("UTC")),
            (start_of_mw - timedelta(days=14, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            MonthWeek.MW2,
            tmo,
            (start_of_mw - timedelta(days=7, seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            MonthWeek.MW3,
            (start_of_mw - timedelta(days=7)).astimezone(ZoneInfo("UTC")),
            (start_of_mw - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            MonthWeek.MW4,
            start_of_mw.astimezone(ZoneInfo("UTC")),
            (start_of_mw + timedelta(days=7) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
    elif mw == MonthWeek.MW5:
        tmo = (start_of_mw - timedelta(days=21)).astimezone(ZoneInfo("UTC"))
        res.append((
            MonthWeek.MW4,
            (start_of_mw - timedelta(days=7)).astimezone(ZoneInfo("UTC")),
            (start_of_mw - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))
        res.append((
            MonthWeek.MW5,
            start_of_mw.astimezone(ZoneInfo("UTC")),
            (start_of_mw + timedelta(days=7) - timedelta(seconds=1)).astimezone(ZoneInfo("UTC"))
        ))

    return res, "" if not tmo else to_date_str(tmo)


def year_quarters_ranges(date_utc: str) -> Tuple[List[Tuple[YearQuarter, datetime, datetime]], str]:
    res = []
    yq = quarters_by_time(date_utc)[0]
    utc_date = to_utc_datetime(date_utc)

    tyo = None

    if yq == YearQuarter.YQ1:
        res.append((
            YearQuarter.YQ4,
            utc_date.replace(year=utc_date.year - 1, month=10, day=1, hour=0, minute=0, second=0, microsecond=0),
            utc_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        ))
        res.append((
            YearQuarter.YQ1,
            utc_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
            utc_date.replace(month=4, day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        ))
    elif yq == YearQuarter.YQ2:
        tyo = utc_date.replace(month=4, day=1, hour=0, minute=0, second=0, microsecond=0)
        res.append((
            YearQuarter.YQ1,
            utc_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
            tyo - timedelta(seconds=1)
        ))
        res.append((
            YearQuarter.YQ2,
            tyo,
            utc_date.replace(month=7, day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        ))
    elif yq == YearQuarter.YQ3:
        tyo = utc_date.replace(month=4, day=1, hour=0, minute=0, second=0, microsecond=0)
        res.append((
            YearQuarter.YQ1,
            utc_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
            utc_date.replace(month=4, day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        ))
        res.append((
            YearQuarter.YQ2,
            tyo,
            utc_date.replace(month=7, day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        ))
        res.append((
            YearQuarter.YQ3,
            utc_date.replace(month=7, day=1, hour=0, minute=0, second=0, microsecond=0),
            utc_date.replace(month=10, day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        ))
    elif yq == YearQuarter.YQ4:
        tyo = utc_date.replace(month=4, day=1, hour=0, minute=0, second=0, microsecond=0)
        res.append((
            YearQuarter.YQ1,
            utc_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
            utc_date.replace(month=4, day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        ))
        res.append((
            YearQuarter.YQ2,
            tyo,
            utc_date.replace(month=7, day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        ))
        res.append((
            YearQuarter.YQ3,
            utc_date.replace(month=7, day=1, hour=0, minute=0, second=0, microsecond=0),
            utc_date.replace(month=10, day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
        ))
        res.append((
            YearQuarter.YQ4,
            utc_date.replace(month=10, day=1, hour=0, minute=0, second=0, microsecond=0),
            utc_date.replace(year=utc_date.year + 1, month=1, day=1, hour=0, minute=0, second=0,
                             microsecond=0) - timedelta(seconds=1)
        ))

    return res, "" if not tyo else to_date_str(tyo)


def prev_year_ranges(date_utc: str) -> Tuple[datetime, datetime]:
    utc_date = to_utc_datetime(date_utc)

    start = utc_date.replace(year=utc_date.year - 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(year=utc_date.year) - timedelta(seconds=1)

    return start, end


def is_some_prev_day_session(checked_time_string, current_day_string, from_ny, to_ny):
    current_day = to_utc_datetime(current_day_string)
    prev_day = current_day - timedelta(days=1)

    if prev_day.isoweekday() in [5, 6]:
        return False

    checked_time = to_utc_datetime(checked_time_string)

    from_h, from_m = [int(x) for x in from_ny.split(":")]
    to_h, to_m = [int(x) for x in to_ny.split(":")]

    range_from = datetime(
        prev_day.year, prev_day.month, prev_day.day, from_h, from_m, tzinfo=ZoneInfo("America/New_York")
    )
    range_to = datetime(
        prev_day.year, prev_day.month, prev_day.day, to_h, to_m, tzinfo=ZoneInfo("America/New_York")
    )

    return range_from <= checked_time < range_to


def is_some_same_day_session(checked_time_string: str, current_day_string: str, from_ny: str, to_ny: str):
    current_day = to_utc_datetime(current_day_string)
    if current_day.isoweekday() in [6, 7]:
        return False

    return is_some_same_day(checked_time_string, current_day_string, from_ny, to_ny)


def is_some_same_day(checked_time_string: str, current_day_string: str, from_ny: str, to_ny: str):
    current_day = to_utc_datetime(current_day_string)
    checked_time = to_utc_datetime(checked_time_string)

    from_h, from_m = [int(x) for x in from_ny.split(":")]
    to_h, to_m = [int(x) for x in to_ny.split(":")]

    range_from = datetime(
        current_day.year, current_day.month, current_day.day, from_h, from_m, tzinfo=ZoneInfo("America/New_York")
    )
    range_to = datetime(
        current_day.year, current_day.month, current_day.day, to_h, to_m, tzinfo=ZoneInfo("America/New_York")
    )

    return range_from <= checked_time < range_to


def to_utc_datetime(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo("UTC"))


def to_date_str(date: datetime) -> str:
    return date.strftime("%Y-%m-%d %H:%M")


def log_warn(message: str):
    print(f"WARN at {to_date_str(now_utc_datetime())} utc: {message}")


def log_warn_ny(message: str):
    print(f"WARN at {to_date_str(now_ny_datetime())} ny: {message}")


def log_info_ny(message: str):
    print(f"INFO at {to_date_str(now_ny_datetime())} ny: {message}")


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


def get_second_monday(date: datetime) -> datetime:
    first_day = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    days_to_first_monday = (7 - first_day.weekday() + 0) % 7
    first_monday = first_day + timedelta(days=days_to_first_monday)
    return first_monday + timedelta(days=7)


def is_previous_week(date_to_check: datetime, reference_date: datetime) -> bool:
    reference_week_start = reference_date - timedelta(days=reference_date.weekday())
    previous_week_start = reference_week_start - timedelta(weeks=1)
    previous_week_end = reference_week_start - timedelta(days=1)

    return previous_week_start <= date_to_check <= previous_week_end


def get_prev_30m_from_to(date_str: str) -> Tuple[datetime, datetime]:
    from_ = get_current_30m_from_to(date_str)[0] - timedelta(minutes=30)
    return from_, from_ + timedelta(minutes=29, seconds=59)


def get_current_30m_from_to(date_str: str) -> Tuple[datetime, datetime]:
    date = to_utc_datetime(date_str)
    from_ = date.replace(minute=(date.minute // 30) * 30, second=0, microsecond=0)
    return from_, from_ + timedelta(minutes=29, seconds=59)


def get_prev_1h_from_to(date_str: str) -> Tuple[datetime, datetime]:
    from_ = get_current_1h_from_to(date_str)[0] - timedelta(hours=1)
    return from_, from_ + timedelta(minutes=59, seconds=59)


def get_current_1h_from_to(date_str: str) -> Tuple[datetime, datetime]:
    date = to_utc_datetime(date_str)
    from_ = date.replace(minute=0, second=0, microsecond=0)
    return from_, from_ + timedelta(minutes=59, seconds=59)


def get_prev_2h_from_to(date_str: str) -> Tuple[datetime, datetime]:
    from_ = get_current_2h_from_to(date_str)[0] - timedelta(hours=2)
    return from_, from_ + timedelta(hours=1, minutes=59, seconds=59)


def get_current_2h_from_to(date_str: str) -> Tuple[datetime, datetime]:
    date = to_utc_datetime(date_str)
    from_ = date.replace(hour=(date.hour // 2) * 2, minute=0, second=0, microsecond=0)
    return from_, from_ + timedelta(hours=1, minutes=59, seconds=59)


def get_prev_4h_from_to(date_str: str) -> Tuple[datetime, datetime]:
    from_ = get_current_4h_from_to(date_str)[0] - timedelta(hours=4)
    return from_, from_ + timedelta(hours=3, minutes=59, seconds=59)


def get_current_4h_from_to(date_str: str) -> Tuple[datetime, datetime]:
    date = to_utc_datetime(date_str)
    from_ = date.replace(hour=(date.hour // 4) * 4, minute=0, second=0, microsecond=0)
    return from_, from_ + timedelta(hours=3, minutes=59, seconds=59)


def get_prev_1d_from_to(date_str: str) -> Tuple[datetime, datetime]:
    from_ = get_current_1d_from_to(date_str)[0] - timedelta(days=1)
    return from_, from_ + timedelta(hours=23, minutes=59, seconds=59)


def get_current_1d_from_to(date_str: str) -> Tuple[datetime, datetime]:
    date = to_utc_datetime(date_str)
    from_ = date.replace(hour=0, minute=0, second=0, microsecond=0)
    return from_, from_ + timedelta(hours=23, minutes=59, seconds=59)


def get_prev_1w_from_to(date_str: str) -> Tuple[datetime, datetime]:
    from_ = get_current_1w_from_to(date_str)[0] - timedelta(days=7)
    return from_, from_ + timedelta(days=6, hours=23, minutes=59, seconds=59)


def get_current_1w_from_to(date_str: str) -> Tuple[datetime, datetime]:
    date = to_utc_datetime(date_str)
    from_ = (date - timedelta(days=date.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return from_, from_ + timedelta(days=6, hours=23, minutes=59, seconds=59)


def get_prev_1month_from_to(date_str: str) -> Tuple[datetime, datetime]:
    to_ = get_current_1month_from_to(date_str)[0] - timedelta(seconds=1)
    from_ = to_.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    return from_, to_


def get_current_1month_from_to(date_str: str) -> Tuple[datetime, datetime]:
    date = to_utc_datetime(date_str)
    from_ = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    to_ = (from_ + timedelta(days=45)).replace(day=1, hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
    return from_, to_


if __name__ == "__main__":
    try:
        log_warn("test")
        # res = get_all_days_between(
        #     datetime(2024, 3, 1, 15, 0, tzinfo=ZoneInfo("UTC")),
        #     datetime(2024, 3, 5, 10, 0, tzinfo=ZoneInfo("UTC"))
        # )
        # print(get_prev_1month_from_to('2025-02-03 11:01'))
        # print(get_prev_1month_from_to('2025-01-03 10:31'))
        # print(get_current_1month_from_to('2024-02-03 11:01'))
        # print(get_current_1month_from_to('2025-01-03 10:31'))
        # print(quarters_by_time('2025-08-03 21:31'))
        # print(quarters_by_time('2025-08-03 22:31'))
        # print(quarters_by_time('2025-08-05 21:31'))
        # print(quarters_by_time('2025-08-05 22:31'))
        # print(quarters_by_time('2025-08-03 23:01'))
        print(weekday_ranges('2025-05-23 00:00'))
        print(weekday_ranges('2025-05-22 21:00'))
        print(weekday_ranges('2025-05-22 18:00'))
        print(weekday_ranges('2025-05-22 15:00'))
        print(weekday_ranges('2025-05-22 12:00'))
        print(weekday_ranges('2025-05-22 09:00'))
        print(weekday_ranges('2025-05-22 06:00'))
        print(weekday_ranges('2025-05-22 03:00'))
        # print(prev_year_ranges('2025-02-04 11:01'))

        # get_prev_1month_from_to
        # get_current_1month_from_to
        # print(get_prev_2h_open('2025-08-03 10:31'))
        # print(get_prev_2h_open('2025-08-03 10:31'))
        # print(get_prev_4h_open('2025-08-03 10:31'))
        # print(get_prev_1d_open('2025-08-03 10:31'))
        # print(get_prev_1w_open('2025-08-03 10:31'))
        # print(get_prev_1month_open('2025-08-03 10:31'))
        # print(get_prev_1month_open('2025-01-03 10:31'))
        # print(get_prev_1month_open('2024-02-29 10:31'))
        # print(get_current_30m_open(to_date_str(datetime.now(tz=ZoneInfo("UTC")))))
        # print(get_current_1h_open(to_date_str(datetime.now(tz=ZoneInfo("UTC")))))
        # print(get_current_2h_open(to_date_str(datetime.now(tz=ZoneInfo("UTC")))))
        # print(get_current_4h_open(to_date_str(datetime.now(tz=ZoneInfo("UTC")))))
        # print(get_current_1d_open(to_date_str(datetime.now(tz=ZoneInfo("UTC")))))
        # print(get_current_1w_open(to_date_str(datetime.now(tz=ZoneInfo("UTC")))))
        # print(get_current_1month_open(to_date_str(datetime.now(tz=ZoneInfo("UTC")))))

        log_warn("done")

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
