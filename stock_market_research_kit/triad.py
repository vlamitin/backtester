import json
import math
from collections import deque
from dataclasses import dataclass, asdict
from datetime import timedelta, datetime
from enum import Enum
from typing import TypeAlias, Tuple, Optional, List, Generator, Deque

from stock_market_research_kit.candle import PriceDate, InnerCandle, as_1_candle, as_1month_candles, as_1w_candles, \
    as_1d_candles, as_4h_candles, as_2h_candles, as_1h_candles, as_30m_candles, AsCandles
from stock_market_research_kit.quarter import Quarter90m, DayQuarter, WeekDay, MonthWeek, YearQuarter
from utils.date_utils import to_utc_datetime, to_date_str, get_prev_30m_from_to, get_current_30m_from_to, \
    get_prev_1h_from_to, get_current_1h_from_to, get_prev_2h_from_to, get_current_2h_from_to, get_prev_4h_from_to, \
    get_current_4h_from_to, get_prev_1d_from_to, get_current_1d_from_to, get_prev_1w_from_to, get_current_1w_from_to, \
    get_prev_1month_from_to, get_current_1month_from_to, quarters90m_ranges, day_quarters_ranges, \
    prev_year_ranges, weekday_ranges, month_week_quarters_ranges, year_quarters_ranges, quarters_by_time, \
    current_year_ranges, GetDateRange

LiqSwept: TypeAlias = Tuple[float, bool]  # (price, is_swept)
# (date_start, date_end, ended, high, half, low)
QuarterLiq: TypeAlias = Tuple[str, str, bool, LiqSwept, LiqSwept, LiqSwept]
Reverse15mGenerator: TypeAlias = Generator[InnerCandle, None, None]


class TriadAsset(Enum):
    A1 = 'Asset 1'
    A2 = 'Asset 2'
    A3 = 'Asset 3'


deprecated_SMT: TypeAlias = Optional[List[TriadAsset]]  # list of 1-2 assets from triad that swept
deprecated_PSP: TypeAlias = Optional[Tuple[
    Tuple[TriadAsset, InnerCandle],
    Tuple[TriadAsset, InnerCandle],
    Tuple[TriadAsset, InnerCandle]
]]


@dataclass
class Asset:
    symbol: str
    snapshot_date_readable: str
    candles_15m: Deque[InnerCandle]

    year_q1: Optional[QuarterLiq]
    year_q2: Optional[QuarterLiq]
    year_q3: Optional[QuarterLiq]
    year_q4: Optional[QuarterLiq]
    true_yo: Optional[PriceDate]  # 1st of april, for 1jan - 31mar well be None

    week1: Optional[QuarterLiq]
    week2: Optional[QuarterLiq]
    week3: Optional[QuarterLiq]
    week4: Optional[QuarterLiq]
    week5: Optional[QuarterLiq]  # joker week
    true_mo: Optional[PriceDate]  # 2nd monday of month, for first uncompleted week well be None

    nwog: Optional[Tuple[LiqSwept, LiqSwept]]  # swept status for high, swept status for low
    mon: Optional[QuarterLiq]
    tue: Optional[QuarterLiq]
    wed: Optional[QuarterLiq]
    thu: Optional[QuarterLiq]
    mon_thu: Optional[QuarterLiq]
    fri: Optional[QuarterLiq]
    mon_fri: Optional[QuarterLiq]
    sat: Optional[QuarterLiq]
    true_wo: Optional[PriceDate]  # monday 6pm NY, for all monday quarters will be None

    asia: Optional[QuarterLiq]
    london: Optional[QuarterLiq]
    nyam: Optional[QuarterLiq]
    nypm: Optional[QuarterLiq]
    true_do: Optional[PriceDate]  # NY 0:00, for asia that's not completed will be None

    q1_90m: Optional[QuarterLiq]
    q2_90m: Optional[QuarterLiq]
    q3_90m: Optional[QuarterLiq]
    q4_90m: Optional[QuarterLiq]
    true_90m_open: Optional[PriceDate]  # first 90m it will be None

    prev_15m_candle: InnerCandle
    prev_30m_candle: InnerCandle
    current_30m_candle: Optional[InnerCandle]
    prev_1h_candle: InnerCandle
    current_1h_candle: Optional[InnerCandle]
    prev_2h_candle: InnerCandle
    current_2h_candle: Optional[InnerCandle]
    prev_4h_candle: InnerCandle
    current_4h_candle: Optional[InnerCandle]
    prev_1d_candle: InnerCandle
    current_1d_candle: Optional[InnerCandle]
    prev_1w_candle: InnerCandle
    current_1w_candle: Optional[InnerCandle]
    prev_1month_candle: InnerCandle
    current_1month_candle: Optional[InnerCandle]

    prev_year: Optional[QuarterLiq]
    current_year_candle: Optional[InnerCandle]

    def get_15m_candles_range(self, from_: str, to: str) -> List[InnerCandle]:
        if len(self.candles_15m) == 0:
            return []
        first_date, from_date, to_date = to_utc_datetime(self.candles_15m[0][5]), to_utc_datetime(
            from_), to_utc_datetime(to)

        segments_15m = math.floor((to_date - from_date).total_seconds() / (15 * 60))
        if segments_15m < 1:
            return []

        first_index = math.floor((from_date - first_date).total_seconds() / (15 * 60))
        if first_index < 0:
            return []

        return list(self.candles_15m)[first_index:first_index + segments_15m]

    def yq_get(self, yq: YearQuarter) -> Optional[QuarterLiq]:
        match yq:
            case YearQuarter.YQ1:
                return self.year_q1
            case YearQuarter.YQ2:
                return self.year_q2
            case YearQuarter.YQ3:
                return self.year_q3
            case YearQuarter.YQ4:
                return self.year_q4

    def yq_set(self, yq: YearQuarter, ql: Optional[QuarterLiq]):
        match yq:
            case YearQuarter.YQ1:
                self.year_q1 = ql
            case YearQuarter.YQ2:
                self.year_q2 = ql
            case YearQuarter.YQ3:
                self.year_q3 = ql
            case YearQuarter.YQ4:
                self.year_q4 = ql

    def mw_get(self, mw: MonthWeek) -> Optional[QuarterLiq]:
        match mw:
            case MonthWeek.MW1:
                return self.week1
            case MonthWeek.MW2:
                return self.week2
            case MonthWeek.MW3:
                return self.week3
            case MonthWeek.MW4:
                return self.week4
            case MonthWeek.MW5:
                return self.week5

    def mw_set(self, mw: MonthWeek, ql: Optional[QuarterLiq]):
        match mw:
            case MonthWeek.MW1:
                self.week1 = ql
            case MonthWeek.MW2:
                self.week2 = ql
            case MonthWeek.MW3:
                self.week3 = ql
            case MonthWeek.MW4:
                self.week4 = ql
            case MonthWeek.MW5:
                self.week5 = ql

    def wd_get(self, wd: WeekDay) -> Optional[QuarterLiq]:
        match wd:
            case WeekDay.Mon:
                return self.mon
            case WeekDay.Tue:
                return self.tue
            case WeekDay.Wed:
                return self.wed
            case WeekDay.Thu:
                return self.thu
            case WeekDay.MonThu:
                return self.mon_thu
            case WeekDay.Fri:
                return self.fri
            case WeekDay.MonFri:
                return self.mon_fri
            case WeekDay.Sat:
                return self.sat

    def wd_set(self, wd: WeekDay, ql: Optional[QuarterLiq]):
        match wd:
            case WeekDay.Mon:
                self.mon = ql
            case WeekDay.Tue:
                self.tue = ql
            case WeekDay.Wed:
                self.wed = ql
            case WeekDay.Thu:
                self.thu = ql
            case WeekDay.MonThu:
                self.mon_thu = ql
            case WeekDay.Fri:
                self.fri = ql
            case WeekDay.MonFri:
                self.mon_fri = ql
            case WeekDay.Sat:
                self.sat = ql

    def dq_get(self, dq: DayQuarter) -> Optional[QuarterLiq]:
        match dq:
            case DayQuarter.DQ1_Asia:
                return self.asia
            case DayQuarter.DQ2_London:
                return self.london
            case DayQuarter.DQ3_NYAM:
                return self.nyam
            case DayQuarter.DQ4_NYPM:
                return self.nypm

    def dq_set(self, dq: DayQuarter, ql: Optional[QuarterLiq]):
        match dq:
            case DayQuarter.DQ1_Asia:
                self.asia = ql
            case DayQuarter.DQ2_London:
                self.london = ql
            case DayQuarter.DQ3_NYAM:
                self.nyam = ql
            case DayQuarter.DQ4_NYPM:
                self.nypm = ql

    def q90m_get(self, q90m: Quarter90m) -> Optional[QuarterLiq]:
        match q90m:
            case Quarter90m.Q1_90m:
                return self.q1_90m
            case Quarter90m.Q2_90m:
                return self.q2_90m
            case Quarter90m.Q3_90m:
                return self.q3_90m
            case Quarter90m.Q4_90m:
                return self.q4_90m

    def q90m_set(self, q90m: Quarter90m, ql: Optional[QuarterLiq]):
        match q90m:
            case Quarter90m.Q1_90m:
                self.q1_90m = ql
            case Quarter90m.Q2_90m:
                self.q2_90m = ql
            case Quarter90m.Q3_90m:
                self.q3_90m = ql
            case Quarter90m.Q4_90m:
                self.q4_90m = ql

    def plus_15m(self, candle: InnerCandle):
        prev_yq, prev_mw, prev_wd, prev_dq, prev_q90m = quarters_by_time(self.snapshot_date_readable)
        self.candles_15m.append(candle)
        self.prev_15m_candle = candle
        self.snapshot_date_readable = to_date_str(to_utc_datetime(self.prev_15m_candle[5]) + timedelta(minutes=15))
        new_yq, new_mw, new_wd, new_dq, new_q90m = quarters_by_time(self.snapshot_date_readable)

        ranges_yq, tyo = year_quarters_ranges(self.snapshot_date_readable)
        for i in range(len(ranges_yq)):
            prev_yql = self.yq_get(ranges_yq[i][0])
            if ranges_yq[i][1] <= to_utc_datetime(candle[5]) < ranges_yq[i][2]:
                new_high, new_low = candle[1], candle[2]
                if prev_yql:
                    new_high, new_low = max(prev_yql[3][0], candle[1]), min(prev_yql[5][0], candle[2])
                new_half = (new_high + new_low) / 2

                self.yq_set(ranges_yq[i][0], (
                    to_date_str(ranges_yq[i][1]), to_date_str(ranges_yq[i][2]),
                    ranges_yq[i][2] < to_utc_datetime(self.snapshot_date_readable),
                    (new_high, False),
                    (new_half, False),
                    (new_low, False)
                ))
            elif prev_yql:
                self.yq_set(ranges_yq[i][0], (
                    prev_yql[0], prev_yql[1], prev_yql[2],
                    (prev_yql[3][0], prev_yql[3][1] or prev_yql[3][0] < candle[1]),
                    (prev_yql[4][0], prev_yql[4][1] or candle[2] <= prev_yql[4][0] <= candle[1]),
                    (prev_yql[5][0], prev_yql[5][1] or prev_yql[5][0] > candle[2])
                ))

        if prev_yq != new_yq:
            if prev_yq == YearQuarter.YQ1:
                self.year_q4 = None
                self.true_yo = (candle[3], to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15)))
            elif prev_yq == YearQuarter.YQ4:
                self.year_q1 = None
                self.year_q2 = None
                self.year_q3 = None

        ranges_mw, tmo = month_week_quarters_ranges(self.snapshot_date_readable)
        for i in range(len(ranges_mw)):
            prev_mwl = self.mw_get(ranges_mw[i][0])
            if ranges_mw[i][1] <= to_utc_datetime(candle[5]) < ranges_mw[i][2]:
                new_high, new_low = candle[1], candle[2]
                if prev_mwl:
                    new_high, new_low = max(prev_mwl[3][0], candle[1]), min(prev_mwl[5][0], candle[2])
                new_half = (new_high + new_low) / 2

                self.mw_set(ranges_mw[i][0], (
                    to_date_str(ranges_mw[i][1]), to_date_str(ranges_mw[i][2]),
                    ranges_mw[i][2] < to_utc_datetime(self.snapshot_date_readable),
                    (new_high, False),
                    (new_half, False),
                    (new_low, False)
                ))
            elif prev_mwl:
                self.mw_set(ranges_mw[i][0], (
                    prev_mwl[0], prev_mwl[1], prev_mwl[2],
                    (prev_mwl[3][0], prev_mwl[3][1] or prev_mwl[3][0] < candle[1]),
                    (prev_mwl[4][0], prev_mwl[4][1] or candle[2] <= prev_mwl[4][0] <= candle[1]),
                    (prev_mwl[5][0], prev_mwl[5][1] or prev_mwl[5][0] > candle[2])
                ))

        if prev_mw != new_mw:
            if prev_mw == MonthWeek.MW1:
                self.week4 = None
                self.week5 = None
                self.true_mo = (candle[3], to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15)))
            elif prev_mw == MonthWeek.MW4:
                if new_mw != MonthWeek.MW5:
                    self.week1 = None
                    self.week2 = None
                    self.week3 = None
            elif prev_mw == MonthWeek.MW5:
                self.week1 = None
                self.week2 = None
                self.week3 = None
                self.week4 = None

        ranges_wd, two = weekday_ranges(self.snapshot_date_readable)
        for i in range(len(ranges_wd)):
            prev_wdl = self.wd_get(ranges_wd[i][0])
            if ranges_wd[i][1] <= to_utc_datetime(candle[5]) < ranges_wd[i][2]:
                new_high, new_low = candle[1], candle[2]
                if prev_wdl:
                    new_high, new_low = max(prev_wdl[3][0], candle[1]), min(prev_wdl[5][0], candle[2])
                new_half = (new_high + new_low) / 2

                self.wd_set(ranges_wd[i][0], (
                    to_date_str(ranges_wd[i][1]), to_date_str(ranges_wd[i][2]),
                    ranges_wd[i][2] < to_utc_datetime(self.snapshot_date_readable),
                    (new_high, False),
                    (new_half, False),
                    (new_low, False)
                ))
            elif prev_wdl:
                self.wd_set(ranges_wd[i][0], (
                    prev_wdl[0], prev_wdl[1], prev_wdl[2],
                    (prev_wdl[3][0], prev_wdl[3][1] or prev_wdl[3][0] < candle[1]),
                    (prev_wdl[4][0], prev_wdl[4][1] or candle[2] <= prev_wdl[4][0] <= candle[1]),
                    (prev_wdl[5][0], prev_wdl[5][1] or prev_wdl[5][0] > candle[2])
                ))

        if prev_wd != new_wd:
            if prev_wd == WeekDay.Mon:
                self.thu = None
                self.fri = None
                self.true_wo = (candle[3], to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15)))
            elif prev_wd == WeekDay.Fri:
                self.mon = None
                self.tue = None
                self.wed = None
            elif prev_wd == WeekDay.Sun:
                self.mon_thu = None
                self.mon_fri = None
                self.sat = None

        ranges_dq, tdo = day_quarters_ranges(self.snapshot_date_readable)
        for i in range(len(ranges_dq)):
            prev_dql = self.dq_get(ranges_dq[i][0])
            if ranges_dq[i][1] <= to_utc_datetime(candle[5]) < ranges_dq[i][2]:
                new_high, new_low = candle[1], candle[2]
                if prev_dql:
                    new_high, new_low = max(prev_dql[3][0], candle[1]), min(prev_dql[5][0], candle[2])
                new_half = (new_high + new_low) / 2

                self.dq_set(ranges_dq[i][0], (
                    to_date_str(ranges_dq[i][1]), to_date_str(ranges_dq[i][2]),
                    ranges_dq[i][2] < to_utc_datetime(self.snapshot_date_readable),
                    (new_high, False),
                    (new_half, False),
                    (new_low, False)
                ))
            elif prev_dql:
                self.dq_set(ranges_dq[i][0], (
                    prev_dql[0], prev_dql[1], prev_dql[2],
                    (prev_dql[3][0], prev_dql[3][1] or prev_dql[3][0] < candle[1]),
                    (prev_dql[4][0], prev_dql[4][1] or candle[2] <= prev_dql[4][0] <= candle[1]),
                    (prev_dql[5][0], prev_dql[5][1] or prev_dql[5][0] > candle[2])
                ))

        if prev_dq != new_dq:
            if prev_dq == DayQuarter.DQ1_Asia:
                self.nypm = None
                self.true_do = (candle[3], to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15)))
            elif prev_dq == DayQuarter.DQ4_NYPM:
                self.asia = None
                self.london = None
                self.nyam = None

        ranges_q90m, true_90m_open = quarters90m_ranges(self.snapshot_date_readable)
        for i in range(len(ranges_q90m)):
            prev_q90ml = self.q90m_get(ranges_q90m[i][0])
            if ranges_q90m[i][1] <= to_utc_datetime(candle[5]) < ranges_q90m[i][2]:
                new_high, new_low = candle[1], candle[2]
                if prev_q90ml:
                    new_high, new_low = max(prev_q90ml[3][0], candle[1]), min(prev_q90ml[5][0], candle[2])
                new_half = (new_high + new_low) / 2

                self.q90m_set(ranges_q90m[i][0], (
                    to_date_str(ranges_q90m[i][1]), to_date_str(ranges_q90m[i][2]),
                    ranges_q90m[i][2] < to_utc_datetime(self.snapshot_date_readable),
                    (new_high, False),
                    (new_half, False),
                    (new_low, False)
                ))
            elif prev_q90ml:
                self.q90m_set(ranges_q90m[i][0], (
                    prev_q90ml[0], prev_q90ml[1], prev_q90ml[2],
                    (prev_q90ml[3][0], prev_q90ml[3][1] or prev_q90ml[3][0] < candle[1]),
                    (prev_q90ml[4][0], prev_q90ml[4][1] or candle[2] <= prev_q90ml[4][0] <= candle[1]),
                    (prev_q90ml[5][0], prev_q90ml[5][1] or prev_q90ml[5][0] > candle[2])
                ))

        if prev_q90m != new_q90m:
            if prev_q90m == Quarter90m.Q1_90m:
                self.q4_90m = None
                self.true_90m_open = (candle[3], to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15)))
            elif prev_q90m == Quarter90m.Q4_90m:
                self.q1_90m = None
                self.q2_90m = None
                self.q3_90m = None

        if candle[5] == true_90m_open:
            self.true_90m_open = (candle[0], true_90m_open)
        if candle[5] == tdo:
            self.true_do = (candle[0], tdo)
        if candle[5] == two:
            self.true_wo = (candle[0], two)
        if candle[5] == tmo:
            self.true_mo = (candle[0], tmo)
        if candle[5] == tyo:
            self.true_yo = (candle[0], tyo)

        prev_30m_from, prev_30m_to = get_prev_30m_from_to(self.snapshot_date_readable)
        current_30m_from, current_30m_to = get_current_30m_from_to(self.snapshot_date_readable)
        prev_1h_from, prev_1h_to = get_prev_1h_from_to(self.snapshot_date_readable)
        current_1h_from, current_1h_to = get_current_1h_from_to(self.snapshot_date_readable)
        prev_2h_from, prev_2h_to = get_prev_2h_from_to(self.snapshot_date_readable)
        current_2h_from, current_2h_to = get_current_2h_from_to(self.snapshot_date_readable)
        prev_4h_from, prev_4h_to = get_prev_4h_from_to(self.snapshot_date_readable)
        current_4h_from, current_4h_to = get_current_4h_from_to(self.snapshot_date_readable)
        prev_1d_from, prev_1d_to = get_prev_1d_from_to(self.snapshot_date_readable)
        current_1d_from, current_1d_to = get_current_1d_from_to(self.snapshot_date_readable)
        prev_1w_from, prev_1w_to = get_prev_1w_from_to(self.snapshot_date_readable)
        current_1w_from, current_1w_to = get_current_1w_from_to(self.snapshot_date_readable)
        prev_1month_from, prev_1month_to = get_prev_1month_from_to(self.snapshot_date_readable)
        current_1month_from, current_1month_to = get_current_1month_from_to(self.snapshot_date_readable)

        prev_year_from, prev_year_to = prev_year_ranges(self.snapshot_date_readable)
        current_year_from, current_year_to = current_year_ranges(self.snapshot_date_readable)

        if current_30m_from <= to_utc_datetime(candle[5]) < current_30m_to:
            if not self.current_30m_candle:
                self.current_30m_candle = candle
            else:
                self.current_30m_candle = as_1_candle([self.current_30m_candle, candle])
        elif prev_30m_from <= to_utc_datetime(candle[5]) < prev_30m_to:
            if to_utc_datetime(self.current_30m_candle[5]) != current_30m_from:
                self.prev_30m_candle = as_1_candle([self.current_30m_candle, candle])
                self.current_30m_candle = None

        if current_1h_from <= to_utc_datetime(candle[5]) < current_1h_to:
            if not self.current_1h_candle:
                self.current_1h_candle = candle
            else:
                self.current_1h_candle = as_1_candle([self.current_1h_candle, candle])
        elif prev_1h_from <= to_utc_datetime(candle[5]) < prev_1h_to:
            if to_utc_datetime(self.current_1h_candle[5]) != current_1h_from:
                self.prev_1h_candle = as_1_candle([self.current_1h_candle, candle])
                self.current_1h_candle = None

        if current_2h_from <= to_utc_datetime(candle[5]) < current_2h_to:
            if not self.current_2h_candle:
                self.current_2h_candle = candle
            else:
                self.current_2h_candle = as_1_candle([self.current_2h_candle, candle])
        elif prev_2h_from <= to_utc_datetime(candle[5]) < prev_2h_to:
            if to_utc_datetime(self.current_2h_candle[5]) != current_2h_from:
                self.prev_2h_candle = as_1_candle([self.current_2h_candle, candle])
                self.current_2h_candle = None

        if current_4h_from <= to_utc_datetime(candle[5]) < current_4h_to:
            if not self.current_4h_candle:
                self.current_4h_candle = candle
            else:
                self.current_4h_candle = as_1_candle([self.current_4h_candle, candle])
        elif prev_4h_from <= to_utc_datetime(candle[5]) < prev_4h_to:
            if to_utc_datetime(self.current_4h_candle[5]) != current_4h_from:
                self.prev_4h_candle = as_1_candle([self.current_4h_candle, candle])
                self.current_4h_candle = None

        if current_1d_from <= to_utc_datetime(candle[5]) < current_1d_to:
            if not self.current_1d_candle:
                self.current_1d_candle = candle
            else:
                self.current_1d_candle = as_1_candle([self.current_1d_candle, candle])
        elif prev_1d_from <= to_utc_datetime(candle[5]) < prev_1d_to:
            if to_utc_datetime(self.current_1d_candle[5]) != current_1d_from:
                self.prev_1d_candle = as_1_candle([self.current_1d_candle, candle])
                self.current_1d_candle = None

        if current_1w_from <= to_utc_datetime(candle[5]) < current_1w_to:
            if not self.current_1w_candle:
                self.current_1w_candle = candle
            else:
                self.current_1w_candle = as_1_candle([self.current_1w_candle, candle])
        elif prev_1w_from <= to_utc_datetime(candle[5]) < prev_1w_to:
            if to_utc_datetime(self.current_1w_candle[5]) != current_1w_from:
                self.prev_1w_candle = as_1_candle([self.current_1w_candle, candle])
                self.current_1w_candle = None

        if current_1month_from <= to_utc_datetime(candle[5]) < current_1month_to:
            if not self.current_1month_candle:
                self.current_1month_candle = candle
            else:
                self.current_1month_candle = as_1_candle([self.current_1month_candle, candle])
        elif prev_1month_from <= to_utc_datetime(candle[5]) < prev_1month_to:
            if to_utc_datetime(self.current_1month_candle[5]) != current_1month_from:
                self.prev_1month_candle = as_1_candle([self.current_1month_candle, candle])
                self.current_1month_candle = None

        if current_year_from <= to_utc_datetime(candle[5]) < current_year_to:
            if not self.current_year_candle:
                self.current_year_candle = candle
            else:
                self.current_year_candle = as_1_candle([self.current_year_candle, candle])

            self.prev_year = (
                self.prev_year[0], self.prev_year[1], self.prev_year[2],
                (self.prev_year[3][0], self.prev_year[3][1] or self.prev_year[3][0] < candle[1]),
                (self.prev_year[4][0], self.prev_year[4][1] or candle[2] <= self.prev_year[4][0] <= candle[1]),
                (self.prev_year[5][0], self.prev_year[5][1] or self.prev_year[5][0] > candle[2])
            )
        elif prev_year_from <= to_utc_datetime(candle[5]) < prev_year_to:
            if to_utc_datetime(self.current_year_candle[5]) != current_year_from:
                current_year = as_1_candle([self.current_year_candle, candle])
                self.prev_year = (
                    to_date_str(prev_year_from), to_date_str(prev_year_to), True,
                    (current_year[1], False),
                    ((current_year[1] + current_year[2]) / 2, False),
                    (current_year[2], False)
                )
                self.current_year_candle = None

    def populate(self, reverse_15m_gen: Reverse15mGenerator):
        self.prev_15m_candle = next(reverse_15m_gen)
        self.candles_15m.appendleft(self.prev_15m_candle)

        highest_sweep = self.prev_15m_candle[1]
        lowest_sweep = self.prev_15m_candle[2]

        self.snapshot_date_readable = to_date_str(to_utc_datetime(self.prev_15m_candle[5]) + timedelta(minutes=15))

        prev_30m_from, prev_30m_to = get_prev_30m_from_to(self.snapshot_date_readable)
        current_30m_from, current_30m_to = get_current_30m_from_to(self.snapshot_date_readable)
        prev_1h_from, prev_1h_to = get_prev_1h_from_to(self.snapshot_date_readable)
        current_1h_from, current_1h_to = get_current_1h_from_to(self.snapshot_date_readable)
        prev_2h_from, prev_2h_to = get_prev_2h_from_to(self.snapshot_date_readable)
        current_2h_from, current_2h_to = get_current_2h_from_to(self.snapshot_date_readable)
        prev_4h_from, prev_4h_to = get_prev_4h_from_to(self.snapshot_date_readable)
        current_4h_from, current_4h_to = get_current_4h_from_to(self.snapshot_date_readable)
        prev_1d_from, prev_1d_to = get_prev_1d_from_to(self.snapshot_date_readable)
        current_1d_from, current_1d_to = get_current_1d_from_to(self.snapshot_date_readable)
        prev_1w_from, prev_1w_to = get_prev_1w_from_to(self.snapshot_date_readable)
        current_1w_from, current_1w_to = get_current_1w_from_to(self.snapshot_date_readable)
        prev_1month_from, prev_1month_to = get_prev_1month_from_to(self.snapshot_date_readable)
        current_1month_from, current_1month_to = get_current_1month_from_to(self.snapshot_date_readable)

        prev_year_from, prev_year_to = prev_year_ranges(self.snapshot_date_readable)
        current_year_from, current_year_to = current_year_ranges(self.snapshot_date_readable)

        ranges_90m, t90m0 = quarters90m_ranges(self.snapshot_date_readable)
        sweeps_90m = []
        cum_90m_quarters = []
        for rng_90m in ranges_90m:
            if rng_90m[1] <= to_utc_datetime(self.prev_15m_candle[5]) < rng_90m[2]:
                sweeps_90m.append(None)
                cum_90m_quarters.append(self.prev_15m_candle)
            else:
                cum_90m_quarters.append(None)
                sweeps_90m.append((highest_sweep, lowest_sweep))

        ranges_dq, tdo = day_quarters_ranges(self.snapshot_date_readable)
        sweeps_dq = []
        cum_dq_quarters = []
        for rng_dq in ranges_dq:
            if rng_dq[1] <= to_utc_datetime(self.prev_15m_candle[5]) < rng_dq[2]:
                sweeps_dq.append(None)
                cum_dq_quarters.append(self.prev_15m_candle)
            else:
                cum_dq_quarters.append(None)
                sweeps_dq.append((highest_sweep, lowest_sweep))

        ranges_wd, two = weekday_ranges(self.snapshot_date_readable)
        sweeps_wd = []
        cum_wd_quarters = []
        for rng_wd in ranges_wd:
            if rng_wd[1] <= to_utc_datetime(self.prev_15m_candle[5]) < rng_wd[2]:
                sweeps_wd.append(None)
                cum_wd_quarters.append(self.prev_15m_candle)
            else:
                cum_wd_quarters.append(None)
                sweeps_wd.append((highest_sweep, lowest_sweep))

        ranges_mw, tmo = month_week_quarters_ranges(self.snapshot_date_readable)
        sweeps_mw = []
        cum_mw_quarters = []
        for rng_mw in ranges_mw:
            if rng_mw[1] <= to_utc_datetime(self.prev_15m_candle[5]) < rng_mw[2]:
                sweeps_mw.append(None)
                cum_mw_quarters.append(self.prev_15m_candle)
            else:
                cum_mw_quarters.append(None)
                sweeps_mw.append((highest_sweep, lowest_sweep))

        ranges_yq, tyo = year_quarters_ranges(self.snapshot_date_readable)
        sweeps_yq = []
        cum_yq_quarters = []
        for rng_yq in ranges_yq:
            if rng_yq[1] <= to_utc_datetime(self.prev_15m_candle[5]) < rng_yq[2]:
                sweeps_yq.append(None)
                cum_yq_quarters.append(self.prev_15m_candle)
            else:
                cum_yq_quarters.append(None)
                sweeps_yq.append((highest_sweep, lowest_sweep))

        self.prev_30m_candle = self.prev_15m_candle if prev_30m_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_30m_to else None
        self.current_30m_candle = self.prev_15m_candle if current_30m_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_30m_to else None

        self.prev_1h_candle = self.prev_15m_candle if prev_1h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_1h_to else None
        self.current_1h_candle = self.prev_15m_candle if current_1h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_1h_to else None

        self.prev_2h_candle = self.prev_15m_candle if prev_2h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_2h_to else None
        self.current_2h_candle = self.prev_15m_candle if current_2h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_2h_to else None

        self.prev_4h_candle = self.prev_15m_candle if prev_4h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_4h_to else None
        self.current_4h_candle = self.prev_15m_candle if current_4h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_4h_to else None

        self.prev_1d_candle = self.prev_15m_candle if prev_1d_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_1d_to else None
        self.current_1d_candle = self.prev_15m_candle if current_1d_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_1d_to else None

        self.prev_1w_candle = self.prev_15m_candle if prev_1w_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_1w_to else None
        self.current_1w_candle = self.prev_15m_candle if current_1w_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_1w_to else None

        self.prev_1month_candle = self.prev_15m_candle if prev_1month_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_1month_to else None
        self.current_1month_candle = self.prev_15m_candle if current_1month_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_1month_to else None

        if prev_year_from <= to_utc_datetime(self.prev_15m_candle[5]) < prev_year_to:
            prev_year_sweeps = None
            cum_prev_year = self.prev_15m_candle
        else:
            prev_year_sweeps = (highest_sweep, lowest_sweep)
            cum_prev_year = None

        self.current_year_candle = self.prev_15m_candle if current_year_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_year_to else None

        while True:
            prev_candle = next(reverse_15m_gen)
            self.candles_15m.appendleft(prev_candle)
            if len(self.candles_15m) % (4 * 24 * 90) == 0:
                print(f"populated {len(self.candles_15m) / (4 * 24 * 90)} days for {self.symbol}")
            pc_date = to_utc_datetime(prev_candle[5])

            if prev_candle[5] == t90m0:
                self.true_90m_open = (prev_candle[0], t90m0)
            if prev_candle[5] == tdo:
                self.true_do = (prev_candle[0], tdo)
            if prev_candle[5] == two:
                self.true_wo = (prev_candle[0], two)
            if prev_candle[5] == tmo:
                self.true_mo = (prev_candle[0], tmo)
            if prev_candle[5] == tyo:
                self.true_yo = (prev_candle[0], tyo)

            if pc_date >= ranges_90m[0][1]:
                for i in range(len(ranges_90m)):
                    if pc_date > ranges_90m[i][2]:
                        sweeps_90m[i] = (highest_sweep, lowest_sweep)
                    if ranges_90m[i][1] <= pc_date < ranges_90m[i][2]:
                        cum_90m_quarters[i] = prev_candle if not cum_90m_quarters[i] else as_1_candle(
                            [prev_candle, cum_90m_quarters[i]])
                        if ranges_90m[i][1] == pc_date:
                            half = (cum_90m_quarters[i][1] + cum_90m_quarters[i][2]) / 2
                            self.q90m_set(ranges_90m[i][0], (
                                cum_90m_quarters[i][5], to_date_str(ranges_90m[i][2]),
                                ranges_90m[i][2] < to_utc_datetime(self.snapshot_date_readable),
                                (cum_90m_quarters[i][1],
                                 False if not sweeps_90m[i] else cum_90m_quarters[i][1] < sweeps_90m[i][0]),
                                (half, False if not sweeps_90m[i] else sweeps_90m[i][1] <= half <= sweeps_90m[i][0]),
                                (cum_90m_quarters[i][2],
                                 False if not sweeps_90m[i] else cum_90m_quarters[i][2] > sweeps_90m[i][1])
                            ))

            if pc_date >= ranges_dq[0][1]:
                for i in range(len(ranges_dq)):
                    if pc_date > ranges_dq[i][2]:
                        sweeps_dq[i] = (highest_sweep, lowest_sweep)
                    if ranges_dq[i][1] <= pc_date < ranges_dq[i][2]:
                        cum_dq_quarters[i] = prev_candle if not cum_dq_quarters[i] else as_1_candle(
                            [prev_candle, cum_dq_quarters[i]])
                        if ranges_dq[i][1] == pc_date:
                            half = (cum_dq_quarters[i][1] + cum_dq_quarters[i][2]) / 2
                            self.dq_set(ranges_dq[i][0], (
                                cum_dq_quarters[i][5], to_date_str(ranges_dq[i][2]),
                                ranges_dq[i][2] < to_utc_datetime(self.snapshot_date_readable),
                                (cum_dq_quarters[i][1],
                                 False if not sweeps_dq[i] else cum_dq_quarters[i][1] < sweeps_dq[i][0]),
                                (half, False if not sweeps_dq[i] else sweeps_dq[i][1] <= half <= sweeps_dq[i][0]),
                                (cum_dq_quarters[i][2],
                                 False if not sweeps_dq[i] else cum_dq_quarters[i][2] > sweeps_dq[i][1])
                            ))

            if pc_date >= ranges_wd[0][1]:
                for i in range(len(ranges_wd)):
                    if pc_date > ranges_wd[i][2]:
                        sweeps_wd[i] = (highest_sweep, lowest_sweep)
                    if ranges_wd[i][1] <= pc_date < ranges_wd[i][2]:
                        cum_wd_quarters[i] = prev_candle if not cum_wd_quarters[i] else as_1_candle(
                            [prev_candle, cum_wd_quarters[i]])
                        if ranges_wd[i][1] == pc_date:
                            half = (cum_wd_quarters[i][1] + cum_wd_quarters[i][2]) / 2
                            self.wd_set(ranges_wd[i][0], (
                                cum_wd_quarters[i][5], to_date_str(ranges_wd[i][2]),
                                ranges_wd[i][2] < to_utc_datetime(self.snapshot_date_readable),
                                (cum_wd_quarters[i][1],
                                 False if not sweeps_wd[i] else cum_wd_quarters[i][1] < sweeps_wd[i][0]),
                                (half, False if not sweeps_wd[i] else sweeps_wd[i][1] <= half <= sweeps_wd[i][0]),
                                (cum_wd_quarters[i][2],
                                 False if not sweeps_wd[i] else cum_wd_quarters[i][2] > sweeps_wd[i][1])
                            ))

            if pc_date >= ranges_mw[0][1]:
                for i in range(len(ranges_mw)):
                    if pc_date > ranges_mw[i][2]:
                        sweeps_mw[i] = (highest_sweep, lowest_sweep)
                    if ranges_mw[i][1] <= pc_date < ranges_mw[i][2]:
                        cum_mw_quarters[i] = prev_candle if not cum_mw_quarters[i] else as_1_candle(
                            [prev_candle, cum_mw_quarters[i]])
                        if ranges_mw[i][1] == pc_date:
                            half = (cum_mw_quarters[i][1] + cum_mw_quarters[i][2]) / 2
                            self.mw_set(ranges_mw[i][0], (
                                cum_mw_quarters[i][5], to_date_str(ranges_mw[i][2]),
                                ranges_mw[i][2] < to_utc_datetime(self.snapshot_date_readable),
                                (cum_mw_quarters[i][1],
                                 False if not sweeps_mw[i] else cum_mw_quarters[i][1] < sweeps_mw[i][0]),
                                (half, False if not sweeps_mw[i] else sweeps_mw[i][1] <= half <= sweeps_mw[i][0]),
                                (cum_mw_quarters[i][2],
                                 False if not sweeps_mw[i] else cum_mw_quarters[i][2] > sweeps_mw[i][1])
                            ))

            if pc_date >= ranges_yq[0][1]:
                for i in range(len(ranges_yq)):
                    if pc_date > ranges_yq[i][2]:
                        sweeps_yq[i] = (highest_sweep, lowest_sweep)
                    if ranges_yq[i][1] <= pc_date < ranges_yq[i][2]:
                        cum_yq_quarters[i] = prev_candle if not cum_yq_quarters[i] else as_1_candle(
                            [prev_candle, cum_yq_quarters[i]])
                        if ranges_yq[i][1] == pc_date:
                            half = (cum_yq_quarters[i][1] + cum_yq_quarters[i][2]) / 2
                            self.yq_set(ranges_yq[i][0], (
                                cum_yq_quarters[i][5], to_date_str(ranges_yq[i][2]),
                                ranges_yq[i][2] < to_utc_datetime(self.snapshot_date_readable),
                                (cum_yq_quarters[i][1],
                                 False if not sweeps_yq[i] else cum_yq_quarters[i][1] < sweeps_yq[i][0]),
                                (half, False if not sweeps_yq[i] else sweeps_yq[i][1] <= half <= sweeps_yq[i][0]),
                                (cum_yq_quarters[i][2],
                                 False if not sweeps_yq[i] else cum_yq_quarters[i][2] > sweeps_yq[i][1])
                            ))

            if pc_date >= prev_1month_from:
                if prev_30m_from <= pc_date < prev_30m_to:
                    self.prev_30m_candle = prev_candle if not self.prev_30m_candle else as_1_candle(
                        [prev_candle, self.prev_30m_candle])
                if current_30m_from <= pc_date < current_30m_to:
                    self.current_30m_candle = prev_candle if not self.current_30m_candle else as_1_candle(
                        [prev_candle, self.current_30m_candle])

                if prev_1h_from <= pc_date < prev_1h_to:
                    self.prev_1h_candle = prev_candle if not self.prev_1h_candle else as_1_candle(
                        [prev_candle, self.prev_1h_candle])
                if current_1h_from <= pc_date < current_1h_to:
                    self.current_1h_candle = prev_candle if not self.current_1h_candle else as_1_candle(
                        [prev_candle, self.current_1h_candle])

                if prev_2h_from <= pc_date < prev_2h_to:
                    self.prev_2h_candle = prev_candle if not self.prev_2h_candle else as_1_candle(
                        [prev_candle, self.prev_2h_candle])
                if current_2h_from <= pc_date < current_2h_to:
                    self.current_2h_candle = prev_candle if not self.current_2h_candle else as_1_candle(
                        [prev_candle, self.current_2h_candle])

                if prev_4h_from <= pc_date < prev_4h_to:
                    self.prev_4h_candle = prev_candle if not self.prev_4h_candle else as_1_candle(
                        [prev_candle, self.prev_4h_candle])
                if current_4h_from <= pc_date < current_4h_to:
                    self.current_4h_candle = prev_candle if not self.current_4h_candle else as_1_candle(
                        [prev_candle, self.current_4h_candle])

                if prev_1d_from <= pc_date < prev_1d_to:
                    self.prev_1d_candle = prev_candle if not self.prev_1d_candle else as_1_candle(
                        [prev_candle, self.prev_1d_candle])
                if current_1d_from <= pc_date < current_1d_to:
                    self.current_1d_candle = prev_candle if not self.current_1d_candle else as_1_candle(
                        [prev_candle, self.current_1d_candle])

                if prev_1w_from <= pc_date < prev_1w_to:
                    self.prev_1w_candle = prev_candle if not self.prev_1w_candle else as_1_candle(
                        [prev_candle, self.prev_1w_candle])
                if current_1w_from <= pc_date < current_1w_to:
                    self.current_1w_candle = prev_candle if not self.current_1w_candle else as_1_candle(
                        [prev_candle, self.current_1w_candle])

                if prev_1month_from <= to_utc_datetime(
                        prev_candle[5]) < prev_1month_to:
                    self.prev_1month_candle = prev_candle if not self.prev_1month_candle else as_1_candle(
                        [prev_candle, self.prev_1month_candle])
                if current_1month_from <= to_utc_datetime(
                        prev_candle[5]) < current_1month_to:
                    self.current_1month_candle = prev_candle if not self.current_1month_candle else as_1_candle(
                        [prev_candle, self.current_1month_candle])

            if current_year_from <= to_utc_datetime(
                    prev_candle[5]) < current_year_to:
                self.current_year_candle = prev_candle if not self.current_year_candle else as_1_candle(
                    [prev_candle, self.current_year_candle])

            if pc_date > prev_year_to:
                prev_year_sweeps = (highest_sweep, lowest_sweep)
            if prev_year_from <= pc_date < prev_year_to:
                cum_prev_year = prev_candle if not cum_prev_year else as_1_candle([prev_candle, cum_prev_year])

                if prev_year_from == pc_date:
                    half = (cum_prev_year[1] + cum_prev_year[2]) / 2
                    self.prev_year = (
                        to_date_str(prev_year_from), to_date_str(prev_year_to),
                        prev_year_to < to_utc_datetime(self.snapshot_date_readable),
                        (cum_prev_year[1], False if not prev_year_sweeps else cum_prev_year[1] < prev_year_sweeps[0]),
                        (half, False if not prev_year_sweeps else prev_year_sweeps[1] <= half <= prev_year_sweeps[0]),
                        (cum_prev_year[2], False if not prev_year_sweeps else cum_prev_year[2] > prev_year_sweeps[1])
                    )

            highest_sweep = max(prev_candle[1], highest_sweep)
            lowest_sweep = min(prev_candle[2], lowest_sweep)

            if pc_date <= prev_year_from:
                break


def new_empty_asset(symbol: str) -> Asset:
    return Asset(
        symbol=symbol,
        snapshot_date_readable="",
        candles_15m=deque(),
        prev_year=None,
        year_q4=None,
        year_q1=None,
        year_q2=None,
        year_q3=None,
        true_yo=None,
        week1=None,
        week2=None,
        week3=None,
        week4=None,
        week5=None,
        true_mo=None,
        nwog=None,
        mon=None,
        tue=None,
        wed=None,
        thu=None,
        mon_thu=None,
        fri=None,
        mon_fri=None,
        sat=None,
        true_wo=None,
        nypm=None,
        asia=None,
        london=None,
        nyam=None,
        true_do=None,
        q4_90m=None,
        q1_90m=None,
        q2_90m=None,
        q3_90m=None,
        true_90m_open=None,
        prev_15m_candle=(-1, -1, -1, -1, -1, ""),
        prev_30m_candle=(-1, -1, -1, -1, -1, ""),
        current_30m_candle=None,
        prev_1h_candle=(-1, -1, -1, -1, -1, ""),
        current_1h_candle=None,
        prev_2h_candle=(-1, -1, -1, -1, -1, ""),
        current_2h_candle=None,
        prev_4h_candle=(-1, -1, -1, -1, -1, ""),
        current_4h_candle=None,
        prev_1d_candle=(-1, -1, -1, -1, -1, ""),
        current_1d_candle=None,
        prev_1w_candle=(-1, -1, -1, -1, -1, ""),
        current_1w_candle=None,
        prev_1month_candle=(-1, -1, -1, -1, -1, ""),
        current_1month_candle=None,
        current_year_candle=None,
    )


@dataclass
class PSP:
    a1_candle: InnerCandle
    a2_candle: InnerCandle
    a3_candle: InnerCandle

    confirmed: bool  # next candle didn't sweep it
    swept_from_to: Optional[List[Tuple[str, str, str]]]  # symbol, from, to_date


@dataclass
class SMT:
    a1q: QuarterLiq
    a2q: QuarterLiq
    a3q: QuarterLiq

    type: str  # 'high' 'low' 'half_high' or 'half_low'

    a1_sweep_candles_15m: List[InnerCandle]
    a2_sweep_candles_15m: List[InnerCandle]
    a3_sweep_candles_15m: List[InnerCandle]

    psps_15m: Optional[List[PSP]]
    psps_30m: Optional[List[PSP]]
    psps_1h: Optional[List[PSP]]
    psps_2h: Optional[List[PSP]]
    psps_4h: Optional[List[PSP]]
    psps_1d: Optional[List[PSP]]
    psps_1_week: Optional[List[PSP]]
    psps_1_month: Optional[List[PSP]]


def new_empty_smt(a1q, a2q, a3q) -> SMT:
    return SMT(
        a1q=a1q,
        a2q=a2q,
        a3q=a3q,
        type='',
        a1_sweep_candles_15m=[],
        a2_sweep_candles_15m=[],
        a3_sweep_candles_15m=[],
        psps_15m=None,
        psps_30m=None,
        psps_1h=None,
        psps_2h=None,
        psps_4h=None,
        psps_1d=None,
        psps_1_week=None,
        psps_1_month=None,
    )


@dataclass
class Triad:
    a1: Asset
    a2: Asset
    a3: Asset

    def new_smt(
            self, next_tick: str, a1_ql, a2_aq, a3_ql: QuarterLiq
    ) -> Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]:
        _, _, _, a1_high, a1_half, a1_low = a1_ql
        _, _, _, a2_high, a2_half, a2_low = a2_aq
        _, _, _, a3_high, a3_half, a3_low = a3_ql

        next_candle_end = to_date_str(to_utc_datetime(next_tick) + timedelta(minutes=15))

        next_candles_a1 = self.a1.get_15m_candles_range(next_tick, next_candle_end)
        next_candles_a2 = self.a2.get_15m_candles_range(next_tick, next_candle_end)
        next_candles_a3 = self.a3.get_15m_candles_range(next_tick, next_candle_end)
        a1_q_close = None if len(next_candles_a1) == 0 else next_candles_a1[0][0]
        a2_q_close = None if len(next_candles_a2) == 0 else next_candles_a2[0][0]
        a3_q_close = None if len(next_candles_a3) == 0 else next_candles_a3[0][0]

        is_high = len([x for x in [a1_high[1], a2_high[1], a3_high[1]] if x]) not in [0, 3]
        high_smt = new_empty_smt(a1_ql, a2_aq, a3_ql) if is_high else None
        if high_smt:
            high_smt.type = 'high'

        is_low = len([x for x in [a1_low[1], a2_low[1], a3_low[1]] if x]) not in [0, 3]
        low_smt = new_empty_smt(a1_ql, a2_aq, a3_ql) if is_low else None
        if low_smt:
            low_smt.type = 'low'

        is_half_swept = len([x for x in [a1_half[1], a2_half[1], a3_half[1]] if x]) not in [0, 3]
        is_half_low = a1_q_close and (a1_low[0] <= a1_q_close < a1_half[0] and
                                      a2_low[0] <= a2_q_close < a2_half[0] and
                                      a3_low[0] <= a3_q_close < a3_half[0])
        is_half_high = a1_q_close and (a1_half[0] < a1_q_close <= a1_high[0] and
                                       a2_half[0] < a2_q_close <= a2_high[0] and
                                       a3_half[0] < a3_q_close <= a3_high[0])

        half_smt = new_empty_smt(self.a1.year_q1, self.a2.year_q1, self.a3.year_q1) \
            if is_half_swept and (is_half_low or is_half_high) else None
        if half_smt and is_half_high:
            half_smt.type = 'half_high'
        elif half_smt and is_half_low:
            half_smt.type = 'half_low'

        return high_smt, half_smt, low_smt

    def calculate_psps(
            self,
            next_tick: str,
            prev_candle_range: Tuple[datetime, datetime],
            current_candle_range_getter: GetDateRange,
            as_candles: AsCandles,
            smt: SMT
    ) -> List[PSP]:
        psps = []

        if len(smt.a1_sweep_candles_15m) == 0:
            smt.a1_sweep_candles_15m = self.a1.get_15m_candles_range(next_tick, self.a1.snapshot_date_readable)
            smt.a2_sweep_candles_15m = self.a2.get_15m_candles_range(next_tick, self.a2.snapshot_date_readable)
            smt.a3_sweep_candles_15m = self.a3.get_15m_candles_range(next_tick, self.a3.snapshot_date_readable)

        # prev_candle_range = get_prev_1d_from_to(next_tick)
        a1_prev_candle = as_1_candle(self.a1.get_15m_candles_range(
            to_date_str(prev_candle_range[0]), to_date_str(prev_candle_range[1])
        ))
        a2_prev_candle = as_1_candle(self.a2.get_15m_candles_range(
            to_date_str(prev_candle_range[0]), to_date_str(prev_candle_range[1])
        ))
        a3_prev_candle = as_1_candle(self.a3.get_15m_candles_range(
            to_date_str(prev_candle_range[0]), to_date_str(prev_candle_range[1])
        ))

        a1_candles = as_candles(smt.a1_sweep_candles_15m)
        a2_candles = as_candles(smt.a2_sweep_candles_15m)
        a3_candles = as_candles(smt.a3_sweep_candles_15m)

        a1_min, a1_max = a1_candles[0][2], a1_candles[0][1]
        a2_min, a2_max = a2_candles[0][2], a2_candles[0][1]
        a3_min, a3_max = a3_candles[0][2], a3_candles[0][1]

        for i in range(len(a1_candles)):
            for j in range(len(psps)):
                c_end = current_candle_range_getter(a1_candles[i][5])[1]
                snap_end = to_utc_datetime(self.a1.snapshot_date_readable) - timedelta(seconds=1)
                if not psps[j].swept_from_to:
                    if smt.type in ['high', 'half_high']:
                        if psps[j].a1_candle[1] < a1_candles[i][1] \
                                or psps[j].a2_candle[1] < a2_candles[i][1] \
                                or psps[j].a3_candle[1] < a3_candles[i][1]:
                            psps[j].swept_from_to = (
                                a1_candles[i][5],
                                to_date_str(min(c_end, snap_end))
                            )
                    elif smt.type in ['low', 'half_low']:
                        if psps[j].a1_candle[2] > a1_candles[i][2] \
                                or psps[j].a2_candle[2] > a2_candles[i][2] \
                                or psps[j].a3_candle[2] > a3_candles[i][2]:
                            psps[j].swept_from_to = (
                                a1_candles[i][5],
                                to_date_str(min(c_end, snap_end))
                            )
            try:
                # check if different colors
                if len([x for x in [a1_candles[i], a2_candles[i], a3_candles[i]] if x[0] > x[3]]) in [0, 3]:
                    continue

                # check if this candle sweeps
                if smt.type == 'high':
                    if a1_candles[i][1] < smt.a1q[3][0] \
                            and a2_candles[i][1] < smt.a2q[3][0] \
                            and a3_candles[i][1] < smt.a3q[3][0]:
                        continue

                if smt.type == 'low':
                    if a1_candles[i][2] > smt.a1q[5][0] \
                            and a2_candles[i][2] > smt.a2q[5][0] \
                            and a3_candles[i][2] > smt.a3q[5][0]:
                        continue

                if smt.type == 'half_high':
                    if a1_candles[i][1] < smt.a1q[4][0] \
                            and a2_candles[i][1] < smt.a2q[4][0] \
                            and a3_candles[i][1] < smt.a3q[4][0]:
                        continue

                if smt.type == 'half_low':
                    if a1_candles[i][2] > smt.a1q[4][0] \
                            and a2_candles[i][2] > smt.a2q[4][0] \
                            and a3_candles[i][2] > smt.a3q[4][0]:
                        continue

                # check if swing
                if i == 0:
                    if smt.type in ['high', 'half_high']:
                        if a1_candles[i][1] < a1_prev_candle[1] \
                                and a2_candles[i][1] < a2_prev_candle[1] \
                                and a3_candles[i][1] < a3_prev_candle[1]:
                            continue
                    elif smt.type in ['low', 'half_low']:
                        if a1_candles[i][2] > a1_prev_candle[2] \
                                and a2_candles[i][2] > a2_prev_candle[2] \
                                and a3_candles[i][2] > a3_prev_candle[2]:
                            continue
                else:
                    if smt.type in ['high', 'half_high']:
                        if a1_candles[i][1] <= a1_max \
                                and a2_candles[i][1] <= a2_max \
                                and a3_candles[i][1] <= a3_max:
                            continue
                    elif smt.type in ['low', 'half_low']:
                        if a1_candles[i][2] >= a1_min \
                                and a2_candles[i][2] >= a2_min \
                                and a3_candles[i][2] >= a3_min:
                            continue

                confirmed = False
                if i != len(a1_candles) - 1:
                    next_c_end = current_candle_range_getter(a1_candles[i + 1][5])[1]
                    next_c_ended = next_c_end < to_utc_datetime(self.a1.snapshot_date_readable)
                    if next_c_ended:
                        if smt.type in ['high', 'half_high']:
                            if a1_candles[i][1] > a1_candles[i + 1][1] \
                                    or a2_candles[i][1] > a2_candles[i + 1][1] \
                                    or a3_candles[i][1] > a3_candles[i + 1][1]:
                                confirmed = True
                        elif smt.type in ['low', 'half_low']:
                            if a1_candles[i][2] < a1_candles[i + 1][2] \
                                    or a2_candles[i][2] < a2_candles[i + 1][2] \
                                    or a3_candles[i][2] < a3_candles[i + 1][2]:
                                confirmed = True

                psps.append(PSP(
                    a1_candle=a1_candles[i],
                    a2_candle=a2_candles[i],
                    a3_candle=a3_candles[i],
                    confirmed=confirmed,
                    swept_from_to=None
                ))
            finally:
                a1_min, a1_max = min(a1_min, a1_candles[i][2]), max(a1_max, a1_candles[i][1])
                a2_min, a2_max = min(a2_min, a2_candles[i][2]), max(a2_max, a2_candles[i][1])
                a3_min, a3_max = min(a3_min, a3_candles[i][2]), max(a3_max, a3_candles[i][1])

        return psps

    def with_15m_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with 15m PSPs
        smt.psps_15m = self.calculate_psps(
            next_tick,
            (to_utc_datetime(next_tick) - timedelta(minutes=15), to_utc_datetime(next_tick)),
            lambda x: (to_utc_datetime(x), to_utc_datetime(x) + timedelta(minutes=14)),
            lambda candles: candles,
            smt
        )
        return smt

    def with_30m_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with 30m PSPs
        smt.psps_30m = self.calculate_psps(
            next_tick,
            get_prev_30m_from_to(next_tick),
            get_current_30m_from_to,
            as_30m_candles,
            smt
        )
        return smt

    def with_1h_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with 1h PSPs
        smt.psps_1h = self.calculate_psps(
            next_tick,
            get_prev_1h_from_to(next_tick),
            get_current_1h_from_to,
            as_1h_candles,
            smt
        )
        return smt

    def with_2h_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with 2h PSPs
        smt.psps_2h = self.calculate_psps(
            next_tick,
            get_prev_2h_from_to(next_tick),
            get_current_2h_from_to,
            as_2h_candles,
            smt
        )
        return smt

    def with_4h_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with 4h PSPs
        smt.psps_4h = self.calculate_psps(
            next_tick,
            get_prev_4h_from_to(next_tick),
            get_current_4h_from_to,
            as_4h_candles,
            smt
        )
        return smt

    def with_day_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with day PSPs
        smt.psps_1d = self.calculate_psps(
            next_tick,
            get_prev_1d_from_to(next_tick),
            get_current_1d_from_to,
            as_1d_candles,
            smt
        )
        return smt

    def with_week_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with week PSPs
        smt.psps_1_week = self.calculate_psps(
            next_tick,
            get_prev_1w_from_to(next_tick),
            get_current_1w_from_to,
            as_1w_candles,
            smt
        )
        return smt

    def with_month_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with week PSPs
        smt.psps_1_month = self.calculate_psps(
            next_tick,
            get_prev_1month_from_to(next_tick),
            get_current_1month_from_to,
            as_1month_candles,
            smt
        )
        return smt

    def prev_year_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.prev_year:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.prev_year[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.prev_year, self.a2.prev_year, self.a3.prev_year
        )
        if high_smt:
            high_smt = self.with_day_psps(next_tick, high_smt)
            high_smt = self.with_week_psps(next_tick, high_smt)
            high_smt = self.with_month_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_day_psps(next_tick, half_smt)
            half_smt = self.with_week_psps(next_tick, half_smt)
            half_smt = self.with_month_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_day_psps(next_tick, low_smt)
            low_smt = self.with_week_psps(next_tick, low_smt)
            low_smt = self.with_month_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def year_q1_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.year_q1:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.year_q1[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.year_q1, self.a2.year_q1, self.a3.year_q1
        )
        if high_smt:
            high_smt = self.with_day_psps(next_tick, high_smt)
            high_smt = self.with_week_psps(next_tick, high_smt)
            high_smt = self.with_month_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_day_psps(next_tick, half_smt)
            half_smt = self.with_week_psps(next_tick, half_smt)
            half_smt = self.with_month_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_day_psps(next_tick, low_smt)
            low_smt = self.with_week_psps(next_tick, low_smt)
            low_smt = self.with_month_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def year_q2_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.year_q2:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.year_q2[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.year_q2, self.a2.year_q2, self.a3.year_q2
        )
        if high_smt:
            high_smt = self.with_day_psps(next_tick, high_smt)
            high_smt = self.with_week_psps(next_tick, high_smt)
            high_smt = self.with_month_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_day_psps(next_tick, half_smt)
            half_smt = self.with_week_psps(next_tick, half_smt)
            half_smt = self.with_month_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_day_psps(next_tick, low_smt)
            low_smt = self.with_week_psps(next_tick, low_smt)
            low_smt = self.with_month_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def year_q3_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.year_q3:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.year_q3[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.year_q3, self.a2.year_q3, self.a3.year_q3
        )
        if high_smt:
            high_smt = self.with_day_psps(next_tick, high_smt)
            high_smt = self.with_week_psps(next_tick, high_smt)
            high_smt = self.with_month_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_day_psps(next_tick, half_smt)
            half_smt = self.with_week_psps(next_tick, half_smt)
            half_smt = self.with_month_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_day_psps(next_tick, low_smt)
            low_smt = self.with_week_psps(next_tick, low_smt)
            low_smt = self.with_month_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def year_q4_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.year_q4:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.year_q4[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.year_q4, self.a2.year_q4, self.a3.year_q4
        )
        if high_smt:
            high_smt = self.with_day_psps(next_tick, high_smt)
            high_smt = self.with_week_psps(next_tick, high_smt)
            high_smt = self.with_month_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_day_psps(next_tick, half_smt)
            half_smt = self.with_week_psps(next_tick, half_smt)
            half_smt = self.with_month_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_day_psps(next_tick, low_smt)
            low_smt = self.with_week_psps(next_tick, low_smt)
            low_smt = self.with_month_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def week1_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.week1:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.week1[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.week1, self.a2.week1, self.a3.week1
        )
        if high_smt:
            high_smt = self.with_4h_psps(next_tick, high_smt)
            high_smt = self.with_day_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_4h_psps(next_tick, half_smt)
            half_smt = self.with_day_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_4h_psps(next_tick, low_smt)
            low_smt = self.with_day_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def week2_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.week2:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.week2[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.week2, self.a2.week2, self.a3.week2
        )
        if high_smt:
            high_smt = self.with_4h_psps(next_tick, high_smt)
            high_smt = self.with_day_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_4h_psps(next_tick, half_smt)
            half_smt = self.with_day_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_4h_psps(next_tick, low_smt)
            low_smt = self.with_day_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def week3_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.week3:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.week3[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.week3, self.a2.week3, self.a3.week3
        )
        if high_smt:
            high_smt = self.with_4h_psps(next_tick, high_smt)
            high_smt = self.with_day_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_4h_psps(next_tick, half_smt)
            half_smt = self.with_day_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_4h_psps(next_tick, low_smt)
            low_smt = self.with_day_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def week4_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.week4:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.week4[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.week4, self.a2.week4, self.a3.week4
        )
        if high_smt:
            high_smt = self.with_4h_psps(next_tick, high_smt)
            high_smt = self.with_day_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_4h_psps(next_tick, half_smt)
            half_smt = self.with_day_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_4h_psps(next_tick, low_smt)
            low_smt = self.with_day_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def week5_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.week5:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.week5[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.week5, self.a2.week5, self.a3.week5
        )
        if high_smt:
            high_smt = self.with_4h_psps(next_tick, high_smt)
            high_smt = self.with_day_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_4h_psps(next_tick, half_smt)
            half_smt = self.with_day_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_4h_psps(next_tick, low_smt)
            low_smt = self.with_day_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def mon_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.mon:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.mon[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.mon, self.a2.mon, self.a3.mon
        )
        if high_smt:
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
            high_smt = self.with_4h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
            half_smt = self.with_4h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
            low_smt = self.with_4h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def tue_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.tue:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.tue[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.tue, self.a2.tue, self.a3.tue
        )
        if high_smt:
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
            high_smt = self.with_4h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
            half_smt = self.with_4h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
            low_smt = self.with_4h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def wed_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.wed:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.wed[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.wed, self.a2.wed, self.a3.wed
        )
        if high_smt:
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
            high_smt = self.with_4h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
            half_smt = self.with_4h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
            low_smt = self.with_4h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def thu_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.thu:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.thu[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.thu, self.a2.thu, self.a3.thu
        )
        if high_smt:
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
            high_smt = self.with_4h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
            half_smt = self.with_4h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
            low_smt = self.with_4h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def mon_thu_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.mon_thu:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.mon_thu[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.mon_thu, self.a2.mon_thu, self.a3.mon_thu
        )
        if high_smt:
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
            high_smt = self.with_4h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
            half_smt = self.with_4h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
            low_smt = self.with_4h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def fri_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.fri:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.fri[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.fri, self.a2.fri, self.a3.fri
        )
        if high_smt:
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
            high_smt = self.with_4h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
            half_smt = self.with_4h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
            low_smt = self.with_4h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def mon_fri_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.mon_fri:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.mon_fri[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.mon_fri, self.a2.mon_fri, self.a3.mon_fri
        )
        if high_smt:
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
            high_smt = self.with_4h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
            half_smt = self.with_4h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
            low_smt = self.with_4h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def sat_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.sat:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.sat[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.sat, self.a2.sat, self.a3.sat
        )
        if high_smt:
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
            high_smt = self.with_4h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
            half_smt = self.with_4h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
            low_smt = self.with_4h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def asia_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.asia:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.asia[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.asia, self.a2.asia, self.a3.asia
        )
        if high_smt:
            high_smt = self.with_30m_psps(next_tick, high_smt)
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def london_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.london:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.london[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.london, self.a2.london, self.a3.london
        )
        if high_smt:
            high_smt = self.with_30m_psps(next_tick, high_smt)
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def nyam_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.nyam:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.nyam[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.nyam, self.a2.nyam, self.a3.nyam
        )
        if high_smt:
            high_smt = self.with_30m_psps(next_tick, high_smt)
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def nypm_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.nypm:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.nypm[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.nypm, self.a2.nypm, self.a3.nypm
        )
        if high_smt:
            high_smt = self.with_30m_psps(next_tick, high_smt)
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def q1_90_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.q1_90m:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.q1_90m[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.q1_90m, self.a2.q1_90m, self.a3.q1_90m
        )
        if high_smt:
            high_smt = self.with_15m_psps(next_tick, high_smt)
            high_smt = self.with_30m_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_15m_psps(next_tick, half_smt)
            half_smt = self.with_30m_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_15m_psps(next_tick, low_smt)
            low_smt = self.with_30m_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def q2_90_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.q2_90m:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.q2_90m[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.q2_90m, self.a2.q2_90m, self.a3.q2_90m
        )
        if high_smt:
            high_smt = self.with_15m_psps(next_tick, high_smt)
            high_smt = self.with_30m_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_15m_psps(next_tick, half_smt)
            half_smt = self.with_30m_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_15m_psps(next_tick, low_smt)
            low_smt = self.with_30m_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def q3_90_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.q3_90m:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.q3_90m[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.q3_90m, self.a2.q3_90m, self.a3.q3_90m
        )
        if high_smt:
            high_smt = self.with_15m_psps(next_tick, high_smt)
            high_smt = self.with_30m_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_15m_psps(next_tick, half_smt)
            half_smt = self.with_30m_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_15m_psps(next_tick, low_smt)
            low_smt = self.with_30m_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def q4_90_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.q4_90m:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.q4_90m[1]) + timedelta(minutes=1))
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.q4_90m, self.a2.q4_90m, self.a3.q4_90m
        )
        if high_smt:
            high_smt = self.with_15m_psps(next_tick, high_smt)
            high_smt = self.with_30m_psps(next_tick, high_smt)
        if half_smt:
            half_smt = self.with_15m_psps(next_tick, half_smt)
            half_smt = self.with_30m_psps(next_tick, half_smt)
        if low_smt:
            low_smt = self.with_15m_psps(next_tick, low_smt)
            low_smt = self.with_30m_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def actual_smt_psp(self):
        result = {
            'smt 1. prev_year': self.prev_year_smt(),
            'smt 2.1 year_q1': self.year_q1_smt(),
            'smt 2.2 year_q2': self.year_q2_smt(),
            'smt 2.3 year_q3': self.year_q3_smt(),
            'smt 2.4 year_q4': self.year_q4_smt(),
            'smt 3.1 week1': self.week1_smt(),
            'smt 3.2 week2': self.week2_smt(),
            'smt 3.3 week3': self.week3_smt(),
            'smt 3.4 week4': self.week4_smt(),
            'smt 3.5 week5': self.week5_smt(),
            'smt 4.1 mon': self.mon_smt(),
            'smt 4.2 tue': self.tue_smt(),
            'smt 4.3 wed': self.wed_smt(),
            'smt 4.4 thu': self.thu_smt(),
            'smt 4.4.1 mon_thu': self.mon_thu_smt(),
            'smt 4.5 fri': self.fri_smt(),
            'smt 4.5.1 mon_fri': self.mon_fri_smt(),
            'smt 4.6 sat': self.sat_smt(),
            'smt 5.1 asia': self.asia_smt(),
            'smt 5.2 london': self.london_smt(),
            'smt 5.3 nyam': self.nyam_smt(),
            'smt 5.4 nypm': self.nypm_smt(),
            'smt 6.1 q1_90': self.q1_90_smt(),
            'smt 6.2 q2_90': self.q2_90_smt(),
            'smt 6.3q3_90': self.q3_90_smt(),
            'smt 6.4 q4_90': self.q4_90_smt(),
        }

        return result

    @staticmethod
    def diff_smt_psp(d1, d2):
        diff = {}
        all_keys = set(d1) | set(d2)

        for key in all_keys:
            v1 = d1.get(key, None)
            v2 = d2.get(key, None)
            if v1 != v2:
                diff[key] = {"old": v1, "new": v2}

        return diff


def new_empty_triad(a1_symbol: str, a2_symbol: str, a3_symbol: str) -> Triad:
    return Triad(
        a1=new_empty_asset(a1_symbol),
        a2=new_empty_asset(a2_symbol),
        a3=new_empty_asset(a3_symbol),
    )


def new_triad(symbols_tuple: Tuple[str, str, str],
              generators_tuple: Tuple[Reverse15mGenerator, Reverse15mGenerator, Reverse15mGenerator]) -> Triad:
    res = new_empty_triad(*symbols_tuple)

    res.a1.populate(generators_tuple[0])
    res.a2.populate(generators_tuple[1])
    res.a3.populate(generators_tuple[2])

    return res


def triad_decoder(dct):
    if "a1" in dct:
        return Triad(**dct)
    if "snapshot_date_readable" in dct:
        return Asset(**dct)


def triad_from_json(json_str):
    return json.loads(json_str, object_hook=triad_decoder)


def json_from_triad(triad: Triad):
    return json.dumps(asdict(triad), indent=2)
