from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import TypeAlias, Tuple, Optional, List, Generator

from stock_market_research_kit.candle import PriceDate, InnerCandle, as_1_candle
from stock_market_research_kit.db_layer import select_full_days_candles_15m
from stock_market_research_kit.quarter import Quarter90m, DayQuarter, WeekDay, MonthWeek, YearQuarter
from utils.date_utils import to_utc_datetime, to_date_str, get_prev_30m_from_to, get_current_30m_from_to, \
    get_prev_1h_from_to, get_current_1h_from_to, get_prev_2h_from_to, get_current_2h_from_to, get_prev_4h_from_to, \
    get_current_4h_from_to, get_prev_1d_from_to, get_current_1d_from_to, get_prev_1w_from_to, get_current_1w_from_to, \
    get_prev_1month_from_to, get_current_1month_from_to, quarters90m_ranges, day_quarters_ranges, \
    prev_year_ranges, weekday_ranges, month_week_quarters_ranges, year_quarters_ranges, quarters_by_time

LiqSwept: TypeAlias = Tuple[float, bool]  # (price, is_swept)
# (date_start, date_end, ended, high, half, low)
QuarterLiq: TypeAlias = Tuple[str, str, bool, LiqSwept, LiqSwept, LiqSwept]
Reverse15mGenerator: TypeAlias = Generator[InnerCandle, None, None]


class TriadAsset(Enum):
    A1 = 'Asset 1'
    A2 = 'Asset 2'
    A3 = 'Asset 3'


@dataclass
class Asset:
    symbol: str
    snapshot_date_readable: str

    prev_year: Optional[QuarterLiq]

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
            else:
                self.yq_set(ranges_yq[i][0], (
                    prev_yql[0], prev_yql[1], prev_yql[2],
                    (prev_yql[3][0], prev_yql[3][0] < candle[1]),
                    (prev_yql[4][0], candle[2] <= prev_yql[4][0] <= candle[1]),
                    (prev_yql[5][0], prev_yql[5][0] > candle[2])
                ))

        if prev_yq != new_yq:
            if prev_yq == YearQuarter.YQ1:
                self.year_q4 = None
                self.true_yo = (candle[4], to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15)))
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
            else:
                self.mw_set(ranges_mw[i][0], (
                    prev_mwl[0], prev_mwl[1], prev_mwl[2],
                    (prev_mwl[3][0], prev_mwl[3][0] < candle[1]),
                    (prev_mwl[4][0], candle[2] <= prev_mwl[4][0] <= candle[1]),
                    (prev_mwl[5][0], prev_mwl[5][0] > candle[2])
                ))

        if prev_mw != new_mw:
            if prev_mw == MonthWeek.MW1:
                self.week4 = None
                self.week5 = None
                self.true_mo = (candle[4], to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15)))
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
            else:
                self.wd_set(ranges_wd[i][0], (
                    prev_wdl[0], prev_wdl[1], prev_wdl[2],
                    (prev_wdl[3][0], prev_wdl[3][0] < candle[1]),
                    (prev_wdl[4][0], candle[2] <= prev_wdl[4][0] <= candle[1]),
                    (prev_wdl[5][0], prev_wdl[5][0] > candle[2])
                ))

        if prev_wd != new_wd:
            if prev_wd == WeekDay.Mon:
                self.thu = None
                self.fri = None
                self.true_wo = (candle[4], to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15)))
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
            else:
                self.dq_set(ranges_dq[i][0], (
                    prev_dql[0], prev_dql[1], prev_dql[2],
                    (prev_dql[3][0], prev_dql[3][0] < candle[1]),
                    (prev_dql[4][0], candle[2] <= prev_dql[4][0] <= candle[1]),
                    (prev_dql[5][0], prev_dql[5][0] > candle[2])
                ))

        if prev_dq != new_dq:
            if prev_dq == DayQuarter.DQ1_Asia:
                self.nypm = None
                self.true_do = (candle[4], to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15)))
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
            else:
                self.q90m_set(ranges_q90m[i][0], (
                    prev_q90ml[0], prev_q90ml[1], prev_q90ml[2],
                    (prev_q90ml[3][0], prev_q90ml[3][0] < candle[1]),
                    (prev_q90ml[4][0], candle[2] <= prev_q90ml[4][0] <= candle[1]),
                    (prev_q90ml[5][0], prev_q90ml[5][0] > candle[2])
                ))

        if prev_q90m != new_q90m:
            if prev_q90m == Quarter90m.Q1_90m:
                self.q4_90m = None
                self.true_90m_open = (candle[4], to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=15)))
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

    def populate(self, reverse_15m_gen: Reverse15mGenerator):
        self.prev_15m_candle = next(reverse_15m_gen)

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

        ranges_90m, t90m0 = quarters90m_ranges(self.snapshot_date_readable)
        sweeps_90m = [(highest_sweep, lowest_sweep) for _ in ranges_90m]
        cum_90m_quarters = []
        for rng_90m in ranges_90m:
            cum_90m_quarters.append(self.prev_15m_candle if rng_90m[1] <= to_utc_datetime(
                self.prev_15m_candle[5]) < rng_90m[2] else None)

        ranges_dq, tdo = day_quarters_ranges(self.snapshot_date_readable)
        sweeps_dq = [(highest_sweep, lowest_sweep) for _ in ranges_dq]
        cum_dq_quarters = []
        for rng_dq in ranges_dq:
            cum_dq_quarters.append(self.prev_15m_candle if rng_dq[1] <= to_utc_datetime(
                self.prev_15m_candle[5]) < rng_dq[2] else None)

        ranges_wd, two = weekday_ranges(self.snapshot_date_readable)
        sweeps_wd = [(highest_sweep, lowest_sweep) for _ in ranges_wd]
        cum_wd_quarters = []
        for rng_wd in ranges_wd:
            cum_wd_quarters.append(self.prev_15m_candle if rng_wd[1] <= to_utc_datetime(
                self.prev_15m_candle[5]) < rng_wd[2] else None)

        ranges_mw, tmo = month_week_quarters_ranges(self.snapshot_date_readable)
        sweeps_mw = [(highest_sweep, lowest_sweep) for _ in ranges_mw]
        cum_mw_quarters = []
        for rng_mw in ranges_mw:
            cum_mw_quarters.append(self.prev_15m_candle if rng_mw[1] <= to_utc_datetime(
                self.prev_15m_candle[5]) < rng_mw[2] else None)

        ranges_yq, tyo = year_quarters_ranges(self.snapshot_date_readable)
        sweeps_yq = [(highest_sweep, lowest_sweep) for _ in ranges_yq]
        cum_yq_quarters = []
        for rng_yq in ranges_yq:
            cum_yq_quarters.append(self.prev_15m_candle if rng_yq[1] <= to_utc_datetime(
                self.prev_15m_candle[5]) < rng_yq[2] else None)

        prev_year_from, prev_year_to = prev_year_ranges(self.snapshot_date_readable)
        prev_year_high_sweep, prev_year_low_sweep = highest_sweep, lowest_sweep
        cum_prev_year = self.prev_15m_candle if prev_year_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_year_to else None

        cum_prev_30m_candle = self.prev_15m_candle if prev_30m_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_30m_to else None
        cum_current_30m_candle = self.prev_15m_candle if current_30m_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_30m_to else None
        cum_prev_1h_candle = self.prev_15m_candle if prev_1h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_1h_to else None
        cum_current_1h_candle = self.prev_15m_candle if current_1h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_1h_to else None
        cum_prev_2h_candle = self.prev_15m_candle if prev_2h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_2h_to else None
        cum_current_2h_candle = self.prev_15m_candle if current_2h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_2h_to else None
        cum_prev_4h_candle = self.prev_15m_candle if prev_4h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_4h_to else None
        cum_current_4h_candle = self.prev_15m_candle if current_4h_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_4h_to else None
        cum_prev_1d_candle = self.prev_15m_candle if prev_1d_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_1d_to else None
        cum_current_1d_candle = self.prev_15m_candle if current_1d_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_1d_to else None
        cum_prev_1w_candle = self.prev_15m_candle if prev_1w_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_1w_to else None
        cum_current_1w_candle = self.prev_15m_candle if current_1w_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_1w_to else None
        cum_prev_1month_candle = self.prev_15m_candle if prev_1month_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < prev_1month_to else None
        cum_current_1month_candle = self.prev_15m_candle if current_1month_from <= to_utc_datetime(
            self.prev_15m_candle[5]) < current_1month_to else None

        while True:
            prev_candle = next(reverse_15m_gen)

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

            for i in range(len(ranges_90m)):
                res_rng = None
                if ranges_90m[i][1] <= to_utc_datetime(prev_candle[5]) < ranges_90m[i][2]:
                    if not cum_90m_quarters[i]:
                        sweeps_90m[i] = (highest_sweep, lowest_sweep)
                        cum_90m_quarters[i] = prev_candle
                    else:
                        cum_90m_quarters[i] = as_1_candle([prev_candle, cum_90m_quarters[i]])
                    if ranges_90m[i][1] == to_utc_datetime(prev_candle[5]):
                        half = (cum_90m_quarters[i][1] + cum_90m_quarters[i][2]) / 2
                        res_rng = (
                            cum_90m_quarters[i][5], to_date_str(ranges_90m[i][2]),
                            ranges_90m[i][2] < to_utc_datetime(self.snapshot_date_readable),
                            (cum_90m_quarters[i][1], cum_90m_quarters[i][1] < sweeps_90m[i][0]),
                            (half, sweeps_90m[i][1] <= half <= sweeps_90m[i][0]),
                            (cum_90m_quarters[i][2], cum_90m_quarters[i][2] > sweeps_90m[i][1])
                        )
                if res_rng and ranges_90m[i][0] == Quarter90m.Q1_90m:
                    self.q1_90m = res_rng
                elif res_rng and ranges_90m[i][0] == Quarter90m.Q2_90m:
                    self.q2_90m = res_rng
                elif res_rng and ranges_90m[i][0] == Quarter90m.Q3_90m:
                    self.q3_90m = res_rng
                elif res_rng and ranges_90m[i][0] == Quarter90m.Q4_90m:
                    self.q4_90m = res_rng

            for i in range(len(ranges_dq)):
                res_rng = None
                if ranges_dq[i][1] <= to_utc_datetime(prev_candle[5]) < ranges_dq[i][2]:
                    if not cum_dq_quarters[i]:
                        sweeps_dq[i] = (highest_sweep, lowest_sweep)
                        cum_dq_quarters[i] = prev_candle
                    else:
                        cum_dq_quarters[i] = as_1_candle([prev_candle, cum_dq_quarters[i]])
                    if ranges_dq[i][1] == to_utc_datetime(prev_candle[5]):
                        half = (cum_dq_quarters[i][1] + cum_dq_quarters[i][2]) / 2
                        res_rng = (
                            cum_dq_quarters[i][5], to_date_str(ranges_dq[i][2]),
                            ranges_dq[i][2] < to_utc_datetime(self.snapshot_date_readable),
                            (cum_dq_quarters[i][1], cum_dq_quarters[i][1] < sweeps_dq[i][0]),
                            (half, sweeps_dq[i][1] <= half <= sweeps_dq[i][0]),
                            (cum_dq_quarters[i][2], cum_dq_quarters[i][2] > sweeps_dq[i][1])
                        )
                if res_rng and ranges_dq[i][0] == DayQuarter.DQ1_Asia:
                    self.asia = res_rng
                elif res_rng and ranges_dq[i][0] == DayQuarter.DQ2_London:
                    self.london = res_rng
                elif res_rng and ranges_dq[i][0] == DayQuarter.DQ3_NYAM:
                    self.nyam = res_rng
                elif res_rng and ranges_dq[i][0] == DayQuarter.DQ4_NYPM:
                    self.nypm = res_rng

            for i in range(len(ranges_wd)):
                res_rng = None
                if ranges_wd[i][1] <= to_utc_datetime(prev_candle[5]) < ranges_wd[i][2]:
                    if not cum_wd_quarters[i]:
                        sweeps_wd[i] = (highest_sweep, lowest_sweep)
                        cum_wd_quarters[i] = prev_candle
                    else:
                        cum_wd_quarters[i] = as_1_candle([prev_candle, cum_wd_quarters[i]])
                    if ranges_wd[i][1] == to_utc_datetime(prev_candle[5]):
                        half = (cum_wd_quarters[i][1] + cum_wd_quarters[i][2]) / 2
                        res_rng = (
                            cum_wd_quarters[i][5], to_date_str(ranges_wd[i][2]),
                            ranges_wd[i][2] < to_utc_datetime(self.snapshot_date_readable),
                            (cum_wd_quarters[i][1], cum_wd_quarters[i][1] < sweeps_wd[i][0]),
                            (half, sweeps_wd[i][1] <= half <= sweeps_wd[i][0]),
                            (cum_wd_quarters[i][2], cum_wd_quarters[i][2] > sweeps_wd[i][1])
                        )
                if res_rng and ranges_wd[i][0] == WeekDay.Mon:
                    self.mon = res_rng
                elif res_rng and ranges_wd[i][0] == WeekDay.Tue:
                    self.tue = res_rng
                elif res_rng and ranges_wd[i][0] == WeekDay.Wed:
                    self.wed = res_rng
                elif res_rng and ranges_wd[i][0] == WeekDay.Thu:
                    self.thu = res_rng
                elif res_rng and ranges_wd[i][0] == WeekDay.MonThu:
                    self.mon_thu = res_rng
                elif res_rng and ranges_wd[i][0] == WeekDay.Fri:
                    self.fri = res_rng
                elif res_rng and ranges_wd[i][0] == WeekDay.MonFri:
                    self.mon_fri = res_rng
                elif res_rng and ranges_wd[i][0] == WeekDay.Sat:
                    self.sat = res_rng

            for i in range(len(ranges_mw)):
                res_rng = None
                if ranges_mw[i][1] <= to_utc_datetime(prev_candle[5]) < ranges_mw[i][2]:
                    if not cum_mw_quarters[i]:
                        sweeps_mw[i] = (highest_sweep, lowest_sweep)
                        cum_mw_quarters[i] = prev_candle
                    else:
                        cum_mw_quarters[i] = as_1_candle([prev_candle, cum_mw_quarters[i]])
                    if ranges_mw[i][1] == to_utc_datetime(prev_candle[5]):
                        half = (cum_mw_quarters[i][1] + cum_mw_quarters[i][2]) / 2
                        res_rng = (
                            cum_mw_quarters[i][5], to_date_str(ranges_mw[i][2]),
                            ranges_mw[i][2] < to_utc_datetime(self.snapshot_date_readable),
                            (cum_mw_quarters[i][1], cum_mw_quarters[i][1] < sweeps_mw[i][0]),
                            (half, sweeps_mw[i][1] <= half <= sweeps_mw[i][0]),
                            (cum_mw_quarters[i][2], cum_mw_quarters[i][2] > sweeps_mw[i][1])
                        )
                if res_rng and ranges_mw[i][0] == MonthWeek.MW1:
                    self.week1 = res_rng
                elif res_rng and ranges_mw[i][0] == MonthWeek.MW2:
                    self.week2 = res_rng
                elif res_rng and ranges_mw[i][0] == MonthWeek.MW3:
                    self.week3 = res_rng
                elif res_rng and ranges_mw[i][0] == MonthWeek.MW4:
                    self.week4 = res_rng
                elif res_rng and ranges_mw[i][0] == MonthWeek.MW5:
                    self.week5 = res_rng

            for i in range(len(ranges_yq)):
                if ranges_yq[i][1] <= to_utc_datetime(prev_candle[5]) < ranges_yq[i][2]:
                    if not cum_yq_quarters[i]:
                        sweeps_yq[i] = (highest_sweep, lowest_sweep)
                        cum_yq_quarters[i] = prev_candle
                    else:
                        cum_yq_quarters[i] = as_1_candle([prev_candle, cum_yq_quarters[i]])
                    if ranges_yq[i][1] == to_utc_datetime(prev_candle[5]):
                        half = (cum_yq_quarters[i][1] + cum_yq_quarters[i][2]) / 2
                        self.yq_set(ranges_yq[i][0], (
                            cum_yq_quarters[i][5], to_date_str(ranges_yq[i][2]),
                            ranges_yq[i][2] < to_utc_datetime(self.snapshot_date_readable),
                            (cum_yq_quarters[i][1], cum_yq_quarters[i][1] < sweeps_yq[i][0]),
                            (half, sweeps_yq[i][1] <= half <= sweeps_yq[i][0]),
                            (cum_yq_quarters[i][2], cum_yq_quarters[i][2] > sweeps_yq[i][1])
                        ))

            if not self.prev_year and prev_year_from <= to_utc_datetime(prev_candle[5]) < prev_year_to:
                if not cum_prev_year:
                    prev_year_high_sweep, prev_year_low_sweep = highest_sweep, lowest_sweep
                    cum_prev_year = prev_candle
                else:
                    cum_prev_year = as_1_candle([prev_candle, cum_prev_year])

                if prev_year_from == to_utc_datetime(prev_candle[5]):
                    half = (cum_prev_year[1] + cum_prev_year[2]) / 2
                    self.prev_year = (
                        to_date_str(prev_year_from), to_date_str(prev_year_to),
                        prev_year_to < to_utc_datetime(self.snapshot_date_readable),
                        (cum_prev_year[1], cum_prev_year[1] < prev_year_high_sweep),
                        (half, prev_year_low_sweep <= half <= prev_year_high_sweep),
                        (cum_prev_year[2], cum_prev_year[2] > prev_year_low_sweep)
                    )

            if self.prev_30m_candle[5] == "" and prev_30m_from <= to_utc_datetime(prev_candle[5]) < prev_30m_to:
                cum_prev_30m_candle = prev_candle if not cum_prev_30m_candle else as_1_candle(
                    [prev_candle, cum_prev_30m_candle])
                if prev_30m_from == to_utc_datetime(prev_candle[5]):
                    self.prev_30m_candle = cum_prev_30m_candle
            if not self.current_30m_candle and current_30m_from <= to_utc_datetime(prev_candle[5]) < current_30m_to:
                cum_current_30m_candle = prev_candle if not cum_current_30m_candle else as_1_candle(
                    [prev_candle, cum_current_30m_candle])
                if current_30m_from == to_utc_datetime(prev_candle[5]):
                    self.current_30m_candle = cum_current_30m_candle

            if self.prev_1h_candle[5] == "" and prev_1h_from <= to_utc_datetime(prev_candle[5]) < prev_1h_to:
                cum_prev_1h_candle = prev_candle if not cum_prev_1h_candle else as_1_candle(
                    [prev_candle, cum_prev_1h_candle])
                if prev_1h_from == to_utc_datetime(prev_candle[5]):
                    self.prev_1h_candle = cum_prev_1h_candle
            if not self.current_1h_candle and current_1h_from <= to_utc_datetime(prev_candle[5]) < current_1h_to:
                cum_current_1h_candle = prev_candle if not cum_current_1h_candle else as_1_candle(
                    [prev_candle, cum_current_1h_candle])
                if current_1h_from == to_utc_datetime(prev_candle[5]):
                    self.current_1h_candle = cum_current_1h_candle

            if self.prev_2h_candle[5] == "" and prev_2h_from <= to_utc_datetime(prev_candle[5]) < prev_2h_to:
                cum_prev_2h_candle = prev_candle if not cum_prev_2h_candle else as_1_candle(
                    [prev_candle, cum_prev_2h_candle])
                if prev_2h_from == to_utc_datetime(prev_candle[5]):
                    self.prev_2h_candle = cum_prev_2h_candle
            if not self.current_2h_candle and current_2h_from <= to_utc_datetime(prev_candle[5]) < current_2h_to:
                cum_current_2h_candle = prev_candle if not cum_current_2h_candle else as_1_candle(
                    [prev_candle, cum_current_2h_candle])
                if current_2h_from == to_utc_datetime(prev_candle[5]):
                    self.current_2h_candle = cum_current_2h_candle

            if self.prev_4h_candle[5] == "" and prev_4h_from <= to_utc_datetime(prev_candle[5]) < prev_4h_to:
                cum_prev_4h_candle = prev_candle if not cum_prev_4h_candle else as_1_candle(
                    [prev_candle, cum_prev_4h_candle])
                if prev_4h_from == to_utc_datetime(prev_candle[5]):
                    self.prev_4h_candle = cum_prev_4h_candle
            if not self.current_4h_candle and current_4h_from <= to_utc_datetime(prev_candle[5]) < current_4h_to:
                cum_current_4h_candle = prev_candle if not cum_current_4h_candle else as_1_candle(
                    [prev_candle, cum_current_4h_candle])
                if current_4h_from == to_utc_datetime(prev_candle[5]):
                    self.current_4h_candle = cum_current_4h_candle

            if self.prev_1d_candle[5] == "" and prev_1d_from <= to_utc_datetime(prev_candle[5]) < prev_1d_to:
                cum_prev_1d_candle = prev_candle if not cum_prev_1d_candle else as_1_candle(
                    [prev_candle, cum_prev_1d_candle])
                if prev_1d_from == to_utc_datetime(prev_candle[5]):
                    self.prev_1d_candle = cum_prev_1d_candle
            if not self.current_1d_candle and current_1d_from <= to_utc_datetime(prev_candle[5]) < current_1d_to:
                cum_current_1d_candle = prev_candle if not cum_current_1d_candle else as_1_candle(
                    [prev_candle, cum_current_1d_candle])
                if current_1d_from == to_utc_datetime(prev_candle[5]):
                    self.current_1d_candle = cum_current_1d_candle

            if self.prev_1w_candle[5] == "" and prev_1w_from <= to_utc_datetime(prev_candle[5]) < prev_1w_to:
                cum_prev_1w_candle = prev_candle if not cum_prev_1w_candle else as_1_candle(
                    [prev_candle, cum_prev_1w_candle])
                if prev_1w_from == to_utc_datetime(prev_candle[5]):
                    self.prev_1w_candle = cum_prev_1w_candle
            if not self.current_1w_candle and current_1w_from <= to_utc_datetime(prev_candle[5]) < current_1w_to:
                cum_current_1w_candle = prev_candle if not cum_current_1w_candle else as_1_candle(
                    [prev_candle, cum_current_1w_candle])
                if current_1w_from == to_utc_datetime(prev_candle[5]):
                    self.current_1w_candle = cum_current_1w_candle

            if self.prev_1month_candle[5] == "" and prev_1month_from <= to_utc_datetime(
                    prev_candle[5]) < prev_1month_to:
                cum_prev_1month_candle = prev_candle if not cum_prev_1month_candle else as_1_candle(
                    [prev_candle, cum_prev_1month_candle])
                if prev_1month_from == to_utc_datetime(prev_candle[5]):
                    self.prev_1month_candle = cum_prev_1month_candle
            if not self.current_1month_candle and current_1month_from <= to_utc_datetime(
                    prev_candle[5]) < current_1month_to:
                cum_current_1month_candle = prev_candle if not cum_current_1month_candle else as_1_candle(
                    [prev_candle, cum_current_1month_candle])
                if current_1month_from == to_utc_datetime(prev_candle[5]):
                    self.current_1month_candle = cum_current_1month_candle

            highest_sweep = max(prev_candle[1], highest_sweep)
            lowest_sweep = min(prev_candle[2], lowest_sweep)

            if to_utc_datetime(prev_candle[5]) <= prev_year_from:
                break


def new_empty_asset(symbol: str) -> Asset:
    return Asset(
        symbol=symbol,
        snapshot_date_readable="",
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
    )


SMT: TypeAlias = Optional[List[TriadAsset]]  # list of 1-2 assets from triad that swept

PSP: TypeAlias = Optional[Tuple[
    Tuple[TriadAsset, InnerCandle],
    Tuple[TriadAsset, InnerCandle],
    Tuple[TriadAsset, InnerCandle]
]]


@dataclass
class Triad:
    a1: Asset
    a2: Asset
    a3: Asset

    @staticmethod
    def calculate_smt(a1_ql, a2_aq, a3_ql: Optional[QuarterLiq]) -> Tuple[SMT, SMT, SMT]:  # high, half, low
        _, _, _, a1_high, a1_half, a1_low = a1_ql
        _, _, _, a2_high, a2_half, a2_low = a2_aq
        _, _, _, a3_high, a3_half, a3_low = a3_ql
        high = [x[0] for x in
                [(TriadAsset.A1, a1_high[1]), (TriadAsset.A2, a2_high[1]), (TriadAsset.A3, a3_high[1])] if x[1]]
        high = None if len(high) == 3 else high

        half = [x[0] for x in
                [(TriadAsset.A1, a1_half[1]), (TriadAsset.A2, a2_half[1]), (TriadAsset.A3, a3_half[1])] if x[1]]
        half = None if len(half) == 3 else half

        low = [x[0] for x in
               [(TriadAsset.A1, a1_low[1]), (TriadAsset.A2, a2_low[1]), (TriadAsset.A3, a3_low[1])] if x[1]]
        low = None if len(low) == 3 else low

        return high, half, low

    def prev_year_smt(self) -> Tuple[SMT, SMT, SMT]:  # high, half, low
        return Triad.calculate_smt(self.a1.prev_year, self.a2.prev_year, self.a3.prev_year)

    def year_q1_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.year_q1:
            return None
        return Triad.calculate_smt(self.a1.year_q1, self.a2.year_q1, self.a3.year_q1)

    def year_q2_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.year_q2:
            return None
        return Triad.calculate_smt(self.a1.year_q2, self.a2.year_q2, self.a3.year_q2)

    def year_q3_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.year_q3:
            return None
        return Triad.calculate_smt(self.a1.year_q3, self.a2.year_q3, self.a3.year_q3)

    def year_q4_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.year_q4:
            return None
        return Triad.calculate_smt(self.a1.year_q4, self.a2.year_q4, self.a3.year_q4)

    def week1_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.week1:
            return None
        return Triad.calculate_smt(self.a1.week1, self.a2.week1, self.a3.week1)

    def week2_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.week2:
            return None
        return Triad.calculate_smt(self.a1.week2, self.a2.week2, self.a3.week2)

    def week3_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.week3:
            return None
        return Triad.calculate_smt(self.a1.week3, self.a2.week3, self.a3.week3)

    def week4_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.week4:
            return None
        return Triad.calculate_smt(self.a1.week4, self.a2.week4, self.a3.week4)

    def week5_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.week5:
            return None
        return Triad.calculate_smt(self.a1.week5, self.a2.week5, self.a3.week5)

    def mon_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.mon:
            return None
        return Triad.calculate_smt(self.a1.mon, self.a2.mon, self.a3.mon)

    def tue_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.tue:
            return None
        return Triad.calculate_smt(self.a1.tue, self.a2.tue, self.a3.tue)

    def wed_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.wed:
            return None
        return Triad.calculate_smt(self.a1.wed, self.a2.wed, self.a3.wed)

    def thu_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.thu:
            return None
        return Triad.calculate_smt(self.a1.thu, self.a2.thu, self.a3.thu)

    def mon_thu_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.mon_thu:
            return None
        return Triad.calculate_smt(self.a1.mon_thu, self.a2.mon_thu, self.a3.mon_thu)

    def fri_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.fri:
            return None
        return Triad.calculate_smt(self.a1.fri, self.a2.fri, self.a3.fri)

    def mon_fri_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.mon_fri:
            return None
        return Triad.calculate_smt(self.a1.mon_fri, self.a2.mon_fri, self.a3.mon_fri)

    def sat_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.sat:
            return None
        return Triad.calculate_smt(self.a1.sat, self.a2.sat, self.a3.sat)

    def asia_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.asia:
            return None
        return Triad.calculate_smt(self.a1.asia, self.a2.asia, self.a3.asia)

    def london_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.london:
            return None
        return Triad.calculate_smt(self.a1.london, self.a2.london, self.a3.london)

    def nyam_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.nyam:
            return None
        return Triad.calculate_smt(self.a1.nyam, self.a2.nyam, self.a3.nyam)

    def nypm_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.nypm:
            return None
        return Triad.calculate_smt(self.a1.nypm, self.a2.nypm, self.a3.nypm)

    def q1_90_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.q1_90m:
            return None
        return Triad.calculate_smt(self.a1.q1_90m, self.a2.q1_90m, self.a3.q1_90m)

    def q2_90_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.q2_90m:
            return None
        return Triad.calculate_smt(self.a1.q2_90m, self.a2.q2_90m, self.a3.q2_90m)

    def q3_90_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.q3_90m:
            return None
        return Triad.calculate_smt(self.a1.q3_90m, self.a2.q3_90m, self.a3.q3_90m)

    def q4_90_smt(self) -> Optional[Tuple[SMT, SMT, SMT]]:  # high, half, low
        if not self.a1.q4_90m:
            return None
        return Triad.calculate_smt(self.a1.q4_90m, self.a2.q4_90m, self.a3.q4_90m)

    @staticmethod
    def calculate_psp(a1_candle, a2_candle, a3_candle) -> PSP:
        if not a1_candle:
            return None
        green_candles = [x for x in [a1_candle, a2_candle, a3_candle] if x[0] > x[3]]
        if [0, 3] in len(green_candles):
            return None

        return (
            (TriadAsset.A1, a1_candle),
            (TriadAsset.A2, a2_candle),
            (TriadAsset.A3, a3_candle)
        )

    def prev_15m_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.prev_15m_candle, self.a2.prev_15m_candle, self.a3.prev_15m_candle)

    def prev_30m_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.prev_30m_candle, self.a2.prev_30m_candle, self.a3.prev_30m_candle)

    def current_30m_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.current_30m_candle, self.a2.current_30m_candle, self.a3.current_30m_candle)

    def prev_1h_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.prev_1h_candle, self.a2.prev_1h_candle, self.a3.prev_1h_candle)

    def current_1h_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.current_1h_candle, self.a2.current_1h_candle, self.a3.current_1h_candle)

    def prev_2h_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.prev_2h_candle, self.a2.prev_2h_candle, self.a3.prev_2h_candle)

    def current_2h_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.current_2h_candle, self.a2.current_2h_candle, self.a3.current_2h_candle)

    def prev_4h_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.prev_4h_candle, self.a2.prev_4h_candle, self.a3.prev_4h_candle)

    def current_4h_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.current_4h_candle, self.a2.current_4h_candle, self.a3.current_4h_candle)

    def prev_1d_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.prev_1d_candle, self.a2.prev_1d_candle, self.a3.prev_1d_candle)

    def current_1d_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.current_1d_candle, self.a2.current_1d_candle, self.a3.current_1d_candle)

    def prev_1w_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.prev_1w_candle, self.a2.prev_1w_candle, self.a3.prev_1w_candle)

    def current_1w_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.current_1w_candle, self.a2.current_1w_candle, self.a3.current_1w_candle)

    def prev_1month_candle_psp(self) -> PSP:
        return Triad.calculate_psp(self.a1.prev_1month_candle, self.a2.prev_1month_candle, self.a3.prev_1month_candle)

    def current_1month_candle_psp(self) -> PSP:
        return Triad.calculate_psp(
            self.a1.current_1month_candle, self.a2.current_1month_candle, self.a3.current_1month_candle)


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


def test_15m_reverse_generator(symbol) -> Reverse15mGenerator:
    candles_2024 = select_full_days_candles_15m(2024, symbol)
    candles_2025 = select_full_days_candles_15m(2025, symbol)
    candles = candles_2024 + candles_2025[:-1]

    index = len(candles) - 1

    while True:
        yield candles[index]
        index = index - 1


if __name__ == "__main__":
    try:
        triad = new_triad(
            ("BTCUSDT", "AVAXUSDT", "CRVUSDT"),
            (
                test_15m_reverse_generator("BTCUSDT"),
                test_15m_reverse_generator("AVAXUSDT"),
                test_15m_reverse_generator("CRVUSDT"),
            )
        )

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
