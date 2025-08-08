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
    get_prev_1month_from_to, get_current_1month_from_to, prev_quarters90m_ranges, prev_day_quarters_ranges, \
    prev_year_ranges, prev_weekday_ranges, prev_month_week_quarters_ranges, prev_year_quarters_ranges

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
    week5: Optional[QuarterLiq]   # joker week
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

        ranges_90m, t90m0 = prev_quarters90m_ranges(self.snapshot_date_readable)
        sweeps_90m = [(highest_sweep, lowest_sweep) for _ in ranges_90m]
        cum_90m_quarters = []
        for rng_90m in ranges_90m:
            cum_90m_quarters.append(self.prev_15m_candle if rng_90m[1] <= to_utc_datetime(
                self.prev_15m_candle[5]) < rng_90m[2] else None)

        ranges_dq, tdo = prev_day_quarters_ranges(self.snapshot_date_readable)
        sweeps_dq = [(highest_sweep, lowest_sweep) for _ in ranges_dq]
        cum_dq_quarters = []
        for rng_dq in ranges_dq:
            cum_dq_quarters.append(self.prev_15m_candle if rng_dq[1] <= to_utc_datetime(
                self.prev_15m_candle[5]) < rng_dq[2] else None)

        ranges_wd, two = prev_weekday_ranges(self.snapshot_date_readable)
        sweeps_wd = [(highest_sweep, lowest_sweep) for _ in ranges_wd]
        cum_wd_quarters = []
        for rng_wd in ranges_wd:
            cum_wd_quarters.append(self.prev_15m_candle if rng_wd[1] <= to_utc_datetime(
                self.prev_15m_candle[5]) < rng_wd[2] else None)

        ranges_mw, tmo = prev_month_week_quarters_ranges(self.snapshot_date_readable)
        sweeps_mw = [(highest_sweep, lowest_sweep) for _ in ranges_mw]
        cum_mw_quarters = []
        for rng_mw in ranges_mw:
            cum_mw_quarters.append(self.prev_15m_candle if rng_mw[1] <= to_utc_datetime(
                self.prev_15m_candle[5]) < rng_mw[2] else None)

        ranges_yq, tyo = prev_year_quarters_ranges(self.snapshot_date_readable)
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

        print('ea')

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
                res_rng = None
                if ranges_yq[i][1] <= to_utc_datetime(prev_candle[5]) < ranges_yq[i][2]:
                    if not cum_yq_quarters[i]:
                        sweeps_yq[i] = (highest_sweep, lowest_sweep)
                        cum_yq_quarters[i] = prev_candle
                    else:
                        cum_yq_quarters[i] = as_1_candle([prev_candle, cum_yq_quarters[i]])
                    if ranges_yq[i][1] == to_utc_datetime(prev_candle[5]):
                        half = (cum_yq_quarters[i][1] + cum_yq_quarters[i][2]) / 2
                        res_rng = (
                            cum_yq_quarters[i][5], to_date_str(ranges_yq[i][2]),
                            ranges_yq[i][2] < to_utc_datetime(self.snapshot_date_readable),
                            (cum_yq_quarters[i][1], cum_yq_quarters[i][1] < sweeps_yq[i][0]),
                            (half, sweeps_yq[i][1] <= half <= sweeps_yq[i][0]),
                            (cum_yq_quarters[i][2], cum_yq_quarters[i][2] > sweeps_yq[i][1])
                        )
                if res_rng and ranges_yq[i][0] == YearQuarter.YQ1:
                    self.year_q1 = res_rng
                elif res_rng and ranges_yq[i][0] == YearQuarter.YQ2:
                    self.year_q2 = res_rng
                elif res_rng and ranges_yq[i][0] == YearQuarter.YQ3:
                    self.year_q3 = res_rng
                elif res_rng and ranges_yq[i][0] == YearQuarter.YQ4:
                    self.year_q4 = res_rng

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

            # year_q4
            # year_q1
            # year_q2
            # year_q3
            # prev_month
            # prev_month_week_4
            # prev_month_week_5
            # week1
            # week2
            # week3
            # week4
            # prev_week_thu
            # prev_week_fri
            # nwog
            # mon
            # tue
            # wed
            # thu
            # mon_thu
            # fri
            # mon_fri
            # sat
            # mon_sat
            # nypm
            # asia
            # london
            # nyam
            # q4_90m
            # q1_90m
            # q2_90m
            # q3_90m

            highest_sweep = max(prev_candle[1], highest_sweep)
            lowest_sweep = min(prev_candle[2], lowest_sweep)

            if to_utc_datetime(prev_candle[5]) <= prev_year_from:
                break

            print('ea')
        print('ea')


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


SMT: TypeAlias = Optional[Tuple[
    Tuple[TriadAsset, str],  # asset and date when swept
    Tuple[TriadAsset, str]
]]

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

    prev_year_smt: Tuple[SMT, SMT, SMT]  # high, half, low

    year_q4_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    year_q1_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    year_q2_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    year_q3_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low

    prev_month_smt: Tuple[SMT, SMT, SMT]  # high, half, low

    prev_month_week_4_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    prev_month_week_5_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    week1_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    week2_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    week3_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    week4_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low

    prev_week_thu_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    prev_week_fri_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    mon_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    tue_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    wed_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    thu_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    mon_thu_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    fri_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    mon_fri_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    sat_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    mon_sat_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low

    prev_pm_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    asia_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    london_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    nyam_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low

    prev_q4_90_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    q1_90_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    q2_90_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
    q3_90_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low

    prev_15m_candle_psp: PSP
    prev_30m_candle_psp: PSP
    current_30m_candle_psp: PSP
    prev_1h_candle_psp: PSP
    current_1h_candle_psp: PSP
    prev_2h_candle_psp: PSP
    current_2h_candle_psp: PSP
    prev_4h_candle_psp: PSP
    current_4h_candle_psp: PSP
    prev_1d_candle_psp: PSP
    current_1d_candle_psp: PSP
    prev_1w_candle_psp: PSP
    current_1w_candle_psp: PSP
    prev_1month_candle_psp: PSP
    current_1month_candle_psp: PSP


def new_empty_triad(a1_symbol: str, a2_symbol: str, a3_symbol: str) -> Triad:
    return Triad(
        a1=new_empty_asset(a1_symbol),
        a2=new_empty_asset(a2_symbol),
        a3=new_empty_asset(a3_symbol),
        prev_year_smt=(None, None, None),
        year_q4_smt=None,
        year_q1_smt=None,
        year_q2_smt=None,
        year_q3_smt=None,
        prev_month_smt=(None, None, None),
        prev_month_week_4_smt=None,
        prev_month_week_5_smt=None,
        week1_smt=None,
        week2_smt=None,
        week3_smt=None,
        week4_smt=None,
        prev_week_thu_smt=None,
        prev_week_fri_smt=None,
        mon_smt=None,
        tue_smt=None,
        wed_smt=None,
        thu_smt=None,
        mon_thu_smt=None,
        fri_smt=None,
        mon_fri_smt=None,
        sat_smt=None,
        mon_sat_smt=None,
        prev_pm_smt=None,
        asia_smt=None,
        london_smt=None,
        nyam_smt=None,
        prev_q4_90_smt=None,
        q1_90_smt=None,
        q2_90_smt=None,
        q3_90_smt=None,
        prev_15m_candle_psp=None,
        prev_30m_candle_psp=None,
        current_30m_candle_psp=None,
        prev_1h_candle_psp=None,
        current_1h_candle_psp=None,
        prev_2h_candle_psp=None,
        current_2h_candle_psp=None,
        prev_4h_candle_psp=None,
        current_4h_candle_psp=None,
        prev_1d_candle_psp=None,
        current_1d_candle_psp=None,
        prev_1w_candle_psp=None,
        current_1w_candle_psp=None,
        prev_1month_candle_psp=None,
        current_1month_candle_psp=None,
    )


def new_triad(symbols_tuple: Tuple[str, str, str],
              generators_tuple: Tuple[Reverse15mGenerator, Reverse15mGenerator, Reverse15mGenerator]) -> Triad:
    res = new_empty_triad(*symbols_tuple)

    res.a1.populate(generators_tuple[0])
    res.a2.populate(generators_tuple[1])
    res.a3.populate(generators_tuple[2])

    return res


def test_15m_reverse_generator(symbol) -> Reverse15mGenerator:
    candles = (select_full_days_candles_15m(2024, symbol)
               + select_full_days_candles_15m(2025, symbol))

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
