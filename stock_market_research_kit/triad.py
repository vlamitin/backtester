import json
import time
from dataclasses import dataclass, asdict
from datetime import timedelta, datetime
from typing import Tuple, Optional, List, Dict, TypeAlias

from stock_market_research_kit.asset import QuarterLiq, Asset, new_empty_asset, Candles15mGenerator, TargetPercent
from stock_market_research_kit.candle import InnerCandle, as_1_candle, as_1month_candles, as_1w_candles, \
    as_1d_candles, as_4h_candles, as_2h_candles, as_1h_candles, as_30m_candles, AsCandles, PriceDate
from stock_market_research_kit.quarter import MonthWeek, DayQuarter, WeekDay, YearQuarter
from utils.date_utils import to_utc_datetime, to_date_str, get_prev_30m_from_to, get_current_30m_from_to, \
    get_prev_1h_from_to, get_current_1h_from_to, get_prev_2h_from_to, get_current_2h_from_to, get_prev_4h_from_to, \
    get_current_4h_from_to, get_prev_1d_from_to, get_current_1d_from_to, get_prev_1w_from_to, get_current_1w_from_to, \
    get_prev_1month_from_to, get_current_1month_from_to, GetDateRange, humanize_timedelta, \
    to_ny_date_str, quarters_by_time, month_week_quarters_ranges, weekday_ranges, day_quarters_ranges, \
    year_quarters_ranges, log_info_ny, log_warn_ny

Target: TypeAlias = Tuple[int, str, str, TargetPercent, TargetPercent, TargetPercent]  # level, direction, label, tp*3
TrueOpen: TypeAlias = Tuple[str, float, float]  # label, price, percent_from_current


@dataclass
class PSP:
    a1_candle: InnerCandle
    a2_candle: InnerCandle
    a3_candle: InnerCandle

    confirmed: bool  # next candle didn't sweep it
    closed: bool
    swept_from_to: Optional[List[Tuple[str, str, str]]]  # symbol, from, to_date


@dataclass
class SMT:
    a1q: QuarterLiq
    a2q: QuarterLiq
    a3q: QuarterLiq

    type: str  # 'high' 'low' 'half_high' or 'half_low'
    first_appeared: str

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


SMTLevels: TypeAlias = Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]  # high, half, low


def new_empty_smt(a1q, a2q, a3q) -> SMT:
    return SMT(
        a1q=a1q,
        a2q=a2q,
        a3q=a3q,
        type='',
        first_appeared='',
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


def percent_from_current(current: float, target: float) -> float:
    return round((target - current) / current * 100, 2)


@dataclass
class Triad:
    a1: Asset
    a2: Asset
    a3: Asset

    def true_opens(
            self
    ) -> Tuple[List[TrueOpen], List[TrueOpen], List[TrueOpen]]:
        def to_tuple(label: str, curr: float, pd: Optional[PriceDate]) -> Optional[TrueOpen]:
            if not pd:
                return None
            return label, pd[0], percent_from_current(curr, pd[0])

        symbols = [self.a1.symbol, self.a2.symbol, self.a3.symbol]
        res = ([], [], [])
        for i, asset in enumerate([self.a1, self.a2, self.a3]):
            res[i].extend(sorted(
                [x for x in [
                    to_tuple('tyo', asset.prev_15m_candle[3], asset.true_yo),
                    to_tuple('tmo', asset.prev_15m_candle[3], asset.true_mo),
                    to_tuple('two', asset.prev_15m_candle[3], asset.true_wo),
                    to_tuple('tdo', asset.prev_15m_candle[3], asset.true_do),
                    to_tuple('t90mo', asset.prev_15m_candle[3], asset.true_90m_open),
                    (symbols[i], asset.prev_15m_candle[3], 0),
                ] if x],
                key=lambda x: x[2],
                reverse=True
            ))

        return res

    def ql_long_targets(
            self, qls: List[Tuple[int, str, QuarterLiq, QuarterLiq, QuarterLiq]]  # level, label, ql1-ql3
    ) -> List[Target]:
        result = []

        for level, label, a1_ql, a2_ql, a3_ql in qls:
            if not a1_ql or not a2_ql or not a3_ql:
                continue
            _, _, _, a1_high, a1_half, _ = a1_ql
            _, _, _, a2_high, a2_half, _ = a2_ql
            _, _, _, a3_high, a3_half, _ = a3_ql
            if len([x for x in [a1_half, a2_half, a3_half] if not x[1]]) == 3:
                pfc1 = percent_from_current(self.a1.prev_15m_candle[3], a1_half[0])
                pfc2 = percent_from_current(self.a2.prev_15m_candle[3], a2_half[0])
                pfc3 = percent_from_current(self.a3.prev_15m_candle[3], a3_half[0])
                if pfc1 > 0 and pfc2 > 0 and pfc3 > 0:
                    result.append((
                        level,
                        "half_high",
                        label,
                        (a1_half[0], pfc1),
                        (a2_half[0], pfc2),
                        (a3_half[0], pfc3)
                    ))
            if len([x for x in [a1_high, a2_high, a3_high] if not x[1]]) == 3:
                result.append((
                    level,
                    "high",
                    label,
                    (a1_high[0], percent_from_current(self.a1.prev_15m_candle[3], a1_high[0])),
                    (a2_high[0], percent_from_current(self.a2.prev_15m_candle[3], a2_high[0])),
                    (a3_high[0], percent_from_current(self.a3.prev_15m_candle[3], a3_high[0]))
                ))

        return result

    def ql_short_targets(
            self, qls: List[Tuple[int, str, QuarterLiq, QuarterLiq, QuarterLiq]]  # label, ql1-ql3
    ) -> List[Target]:
        result = []

        for level, label, a1_ql, a2_ql, a3_ql in qls:
            if not a1_ql or not a2_ql or not a3_ql:
                continue
            _, _, _, _, a1_half, a1_low = a1_ql
            _, _, _, _, a2_half, a2_low = a2_ql
            _, _, _, _, a3_half, a3_low = a3_ql
            if len([x for x in [a1_half, a2_half, a3_half] if not x[1]]) == 3:
                pfc1 = percent_from_current(self.a1.prev_15m_candle[3], a1_half[0])
                pfc2 = percent_from_current(self.a2.prev_15m_candle[3], a2_half[0])
                pfc3 = percent_from_current(self.a3.prev_15m_candle[3], a3_half[0])
                if pfc1 < 0 and pfc2 < 0 and pfc3 < 0:
                    result.append((
                        level,
                        "half_low",
                        label,
                        (a1_half[0], pfc1),
                        (a2_half[0], pfc2),
                        (a3_half[0], pfc3)
                    ))
            if len([x for x in [a1_low, a2_low, a3_low] if not x[1]]) == 3:
                result.append((
                    level,
                    "low",
                    label,
                    (a1_low[0], percent_from_current(self.a1.prev_15m_candle[3], a1_low[0])),
                    (a2_low[0], percent_from_current(self.a2.prev_15m_candle[3], a2_low[0])),
                    (a3_low[0], percent_from_current(self.a3.prev_15m_candle[3], a3_low[0]))
                ))

        return result

    def long_targets(self) -> List[Target]:
        return sorted(
            self.ql_long_targets(self.actual_prev_qls()),
            key=lambda t: t[1][1]
        )

    def short_targets(self) -> List[Target]:
        return sorted(
            self.ql_short_targets(self.actual_prev_qls()),
            key=lambda t: t[1][1],
            reverse=True
        )

    def actual_prev_qls(self) -> List[Tuple[int, str, QuarterLiq, QuarterLiq, QuarterLiq]]:
        result = []

        curr_yq, curr_mw, curr_wd, curr_dq, curr_q90m = quarters_by_time(self.a1.snapshot_date_readable)

        # for q90m, _, _ in quarters90m_ranges(self.a1.snapshot_date_readable)[0]:
        #     match q90m:
        #         case Quarter90m.Q1_90m:
        #             result.append(('q1_90m', self.a1.q1_90m, self.a2.q1_90m, self.a3.q1_90m))
        #         case Quarter90m.Q2_90m:
        #             result.append(('q2_90m', self.a1.q2_90m, self.a2.q2_90m, self.a3.q2_90m))
        #         case Quarter90m.Q3_90m:
        #             result.append(('q3_90m', self.a1.q3_90m, self.a2.q3_90m, self.a3.q3_90m))
        #         case Quarter90m.Q4_90m:
        #             result.append(('q4_90m', self.a1.q4_90m, self.a2.q4_90m, self.a3.q4_90m))

        for dq, _, _ in day_quarters_ranges(self.a1.snapshot_date_readable)[0]:
            if dq == curr_dq:
                continue
            match dq:
                case DayQuarter.DQ1_Asia:
                    result.append((5, 'asia', self.a1.asia, self.a2.asia, self.a3.asia))
                case DayQuarter.DQ2_London:
                    result.append((5, 'london', self.a1.london, self.a2.london, self.a3.london))
                case DayQuarter.DQ3_NYAM:
                    result.append((5, 'nyam', self.a1.nyam, self.a2.nyam, self.a3.nyam))
                case DayQuarter.DQ4_NYPM:
                    result.append((5, 'nypm', self.a1.nypm, self.a2.nypm, self.a3.nypm))

        for wd, _, _ in weekday_ranges(self.a1.snapshot_date_readable)[0]:
            if wd == curr_wd:
                continue
            match wd:
                case WeekDay.Mon:
                    result.append((4, 'mon', self.a1.mon, self.a2.mon, self.a3.mon))
                case WeekDay.Tue:
                    result.append((4, 'tue', self.a1.tue, self.a2.tue, self.a3.tue))
                case WeekDay.Wed:
                    result.append((4, 'wed', self.a1.wed, self.a2.wed, self.a3.wed))
                case WeekDay.Thu:
                    result.append((4, 'thu', self.a1.thu, self.a2.thu, self.a3.thu))
                case WeekDay.MonThu:
                    result.append((4, 'mon_thu', self.a1.mon_thu, self.a2.mon_thu, self.a3.mon_thu))
                case WeekDay.Fri:
                    result.append((4, 'fri', self.a1.fri, self.a2.fri, self.a3.fri))
                case WeekDay.MonFri:
                    result.append((4, 'mon_fri', self.a1.mon_fri, self.a2.mon_fri, self.a3.mon_fri))
                case WeekDay.Sat:
                    result.append((4, 'sat', self.a1.sat, self.a2.sat, self.a3.sat))

        for mw, _, _ in month_week_quarters_ranges(self.a1.snapshot_date_readable)[0]:
            if mw == curr_mw:
                continue
            match mw:
                case MonthWeek.MW1:
                    result.append((3, 'week1', self.a1.week1, self.a2.week1, self.a3.week1))
                case MonthWeek.MW2:
                    result.append((3, 'week2', self.a1.week2, self.a2.week2, self.a3.week2))
                case MonthWeek.MW3:
                    result.append((3, 'week3', self.a1.week3, self.a2.week3, self.a3.week3))
                case MonthWeek.MW4:
                    result.append((3, 'week4', self.a1.week4, self.a2.week4, self.a3.week4))
                case MonthWeek.MW5:
                    result.append((3, 'week5', self.a1.week5, self.a2.week5, self.a3.week5))

        for yq, _, _ in year_quarters_ranges(self.a1.snapshot_date_readable)[0]:
            if yq == curr_yq:
                continue
            match yq:
                case YearQuarter.YQ1:
                    result.append((2, 'year_q1', self.a1.year_q1, self.a2.year_q1, self.a3.year_q1))
                case YearQuarter.YQ2:
                    result.append((2, 'year_q2', self.a1.year_q2, self.a2.year_q2, self.a3.year_q2))
                case YearQuarter.YQ3:
                    result.append((2, 'year_q3', self.a1.year_q3, self.a2.year_q3, self.a3.year_q3))
                case YearQuarter.YQ4:
                    result.append((2, 'year_q4', self.a1.year_q4, self.a2.year_q4, self.a3.year_q4))

        result.append((1, 'prev_year', self.a1.prev_year, self.a2.prev_year, self.a3.prev_year))
        return result

    def new_smt(
            self, next_tick: str, a1_ql, a2_ql, a3_ql: QuarterLiq
    ) -> SMTLevels:
        _, _, _, a1_high, a1_half, a1_low = a1_ql
        _, _, _, a2_high, a2_half, a2_low = a2_ql
        _, _, _, a3_high, a3_half, a3_low = a3_ql

        # _get_15m_candles_range_time = time.perf_counter()
        a1_sweep_candles_15m = self.a1.get_15m_candles_range(next_tick, self.a1.snapshot_date_readable)
        a2_sweep_candles_15m = self.a2.get_15m_candles_range(next_tick, self.a2.snapshot_date_readable)
        a3_sweep_candles_15m = self.a3.get_15m_candles_range(next_tick, self.a3.snapshot_date_readable)
        # log_info_ny(f"get_15m_candles_range took {(time.perf_counter() - _get_15m_candles_range_time):.6f} seconds")

        a1_q_close = None if len(a1_sweep_candles_15m) == 0 else a1_sweep_candles_15m[0][0]
        a2_q_close = None if len(a2_sweep_candles_15m) == 0 else a2_sweep_candles_15m[0][0]
        a3_q_close = None if len(a3_sweep_candles_15m) == 0 else a3_sweep_candles_15m[0][0]

        is_high = len([x for x in [a1_high[1], a2_high[1], a3_high[1]] if x]) not in [0, 3]
        high_smt = new_empty_smt(a1_ql, a2_ql, a3_ql) if is_high else None
        if high_smt:
            high_smt.type = 'high'
            high_smt.a1_sweep_candles_15m = a1_sweep_candles_15m
            high_smt.a2_sweep_candles_15m = a2_sweep_candles_15m
            high_smt.a3_sweep_candles_15m = a3_sweep_candles_15m
            for i in range(len(high_smt.a1_sweep_candles_15m)):
                c1, c2, c3 = high_smt.a1_sweep_candles_15m[i], high_smt.a2_sweep_candles_15m[i], \
                    high_smt.a3_sweep_candles_15m[i]
                if len([x for x in [c1[1] > a1_high[0], c2[1] > a2_high[0], c3[1] > a3_high[0]] if x]) not in [0, 3]:
                    high_smt.first_appeared = c1[5]
                    break

        is_low = len([x for x in [a1_low[1], a2_low[1], a3_low[1]] if x]) not in [0, 3]
        low_smt = new_empty_smt(a1_ql, a2_ql, a3_ql) if is_low else None
        if low_smt:
            low_smt.type = 'low'
            low_smt.a1_sweep_candles_15m = a1_sweep_candles_15m
            low_smt.a2_sweep_candles_15m = a2_sweep_candles_15m
            low_smt.a3_sweep_candles_15m = a3_sweep_candles_15m
            for i in range(len(low_smt.a1_sweep_candles_15m)):
                c1, c2, c3 = low_smt.a1_sweep_candles_15m[i], low_smt.a2_sweep_candles_15m[i], \
                    low_smt.a3_sweep_candles_15m[i]
                if len([x for x in [c1[2] < a1_low[0], c2[2] < a2_low[0], c3[2] < a3_low[0]] if x]) not in [0, 3]:
                    low_smt.first_appeared = c1[5]
                    break

        is_half_swept = len([x for x in [a1_half[1], a2_half[1], a3_half[1]] if x]) not in [0, 3]
        is_half_high = a1_q_close and (a1_low[0] <= a1_q_close < a1_half[0] and
                                       a2_low[0] <= a2_q_close < a2_half[0] and
                                       a3_low[0] <= a3_q_close < a3_half[0])
        is_half_low = a1_q_close and (a1_half[0] < a1_q_close <= a1_high[0] and
                                      a2_half[0] < a2_q_close <= a2_high[0] and
                                      a3_half[0] < a3_q_close <= a3_high[0])

        half_smt = new_empty_smt(a1_ql, a2_ql, a3_ql) \
            if is_half_swept and (is_half_low or is_half_high) else None
        if half_smt:
            half_smt.type = 'half_high' if is_half_high else 'half_low' if is_half_low else ''
            half_smt.a1_sweep_candles_15m = a1_sweep_candles_15m
            half_smt.a2_sweep_candles_15m = a2_sweep_candles_15m
            half_smt.a3_sweep_candles_15m = a3_sweep_candles_15m
            for i in range(len(half_smt.a1_sweep_candles_15m)):
                c1, c2, c3 = half_smt.a1_sweep_candles_15m[i], half_smt.a2_sweep_candles_15m[i], \
                    half_smt.a3_sweep_candles_15m[i]
                if len([x for x in [
                    c1[1] > a1_half[0] > c1[2],
                    c2[1] > a2_half[0] > c2[2],
                    c3[1] > a3_half[0] > c3[2]
                ] if x]) not in [0, 3]:
                    half_smt.first_appeared = c1[5]
                    break

        return high_smt, half_smt, low_smt

    def calculate_psps(
            self,
            prev_candle_range: Tuple[datetime, datetime],
            current_candle_range_getter: GetDateRange,
            as_candles: AsCandles,
            smt: SMT
    ) -> List[PSP]:
        _as_1_candle_time = time.perf_counter()

        psps = []
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

        _as_1_candle_took = time.perf_counter() - _as_1_candle_time
        if _as_1_candle_took > 0.05:
            log_warn_ny(f"as_1_candle took {_as_1_candle_took:.6f} seconds, as_candles is {as_candles.__name__}")

        if len(a1_candles) == 0:
            return []

        a1_min, a1_max = a1_candles[0][2], a1_candles[0][1]
        a2_min, a2_max = a2_candles[0][2], a2_candles[0][1]
        a3_min, a3_max = a3_candles[0][2], a3_candles[0][1]

        _psps_calculation_time = time.perf_counter()
        for i in range(len(a1_candles)):
            c_end = current_candle_range_getter(a1_candles[i][5])[1]
            snap_end = to_utc_datetime(self.a1.snapshot_date_readable) - timedelta(seconds=1)
            for j in range(len(psps)):
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
                    closed=c_end <= snap_end,
                    swept_from_to=None
                ))
            finally:
                a1_min, a1_max = min(a1_min, a1_candles[i][2]), max(a1_max, a1_candles[i][1])
                a2_min, a2_max = min(a2_min, a2_candles[i][2]), max(a2_max, a2_candles[i][1])
                a3_min, a3_max = min(a3_min, a3_candles[i][2]), max(a3_max, a3_candles[i][1])

        _psps_calculation_took = time.perf_counter() - _psps_calculation_time
        if _psps_calculation_took > 0.02:
            log_warn_ny(
                f"psps_calculation took {_psps_calculation_took:.6f} seconds, as_candles is {as_candles.__name__}")

        return psps

    def with_15m_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with 15m PSPs
        smt.psps_15m = self.calculate_psps(
            (to_utc_datetime(next_tick) - timedelta(minutes=15), to_utc_datetime(next_tick)),
            lambda x: (to_utc_datetime(x), to_utc_datetime(x) + timedelta(minutes=14)),
            lambda candles: candles,
            smt
        )
        return smt

    def with_30m_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with 30m PSPs
        smt.psps_30m = self.calculate_psps(
            get_prev_30m_from_to(next_tick),
            get_current_30m_from_to,
            as_30m_candles,
            smt
        )
        return smt

    def with_1h_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with 1h PSPs
        smt.psps_1h = self.calculate_psps(
            get_prev_1h_from_to(next_tick),
            get_current_1h_from_to,
            as_1h_candles,
            smt
        )
        return smt

    def with_2h_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with 2h PSPs
        smt.psps_2h = self.calculate_psps(
            get_prev_2h_from_to(next_tick),
            get_current_2h_from_to,
            as_2h_candles,
            smt
        )
        return smt

    def with_4h_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with 4h PSPs
        smt.psps_4h = self.calculate_psps(
            get_prev_4h_from_to(next_tick),
            get_current_4h_from_to,
            as_4h_candles,
            smt
        )
        return smt

    def with_day_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with day PSPs
        smt.psps_1d = self.calculate_psps(
            get_prev_1d_from_to(next_tick),
            get_current_1d_from_to,
            as_1d_candles,
            smt
        )
        return smt

    def with_week_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with week PSPs
        smt.psps_1_week = self.calculate_psps(
            get_prev_1w_from_to(next_tick),
            get_current_1w_from_to,
            as_1w_candles,
            smt
        )
        return smt

    def with_month_psps(self, next_tick: str, smt: SMT) -> SMT:  # enriches smt with week PSPs
        smt.psps_1_month = self.calculate_psps(
            get_prev_1month_from_to(next_tick),
            get_current_1month_from_to,
            as_1month_candles,
            smt
        )
        return smt

    def prev_year_smt(self) -> Optional[SMTLevels]:  # high, half, low
        # _prev_year_smt_time = time.perf_counter()
        if not self.a1.prev_year:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.prev_year[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
        # _new_smt_time = time.perf_counter()
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.prev_year, self.a2.prev_year, self.a3.prev_year
        )
        # log_info_ny(f"new_smt took {(time.perf_counter() - _new_smt_time):.6f} seconds")

        # _with_psp_time = time.perf_counter()
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
        # log_info_ny(f"with_psp took {(time.perf_counter() - _with_psp_time):.6f} seconds")
        #         log_info_ny(f"prev_year_smt took {(time.perf_counter() - _prev_year_smt_time):.6f} seconds")

        return high_smt, half_smt, low_smt

    def year_q1_smt(self) -> Optional[SMTLevels]:  # high, half, low
        # _year_q1_smt_time = time.perf_counter()
        if not self.a1.year_q1:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.year_q1[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None

        # _new_smt_time = time.perf_counter()
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.year_q1, self.a2.year_q1, self.a3.year_q1
        )
        # log_info_ny(f"new_smt took {(time.perf_counter() - _new_smt_time):.6f} seconds")

        _with_psp_time = time.perf_counter()
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
        # log_info_ny(f"with_psp took {(time.perf_counter() - _with_psp_time):.6f} seconds")
        # log_info_ny(f"year_q1_smt took {(time.perf_counter() - _year_q1_smt_time):.6f} seconds")
        return high_smt, half_smt, low_smt

    def year_q2_smt(self) -> Optional[SMTLevels]:  # high, half, low
        #         _year_q2_smt_time = time.perf_counter()
        if not self.a1.year_q2:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.year_q2[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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
        #         log_info_ny(f"year_q2_smt took {(time.perf_counter() - _year_q2_smt_time):.6f} seconds")
        return high_smt, half_smt, low_smt

    def year_q3_smt(self) -> Optional[SMTLevels]:  # high, half, low
        #         _year_q3_smt_time = time.perf_counter()
        if not self.a1.year_q3:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.year_q3[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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
        #         log_info_ny(f"year_q3_smt took {(time.perf_counter() - _year_q3_smt_time):.6f} seconds")
        return high_smt, half_smt, low_smt

    def year_q4_smt(self) -> Optional[SMTLevels]:  # high, half, low
        #         _year_q4_smt_time = time.perf_counter()
        if not self.a1.year_q4:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.year_q4[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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
        #         log_info_ny(f"year_q4_smt took {(time.perf_counter() - _year_q4_smt_time):.6f} seconds")
        return high_smt, half_smt, low_smt

    def week1_smt(self) -> Optional[SMTLevels]:  # high, half, low
        #         _week1_smt_time = time.perf_counter()
        if not self.a1.week1:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.week1[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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
        #         log_info_ny(f"week1_smt took {(time.perf_counter() - _week1_smt_time):.6f} seconds")
        return high_smt, half_smt, low_smt

    def week2_smt(self) -> Optional[SMTLevels]:  # high, half, low
        #         _week2_smt_time = time.perf_counter()
        if not self.a1.week2:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.week2[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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
        #         log_info_ny(f"week2_smt took {(time.perf_counter() - _week2_smt_time):.6f} seconds")
        return high_smt, half_smt, low_smt

    def week3_smt(self) -> Optional[SMTLevels]:  # high, half, low
        #         _week3_smt_time = time.perf_counter()
        if not self.a1.week3:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.week3[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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
        #         log_info_ny(f"week3_smt took {(time.perf_counter() - _week3_smt_time):.6f} seconds")
        return high_smt, half_smt, low_smt

    def week4_smt(self) -> Optional[SMTLevels]:  # high, half, low
        #         _week4_smt_time = time.perf_counter()
        if not self.a1.week4:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.week4[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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
        #         log_info_ny(f"week4_smt took {(time.perf_counter() - _week4_smt_time):.6f} seconds")
        return high_smt, half_smt, low_smt

    def week5_smt(self) -> Optional[SMTLevels]:  # high, half, low
        #         _week5_smt_time = time.perf_counter()
        if not self.a1.week5:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.week5[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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
        #         log_info_ny(f"week5_smt took {(time.perf_counter() - _week5_smt_time):.6f} seconds")
        return high_smt, half_smt, low_smt

    def mon_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.mon:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.mon[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def tue_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.tue:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.tue[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def wed_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.wed:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.wed[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def thu_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.thu:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.thu[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def mon_thu_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.mon_thu:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.mon_thu[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def fri_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.fri:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.fri[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def mon_fri_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.mon_fri:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.mon_fri[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def sat_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.sat:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.sat[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def asia_smt(self) -> Optional[SMTLevels]:  # high, half, low
        # _asia_smt_time = time.perf_counter()
        if not self.a1.asia:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.asia[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None

        _new_smt_time = time.perf_counter()
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.asia, self.a2.asia, self.a3.asia
        )
        # log_info_ny(f"new_smt took {(time.perf_counter() - _new_smt_time):.6f} seconds")

        _with_psp_time = time.perf_counter()
        if high_smt:
            # high_smt = self.with_30m_psps(next_tick, high_smt)
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
        if half_smt:
            # half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
            # low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        # log_info_ny(f"with_psp took {(time.perf_counter() - _with_psp_time):.6f} seconds")
        # log_info_ny(f"asia_smt took {(time.perf_counter() - _asia_smt_time):.6f} seconds")

        return high_smt, half_smt, low_smt

    def london_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.london:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.london[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.london, self.a2.london, self.a3.london
        )
        if high_smt:
            # high_smt = self.with_30m_psps(next_tick, high_smt)
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
        if half_smt:
            # half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
            # low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def nyam_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.nyam:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.nyam[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.nyam, self.a2.nyam, self.a3.nyam
        )
        if high_smt:
            # high_smt = self.with_30m_psps(next_tick, high_smt)
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
        if half_smt:
            #  half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
            #  low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def nypm_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.nypm:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.nypm[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.nypm, self.a2.nypm, self.a3.nypm
        )
        if high_smt:
            # high_smt = self.with_30m_psps(next_tick, high_smt)
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
        if half_smt:
            # half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
            # low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def q1_90_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.q1_90m:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.q1_90m[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def q2_90_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.q2_90m:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.q2_90m[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def q3_90_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.q3_90m:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.q3_90m[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def q4_90_smt(self) -> Optional[SMTLevels]:  # high, half, low
        if not self.a1.q4_90m:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.q4_90m[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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

    def actual_smt_psp(self) -> List[Tuple[int, str, Optional[SMTLevels]]]:  # level, label, smt levels
        return [
            (1, 'prev_year SMT', self.prev_year_smt()),
            (2, 'year_q1 SMT', self.year_q1_smt()),
            (2, 'year_q2 SMT', self.year_q2_smt()),
            (2, 'year_q3 SMT', self.year_q3_smt()),
            (2, 'year_q4 SMT', self.year_q4_smt()),
            (3, 'week1 SMT', self.week1_smt()),
            (3, 'week2 SMT', self.week2_smt()),
            (3, 'week3 SMT', self.week3_smt()),
            (3, 'week4 SMT', self.week4_smt()),
            (3, 'week5 SMT', self.week5_smt()),
            (4, 'mon SMT', self.mon_smt()),
            (4, 'tue SMT', self.tue_smt()),
            (4, 'wed SMT', self.wed_smt()),
            (4, 'thu SMT', self.thu_smt()),
            (4, 'mon_thu SMT', self.mon_thu_smt()),
            (4, 'fri SMT', self.fri_smt()),
            (4, 'mon_fri SMT', self.mon_fri_smt()),
            (4, 'sat SMT', self.sat_smt()),
            (5, 'asia SMT', self.asia_smt()),
            (5, 'london SMT', self.london_smt()),
            (5, 'nyam SMT', self.nyam_smt()),
            (5, 'nypm SMT', self.nypm_smt()),
            #     (6, 'q1_90 SMT', self.q1_90_smt()),
            #     (6, 'q2_90 SMT', self.q2_90_smt()),
            #     (6, 'q3_90 SMT', self.q3_90_smt()),
            #     (6, 'q4_90 SMT', self.q4_90_smt()),
        ]


def new_smt_found(
        l_old: List[Tuple[int, str, Optional[SMTLevels]]],
        l_new: List[Tuple[int, str, Optional[SMTLevels]]],
) -> List[Tuple[int, str, SMT]]:  # level, label, smt
    result = []

    d_old = {}
    for level, label, smt_tuple_old in l_old:
        d_old[f"{level}_{label}"] = (level, label, smt_tuple_old)

    for level, label, smt_tuple_new in l_new:
        if not smt_tuple_new:
            continue
        high_new, half_new, low_new = smt_tuple_new

        _, _, smt_tuple_old = d_old.get(f"{level}_{label}", None)
        if not smt_tuple_old:
            for smt in [high_new, half_new, low_new]:
                if smt:
                    result.append((level, label, smt))
            continue

        high_old, half_old, low_old = smt_tuple_old
        for smts in [(high_new, high_old), (half_new, half_old), (low_new, low_old)]:
            if smts[0] and not smts[1]:
                result.append((level, label, smts[0]))

    return result


def smt_dict_old_smt_cancelled(
        l_old: List[Tuple[int, str, Optional[SMTLevels]]],
        l_new: List[Tuple[int, str, Optional[SMTLevels]]],
) -> List[Tuple[int, str, SMT]]:  # level, label, smt
    result = []

    d_new = {}
    for level, label, smt_tuple_new in l_new:
        d_new[f"{level}_{label}"] = (level, label, smt_tuple_new)

    for level, label, smt_tuple_old in l_old:
        if not smt_tuple_old:
            continue
        high_old, half_old, low_old = smt_tuple_old

        _, _, smt_tuple_new = d_new.get(f"{level}_{label}", None)
        if not smt_tuple_new:
            for smt in [high_old, half_old, low_old]:
                if smt:
                    result.append((level, label, smt))
            continue

        high_new, half_new, low_new = smt_tuple_new
        for smts in [(high_old, high_new), (half_old, half_new), (low_old, low_new)]:
            if smts[0] and not smts[1]:
                result.append((level, label, smts[0]))

    return result


def targets_reached(
        last_candles: Tuple[InnerCandle, InnerCandle, InnerCandle],
        targets_old: List[Target],
        targets_new: List[Target]
) -> List[Tuple[int, str, str, int, float]]:  # level, direction, label, asset_index, price
    result = []
    if not targets_old:
        return []
    for level, direction, label, tp_a1, tp_a2, tp_a3 in targets_old:
        if not any(f"{direction}_{label}" == f"{x[1]}_{x[2]}" for x in targets_new):
            if last_candles[0][2] < tp_a1[0] < last_candles[0][1]:
                result.append((level, direction, label, 0, tp_a1[0]))
            if last_candles[1][2] < tp_a2[0] < last_candles[1][1]:
                result.append((level, direction, label, 1, tp_a2[0]))
            if last_candles[2][2] < tp_a3[0] < last_candles[2][1]:
                result.append((level, direction, label, 2, tp_a3[0]))

    return result


def targets_new_appeared(
        targets_old: List[Target],
        targets_new: List[Target]
) -> List[Target]:
    result = []
    if not targets_new:
        return []
    for level, direction, label, tp_a1, tp_a2, tp_a3 in targets_new:
        if not any(f"{direction}_{label}" == f"{x[1]}_{x[2]}" for x in targets_old):
            result.append((level, direction, label, tp_a1, tp_a2, tp_a3))

    return result


def calc_psp_changed(
        symbols: Tuple[str, str, str],
        l_old: List[Tuple[int, str, Optional[SMTLevels]]],
        l_new: List[Tuple[int, str, Optional[SMTLevels]]],
) -> List[Tuple[int, str, str, str, str, str, str]]:
    # returns (smt_level, smt_key, smt_type, smt_flags, psp_key, psp_date, possible|closed|confirmed|swept)
    result = []

    d_old = {}
    for level, label, smt_tuple_old in l_old:
        d_old[f"{level}_{label}"] = (level, label, smt_tuple_old)

    for level, label, smt_tuple_new in l_new:
        _, _, smt_tuple_old = d_old.get(f"{level}_{label}", None)
        if not smt_tuple_new or not smt_tuple_old:
            continue
        high_new, half_new, low_new = smt_tuple_new
        high_old, half_old, low_old = smt_tuple_old

        if high_new and high_old:
            result.extend(
                [(level, label, high_new.type, to_smt_flags(symbols, high_new), '15m', x[0], x[1]) for x in
                 psps_changed(high_old.psps_15m, high_new.psps_15m)])
            result.extend(
                [(level, label, high_new.type, to_smt_flags(symbols, high_new), '30m', x[0], x[1]) for x in
                 psps_changed(high_old.psps_30m, high_new.psps_30m)])
            result.extend(
                [(level, label, high_new.type, to_smt_flags(symbols, high_new), '1h', x[0], x[1]) for x in
                 psps_changed(high_old.psps_1h, high_new.psps_1h)])
            result.extend(
                [(level, label, high_new.type, to_smt_flags(symbols, high_new), '2h', x[0], x[1]) for x in
                 psps_changed(high_old.psps_2h, high_new.psps_2h)])
            result.extend(
                [(level, label, high_new.type, to_smt_flags(symbols, high_new), '4h', x[0], x[1]) for x in
                 psps_changed(high_old.psps_4h, high_new.psps_4h)])
            result.extend(
                [(level, label, high_new.type, to_smt_flags(symbols, high_new), '1d', x[0], x[1]) for x in
                 psps_changed(high_old.psps_1d, high_new.psps_1d)])
            result.extend([(level, label, high_new.type, to_smt_flags(symbols, high_new), '1_week', x[0], x[1]) for x in
                           psps_changed(high_old.psps_1_week, high_new.psps_1_week)])

        if half_new and half_old:
            result.extend(
                [(level, label, half_new.type, to_smt_flags(symbols, half_new), '15m', x[0], x[1]) for x in
                 psps_changed(half_old.psps_15m, half_new.psps_15m)])
            result.extend(
                [(level, label, half_new.type, to_smt_flags(symbols, half_new), '30m', x[0], x[1]) for x in
                 psps_changed(half_old.psps_30m, half_new.psps_30m)])
            result.extend(
                [(level, label, half_new.type, to_smt_flags(symbols, half_new), '1h', x[0], x[1]) for x in
                 psps_changed(half_old.psps_1h, half_new.psps_1h)])
            result.extend(
                [(level, label, half_new.type, to_smt_flags(symbols, half_new), '2h', x[0], x[1]) for x in
                 psps_changed(half_old.psps_2h, half_new.psps_2h)])
            result.extend(
                [(level, label, half_new.type, to_smt_flags(symbols, half_new), '4h', x[0], x[1]) for x in
                 psps_changed(half_old.psps_4h, half_new.psps_4h)])
            result.extend(
                [(level, label, half_new.type, to_smt_flags(symbols, half_new), '1d', x[0], x[1]) for x in
                 psps_changed(half_old.psps_1d, half_new.psps_1d)])
            result.extend([(level, label, half_new.type, to_smt_flags(symbols, half_new), '1_week', x[0], x[1]) for x in
                           psps_changed(half_old.psps_1_week, half_new.psps_1_week)])

        if low_new and low_old:
            result.extend(
                [(level, label, low_new.type, to_smt_flags(symbols, low_new), '15m', x[0], x[1]) for x in
                 psps_changed(low_old.psps_15m, low_new.psps_15m)])
            result.extend(
                [(level, label, low_new.type, to_smt_flags(symbols, low_new), '30m', x[0], x[1]) for x in
                 psps_changed(low_old.psps_30m, low_new.psps_30m)])
            result.extend(
                [(level, label, low_new.type, to_smt_flags(symbols, low_new), '1h', x[0], x[1]) for x in
                 psps_changed(low_old.psps_1h, low_new.psps_1h)])
            result.extend(
                [(level, label, low_new.type, to_smt_flags(symbols, low_new), '2h', x[0], x[1]) for x in
                 psps_changed(low_old.psps_2h, low_new.psps_2h)])
            result.extend(
                [(level, label, low_new.type, to_smt_flags(symbols, low_new), '4h', x[0], x[1]) for x in
                 psps_changed(low_old.psps_4h, low_new.psps_4h)])
            result.extend(
                [(level, label, low_new.type, to_smt_flags(symbols, low_new), '1d', x[0], x[1]) for x in
                 psps_changed(low_old.psps_1d, low_new.psps_1d)])
            result.extend([(level, label, low_new.type, to_smt_flags(symbols, low_new), '1_week', x[0], x[1]) for x in
                           psps_changed(low_old.psps_1_week, low_new.psps_1_week)])

    return result


def psps_changed(
        psps_old: Optional[List[PSP]], psps_new: Optional[List[PSP]]
) -> List[Tuple[str, str]]:  # psp_date, possible|closed|confirmed|swept
    result = []
    if not psps_new:
        return []
    for i in range(len(psps_new)):
        if not psps_old or len(psps_old) < i + 1:
            if psps_new[i].confirmed:
                result.append((psps_new[i].a1_candle[5], 'confirmed'))
            elif psps_new[i].closed:
                result.append((psps_new[i].a1_candle[5], 'closed'))
            else:
                result.append((psps_new[i].a1_candle[5], 'possible'))
            continue
        if not psps_old[i].swept_from_to and psps_new[i].swept_from_to:
            result.append((psps_new[i].a1_candle[5], 'swept'))
            continue
        if not psps_old[i].confirmed and psps_new[i].confirmed:
            if not psps_new[i].swept_from_to:
                result.append((psps_new[i].a1_candle[5], 'confirmed'))
            continue
        if not psps_old[i].closed and psps_new[i].closed:
            if not psps_new[i].swept_from_to:
                result.append((psps_new[i].a1_candle[5], 'closed'))
            continue

    return result


def smt_dict_readable(
        l: List[Tuple[int, str, Optional[SMTLevels]]],
        triad: Triad
) -> Tuple[str, str]:
    pos_short = []
    for level, label, smt_tuple in sorted(l, key=lambda x: x[0], reverse=True):
        if smt_tuple is None:
            continue

        high, half, _ = smt_tuple
        if high is not None:
            pos_short.append(f"{smt_readable(high, label, triad)}")
        if half is not None and half.type == 'half_high':
            pos_short.append(f"{smt_readable(half, label, triad)}")
    if len(pos_short) == 0:
        pos_short.append("No short SMT found")

    pos_long = []
    for level, label, smt_tuple in sorted(l, key=lambda x: x[0], reverse=True):
        if smt_tuple is None:
            continue

        _, half, low = smt_tuple
        if low is not None:
            pos_long.append(f"{smt_readable(low, label, triad)}")
        if half is not None and half.type == 'half_low':
            pos_long.append(f"{smt_readable(half, label, triad)}")
    if len(pos_long) == 0:
        pos_long.append("No long SMT found")

    return "\n\n".join(pos_short), "\n\n".join(pos_long)


def to_smt_flags(symbols: Tuple[str, str, str], smt: SMT) -> str:
    def swept(symbol: str, ql: QuarterLiq) -> str:
        nonlocal smt
        smt_types_ql_match = {
            'high': 3,
            'half_high': 4,
            'half_low': 4,
            'low': 5,
        }
        if ql[smt_types_ql_match[smt.type]][1]:
            return symbol[0].upper()
        return symbol[0].lower()

    return f"{swept(symbols[0], smt.a1q)}{swept(symbols[1], smt.a2q)}{swept(symbols[2], smt.a3q)}"


def smt_readable(smt: SMT, label: str, triad: Triad) -> str:
    have_closed_psps = False
    have_confirmed_psps = False

    smt_ago = humanize_timedelta(to_utc_datetime(triad.a1.snapshot_date_readable) - to_utc_datetime(smt.first_appeared))
    smt_flags = to_smt_flags((triad.a1.symbol, triad.a2.symbol, triad.a3.symbol), smt)
    at = f"{to_ny_date_str(smt.first_appeared)} (<b>{smt_ago} ago</b>)"
    result = f"<b>{smt.type.capitalize()} {smt_flags} {label}</b> at {at}"

    def plus_psps(psps: List[PSP], psp_label: str):
        nonlocal result
        nonlocal triad
        nonlocal smt
        nonlocal have_closed_psps
        nonlocal have_confirmed_psps

        if not psps:
            return
        for p in psps:
            if p.swept_from_to:
                continue
            status = 'Confirmed' if p.confirmed else 'Closed' if p.closed else 'Possible'
            if status == 'Closed':
                have_closed_psps = True
            if status == 'Confirmed':
                have_confirmed_psps = True
            psp_ago = humanize_timedelta(to_utc_datetime(triad.a1.snapshot_date_readable) -
                                         to_utc_datetime(p.a1_candle[5]))

            def boldify(s: str) -> str:
                nonlocal status
                return s if status in ['Possible'] else f"<b>{s}</b>"

            edge = ""
            plus = "+" if smt.type in ['high', 'half_high'] else ""
            extremum_1 = p.a1_candle[1] if smt.type in ['high', 'half_high'] else p.a1_candle[2]
            extremum_2 = p.a2_candle[1] if smt.type in ['high', 'half_high'] else p.a2_candle[2]
            extremum_3 = p.a3_candle[1] if smt.type in ['high', 'half_high'] else p.a3_candle[2]

            extremum_txt = "high" if smt.type in ['high', 'half_high'] else "low"

            c1_diff = percent_from_current(triad.a1.prev_15m_candle[3], extremum_1)
            c2_diff = percent_from_current(triad.a2.prev_15m_candle[3], extremum_2)
            c3_diff = percent_from_current(triad.a3.prev_15m_candle[3], extremum_3)
            edge += f"{extremum_txt} {round(extremum_1, 3)} ({boldify(f'{plus}{c1_diff}%')}), "
            edge += f"{round(extremum_2, 3)} ({boldify(f'{plus}{c2_diff}%')}), "
            edge += f"{round(extremum_3, 3)} ({boldify(f'{plus}{c3_diff}%')})"

            result += f"""
    {boldify(f'{status} {psp_label} PSP')} {to_ny_date_str(p.a1_candle[5])} ({boldify(f'{psp_ago} ago')}) with {edge}"""

    plus_psps(smt.psps_1_month, '1M')
    plus_psps(smt.psps_1_week, '1W')
    plus_psps(smt.psps_1d, '1D')
    plus_psps(smt.psps_4h, '4H')
    plus_psps(smt.psps_2h, '2H')
    plus_psps(smt.psps_1h, '1H')
    plus_psps(smt.psps_30m, '30m')
    plus_psps(smt.psps_15m, '15m')

    emoji = f"{'☑️' if have_confirmed_psps else ''}{'🔚' if have_closed_psps else ''}"
    return f"{emoji}{result}"


def true_opens_readable(
        tos: Tuple[List[TrueOpen], List[TrueOpen], List[TrueOpen]]
) -> str:
    def fmt(smb: str, value: float, change: float) -> str:
        if change == 0:
            change_str = "0%"
        else:
            change_str = f"{change:+.2f}%"
            change_str = change_str.replace(".00", "").rstrip("0").rstrip(".")
        if value >= 1000:
            value_str = f"{value:,.2f}".replace(",", "")
            if value_str.endswith(".00"):
                value_str = value_str[:-3]
        else:
            value_str = f"{value:.2f}"
        return f"{smb} {value_str} ({change_str})"

    col_widths = []
    for i in range(2):
        texts = []
        for row in tos[i]:
            texts.append(fmt(*row))
        col_widths.append(max(len(t) for t in texts) + 2)

    out = ""

    for i in range(len(tos[0])):
        c1 = fmt(*tos[0][i]).ljust(col_widths[0])
        c2 = fmt(*tos[1][i]).ljust(col_widths[1])
        out += c1 + c2 + "\n"

    out += "\n"

    for i in range(len(tos[0])):
        c3 = fmt(*tos[2][i])
        out += c3 + "\n"

    return out


def targets_readable(targets: List[Target]) -> str:
    def to_str(t: Target) -> str:
        result = f"{t[1]} {t[2]}</b>: "
        plus = "+" if t[3][1] > 0 else ""

        result += f"{round(t[3][0], 3)} (<b>{plus}{t[3][1]}%</b>), "
        result += f"{round(t[4][0], 3)} (<b>{plus}{t[4][1]}%</b>), "
        result += f"{round(t[5][0], 3)} (<b>{plus}{t[5][1]}%</b>)"

        return result

    return "\n".join([f"<b>{i + 1}. {to_str(x)}" for i, x in enumerate(targets)])


def new_empty_triad(a1_symbol: str, a2_symbol: str, a3_symbol: str) -> Triad:
    return Triad(
        a1=new_empty_asset(a1_symbol),
        a2=new_empty_asset(a2_symbol),
        a3=new_empty_asset(a3_symbol),
    )


def new_triad(symbols_tuple: Tuple[str, str, str],
              generators_tuple: Tuple[Candles15mGenerator, Candles15mGenerator, Candles15mGenerator]) -> Triad:
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
