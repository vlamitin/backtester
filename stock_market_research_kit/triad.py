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
    get_prev_1month_from_to, get_current_1month_from_to, GetDateRange, to_ny_datetime, ny_zone


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
