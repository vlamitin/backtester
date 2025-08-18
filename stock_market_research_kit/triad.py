import json
from dataclasses import dataclass, asdict
from datetime import timedelta, datetime
from typing import Tuple, Optional, List, Dict

from stock_market_research_kit.asset import QuarterLiq, Asset, new_empty_asset, Reverse15mGenerator
from stock_market_research_kit.candle import InnerCandle, as_1_candle, as_1month_candles, as_1w_candles, \
    as_1d_candles, as_4h_candles, as_2h_candles, as_1h_candles, as_30m_candles, AsCandles
from utils.date_utils import to_utc_datetime, to_date_str, get_prev_30m_from_to, get_current_30m_from_to, \
    get_prev_1h_from_to, get_current_1h_from_to, get_prev_2h_from_to, get_current_2h_from_to, get_prev_4h_from_to, \
    get_current_4h_from_to, get_prev_1d_from_to, get_current_1d_from_to, get_prev_1w_from_to, get_current_1w_from_to, \
    get_prev_1month_from_to, get_current_1month_from_to, GetDateRange, humanize_timedelta, \
    to_ny_date_str


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

        a1_sweep_candles_15m = self.a1.get_15m_candles_range(next_tick, self.a1.snapshot_date_readable)
        a2_sweep_candles_15m = self.a2.get_15m_candles_range(next_tick, self.a2.snapshot_date_readable)
        a3_sweep_candles_15m = self.a3.get_15m_candles_range(next_tick, self.a3.snapshot_date_readable)

        a1_q_close = None if len(a1_sweep_candles_15m) == 0 else a1_sweep_candles_15m[0][0]
        a2_q_close = None if len(a2_sweep_candles_15m) == 0 else a2_sweep_candles_15m[0][0]
        a3_q_close = None if len(a3_sweep_candles_15m) == 0 else a3_sweep_candles_15m[0][0]

        is_high = len([x for x in [a1_high[1], a2_high[1], a3_high[1]] if x]) not in [0, 3]
        high_smt = new_empty_smt(a1_ql, a2_aq, a3_ql) if is_high else None
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
        low_smt = new_empty_smt(a1_ql, a2_aq, a3_ql) if is_low else None
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

        half_smt = new_empty_smt(a1_ql, a2_aq, a3_ql) \
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

        if len(a1_candles) == 0:
            return []

        a1_min, a1_max = a1_candles[0][2], a1_candles[0][1]
        a2_min, a2_max = a2_candles[0][2], a2_candles[0][1]
        a3_min, a3_max = a3_candles[0][2], a3_candles[0][1]

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

    def prev_year_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.prev_year:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.prev_year[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
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
        return high_smt, half_smt, low_smt

    def year_q3_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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
        return high_smt, half_smt, low_smt

    def year_q4_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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
        return high_smt, half_smt, low_smt

    def week1_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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
        return high_smt, half_smt, low_smt

    def week2_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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
        return high_smt, half_smt, low_smt

    def week3_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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
        return high_smt, half_smt, low_smt

    def week4_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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
        return high_smt, half_smt, low_smt

    def week5_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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
        return high_smt, half_smt, low_smt

    def mon_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def tue_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def wed_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def thu_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def mon_thu_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def fri_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def mon_fri_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def sat_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def asia_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.asia:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.asia[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None

        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.asia, self.a2.asia, self.a3.asia
        )
        if high_smt:
            # high_smt = self.with_30m_psps(next_tick, high_smt)
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
        if half_smt:
#             half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
#             low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def london_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
        if not self.a1.london:
            return None

        next_tick = to_date_str(to_utc_datetime(self.a1.london[1]) + timedelta(minutes=1))
        if to_utc_datetime(self.a1.snapshot_date_readable) <= to_utc_datetime(next_tick):
            return None
        high_smt, half_smt, low_smt = self.new_smt(
            next_tick, self.a1.london, self.a2.london, self.a3.london
        )
        if high_smt:
#             high_smt = self.with_30m_psps(next_tick, high_smt)
            high_smt = self.with_1h_psps(next_tick, high_smt)
            high_smt = self.with_2h_psps(next_tick, high_smt)
        if half_smt:
#             half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
#             low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def nyam_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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
#             half_smt = self.with_30m_psps(next_tick, half_smt)
            half_smt = self.with_1h_psps(next_tick, half_smt)
            half_smt = self.with_2h_psps(next_tick, half_smt)
        if low_smt:
#             low_smt = self.with_30m_psps(next_tick, low_smt)
            low_smt = self.with_1h_psps(next_tick, low_smt)
            low_smt = self.with_2h_psps(next_tick, low_smt)
        return high_smt, half_smt, low_smt

    def nypm_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def q1_90_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def q2_90_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def q3_90_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def q4_90_smt(self) -> Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]:  # high, half, low
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

    def actual_smt_psp(self) -> Dict[str, Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]]:
        result = {
            '1.1 prev_year SMT': self.prev_year_smt(),
            '2.1 year_q1 SMT': self.year_q1_smt(),
            '2.2 year_q2 SMT': self.year_q2_smt(),
            '2.3 year_q3 SMT': self.year_q3_smt(),
            '2.4 year_q4 SMT': self.year_q4_smt(),
            '3.1 week1 SMT': self.week1_smt(),
            '3.2 week2 SMT': self.week2_smt(),
            '3.3 week3 SMT': self.week3_smt(),
            '3.4 week4 SMT': self.week4_smt(),
            '3.5 week5 SMT': self.week5_smt(),
            '4.1 mon SMT': self.mon_smt(),
            '4.2 tue SMT': self.tue_smt(),
            '4.3 wed SMT': self.wed_smt(),
            '4.4 thu SMT': self.thu_smt(),
            '4.4.1 mon_thu SMT': self.mon_thu_smt(),
            '4.5 fri SMT': self.fri_smt(),
            '4.5.1 mon_fri SMT': self.mon_fri_smt(),
            '4.6 sat SMT': self.sat_smt(),
            '5.1 asia SMT': self.asia_smt(),
            '5.2 london SMT': self.london_smt(),
            '5.3 nyam SMT': self.nyam_smt(),
            '5.4 nypm SMT': self.nypm_smt(),
            # '6.1 q1_90 SMT': self.q1_90_smt(),
            # '6.2 q2_90 SMT': self.q2_90_smt(),
            # '6.3 q3_90 SMT': self.q3_90_smt(),
            # '6.4 q4_90 SMT': self.q4_90_smt(),
        }

        return result


def smt_dict_new_smt_found(
        d_old: Dict[str, Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]],
        d_new: Dict[str, Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]],
) -> List[Tuple[str, SMT]]:
    result = []

    for key in d_new:
        smt_tuple_new = d_new.get(key, None)
        smt_tuple_old = d_old.get(key, None)
        if not smt_tuple_new:
            continue

        high_new, half_new, low_new = smt_tuple_new
        if not smt_tuple_old:
            for smt in [high_new, half_new, low_new]:
                if smt:
                    result.append((key, smt))
            continue

        high_old, half_old, low_old = smt_tuple_old
        for smts in [(high_new, high_old), (half_new, half_old), (low_new, low_old)]:
            if smts[0] and not smts[1]:
                result.append((key, smts[0]))

    return result


def smt_dict_old_smt_cancelled(
        d_old: Dict[str, Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]],
        d_new: Dict[str, Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]],
) -> List[Tuple[str, SMT]]:
    result = []

    for key in d_new:
        smt_tuple_new = d_new.get(key, None)
        smt_tuple_old = d_old.get(key, None)
        if not smt_tuple_old:
            continue

        high_old, half_old, low_old = smt_tuple_old
        if not smt_tuple_new:
            for smt in [high_old, half_old, low_old]:
                if smt:
                    result.append((key, smt))
            continue

        high_new, half_new, low_new = smt_tuple_new
        for smts in [(high_old, high_new), (half_old, half_new), (low_old, low_new)]:
            if smts[0] and not smts[1]:
                result.append((key, smts[0]))

    return result


def smt_dict_psp_changed(
        d_old: Dict[str, Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]],
        d_new: Dict[str, Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]],
) -> List[Tuple[str, str, str, str, str]]:  # (smt_key, smt_type, psp_key, psp_date, possible|closed|confirmed|swept)
    result = []
    for key in d_new:
        smt_tuple_new = d_new.get(key, None)
        smt_tuple_old = d_old.get(key, None)
        if not smt_tuple_new or not smt_tuple_old:
            continue
        high_new, half_new, low_new = smt_tuple_new
        high_old, half_old, low_old = smt_tuple_old

        if high_new and high_old:
            result.extend(
                [(key, high_new.type, '15m', x[0], x[1]) for x in psps_changed(high_old.psps_15m, high_new.psps_15m)])
            result.extend(
                [(key, high_new.type, '30m', x[0], x[1]) for x in psps_changed(high_old.psps_30m, high_new.psps_30m)])
            result.extend(
                [(key, high_new.type, '1h', x[0], x[1]) for x in psps_changed(high_old.psps_1h, high_new.psps_1h)])
            result.extend(
                [(key, high_new.type, '2h', x[0], x[1]) for x in psps_changed(high_old.psps_2h, high_new.psps_2h)])
            result.extend(
                [(key, high_new.type, '4h', x[0], x[1]) for x in psps_changed(high_old.psps_4h, high_new.psps_4h)])
            result.extend(
                [(key, high_new.type, '1d', x[0], x[1]) for x in psps_changed(high_old.psps_1d, high_new.psps_1d)])
            result.extend([(key, high_new.type, '1_week', x[0], x[1]) for x in
                           psps_changed(high_old.psps_1_week, high_new.psps_1_week)])

        if half_new and half_old:
            result.extend(
                [(key, half_new.type, '15m', x[0], x[1]) for x in psps_changed(half_old.psps_15m, half_new.psps_15m)])
            result.extend(
                [(key, half_new.type, '30m', x[0], x[1]) for x in psps_changed(half_old.psps_30m, half_new.psps_30m)])
            result.extend(
                [(key, half_new.type, '1h', x[0], x[1]) for x in psps_changed(half_old.psps_1h, half_new.psps_1h)])
            result.extend(
                [(key, half_new.type, '2h', x[0], x[1]) for x in psps_changed(half_old.psps_2h, half_new.psps_2h)])
            result.extend(
                [(key, half_new.type, '4h', x[0], x[1]) for x in psps_changed(half_old.psps_4h, half_new.psps_4h)])
            result.extend(
                [(key, half_new.type, '1d', x[0], x[1]) for x in psps_changed(half_old.psps_1d, half_new.psps_1d)])
            result.extend([(key, half_new.type, '1_week', x[0], x[1]) for x in
                           psps_changed(half_old.psps_1_week, half_new.psps_1_week)])

        if low_new and low_old:
            result.extend(
                [(key, low_new.type, '15m', x[0], x[1]) for x in psps_changed(low_old.psps_15m, low_new.psps_15m)])
            result.extend(
                [(key, low_new.type, '30m', x[0], x[1]) for x in psps_changed(low_old.psps_30m, low_new.psps_30m)])
            result.extend(
                [(key, low_new.type, '1h', x[0], x[1]) for x in psps_changed(low_old.psps_1h, low_new.psps_1h)])
            result.extend(
                [(key, low_new.type, '2h', x[0], x[1]) for x in psps_changed(low_old.psps_2h, low_new.psps_2h)])
            result.extend(
                [(key, low_new.type, '4h', x[0], x[1]) for x in psps_changed(low_old.psps_4h, low_new.psps_4h)])
            result.extend(
                [(key, low_new.type, '1d', x[0], x[1]) for x in psps_changed(low_old.psps_1d, low_new.psps_1d)])
            result.extend([(key, low_new.type, '1_week', x[0], x[1]) for x in
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
            result.append((psps_new[i].a1_candle[5], 'possible'))
            continue
        if not psps_old[i].closed and psps_new[i].closed:
            if not psps_new[i].swept_from_to:
                result.append((psps_new[i].a1_candle[5], 'closed'))
            continue
        if not psps_old[i].confirmed and psps_new[i].confirmed:
            if not psps_new[i].swept_from_to:
                result.append((psps_new[i].a1_candle[5], 'confirmed'))
            continue
        if not psps_old[i].swept_from_to and psps_new[i].swept_from_to:
            result.append((psps_new[i].a1_candle[5], 'swept'))
            continue

    return result


def smt_dict_readable(
        d: Dict[str, Optional[Tuple[Optional[SMT], Optional[SMT], Optional[SMT]]]],
        triad: Triad
) -> str:
    result = "<b>Possible short:</b>"
    for key in d:
        smt_tuple = d.get(key, None)
        if smt_tuple is None:
            continue

        high, half, _ = smt_tuple
        if high is not None:
            result += f"\n\n{smt_readable(high, key, triad)}"
        if half is not None and half.type == 'half_high':
            result += f"\n\n{smt_readable(half, key, triad)}"
    if result.endswith("short:"):
        result += "\nNo short SMT found"

    result += "\n\n\n<b>Possible long:</b>"
    for key in d:
        smt_tuple = d.get(key, None)
        if smt_tuple is None:
            continue

        _, half, low = smt_tuple
        if low is not None:
            result += f"\n\n{smt_readable(low, key, triad)}"
        if half is not None and half.type == 'half_low':
            result += f"\n\n{smt_readable(half, key, triad)}"
    if result.endswith("long:"):
        result += "\nNo long SMT found"

    return result


def smt_readable(smt: SMT, key: str, triad: Triad) -> str:
    def swept(symbol: str, ql: QuarterLiq) -> str:
        nonlocal triad
        nonlocal smt
        smt_types_match = {
            'high': 3,
            'half_high': 4,
            'half_low': 4,
            'low': 5,
        }
        if ql[smt_types_match[smt.type]][1]:
            return f"{symbol} swept {ql[smt_types_match[smt.type]][0]}"
        return f"{symbol} not swept {ql[smt_types_match[smt.type]][0]}"

    smt_ago = humanize_timedelta(to_utc_datetime(triad.a1.snapshot_date_readable) - to_utc_datetime(smt.first_appeared))
    result = f"""{smt.type.capitalize()} {key} at {to_ny_date_str(smt.first_appeared)} (<b>{smt_ago} ago</b>):
  {swept(triad.a1.symbol, smt.a1q)}, {swept(triad.a2.symbol, smt.a2q)}, {swept(triad.a3.symbol, smt.a3q)}"""

    def plus_psps(psps: List[PSP], label: str):
        nonlocal result
        nonlocal triad
        nonlocal smt
        if not psps:
            return
        for p in psps:
            if p.swept_from_to:
                continue
            status = 'Confirmed' if p.confirmed else 'Closed' if p.closed else 'Possible'
            psp_ago = humanize_timedelta(to_utc_datetime(triad.a1.snapshot_date_readable) -
                                         to_utc_datetime(p.a1_candle[5]))
            edge = ""
            if smt.type in ['high', 'half_high']:
                c1_diff = round(100 * ((p.a1_candle[1] - triad.a1.prev_15m_candle[3]) / triad.a1.prev_15m_candle[3]), 3)
                c2_diff = round(100 * ((p.a2_candle[1] - triad.a2.prev_15m_candle[3]) / triad.a2.prev_15m_candle[3]), 3)
                c3_diff = round(100 * ((p.a3_candle[1] - triad.a3.prev_15m_candle[3]) / triad.a3.prev_15m_candle[3]), 3)
                edge += f"high {p.a1_candle[1]}(+{c1_diff}%), {p.a2_candle[1]}(+{c2_diff}%), {p.a3_candle[1]}(+{c3_diff}%)"
            elif smt.type in ['low', 'half_low']:
                c1_diff = round(100 * ((p.a1_candle[2] - triad.a1.prev_15m_candle[3]) / triad.a1.prev_15m_candle[3]), 3)
                c2_diff = round(100 * ((p.a2_candle[2] - triad.a2.prev_15m_candle[3]) / triad.a2.prev_15m_candle[3]), 3)
                c3_diff = round(100 * ((p.a3_candle[2] - triad.a3.prev_15m_candle[3]) / triad.a3.prev_15m_candle[3]), 3)
                edge += f"low {p.a1_candle[2]}({c1_diff}%), {p.a2_candle[2]}({c2_diff}%), {p.a3_candle[2]}({c3_diff}%)"
            result += f"""
    {status} {label} PSP {to_ny_date_str(p.a1_candle[5])} (<b>{psp_ago} ago</b>) with {edge}"""

    plus_psps(smt.psps_1_month, '1M')
    plus_psps(smt.psps_1_week, '1W')
    plus_psps(smt.psps_1d, '1D')
    plus_psps(smt.psps_4h, '4H')
    plus_psps(smt.psps_2h, '2H')
    plus_psps(smt.psps_1h, '1H')
    plus_psps(smt.psps_30m, '30m')
    plus_psps(smt.psps_15m, '15m')

    return result


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
