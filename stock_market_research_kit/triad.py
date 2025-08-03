from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import TypeAlias, Tuple, Optional, List, Generator

from stock_market_research_kit.candle import PriceDate, InnerCandle, as_1_candle
from stock_market_research_kit.db_layer import select_full_days_candles_15m
from stock_market_research_kit.quarter import Quarter90m
from utils.date_utils import to_utc_datetime, to_date_str, get_prev_30m_from_to, get_current_30m_from_to, \
    get_prev_1h_from_to, get_current_1h_from_to, get_prev_2h_from_to, get_current_2h_from_to, get_prev_4h_from_to, \
    get_current_4h_from_to, get_prev_1d_from_to, get_current_1d_from_to, get_prev_1w_from_to, get_current_1w_from_to, \
    get_prev_1month_from_to, get_current_1month_from_to, prev_quarters90m_ranges

LiqSwept: TypeAlias = Tuple[float, str]  # (price, date_swept)
QuarterLiq: TypeAlias = Tuple[
    str, str, bool, LiqSwept, LiqSwept, LiqSwept]  # (date_start, date_end, ended, high, half, low)
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

    prev_year_q4: Optional[QuarterLiq]
    year_q1: Optional[QuarterLiq]
    year_q2: Optional[QuarterLiq]
    year_q3: Optional[QuarterLiq]
    yo: Optional[PriceDate]  # for 1st january asia that's not completed will be None
    true_yo: Optional[PriceDate]  # 1st of april, for 1jan - 31mar well be None

    prev_month: Optional[QuarterLiq]

    prev_month_week_4: Optional[QuarterLiq]
    prev_month_week_5: Optional[QuarterLiq]
    week1: Optional[QuarterLiq]
    week2: Optional[QuarterLiq]
    week3: Optional[QuarterLiq]
    week4: Optional[QuarterLiq]
    mo: Optional[PriceDate]  # for 1st month day asia that's not completed will be None
    true_mo: Optional[PriceDate]  # 2nd monday of month, for first uncompleted week well be None

    prev_week_thu: Optional[QuarterLiq]
    prev_week_fri: Optional[QuarterLiq]
    nwog: Optional[Tuple[LiqSwept, LiqSwept]]  # swept status for high, swept status for low
    mon: Optional[QuarterLiq]
    tue: Optional[QuarterLiq]
    wed: Optional[QuarterLiq]
    thu: Optional[QuarterLiq]
    mon_thu: Optional[QuarterLiq]
    fri: Optional[QuarterLiq]
    mon_fri: Optional[QuarterLiq]
    sat: Optional[QuarterLiq]
    mon_sat: Optional[QuarterLiq]
    wo: Optional[PriceDate]  # for monday asia that's not completed will be None
    true_wo: Optional[PriceDate]  # monday 6pm NY, for all monday quarters will be None

    prev_pm: Optional[QuarterLiq]
    asia: Optional[QuarterLiq]
    london: Optional[QuarterLiq]
    nyam: Optional[QuarterLiq]
    do: Optional[PriceDate]  # UTC 0:00, for asia that's not completed will be None
    true_do: Optional[PriceDate]  # NY 0:00, for asia that's not completed will be None

    prev_q4_90m: Optional[QuarterLiq]
    q1_90m: Optional[QuarterLiq]
    q2_90m: Optional[QuarterLiq]
    q3_90m: Optional[QuarterLiq]
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
        self.snapshot_date_readable = to_date_str(to_utc_datetime(self.prev_15m_candle[5]) + timedelta(minutes=15))

        highest_sweep = self.prev_15m_candle[1]
        lowest_sweep = self.prev_15m_candle[2]

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

        ranges_90m = prev_quarters90m_ranges(self.snapshot_date_readable)
        cum_90m_quarters = []
        for rng_90m in ranges_90m:
            cum_90m_quarters.append(self.prev_15m_candle if rng_90m[1] <= to_utc_datetime(
                self.prev_15m_candle[5]) < rng_90m[2] else None)

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

            highest_sweep = max(highest_sweep, prev_candle[1])
            lowest_sweep = min(lowest_sweep, prev_candle[2])

            for i in range(len(ranges_90m)):
                res_rng = None
                if ranges_90m[i][1] <= to_utc_datetime(prev_candle[5]) < ranges_90m[i][2]:
                    cum_90m_quarters[i] = prev_candle if not cum_90m_quarters[i] else as_1_candle(
                        [prev_candle, cum_90m_quarters[i]])
                    if ranges_90m[i][1] == to_utc_datetime(prev_candle[5]):
                        res_rng = (
                            cum_90m_quarters[i][5], to_date_str(ranges_90m[i][2]),
                            ranges_90m[i][2] < to_utc_datetime(self.snapshot_date_readable),
                            (cum_90m_quarters[i][1], ""),
                            ((cum_90m_quarters[i][1] + cum_90m_quarters[i][2]) / 2, ""),
                            (cum_90m_quarters[i][2], "")
                        )
                if res_rng and ranges_90m[i][0] == Quarter90m.Q1_90m:
                    self.q1_90m = res_rng
                elif res_rng and ranges_90m[i][0] == Quarter90m.Q2_90m:
                    self.q2_90m = res_rng
                elif res_rng and ranges_90m[i][0] == Quarter90m.Q3_90m:
                    self.q3_90m = res_rng
                elif res_rng and ranges_90m[i][0] == Quarter90m.Q4_90m:
                    self.q4_90m = res_rng

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

            print('ea')


def new_empty_asset(symbol: str) -> Asset:
    return Asset(
        symbol=symbol,
        snapshot_date_readable="",
        prev_year=None,
        prev_year_q4=None,
        year_q1=None,
        year_q2=None,
        year_q3=None,
        yo=None,
        true_yo=None,
        prev_month=None,
        prev_month_week_4=None,
        prev_month_week_5=None,
        week1=None,
        week2=None,
        week3=None,
        week4=None,
        mo=None,
        true_mo=None,
        prev_week_thu=None,
        prev_week_fri=None,
        nwog=None,
        mon=None,
        tue=None,
        wed=None,
        thu=None,
        mon_thu=None,
        fri=None,
        mon_fri=None,
        sat=None,
        mon_sat=None,
        wo=None,
        true_wo=None,
        prev_pm=None,
        asia=None,
        london=None,
        nyam=None,
        do=None,
        true_do=None,
        prev_q4_90m=None,
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

    prev_year_q4_smt: Optional[Tuple[SMT, SMT, SMT]]  # high, half, low
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
        prev_year_q4_smt=None,
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
    candles = (select_full_days_candles_15m(2023, symbol)
               + select_full_days_candles_15m(2024, symbol))

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
