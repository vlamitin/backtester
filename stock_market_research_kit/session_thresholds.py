import time
from dataclasses import dataclass
from enum import Enum
from typing import Tuple, Literal, List, TypeAlias, Callable, Dict

import numpy as np
import pandas as pd

from scripts.setup_db import connect_to_db
from stock_market_research_kit.candle import InnerCandle
from stock_market_research_kit.candle_with_stat import CandleWithStat
from stock_market_research_kit.day import day_from_json, Day
from stock_market_research_kit.session import SessionName


class ThresholdsCalcMethod(Enum):
    EXPERT = 'EXPERT'
    # сравниваем с этими же сессиями других дней
    SESSION_ALL = 'SESSION_ALL'
    SESSION_YEAR_2023 = 'SESSION_YEAR_2023'
    SESSION_YEAR_2024 = 'SESSION_YEAR_2024'
    SESSION_YEAR_2025 = 'SESSION_YEAR_2025'
    SESSION_SMA_10 = 'SESSION_SMA_10'
    SESSION_SMA_20 = 'SESSION_SMA_20'
    # перф сессии = перф дня * доля сессии в часах от 24ч
    # HOURS_WEIGHTED_ALL = 'HOURS_WEIGHTED_ALL'
    # HOURS_WEIGHTED_SMA_10 = 'HOURS_WEIGHTED_SMA_10'
    # HOURS_WEIGHTED_SMA_20 = 'HOURS_WEIGHTED_SMA_20'
    # сравниваем сессии друг с другом, по ним считаем удельные веса и далее перф сессии = перф дня * удельный вес
    # PERF_WEIGHTED_ALL = 'PERF_WEIGHTED_ALL'
    # PERF_WEIGHTED_SMA_10 = 'PERF_WEIGHTED_SMA_10'
    # PERF_WEIGHTED_SMA_20 = 'PERF_WEIGHTED_SMA_20'


@dataclass
class SessionThresholds:
    session: SessionName
    description: str  # describes method of calculating it

    method: Literal[ThresholdsCalcMethod.EXPERT]

    slow_range: Tuple[float, float]  # from, to in percents
    fast_range: Tuple[float, float]  # from, to in percents

    # bear_range: Tuple[float, float]  # from, to in percents
    # bull_range: Tuple[float, float]  # from, to in percents

    doji_max_fraction: float
    indecision_max_fraction: float
    directional_body_min_fraction: float
    hammer_wick_max_fraction: float


ThresholdsGetter: TypeAlias = Callable[[SessionName, InnerCandle], SessionThresholds]

btc_universal_threshold = SessionThresholds(
    session=SessionName.UNSPECIFIED,
    description="""I used chart on js to visualise BTCUSDT 2024 chart and calibrated those thresholds for my
SessionType types looked realistic""",
    method=ThresholdsCalcMethod.EXPERT,
    slow_range=(0.6, 1),
    fast_range=(1.8, 3),
    doji_max_fraction=0.1,
    indecision_max_fraction=0.3,
    directional_body_min_fraction=0.6,
    hammer_wick_max_fraction=0.1
)

impact_thresholds = {
    'min_meaningful_wick': 0.2,
    'min_meaningful_body': 0.3,
    'body_adj_coef': 0.05,
    'min_wick_overlap_percent': 50,
    'min_body_overlap_percent': 50,
}


def threshold_session_year(symbol: str, session: SessionName, year: int,
                           candles: List[InnerCandle]) -> SessionThresholds:
    df = pd.DataFrame(candles, columns=['open', 'high', 'low', 'close', '_', '_'])
    candle_range = (df['high'] - df['low']).replace(0, np.nan)

    df['upper_wick_fraction'] = (df['high'] - np.maximum(df['open'], df['close'])) / candle_range
    df['body_fraction'] = np.abs(df['open'] - df['close']) / candle_range
    df['lower_wick_fraction'] = (np.minimum(df['open'], df['close']) - df['low']) / candle_range

    volat_percentiles = np.percentile(candle_range * 100 / df['open'], [10, 30, 70, 90])
    body_percentiles = np.percentile(df['body_fraction'], [10, 30, 70, 90])
    wicks_percentiles = np.percentile(pd.concat([df['upper_wick_fraction'], df['lower_wick_fraction']]),
                                      [10, 30, 70, 90])

    return SessionThresholds(
        session=session,
        description=f"{symbol} thresholds based on quantiles of {session.value} session for full {year} year",
        method=ThresholdsCalcMethod(f"SESSION_YEAR_{year}"),
        slow_range=(float(volat_percentiles[0]), float(volat_percentiles[1])),
        fast_range=(float(volat_percentiles[2]), float(volat_percentiles[3])),
        doji_max_fraction=float(body_percentiles[0]),
        indecision_max_fraction=float(body_percentiles[1]),
        directional_body_min_fraction=float(body_percentiles[2]),
        hammer_wick_max_fraction=float(wicks_percentiles[0])
    )


def quantile_per_session_year_thresholds(symbol: str, year: int) -> Dict[SessionName, SessionThresholds]:
    start_time = time.perf_counter()
    conn = connect_to_db(year)

    c = conn.cursor()
    c.execute("""SELECT data FROM days WHERE symbol = ?""", (symbol,))
    rows = c.fetchall()

    conn.close()

    days: List[Day] = [day_from_json(x[0]) for x in rows]

    result = {
        SessionName.CME: threshold_session_year(symbol, SessionName.CME, year,
                                                [x.cme_as_candle for x in days if x.cme_as_candle[5] != ""]),
        SessionName.ASIA: threshold_session_year(symbol, SessionName.ASIA, year,
                                                 [x.asia_as_candle for x in days if x.asia_as_candle[5] != ""]),
        SessionName.LONDON: threshold_session_year(symbol, SessionName.LONDON, year,
                                                   [x.london_as_candle for x in days if x.london_as_candle[5] != ""]),
        SessionName.EARLY: threshold_session_year(symbol, SessionName.EARLY, year,
                                                  [x.early_session_as_candle for x in days if
                                                   x.early_session_as_candle[5] != ""]),
        SessionName.PRE: threshold_session_year(symbol, SessionName.PRE, year,
                                                [x.premarket_as_candle for x in days if
                                                 x.premarket_as_candle[5] != ""]),
        SessionName.NY_OPEN: threshold_session_year(symbol, SessionName.NY_OPEN, year,
                                                    [x.ny_am_open_as_candle for x in days if
                                                     x.ny_am_open_as_candle[5] != ""]),
        SessionName.NY_AM: threshold_session_year(symbol, SessionName.NY_AM, year,
                                                  [x.ny_am_as_candle for x in days if x.ny_am_as_candle[5] != ""]),
        SessionName.NY_LUNCH: threshold_session_year(symbol, SessionName.NY_LUNCH, year,
                                                     [x.ny_lunch_as_candle for x in days if
                                                      x.ny_lunch_as_candle[5] != ""]),
        SessionName.NY_PM: threshold_session_year(symbol, SessionName.NY_PM, year,
                                                  [x.ny_pm_as_candle for x in days if x.ny_pm_as_candle[5] != ""]),
        SessionName.NY_CLOSE: threshold_session_year(symbol, SessionName.NY_CLOSE, year,
                                                     [x.ny_pm_close_as_candle for x in days if
                                                      x.ny_pm_close_as_candle[5] != ""]),
    }

    print(
        f"Calculation quantiles for {year} {symbol} sessions took {(time.perf_counter() - start_time):.6f} seconds")

    return result


if __name__ == "__main__":
    try:
        res_btc = quantile_per_session_year_thresholds("BTCUSDT", 2024)
        res_aave = quantile_per_session_year_thresholds("AAVEUSDT", 2024)
        res_avax = quantile_per_session_year_thresholds("AVAXUSDT", 2024)
        res_crv = quantile_per_session_year_thresholds("CRVUSDT", 2024)
        print(res_crv, res_btc)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
