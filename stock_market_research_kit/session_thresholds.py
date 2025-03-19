import json
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Literal

from stock_market_research_kit.session import SessionName


class ThresholdsCalcMethod(Enum):
    EXPERT = 'EXPERT'
    # сравниваем с этими же сессиями других дней
    SESSION_ALL = 'SESSION_ALL'
    SESSION_SMA_10 = 'SESSION_SMA_10'
    SESSION_SMA_20 = 'SESSION_SMA_20'
    # перф сессии = перф дня * доля сессии в часах от 24ч
    HOURS_WEIGHTED_ALL = 'HOURS_WEIGHTED_ALL'
    HOURS_WEIGHTED_SMA_10 = 'HOURS_WEIGHTED_SMA_10'
    HOURS_WEIGHTED_SMA_20 = 'HOURS_WEIGHTED_SMA_20'
    # сравниваем сессии друг с другом, по ним считаем удельные веса и далее перф сессии = перф дня * удельный вес
    PERF_WEIGHTED_ALL = 'PERF_WEIGHTED_ALL'
    PERF_WEIGHTED_SMA_10 = 'PERF_WEIGHTED_SMA_10'
    PERF_WEIGHTED_SMA_20 = 'PERF_WEIGHTED_SMA_20'


@dataclass
class SessionThresholds:
    name: Literal[
        None,
        SessionName.CME, SessionName.ASIA, SessionName.LONDON, SessionName.EARLY, SessionName.PRE,
        SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE]

    method: Literal[ThresholdsCalcMethod.EXPERT]

    slow_range: Tuple[float, float]  # from, to in percents
    fast_range: Tuple[float, float]  # from, to in percents

    # bear_range: Tuple[float, float]  # from, to in percents
    # bull_range: Tuple[float, float]  # from, to in percents

    doji_max_fraction: float
    indecision_max_fraction: float
    directional_body_min_fraction: float
    hammer_wick_max_min_fraction: float


btc_universal_threshold = SessionThresholds(
    name=None,
    method=ThresholdsCalcMethod.EXPERT,
    slow_range=(0.6, 1),
    fast_range=(1.8, 3),
    doji_max_fraction=0.1,
    indecision_max_fraction=0.3,
    directional_body_min_fraction=0.6,
    hammer_wick_max_min_fraction=0.1
)

# btc_cme_threshold = SessionThresholds(
#     name=SessionName.CME,
#     method=ThresholdsCalcMethod.EXPERT,
#     slow_range=(0.4, 0.7),
#     fast_range=(1.2, 2),
#     # bear_range=(-1.7, -1),
#     # bull_range=(1, 1.7),
#     doji_max_fraction=0.2,
#     indecision_max_fraction=0.35,
#     directional_body_min_fraction=0.7,
#     hammer_wick_max_min_fraction=0.1
# )
#
# btc_asia_threshold = SessionThresholds(
#     name=SessionName.ASIA,
#     method=ThresholdsCalcMethod.EXPERT,
#     slow_range=(0.7, 1),
#     fast_range=(2, 3),
#     # bear_range=(-2.2, -0.8),
#     # bull_range=(0.8, 2.2),
#     doji_max_fraction=0.2,
#     indecision_max_fraction=0.35,
#     directional_body_min_fraction=0.7,
#     hammer_wick_max_min_fraction=0.1
# )
#
# btc_london_threshold = SessionThresholds(
#     name=SessionName.LONDON,
#     method=ThresholdsCalcMethod.EXPERT,
#     slow_range=(0.6, 1),
#     fast_range=(1.8, 3),
#     # bear_range=(-2.2, -0.85),
#     # bull_range=(0.85, 2.2),
#     doji_max_fraction=0.2,
#     indecision_max_fraction=0.35,
#     directional_body_min_fraction=0.7,
#     hammer_wick_max_min_fraction=0.1
# )
#
# btc_early_threshold = SessionThresholds(
#     name=SessionName.EARLY,
#     method=ThresholdsCalcMethod.EXPERT,
#     slow_range=(0.4, 0.6),
#     fast_range=(0.8, 1.5),
#     # bear_range=(-1.3, -0.3),
#     # bull_range=(0.3, 1.3),
#     doji_max_fraction=0.2,
#     indecision_max_fraction=0.35,
#     directional_body_min_fraction=0.7,
#     hammer_wick_max_min_fraction=0.1
# )
#
# btc_pre_threshold = SessionThresholds(
#     name=SessionName.PRE,
#     method=ThresholdsCalcMethod.EXPERT,
#     slow_range=(0.6, 1),
#     fast_range=(1.8, 3),
#     # bear_range=(-2, -1.3),
#     # bull_range=(1.3, 2),
#     doji_max_fraction=0.2,
#     indecision_max_fraction=0.35,
#     directional_body_min_fraction=0.7,
#     hammer_wick_max_min_fraction=0.1
# )
#
# btc_open_threshold = SessionThresholds(
#     name=SessionName.NY_OPEN,
#     method=ThresholdsCalcMethod.EXPERT,
#     slow_range=(0.6, 0.9),
#     fast_range=(1.8, 3),
#     # bear_range=(-2.0, -0.85),
#     # bull_range=(0.85, 2.0),
#     doji_max_fraction=0.2,
#     indecision_max_fraction=0.35,
#     directional_body_min_fraction=0.7,
#     hammer_wick_max_min_fraction=0.1
# )
#
# btc_nyam_threshold = SessionThresholds(
#     name=SessionName.NY_AM,
#     method=ThresholdsCalcMethod.EXPERT,
#     slow_range=(0.6, 1.3),
#     fast_range=(2, 3),
#     # bear_range=(-2.2, -1.25),
#     # bull_range=(1.25, 2.2),
#     doji_max_fraction=0.2,
#     indecision_max_fraction=0.35,
#     directional_body_min_fraction=0.7,
#     hammer_wick_max_min_fraction=0.1
# )
#
# btc_lunch_threshold = SessionThresholds(
#     name=SessionName.NY_LUNCH,
#     method=ThresholdsCalcMethod.EXPERT,
#     slow_range=(0.6, 1),
#     fast_range=(1.7, 2.7),
#     # bear_range=(-1.8, -0.6),
#     # bull_range=(0.6, 1.8),
#     doji_max_fraction=0.2,
#     indecision_max_fraction=0.35,
#     directional_body_min_fraction=0.7,
#     hammer_wick_max_min_fraction=0.1
# )
#
# btc_nypm_threshold = SessionThresholds(
#     name=SessionName.NY_PM,
#     method=ThresholdsCalcMethod.EXPERT,
#     slow_range=(0.6, 1),
#     fast_range=(2, 2.6),
#     # bear_range=(-2, -0.8),
#     # bull_range=(0.8, 2),
#     doji_max_fraction=0.2,
#     indecision_max_fraction=0.35,
#     directional_body_min_fraction=0.7,
#     hammer_wick_max_min_fraction=0.1
# )
#
# btc_close_threshold = SessionThresholds(
#     name=SessionName.NY_CLOSE,
#     method=ThresholdsCalcMethod.EXPERT,
#     slow_range=(0.6, 1),
#     fast_range=(1.7, 2.6),
#     # bear_range=(-1.45, -0.8),
#     # bull_range=(0.8, 1.45),
#     doji_max_fraction=0.2,
#     indecision_max_fraction=0.35,
#     directional_body_min_fraction=0.7,
#     hammer_wick_max_min_fraction=0.1
# )
