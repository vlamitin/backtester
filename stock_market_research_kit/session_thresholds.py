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
