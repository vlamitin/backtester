from dataclasses import dataclass
from enum import Enum
from typing import Tuple, Literal, List

from stock_market_research_kit.session import SessionName


class ThresholdsCalcMethod(Enum):
    EXPERT = 'EXPERT'
    # сравниваем с этими же сессиями других дней
    SESSION_ALL = 'SESSION_ALL'
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
    hammer_wick_max_min_fraction: float


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
    hammer_wick_max_min_fraction=0.1
)

impact_thresholds = {
    'min_meaningful_wick': 0.2,
    'min_meaningful_body': 0.3,
    'body_adj_coef': 0.05,
    'min_wick_overlap_percent': 50,
    'min_body_overlap_percent': 50,
}


# if __name__ == "__main__":
#     try:
#
#
#     except KeyboardInterrupt:
#         print(f"KeyboardInterrupt, exiting ...")
#         quit(0)
