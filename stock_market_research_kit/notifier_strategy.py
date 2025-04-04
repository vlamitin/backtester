from dataclasses import dataclass
from typing import List

from stock_market_research_kit.session_thresholds import SessionThresholds, btc_universal_threshold


@dataclass
class NotifierStrategy:
    name: str
    session_thresholds: SessionThresholds

    profiles_min_chance: int
    profiles_min_times: int
    sl_percent: float
    tp_percent: float
    profile_years: List[int]

    include_profile_year_to_backtest: bool
    backtest_years: List[int]
    backtest_min_pnl_per_trade: float
    backtest_min_win_rate: float


btc_naive_strategy = NotifierStrategy(
    name="#1 Naive strategy with all expert thresholds",
    session_thresholds=btc_universal_threshold,
    profiles_min_chance=40,
    profiles_min_times=2,
    sl_percent=0.5,
    tp_percent=3,
    include_profile_year_to_backtest=False,
    profile_years=[
        2021,
        2022,
        2023,
        2024,
        2025
    ],
    backtest_years=[
        2021,
        2022,
        2023,
        2024,
        2025
    ],
    backtest_min_pnl_per_trade=0.5,
    backtest_min_win_rate=0.4,
)


btc_naive_strategy_copy = NotifierStrategy(
    name="COPY #1 Naive strategy with all expert thresholds",
    session_thresholds=btc_universal_threshold,
    profiles_min_chance=60,
    profiles_min_times=3,
    sl_percent=1,
    tp_percent=3,
    include_profile_year_to_backtest=False,
    profile_years=[
        2021,
        2022,
        2023,
        2024,
        2025
    ],
    backtest_years=[
        # 2021,
        # 2022,
        # 2023,
        # 2024,
        2025
    ],
    # backtest_min_pnl_per_trade=-5.5,
    backtest_min_pnl_per_trade=0.5,
    backtest_min_win_rate=0.5,
    # backtest_min_win_rate=0.1,
)
