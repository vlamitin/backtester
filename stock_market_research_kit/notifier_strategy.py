from dataclasses import dataclass
from typing import List, Dict

from stock_market_research_kit.session import SessionName
from stock_market_research_kit.session_thresholds import SessionThresholds, btc_universal_threshold, ThresholdsGetter


@dataclass
class NotifierStrategy:
    name: str
    thresholds_getter: ThresholdsGetter

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
    thresholds_getter=lambda x, y: btc_universal_threshold,
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


def session_2024_thresholds_strategy(symbol_thresholds: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#2 Naive strategy with all expert thresholds",
        thresholds_getter=lambda session_name, _: symbol_thresholds[session_name],
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
