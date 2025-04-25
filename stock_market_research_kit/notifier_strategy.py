from dataclasses import dataclass
from typing import List, Dict

from stock_market_research_kit.session import SessionName
from stock_market_research_kit.session_thresholds import SessionThresholds, btc_universal_threshold, ThresholdsGetter, \
    SGetter


@dataclass
class NotifierStrategy:
    name: str
    thresholds_getter: ThresholdsGetter

    profiles_min_chance: int
    profiles_min_times: int
    slg: SGetter
    tpg: SGetter
    profile_years: List[int]

    include_profile_year_to_backtest: bool
    backtest_years: List[int]
    backtest_min_pnl_per_trade: float
    backtest_min_win_rate: float


btc_naive_strategy01 = NotifierStrategy(
    name="#1 Naive strategy with all expert thresholds",
    thresholds_getter=lambda x, y: btc_universal_threshold,
    profiles_min_chance=40,
    profiles_min_times=2,
    slg=lambda s, c, d: 0.5,
    tpg=lambda s, c, d: 3,
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


def thr2024_strategy02(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#2 Same as #1, but sessions thresholds are now calculated per-session based on quantiles for 2024 year",
        thresholds_getter=lambda session_name, _: thresholds_2024[session_name],
        profiles_min_chance=40,
        profiles_min_times=2,
        slg=lambda s, c, d: 0.5,
        tpg=lambda s, c, d: 3,
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


def thr2024_strict_strategy03(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#3 Same as #2, but more strict profiles first filtering",
        thresholds_getter=lambda session_name, _: thresholds_2024[session_name],
        profiles_min_chance=41,
        profiles_min_times=3,
        slg=lambda s, c, d: 0.5,
        tpg=lambda s, c, d: 3,
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


def thr2024_loose_strategy04(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#4 Loose first filtering, loose backtested profiles filtering",
        thresholds_getter=lambda session_name, _: thresholds_2024[session_name],
        profiles_min_chance=27,
        profiles_min_times=2,
        slg=lambda s, c, d: 0.5,
        tpg=lambda s, c, d: 3,
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
        backtest_min_pnl_per_trade=-2.5,
        backtest_min_win_rate=0,
    )


def thr2024_p70_safe_stops_strategy05(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#5 Same as #2, but sl is took from session p70 safe_stop, and tp is sl * 3",
        thresholds_getter=lambda session_name, _: thresholds_2024[session_name],
        profiles_min_chance=40,
        profiles_min_times=2,
        slg=lambda s, c, d: thresholds_2024[s].p70_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p70_safe_stop_bear,
        tpg=lambda s, c, d: (thresholds_2024[s].p70_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p70_safe_stop_bear) * 3,
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


def thr2024_loose_p70_safe_stops_strategy06(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#6 Same as #4, but sl is took from session p70 safe_stop, and tp is sl * 3",
        thresholds_getter=lambda session_name, _: thresholds_2024[session_name],
        profiles_min_chance=27,
        profiles_min_times=2,
        slg=lambda s, c, d: thresholds_2024[s].p70_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p70_safe_stop_bear,
        tpg=lambda s, c, d: (thresholds_2024[s].p70_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p70_safe_stop_bear) * 3,
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
        backtest_min_pnl_per_trade=-2.5,
        backtest_min_win_rate=0,
    )


def thr2024_p30_safe_stops_strategy07(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#7 Same as #2, but sl is took from session p30 safe_stop, and tp is sl * 3",
        thresholds_getter=lambda session_name, _: thresholds_2024[session_name],
        profiles_min_chance=40,
        profiles_min_times=2,
        slg=lambda s, c, d: thresholds_2024[s].p30_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p30_safe_stop_bear,
        tpg=lambda s, c, d: (thresholds_2024[s].p30_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p30_safe_stop_bear) * 3,
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


def thr2024_loose_p30_safe_stops_strategy08(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#8 Same as #4, but sl is took from session p30 safe_stop, and tp is sl * 3",
        thresholds_getter=lambda session_name, _: thresholds_2024[session_name],
        profiles_min_chance=27,
        profiles_min_times=2,
        slg=lambda s, c, d: thresholds_2024[s].p30_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p30_safe_stop_bear,
        tpg=lambda s, c, d: (thresholds_2024[s].p30_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p30_safe_stop_bear) * 3,
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
        backtest_min_pnl_per_trade=-2.5,
        backtest_min_win_rate=0,
    )


def thr2024_strict_p30_safe_stops_strategy09(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#9 Same as #3, but sl is took from session p30 safe_stop, and tp is sl * 3",
        thresholds_getter=lambda session_name, _: thresholds_2024[session_name],
        profiles_min_chance=41,
        profiles_min_times=3,
        slg=lambda s, c, d: thresholds_2024[s].p30_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p30_safe_stop_bear,
        tpg=lambda s, c, d: (thresholds_2024[s].p30_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p30_safe_stop_bear) * 3,
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


def thr2024_strict_p70_safe_stops_strategy10(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#10 Same as #3, but sl is took from session p70 safe_stop, and tp is sl * 3",
        thresholds_getter=lambda session_name, _: thresholds_2024[session_name],
        profiles_min_chance=41,
        profiles_min_times=3,
        slg=lambda s, c, d: thresholds_2024[s].p70_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p70_safe_stop_bear,
        tpg=lambda s, c, d: (thresholds_2024[s].p70_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p70_safe_stop_bear) * 3,
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


def btc_naive_p30_safe_stops_strategy_strategy11(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#11 Same as #1, but sl is took from session p30 safe_stop, and tp is sl * 3",
        thresholds_getter=lambda x, y: btc_universal_threshold,
        profiles_min_chance=40,
        profiles_min_times=2,
        slg=lambda s, c, d: thresholds_2024[s].p30_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p30_safe_stop_bear,
        tpg=lambda s, c, d: (thresholds_2024[s].p30_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p30_safe_stop_bear) * 3,
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


def btc_naive_p70_safe_stops_strategy_strategy12(thresholds_2024: Dict[SessionName, SessionThresholds]):
    return NotifierStrategy(
        name="#12 Same as #1, but sl is took from session p70 safe_stop, and tp is sl * 3",
        thresholds_getter=lambda x, y: btc_universal_threshold,
        profiles_min_chance=40,
        profiles_min_times=2,
        slg=lambda s, c, d: thresholds_2024[s].p70_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p70_safe_stop_bear,
        tpg=lambda s, c, d: (thresholds_2024[s].p70_safe_stop_bull if d == 'UP' else thresholds_2024[
            s].p70_safe_stop_bear) * 3,
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
