from dataclasses import dataclass
from typing import List, Dict

from stock_market_research_kit.session_trade import SessionTrade


@dataclass
class StrategyTrades:
    win: int
    lose: int
    guessed: int
    missed: int
    pnl: float
    # max_win_series: int  TODO
    # max_lose_series: int  TODO
    trades: List[SessionTrade]


@dataclass
class Backtest:
    profiles_symbol: str
    profiles_year: int
    profiles_min_chance: int
    profiles_min_times: int

    test_symbol: str
    test_year: int
    sl_percent: float
    tp_percent: float

    trades: int
    win: int
    lose: int
    win_rate: float
    sessions_guessed: int
    sessions_missed: int
    guess_rate: float
    pnl_all: float
    pnl_prof: float
    pnl_loss: float

    all_trades: List[SessionTrade]
    trades_by_strategy: Dict[str, StrategyTrades]
