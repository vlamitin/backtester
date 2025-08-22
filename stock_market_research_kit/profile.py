from dataclasses import dataclass
from typing import List, Tuple

from stock_market_research_kit.session import SessionName, SessionType
from stock_market_research_kit.session_trade import SessionTrade


@dataclass
class Profile:
    strategy_name: str
    profile_seq: List[Tuple[SessionName, SessionType]]
    profile_year_stats: Tuple[int, int, int, float]  # win guessed trades pnl
    profile_symbol: str
    profile_year: int
    win: int
    lose: int
    guessed: int
    missed: int
    pnl: float
    trades: List[SessionTrade]
