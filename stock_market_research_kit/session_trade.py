from dataclasses import dataclass
from typing import List, Tuple

from stock_market_research_kit.session import SessionName


@dataclass
class SessionTrade:
    entry_time: str
    entry_price: float
    entry_position_usd: float
    position_usd: float  # actual position in coins

    hunting_session: SessionName
    predict_direction: str  # UP or DOWN
    entry_strategy: str  # in free format, e.g. 'NY Lunch__BULL  NY PM__BULL NY PM Close__BULL'

    initial_stop: float
    stop: float  # in some strategies we move initial stop

    deadline_close: str  # this can be 3099 year, but take_profit should present
    take_profit: float  # this can be +Infinity, but deadline_close should present

    closes: List[Tuple[float, float, str, str]]  # list of closes in (position_percent, price, time, reason) format

    pnl_usd: float
