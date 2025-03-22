from dataclasses import dataclass
from typing import Literal

from stock_market_research_kit.session import SessionName, SessionType


@dataclass
class SessionsSequence:
    session: Literal[
        SessionName.CME, SessionName.ASIA, SessionName.LONDON, SessionName.EARLY, SessionName.PRE,
        SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE]
    candle_type: Literal[
        SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL, SessionType.TO_THE_MOON,
        SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
        SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
        SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP]
    parent_session: Literal[
        None, SessionName.CME, SessionName.ASIA, SessionName.LONDON, SessionName.EARLY, SessionName.PRE,
        SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE]
    parent_candle_type: Literal[
        None, SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL, SessionType.TO_THE_MOON,
        SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
        SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
        SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP]
    count: int

    def to_db_format(self, symbol: str, tree_name: str):
        return (symbol,
                tree_name,
                self.session.value,
                self.candle_type.value,
                self.parent_session.value,
                self.parent_candle_type.value,
                self.count)

# перенести дерево в python
# предрассчитать все варианты (close -> ... open, close -> ... cme, pm -> ... open, pm -> ... cme, ...)
# просто фильтровать по % встречаемости - плохо, лучше вывести на графики, подумать что объединяет
