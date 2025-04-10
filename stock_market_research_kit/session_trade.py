import json
from dataclasses import dataclass, asdict
from enum import Enum
from typing import List, Tuple

from stock_market_research_kit.session import SessionName, SessionType


@dataclass
class SessionTrade:
    entry_time: str
    entry_price: float
    entry_position_usd: float
    position_usd: float  # actual position in coins

    hunting_session: SessionName
    hunting_type: SessionType
    predict_direction: str  # UP or DOWN
    entry_profile_key: str  # TODO make in-dataclass methods to parse and unparse this key

    initial_stop: float
    stop: float  # in some strategies we move initial stop

    deadline_close: str  # this can be 3099 year, but take_profit should present
    take_profit: float  # this can be +Infinity, but deadline_close should present

    closes: List[Tuple[float, float, str, str]]  # list of closes in (position_percent, price, time, reason) format

    result_type: SessionType
    pnl_usd: float


def session_trade_decoder(dct):
    if "hunting_session" in dct:
        dct["hunting_session"] = SessionName(dct["hunting_session"])
    if "hunting_type" in dct:
        dct["hunting_type"] = SessionType(dct["hunting_type"])
    if "result_type" in dct:
        dct["result_type"] = SessionType(dct["result_type"])
    return SessionTrade(**dct)


def session_trade_from_json(json_str: str) -> SessionTrade:
    return json.loads(json_str, object_hook=session_trade_decoder)


def enum_serializer(obj):
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"Type {type(obj)} not serializable")


def json_from_session_trade(session_trade: SessionTrade) -> str:
    return json.dumps(asdict(session_trade), default=enum_serializer, indent=4)


def json_from_session_trades(session_trades: List[SessionTrade]) -> str:
    return json.dumps([session_trade.__dict__ for session_trade in session_trades], default=enum_serializer, indent=4)
