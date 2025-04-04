import json
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional

from scripts.run_day_markuper import cme_open_from_to, asia_from_to, london_from_to, early_from_to, pre_from_to, \
    open_from_to, nyam_from_to, lunch_from_to, nypm_from_to, close_from_to


class SessionName(Enum):
    UNSPECIFIED = 'UNSPECIFIED'
    CME = 'CME Open'
    ASIA = 'Asia Open'
    LONDON = 'London Open'
    EARLY = 'Early session'
    PRE = 'Premarket'
    NY_OPEN = 'NY AM Open'
    NY_AM = 'NY AM'
    NY_LUNCH = 'NY Lunch'
    NY_PM = 'NY PM'
    NY_CLOSE = 'NY PM Close'


sessions_in_order = [
    SessionName.CME, SessionName.ASIA, SessionName.LONDON, SessionName.EARLY, SessionName.PRE,
    SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE]


class SessionType(Enum):
    UNSPECIFIED = 'UNSPECIFIED'

    COMPRESSION = 'COMPRESSION'  # almost still (e.g -0.2% or + 0.15% price and 0.4% volatility)
    DOJI = 'DOJI'  # medium volatile, but open ~ close
    INDECISION = 'INDECISION'  # long (or extreme) wicks but open not ~ to close (some bull or bear performance)

    BULL = 'BULL'  # good bull performance, no long wicks
    TO_THE_MOON = 'TO_THE_MOON'  # directional body with when extra volatility
    STB = 'STB'  # close > open, but long bear wick
    REJECTION_BULL = 'REJECTION_BULL'  # close > open, but quite long bull wick
    HAMMER = 'HAMMER'  # small body (bull or bear), very long bear wick and almost no bull wick

    BEAR = 'BEAR'  # good bear performance, no long wicks
    FLASH_CRASH = 'FLASH_CRASH'  # directional body with when extra volatility
    BTS = 'BTS'  # close < open, but long bull wick
    REJECTION_BEAR = 'REJECTION_BEAR'  # close > open, but quite long bull wick
    BEAR_HAMMER = 'BEAR_HAMMER'  # small body (bull or bear), very long bull wick and almost no bear wick

    V_SHAPE = 'V_SHAPE'  # extraordinary bear wick, but body very small compared to wick (bear or bull, whatever)
    PUMP_AND_DUMP = 'PUMP_AND_DUMP'  # extraordinary bull wick, but body very small compared to wick (bear or bull, whatever)


@dataclass
class Session:
    day_date: str
    session_date: str
    session_end_date: str
    name: SessionName
    type: SessionType
    open: float
    high: float
    low: float
    close: float


def session_decoder(dct):
    if "name" in dct:
        dct["name"] = SessionName(dct["name"])
    if "type" in dct:
        dct["type"] = SessionType(dct["type"])
    return Session(**dct)


def session_from_json(json_str):
    return json.loads(json_str, object_hook=session_decoder)


def enum_serializer(obj):
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"Type {type(obj)} not serializable")


def json_from_session(session):
    return json.dumps(asdict(session), default=enum_serializer, indent=4)


def json_from_sessions(sessions):
    return json.dumps([session.__dict__ for session in sessions], default=enum_serializer, indent=4)


def get_next_session_mock(session_name: SessionName, date_str) -> Optional[Session]:
    new_session_name = SessionName.UNSPECIFIED
    from_to = ("", "")

    for i, s in enumerate(sessions_in_order):
        if i == len(sessions_in_order) - 1:
            return None
        if s == session_name:
            new_session_name = sessions_in_order[i + 1]
            break

    match new_session_name:
        case SessionName.CME:
            from_to = cme_open_from_to(date_str)
        case SessionName.ASIA:
            from_to = asia_from_to(date_str)
        case SessionName.LONDON:
            from_to = london_from_to(date_str)
        case SessionName.EARLY:
            from_to = early_from_to(date_str)
        case SessionName.PRE:
            from_to = pre_from_to(date_str)
        case SessionName.NY_OPEN:
            from_to = open_from_to(date_str)
        case SessionName.NY_AM:
            from_to = nyam_from_to(date_str)
        case SessionName.NY_LUNCH:
            from_to = lunch_from_to(date_str)
        case SessionName.NY_PM:
            from_to = nypm_from_to(date_str)
        case SessionName.NY_CLOSE:
            from_to = close_from_to(date_str)

    return Session(
        day_date=date_str,
        session_date=from_to[0],
        session_end_date=from_to[1],
        name=new_session_name,
        type=SessionType.UNSPECIFIED,
        open=0,
        high=0,
        low=0,
        close=0
    )
