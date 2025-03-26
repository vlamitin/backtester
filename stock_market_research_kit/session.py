import json
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum


def session_decoder(dct):
    if "name" in dct:
        dct["name"] = SessionName(dct["name"])
    if "type" in dct:
        dct["type"] = SessionType(dct["type"])
    return Session(**dct)


def session_from_json(json_str):
    return json.loads(json_str, object_hook=session_decoder)


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


sessions = [
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

    def to_db_format(self, symbol: str):
        return (symbol,
                datetime.strptime(self.day_date, "%Y-%m-%d %H:%M").timestamp(),
                datetime.strptime(self.session_date, "%Y-%m-%d %H:%M").timestamp(),
                self.name.value,
                json.dumps(asdict(self), default=enum_serializer, indent=4))


def enum_serializer(obj):
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"Type {type(obj)} not serializable")
