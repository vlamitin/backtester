import json
from dataclasses import dataclass, asdict, field
from enum import Enum
from typing import Optional, Tuple, List

from stock_market_research_kit.candle import InnerCandle, PriceDate
from utils.date_utils import now_utc_datetime, to_date_str, start_of_day, cme_open_from_to, asia_from_to, \
    london_from_to, early_from_to, pre_from_to, \
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
    PUMP_AND_DUMP = 'PUMP_AND_DUMP'  # extraordinary bull wick, but body very small (bear or bull, whatever)


class SessionImpact(Enum):
    UNSPECIFIED = 'UNSPECIFIED'

    DAILY_HIGH = 'DAILY_HIGH'  # whatever session with session high forms daily high (manipulation is done ✔)
    DAILY_LOW = 'DAILY_LOW'  # whatever session with session low forms daily low (manipulation is done ✔)

    # WARN! We skip daily wicks that are very short (< 0.2 of candle volatility)
    HIGH_WICK_BUILDER = 'HIGH_WICK_BUILDER'  # DH not yet happened and session is responsible for > 0.5 of upper wick
    LOW_WICK_BUILDER = 'LOW_WICK_BUILDER'  # DL not yet happened and session is responsible for > 0.5 of lower wick

    # WARN! We skip daily wicks that are very short (< 0.2 of candle volatility)
    HIGH_TO_BODY_REVERSAL = 'HIGH_TO_BODY_REVERSAL'  # after DH & SO in upper wick & SC in body or close to it
    LOW_TO_BODY_REVERSAL = 'LOW_TO_BODY_REVERSAL'  # after DL & SO in lower wick & SC in body or close to it

    # WARN! We skip daily bodies that are very short (< 0.3 of candle volatility)
    FORWARD_BODY_BUILDER = 'FORWARD_BODY_BUILDER'  # responsible for > 0.5 of Daily body, and performed same way
    BACKWARD_BODY_BUILDER = 'BACKWARD_BODY_BUILDER'  # responsible for > 0.5 of Daily body, and performed same way


@dataclass
class Session:
    day_date: str
    session_date: str
    session_end_date: str
    name: SessionName
    type: SessionType
    impact: SessionImpact
    open: float
    high: float
    low: float
    close: float


def session_decoder(dct):
    if "name" in dct:
        dct["name"] = SessionName(dct["name"])
    if "type" in dct:
        dct["type"] = SessionType(dct["type"])
    if "impact" in dct:
        dct["impact"] = SessionImpact(dct["impact"])
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


def get_from_to(session_name: SessionName, date_str: str) -> Tuple[str, str]:
    match session_name:
        case SessionName.CME:
            return cme_open_from_to(date_str)
        case SessionName.ASIA:
            return asia_from_to(date_str)
        case SessionName.LONDON:
            return london_from_to(date_str)
        case SessionName.EARLY:
            return early_from_to(date_str)
        case SessionName.PRE:
            return pre_from_to(date_str)
        case SessionName.NY_OPEN:
            return open_from_to(date_str)
        case SessionName.NY_AM:
            return nyam_from_to(date_str)
        case SessionName.NY_LUNCH:
            return lunch_from_to(date_str)
        case SessionName.NY_PM:
            return nypm_from_to(date_str)
        case SessionName.NY_CLOSE:
            return close_from_to(date_str)
    return "", ""


def get_next_session_mock(session_name: SessionName, date_str: str) -> Optional[Session]:
    new_session_name = SessionName.UNSPECIFIED

    for i, s in enumerate(sessions_in_order):
        if i == len(sessions_in_order) - 1:
            return None
        if s == session_name:
            new_session_name = sessions_in_order[i + 1]
            break

    from_to = get_from_to(new_session_name, date_str)

    return Session(
        day_date=date_str,
        session_date=from_to[0],
        session_end_date=from_to[1],
        name=new_session_name,
        type=SessionType.UNSPECIFIED,
        impact=SessionImpact.UNSPECIFIED,
        open=0,
        high=0,
        low=0,
        close=0
    )


class LevelAction(Enum):
    UNSPECIFIED = 'UNS'
    BREAK_BULL = 'BRK_BLL'  # пробой и закрытие за уровнем (больше чем на threshold в допустим 0.1% Open Price)
    FAKEBREAK_BULL = 'FKB_BLL'
    BOUNCE_BULL = 'BNC_BLL'  # отскок прям от уровня (или +/- пару пипсов)
    TURNAWAY_BULL = 'TRN_BLL'  # отскок не доходя больше чем пару пипсов, но меньше чем threshold (0.1% от Open Price напр)
    IGNORE_BULL = 'IGN_BLL'  # не дошли к уровню ближе чем на TURNAWAY threshold
    BREAK_BEAR = 'BRK_BEA'
    FAKEBREAK_BEAR = 'FKB_BEA'
    BOUNCE_BEAR = 'BNC_BEA'
    TURNAWAY_BEAR = 'TRN_BEA'
    IGNORE_BEAR = 'IGN_BEA'


@dataclass
class SessionPriceAction:
    name: SessionName
    candles_15m: List[InnerCandle]
    session_candle: InnerCandle

    do: PriceDate
    true_do: PriceDate  # NY midnight
    wo: PriceDate
    true_wo: PriceDate  # monday 6pm NY
    mo: PriceDate
    true_mo: PriceDate  # 2nd monday of month
    yo: PriceDate
    true_yo: PriceDate  # 1st of april

    prev_ny_close: PriceDate  # 16:00 NY
    prev_cme_close: PriceDate  # 17:00 NY, it's also called True daily close

    week_high: PriceDate  # все week экстремумы считаются от предыдущего дня
    week_range_75q: PriceDate
    week_range_50q: PriceDate
    week_range_25q: PriceDate
    week_low: PriceDate

    def level_action(self, level: float) -> LevelAction:
        threshold = 0.001  # 0.1%
        pip_slack = 0.0002  # 0.02%

        if len(self.candles_15m) == 0:
            return LevelAction.UNSPECIFIED

        open_, high, low, close_ = self.session_candle[:4]

        def within_pip(price):
            return abs(price - level) / open_ <= pip_slack

        def within_turnaway_range(price):
            return pip_slack < abs(price - level) / open_ <= threshold

        if open_ < level:
            if high > level:
                if close_ > level:
                    return LevelAction.BREAK_BULL
                return LevelAction.FAKEBREAK_BULL
            if within_pip(high) and close_ < level * (1 - pip_slack):
                return LevelAction.BOUNCE_BULL
            if within_turnaway_range(high):
                return LevelAction.TURNAWAY_BULL
            if high < level * (1 - threshold):
                return LevelAction.IGNORE_BULL

        if open_ > level:
            if low < level:
                if close_ < level:
                    return LevelAction.BREAK_BEAR
                return LevelAction.FAKEBREAK_BEAR
            if within_pip(low) and close_ > level * (1 + pip_slack):
                return LevelAction.BOUNCE_BEAR
            if within_turnaway_range(low):
                return LevelAction.TURNAWAY_BEAR
            if low > level * (1 + threshold):
                return LevelAction.IGNORE_BEAR

        return LevelAction.UNSPECIFIED


def new_spa(session_name: SessionName) -> SessionPriceAction:
    return SessionPriceAction(
        name=session_name,
        candles_15m=[],
        session_candle=(-1, -1, -1, -1, -1, ""),
        do=(-1, ""),
        true_do=(-1, ""),
        wo=(-1, ""),
        true_wo=(-1, ""),
        mo=(-1, ""),
        true_mo=(-1, ""),
        yo=(-1, ""),
        true_yo=(-1, ""),
        prev_ny_close=(-1, ""),
        prev_cme_close=(-1, ""),
        week_high=(-1, ""),
        week_range_75q=(-1, ""),
        week_range_50q=(-1, ""),
        week_range_25q=(-1, ""),
        week_low=(-1, ""),
    )


if __name__ == "__main__":
    try:
        test = get_from_to(SessionName.NY_OPEN, to_date_str(start_of_day(now_utc_datetime())))
        test1 = get_from_to(SessionName.NY_OPEN, to_date_str(now_utc_datetime()))

        print(f"hello")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
