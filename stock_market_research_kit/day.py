import json
from dataclasses import dataclass, asdict
from datetime import timedelta
from enum import Enum
from typing import List, Optional

from stock_market_research_kit.candle import InnerCandle, PriceDate
from stock_market_research_kit.session import SessionName, SessionPriceAction, new_spa, sessions_in_order
from utils.date_utils import to_utc_datetime, to_date_str


@dataclass
class Day:
    day_of_week: int
    date_readable: str

    candle_1d: InnerCandle
    candles_15m: List[InnerCandle]

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

    prev_week_high: PriceDate
    prev_week_range_75q: PriceDate
    prev_week_range_50q: PriceDate
    prev_week_range_25q: PriceDate
    prev_week_low: PriceDate

    # month_high: PriceDate
    # month_range_75q: PriceDate
    # month_range_50q: PriceDate
    # month_range_25q: PriceDate
    # month_low: PriceDate

    # prev_month_high: PriceDate
    # prev_month_range_75q: PriceDate
    # prev_month_range_50q: PriceDate
    # prev_month_range_25q: PriceDate
    # prev_month_low: PriceDate

    cme: Optional[SessionPriceAction]  # 18:00 - 19:00 NY time
    asia: Optional[SessionPriceAction]  # 19:00 - 22:00 NY time
    london: Optional[SessionPriceAction]  # 02:00 - 05:00 NY time
    early_session: Optional[SessionPriceAction]  # 07:00 - 08:00 NY time
    premarket: Optional[SessionPriceAction]  # 08:00 - 09:30 NY time
    ny_am_open: Optional[SessionPriceAction]  # 09:30 - 10:00 NY time
    ny_am: Optional[SessionPriceAction]  # 10:00 - 12:00 NY time
    ny_lunch: Optional[SessionPriceAction]  # 12:00 - 13:00 NY time
    ny_pm: Optional[SessionPriceAction]  # 13:00 - 15:00 NY time
    ny_pm_close: Optional[SessionPriceAction]  # 15:00 - 16:00 NY time

    def candles_before_date(self, date: str):
        if date == "":
            raise ValueError("day_candles_before date is empty!")
        result = []
        date_before = to_utc_datetime(date)
        for candle in self.candles_15m:
            if to_utc_datetime(candle[5]) < date_before:
                result.append(candle)
            else:
                break
        return result

    def candles_before_session(self, session_name: SessionName):
        session_index = sessions_in_order.index(session_name)
        if session_index == 0:
            return []
        prev_spa = self.spa_by_session(sessions_in_order[session_index - 1])
        return self.candles_before_date(
            to_date_str(to_utc_datetime(prev_spa.candles_15m[-1][5]) + timedelta(minutes=15)))

    def candles_by_session(self, session_name: SessionName) -> List[InnerCandle]:
        spa = self.spa_by_session(session_name)
        return [] if not spa else spa.candles_15m

    def spa_by_session(self, session_name: SessionName) -> Optional[SessionPriceAction]:
        match session_name:
            case SessionName.CME:
                return self.cme
            case SessionName.ASIA:
                return self.asia
            case SessionName.LONDON:
                return self.london
            case SessionName.EARLY:
                return self.early_session
            case SessionName.PRE:
                return self.premarket
            case SessionName.NY_OPEN:
                return self.ny_am_open
            case SessionName.NY_AM:
                return self.ny_am
            case SessionName.NY_LUNCH:
                return self.ny_lunch
            case SessionName.NY_PM:
                return self.ny_pm
            case SessionName.NY_CLOSE:
                return self.ny_pm_close
        return None


def day_decoder(dct):
    if "date_readable" in dct:
        return Day(**dct)
    if "name" in dct and "session_candle" in dct:
        dct["name"] = SessionName(dct["name"])
    return SessionPriceAction(**dct)


def day_from_json(json_str):
    return json.loads(json_str, object_hook=day_decoder)


def enum_serializer(obj):
    if isinstance(obj, Enum):
        return obj.value
    elif isinstance(obj, SessionPriceAction):
        return asdict(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def json_from_day(day):
    return json.dumps(asdict(day), default=enum_serializer, indent=4)


def json_from_days(days):
    return json.dumps([day.__dict__ for day in days], default=enum_serializer, indent=4)


def new_day():
    return Day(
        day_of_week=-1,
        date_readable="",
        candle_1d=(-1, -1, -1, -1, 0, ""),
        candles_15m=[],
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
        prev_week_high=(-1, ""),
        prev_week_range_75q=(-1, ""),
        prev_week_range_50q=(-1, ""),
        prev_week_range_25q=(-1, ""),
        prev_week_low=(-1, ""),
        cme=None,
        asia=None,
        london=None,
        early_session=None,
        premarket=None,
        ny_am_open=None,
        ny_am=None,
        ny_lunch=None,
        ny_pm=None,
        ny_pm_close=None,
    )


if __name__ == '__main__':
    try:
        d = new_day()
        d.london = new_spa(SessionName.LONDON)
        marsh_d = json_from_day(d)
        unm_d = day_from_json(marsh_d)
        marshalled_list = json_from_days([d])
        unmarshalled_list = [x for x in json.loads(marshalled_list, object_hook=day_decoder)]
        print(marshalled_list, unmarshalled_list)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
