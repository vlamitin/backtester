import json
from dataclasses import dataclass, asdict
from enum import Enum
from typing import List, Optional

from stock_market_research_kit.candle import InnerCandle, PriceDate
from stock_market_research_kit.session import SessionName, SessionPriceAction, new_spa
from utils.date_utils import to_utc_datetime


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

    def day_candles_before(self, date: str):
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

    def candles_by_session(self, session_name: SessionName) -> List[InnerCandle]:
        match session_name:
            case SessionName.CME:
                return [] if not self.cme else self.cme.candles_15m
            case SessionName.ASIA:
                return [] if not self.asia else self.asia.candles_15m
            case SessionName.LONDON:
                return [] if not self.london else self.london.candles_15m
            case SessionName.EARLY:
                return [] if not self.early_session else self.early_session.candles_15m
            case SessionName.PRE:
                return [] if not self.premarket else self.premarket.candles_15m
            case SessionName.NY_OPEN:
                return [] if not self.ny_am_open else self.ny_am_open.candles_15m
            case SessionName.NY_AM:
                return [] if not self.ny_am else self.ny_am.candles_15m
            case SessionName.NY_LUNCH:
                return [] if not self.ny_lunch else self.ny_lunch.candles_15m
            case SessionName.NY_PM:
                return [] if not self.ny_pm else self.ny_pm.candles_15m
            case SessionName.NY_CLOSE:
                return [] if not self.ny_pm_close else self.ny_pm_close.candles_15m
        return []

    @classmethod
    def from_json(cls, data_str: str):
        data_dict = json.loads(data_str)
        known_fields = set(cls.__dataclass_fields__.keys())  # Get dataclass fields
        filtered_data = {k: v for k, v in data_dict.items() if k in known_fields}
        return cls(**filtered_data)


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
