import json
from dataclasses import dataclass, asdict
from typing import List, TypeAlias, Tuple

from stock_market_research_kit.candle import InnerCandle
from stock_market_research_kit.session import SessionName
from utils.date_utils import to_utc_datetime


def day_from_json(json_str):
    return Day.from_json(json.loads(json_str))


PriceDate: TypeAlias = Tuple[float, str]


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

    # prev_ny_close: PriceDate  # 16:00 NY
    # prev_cme_close: PriceDate  # 17:00 NY, it's also called True daily close

    # week_high: PriceDate
    # week_range_75q: PriceDate
    # week_range_50q: PriceDate
    # week_range_25q: PriceDate
    # week_low: PriceDate

    # prev_week_high: PriceDate
    # prev_week_range_75q: PriceDate
    # prev_week_range_50q: PriceDate
    # prev_week_range_25q: PriceDate
    # prev_week_low: PriceDate

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

    cme_open_candles_15m: List[InnerCandle]  # 18:00 - 19:00 NY time
    asian_candles_15m: List[InnerCandle]  # 19:00 - 22:00 NY time
    london_candles_15m: List[InnerCandle]  # 02:00 - 05:00 NY time
    early_session_candles_15m: List[InnerCandle]  # 07:00 - 08:00 NY time
    premarket_candles_15m: List[InnerCandle]  # 08:00 - 09:30 NY time
    ny_am_open_candles_15m: List[InnerCandle]  # 09:30 - 10:00 NY time
    ny_am_candles_15m: List[InnerCandle]  # 10:00 - 12:00 NY time
    ny_lunch_candles_15m: List[InnerCandle]  # 12:00 - 13:00 NY time
    ny_pm_candles_15m: List[InnerCandle]  # 13:00 - 15:00 NY time
    ny_pm_close_candles_15m: List[InnerCandle]  # 15:00 - 16:00 NY time

    cme_as_candle: InnerCandle
    asia_as_candle: InnerCandle
    london_as_candle: InnerCandle
    early_session_as_candle: InnerCandle
    premarket_as_candle: InnerCandle
    ny_am_open_as_candle: InnerCandle
    ny_am_as_candle: InnerCandle
    ny_lunch_as_candle: InnerCandle
    ny_pm_as_candle: InnerCandle
    ny_pm_close_as_candle: InnerCandle

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
                return self.cme_open_candles_15m
            case SessionName.ASIA:
                return self.asian_candles_15m
            case SessionName.LONDON:
                return self.london_candles_15m
            case SessionName.EARLY:
                return self.early_session_candles_15m
            case SessionName.PRE:
                return self.premarket_candles_15m
            case SessionName.NY_OPEN:
                return self.ny_am_open_candles_15m
            case SessionName.NY_AM:
                return self.ny_am_candles_15m
            case SessionName.NY_LUNCH:
                return self.ny_lunch_candles_15m
            case SessionName.NY_PM:
                return self.ny_pm_candles_15m
            case SessionName.NY_CLOSE:
                return self.ny_pm_close_candles_15m
        return []

    @classmethod
    def from_json(cls, data: dict):
        known_fields = set(cls.__dataclass_fields__.keys())  # Get dataclass fields
        filtered_data = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered_data)


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
        # pdo
        # pdc
        cme_open_candles_15m=[],
        asian_candles_15m=[],
        london_candles_15m=[],
        early_session_candles_15m=[],
        premarket_candles_15m=[],
        ny_am_open_candles_15m=[],
        ny_am_candles_15m=[],
        ny_lunch_candles_15m=[],
        ny_pm_candles_15m=[],
        ny_pm_close_candles_15m=[],
        cme_as_candle=(-1, -1, -1, -1, 0, ""),
        asia_as_candle=(-1, -1, -1, -1, 0, ""),
        london_as_candle=(-1, -1, -1, -1, 0, ""),
        early_session_as_candle=(-1, -1, -1, -1, 0, ""),
        premarket_as_candle=(-1, -1, -1, -1, 0, ""),
        ny_am_open_as_candle=(-1, -1, -1, -1, 0, ""),
        ny_am_as_candle=(-1, -1, -1, -1, 0, ""),
        ny_lunch_as_candle=(-1, -1, -1, -1, 0, ""),
        ny_pm_as_candle=(-1, -1, -1, -1, 0, ""),
        ny_pm_close_as_candle=(-1, -1, -1, -1, 0, "")
    )


if __name__ == '__main__':
    try:
        d = new_day()
        marshalled = json.dumps([asdict(d)], indent=4)
        unmarshalled = [Day.from_json(x) for x in json.loads(marshalled)]
        print(marshalled, unmarshalled)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
