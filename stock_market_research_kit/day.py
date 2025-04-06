import json
from dataclasses import dataclass, asdict
from typing import List

from stock_market_research_kit.candle import InnerCandle


def day_from_json(json_str):
    return Day.from_json(json.loads(json_str))


@dataclass
class Day:
    day_of_week: int
    date_readable: str

    candle_1d: InnerCandle
    candles_15m: List[InnerCandle]

    # do: float  # TODO заполнить скриптом и это
    # true_do: float
    # wo: float
    # true_wo: float
    # mo: float
    # true_mo: float
    # yo: float
    # true_yo: float

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

    @classmethod
    def from_json(cls, data: dict):
        known_fields = set(cls.__dataclass_fields__.keys())  # Get dataclass fields
        filtered_data = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered_data)

    def to_db_format(self, symbol: str):
        return symbol, self.date_readable, json.dumps(asdict(self), indent=4)


def new_day():
    return Day(
        day_of_week=-1,
        date_readable="",
        candle_1d=(-1, -1, -1, -1, 0, ""),
        candles_15m=[],
        # do=-1,
        # true_do=-1,
        # wo=-1,
        # true_wo=-1,
        # mo=-1,
        # true_mo=-1,
        # yo=-1,
        # true_yo=-1,
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
