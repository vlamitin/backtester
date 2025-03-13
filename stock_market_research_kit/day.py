from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class Day:
    day_of_week: int
    date_readable: str

    candle_1d: Tuple[float, float, float, float, float, str]
    candles_1h: List[Tuple[float, float, float, float, float, str]]
    candles_15m: List[Tuple[float, float, float, float, float, str]]

    prev_day_candle_1d: Tuple[float, float, float, float, float, str]
    prev_day_candles_15m: List[Tuple[float, float, float, float, float, str]]

    cme_open_candles_15m: List[Tuple[float, float, float, float, float, str]]  # 18:00 - 19:00 NY time
    asian_candles_15m: List[Tuple[float, float, float, float, float, str]]  # 19:00 - 22:00 NY time
    london_candles_15m: List[Tuple[float, float, float, float, float, str]]  # 02:00 - 05:00 NY time
    early_session_candles_15m: List[Tuple[float, float, float, float, float, str]]  # 07:00 - 08:00 NY time
    premarket_candles_15m: List[Tuple[float, float, float, float, float, str]]  # 08:00 - 09:30 NY time
    ny_am_open_candles_15m: List[Tuple[float, float, float, float, float, str]]  # 09:30 - 10:00 NY time
    ny_am_candles_15m: List[Tuple[float, float, float, float, float, str]]  # 10:00 - 12:00 NY time
    ny_lunch_candles_15m: List[Tuple[float, float, float, float, float, str]]  # 12:00 - 13:00 NY time
    ny_pm_candles_15m: List[Tuple[float, float, float, float, float, str]]  # 13:00 - 15:00 NY time
    ny_pm_close_candles_15m: List[Tuple[float, float, float, float, float, str]]  # 15:00 - 16:00 NY time

    cme_as_candle: Tuple[float, float, float, float, float, str]
    asia_as_candle: Tuple[float, float, float, float, float, str]
    london_as_candle: Tuple[float, float, float, float, float, str]
    early_session_as_candle: Tuple[float, float, float, float, float, str]
    premarket_as_candle: Tuple[float, float, float, float, float, str]
    ny_am_open_as_candle: Tuple[float, float, float, float, float, str]
    ny_am_as_candle: Tuple[float, float, float, float, float, str]
    ny_lunch_as_candle: Tuple[float, float, float, float, float, str]
    ny_pm_as_candle: Tuple[float, float, float, float, float, str]
    ny_pm_close_as_candle: Tuple[float, float, float, float, float, str]


def new_day():
    return Day(
        day_of_week=-1,
        date_readable="",
        candle_1d=(-1, -1, -1, -1, 0, ""),
        candles_1h=[],
        candles_15m=[],
        prev_day_candle_1d=(-1, -1, -1, -1, 0, ""),
        prev_day_candles_15m=[],
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
