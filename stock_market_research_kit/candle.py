from typing import TypeAlias, Tuple, List

InnerCandle: TypeAlias = Tuple[float, float, float, float, float, str]


def as_1_candle(candles: List[InnerCandle]) -> InnerCandle:
    if len(candles) == 0:
        return -1, -1, -1, -1, 0, ""

    open_, high, low, close, volume, date = candles[0][0], -1, -1, candles[-1][3], 0, candles[0][5]

    for _, h, l, _, v, _ in candles:
        volume += v
        if h >= high or high == -1:
            high = h
        if l <= low or low == -1:
            low = l

    return open_, high, low, close, volume, date
