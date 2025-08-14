from typing import TypeAlias, Tuple, List

from utils.date_utils import to_utc_datetime, get_current_30m_from_to, get_current_1h_from_to, get_current_2h_from_to, \
    get_current_4h_from_to, get_current_1d_from_to, get_current_1w_from_to, get_current_1month_from_to

InnerCandle: TypeAlias = Tuple[float, float, float, float, float, str]

PriceDate: TypeAlias = Tuple[float, str]


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

    return open_, high, low, close, round(volume, 3), date


def as_30m_candles(candles_15m: List[InnerCandle]) -> List[InnerCandle]:
    if len(candles_15m) == 0:
        return []

    result = []
    for candle in candles_15m:
        if len(result) > 0:
            prev_date = to_utc_datetime(result[-1][5])
            curr_range = get_current_30m_from_to(candle[5])

            if curr_range[0] <= prev_date < curr_range[1]:
                result[-1] = as_1_candle([result[-1], candle])
            else:
                result.append(candle)
        else:
            result.append(candle)

    return result


def as_1h_candles(candles_15m: List[InnerCandle]) -> List[InnerCandle]:
    if len(candles_15m) == 0:
        return []

    result = []
    for candle in candles_15m:
        if len(result) > 0:
            prev_date = to_utc_datetime(result[-1][5])
            curr_range = get_current_1h_from_to(candle[5])

            if curr_range[0] <= prev_date < curr_range[1]:
                result[-1] = as_1_candle([result[-1], candle])
            else:
                result.append(candle)
        else:
            result.append(candle)

    return result


def as_2h_candles(candles_15m: List[InnerCandle]) -> List[InnerCandle]:
    if len(candles_15m) == 0:
        return []

    result = []
    for candle in candles_15m:
        if len(result) > 0:
            prev_date = to_utc_datetime(result[-1][5])
            curr_range = get_current_2h_from_to(candle[5])

            if curr_range[0] <= prev_date < curr_range[1]:
                result[-1] = as_1_candle([result[-1], candle])
            else:
                result.append(candle)
        else:
            result.append(candle)

    return result


def as_4h_candles(candles_15m: List[InnerCandle]) -> List[InnerCandle]:
    if len(candles_15m) == 0:
        return []

    result = []
    for candle in candles_15m:
        if len(result) > 0:
            prev_date = to_utc_datetime(result[-1][5])
            curr_range = get_current_4h_from_to(candle[5])

            if curr_range[0] <= prev_date < curr_range[1]:
                result[-1] = as_1_candle([result[-1], candle])
            else:
                result.append(candle)
        else:
            result.append(candle)

    return result


def as_1d_candles(candles_15m: List[InnerCandle]) -> List[InnerCandle]:
    if len(candles_15m) == 0:
        return []

    result = []
    for candle in candles_15m:
        if len(result) > 0:
            prev_date = to_utc_datetime(result[-1][5])
            curr_range = get_current_1d_from_to(candle[5])

            if curr_range[0] <= prev_date < curr_range[1]:
                result[-1] = as_1_candle([result[-1], candle])
            else:
                result.append(candle)
        else:
            result.append(candle)

    return result


def as_1w_candles(candles_15m: List[InnerCandle]) -> List[InnerCandle]:
    if len(candles_15m) == 0:
        return []

    result = []
    for candle in candles_15m:
        if len(result) > 0:
            prev_date = to_utc_datetime(result[-1][5])
            curr_range = get_current_1w_from_to(candle[5])

            if curr_range[0] <= prev_date < curr_range[1]:
                result[-1] = as_1_candle([result[-1], candle])
            else:
                result.append(candle)
        else:
            result.append(candle)

    return result


def as_1month_candles(candles_15m: List[InnerCandle]) -> List[InnerCandle]:
    if len(candles_15m) == 0:
        return []

    result = []
    for candle in candles_15m:
        if len(result) > 0:
            prev_date = to_utc_datetime(result[-1][5])
            curr_range = get_current_1month_from_to(candle[5])

            if curr_range[0] <= prev_date < curr_range[1]:
                result[-1] = as_1_candle([result[-1], candle])
            else:
                result.append(candle)
        else:
            result.append(candle)

    return result
