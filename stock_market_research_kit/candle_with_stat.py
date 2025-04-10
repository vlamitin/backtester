from dataclasses import dataclass
from enum import Enum
from typing import List

import numpy as np
import pandas as pd

from stock_market_research_kit.candle import InnerCandle
from stock_market_research_kit.db_layer import select_days


class PercentileGroup(Enum):
    UNSPECIFIED = 'UNSPECIFIED'

    LP10 = '<p10'
    LP30 = '<p30'
    LP70 = '<p70'
    LP90 = '<p90'
    HP90 = '>=p90'


@dataclass
class CandleWithStat:
    open: float
    high: float
    low: float
    close: float
    volume: float
    date: str

    volume_perc_all: PercentileGroup
    volume_perc_sma20: PercentileGroup

    perf: float
    perf_perc_all: PercentileGroup
    perf_perc_sma20: PercentileGroup

    volat: float
    volat_perc_all: PercentileGroup
    volat_perc_sma20: PercentileGroup

    upper_wick_fraction: float
    upper_wick_fraction_perc_all: PercentileGroup
    upper_wick_fraction_perc_sma20: PercentileGroup

    body_fraction: float
    body_fraction_perc_all: PercentileGroup
    body_fraction_perc_sma20: PercentileGroup

    lower_wick_fraction: float
    lower_wick_fraction_perc_all: PercentileGroup
    lower_wick_fraction_perc_sma20: PercentileGroup


def to_candle_with_stat(dct):
    if "perf_perc_all" in dct:
        dct["perf_perc_all"] = PercentileGroup(dct["perf_perc_all"])
    if "perf_perc_sma20" in dct:
        dct["perf_perc_sma20"] = PercentileGroup(dct["perf_perc_sma20"])
    if "volat_perc_all" in dct:
        dct["volat_perc_all"] = PercentileGroup(dct["volat_perc_all"])
    if "volat_perc_sma20" in dct:
        dct["volat_perc_sma20"] = PercentileGroup(dct["volat_perc_sma20"])
    if "upper_wick_fraction_perc_all" in dct:
        dct["upper_wick_fraction_perc_all"] = PercentileGroup(dct["upper_wick_fraction_perc_all"])
    if "upper_wick_fraction_perc_sma20" in dct:
        dct["upper_wick_fraction_perc_sma20"] = PercentileGroup(dct["upper_wick_fraction_perc_sma20"])
    if "body_fraction_perc_all" in dct:
        dct["body_fraction_perc_all"] = PercentileGroup(dct["body_fraction_perc_all"])
    if "body_fraction_perc_sma20" in dct:
        dct["body_fraction_perc_sma20"] = PercentileGroup(dct["body_fraction_perc_sma20"])
    if "lower_wick_fraction_perc_all" in dct:
        dct["lower_wick_fraction_perc_all"] = PercentileGroup(dct["lower_wick_fraction_perc_all"])
    if "lower_wick_fraction_perc_sma20" in dct:
        dct["lower_wick_fraction_perc_sma20"] = PercentileGroup(dct["lower_wick_fraction_perc_sma20"])
    return CandleWithStat(**dct)


def get_volat_group(value, window_values):
    percentiles = np.percentile(window_values, [10, 30, 70, 90])
    if value < percentiles[0]:
        return PercentileGroup.LP10.value
    elif value < percentiles[1]:
        return PercentileGroup.LP30.value
    elif value < percentiles[2]:
        return PercentileGroup.LP70.value
    elif value < percentiles[3]:
        return PercentileGroup.LP90.value
    return PercentileGroup.HP90.value


def sma_percentile_group(series, window_size):
    return [
        get_volat_group(series.iloc[i],
                        series.iloc[max(0, i - window_size - 1):i + 1]) if i >= window_size - 1 else 'UNSPECIFIED'
        for i in range(len(series))
    ]


def perc_all_and_sma20(df_array):
    percentiles = np.percentile(df_array, [10, 30, 70, 90])
    return (
        np.select(
            [
                df_array < percentiles[0],
                df_array < percentiles[1],
                df_array < percentiles[2],
                df_array < percentiles[3],
            ], [
                PercentileGroup.LP10.value, PercentileGroup.LP30.value, PercentileGroup.LP70.value,
                PercentileGroup.LP90.value
            ], default=PercentileGroup.HP90.value
        ),
        sma_percentile_group(df_array, 20)
    )


def to_candles_with_stat(candles: List[InnerCandle]):
    df = pd.DataFrame(candles, columns=['open', 'high', 'low', 'close', 'volume', 'date'])

    df['volume_perc_all'], df['volume_perc_sma20'] = perc_all_and_sma20(df['volume'])

    df['perf'] = (df['close'] - df['open']) / df['open'] * 100
    df['perf_perc_all'], df['perf_perc_sma20'] = perc_all_and_sma20(df['perf'])

    df['volat'] = (df['high'] - df['low']) / df['open'] * 100
    df['volat_perc_all'], df['volat_perc_sma20'] = perc_all_and_sma20(df['volat'])

    candle_range = (df['high'] - df['low']).replace(0, np.nan)

    df['upper_wick_fraction'] = (df['high'] - np.maximum(df['open'], df['close'])) / candle_range * 100
    df['upper_wick_fraction_perc_all'], df['upper_wick_fraction_perc_sma20'] = perc_all_and_sma20(
        df['upper_wick_fraction'])

    df['body_fraction'] = np.abs(df['open'] - df['close']) / candle_range * 100
    df['body_fraction_perc_all'], df['body_fraction_perc_sma20'] = perc_all_and_sma20(
        df['body_fraction'])

    df['lower_wick_fraction'] = (np.minimum(df['open'], df['close']) - df['low']) / candle_range * 100
    df['lower_wick_fraction_perc_all'], df['lower_wick_fraction_perc_sma20'] = perc_all_and_sma20(
        df['lower_wick_fraction'])

    return [to_candle_with_stat(x) for x in df.to_dict(orient='records')]


if __name__ == "__main__":
    try:
        crv_days = select_days(2024, "CRVUSDT")
        london_candles = [x.london_as_candle for x in crv_days if x.london_as_candle[5] != ""]

        res = to_candles_with_stat(london_candles)
        print(res)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
