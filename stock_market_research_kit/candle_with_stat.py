from dataclasses import dataclass
from enum import Enum
from typing import List

import numpy as np
import pandas as pd

from stock_market_research_kit.candle import InnerCandle, as_1_candle
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


def group_by_popular(df_series):
    vc = df_series.value_counts().sort_index()

    step = 0.1
    intervals = []
    for val in vc.index:
        low = val * (1 - step)
        high = val * (1 + step)
        intervals.append((val, low, high))

    def assign_group(x):
        for center, low, high in intervals:
            if low <= x <= high:
                return f"{low:.1f}–{high:.1f}"
        return np.nan  # если не попало ни в одну группу

    s_grouped = df_series.apply(assign_group)

    return s_grouped

    # grouped_freq = s_grouped.value_counts()
    # # grouped_freq.apply(lambda x: x / 262 * 100)
    # res = pd.DataFrame(grouped_freq, columns=['count'])
    # res['percent'] = grouped_freq.apply(lambda x: x / len(df_series) * 100)
    # return res


def to_df(candles: List[InnerCandle]):
    df = pd.DataFrame(candles, columns=['open', 'high', 'low', 'close', 'volume', 'date'])

    df['volume_perc_all'], df['volume_perc_sma20'] = perc_all_and_sma20(df['volume'])

    df['perf'] = (df['close'] - df['open']) / df['open'] * 100
    perf_perc = [float(x) for x in np.percentile(df['perf'], [10, 30, 50, 70, 90])]
    df['perf_perc_all'], df['perf_perc_sma20'] = perc_all_and_sma20(df['perf'])

    df['volat'] = (df['high'] - df['low']) / df['open'] * 100
    volat_perc = [float(x) for x in np.percentile(df['volat'], [10, 30, 50, 70, 90])]
    df['volat_perc_all'], df['volat_perc_sma20'] = perc_all_and_sma20(df['volat'])

    candle_range = (df['high'] - df['low']).replace(0, np.nan)

    df['upper_wick_fraction'] = (df['high'] - np.maximum(df['open'], df['close'])) / candle_range * 100
    upper_wick_fraction_perc = [float(x) for x in np.percentile(df['upper_wick_fraction'], [10, 30, 50, 70, 90])]
    df['upper_wick_fraction_perc_all'], df['upper_wick_fraction_perc_sma20'] = perc_all_and_sma20(
        df['upper_wick_fraction'])

    df['body_fraction'] = np.abs(df['open'] - df['close']) / candle_range * 100
    body_fraction_perc = [float(x) for x in np.percentile(df['body_fraction'], [10, 30, 50, 70, 90])]
    df['body_fraction_perc_all'], df['body_fraction_perc_sma20'] = perc_all_and_sma20(
        df['body_fraction'])
    df['body_fraction_popular_groups'] = group_by_popular(df['body_fraction'])

    # df['body_fraction'].value_counts()

    df['lower_wick_fraction'] = (np.minimum(df['open'], df['close']) - df['low']) / candle_range * 100
    lower_wick_fraction_perc = [float(x) for x in np.percentile(df['lower_wick_fraction'], [10, 30, 50, 70, 90])]
    df['lower_wick_fraction_perc_all'], df['lower_wick_fraction_perc_sma20'] = perc_all_and_sma20(
        df['lower_wick_fraction'])

    df['min_safe_stop_bull'] = (df['open'] - df['low']) / df['open'] * 100
    min_safe_stop_bull_percentiles = np.percentile(df['min_safe_stop_bull'], [10, 30, 50, 70, 90])

    df['min_safe_stop_bear'] = (df['high'] - df['open']) / df['open'] * 100
    min_safe_stop_bear_percentiles = np.percentile(df['min_safe_stop_bear'], [10, 30, 50, 70, 90])

    return df


def to_candles_with_stat(df: pd.DataFrame):
    return [to_candle_with_stat(x) for x in df.to_dict(orient='records')]


def predicts(df_session, df_incomplete_session):
    df = pd.DataFrame({
        'date': df_session['date'],
        'body_fraction_popular_groups_incomplete': df_incomplete_session['body_fraction_popular_groups'],
        'body_fraction_popular_groups_session': df_session['body_fraction_popular_groups'],
    })

    df['body_fraction_sequence'] = df['body_fraction_popular_groups_incomplete'].astype(str) + ' -> ' + df[
        'body_fraction_popular_groups_session'].astype(str)
    counts = df['body_fraction_sequence'].value_counts()

    sequence_df = pd.DataFrame({
        'count': counts,
        'percent_total': (counts / len(df) * 100).round(2),
        'percent_incomplete': df.groupby('body_fraction_sequence')['body_fraction_popular_groups_incomplete'].apply(
            lambda x: len(x) / df['body_fraction_popular_groups_incomplete'].value_counts().get(x.iloc[0], 1) * 100),
        'percent_session': df.groupby('body_fraction_sequence')['body_fraction_popular_groups_session'].apply(
            lambda x: len(x) / df['body_fraction_popular_groups_session'].value_counts().get(x.iloc[0], 1) * 100),
    })
    sequence_df = sequence_df.sort_values(by='count', ascending=False)

    return sequence_df


def predicts_for_incomplete(year_session_candles: List[List[InnerCandle]]):
    df_session = to_df([as_1_candle(scs) for scs in year_session_candles if len(scs) > 0])
    df_minus15 = to_df([as_1_candle(scs[:-1]) for scs in year_session_candles if len(scs) > 0])
    df_minus30 = to_df([as_1_candle(scs[:-2]) for scs in year_session_candles if len(scs) > 0])
    return predicts(df_session, df_minus15), predicts(df_session, df_minus30)


if __name__ == "__main__":
    try:
        days = [
            *select_days(2023, "BTCUSDT"),
            *select_days(2024, "BTCUSDT"),
            *select_days(2025, "BTCUSDT"),
        ]

        res_cme15, res_cme30 = predicts_for_incomplete([x.cme_open_candles_15m for x in days])
        res_asia15, res_asia30 = predicts_for_incomplete([x.asian_candles_15m for x in days])
        res_london15, res_london30 = predicts_for_incomplete([x.london_candles_15m for x in days])
        res_early15, res_early30 = predicts_for_incomplete([x.early_session_candles_15m for x in days])
        res_pre15, res_pre30 = predicts_for_incomplete([x.premarket_candles_15m for x in days])
        res_open15, res_open30 = predicts_for_incomplete([x.ny_am_open_candles_15m for x in days])
        res_nyam15, res_nyam30 = predicts_for_incomplete([x.ny_am_candles_15m for x in days])
        res_nylunch15, res_nylunch30 = predicts_for_incomplete([x.ny_lunch_candles_15m for x in days])
        res_nypm15, res_nypm30 = predicts_for_incomplete([x.ny_pm_candles_15m for x in days])
        res_close15, res_close30 = predicts_for_incomplete([x.ny_pm_close_candles_15m for x in days])
        print("done!")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
