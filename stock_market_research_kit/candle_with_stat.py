from dataclasses import dataclass
from enum import Enum
from typing import List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
import scipy.stats as stats

from stock_market_research_kit.candle import InnerCandle, as_1_candle
from stock_market_research_kit.db_layer import select_days
from utils.date_utils import start_of_day, to_date_str, to_utc_datetime


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
    vc = ((df_series / 4).round() * 4).value_counts()

    def assign_group(x):
        for group in vc.index:
            if round(x / 4, 0) * 4 == group:
                return f"~{group}"
        return np.nan  # если не попало ни в одну группу

    s_grouped = df_series.apply(assign_group)

    return s_grouped


def to_df(candles: List[InnerCandle]):
    df = pd.DataFrame(candles, columns=['open', 'high', 'low', 'close', 'volume', 'date'])
    df['day_date'] = df['date'].apply(lambda x: to_date_str(start_of_day(to_utc_datetime(x))))

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


def show_correlation_2_sessions(title: str, session1_candles: List[InnerCandle], session2_candles: List[InnerCandle]):
    show_corr_charts(
        title,
        to_df([candle for candle in session1_candles if candle[5] != ""]),
        to_df([candle for candle in session2_candles if candle[5] != ""]),
        ['perf', 'volat', 'upper_wick_fraction', 'body_fraction', 'lower_wick_fraction'],
        ['perf', 'upper_wick_fraction', 'body_fraction', 'lower_wick_fraction'],
    )


def show_corr_charts(title, df1, df2, columns1, columns2):
    pairs = []
    for col1 in columns1:
        for col2 in columns2:
            df1[f"target_{col2}"] = df2[col2]
            corr, p_value = stats.pearsonr(df1[col1], df1[f"target_{col2}"])
            if -0.2 < corr < 0.2:
                continue
            pairs.append((col1, f"target_{col2}", corr, p_value))

    if len(pairs) == 0:
        print(f"No pairs with good correlation for '{title}'!")
        return

    chunks = []
    for i, pair in enumerate(pairs):
        if i % 4 == 0:
            chunks.append([])
        chunks[-1].append(pair)

    for j, chunk in enumerate(chunks):
        fig = plt.figure(figsize=(10, 8))
        gs = gridspec.GridSpec(2, 2)
        print(f"Showing '{title}' chunk {j + 1}/{len(chunks)}...")
        for i, pair in enumerate(chunk):
            match i:
                case 0:
                    ax = fig.add_subplot(gs[0, 0])
                case 1:
                    ax = fig.add_subplot(gs[0, 1])
                case 2:
                    ax = fig.add_subplot(gs[1, 0])
                case 3:
                    ax = fig.add_subplot(gs[1, 1])

            sns.regplot(x=pair[0], y=pair[1], data=df1, ax=ax)
            plt.title(f"Regression: {pair[1]} ~ {pair[0]}, corr={pair[2]:.2f}, p-value={pair[3]:.4f}")

        fig.suptitle(title, fontsize=14)
        plt.tight_layout(rect=[0, 0, 1, 0.985])
        plt.show()


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

    return sequence_df.loc[sequence_df.index.map(lambda x: x.split(' -> ')[0] != x.split(' -> ')[1])]
    # return sequence_df


def predicts_for_incomplete(year_session_candles: List[List[InnerCandle]]):
    df_session = to_df([as_1_candle(sess_candles) for sess_candles in year_session_candles if len(sess_candles) > 0])
    df_minus15 = to_df(
        [as_1_candle(sess_candles[:-1]) for sess_candles in year_session_candles if len(sess_candles) > 0])
    df_minus30 = to_df(
        [as_1_candle(sess_candles[:-2]) for sess_candles in year_session_candles if len(sess_candles) > 0])
    return predicts(df_session, df_minus15), predicts(df_session, df_minus30)


if __name__ == "__main__":
    try:
        days = [
            # *select_days(2023, "BTCUSDT"),
            *select_days(2024, "AVAXUSDT"),
            *select_days(2025, "AVAXUSDT"),
        ]

        # res_cme15, res_cme30 = predicts_for_incomplete([x.cme_open_candles_15m for x in days])
        # res_asia15, res_asia30 = predicts_for_incomplete([x.asian_candles_15m for x in days])
        # res_london15, res_london30 = predicts_for_incomplete([x.london_candles_15m for x in days])
        # res_early15, res_early30 = predicts_for_incomplete([x.early_session_candles_15m for x in days])
        # res_pre15, res_pre30 = predicts_for_incomplete([x.premarket_candles_15m for x in days])
        # res_open15, _ = predicts_for_incomplete([x.ny_am_open_candles_15m for x in days])
        # res_nyam15, res_nyam30 = predicts_for_incomplete([x.ny_am_candles_15m for x in days])
        # res_nylunch15, res_nylunch30 = predicts_for_incomplete([x.ny_lunch_candles_15m for x in days])
        # res_nypm15, res_nypm30 = predicts_for_incomplete([x.ny_pm_candles_15m for x in days])
        # res_close15, res_close30 = predicts_for_incomplete([x.ny_pm_close_candles_15m for x in days])
        # # TODO last 1-2 candles
        # show_correlation_2_sessions(
        #     "Asia[:-1] -> Asia[-1:]",
        #     [as_1_candle([*x.asian_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.asian_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Asia[:-2] -> Asia[-2:]",
        #     [as_1_candle([*x.asian_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.asian_candles_15m[-2:]]) for x in days]
        # )
        #
        # show_correlation_2_sessions(
        #     "London[:-1] -> London[-1:]",
        #     [as_1_candle([*x.london_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.london_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "London[:-2] -> London[-2:]",
        #     [as_1_candle([*x.london_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.london_candles_15m[-2:]]) for x in days]
        # )
        #
        # show_correlation_2_sessions(
        #     "Early[:-1] -> Early[-1:]",
        #     [as_1_candle([*x.early_session_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.early_session_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Early[:-2] -> Early[-2:]",
        #     [as_1_candle([*x.early_session_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.early_session_candles_15m[-2:]]) for x in days]
        # )
        #
        # show_correlation_2_sessions(
        #     "Pre[:-1] -> Pre[-1:]",
        #     [as_1_candle([*x.premarket_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.premarket_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Pre[:-2] -> Pre[-2:]",
        #     [as_1_candle([*x.premarket_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.premarket_candles_15m[-2:]]) for x in days]
        # )
        #
        # show_correlation_2_sessions(
        #     "Open[:-1] -> Open[-1:]",
        #     [as_1_candle([*x.ny_am_open_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.ny_am_open_candles_15m[-1:]]) for x in days]
        # )
        #
        # show_correlation_2_sessions(
        #     "NYAM[:-1] -> NYAM[-1:]",
        #     [as_1_candle([*x.ny_am_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.ny_am_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "NYAM[:-2] -> NYAM[-2:]",
        #     [as_1_candle([*x.ny_am_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.ny_am_candles_15m[-2:]]) for x in days]
        # )
        #
        # show_correlation_2_sessions(
        #     "Open + NYAM[:-1] -> NYAM[-1:]",
        #     [as_1_candle([*x.ny_am_open_candles_15m, *x.ny_am_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.ny_am_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Open + NYAM[:-2] -> NYAM[-2:]",
        #     [as_1_candle([*x.ny_am_open_candles_15m, *x.ny_am_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.ny_am_candles_15m[-2:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Lunch[:-1] -> Lunch[-1:]",
        #     [as_1_candle([*x.ny_lunch_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.ny_lunch_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Lunch[:-2] -> Lunch[-2:]",
        #     [as_1_candle([*x.ny_lunch_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.ny_lunch_candles_15m[-2:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Open + NYAM + Lunch[:-1] -> Lunch[-1:]",
        #     [as_1_candle([*x.ny_am_open_candles_15m, *x.ny_am_candles_15m, *x.ny_lunch_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.ny_lunch_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Lunch[:-2] -> Lunch[-2:]",
        #     [as_1_candle([*x.ny_am_open_candles_15m, *x.ny_am_candles_15m, *x.ny_lunch_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.ny_lunch_candles_15m[-2:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "NYPM[:-1] -> NYPM[-1:]",
        #     [as_1_candle([*x.ny_pm_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.ny_pm_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "NYPM[:-2] -> NYPM[-2:]",
        #     [as_1_candle([*x.ny_pm_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.ny_pm_candles_15m[-2:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Close[:-1] -> Close[-1:]",
        #     [as_1_candle([*x.ny_pm_close_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.ny_pm_close_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Close[:-2] -> Close[-2:]",
        #     [as_1_candle([*x.ny_pm_close_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.ny_pm_close_candles_15m[-2:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "NYPM + Close[:-1] -> Close[-1:]",
        #     [as_1_candle([*x.ny_pm_candles_15m, *x.ny_pm_close_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.ny_pm_close_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "NYPM + Close[:-2] -> Close[-2:]",
        #     [as_1_candle([*x.ny_pm_candles_15m, *x.ny_pm_close_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.ny_pm_close_candles_15m[-2:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Open + NYAM + Lunch + NYPM + Close[:-1] -> Close[-1:]",
        #     [as_1_candle([*x.ny_am_open_candles_15m, *x.ny_am_candles_15m, *x.ny_lunch_candles_15m, *x.ny_pm_candles_15m, *x.ny_pm_close_candles_15m[:-1]]) for x in days],
        #     [as_1_candle([*x.ny_pm_close_candles_15m[-1:]]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Open + NYAM + Lunch + NYPM + Close[:-2] -> Close[-2:]",
        #     [as_1_candle([*x.ny_am_open_candles_15m, *x.ny_am_candles_15m, *x.ny_lunch_candles_15m, *x.ny_pm_candles_15m, *x.ny_pm_close_candles_15m[:-2]]) for x in days],
        #     [as_1_candle([*x.ny_pm_close_candles_15m[-2:]]) for x in days]
        # )
        # # TODO 2 and more sessions
        # show_correlation_2_sessions(
        #     "Asia + London -> PM + Close",
        #     [as_1_candle([*x.asian_candles_15m, *x.london_candles_15m]) for x in days],
        #     [as_1_candle([*x.ny_pm_candles_15m, *x.ny_pm_close_candles_15m]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Asia + London -> Early",
        #     [as_1_candle([*x.asian_candles_15m, *x.london_candles_15m]) for x in days],
        #     [as_1_candle([*x.early_session_candles_15m]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Asia + London -> Early + Pre",
        #     [as_1_candle([*x.asian_candles_15m, *x.london_candles_15m]) for x in days],
        #     [as_1_candle([*x.early_session_candles_15m, *x.premarket_candles_15m]) for x in days]
        # )
        # # TODO Day before
        # show_correlation_2_sessions(
        #     "Day before London -> London",
        #     [as_1_candle([*x.day_candles_before(x.london_candles_15m[0][5])]) for x in days if len(x.london_candles_15m) > 0],
        #     [as_1_candle([*x.london_candles_15m]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Day before Early -> Early",
        #     [as_1_candle([*x.day_candles_before(x.early_session_candles_15m[0][5])]) for x in days if len(x.early_session_candles_15m) > 0],
        #     [as_1_candle([*x.early_session_candles_15m]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Day before Pre -> Pre",
        #     [as_1_candle([*x.day_candles_before(x.premarket_candles_15m[0][5])]) for x in days if len(x.premarket_candles_15m) > 0],
        #     [as_1_candle([*x.premarket_candles_15m]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Day before Open -> Open",
        #     [as_1_candle([*x.day_candles_before(x.ny_am_open_candles_15m[0][5])]) for x in days if len(x.ny_am_open_candles_15m) > 0],
        #     [as_1_candle([*x.ny_am_open_candles_15m]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Day before NYAM -> NYAM",
        #     [as_1_candle([*x.day_candles_before(x.ny_am_candles_15m[0][5])]) for x in days if len(x.ny_am_candles_15m) > 0],
        #     [as_1_candle([*x.ny_am_candles_15m]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Day before Lunch -> Lunch",
        #     [as_1_candle([*x.day_candles_before(x.ny_lunch_candles_15m[0][5])]) for x in days if len(x.ny_lunch_candles_15m) > 0],
        #     [as_1_candle([*x.ny_lunch_candles_15m]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Day before PM -> PM",
        #     [as_1_candle([*x.day_candles_before(x.ny_pm_candles_15m[0][5])]) for x in days if len(x.ny_pm_candles_15m) > 0],
        #     [as_1_candle([*x.ny_pm_candles_15m]) for x in days]
        # )
        # show_correlation_2_sessions(
        #     "Day before Close -> Close",
        #     [as_1_candle([*x.day_candles_before(x.ny_pm_close_candles_15m[0][5])]) for x in days if len(x.ny_pm_close_candles_15m) > 0],
        #     [as_1_candle([*x.ny_pm_close_candles_15m]) for x in days]
        # )
        # # TODO session -> 1d
        # show_correlation_2_sessions("London -> 1D", [x.london_as_candle for x in days], [x.candle_1d for x in days])
        # show_correlation_2_sessions("Early -> 1D", [x.early_session_as_candle for x in days], [x.candle_1d for x in days])
        # show_correlation_2_sessions("Pre -> 1D", [x.premarket_as_candle for x in days], [x.candle_1d for x in days])
        # show_correlation_2_sessions("Open -> 1D", [x.ny_am_open_as_candle for x in days], [x.candle_1d for x in days])
        # show_correlation_2_sessions("NYAM -> 1D", [x.ny_am_as_candle for x in days], [x.candle_1d for x in days])
        # show_correlation_2_sessions("Lunch -> 1D", [x.ny_lunch_as_candle for x in days], [x.candle_1d for x in days])
        # show_correlation_2_sessions("PM -> 1D", [x.ny_pm_as_candle for x in days], [x.candle_1d for x in days])
        # show_correlation_2_sessions("Close -> 1D", [x.ny_pm_close_as_candle for x in days], [x.candle_1d for x in days])
        # # TODO session1 -> session2
        # show_correlation_2_sessions("Pre -> Open", [x.premarket_as_candle for x in days], [x.ny_am_open_as_candle for x in days])
        # show_correlation_2_sessions("Open -> AM", [x.ny_am_open_as_candle for x in days], [x.ny_am_as_candle for x in days])
        # show_correlation_2_sessions("AM -> Lunch", [x.ny_am_as_candle for x in days], [x.ny_lunch_as_candle for x in days])
        # show_correlation_2_sessions("AM -> PM", [x.ny_am_as_candle for x in days], [x.ny_pm_as_candle for x in days])
        # show_correlation_2_sessions("Lunch -> PM", [x.ny_lunch_as_candle for x in days], [x.ny_pm_as_candle for x in days])
        # show_correlation_2_sessions("PM -> Close", [x.ny_pm_as_candle for x in days], [x.ny_pm_close_as_candle for x in days])
        # # TODO первая и вторая свечка сессии
        print("done!")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
