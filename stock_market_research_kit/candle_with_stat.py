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
from stock_market_research_kit.day import Day
from stock_market_research_kit.db_layer import select_days
from stock_market_research_kit.session import SessionName, sessions_in_order
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


def to_days_df(days: List[Day]):
    df = pd.DataFrame(days, columns=['date_readable'])
    return df


def to_sessions_df(session_candles: List[InnerCandle], days: List[Day]):
    df = pd.DataFrame(session_candles, columns=['open', 'high', 'low', 'close', 'volume', 'date'])
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


def days_matching_session_df_condition(days, session_name, condition):
    session_candles = [as_1_candle(x.candles_by_session(session_name)) for x in days if
                       len(x.candles_by_session(session_name)) > 0]
    sess_df = to_sessions_df(session_candles, days)
    indices = sess_df.index[condition(sess_df)].to_list()
    return [days[x] for x in indices]


def show_correlation_2_sessions(title: str, days: List[Day],
                                session1_candles: List[InnerCandle], session2_candles: List[InnerCandle]):
    show_corr_charts(
        title,
        to_sessions_df([candle for candle in session1_candles if candle[5] != ""], days),
        to_sessions_df([candle for candle in session2_candles if candle[5] != ""], days),
        ['perf', 'volat', 'upper_wick_fraction', 'body_fraction', 'lower_wick_fraction'],
        ['perf', 'upper_wick_fraction', 'body_fraction', 'lower_wick_fraction', 'min_safe_stop_bull',
         'min_safe_stop_bear']
    )


def show_corr_charts(title, df1, df2, columns1, columns2):
    pairs = []
    for col1 in columns1:
        for col2 in columns2:
            df1[f"target_{col2}"] = df2[col2]
            corr, p_value = stats.pearsonr(df1[col1], df1[f"target_{col2}"])
            if -0.25 < corr < 0.25:
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
            plt.title(f"Regression: {pair[0]} ~> {pair[1]}, corr={pair[2]:.2f}, p-value={pair[3]:.4f}")

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


def predicts_for_incomplete(year_session_candles: List[List[InnerCandle]], days: List[Day]):
    df_session = to_sessions_df(
        [as_1_candle(sess_candles) for sess_candles in year_session_candles if len(sess_candles) > 0], days)
    df_minus15 = to_sessions_df(
        [as_1_candle(sess_candles[:-1]) for sess_candles in year_session_candles if len(sess_candles) > 0], days)
    df_minus30 = to_sessions_df(
        [as_1_candle(sess_candles[:-2]) for sess_candles in year_session_candles if len(sess_candles) > 0], days)
    return predicts(df_session, df_minus15), predicts(df_session, df_minus30)


def show_correlations_for_session(days: List[Day], target_session: SessionName, predict_sessions: List[SessionName],
                                  only_predict_sessions: bool):
    def to_sn(names: List[SessionName]):
        return ' + '.join([str(x).replace('SessionName.', '') for x in names])

    sn = to_sn([target_session])

    def to_candles(day: Day, names: List[SessionName]):
        candles = []
        for name in names:
            candles.extend(day.candles_by_session(name))
        return candles

    if len(predict_sessions) > 0:
        show_correlation_2_sessions(
            f"{to_sn(predict_sessions)} + {sn}[:-1] -> {sn}[-1:]", days,
            [as_1_candle([*to_candles(x, predict_sessions), *x.candles_by_session(target_session)[:-1]]) for x in days],
            [as_1_candle([*x.candles_by_session(target_session)[-1:]]) for x in days]
        )
        if target_session not in [SessionName.NY_OPEN]:
            show_correlation_2_sessions(
                f"{to_sn(predict_sessions)} + {sn}[:-2] -> {sn}[-2:]", days,
                [as_1_candle([*to_candles(x, predict_sessions), *x.candles_by_session(target_session)[:-2]]) for x in
                 days],
                [as_1_candle([*x.candles_by_session(target_session)[-2:]]) for x in days]
            )

        show_correlation_2_sessions(
            f"{to_sn(predict_sessions)} -> {sn}", days,
            [as_1_candle(to_candles(x, predict_sessions)) for x in days],
            [as_1_candle(x.candles_by_session(target_session)) for x in days]
        )
        show_correlation_2_sessions(
            f"{to_sn(predict_sessions)} -> {sn}[:1]", days,
            [as_1_candle(to_candles(x, predict_sessions)) for x in days],
            [as_1_candle([*x.candles_by_session(target_session)][:1]) for x in days]
        )
        if target_session not in [SessionName.NY_OPEN]:
            show_correlation_2_sessions(
                f"{to_sn(predict_sessions)} -> {sn}[:2]", days,
                [as_1_candle(to_candles(x, predict_sessions)) for x in days],
                [as_1_candle([*x.candles_by_session(target_session)][:2]) for x in days]
            )

    if only_predict_sessions:
        return

    show_correlation_2_sessions(
        f"{sn}[:-1] -> {sn}[-1:]", days,
        [as_1_candle([*x.candles_by_session(target_session)[:-1]]) for x in days],
        [as_1_candle([*x.candles_by_session(target_session)[-1:]]) for x in days]
    )
    if target_session not in [SessionName.NY_OPEN]:
        show_correlation_2_sessions(
            f"{sn}[:-2] -> {sn}[-2:]", days,
            [as_1_candle([*x.candles_by_session(target_session)[:-2]]) for x in days],
            [as_1_candle([*x.candles_by_session(target_session)[-2:]]) for x in days]
        )

    if target_session not in [SessionName.CME, SessionName.ASIA]:
        target_session_days = [x for x in days if len(x.candles_by_session(target_session)) > 0]
        show_correlation_2_sessions(
            f"Current day before {sn} -> {sn}", target_session_days,
            [as_1_candle([*x.day_candles_before(x.candles_by_session(target_session)[0][5])]) for x in
             target_session_days],
            [as_1_candle([*x.candles_by_session(target_session)]) for x in days]
        )
        show_correlation_2_sessions(
            f"Current day before {sn} -> {sn}[:1]", target_session_days,
            [as_1_candle([*x.day_candles_before(x.candles_by_session(target_session)[0][5])]) for x in
             target_session_days],
            [as_1_candle([*x.candles_by_session(target_session)][:1]) for x in days]
        )
        if target_session not in [SessionName.NY_OPEN]:
            show_correlation_2_sessions(
                f"Current day before {sn} -> {sn}[:2]", target_session_days,
                [as_1_candle([*x.day_candles_before(x.candles_by_session(target_session)[0][5])]) for x in
                 target_session_days],
                [as_1_candle([*x.candles_by_session(target_session)][:2]) for x in target_session_days]
            )

    show_correlation_2_sessions(
        f"{sn} -> 1D", days,
        [as_1_candle(x.candles_by_session(target_session)) for x in days],
        [x.candle_1d for x in days]
    )


def show_sessions(days: List[Day]):
    if len(days) == 0:
        print('no days!')
        return
    print(f'Start showing charts for {len(days)} days (weekends will probably also be excluded from this number)')
    show_correlations_for_session(days, SessionName.CME, [], False)
    show_correlations_for_session(days, SessionName.ASIA, [SessionName.CME], False)
    show_correlations_for_session(days, SessionName.LONDON, [SessionName.ASIA], False)
    show_correlations_for_session(days, SessionName.EARLY, [SessionName.LONDON], False)
    show_correlations_for_session(days, SessionName.EARLY, [SessionName.ASIA, SessionName.LONDON], True)
    show_correlations_for_session(days, SessionName.EARLY, [SessionName.CME, SessionName.ASIA, SessionName.LONDON],
                                  True)
    show_correlations_for_session(days, SessionName.EARLY, [SessionName.CME], True)
    show_correlations_for_session(days, SessionName.PRE, [SessionName.EARLY], False)
    show_correlations_for_session(days, SessionName.PRE, [SessionName.LONDON, SessionName.EARLY], True)
    show_correlations_for_session(days, SessionName.PRE,
                                  [SessionName.CME, SessionName.ASIA, SessionName.LONDON, SessionName.EARLY], True)
    show_correlations_for_session(days, SessionName.PRE, [SessionName.CME], True)
    show_correlations_for_session(days, SessionName.NY_OPEN, [SessionName.PRE], False)
    show_correlations_for_session(days, SessionName.NY_OPEN, [SessionName.CME], True)
    show_correlations_for_session(days, SessionName.NY_OPEN, [SessionName.ASIA, SessionName.LONDON], True)
    show_correlations_for_session(days, SessionName.NY_OPEN, [SessionName.LONDON], True)
    show_correlations_for_session(days, SessionName.NY_OPEN, [SessionName.EARLY, SessionName.PRE], True)
    show_correlations_for_session(days, SessionName.NY_AM, [SessionName.NY_OPEN], False)
    show_correlations_for_session(days, SessionName.NY_AM, [SessionName.PRE, SessionName.NY_OPEN], True)
    show_correlations_for_session(days, SessionName.NY_LUNCH, [SessionName.NY_AM], False)
    show_correlations_for_session(days, SessionName.NY_LUNCH, [SessionName.NY_OPEN, SessionName.NY_AM], True)
    show_correlations_for_session(days, SessionName.NY_PM, [SessionName.NY_LUNCH], False)
    show_correlations_for_session(days, SessionName.NY_PM, [SessionName.NY_AM], True)
    show_correlations_for_session(days, SessionName.NY_PM, [SessionName.NY_OPEN, SessionName.NY_AM], True)
    show_correlations_for_session(days, SessionName.NY_PM,
                                  [SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH], True)
    show_correlations_for_session(days, SessionName.NY_PM,
                                  [SessionName.PRE, SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH], True)
    show_correlations_for_session(days, SessionName.NY_PM,
                                  [SessionName.EARLY, SessionName.PRE, SessionName.NY_OPEN, SessionName.NY_AM,
                                   SessionName.NY_LUNCH], True)
    show_correlations_for_session(days, SessionName.NY_CLOSE, [SessionName.NY_PM], False)
    show_correlations_for_session(days, SessionName.NY_CLOSE,
                                  [SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM],
                                  True)
    show_correlations_for_session(days, SessionName.NY_CLOSE,
                                  [SessionName.PRE, SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH,
                                   SessionName.NY_PM], True)
    show_correlations_for_session(days, SessionName.NY_CLOSE,
                                  [SessionName.EARLY, SessionName.PRE, SessionName.NY_OPEN, SessionName.NY_AM,
                                   SessionName.NY_LUNCH, SessionName.NY_PM], True)


if __name__ == "__main__":
    try:
        days_from_db: List[Day] = [
            *select_days(2023, "AVAXUSDT"),
            *select_days(2024, "AVAXUSDT"),
            *select_days(2025, "AVAXUSDT"),
        ]
        # show_sessions(days_from_db)
        #
        # mondays = [x for x in days_from_db if to_utc_datetime(x.date_readable).isoweekday() == 1]
        # show_sessions(mondays)

        fridays = [x for x in days_from_db if to_utc_datetime(x.date_readable).isoweekday() == 5]
        show_sessions(fridays)

        # days_with_asia_tothemoon = days_matching_session_df_condition(
        #     days_from_db, SessionName.ASIA,
        #     # lambda df: (df['volat_perc_all'] == '<p90') & (df['body_fraction_perc_all'] == '<p90')
        #     lambda df: (df['perf_perc_all'] == '>=p90') & (df['body_fraction_perc_all'] == '>=p90')
        # )
        # show_sessions(days_with_asia_tothemoon)

        # days_with_open_stb = days_matching_session_df_condition(
        #     days_from_db, SessionName.NY_OPEN,
        #     # lambda df: (df['volat_perc_all'] == '<p90') & (df['body_fraction_perc_all'] == '<p90')
        #     lambda df: (df['lower_wick_fraction_perc_all'] == '>=p90') | (df['lower_wick_fraction_perc_all'] == '<p90')
        # )
        # show_sessions(days_with_open_stb)

        # res_cme15, res_cme30 = predicts_for_incomplete([x.cme_open_candles_15m for x in days_from_db])
        # res_asia15, res_asia30 = predicts_for_incomplete([x.asian_candles_15m for x in days_from_db])
        # res_london15, res_london30 = predicts_for_incomplete([x.london_candles_15m for x in days_from_db])
        # res_early15, res_early30 = predicts_for_incomplete([x.early_session_candles_15m for x in days_from_db])
        # res_pre15, res_pre30 = predicts_for_incomplete([x.premarket_candles_15m for x in days_from_db])
        # res_open15, _ = predicts_for_incomplete([x.ny_am_open_candles_15m for x in days_from_db])
        # res_nyam15, res_nyam30 = predicts_for_incomplete([x.ny_am_candles_15m for x in days_from_db])
        # res_nylunch15, res_nylunch30 = predicts_for_incomplete([x.ny_lunch_candles_15m for x in days_from_db])
        # res_nypm15, res_nypm30 = predicts_for_incomplete([x.ny_pm_candles_15m for x in days_from_db])
        # res_close15, res_close30 = predicts_for_incomplete([x.ny_pm_close_candles_15m for x in days_from_db])

        # # TODO 2 and more sessions
        # show_correlation_2_sessions(
        #     "Asia + London -> PM + Close",
        #     [as_1_candle([*x.asian_candles_15m, *x.london_candles_15m]) for x in days_from_db],
        #     [as_1_candle([*x.ny_pm_candles_15m, *x.ny_pm_close_candles_15m]) for x in days_from_db]
        # )
        # show_correlation_2_sessions(
        #     "Asia + London -> Early + Pre",
        #     [as_1_candle([*x.asian_candles_15m, *x.london_candles_15m]) for x in days_from_db],
        #     [as_1_candle([*x.early_session_candles_15m, *x.premarket_candles_15m]) for x in days_from_db]
        # )

        # # TODO первая и вторая свечка сессии
        print("done!")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
