from typing import List

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
import seaborn as sns

from scripts.run_sessions_typifier import candle_anatomy
from stock_market_research_kit.candle import InnerCandle
from stock_market_research_kit.db_layer import select_days


def charts_example():
    # Нарисуем какую-нибудь функцию, например, сигмоиду:  1/(1+np.exp(-x))
    sigmoid = lambda x: 1 / (1 + np.exp(-x))  # задаём функцию

    f, (ax1, ax2, ax3) = plt.subplots(3, 1)  # создаем изображение с тремя графиками

    f.set_figheight(8)  # устанавливаем высоту figure
    f.set_figwidth(12)  # устанавливаем ширину figure

    x1 = np.linspace(-1, 1)  # задаем сетку значений для х
    y1 = sigmoid(x1)  # вычисляем значения функции y по значениям x

    x2 = np.linspace(-5, 5)
    y2 = sigmoid(x2)

    x3 = np.linspace(-50, 50)
    y3 = sigmoid(x3)

    ax1.plot(x1, y1, '--')  # рисуем график на первой картинке
    ax1.set_title('Sigmoid near zero', )  # устанавливаем заголовок для первой картинки

    ax2.plot(x2, y2, '--')
    ax2.set_title('Sigmoid small scale', )

    ax3.plot(x3, y3, '--')
    ax3.set_title('Sigmoid large scale', )

    f.set_tight_layout(True)  # убираем наложения заголовков на ось картинки выше
    plt.show()


def get_volat_group(value, window_values):
    percentiles = np.percentile(window_values, [10, 30, 70, 90])
    if value < percentiles[0]:
        return '<p10'
    elif value < percentiles[1]:
        return '<p30'
    elif value < percentiles[2]:
        return '<p70'
    elif value < percentiles[3]:
        return '<p90'
    return '>=p90'


def sma_percentile_group(series, window_size):
    return [
        get_volat_group(series.iloc[i],
                        series.iloc[max(0, i - window_size - 1):i + 1]) if i >= window_size - 1 else np.nan
        for i in range(len(series))
    ]


def enrich_with_stat(candles: List[InnerCandle]):
    df = pd.DataFrame(candles, columns=['open', 'high', 'low', 'close', 'volume', 'date'])
    df['volat'] = (df['high'] - df['low']) / df['open'] * 100
    volat_percentiles = np.percentile(df['volat'], [10, 30, 70, 90])
    conditions = [
        df['volat'] < volat_percentiles[0],
        df['volat'] < volat_percentiles[1],
        df['volat'] < volat_percentiles[2],
        df['volat'] < volat_percentiles[3],
    ]
    choices = ['<p10', '<p30', '<p70', '<p90']
    default = '>=p90'
    df['volat_perc_all'] = np.select(conditions, choices, default=default)
    df['volat_perc_sma_20'] = sma_percentile_group(df['volat'], 20)

    return df


def test_groups():
    pass


# step = (end - start) * step_fraction
# points = np.arange(start, end, step)
#
# intervals = list(zip(points, points + step))
#
# def assign_group(x):
#     for i, (low, high) in enumerate(intervals):
#         if i == len(intervals) - 1:
#             if low <= x <= high:
#                 return f"{low:.1f}–{high:.1f}"
#         elif low <= x < high:
#             return f"{low:.1f}–{high:.1f}"
#     return np.nan
#
# s_grouped = df_series.apply(assign_group)
# return s_grouped


def test_corr():
    data1 = {
        'date': pd.date_range(start='2023-01-01', periods=5),
        'open': [100, 102, 104, 106, 108],
        'high': [105, 107, 109, 111, 113],
        'low': [98, 100, 101, 103, 105],
        'close': [104, 101, 108, 107, 110]
    }

    data2 = {
        'date': pd.date_range(start='2023-01-01', periods=5),
        'open': [200, 202, 204, 206, 208],
        'high': [210, 212, 214, 216, 218],
        'low': [195, 198, 199, 201, 203],
        'close': [205, 200, 213, 208, 215]
    }

    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)

    # Вычисления
    df1['range'] = df1['high'] - df1['low']
    df1['body'] = (df1['open'] - df1['close']).abs()

    df2['range'] = df2['high'] - df2['low']
    df2['body'] = (df2['open'] - df2['close']).abs()
    df2['target'] = df2['range'] / df2['body'].replace(0, np.nan)

    # Объединение
    merged = df1[['date', 'range', 'body']].merge(df2[['date', 'target']], on='date')
    merged.columns = ['date', 'range_df1', 'body_df1', 'target_df2']

    # # Визуализация pairplot
    # sns.pairplot(merged[['range_df1', 'body_df1', 'target_df2']])
    # plt.suptitle("Pairplot — Взаимосвязи между признаками", y=1.02)
    # plt.show()

    # Визуализация heatmap
    # corr = merged[['range_df1', 'body_df1', 'target_df2']].corr()
    # plt.figure(figsize=(6, 4))
    # sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.2f')
    # plt.title("Heatmap — Корреляции признаков")
    # plt.show()

        # Построение регрессионных графиков
    plt.figure(figsize=(12, 5))

    # 1. Зависимость target_df2 от range_df1
    # plt.subplot(1, 2, 1)
    sns.regplot(x='range_df1', y='target_df2', data=merged)
    plt.title('Регрессия: target_df2 ~ range_df1')

    # 2. Зависимость target_df2 от body_df1
    # plt.subplot(1, 2, 2)
    # sns.regplot(x='body_df1', y='target_df2', data=merged)
    # plt.title('Регрессия: target_df2 ~ body_df1')

    plt.tight_layout()
    plt.show()


# def linear_regression(df1: pd.DataFrame, df2: pd.DataFrame):


if __name__ == "__main__":
    try:
        test_corr()
        # charts_example()
        # test_groups()
        #
        # crv_days = select_days(2024, "CRVUSDT")
        # london_candles = [x.london_as_candle for x in crv_days if x.london_as_candle[5] != ""]
        # enrich_with_stat(london_candles)
        #
        # london_candles_anatomy = [list(candle_anatomy(x.london_as_candle)) for x in crv_days if
        #                           x.london_as_candle[5] != ""]
        # np_candles = np.array(london_candles_anatomy, dtype=np.float64)
        #
        # # normalized data used mean as base, while percentile-based calculation uses median data, which is better
        # normalized_volat = (np_candles[:, 1] - np.mean(np_candles[:, 1])) / np.std(np_candles[:, 1])
        # print(np.arange(24).reshape(2, -1, 3))
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
