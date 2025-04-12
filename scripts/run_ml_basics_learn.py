from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

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
    data = [10, 12, 13, 14, 15, 18, 20, 21, 22, 25, 25, 30, 35]

    # Создаём DataFrame
    df = pd.DataFrame(data, columns=['value'])

    # Вычисляем среднее значение и отклонение (5% от среднего)
    mean_value = df['value'].mean()
    percent_range = mean_value * 0.05  # 5% отклонение

    # Используем pd.cut() для группировки значений в диапазоны с шагом 5% от среднего
    bins = pd.cut(df['value'], bins=pd.interval_range(start=df['value'].min(), end=df['value'].max(), freq=percent_range))

    # Добавляем результат группировки в DataFrame
    df['group'] = bins

    # Считаем количество значений в каждой группе
    vc = df['group'].value_counts().sort_index()

    # Печатаем результат
    print(df)
    print("\nГруппировка по диапазонам с отклонением ±5%:")
    print(vc)


if __name__ == "__main__":
    try:
        # charts_example()
        test_groups()

        crv_days = select_days(2024, "CRVUSDT")
        london_candles = [x.london_as_candle for x in crv_days if x.london_as_candle[5] != ""]
        enrich_with_stat(london_candles)

        london_candles_anatomy = [list(candle_anatomy(x.london_as_candle)) for x in crv_days if
                                  x.london_as_candle[5] != ""]
        np_candles = np.array(london_candles_anatomy, dtype=np.float64)

        # normalized data used mean as base, while percentile-based calculation uses median data, which is better
        normalized_volat = (np_candles[:, 1] - np.mean(np_candles[:, 1])) / np.std(np_candles[:, 1])
        print(np.arange(24).reshape(2, -1, 3))
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
