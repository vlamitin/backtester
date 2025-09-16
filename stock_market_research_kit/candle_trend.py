from typing import List, Tuple

import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas
import pandas as pd
from scipy.signal import find_peaks

from stock_market_research_kit.candle import as_1d_candles, InnerCandle, as_4h_candles
from stock_market_research_kit.db_layer import select_multiyear_candles_15m


def to_df(candles: List[InnerCandle]) -> pandas.DataFrame:
    df = pd.DataFrame(candles, columns=["Open", "High", "Low", "Close", "Volume", "Date"])
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)

    return df


def calc_trends(highs_: List[Tuple[float, str]], lows_: List[Tuple[float, str]]) -> List[Tuple[str, str]]:
    res: List[Tuple[str, str]] = []

    # Берём минимум по длине, чтобы шаги совпадали
    n = min(len(highs_), len(lows_))

    # начинаем с индекса 1, чтобы сравнивать i-1 и i
    for i_ in range(1, n):
        prev_high, _ = highs_[i_ - 1]
        curr_high, date_h = highs_[i_]

        prev_low, _ = lows_[i_ - 1]
        curr_low, date_l = lows_[i_]

        # выбираем более "свежую" дату (обычно у high и low они разные)
        date = max(date_h, date_l)

        if curr_high > prev_high and curr_low > prev_low:
            trend = "uptrend"
        elif curr_high < prev_high and curr_low < prev_low:
            trend = "downtrend"
        else:
            trend = "range"

        res.append((trend, date))

    return res


def find_trends(
        highs_candles: List[InnerCandle], lows_candles: List[InnerCandle],
        all_candles: List[InnerCandle]
) -> List[Tuple[str, str]]:
    extremums_d = {}
    last_lc = 0
    for hc in highs_candles:
        while True:
            if len(lows_candles) == last_lc:
                break
            if hc[5] == lows_candles[last_lc][5]:
                extremums_d[lows_candles[last_lc][5]] = (hc, lows_candles[last_lc])
            elif hc[5] > lows_candles[last_lc][5]:
                extremums_d[lows_candles[last_lc][5]] = (None, lows_candles[last_lc])
            else:
                break
            last_lc += 1
        extremums_d[hc[5]] = (hc, None)

    if len(lows_candles) > last_lc:
        for lc in lows_candles[last_lc:]:
            extremums_d[lc[5]] = (None, lc)

    prev_hh, hl, hh = None, None, None
    prev_ll, lh, ll = None, None, None
    hl_candidate, lh_candidate = None, None
    highest_till_first, lowest_till_first = None, None
    res = []

    for c in all_candles:
        if len(res) == 0:
            if not highest_till_first or c[1] >= highest_till_first[1]:
                highest_till_first = c
            if not lowest_till_first or c[2] <= lowest_till_first[2]:
                lowest_till_first = c
        if prev_hh and hl and hh:
            if c[3] < hl[2]:
                prev_ll, lh, ll = hl, hh, c
                prev_hh, hl, hh = None, None, None
                res.append(("downtrend", lh[5]))
                res.append(("high_bos", c[5]))
        if prev_ll and lh and ll:
            if c[3] > lh[1]:
                prev_hh, hl, hh = lh, ll, c
                prev_ll, lh, ll = None, None, None
                res.append(("uptrend", hl[5]))
                res.append(("low_bos", c[5]))

        if c[5] in extremums_d:
            high_, low_ = extremums_d[c[5]]
            if high_:
                if len(res) == 0 and (not hh or not ll):
                    hh = high_
                elif hh:
                    if high_[1] > hh[1]:
                        prev_hh, hh = hh, high_
                        hl, hl_candidate = hl_candidate, None
                        if len(res) == 0:
                            res.append(("uptrend", lowest_till_first[5]))
                            ll, prev_ll = None, None
            if low_:
                if len(res) == 0 and (not hh or not ll):
                    ll = low_
                    if hh:
                        hl_candidate = low_
                elif ll:
                    if low_[2] < ll[2]:
                        prev_ll, ll = ll, low_
                        lh, lh_candidate = lh_candidate, None
                        if len(res) == 0:
                            res.append(("downtrend", highest_till_first[5]))
                            hh, prev_hh = None, None

        if ll and ll[5] != c[5] and (not lh_candidate or c[1] >= lh_candidate[1]):
            lh_candidate = c

        if hh and hh[5] != c[5] and (not hl_candidate or c[2] <= hl_candidate[2]):
            hl_candidate = c

    return res


if __name__ == "__main__":
    try:
        candles_15m = select_multiyear_candles_15m("BTCUSDT", "2024-01-01 00:00", "2025-09-15 00:00")
        candles_ = as_4h_candles(list(candles_15m))[-60:]
        highs = [x[1] for x in candles_]
        lows = [x[2] for x in candles_]

        cdf = to_df(candles_)

        highs_idxs, _ = find_peaks(highs, distance=2, prominence=0.3)
        lows_idxs, _ = find_peaks([-x for x in lows], distance=2, prominence=0, plateau_size=1)

        peak_h_series = np.full(len(cdf), np.nan)
        for i in highs_idxs:
            peak_h_series[i] = cdf["High"].iloc[i]

        peak_h_dates = [cdf.index[i] for i in highs_idxs]
        peak_l_series = np.full(len(cdf), np.nan)
        for i in lows_idxs:
            peak_l_series[i] = cdf["Low"].iloc[i]
        peak_l_dates = [cdf.index[i] for i in lows_idxs]

        trends1 = find_trends([candles_[i] for i in highs_idxs], [candles_[i] for i in lows_idxs], candles_)
        print("trends1", trends1)

        # trends = calc_trends(
        #     [(candles_1d[i][1], candles_1d[i][5]) for i in highs_idxs],
        #     [(candles_1d[i][2], candles_1d[i][5]) for i in lows_idxs],
        # )
        trend_up_series = np.full(len(cdf), np.nan)
        trend_down_series = np.full(len(cdf), np.nan)
        trend_low_bos_series = np.full(len(cdf), np.nan)
        trend_high_bos_series = np.full(len(cdf), np.nan)
        bos_location = float(np.nanmin(peak_l_series)) * 0.995

        amplitude = float(np.nanmax(peak_l_series)) - float(np.nanmin(peak_l_series))

        trends_d = {}
        for d in trends1:
            if d[1] in trends_d:
                trends_d[d[1]].append(d[0])
            else:
                trends_d[d[1]] = [d[0]]

        for i, d in enumerate(cdf.index):
            if str(d)[:-3] in trends_d:
                trends = trends_d[str(d)[:-3]]
                for trend in trends:
                    if trend == "uptrend":
                        trend_up_series[i] = cdf["Low"].iloc[i] - amplitude * 0.05
                    elif trend == "downtrend":
                        trend_down_series[i] = cdf["High"].iloc[i] + amplitude * 0.05
                    elif trend == "low_bos":
                        trend_low_bos_series[i] = bos_location
                    elif trend == "high_bos":
                        trend_high_bos_series[i] = bos_location

        apds = [
            mpf.make_addplot(peak_h_series, type='scatter', markersize=35, marker='.',
                             color='g', panel=0, scatter=True, secondary_y=False),
            mpf.make_addplot(peak_l_series, type='scatter', markersize=35, marker='.',
                             color='r', panel=0, scatter=True, secondary_y=False),
            mpf.make_addplot(trend_up_series, type='scatter', markersize=80, marker='^',
                             color='g', panel=0, scatter=True, secondary_y=False),
            mpf.make_addplot(trend_down_series, type='scatter', markersize=80, marker='v',
                             color='r', panel=0, scatter=True, secondary_y=False),
            mpf.make_addplot(trend_low_bos_series, type='scatter', markersize=110, marker='^',
                             color='blue', panel=0, scatter=True, secondary_y=False),
            mpf.make_addplot(trend_high_bos_series, type='scatter', markersize=110, marker='v',
                             color='purple', panel=0, scatter=True, secondary_y=False),
        ]

        fig, axes = mpf.plot(cdf, type='candle', style='charles', addplot=apds, volume=False, returnfig=True)
        ax = axes[0]
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
        fig.autofmt_xdate()

        plt.show()

        # print(peaks)

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
