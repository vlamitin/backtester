import random
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Tuple, Optional

import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas
import pandas as pd
from scipy.signal import find_peaks

from stock_market_research_kit.candle import as_1d_candles, InnerCandle, as_4h_candles
from stock_market_research_kit.db_layer import select_multiyear_candles_15m
from utils.date_utils import random_date, to_date_str, to_utc_datetime


@dataclass
class Trend:
    trend: str  # "uptrend"|"downtrend"
    date_from: str
    length: int  # in candles
    bos_type: Optional[str]  # "high_bos"|"low_bos"
    bos_date: Optional[str]
    bos_ago: Optional[int]  # in candles


def _to_df(candles: List[InnerCandle]) -> pandas.DataFrame:
    df = pd.DataFrame(candles, columns=["Open", "High", "Low", "Close", "Volume", "Date"])
    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)

    return df


def _find_trends(
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
                prev_hh, hl, hh, hl_candidate = None, None, None, None
                res.append(("downtrend", lh[5]))
                res.append(("high_bos", c[5]))
        if prev_ll and lh and ll:
            if c[3] > lh[1]:
                prev_hh, hl, hh = lh, ll, c
                prev_ll, lh, ll, lh_candidate = None, None, None, None
                res.append(("uptrend", hl[5]))
                res.append(("low_bos", c[5]))

        if c[5] in extremums_d:
            high_, low_ = extremums_d[c[5]]
            if high_:
                if len(res) == 0 and (not hh or not ll):
                    hh = high_
                elif hh:
                    if high_[1] > hh[1]:
                        if hh[5] not in extremums_d:
                            hh, hl_candidate = c, None
                        else:
                            prev_hh, hh = hh, high_
                            hl, hl_candidate, lh_candidate = hl_candidate, None, None
                            if len(res) == 0:
                                res.append(("uptrend", lowest_till_first[5]))
                                ll = None
            if low_:
                if len(res) == 0 and (not hh or not ll):
                    ll = low_
                elif ll:
                    if low_[2] < ll[2]:
                        if ll[5] not in extremums_d:
                            ll, lh_candidate = c, None
                        else:
                            prev_ll, ll = ll, low_
                            lh, lh_candidate, hl_candidate = lh_candidate, None, None
                            if len(res) == 0:
                                res.append(("downtrend", highest_till_first[5]))
                                hh = None

        if ll and ll[5] != c[5] and (not lh_candidate or c[1] >= lh_candidate[1]):
            lh_candidate = c

        if hh and hh[5] != c[5] and (not hl_candidate or c[2] <= hl_candidate[2]):
            hl_candidate = c

    return res


def _peaks(candles: List[InnerCandle]) -> Tuple[List[int], List[int]]:  # highs_idxs, lows_idxs
    highs = [x[1] for x in candles]
    lows = [x[2] for x in candles]

    highs_idxs, _ = find_peaks(highs, distance=2, prominence=0.3, plateau_size=1)
    lows_idxs, _ = find_peaks([-x for x in lows], distance=2, prominence=0, plateau_size=1)

    return highs_idxs, lows_idxs


def find_last_trend(candles: List[InnerCandle]) -> Optional[Trend]:
    highs_idxs, lows_idxs = _peaks(candles)
    trends = _find_trends([candles[i] for i in highs_idxs], [candles[i] for i in lows_idxs], candles)

    last_bos_type, last_bos_date = None, None
    for trend, date in reversed(trends):
        if trend in ["high_bos", "low_bos"]:
            last_bos_type, last_bos_date = trend, date
        if trend in ["uptrend", "downtrend"]:
            c_idx = next((i for i in range(len(candles) - 1, -1, -1) if candles[i][5] == date), -1)
            bos_idx = next((i for i in range(len(candles) - 1, -1, -1) if candles[i][5] == date), -1)
            t = Trend(
                trend=trend,
                date_from=date,
                length=len(candles) - c_idx - 1,
                bos_type=None,
                bos_date=None,
                bos_ago=None,
            )
            if last_bos_date:
                bos_idx = next((i for i in range(len(candles) - 1, -1, -1) if candles[i][5] == last_bos_date), -1)
                t.bos_type, t.bos_date, t.bos_ago = last_bos_type, last_bos_date, len(candles) - bos_idx - 1
            return t
    return None


def _show_trends_chart(candles: List[InnerCandle]):
    highs_idxs, lows_idxs = _peaks(candles)
    trends = _find_trends([candles[i] for i in highs_idxs], [candles[i] for i in lows_idxs], candles)
    print("trends1", trends)

    cdf = _to_df(candles)
    peak_h_series = np.full(len(cdf), np.nan)
    for i in highs_idxs:
        peak_h_series[i] = cdf["High"].iloc[i]

    peak_l_series = np.full(len(cdf), np.nan)
    for i in lows_idxs:
        peak_l_series[i] = cdf["Low"].iloc[i]

    trend_up_series = np.full(len(cdf), np.nan)
    trend_down_series = np.full(len(cdf), np.nan)
    trend_low_bos_series = np.full(len(cdf), np.nan)
    trend_high_bos_series = np.full(len(cdf), np.nan)
    bos_location = float(np.nanmin(peak_l_series)) * 0.995

    amplitude = float(np.nanmax(peak_l_series)) - float(np.nanmin(peak_l_series))

    trends_d = {}
    for d in trends:
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

    plots = [
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

    fig, axes = mpf.plot(cdf, type='candle', style='charles', addplot=plots, volume=False, returnfig=True)
    ax = axes[0]
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    fig.autofmt_xdate()

    plt.show()

    # print(peaks)


if __name__ == "__main__":
    try:
        random_end = random_date("2023-05-01 00:00", "2025-09-16 00:00")
        random_start = to_date_str(to_utc_datetime(random_end) - timedelta(days=40))
        random_symbol = random.choice(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        candles_15m_ = select_multiyear_candles_15m(random_symbol, random_start, random_end)
        candles_ = as_1d_candles(candles_15m_)[-15:]
        print(f"showing {random_symbol} from {random_start} to {random_end}")
        print(f"last trend is {find_last_trend(candles_)}")
        _show_trends_chart(candles_)

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
