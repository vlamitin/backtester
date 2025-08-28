from typing import List, Tuple

import numpy as np
import pandas as pd

from stock_market_research_kit.candle_with_stat import perc_all_and_sma20, show_corr_charts
from stock_market_research_kit.smt_psp_trade import SmtPspTrade, smt_psp_trades_from_json
from utils.date_utils import to_utc_datetime, quarters_by_time

strategy01_2024_snapshot = "scripts/test_snapshots/strategy_01_2024_btc_eth_sol.json"
strategy03_2024_snapshot = "scripts/test_snapshots/strategy_03_2024_btc_eth_sol.json"
strategy07_2024_snapshot = "scripts/test_snapshots/strategy_07_2024_btc_eth_sol.json"
strategy08_2024_snapshot = "scripts/test_snapshots/strategy_08_2024_btc_eth_sol.json"
strategy09_2024_snapshot = "scripts/test_snapshots/strategy_09_2024_btc_eth_sol.json"
strategy10_2024_snapshot = "scripts/test_snapshots/strategy_10_2024_btc_eth_sol.json"
strategy11_2024_snapshot = "scripts/test_snapshots/strategy_11_2024_btc_eth_sol.json"
strategy12_2024_snapshot = "scripts/test_snapshots/strategy_12_2024_btc_eth_sol.json"
strategy13_2024_snapshot = "scripts/test_snapshots/strategy_13_2024_btc_eth_sol.json"
strategy14_2024_snapshot = "scripts/test_snapshots/strategy_14_2024_btc_eth_sol.json"
strategy15_2024_snapshot = "scripts/test_snapshots/strategy_15_2024_btc_eth_sol.json"
strategy16_2024_snapshot = "scripts/test_snapshots/strategy_16_2024_btc_eth_sol.json"

strategy03_2025_snapshot = "scripts/test_snapshots/strategy_03_2025_btc_eth_sol.json"


def with_3d_window_stagnation(df: pd.DataFrame) -> pd.DataFrame:
    # === шаг 1. считаем 3-дневные окна ===
    df["entry_date"] = pd.to_datetime(df["entry_time"], errors="coerce")
    min_date = df["entry_date"].min().normalize()
    cum_3d_window_num = ((df["entry_date"] - min_date).dt.days // 3)
    df["3d_window_start_date"] = min_date + pd.to_timedelta(cum_3d_window_num * 3, unit="D")
    df["3d_pnl_cumsum"] = df.groupby("3d_window_start_date")["pnl_usd"].cumsum()

    # === шаг 2. берём последний элемент каждого окна ===
    last_in_window = df.groupby("3d_window_start_date").tail(1).copy()

    # === шаг 3. убираем отрицательные ===
    positive_last = last_in_window[last_in_window["3d_pnl_cumsum"] >= 0]

    # === шаг 4. считаем перцентили ===
    percentiles = positive_last["3d_pnl_cumsum"].quantile([0.1, 0.3, 0.5, 0.7, 0.9])
    p10 = percentiles.loc[0.1]

    # === шаг 5. метки окон ===
    last_in_window.loc[:, "status"] = last_in_window["3d_pnl_cumsum"].apply(
        lambda x: "start_stagnation" if x < p10 else "reset_stagnation"
    )
    status_by_window = last_in_window.set_index("3d_window_start_date")["status"].to_dict()

    # === шаг 6. считаем stagnation_trades_count и stagnation_days ===
    stagnation_trades_count = []
    stagnation_days = []

    trades_counter = 0
    stagnation_start_date = None

    for idx, row in df.iterrows():
        win = row["3d_window_start_date"]
        status = status_by_window.get(win, None)

        if status == "reset_stagnation":
            trades_counter = 0
            stagnation_start_date = None
            stagnation_trades_count.append(0)
            stagnation_days.append(0)

        elif status == "start_stagnation":
            if trades_counter == 0:
                trades_counter = 1
                stagnation_start_date = row["entry_date"].normalize()
            else:
                trades_counter += 1
            stagnation_trades_count.append(trades_counter)
            days = (row["entry_date"].normalize() - stagnation_start_date).days
            stagnation_days.append(days)

        elif trades_counter > 0:  # продолжаем застой
            trades_counter += 1
            stagnation_trades_count.append(trades_counter)
            days = (row["entry_date"].normalize() - stagnation_start_date).days
            stagnation_days.append(days)

        else:  # вне застоя
            stagnation_trades_count.append(0)
            stagnation_days.append(0)

    df["stagnation_trades_count"] = stagnation_trades_count
    df["stagnation_days"] = stagnation_days
    df["3d_pnl_cumsum_last_in_window"] = (
        df.groupby("3d_window_start_date")["3d_pnl_cumsum"].transform("last")
    )

    return df


def item_shifted(smb: str, arr1: List[Tuple[str, float, float]], arr2: List[Tuple[str, float, float]]) -> bool:
    common = {(x[0], x[1]) for x in arr1} & {(x[0], x[1]) for x in arr2}

    filtered1 = [x[0] for x in arr1 if (x[0], x[1]) in common or x[0] == smb]
    filtered2 = [x[0] for x in arr2 if (x[0], x[1]) in common or x[0] == smb]

    idx1 = filtered1.index(smb)
    idx2 = filtered2.index(smb)

    neighbors1 = (filtered1[idx1 - 1] if idx1 > 0 else None,
                  filtered1[idx1 + 1] if idx1 < len(filtered1) - 1 else None)
    neighbors2 = (filtered2[idx2 - 1] if idx2 > 0 else None,
                  filtered2[idx2 + 1] if idx2 < len(filtered2) - 1 else None)

    return neighbors1 != neighbors2


def to_trade_df(trades: List[SmtPspTrade]):
    df = pd.DataFrame(trades, columns=[
        'entry_time', 'asset', 'pnl_usd', 'direction', 'entry_rr', 'psp_key_used',
        'smt_type', 'smt_label', 'smt_flags',
        'best_entry_time', 'best_entry_time_ny', 'best_entry_price', 'best_entry_rr',
        'entry_position_usd',
        'entry_position_fee',
        'close_position_fee'
    ])
    df["cum_pnl_usd"] = df["pnl_usd"].cumsum()
    df["close_time"] = df.index.map(lambda i: trades[i].closes[0][2])
    df['minutes_in_market'] = df.apply(
        lambda row: (to_utc_datetime(row['close_time']) - to_utc_datetime(row['entry_time'])).total_seconds() / 60,
        axis=1
    )
    df['best_entry_minutes_in_market'] = df.apply(
        lambda row: (to_utc_datetime(row['close_time']) - to_utc_datetime(row['best_entry_time'])).total_seconds() / 60,
        axis=1
    )
    df['entry_yq'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[0].name)
    df['entry_mw'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[1].name)
    df['entry_wd'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[2].name)
    df['entry_dq'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[3].name)
    df['entry_q90m'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[4].name)
    df['entry_rr_perc'], _ = perc_all_and_sma20(df['entry_rr'])
    df['minutes_in_market_perc'], _ = perc_all_and_sma20(df['minutes_in_market'])
    df['entry_position_usd_perc'], _ = perc_all_and_sma20(df['entry_position_usd'])
    df['pnl_minus_fees'] = df['pnl_usd'] - df['entry_position_fee'] - df['close_position_fee']
    df['entry_tos'] = df.index.map(lambda i: '-'.join([x[0] for x in trades[i].entry_tos]))
    df['best_entry_tos'] = df.index.map(lambda i: '-'.join([x[0] for x in trades[i].best_entry_tos]))

    # # чем больше TO, которые с нужной от нас стороны, и чем они ближе к нашей цене, тем лучше
    # entry_tos_aside_coefs1 = []
    # # best_entry_tos_aside_coefs1 = []
    # # чем больше TO, которые с нужной от нас стороны, и чем они дальше от нашей цены, тем лучше
    # entry_tos_aside_coefs2 = []
    # # best_entry_tos_aside_coefs2 = []
    # for trade in trades:
    #     asset_i = next(i for i, x in enumerate(trade.best_entry_tos) if x[0] == trade.asset)
    #     coef1 = 0
    #     coef2 = 0
    #     aside_tos = trade.best_entry_tos[asset_i + 1:] if trade.direction == "UP" else trade.best_entry_tos[:asset_i]
    #     for to in aside_tos:
    #         # if abs(to[2]) < 0.1:
    #         #     coef1 += 1.5
    #         #     coef2 += 0.2
    #         #     continue
    #         # if abs(to[2]) < 0.2:
    #         #     coef1 += 1
    #         #     coef2 += 0.5
    #         #     continue
    #         # if abs(to[2]) < 0.3:
    #         #     coef1 += 0.5
    #         #     coef2 += 1
    #         #     continue
    #         coef1 += 0.2
    #         coef2 += 1.5
    #     entry_tos_aside_coefs1.append(coef1)
    #     entry_tos_aside_coefs2.append(coef2)
    # df["entry_tos_aside_coefs1"] = df.index.map(lambda i: entry_tos_aside_coefs1[i])
    # df['entry_tos_aside_coefs1_perc'], _ = perc_all_and_sma20(df['entry_tos_aside_coefs1'])
    # df["entry_tos_aside_coefs2"] = df.index.map(lambda i: entry_tos_aside_coefs2[i])
    # df['entry_tos_aside_coefs2_perc'], _ = perc_all_and_sma20(df['entry_tos_aside_coefs2'])


    # with_shift_in_tos = []
    # prev_tos_in_shifted = []
    # for trade in trades:
    #     if trade.pnl_usd < 0:
    #         continue
    #     if trade.best_entry_rr == trade.entry_rr:
    #         continue
    #     if item_shifted(trade.asset, trade.entry_tos, trade.best_entry_tos):
    #         with_shift_in_tos.append(trade)
    #         asset_i = next(i for i, x in enumerate(trade.best_entry_tos) if x[0] == trade.asset)
    #         if trade.direction == "UP":
    #             prev_tos_in_shifted.append(trade.best_entry_tos[asset_i - 1])
    #         else:
    #             prev_tos_in_shifted.append(trade.best_entry_tos[asset_i + 1])
    #
    # pref_tos_df = pd.DataFrame(prev_tos_in_shifted, columns=["to_label", "price", "percent"])
    # pref_tos_df["abs_percent"] = pref_tos_df["percent"].abs()
    # pref_tos_grouped_mmc = pref_tos_df.groupby("to_label")["abs_percent"].agg(["count", "mean", "median"])
    # pref_tos_percents = np.array([abs(x[2]) for x in prev_tos_in_shifted])
    # pref_tos_mean, pref_tos_median = np.mean(pref_tos_percents), np.median(pref_tos_percents)

    df = with_3d_window_stagnation(df)

    first_cols = [
        'entry_time', 'asset', 'pnl_usd', 'cum_pnl_usd', 'pnl_minus_fees',
        '3d_pnl_cumsum', '3d_window_start_date', '3d_pnl_cumsum_last_in_window',
        'stagnation_trades_count', 'stagnation_days'
    ]
    cols = first_cols + [c for c in df.columns if c not in first_cols]
    df = df[cols]

    df1 = df[[
        'asset', 'pnl_usd', 'direction', 'entry_rr', 'psp_key_used', 'smt_type', 'smt_label', 'smt_flags',
        'entry_yq',
        'entry_mw',
        'entry_wd',
        'entry_dq',
        'entry_q90m',
        'entry_rr_perc',
        'entry_tos',
    ]]

    return df


if __name__ == "__main__":
    try:
        with open(strategy11_2024_snapshot, "r", encoding="utf-8") as f:
            json_str = f.read()
            trades_list = smt_psp_trades_from_json(json_str)
            trades_df = to_trade_df(trades_list)

            # show_corr_charts("bla", trades_df, trades_df, [
            #     "entry_tos_aside_coefs1",
            #     "entry_tos_aside_coefs2",
            # ], ["pnl_usd"])

            print("cum pnl --")
            print(trades_df['cum_pnl_usd'].iloc[-1])
            print("\n")

            print("direction mean median count --")
            print(trades_df.groupby("direction")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            print("asset mean median count --")
            print(trades_df.groupby("asset")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            print("smt_type mean median count --")
            print(trades_df.groupby("smt_type")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            print("smt_label mean median count --")
            print(trades_df.groupby("smt_label")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            print("smt_flags mean median count --")
            print(trades_df.groupby("smt_flags")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            # print("direction mean median count pnl_minus_fees --")
            # print(trades_df.groupby("direction")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            # print("\n")

            print("entry_position_usd_perc --")
            print([float(x) for x in np.percentile(trades_df['entry_position_usd'], [10, 30, 70, 90])])
            print("\n")

            print("entry_position_usd_perc mean median count --")
            print(trades_df.groupby("entry_position_usd_perc")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")
            #
            # print("entry_tos_aside_coefs1_perc --")
            # print([float(x) for x in np.percentile(trades_df['entry_tos_aside_coefs1'], [10, 30, 70, 90])])
            # print("\n")
            #
            # print("entry_tos_aside_coefs1_perc mean median count --")
            # print(trades_df.groupby("entry_tos_aside_coefs1_perc")["pnl_usd"].agg(["mean", "median", "count"]))
            # print("\n")
            #
            # print("entry_tos_aside_coefs2_perc --")
            # print([float(x) for x in np.percentile(trades_df['entry_tos_aside_coefs2'], [10, 30, 70, 90])])
            # print("\n")
            #
            # print("entry_tos_aside_coefs2_perc mean median count --")
            # print(trades_df.groupby("entry_tos_aside_coefs2_perc")["pnl_usd"].agg(["mean", "median", "count"]))
            # print("\n")

            #
            # print("entry_position_usd_perc mean median count pnl_minus_fees --")
            # print(trades_df.groupby("entry_position_usd_perc")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            # print("\n")

            print("entry_rr_perc --")
            print([float(x) for x in np.percentile(trades_df['entry_rr'], [10, 30, 70, 90])])
            print("\n")

            print("entry_rr_perc mean median count --")
            print(trades_df.groupby("entry_rr_perc")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            print("entry_wd + entry_rr_perc mean median count --")
            print(
                trades_df.groupby(["entry_wd", "entry_rr_perc"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            #             print("entry_rr_perc mean median count pnl_minus_fees --")
            #             print(trades_df.groupby("entry_rr_perc")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            #             print("\n")

            print("minutes_in_market_perc --")
            print([float(x) for x in np.percentile(trades_df['minutes_in_market'], [10, 30, 70, 90])])
            print("\n")

            print("minutes_in_market_perc mean median count --")
            print(trades_df.groupby("minutes_in_market_perc")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            print("entry_wd + minutes_in_market_perc mean median count --")
            print(
                trades_df.groupby(["entry_wd", "minutes_in_market_perc"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("entry_rr_perc + minutes_in_market_perc mean median count --")
            print(
                trades_df.groupby(["entry_rr_perc", "minutes_in_market_perc"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("entry_rr_perc < 90 and >=90 + minutes_in_market_perc + entry_wd mean median count --")
            print(
                trades_df[trades_df["entry_rr_perc"].isin(['<p90', '>=p90'])]
                .groupby(["entry_rr_perc", "minutes_in_market_perc", "entry_wd"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("psp_key_used mean median count --")
            print(trades_df.groupby("psp_key_used")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")
            #
            # print("entry_tos mean median count --")
            # print(trades_df.groupby("entry_tos")["pnl_usd"].agg(["mean", "median", "count"]))
            # print("\n")
            #
            # print("best_entry_tos mean median count --")
            # print(trades_df.groupby("best_entry_tos")["pnl_usd"].agg(["mean", "median", "count"]))
            # print("\n")

            #             print("psp_key_used mean median count pnl_minus_fees --")
            #             print(trades_df.groupby("psp_key_used")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            #             print("\n")

            print("entry_yq mean median count --")
            print(trades_df.groupby("entry_yq")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            #             print("entry_yq mean median count pnl_minus_fees --")
            #             print(trades_df.groupby("entry_yq")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            #             print("\n")

            print("entry_mw mean median count --")
            print(trades_df.groupby("entry_mw")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            #             print("entry_mw mean median count pnl_minus_fees --")
            #             print(trades_df.groupby("entry_mw")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            #             print("\n")

            print("entry_wd mean median count --")
            print(trades_df.groupby("entry_wd")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            #             print("entry_wd mean median count pnl_minus_fees --")
            #             print(trades_df.groupby("entry_wd")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            #             print("\n")

            print("entry_dq mean median count --")
            print(trades_df.groupby("entry_dq")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            #             print("entry_dq mean median count pnl_minus_fees --")
            #             print(trades_df.groupby("entry_dq")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            #             print("\n")

            print("entry_q90m mean median count --")
            print(trades_df.groupby("entry_q90m")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            #             print("entry_q90m mean median count pnl_minus_fees --")
            #             print(trades_df.groupby("entry_q90m")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            #             print("\n")

            print("psp_key_used + direction mean median count --")
            print(
                trades_df.groupby(["psp_key_used", "direction"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("psp_key_used + direction mean median count --")
            print(
                trades_df.groupby(["smt_type", "smt_label", "asset", "smt_flags"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("psp_key_used + direction + asset mean median count --")
            print(
                trades_df.groupby(["psp_key_used", "direction", "asset"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            #             print("psp_key_used + direction mean median count pnl_minus_fees --")
            #             print(
            #                 trades_df.groupby(["psp_key_used", "direction"])["pnl_minus_fees"]
            #                 .agg(["mean", "median", "count"])
            #                 .reset_index()
            #             )
            #             print("\n")

            print("psp_key_used + direction + entry_yq mean median count --")
            print(
                trades_df.groupby(["psp_key_used", "direction", "entry_yq"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("psp_key_used + direction + entry_mw mean median count --")
            print(
                trades_df.groupby(["psp_key_used", "direction", "entry_mw"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("psp_key_used + direction + entry_wd mean median count --")
            print(
                trades_df.groupby(["psp_key_used", "direction", "entry_wd"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("psp_key_used + direction + entry_dq mean median count --")
            print(
                trades_df.groupby(["psp_key_used", "direction", "entry_dq"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("psp_key_used + direction + entry_q90m mean median count --")
            print(
                trades_df.groupby(["psp_key_used", "direction", "entry_q90m"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("entry_wd + entry_dq mean median count --")
            print(
                trades_df.groupby(["entry_wd", "entry_dq"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

            print("entry_wd + smt_label mean median count --")
            print(
                trades_df.groupby(["entry_wd", "smt_label"])["pnl_usd"]
                .agg(["mean", "median", "count"])
                .reset_index()
            )
            print("\n")

        print("done!")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
