from typing import List

import numpy as np
import pandas as pd

from stock_market_research_kit.candle_with_stat import perc_all_and_sma20, show_corr_charts
from stock_market_research_kit.smt_psp_trade import SmtPspTrade, smt_psp_trades_from_json
from utils.date_utils import to_utc_datetime, quarters_by_time

strategy01_2024_snapshot = "scripts/test_snapshots/strategy_1_2024_btc_eth_sol.json"
strategy03_2024_snapshot = "scripts/test_snapshots/strategy_3_2024_btc_eth_sol.json"
strategy07_2024_snapshot = "scripts/test_snapshots/strategy_7_2024_btc_eth_sol.json"
strategy08_2024_snapshot = "scripts/test_snapshots/strategy_8_2024_btc_eth_sol.json"
strategy03_2025_snapshot = "scripts/test_snapshots/strategy_3_2025_btc_eth_sol.json"


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


def to_trade_df(trades: List[SmtPspTrade]):
    df = pd.DataFrame(trades, columns=[
        'entry_time', 'asset', 'pnl_usd', 'direction', 'entry_rr', 'psp_key_used',
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
    df['entry_yq'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[0].name)
    df['entry_mw'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[1].name)
    df['entry_wd'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[2].name)
    df['entry_dq'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[3].name)
    df['entry_q90m'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[4].name)
    df['entry_rr_perc'], _ = perc_all_and_sma20(df['entry_rr'])
    df['minutes_in_market_perc'], _ = perc_all_and_sma20(df['minutes_in_market'])
    df['entry_position_usd_perc'], _ = perc_all_and_sma20(df['entry_position_usd'])
    df['pnl_minus_fees'] = df['pnl_usd'] - df['entry_position_fee'] - df['close_position_fee']

    df = with_3d_window_stagnation(df)

    first_cols = [
        'entry_time', 'asset', 'pnl_usd', 'cum_pnl_usd', 'pnl_minus_fees',
        '3d_pnl_cumsum', '3d_window_start_date', '3d_pnl_cumsum_last_in_window',
        'stagnation_trades_count', 'stagnation_days'
    ]
    cols = first_cols + [c for c in df.columns if c not in first_cols]
    df = df[cols]

    return df


if __name__ == "__main__":
    try:
        with open(strategy08_2024_snapshot, "r", encoding="utf-8") as f:
            json_str = f.read()
            trades_list = smt_psp_trades_from_json(json_str)
            trades_df = to_trade_df(trades_list)

            print("cum pnl --")
            print(trades_df['cum_pnl_usd'].iloc[-1])
            print("\n")

            print("direction mean median count --")
            print(trades_df.groupby("direction")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            print("asset mean median count --")
            print(trades_df.groupby("asset")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            # print("direction mean median count pnl_minus_fees --")
            # print(trades_df.groupby("direction")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            # print("\n")

            print("entry_position_usd_perc --")
            print([float(x) for x in np.percentile(trades_df['entry_position_usd'], [10, 30, 50, 70, 90])])
            print("\n")

            print("entry_position_usd_perc mean median count --")
            print(trades_df.groupby("entry_position_usd_perc")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            #
            # print("entry_position_usd_perc mean median count pnl_minus_fees --")
            # print(trades_df.groupby("entry_position_usd_perc")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            # print("\n")

            print("entry_rr_perc --")
            print([float(x) for x in np.percentile(trades_df['entry_rr'], [10, 30, 50, 70, 90])])
            print("\n")

            print("entry_rr_perc mean median count --")
            print(trades_df.groupby("entry_rr_perc")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            #             print("entry_rr_perc mean median count pnl_minus_fees --")
            #             print(trades_df.groupby("entry_rr_perc")["pnl_minus_fees"].agg(["mean", "median", "count"]))
            #             print("\n")

            print("minutes_in_market_perc --")
            print([float(x) for x in np.percentile(trades_df['minutes_in_market'], [10, 30, 50, 70, 90])])
            print("\n")

            print("minutes_in_market_perc mean median count --")
            print(trades_df.groupby("minutes_in_market_perc")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            print("psp_key_used mean median count --")
            print(trades_df.groupby("psp_key_used")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

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

        print("done!")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
