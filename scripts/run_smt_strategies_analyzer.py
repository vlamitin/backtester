from typing import List

import numpy as np
import pandas as pd

from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

from stock_market_research_kit.candle_with_stat import perc_all_and_sma20, show_corr_charts
from stock_market_research_kit.smt_psp_trade import SmtPspTrade, smt_psp_trades_from_json
from utils.date_utils import to_utc_datetime, quarters_by_time

strategy03_2024_snapshot = "scripts/test_snapshots/strategy_3_2024_btc_eth_sol.json"
strategy03_2025_snapshot = "scripts/test_snapshots/strategy_3_2025_btc_eth_sol.json"


def to_trade_df(trades: List[SmtPspTrade]):
    df = pd.DataFrame(trades, columns=[
        'asset', 'entry_time', 'entry_rr', 'psp_key_used', 'direction', 'pnl_usd', 'entry_position_usd',
        'entry_position_fee',
        'close_position_fee'
    ])
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

    return df


if __name__ == "__main__":
    try:
        with open(strategy03_2024_snapshot, "r", encoding="utf-8") as f:
            json_str = f.read()
            trades_list = smt_psp_trades_from_json(json_str)
            trades_df = to_trade_df(trades_list)

            corr = trades_df.corr(numeric_only=True)['pnl_usd'].sort_values(ascending=False)
            print("corr --")
            print(corr)
            print("\n")

            print("direction mean --")
            print(trades_df.groupby("direction")["pnl_usd"].mean())
            print("\n")

            print("asset mean --")
            print(trades_df.groupby("asset")["pnl_usd"].mean())
            print("\n")

            print("direction mean median count --")
            print(trades_df.groupby("direction")["pnl_usd"].agg(["mean", "median", "count"]))
            print("\n")

            # X = trades_df.drop(columns=["pnl_usd"])
            # y = trades_df["pnl_usd"]
            #
            # categorical = [
            #     "asset", "direction",
            #     "entry_yq",
            #     "entry_mw",
            #     "entry_wd",
            #     "entry_dq",
            #     "entry_q90m",
            #     "entry_rr_perc",
            # ]
            # numeric = [
            #     "entry_rr", "entry_position_usd", "entry_position_fee", "close_position_fee", "minutes_in_market"
            # ]
            #
            # preprocess = ColumnTransformer([
            #     ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
            #     ("num", "passthrough", numeric)
            # ])
            #
            # model = Pipeline([
            #     ("preprocess", preprocess),
            #     ("regressor", LinearRegression())
            # ])
            #
            # model.fit(X, y)
            #
            # print("R^2:", model.score(X, y))
            # print("Coefficients:", model.named_steps["regressor"].coef_)

        # [
        #     'asset',
        #     'entry_rr',
        #     'psp_key_used',
        #     'direction',
        #     'pnl_usd',
        #     'entry_position_usd',
        #     'minutes_in_market',
        #     'entry_yq',
        #     'entry_mw',
        #     'entry_wd',
        #     'entry_dq',
        #     'entry_q90m',
        #     'entry_rr_perc',
        # ]

        print("done!")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
