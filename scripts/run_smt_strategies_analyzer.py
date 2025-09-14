from datetime import timedelta
from typing import List, Tuple

import numpy as np
import pandas
import pandas as pd
from IPython.core.display_functions import display

from stock_market_research_kit.candle_with_stat import perc_all_and_sma20
from stock_market_research_kit.smt_psp_trade import SmtPspTrade, smt_psp_trades_from_json
from utils.date_utils import to_utc_datetime, quarters_by_time, to_timestamp, to_ny_date_str


def snapshot_file(strategy: str, year: int, symbols: List[str]) -> str:
    smb = '_'.join([x.replace('USDT', '').lower() for x in symbols])
    return f"scripts/test_snapshots/strategy_{strategy}_{year}_{smb}.json"


def snap_dfs(snap_file: str, symbols: List[str]) -> Tuple[
    str,
    List[Tuple[str, pd.DataFrame]],
    List[Tuple[str, pd.DataFrame]],
]:  # filename, tdfs, lodfs
    with open(snap_file, "r", encoding="utf-8") as f:
        # with open(strategy31_2024_snapshot, "r", encoding="utf-8") as f:
        json_str = f.read()
    df_m, df_l, df_lo = to_trade_dfs(smt_psp_trades_from_json(json_str), symbols)

    df_m = with_cum_and_stagnation(df_m)
    df_l = with_cum_and_stagnation(df_l)
    df_l_chase_median_rr = with_cum_and_stagnation(df_l.query('limit_reason == "chase_median_rr"'))
    df_l_chase_mean_rr = with_cum_and_stagnation(df_l.query('limit_reason == "chase_mean_rr"'))
    df_l_chase_absent_tyo = with_cum_and_stagnation(df_l.query('limit_reason == "chase_absent_tyo"'))
    df_l_chase_absent_tmo = with_cum_and_stagnation(df_l.query('limit_reason == "chase_absent_tmo"'))
    df_l_chase_absent_two = with_cum_and_stagnation(df_l.query('limit_reason == "chase_absent_two"'))
    df_l_chase_absent_tdo = with_cum_and_stagnation(df_l.query('limit_reason == "chase_absent_tdo"'))
    df_l_chase_absent_t90mo = with_cum_and_stagnation(df_l.query('limit_reason == "chase_absent_t90mo"'))
    df_l_chase_tyo = with_cum_and_stagnation(df_l.query('limit_reason == "chase_tyo"'))
    df_l_chase_tmo = with_cum_and_stagnation(df_l.query('limit_reason == "chase_tmo"'))
    df_l_chase_two = with_cum_and_stagnation(df_l.query('limit_reason == "chase_two"'))
    df_l_chase_tdo = with_cum_and_stagnation(df_l.query('limit_reason == "chase_tdo"'))
    df_l_chase_t90mo = with_cum_and_stagnation(df_l.query('limit_reason == "chase_t90mo"'))

    df_lo_chase_median_rr = df_lo.query('limit_reason == "chase_median_rr"')
    df_lo_chase_mean_rr = df_lo.query('limit_reason == "chase_mean_rr"')
    df_lo_chase_absent_tyo = df_lo.query('limit_reason == "chase_absent_tyo"')
    df_lo_chase_absent_tmo = df_lo.query('limit_reason == "chase_absent_tmo"')
    df_lo_chase_absent_two = df_lo.query('limit_reason == "chase_absent_two"')
    df_lo_chase_absent_tdo = df_lo.query('limit_reason == "chase_absent_tdo"')
    df_lo_chase_absent_t90mo = df_lo.query('limit_reason == "chase_absent_t90mo"')
    df_lo_chase_tyo = df_lo.query('limit_reason == "chase_tyo"')
    df_lo_chase_tmo = df_lo.query('limit_reason == "chase_tmo"')
    df_lo_chase_two = df_lo.query('limit_reason == "chase_two"')
    df_lo_chase_tdo = df_lo.query('limit_reason == "chase_tdo"')
    df_lo_chase_t90mo = df_lo.query('limit_reason == "chase_t90mo"')

    trades_dfs = [
        ('market all', df_m),
        ('limit all', df_l),
        ('chase_median_rr', df_l_chase_median_rr),
        ('chase_mean_rr', df_l_chase_mean_rr),
        ('chase_absent_tyo', df_l_chase_absent_tyo),
        ('chase_absent_tmo', df_l_chase_absent_tmo),
        ('chase_absent_two', df_l_chase_absent_two),
        ('chase_absent_tdo', df_l_chase_absent_tdo),
        ('chase_absent_t90mo', df_l_chase_absent_t90mo),
        ('chase_tyo', df_l_chase_tyo),
        ('chase_tmo', df_l_chase_tmo),
        ('chase_two', df_l_chase_two),
        ('chase_tdo', df_l_chase_tdo),
        ('chase_t90mo', df_l_chase_t90mo),
    ]

    lo_dfs = [
        ('limit all', df_lo),
        ('chase_median_rr', df_lo_chase_median_rr),
        ('chase_mean_rr', df_lo_chase_mean_rr),
        ('chase_absent_tyo', df_lo_chase_absent_tyo),
        ('chase_absent_tmo', df_lo_chase_absent_tmo),
        ('chase_absent_two', df_lo_chase_absent_two),
        ('chase_absent_tdo', df_lo_chase_absent_tdo),
        ('chase_absent_t90mo', df_lo_chase_absent_t90mo),
        ('chase_tyo', df_lo_chase_tyo),
        ('chase_tmo', df_lo_chase_tmo),
        ('chase_two', df_lo_chase_two),
        ('chase_tdo', df_lo_chase_tdo),
        ('chase_t90mo', df_lo_chase_t90mo),
    ]

    return f.name, trades_dfs, lo_dfs


def merge_year_dfs(df_lists: List[List[Tuple[str, pd.DataFrame]]], query: str, order_type_filter: List[str]) -> List[
    Tuple[str, pd.DataFrame]
]:
    result = []
    for df_list in df_lists:
        filtered = df_list
        if len(order_type_filter) > 0:
            filtered = [x for x in filtered if x[0] in order_type_filter]
        for i, df_tuple in enumerate(filtered):
            res_df = df_tuple[1]
            if query:
                res_df = res_df.query(query)
            if len(result) == i:
                result.append((df_tuple[0], res_df))
                continue
            prev_df_tuple = result[i]
            result[i] = (prev_df_tuple[0], pd.concat([prev_df_tuple[1], res_df], ignore_index=True))

    return result


def basic_trade_stat(name, df) -> Tuple[str, int, float, float, float, float, float, float, float, float, float, float]:
    if len(df) == 0:
        return name, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0

    len_trades = (df["entry_time"] != "").sum()

    return (
        name,
        len_trades,
        round(df['cum_pnl_usd'].iloc[-1], 2),
        round(df['cum_pnl_minus_fees'].iloc[-1], 2),
        round(df['cum_pnl_minus_fees'].iloc[-1] / len_trades, 2),
        round(df['cum_final_close_pnl'].iloc[-1], 2),
        round(df['cum_pnl_if_full_tp_sl'].iloc[-1], 2),
        round(df['cum_pnl_minus_fees_if_full_tp_sl'].iloc[-1], 2),
        round(df['cum_pre_final_closes_pnl'].iloc[-1], 2),
        round(df['cum_pnl_if_full_preclose'].iloc[-1], 2),
        round(df['cum_pnl_minus_fees_if_full_preclose'].iloc[-1], 2),
        round(len(df.query('won')) / len_trades, 3)
    )


def basic_lo_stat(name, df) -> Tuple[str, int, int, int, float]:
    if len(df) == 0:
        return name, 0, 0, 0, 0
    len_filled = (df["limit_status"] == "FILLED").sum()
    len_cancelled = (df["limit_status"] == "CANCELLED").sum()
    return name, len(df), len_cancelled, len_filled, round(100 * len_filled / (len_filled + len_cancelled), 2)


def basic_stats(trade_dfs, lo_dfs):
    df_trade_stat = pd.DataFrame([basic_trade_stat(label, df) for label, df in trade_dfs], columns=[
        'name',
        'trades',
        'pnl',
        'mf pnl',
        'mf pnl/trade',
        'tpsl close',
        'full tpsl',
        'mf full tpsl',
        'pre',
        'if full pre',
        'mf full pre',
        'win rate',
    ])

    df_lo_stat = pd.DataFrame([basic_lo_stat(label, df) for label, df in lo_dfs], columns=[
        'name',
        'total_orders',
        'cancelled',
        'filled',
        'conversion',
    ])

    return df_trade_stat, df_lo_stat


def pnls(stat_df):
    market_dict = stat_df.loc[stat_df["name"] == "market all"].iloc[0].to_dict()
    limit_dict = stat_df.loc[stat_df["name"] == "limit all"].iloc[0].to_dict()
    return {
        'trades': market_dict["trades"] + limit_dict["trades"],
        'pnl': round(market_dict["pnl"] + limit_dict["pnl"], 2),
        'mf pnl': round(market_dict["mf pnl"] + limit_dict["mf pnl"], 2),
        'mf pnl/trade': round(market_dict["mf pnl/trade"] + limit_dict["mf pnl/trade"], 2),
        'tpsl close': round(market_dict["tpsl close"] + limit_dict["tpsl close"], 2),
        'full tpsl': round(market_dict["full tpsl"] + limit_dict["full tpsl"], 2),
        'mf full tpsl': round(market_dict["mf full tpsl"] + limit_dict["mf full tpsl"], 2),
        'pre': round(market_dict["pre"] + limit_dict["pre"], 2),
        'if full pre': round(market_dict["if full pre"] + limit_dict["if full pre"], 2),
        'mf full pre': round(market_dict["mf full pre"] + limit_dict["mf full pre"], 2),
        'win rate': round(market_dict["win rate"] + limit_dict["win rate"], 2),
    }


def wr_mean(pnls: pd.Series) -> float:
    won = [x for x in pnls.to_list() if x > 0]
    return round(len(won) / len(pnls), 2)


def wr_count(pnls: pd.Series) -> float:
    won = [x for x in pnls.to_list() if x > 0]
    return len(won)


def group_display(trade_dfs, fields: List[str], display_good: bool, show_percs: List[str], aggs_to_hide: List[str]):
    def to_agg_df(label, df):
        if df.empty:
            return pd.DataFrame()

        def aggs_hide_filter(x):
            for agg in aggs_to_hide:
                x_name = x
                if callable(x):
                    x_name = x.__name__
                if x_name == agg:
                    return False
            return True

        agg_pnl = df.groupby(fields)["pnl_minus_fees"].agg(list(filter(aggs_hide_filter, [
            "mean", "sem", wr_mean, "median", "count", wr_count, "sum", "min", "max", "std", "var"
        ])))
        agg_pnl = agg_pnl.round({
            "mean": 2, "sem": 2, "median": 2, "sum": 2, "min": 2, "max": 2, "std": 2, "var": 2,
        })

        if display_good:
            agg_pnl_filtered = agg_pnl[(agg_pnl["count"] > 4) & (agg_pnl["mean"] > 15)]
            if not agg_pnl_filtered.empty:
                display(label, agg_pnl_filtered)

        agg_pnl.columns = pd.MultiIndex.from_product([[label], agg_pnl.columns])
        for col in agg_pnl.columns:
            if col[1] == "count":  # второй уровень MultiIndex
                agg_pnl[col] = agg_pnl[col].astype("Int64")  # nullable integer
        return agg_pnl

    df_joined = pd.concat(
        [to_agg_df(label, df) for label, df in trade_dfs if not df.empty],
        axis=1
    )

    for show_perc in show_percs:
        for df_tuple in trade_dfs:
            if df_tuple[1].empty:
                continue
            display(
                f"{show_perc} {df_tuple[0]} percentiles {[round(float(x), 2) for x in np.percentile(df_tuple[1][show_perc], [10, 30, 70, 90])]}")

    return df_joined


def with_cum_and_stagnation(df: pd.DataFrame) -> pd.DataFrame:
    if len(df) == 0:
        return df
    df = df.copy()
    df["cum_pnl_usd"] = df["pnl_usd"].cumsum()
    df["cum_pnl_minus_fees"] = df["pnl_minus_fees"].cumsum()
    df["cum_final_close_pnl"] = df["final_close_pnl"].cumsum()
    df["cum_pre_final_closes_pnl"] = df["pre_final_closes_sum_pnl"].cumsum()
    df["cum_pnl_if_full_preclose"] = df["pnl_if_full_preclose"].cumsum()
    df["cum_pnl_minus_fees_if_full_preclose"] = df["pnl_minus_fees_if_full_preclose"].cumsum()
    df["cum_pnl_if_full_tp_sl"] = df["pnl_if_full_tp_sl"].cumsum()
    df["cum_pnl_minus_fees_if_full_tp_sl"] = df["pnl_minus_fees_if_full_tp_sl"].cumsum()
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


def _to_trade_df(trades: List[SmtPspTrade], symbols: List[str]) -> pd.DataFrame:
    df = pd.DataFrame(trades, columns=[
        'signal_time', 'signal_time_ny', 'entry_time', 'entry_time_ny', 'asset', 'pnl_usd', 'direction', 'entry_rr',
        'entry_price', 'stop', 'take_profit', 'entry_position_usd', 'entry_reason',
        'smt_level', 'smt_type', 'smt_label', 'smt_flags', 'smt_first_appeared',
        'target_level', 'target_direction', 'target_label', 'target_ql_start',
        'best_entry_time', 'best_entry_time_ny', 'best_entry_price', 'best_entry_rr',
        'best_pnl', 'best_pnl_time', 'best_pnl_time_ny', 'best_pnl_price',
        'psp_extremums', 'targets',  # TODO придумать как проанализировать
        'entry_position_fee',
        'close_position_fee'
    ])
    if len(trades) == 0:
        return df
    df["won"] = df["pnl_usd"] > 0

    df["smt_is_half"] = df.index.map(lambda i: trades[i].smt_type in ['half_high', 'half_low'])
    df["target_is_half"] = df.index.map(lambda i: trades[i].target_direction in ['half_high', 'half_low'])
    df["asset_smt_swept"] = df.index.map(lambda i: trades[i].asset[0] in trades[i].smt_flags)

    df['pnl_minus_fees'] = df['pnl_usd'] - df['entry_position_fee'] - df['close_position_fee']
    df['final_close_percent'] = df.index.map(lambda i: trades[i].closes[-1][0])
    df['final_close_pnl'] = df.index.map(lambda i: trades[i].pnls_per_closes()[-1])
    df['pre_final_closes_sum_pnl'] = df.index.map(lambda i: sum(trades[i].pnls_per_closes()[0:-1]))
    df['pnl_if_full_preclose'] = df.index.map(lambda i: trades[i].pnl_if_full_preclose()[0])
    df['pnl_minus_fees_if_full_preclose'] = df.index.map(
        lambda i: trades[i].pnl_if_full_preclose()[0] - trades[i].pnl_if_full_preclose()[1] - trades[
            i].entry_position_fee)
    df['pnl_if_full_tp_sl'] = df.index.map(lambda i: trades[i].pnl_if_full_tp_sl()[0])
    df['pnl_minus_fees_if_full_tp_sl'] = df.index.map(
        lambda i: trades[i].pnl_if_full_tp_sl()[0] - trades[i].pnl_if_full_tp_sl()[1] - trades[i].entry_position_fee)

    df["final_close_time"] = df.index.map(lambda i: trades[i].closes[-1][2])
    df["final_close_time_ny"] = df.index.map(lambda i: trades[i].closes[-1][3])
    df["final_close_price"] = df.index.map(lambda i: trades[i].closes[-1][1])
    df['minutes_in_market'] = df.apply(
        lambda row: (to_utc_datetime(row['final_close_time']) - to_utc_datetime(
            row['entry_time'])).total_seconds() / 60,
        axis=1
    )
    df['minutes_between_signal_and_entry'] = df.apply(
        lambda row: (to_utc_datetime(row['entry_time']) - to_utc_datetime(
            row['signal_time'])).total_seconds() / 60,
        axis=1
    )
    df['best_entry_minutes_in_market'] = df.apply(
        lambda row: (to_utc_datetime(row['final_close_time']) - to_utc_datetime(
            row['best_entry_time'])).total_seconds() / 60,
        axis=1
    )
    asset_to_index = {}
    for i_, s in enumerate(symbols):
        asset_to_index[s] = i_
    df['percents_till_stop'] = df.index.map(lambda i: abs(trades[i].psp_extremums[asset_to_index[trades[i].asset]]))
    df['percents_till_stop_perc'], _ = perc_all_and_sma20(df['percents_till_stop'])
    df['percents_till_take'] = df.index.map(lambda i: abs(trades[i].targets[asset_to_index[trades[i].asset]]))
    df['percents_till_take_perc'], _ = perc_all_and_sma20(df['percents_till_take'])
    df['psp_key'] = df.index.map(lambda i: trades[i].psp_key_used)
    df['psp_date'] = df.index.map(lambda i: trades[i].psp_date)
    df['psp_date_ny'] = df.index.map(lambda i: to_ny_date_str(trades[i].psp_date))
    df['year'] = df['signal_time'].apply(lambda x: int(x[0:4]))
    df['signal_yq'] = df['signal_time'].apply(lambda x: quarters_by_time(x)[0].name)
    df['signal_mw'] = df['signal_time'].apply(lambda x: quarters_by_time(x)[1].name)
    df['signal_wd'] = df['signal_time'].apply(lambda x: quarters_by_time(x)[2].name)
    df['signal_dq'] = df['signal_time'].apply(lambda x: quarters_by_time(x)[3].name)
    df['signal_q90m'] = df['signal_time'].apply(lambda x: quarters_by_time(x)[4].name)
    df['entry_yq'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[0].name)
    df['entry_mw'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[1].name)
    df['entry_wd'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[2].name)
    df['entry_dq'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[3].name)
    df['entry_q90m'] = df['entry_time'].apply(lambda x: quarters_by_time(x)[4].name)
    df['best_entry_yq'] = df.index.map(lambda i: quarters_by_time(trades[i].best_entry_time)[0].name)
    df['best_entry_mw'] = df.index.map(lambda i: quarters_by_time(trades[i].best_entry_time)[1].name)
    df['best_entry_wd'] = df.index.map(lambda i: quarters_by_time(trades[i].best_entry_time)[2].name)
    df['best_entry_dq'] = df.index.map(lambda i: quarters_by_time(trades[i].best_entry_time)[3].name)
    df['best_entry_q90m'] = df.index.map(lambda i: quarters_by_time(trades[i].best_entry_time)[4].name)
    df['final_close_yq'] = df.index.map(lambda i: quarters_by_time(trades[i].closes[-1][2])[0].name)
    df['final_close_mw'] = df.index.map(lambda i: quarters_by_time(trades[i].closes[-1][2])[1].name)
    df['final_close_wd'] = df.index.map(lambda i: quarters_by_time(trades[i].closes[-1][2])[2].name)
    df['final_close_dq'] = df.index.map(lambda i: quarters_by_time(trades[i].closes[-1][2])[3].name)
    df['final_close_q90m'] = df.index.map(lambda i: quarters_by_time(trades[i].closes[-1][2])[4].name)
    df['entry_rr_perc'], _ = perc_all_and_sma20(df['entry_rr'])
    df['minutes_in_market_perc'], _ = perc_all_and_sma20(df['minutes_in_market'])
    df['entry_position_usd_perc'], _ = perc_all_and_sma20(df['entry_position_usd'])
    df['entry_tos'] = df.index.map(lambda i: '-'.join([x[0] for x in trades[i].entry_tos]))
    df['best_entry_tos'] = df.index.map(lambda i: '-'.join([x[0] for x in trades[i].best_entry_tos]))
    df['best_pnl_tos'] = df.index.map(lambda i: '-'.join([x[0] for x in trades[i].best_pnl_tos]))

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

    first_cols = [
        'signal_time', 'entry_time', 'asset', 'direction', 'pnl_usd', 'pnl_minus_fees',
    ]
    cols = first_cols + [c for c in df.columns if c not in first_cols]
    df = df[cols]

    return df


def _to_lo_df(trades: List[SmtPspTrade]) -> pd.DataFrame:
    df = pd.DataFrame(trades, columns=[
        'signal_time', 'signal_time_ny', 'asset', 'pnl_usd', 'direction',
        'limit_stop', 'limit_take_profit', 'limit_rr', 'limit_position_usd', 'limit_status',
        'limit_chase_to_label', 'limit_chase_rr',
        'limit_price_history',  # TODO придумать как анализировать

        'entry_time', 'entry_time_ny',

        'smt_level', 'smt_type', 'smt_label', 'smt_flags', 'smt_first_appeared',
        'target_level', 'target_direction', 'target_label',
        'psp_extremums', 'targets',  # TODO придумать как проанализировать
    ])
    df['limit_reason'] = df.index.map(lambda i: trades[i].entry_reason.split()[0])
    df['final_close_reason'] = df.index.map(lambda i: trades[i].closes[-1][4])
    df['final_close_time'] = df.index.map(lambda i: trades[i].closes[-1][2])
    df['minutes_till_final_close'] = df.apply(
        lambda row: (to_utc_datetime(row['final_close_time']) - to_utc_datetime(
            row['signal_time'])).total_seconds() / 60,
        axis=1
    )

    return df


#  market_trades, limit_trades, limit_orders (filled and cancelled)
def to_trade_dfs(trades: List[SmtPspTrade], symbols: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    m_trades = [x for x in trades if x.entry_order_type == 'MARKET']
    l_trades = [x for x in trades if x.entry_order_type == 'LIMIT']
    l_orders = [x for x in trades if x.limit_status]
    df_m = _to_trade_df(m_trades, symbols)

    df_l = _to_trade_df(l_trades, symbols)
    df_l['limit_reason'] = df_l.index.map(lambda i: l_trades[i].entry_reason.split()[0])

    def with_market_trade_columns(row):
        candidates = df_m[
            (df_m["asset"] == row["asset"]) &
            (df_m["stop"] == row["stop"]) &
            (df_m["take_profit"] == row["take_profit"]) &
            (df_m["signal_time"] == row["signal_time"])
            ]
        if candidates.empty:
            return pd.Series({"market_pnl": None, "market_rr": None})
        return pd.Series({"market_pnl": candidates.iloc[0]["pnl_usd"], "market_rr": candidates.iloc[0]["entry_rr"]})

    df_l[["market_pnl", "market_rr"]] = df_l.apply(with_market_trade_columns, axis=1)

    df_lo = _to_lo_df(l_orders)

    return df_m, df_l, df_lo


def to_tw_arrs(symbol: str, df: pandas.DataFrame) -> str:
    s_df = df.query(f"asset == '{symbol}'")
    psp_times = [to_timestamp(to_utc_datetime(x)) for x in s_df["psp_date"].tolist()]
    entry_times = [to_timestamp(to_utc_datetime(x)) for x in s_df["entry_time"].tolist()]
    entry_prices = s_df["entry_price"].tolist()
    stop_prices = s_df["stop"].tolist()
    take_profit_prices = s_df["take_profit"].tolist()
    best_entry_times = [to_timestamp(to_utc_datetime(x) - timedelta(minutes=15)) for x in s_df["best_entry_time"].tolist()]
    best_entry_prices = s_df["best_entry_price"].tolist()
    final_close_times = [to_timestamp(to_utc_datetime(x) - timedelta(minutes=15)) for x in s_df["final_close_time"].tolist()]
    final_close_prices = s_df["final_close_price"].tolist()

    directions = [1 if x == "UP" else -1 for x in s_df["direction"].tolist()]
    res = f"""var int[] psp_times = array.from({', '.join(map(str, psp_times))})
var int[] entry_times = array.from({', '.join(map(str, entry_times))})
var float[] entry_prices = array.from({', '.join(map(str, entry_prices))})
var float[] stop_prices = array.from({', '.join(map(str, stop_prices))})
var float[] take_profit_prices = array.from({', '.join(map(str, take_profit_prices))})
var int[] best_entry_times = array.from({', '.join(map(str, best_entry_times))})
var float[] best_entry_prices = array.from({', '.join(map(str, best_entry_prices))})
var int[] final_close_times = array.from({', '.join(map(str, final_close_times))})
var float[] final_close_prices = array.from({', '.join(map(str, final_close_prices))})
var int[] directions = array.from({', '.join(map(str, directions))})"""

    return res


def analyze():
    symbols = ['BTCUSDT', 'ETHUSDT', 'TOTAL3']
    f_name_2024, trade_dfs_2024, lo_dfs_2024 = snap_dfs(snapshot_file('30', 2024, symbols), symbols)
    f_name_2025, trade_dfs_2025, lo_dfs_2025 = snap_dfs(snapshot_file('30', 2025, symbols), symbols)

    trade_stat_2024, lo_stat_2024 = basic_stats(trade_dfs_2024, lo_dfs_2024)
    trade_stat_2025, lo_stat_2025 = basic_stats(trade_dfs_2025, lo_dfs_2025)

    trade_dfs = merge_year_dfs([trade_dfs_2024, trade_dfs_2025], "", ['market all'])

    # print(trade_dfs[1][1]['smt_is_half'])
    #
    # print(f"2024 {f_name_2024[23:34]}:")
    # print(f"2024 Trade stat:")
    # display(trade_stat_2024)
    # print(f"2025 {f_name_2025[23:34]}:")
    # print(f"2025 Trade stat:")
    # display(trade_stat_2025)
    #
    # print(f"2024 Limit orders stat:")
    # display(lo_stat_2024)
    # print(f"2025 Limit orders stat:")
    # display(lo_stat_2025)

    df_group = group_display(trade_dfs, ["asset"], False, [], ["mean", "wr_mean"])
    # group_by_display(["smt_type"])
    # group_by_display(["smt_label"])
    # group_by_display(["smt_flags"])

    # print(df_group)
    print(to_tw_arrs("BTCUSDT", trade_dfs[0][1]))


if __name__ == "__main__":
    try:
        analyze()
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
