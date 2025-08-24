import time
from typing import List, Dict

from stock_market_research_kit.asset import TriadCandles15mGenerator
from stock_market_research_kit.smt_psp_strategy import SmtPspStrategy
from stock_market_research_kit.smt_psp_trade import SmtPspTrade
from stock_market_research_kit.triad import Triad, new_smt_found, smt_dict_old_smt_cancelled, targets_reached, \
    targets_new_appeared, calc_psp_changed
from utils.date_utils import to_utc_datetime, log_info_ny


def fronttest(
        triad: Triad,
        strategies: List[SmtPspStrategy],
        candles_gen: TriadCandles15mGenerator,
        stop_after: str
) -> Dict[str, List[SmtPspTrade]]:
    closed_trades: Dict[str, List[SmtPspTrade]] = {}
    active_trades: Dict[str, List[SmtPspTrade]] = {}
    for s in strategies:
        closed_trades[s.name] = []
        active_trades[s.name] = []

    counter = 0
    trades_count, pnl = 0, 0
    prev_smt_psp = triad.actual_smt_psp()
    prev_long_targets = triad.long_targets()
    prev_short_targets = triad.short_targets()

    prev_handle_day_time = time.perf_counter()
    while True:
        a1_candle, a2_candle, a3_candle = next(candles_gen)
        if counter % (4 * 24 * 1) == 0:
            print(f"candle {a1_candle[5]}, handled {counter // (4 * 24)} days, closed {trades_count} trades pnl is {pnl}, took {(time.perf_counter() - prev_handle_day_time):.6f} seconds")
            prev_handle_day_time = time.perf_counter()
        counter += 1

        # plus_15m_time = time.perf_counter()
        triad.a1.plus_15m(a1_candle)
        triad.a2.plus_15m(a2_candle)
        triad.a3.plus_15m(a3_candle)
        # log_info_ny(f"plus_15m took {(time.perf_counter() - plus_15m_time):.6f} seconds")

        # smt_psp_targets_time = time.perf_counter()
        smt_psp = triad.actual_smt_psp()
        long_targets = triad.long_targets()
        short_targets = triad.short_targets()
        tos = triad.true_opens()

        new_smts = new_smt_found(prev_smt_psp, smt_psp)
        cancelled_smts = smt_dict_old_smt_cancelled(prev_smt_psp, smt_psp)
        psp_changed = calc_psp_changed(prev_smt_psp, smt_psp)

        reached_long_targets = targets_reached(
            (triad.a1.prev_15m_candle, triad.a2.prev_15m_candle, triad.a3.prev_15m_candle),
            prev_long_targets, long_targets)
        reached_short_targets = targets_reached(
            (triad.a1.prev_15m_candle, triad.a2.prev_15m_candle, triad.a3.prev_15m_candle),
            prev_short_targets, short_targets)

        new_long_targets = targets_new_appeared(prev_long_targets, long_targets)
        new_short_targets = targets_new_appeared(prev_short_targets, short_targets)
        # log_info_ny(f"smt_psp_targets_time took {(time.perf_counter() - smt_psp_targets_time):.6f} seconds")

        for s in strategies:
            # handle_trades_time = time.perf_counter()
            active_trades[s.name], s_closed_trades = s.trades_handler(
                stop_after,
                active_trades[s.name],
                triad, tos,
                (prev_smt_psp, smt_psp, new_smts, cancelled_smts, psp_changed),
                (
                    prev_long_targets, prev_short_targets, long_targets, short_targets,
                    reached_long_targets, reached_short_targets, new_long_targets, new_short_targets
                )
            )
            for t in s_closed_trades:
                trades_count += 1
                pnl += t.pnl_usd
            closed_trades[s.name].extend(s_closed_trades)
            # if len(active_trades[s.name]) > 0:
            #     log_info_ny(f"handle_trades_time took {(time.perf_counter() - handle_trades_time):.6f} seconds")

            if to_utc_datetime(triad.a1.snapshot_date_readable) >= to_utc_datetime(stop_after):
                return closed_trades

            # open_trades_time = time.perf_counter()
            s_active_trades = s.trade_opener(
                triad, tos,
                (prev_smt_psp, smt_psp, new_smts, cancelled_smts, psp_changed),
                (
                    prev_long_targets, prev_short_targets, long_targets, short_targets,
                    reached_long_targets, reached_short_targets, new_long_targets, new_short_targets
                )
            )
            active_trades[s.name].extend(s_active_trades)
            # if len(s_active_trades) > 0:
            #     log_info_ny(f"open_trades_time took {(time.perf_counter() - open_trades_time):.6f} seconds")

        prev_smt_psp = smt_psp
        prev_long_targets = long_targets
        prev_short_targets = short_targets


