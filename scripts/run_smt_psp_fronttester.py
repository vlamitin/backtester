import time
from typing import List, Dict

from stock_market_research_kit.asset import TriadCandles15mGenerator
from stock_market_research_kit.smt_psp_strategy import SmtPspStrategy
from stock_market_research_kit.smt_psp_trade import SmtPspTrade
from stock_market_research_kit.triad import Triad, new_smt_found, smt_dict_old_smt_cancelled, targets_reached, \
    targets_new_appeared, calc_psp_changed
from utils.date_utils import to_utc_datetime, log_info_ny, log_warn_ny


def fronttest(
        triad: Triad,
        strategies: List[SmtPspStrategy],
        candles_gen: TriadCandles15mGenerator,
        stop_after: str
) -> Dict[str, List[SmtPspTrade]]:
    symbols = (triad.a1.symbol, triad.a2.symbol, triad.a3.symbol)
    closed_trades: Dict[str, List[SmtPspTrade]] = {}
    active_trades: Dict[str, List[SmtPspTrade]] = {}

    _tc_pnl = []
    for s in strategies:
        _tc_pnl.append((int(s.name[0:2]), 0, 0))
        closed_trades[s.name] = []
        active_trades[s.name] = []

    prev_smt_psp = triad.actual_smt_psp()
    prev_long_targets = triad.long_targets()
    prev_short_targets = triad.short_targets()

    _prev_handle_day_time = time.perf_counter()
    _counter = 0
    while True:
        a1_candle, a2_candle, a3_candle = next(candles_gen)
        if _counter % (4 * 24 * 1) == 0:
            log_info_ny(
                f"candle {a1_candle[5]}, handled {_counter // (4 * 24)} days, strategies: {'; '.join([f'{x[0]}) {x[1]}/{round(x[2], 2)}' for x in _tc_pnl])}. Took {(time.perf_counter() - _prev_handle_day_time):.3f} seconds")
            _prev_handle_day_time = time.perf_counter()
        _counter += 1

        _plus_15m_time = time.perf_counter()
        triad.a1.plus_15m(a1_candle)
        triad.a2.plus_15m(a2_candle)
        triad.a3.plus_15m(a3_candle)
        _plus_15m_time_took = time.perf_counter() - _plus_15m_time
        if _plus_15m_time_took > 0.02:
            log_warn_ny(f"plus_15m took {_plus_15m_time_took:.6f} seconds")

        # _smt_psp_targets_time = time.perf_counter()

        _smt_psp_time = time.perf_counter()
        smt_psp = triad.actual_smt_psp()
        _smt_psp_took = time.perf_counter() - _smt_psp_time
        # if _smt_psp_took > 0.5:
        #     log_warn_ny(f"smt_psp took {_smt_psp_took:.6f} seconds")

        # _targets_tos_time = time.perf_counter()
        long_targets = triad.long_targets()
        short_targets = triad.short_targets()
        tos = triad.true_opens()
        # log_info_ny(f"targets_tos_time took {(time.perf_counter() - _targets_tos_time):.6f} seconds")

        # _smts_psps_change_time = time.perf_counter()
        new_smts = new_smt_found(prev_smt_psp, smt_psp)
        cancelled_smts = smt_dict_old_smt_cancelled(prev_smt_psp, smt_psp)
        psp_changed = calc_psp_changed(symbols, prev_smt_psp, smt_psp)
        # log_info_ny(f"smts_psps_change_time took {(time.perf_counter() - _smts_psps_change_time):.6f} seconds")

        # _targets_change_time = time.perf_counter()
        reached_long_targets = targets_reached(
            (triad.a1.prev_15m_candle, triad.a2.prev_15m_candle, triad.a3.prev_15m_candle),
            prev_long_targets, long_targets)
        reached_short_targets = targets_reached(
            (triad.a1.prev_15m_candle, triad.a2.prev_15m_candle, triad.a3.prev_15m_candle),
            prev_short_targets, short_targets)

        new_long_targets = targets_new_appeared(prev_long_targets, long_targets)
        new_short_targets = targets_new_appeared(prev_short_targets, short_targets)
        # log_info_ny(f"targets_change_time took {(time.perf_counter() - _targets_change_time):.6f} seconds")

        # _smt_psp_targets_time_took = time.perf_counter() - _smt_psp_targets_time
        # if _smt_psp_targets_time_took > 0.7:
        #     log_warn_ny(f"smt_psp_targets_time took {_smt_psp_targets_time_took:.6f} seconds")

        _handle_strategies_time = time.perf_counter()
        for i, s in enumerate(strategies):
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
                _tc_pnl[i] = (_tc_pnl[i][0], _tc_pnl[i][1] + 1, _tc_pnl[i][2] + t.pnl_usd)
            closed_trades[s.name].extend(s_closed_trades)

            if to_utc_datetime(triad.a1.snapshot_date_readable) >= to_utc_datetime(stop_after):
                return closed_trades

            s_active_trades = s.trade_opener(
                triad, tos,
                (prev_smt_psp, smt_psp, new_smts, cancelled_smts, psp_changed),
                (
                    prev_long_targets, prev_short_targets, long_targets, short_targets,
                    reached_long_targets, reached_short_targets, new_long_targets, new_short_targets
                )
            )
            active_trades[s.name].extend(s_active_trades)

        _handle_strategies_took = time.perf_counter() - _handle_strategies_time
        if _handle_strategies_took > 0.02:
            log_warn_ny(f"handle_strategies took {_handle_strategies_took:.6f} seconds")

        prev_smt_psp = smt_psp
        prev_long_targets = long_targets
        prev_short_targets = short_targets
