from dataclasses import dataclass
from typing import TypeAlias, Callable, List, Tuple, Optional

from stock_market_research_kit.smt_psp_trade import SmtPspTrade
from stock_market_research_kit.triad import Triad, SMTLevels, SMT, Target, TrueOpen
from utils.date_utils import to_utc_datetime

ONE_RR_IN_USD = 100

TrueOpens: TypeAlias = Tuple[List[TrueOpen], List[TrueOpen], List[TrueOpen]]
SmtPspChange: TypeAlias = Tuple[
    List[Tuple[int, str, Optional[SMTLevels]]],  # old (level, label, smt levels)
    List[Tuple[int, str, Optional[SMTLevels]]],  # new (level, label, smt levels)
    List[Tuple[int, str, SMT]],  # new: key, SMT
    List[Tuple[int, str, SMT]],  # cancelled: key, SMT
    List[Tuple[int, str, str, str, str, str]]
    # smt_level, smt_key, smt_type, psp_key, psp_date, possible|closed|confirmed|swept
]
TargetChange: TypeAlias = Tuple[
    List[Target],  # old long
    List[Target],  # old short
    List[Target],  # all new long targets
    List[Target],  # all new short targets
    List[Tuple[int, str, str, int, float]],  # reached long: level, direction, target_label, asset_index, price
    List[Tuple[int, str, str, int, float]],  # reached short: level, direction, target_label, asset_index, price
    List[Target],  # only new long targets
    List[Target],  # only new short targets
]

TOpener: TypeAlias = Callable[[Triad, TrueOpens, SmtPspChange, TargetChange], List[SmtPspTrade]]
TsHandler: TypeAlias = Callable[
    [str, List[SmtPspTrade], Triad, TrueOpens, SmtPspChange, TargetChange],
    Tuple[List[SmtPspTrade], List[SmtPspTrade]]  # active_trades, closed_trades
]


@dataclass
class SmtPspStrategy:
    name: str
    trade_opener: TOpener
    trades_handler: TsHandler


smt_types_is_long = {
    'high': False,
    'half_high': False,
    'half_low': True,
    'low': True,
}


def _psp_extremums(
        smt_lvls: SMTLevels, smt_type: str, psp_key: str, psp_date: str
) -> Tuple[float, float, float]:
    smt = smt_lvls[0] if smt_type == 'high' else smt_lvls[2] if smt_type == 'low' else smt_lvls[1]
    psps = (
        smt.psps_15m if psp_key == '15m' else smt.psps_30m if psp_key == '30m' else
        smt.psps_1h if psp_key == '1h' else smt.psps_2h if psp_key == '2h' else
        smt.psps_4h if psp_key == '4h' else smt.psps_1d if psp_key == '1d' else
        smt.psps_1_week if psp_key == '1_week' else smt.psps_1_month
    )
    for psp in psps:
        if psp.a1_candle[5] == psp_date:
            if smt_type in ['high', 'half_high']:
                return psp.a1_candle[1], psp.a2_candle[1], psp.a3_candle[1]
            else:
                return psp.a1_candle[2], psp.a2_candle[2], psp.a3_candle[2]


def _calculate_rr_and_pos_size(current: float, target: float, stop: float) -> Tuple[float, float, float]:
    reward = abs(target - current)
    risk = abs(current - stop)

    rr = reward / risk
    pos_size_assets = ONE_RR_IN_USD / risk

    return rr, pos_size_assets, pos_size_assets * current


def _close_trade(trade: SmtPspTrade, close_price: float, time_str: str, reason: str) -> SmtPspTrade:
    if trade.direction == 'UP':
        pnl = trade.entry_position_usd / trade.entry_price * (close_price - trade.entry_price)
    else:
        pnl = trade.entry_position_usd / trade.entry_price * (trade.entry_price - close_price)

    trade.pnl_usd = pnl
    trade.closes.append((100, close_price, time_str, reason))

    return trade


def strategy01_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
    my_psp_change = None
    for smt_level, smt_label, smt_type, psp_key, psp_date, change in spc[4]:  # psp_changes
        if change != 'closed' or smt_level != 4 or psp_key not in ['1h', '2h', '4h']:
            continue
        if my_psp_change is None or int(psp_key[0]) > int(my_psp_change[3][0]):
            my_psp_change = (smt_level, smt_label, smt_type, psp_key, psp_date, change in spc[4])
        if (int(psp_key[0]) == int(my_psp_change[3][0]) and
                smt_types_is_long[smt_type] != smt_types_is_long[my_psp_change[2]]):
            my_psp_change = None
    if not my_psp_change:
        return []

    smt_level, smt_label, smt_type, psp_key, psp_date, change = my_psp_change

    my_target: Optional[Target] = None
    targets = tc[2] if smt_types_is_long[smt_type] else tc[3]
    for target_level, direction, label, tp_a1, tp_a2, tp_a3 in targets:
        if target_level != 5:
            continue
        if smt_types_is_long[smt_type]:
            if direction != "high":
                continue
            if not my_target:
                my_target = (target_level, direction, label, tp_a1, tp_a2, tp_a3)
                continue
            if tp_a1[0] < my_target[3][0]:  # we choose more conservative target
                my_target = (target_level, direction, label, tp_a1, tp_a2, tp_a3)
                continue
        else:
            if direction != "low":
                continue
            if not my_target:
                my_target = (target_level, direction, label, tp_a1, tp_a2, tp_a3)
                continue
            if tp_a1[0] > my_target[3][0]:  # we choose more conservative target
                my_target = (target_level, direction, label, tp_a1, tp_a2, tp_a3)
                continue
    if not my_target:
        return []

    stop_a1, stop_a2, stop_a3 = _psp_extremums(
        [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
        smt_type, psp_key, psp_date)

    if (tr.a1.prev_15m_candle[3] == my_target[3][0] or tr.a1.prev_15m_candle[3] == stop_a1 or
            tr.a2.prev_15m_candle[3] == my_target[4][0] or tr.a2.prev_15m_candle[3] == stop_a2 or
            tr.a3.prev_15m_candle[3] == my_target[5][0] or tr.a3.prev_15m_candle[3] == stop_a3):
        return []

    rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_calculate_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[3][0], stop_a1),
                                       _calculate_rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[4][0], stop_a2),
                                       _calculate_rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[5][0], stop_a3))

    max_rr_smb, max_rr = "", 0
    for symbol, rr in [(tr.a1.symbol, rr_pos_a1[0]), (tr.a2.symbol, rr_pos_a2[0]), (tr.a3.symbol, rr_pos_a3[0])]:
        if rr < 1.5:
            return []
        if rr > max_rr:
            max_rr_smb, max_rr = symbol, rr

    max_good_tos_smb, max_good_tos_ratio = "", 0
    for symbol, asset_tos in [(tr.a1.symbol, tos[0]), (tr.a2.symbol, tos[1]), (tr.a3.symbol, tos[2])]:
        tos_above, tos_below = 0, 0
        for _, price, _ in asset_tos:
            if price > tr.a1.prev_15m_candle[3]:
                tos_above += 1
            elif price < tr.a1.prev_15m_candle[3]:
                tos_below += 1

        tos_ratio = 1
        if smt_types_is_long[smt_type]:
            if tos_below > 0:
                tos_ratio = tos_above / tos_below
        else:
            if tos_above > 0:
                tos_ratio = tos_below / tos_above

        if tos_ratio < 0.5:
            return []
        if tos_ratio > max_good_tos_ratio:
            max_good_tos_smb, max_good_tos_ratio = symbol, tos_ratio
        if tos_ratio == max_good_tos_ratio and max_good_tos_smb != max_rr_smb:
            max_good_tos_smb, max_good_tos_ratio = symbol, tos_ratio

    reason = f"{psp_key} psp for {smt_type} {smt_label} -> {my_target[1]} {my_target[2]}"
    match max_good_tos_smb:
        case tr.a1.symbol:
            return [SmtPspTrade(
                asset=max_good_tos_smb,
                entry_time=tr.a1.snapshot_date_readable,
                entry_price=tr.a1.prev_15m_candle[3],
                entry_position_assets=rr_pos_a1[1],
                entry_position_usd=rr_pos_a1[2],
                entry_rr=rr_pos_a1[0],
                entry_reason=reason,
                direction="UP" if smt_types_is_long[smt_type] else "DOWN",
                initial_stop=stop_a1,
                stop=stop_a1,
                psp_extremums=(stop_a1, stop_a2, stop_a3),
                deadline_close="",
                initial_take_profit=my_target[3][0],
                take_profit=my_target[3][0],
                initial_targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
                targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
                closes=[],
                pnl_usd=0
            )]
        case tr.a2.symbol:
            return [SmtPspTrade(
                asset=max_good_tos_smb,
                entry_time=tr.a2.snapshot_date_readable,
                entry_price=tr.a2.prev_15m_candle[3],
                entry_position_assets=rr_pos_a2[1],
                entry_position_usd=rr_pos_a2[2],
                entry_rr=rr_pos_a2[0],
                entry_reason=reason,
                direction="UP" if smt_types_is_long[smt_type] else "DOWN",
                initial_stop=stop_a2,
                stop=stop_a2,
                psp_extremums=(stop_a1, stop_a2, stop_a3),
                deadline_close="",
                initial_take_profit=my_target[4][0],
                take_profit=my_target[4][0],
                initial_targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
                targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
                closes=[],
                pnl_usd=0
            )]
        case tr.a3.symbol:
            return [SmtPspTrade(
                asset=max_good_tos_smb,
                entry_time=tr.a3.snapshot_date_readable,
                entry_price=tr.a3.prev_15m_candle[3],
                entry_position_assets=rr_pos_a3[1],
                entry_position_usd=rr_pos_a3[2],
                entry_rr=rr_pos_a3[0],
                entry_reason=reason,
                direction="UP" if smt_types_is_long[smt_type] else "DOWN",
                initial_stop=stop_a3,
                stop=stop_a3,
                psp_extremums=(stop_a1, stop_a2, stop_a3),
                deadline_close="",
                initial_take_profit=my_target[5][0],
                take_profit=my_target[5][0],
                initial_targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
                targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
                closes=[],
                pnl_usd=0
            )]


def strategy01_th(
        stop_after: str, active_trades: List[SmtPspTrade], tr: Triad,
        tos: TrueOpens, spc: SmtPspChange, tc: TargetChange
) -> Tuple[List[SmtPspTrade], List[SmtPspTrade]]:
    closed_trades = {}
    for at in active_trades:
        key = f"{at.asset}_{at.entry_time}_{at.entry_price}_{at.direction}"
        assets = [tr.a1, tr.a2, tr.a3]
        assets_d = {asset.symbol: asset for asset in assets}
        trade_asset = assets_d[at.asset]
        if to_utc_datetime(trade_asset.snapshot_date_readable) >= to_utc_datetime(stop_after):
            closed_trades[key] = _close_trade(
                at, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "strategy_stop")
            continue
        if (at.deadline_close and
                to_utc_datetime(trade_asset.snapshot_date_readable) >= to_utc_datetime(at.deadline_close)):
            closed_trades[key] = _close_trade(
                at, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "deadline")
            continue
        if at.direction == 'UP':
            if trade_asset.prev_15m_candle[2] <= at.stop:
                closed_trades[key] = _close_trade(at, at.stop, trade_asset.snapshot_date_readable, "stop")
                continue
            if trade_asset.prev_15m_candle[1] >= at.take_profit:
                closed_trades[key] = _close_trade(
                    at, at.take_profit, trade_asset.snapshot_date_readable, "take_profit")

                continue
        if at.direction == 'DOWN':
            if trade_asset.prev_15m_candle[1] >= at.stop:
                closed_trades[key] = _close_trade(at, at.stop, trade_asset.snapshot_date_readable, "stop")
                continue
            if trade_asset.prev_15m_candle[2] <= at.take_profit:
                closed_trades[key] = _close_trade(
                    at, at.take_profit, trade_asset.snapshot_date_readable, "take_profit")
                continue

        for asset, extremum in [(assets[i], x) for i, x in enumerate(at.psp_extremums)]:
            if asset.prev_15m_candle[2] <= extremum <= asset.prev_15m_candle[1]:
                closed_trades[key] = _close_trade(
                    at, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable,
                    f"stop because {asset.symbol} swept PSP"
                )
            continue

        for asset, target in [(assets[i], x) for i, x in enumerate(at.targets)]:
            if asset.prev_15m_candle[2] <= target <= asset.prev_15m_candle[1]:
                closed_trades[key] = _close_trade(
                    at, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable,
                    f"close because {asset.symbol} reached target")
            continue

    return (
        [x for x in active_trades if f"{x.asset}_{x.entry_time}_{x.entry_price}_{x.direction}" not in closed_trades],
        [closed_trades[x] for x in closed_trades.keys()]
    )


# триггер - закрытие 1h/2h/4h PSP в рамках недельного SMT
# цель - ближайший неснятый high/low азии/лондона и пр (не half)
# вход по маркету от точки закрытия prev 15m свечи
# to во всех 3 активах мы ниже/выше больше половины из тру опенов
# минимальный RR (в любом из 3 активов) - 1/1.5
# выбор актива из трёх: сначала по to, потом по RR
# закрытие до стопа: любой из 3 снял наш PSP
# закрытие до тейка: любой из 3 дошёл до нашей цели
strategy01_wq_smt_conservative = SmtPspStrategy(
    name="01. Closed Week PSP trigger - conservative dq target - conservative - pre-take - medium TO filter",
    trade_opener=strategy01_to,
    trades_handler=strategy01_th
)
