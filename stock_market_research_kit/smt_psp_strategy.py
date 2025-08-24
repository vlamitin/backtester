from dataclasses import dataclass
from typing import TypeAlias, Callable, List, Tuple, Optional, Dict

from stock_market_research_kit.smt_psp_trade import SmtPspTrade
from stock_market_research_kit.triad import Triad, SMTLevels, SMT, Target, TrueOpen
from utils.date_utils import to_utc_datetime, to_ny_datetime, to_date_str, to_ny_date_str

ONE_RR_IN_USD = 100
MARKET_ORDER_FEE_PERCENT = 0.55  # bybit default

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


def _rr_and_pos_size(current: float, target: float, stop: float) -> Tuple[float, float, float]:
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
    trade.close_position_fee = close_price * trade.entry_position_assets * MARKET_ORDER_FEE_PERCENT / 100
    trade.closes.append((100, close_price, time_str, to_ny_date_str(time_str), reason))

    return trade


def _filter_by_psp_change(
        smt_level_filter: int,
        psp_change: List[Tuple[int, str, str, str, str, str]],
        psp_change_filter: List[str],  # 'closed'|'confirmed' etc
        psp_key_filter: List[str]  #
) -> Optional[Tuple[int, str, str, str, str, str]]:
    my_psp_change = None
    for smt_level, smt_label, smt_type, psp_key, psp_date, change in psp_change:  # psp_changes
        if change not in psp_change_filter or smt_level != smt_level_filter or psp_key not in psp_key_filter:
            continue
        if my_psp_change is None or int(psp_key[0]) > int(my_psp_change[3][0]):
            my_psp_change = (smt_level, smt_label, smt_type, psp_key, psp_date, change)
        if (int(psp_key[0]) == int(my_psp_change[3][0]) and
                smt_types_is_long[smt_type] != smt_types_is_long[my_psp_change[2]]):
            my_psp_change = None

    return my_psp_change


TargetSorter: TypeAlias = Callable[[Target, Target], Target]


def _filter_by_target(
        is_long: bool, tc: TargetChange, target_levels: List[int], ts: TargetSorter
) -> Optional[Target]:
    my_target: Optional[Target] = None
    targets = tc[2] if is_long else tc[3]
    for tl, direction, label, tp_a1, tp_a2, tp_a3 in targets:
        if tl not in target_levels:
            continue
        if is_long:
            if direction != "high":
                continue
            if not my_target:
                my_target = (tl, direction, label, tp_a1, tp_a2, tp_a3)
                continue
        else:
            if direction != "low":
                continue
            if not my_target:
                my_target = (tl, direction, label, tp_a1, tp_a2, tp_a3)
                continue
        my_target = ts(my_target, (tl, direction, label, tp_a1, tp_a2, tp_a3))

    return my_target


def _filter_by_tos_ratio_and_rr(
        tos: TrueOpens,
        symbols: Tuple[str, str, str],
        currents: Tuple[float, float, float],
        rrs: Tuple[float, float, float],
        min_rr_all: float,
        min_tos_ratio_all: float,
        is_long: bool
) -> str:  # returns symbol to trade
    max_rr_smb, max_rr = "", 0
    for symbol, rr in [(symbols[0], rrs[0]), (symbols[1], rrs[1]), (symbols[2], rrs[2])]:
        if rr < min_rr_all:
            return ""
        if rr > max_rr:
            max_rr_smb, max_rr = symbol, rr

    max_good_tos_smb, max_good_tos_ratio = "", 0
    for symbol, current, asset_tos in [
        (symbols[0], currents[0], tos[0]), (symbols[1], currents[1], tos[1]), (symbols[2], currents[2], tos[2])
    ]:
        tos_above, tos_below = 0, 0
        for _, to_price, _ in asset_tos:
            if current < to_price:
                tos_above += 1
            elif current > to_price:
                tos_below += 1

        tos_ratio = 1
        if is_long:
            if tos_below > 0:
                tos_ratio = tos_above / tos_below
        else:
            if tos_above > 0:
                tos_ratio = tos_below / tos_above

        if tos_ratio < min_tos_ratio_all:
            return ""
        if tos_ratio > max_good_tos_ratio:
            max_good_tos_smb, max_good_tos_ratio = symbol, tos_ratio
        if tos_ratio == max_good_tos_ratio and max_good_tos_smb != max_rr_smb:
            max_good_tos_smb, max_good_tos_ratio = symbol, tos_ratio

        return max_good_tos_smb


def _filter_by_rr(
        symbols: Tuple[str, str, str],
        rrs: Tuple[float, float, float],
        rr_all_min: float,
        rr_all_max: float,  # в слишком большом RR мб слишком большая комиссия за вход/выход
) -> str:
    max_rr_smb, max_rr = "", 0
    for symbol, rr in [(symbols[0], rrs[0]), (symbols[1], rrs[1]), (symbols[2], rrs[2])]:
        if not rr_all_min < rr < rr_all_max:
            return ""
        if rr > max_rr:
            max_rr_smb, max_rr = symbol, rr

    return symbol


def _filter_by_tos(
        tos: TrueOpens,
        symbols: Tuple[str, str, str],
        currents: Tuple[float, float, float],
        is_long: bool,
        tos_aside_filter_all: List[str],
        tos_aside_filter_my: List[str]
) -> Optional[Dict[str, List[str]]]:  # returns symbols to trade and list of its aside tos
    tos_sides = {}
    for smb in symbols:
        tos_sides[smb] = ([], [])  # aside, inside
    for symbol, current, asset_tos in [
        (symbols[0], currents[0], tos[0]), (symbols[1], currents[1], tos[1]), (symbols[2], currents[2], tos[2])
    ]:
        for to_label, to_price, _ in asset_tos:
            if is_long:
                if current < to_price:
                    tos_sides[symbol][0].append(to_label)
                elif current > to_price:
                    tos_sides[symbol][1].append(to_label)
            else:
                if current > to_price:
                    tos_sides[symbol][0].append(to_label)
                elif current < to_price:
                    tos_sides[symbol][1].append(to_label)

    result = {}
    for smb in tos_sides:
        if not set(tos_aside_filter_all).issubset(tos_sides[smb][0]):
            return None
        if set(tos_aside_filter_my).issubset(tos_sides[smb][0]):
            result[smb] = tos_sides[smb][0]
    if len(result) == 0:
        return None

    return result


def strategy01_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
    my_psp_change = _filter_by_psp_change(4, spc[4], ['closed'], ['1h', '2h', '4h'])
    if not my_psp_change:
        return []
    smt_level, smt_label, smt_type, psp_key, psp_date, change = my_psp_change

    def targets_sorter(prev_t: Target, t: Target) -> Target:
        if smt_types_is_long[smt_type] and t[3][0] < prev_t[3][0]:  # we choose more conservative target
            return t
        elif t[3][0] > prev_t[3][0]:  # we choose more conservative target
            return t
        return prev_t

    my_target = _filter_by_target(smt_types_is_long[smt_type], tc, [5], targets_sorter)
    if not my_target:
        return []

    stop_a1, stop_a2, stop_a3 = _psp_extremums(
        [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
        smt_type, psp_key, psp_date)

    if (tr.a1.prev_15m_candle[3] in [my_target[3][0], stop_a1] or
            tr.a2.prev_15m_candle[3] in [my_target[4][0], stop_a2] or
            tr.a3.prev_15m_candle[3] in [my_target[5][0], stop_a3]):
        return []

    rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[3][0], stop_a1),
                                       _rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[4][0], stop_a2),
                                       _rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[5][0], stop_a3))

    smb_to_trade = _filter_by_tos_ratio_and_rr(
        tos, (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
        (tr.a1.prev_15m_candle[3], tr.a2.prev_15m_candle[3], tr.a3.prev_15m_candle[3]),
        (rr_pos_a1[0], rr_pos_a2[0], rr_pos_a3[0]),
        1.5, 0.5, smt_types_is_long[smt_type]
    )
    if smb_to_trade == "":
        return []

    reason = f"{psp_key} psp for {smt_type} {smt_label} -> {my_target[1]} {my_target[2]}"
    entry_time = tr.a1.snapshot_date_readable
    entry_price = tr.a1.prev_15m_candle[3]
    entry_position_assets = rr_pos_a1[1]
    entry_position_usd = rr_pos_a1[2]
    entry_rr = rr_pos_a1[0]
    stop = stop_a1
    take_profit = my_target[3][0]

    match smb_to_trade:
        case tr.a2.symbol:
            entry_time = tr.a2.snapshot_date_readable
            entry_price = tr.a2.prev_15m_candle[3]
            entry_position_assets = rr_pos_a2[1]
            entry_position_usd = rr_pos_a2[2]
            entry_rr = rr_pos_a2[0]
            stop = stop_a2
            take_profit = my_target[4][0]
        case tr.a3.symbol:
            entry_time = tr.a3.snapshot_date_readable
            entry_price = tr.a3.prev_15m_candle[3]
            entry_position_assets = rr_pos_a3[1]
            entry_position_usd = rr_pos_a3[2]
            entry_rr = rr_pos_a3[0]
            stop = stop_a3
            take_profit = my_target[5][0]

    return [SmtPspTrade(
        asset=smb_to_trade,
        entry_time=entry_time,
        entry_time_ny=to_ny_date_str(entry_time),
        entry_price=entry_price,
        entry_position_assets=entry_position_assets,
        entry_position_usd=entry_position_usd,
        entry_position_fee=entry_position_usd * MARKET_ORDER_FEE_PERCENT / 100,
        entry_rr=entry_rr,
        entry_reason=reason,
        psp_key_used=psp_key,
        direction="UP" if smt_types_is_long[smt_type] else "DOWN",
        stop=stop,
        psp_extremums=(stop_a1, stop_a2, stop_a3),
        deadline_close="",
        take_profit=take_profit,
        targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
        closes=[],
        pnl_usd=0,
        close_position_fee=0
    )]


def strategy02_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
    my_psp_change = _filter_by_psp_change(4, spc[4], ['closed'], ['1h', '2h', '4h'])
    if not my_psp_change:
        return []
    smt_level, smt_label, smt_type, psp_key, psp_date, change = my_psp_change

    def targets_sorter(prev_t: Target, t: Target) -> Target:
        if smt_types_is_long[smt_type] and t[3][0] > prev_t[3][0]:  # we choose most challenging target
            return t
        elif t[3][0] < prev_t[3][0]:  # we choose most challenging target
            return t
        return prev_t

    my_target = _filter_by_target(smt_types_is_long[smt_type], tc, [5], targets_sorter)
    if not my_target:
        return []

    stop_a1, stop_a2, stop_a3 = _psp_extremums(
        [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
        smt_type, psp_key, psp_date)

    if (tr.a1.prev_15m_candle[3] in [my_target[3][0], stop_a1] or
            tr.a2.prev_15m_candle[3] in [my_target[4][0], stop_a2] or
            tr.a3.prev_15m_candle[3] in [my_target[5][0], stop_a3]):
        return []

    rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[3][0], stop_a1),
                                       _rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[4][0], stop_a2),
                                       _rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[5][0], stop_a3))

    smb_to_trade = _filter_by_tos_ratio_and_rr(
        tos, (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
        (tr.a1.prev_15m_candle[3], tr.a2.prev_15m_candle[3], tr.a3.prev_15m_candle[3]),
        (rr_pos_a1[0], rr_pos_a2[0], rr_pos_a3[0]),
        1.5, 0.5, smt_types_is_long[smt_type]
    )
    if smb_to_trade == "":
        return []

    reason = f"{psp_key} psp for {smt_type} {smt_label} -> {my_target[1]} {my_target[2]}"
    entry_time = tr.a1.snapshot_date_readable
    entry_price = tr.a1.prev_15m_candle[3]
    entry_position_assets = rr_pos_a1[1]
    entry_position_usd = rr_pos_a1[2]
    entry_rr = rr_pos_a1[0]
    stop = stop_a1
    take_profit = my_target[3][0]

    match smb_to_trade:
        case tr.a2.symbol:
            entry_time = tr.a2.snapshot_date_readable
            entry_price = tr.a2.prev_15m_candle[3]
            entry_position_assets = rr_pos_a2[1]
            entry_position_usd = rr_pos_a2[2]
            entry_rr = rr_pos_a2[0]
            stop = stop_a2
            take_profit = my_target[4][0]
        case tr.a3.symbol:
            entry_time = tr.a3.snapshot_date_readable
            entry_price = tr.a3.prev_15m_candle[3]
            entry_position_assets = rr_pos_a3[1]
            entry_position_usd = rr_pos_a3[2]
            entry_rr = rr_pos_a3[0]
            stop = stop_a3
            take_profit = my_target[5][0]

    return [SmtPspTrade(
        asset=smb_to_trade,
        entry_time=entry_time,
        entry_time_ny=to_ny_date_str(entry_time),
        entry_price=entry_price,
        entry_position_assets=entry_position_assets,
        entry_position_usd=entry_position_usd,
        entry_position_fee=entry_position_usd * MARKET_ORDER_FEE_PERCENT / 100,
        entry_rr=entry_rr,
        entry_reason=reason,
        psp_key_used=psp_key,
        direction="UP" if smt_types_is_long[smt_type] else "DOWN",
        stop=stop,
        psp_extremums=(stop_a1, stop_a2, stop_a3),
        deadline_close="",
        take_profit=take_profit,
        targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
        closes=[],
        pnl_usd=0,
        close_position_fee=0
    )]


def strategy03_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
    my_psp_change = _filter_by_psp_change(4, spc[4], ['closed'], ['1h', '2h', '4h'])
    if not my_psp_change:
        return []
    smt_level, smt_label, smt_type, psp_key, psp_date, change = my_psp_change

    def targets_sorter(prev_t: Target, t: Target) -> Target:
        if smt_types_is_long[smt_type] and t[3][0] < prev_t[3][0]:  # we choose more conservative target
            return t
        elif t[3][0] > prev_t[3][0]:  # we choose more conservative target
            return t
        return prev_t

    my_target = _filter_by_target(smt_types_is_long[smt_type], tc, [5], targets_sorter)
    if not my_target:
        return []

    stop_a1, stop_a2, stop_a3 = _psp_extremums(
        [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
        smt_type, psp_key, psp_date)

    if (tr.a1.prev_15m_candle[3] in [my_target[3][0], stop_a1] or
            tr.a2.prev_15m_candle[3] in [my_target[4][0], stop_a2] or
            tr.a3.prev_15m_candle[3] in [my_target[5][0], stop_a3]):
        return []

    rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[3][0], stop_a1),
                                       _rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[4][0], stop_a2),
                                       _rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[5][0], stop_a3))

    smb_to_trade = _filter_by_tos_ratio_and_rr(
        tos, (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
        (tr.a1.prev_15m_candle[3], tr.a2.prev_15m_candle[3], tr.a3.prev_15m_candle[3]),
        (rr_pos_a1[0], rr_pos_a2[0], rr_pos_a3[0]),
        1.5, 0.5, smt_types_is_long[smt_type]
    )
    if smb_to_trade == "":
        return []

    reason = f"{psp_key} psp for {smt_type} {smt_label} -> {my_target[1]} {my_target[2]}"
    entry_time = tr.a1.snapshot_date_readable
    entry_price = tr.a1.prev_15m_candle[3]
    entry_position_assets = rr_pos_a1[1]
    entry_position_usd = rr_pos_a1[2]
    entry_rr = rr_pos_a1[0]
    stop = stop_a1
    take_profit = my_target[3][0]

    trades = []
    for smb in (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol):
        match smb:
            case tr.a2.symbol:
                entry_time = tr.a2.snapshot_date_readable
                entry_price = tr.a2.prev_15m_candle[3]
                entry_position_assets = rr_pos_a2[1]
                entry_position_usd = rr_pos_a2[2]
                entry_rr = rr_pos_a2[0]
                stop = stop_a2
                take_profit = my_target[4][0]
            case tr.a3.symbol:
                entry_time = tr.a3.snapshot_date_readable
                entry_price = tr.a3.prev_15m_candle[3]
                entry_position_assets = rr_pos_a3[1]
                entry_position_usd = rr_pos_a3[2]
                entry_rr = rr_pos_a3[0]
                stop = stop_a3
                take_profit = my_target[5][0]

        trades.append(SmtPspTrade(
            asset=smb_to_trade,
            entry_time=entry_time,
            entry_time_ny=to_ny_date_str(entry_time),
            entry_price=entry_price,
            entry_position_assets=entry_position_assets,
            entry_position_usd=entry_position_usd,
            entry_position_fee=entry_position_usd * MARKET_ORDER_FEE_PERCENT / 100,
            entry_rr=entry_rr,
            entry_reason=reason,
            psp_key_used=psp_key,
            direction="UP" if smt_types_is_long[smt_type] else "DOWN",
            stop=stop,
            psp_extremums=(stop_a1, stop_a2, stop_a3),
            deadline_close="",
            take_profit=take_profit,
            targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
            closes=[],
            pnl_usd=0,
            close_position_fee=0
        ))
    return trades


def strategy04_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
    my_psp_change = _filter_by_psp_change(4, spc[4], ['closed'], ['1h', '2h', '4h'])
    if not my_psp_change:
        return []
    smt_level, smt_label, smt_type, psp_key, psp_date, change = my_psp_change

    def targets_sorter(prev_t: Target, t: Target) -> Target:
        if smt_types_is_long[smt_type] and t[3][0] < prev_t[3][0]:  # we choose more conservative target
            return t
        elif t[3][0] > prev_t[3][0]:  # we choose more conservative target
            return t
        return prev_t

    my_target = _filter_by_target(smt_types_is_long[smt_type], tc, [5], targets_sorter)
    if not my_target:
        return []

    stop_a1, stop_a2, stop_a3 = _psp_extremums(
        [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
        smt_type, psp_key, psp_date)

    if (tr.a1.prev_15m_candle[3] in [my_target[3][0], stop_a1] or
            tr.a2.prev_15m_candle[3] in [my_target[4][0], stop_a2] or
            tr.a3.prev_15m_candle[3] in [my_target[5][0], stop_a3]):
        return []

    rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[3][0], stop_a1),
                                       _rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[4][0], stop_a2),
                                       _rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[5][0], stop_a3))

    symbols_to_trade_d = _filter_by_tos(
        tos, (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
        (tr.a1.prev_15m_candle[3], tr.a2.prev_15m_candle[3], tr.a3.prev_15m_candle[3]),
        smt_types_is_long[smt_type],
        ['two', 'tdo'],
        ['two', 'tdo', 't90mo'],
    )
    if not symbols_to_trade_d:
        return []
    symbol_max_rr = _filter_by_rr(
        (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
        (rr_pos_a1[0], rr_pos_a2[0], rr_pos_a3[0]),
        3.2, 80
    )
    if symbol_max_rr == "" or symbol_max_rr not in symbols_to_trade_d:
        return []

    reason = f"{psp_key} psp for {smt_type} {smt_label} -> {my_target[1]} {my_target[2]}, aside tos: {','.join(symbols_to_trade_d[symbol_max_rr])}"
    entry_time = tr.a1.snapshot_date_readable
    entry_price = tr.a1.prev_15m_candle[3]
    entry_position_assets = rr_pos_a1[1]
    entry_position_usd = rr_pos_a1[2]
    entry_rr = rr_pos_a1[0]
    stop = stop_a1
    take_profit = my_target[3][0]

    match symbol_max_rr:
        case tr.a2.symbol:
            entry_time = tr.a2.snapshot_date_readable
            entry_price = tr.a2.prev_15m_candle[3]
            entry_position_assets = rr_pos_a2[1]
            entry_position_usd = rr_pos_a2[2]
            entry_rr = rr_pos_a2[0]
            stop = stop_a2
            take_profit = my_target[4][0]
        case tr.a3.symbol:
            entry_time = tr.a3.snapshot_date_readable
            entry_price = tr.a3.prev_15m_candle[3]
            entry_position_assets = rr_pos_a3[1]
            entry_position_usd = rr_pos_a3[2]
            entry_rr = rr_pos_a3[0]
            stop = stop_a3
            take_profit = my_target[5][0]

    return [SmtPspTrade(
        asset=symbol_max_rr,
        entry_time=entry_time,
        entry_time_ny=to_ny_date_str(entry_time),
        entry_price=entry_price,
        entry_position_assets=entry_position_assets,
        entry_position_usd=entry_position_usd,
        entry_position_fee=entry_position_usd * MARKET_ORDER_FEE_PERCENT / 100,
        entry_rr=entry_rr,
        entry_reason=reason,
        psp_key_used=psp_key,
        direction="UP" if smt_types_is_long[smt_type] else "DOWN",
        stop=stop,
        psp_extremums=(stop_a1, stop_a2, stop_a3),
        deadline_close="",
        take_profit=take_profit,
        targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
        closes=[],
        pnl_usd=0,
        close_position_fee=0
    )]


def strategy05_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
    my_psp_change = _filter_by_psp_change(4, spc[4], ['closed'], ['1h', '2h', '4h'])
    if not my_psp_change:
        return []
    smt_level, smt_label, smt_type, psp_key, psp_date, change = my_psp_change

    def targets_sorter(prev_t: Target, t: Target) -> Target:
        if smt_types_is_long[smt_type] and t[3][0] > prev_t[3][0]:  # we choose less conservative target
            return t
        elif t[3][0] < prev_t[3][0]:  # we choose less conservative target
            return t
        return prev_t

    my_target = _filter_by_target(smt_types_is_long[smt_type], tc, [5], targets_sorter)
    if not my_target:
        return []

    stop_a1, stop_a2, stop_a3 = _psp_extremums(
        [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
        smt_type, psp_key, psp_date)

    if (tr.a1.prev_15m_candle[3] in [my_target[3][0], stop_a1] or
            tr.a2.prev_15m_candle[3] in [my_target[4][0], stop_a2] or
            tr.a3.prev_15m_candle[3] in [my_target[5][0], stop_a3]):
        return []

    rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[3][0], stop_a1),
                                       _rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[4][0], stop_a2),
                                       _rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[5][0], stop_a3))

    smb_to_trade = _filter_by_tos_ratio_and_rr(
        tos, (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
        (tr.a1.prev_15m_candle[3], tr.a2.prev_15m_candle[3], tr.a3.prev_15m_candle[3]),
        (rr_pos_a1[0], rr_pos_a2[0], rr_pos_a3[0]),
        1.5, 0.5, smt_types_is_long[smt_type]
    )
    if smb_to_trade == "":
        return []

    reason = f"{psp_key} psp for {smt_type} {smt_label} -> {my_target[1]} {my_target[2]}"
    entry_time = tr.a1.snapshot_date_readable
    entry_price = tr.a1.prev_15m_candle[3]
    entry_position_assets = rr_pos_a1[1]
    entry_position_usd = rr_pos_a1[2]
    entry_rr = rr_pos_a1[0]
    stop = stop_a1
    take_profit = my_target[3][0]

    trades = []
    for smb in (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol):
        match smb:
            case tr.a2.symbol:
                entry_time = tr.a2.snapshot_date_readable
                entry_price = tr.a2.prev_15m_candle[3]
                entry_position_assets = rr_pos_a2[1]
                entry_position_usd = rr_pos_a2[2]
                entry_rr = rr_pos_a2[0]
                stop = stop_a2
                take_profit = my_target[4][0]
            case tr.a3.symbol:
                entry_time = tr.a3.snapshot_date_readable
                entry_price = tr.a3.prev_15m_candle[3]
                entry_position_assets = rr_pos_a3[1]
                entry_position_usd = rr_pos_a3[2]
                entry_rr = rr_pos_a3[0]
                stop = stop_a3
                take_profit = my_target[5][0]

        trades.append(SmtPspTrade(
            asset=smb_to_trade,
            entry_time=entry_time,
            entry_time_ny=to_ny_date_str(entry_time),
            entry_price=entry_price,
            entry_position_assets=entry_position_assets,
            entry_position_usd=entry_position_usd,
            entry_position_fee=entry_position_usd * MARKET_ORDER_FEE_PERCENT / 100,
            entry_rr=entry_rr,
            entry_reason=reason,
            psp_key_used=psp_key,
            direction="UP" if smt_types_is_long[smt_type] else "DOWN",
            stop=stop,
            psp_extremums=(stop_a1, stop_a2, stop_a3),
            deadline_close="",
            take_profit=take_profit,
            targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
            closes=[],
            pnl_usd=0,
            close_position_fee=0
        ))
    return trades


def strategy06_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
    my_psp_change = _filter_by_psp_change(4, spc[4], ['closed'], ['1h', '2h', '4h'])
    if not my_psp_change:
        return []
    smt_level, smt_label, smt_type, psp_key, psp_date, change = my_psp_change

    def targets_sorter(prev_t: Target, t: Target) -> Target:
        if smt_types_is_long[smt_type] and t[3][0] < prev_t[3][0]:  # we choose more conservative target
            return t
        elif t[3][0] > prev_t[3][0]:  # we choose more conservative target
            return t
        return prev_t

    my_target = _filter_by_target(smt_types_is_long[smt_type], tc, [5], targets_sorter)
    if not my_target:
        return []

    stop_a1, stop_a2, stop_a3 = _psp_extremums(
        [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
        smt_type, psp_key, psp_date)

    if (tr.a1.prev_15m_candle[3] in [my_target[3][0], stop_a1] or
            tr.a2.prev_15m_candle[3] in [my_target[4][0], stop_a2] or
            tr.a3.prev_15m_candle[3] in [my_target[5][0], stop_a3]):
        return []

    rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[3][0], stop_a1),
                                       _rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[4][0], stop_a2),
                                       _rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[5][0], stop_a3))

    symbols_to_trade_d = _filter_by_tos(
        tos, (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
        (tr.a1.prev_15m_candle[3], tr.a2.prev_15m_candle[3], tr.a3.prev_15m_candle[3]),
        smt_types_is_long[smt_type],
        [],
        ['two', 'tdo', 't90mo'],
    )
    if not symbols_to_trade_d:
        return []
    symbol_max_rr = _filter_by_rr(
        (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
        (rr_pos_a1[0], rr_pos_a2[0], rr_pos_a3[0]),
        3.2, 80
    )
    if symbol_max_rr == "" or symbol_max_rr not in symbols_to_trade_d:
        return []

    reason = f"{psp_key} psp for {smt_type} {smt_label} -> {my_target[1]} {my_target[2]}, aside tos: {','.join(symbols_to_trade_d[symbol_max_rr])}"
    entry_time = tr.a1.snapshot_date_readable
    entry_price = tr.a1.prev_15m_candle[3]
    entry_position_assets = rr_pos_a1[1]
    entry_position_usd = rr_pos_a1[2]
    entry_rr = rr_pos_a1[0]
    stop = stop_a1
    take_profit = my_target[3][0]

    match symbol_max_rr:
        case tr.a2.symbol:
            entry_time = tr.a2.snapshot_date_readable
            entry_price = tr.a2.prev_15m_candle[3]
            entry_position_assets = rr_pos_a2[1]
            entry_position_usd = rr_pos_a2[2]
            entry_rr = rr_pos_a2[0]
            stop = stop_a2
            take_profit = my_target[4][0]
        case tr.a3.symbol:
            entry_time = tr.a3.snapshot_date_readable
            entry_price = tr.a3.prev_15m_candle[3]
            entry_position_assets = rr_pos_a3[1]
            entry_position_usd = rr_pos_a3[2]
            entry_rr = rr_pos_a3[0]
            stop = stop_a3
            take_profit = my_target[5][0]

    return [SmtPspTrade(
        asset=symbol_max_rr,
        entry_time=entry_time,
        entry_time_ny=to_ny_date_str(entry_time),
        entry_price=entry_price,
        entry_position_assets=entry_position_assets,
        entry_position_usd=entry_position_usd,
        entry_position_fee=entry_position_usd * MARKET_ORDER_FEE_PERCENT / 100,
        entry_rr=entry_rr,
        entry_reason=reason,
        psp_key_used=psp_key,
        direction="UP" if smt_types_is_long[smt_type] else "DOWN",
        stop=stop,
        psp_extremums=(stop_a1, stop_a2, stop_a3),
        deadline_close="",
        take_profit=take_profit,
        targets=(my_target[3][0], my_target[4][0], my_target[5][0]),
        closes=[],
        pnl_usd=0,
        close_position_fee=0
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

        continue_outer = False
        for asset, extremum in [(assets[i], x) for i, x in enumerate(at.psp_extremums)]:
            if asset.prev_15m_candle[2] <= extremum <= asset.prev_15m_candle[1]:
                closed_trades[key] = _close_trade(
                    at, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable,
                    f"stop because {asset.symbol} swept PSP"
                )
                continue_outer = True
                break
        if continue_outer:
            continue

        for asset, target in [(assets[i], x) for i, x in enumerate(at.targets)]:
            if asset.prev_15m_candle[2] <= target <= asset.prev_15m_candle[1]:
                closed_trades[key] = _close_trade(
                    at, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable,
                    f"close because {asset.symbol} reached target")
                continue_outer = True
                break
        if continue_outer:
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
    name="01. Closed PSP trigger in weekly SMT - conservative dq target - conservative - pre-take - medium TO filter",
    trade_opener=strategy01_to,
    trades_handler=strategy01_th
)

# всё как в strategy01, но если есть 2-3 неснятых high/low дня, то берём который с большим RR
strategy02_wq_smt_moderate_conservative = SmtPspStrategy(
    name="02. Closed PSP trigger in weekly SMT - moderate dq target - conservative - pre-take - medium TO filter",
    trade_opener=strategy02_to,
    trades_handler=strategy01_th
)

# триггер цель и вход как в strategy01
# отсутствие rr и smt фильтров, входим во все 3 актива
strategy03_wq_smt_conservative_no_filters = SmtPspStrategy(
    name="03. Closed PSP trigger in weekly SMT - conservative dq target - conservative - pre-take - no TO or RR filter",
    trade_opener=strategy03_to,
    trades_handler=strategy01_th
)

# триггер цель и вход как в strategy01
# RR от 3.2 до 80, фильтр на aside two tdo и t90mo
strategy04_wq_smt_conservative_moderate_rr_to_filters = SmtPspStrategy(
    name="04. Closed PSP trigger in weekly SMT - conservative dq target - conservative - pre-take - moderate all TO and RR filter",
    trade_opener=strategy04_to,
    trades_handler=strategy01_th
)

# всё как в strategy01, но цели как в strategy02
strategy05_wq_smt_conservative_moderate_no_filters = SmtPspStrategy(
    name="05. Closed PSP trigger in weekly SMT - mod conservative dq target - conservative - pre-take - no TO or RR filter",
    trade_opener=strategy05_to,
    trades_handler=strategy01_th
)

# всё как в strategy04
# RR от 3.2 до 80, фильтр на aside two tdo и t90mo только в выбранном активе, а не во всех
strategy06_wq_smt_conservative_moderate_rr_to_filters = SmtPspStrategy(
    name="06. Closed PSP trigger in weekly SMT - conservative dq target - conservative - pre-take - moderate asset TO and RR filter",
    trade_opener=strategy06_to,
    trades_handler=strategy01_th
)
