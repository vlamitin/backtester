from dataclasses import dataclass
from types import MethodType
from typing import TypeAlias, Callable, List, Tuple, Optional, Dict

from stock_market_research_kit.asset import Asset
from stock_market_research_kit.candle import as_1_candle
from stock_market_research_kit.quarter import DayQuarter
from stock_market_research_kit.smt_psp_trade import SmtPspTrade, ONE_RR_IN_USD, MAX_ENTRY, MARKET_ORDER_FEE_PERCENT, \
    LIMIT_ORDER_FEE_PERCENT
from stock_market_research_kit.triad import Triad, SMTLevels, SMT, Target, TrueOpen, to_smt_flags, percent_from_current
from utils.date_utils import to_utc_datetime, to_ny_datetime, to_date_str, to_ny_date_str, quarters_by_time

TrueOpens: TypeAlias = Tuple[List[TrueOpen], List[TrueOpen], List[TrueOpen]]
SmtPspChange: TypeAlias = Tuple[
    List[Tuple[int, str, Optional[SMTLevels]]],  # old (level, label, smt levels)
    List[Tuple[int, str, Optional[SMTLevels]]],  # new (level, label, smt levels)
    List[Tuple[int, str, SMT]],  # new: key, SMT
    List[Tuple[int, str, SMT]],  # cancelled: key, SMT
    List[Tuple[int, str, str, str, str, str, str, str]]
    # smt_level, smt_key, smt_type, smt_flags, smt_first_appeared, psp_key, psp_date, possible|closed|confirmed|swept
]
TargetChange: TypeAlias = Tuple[
    List[Target],  # old long
    List[Target],  # old short
    List[Target],  # all new long targets
    List[Target],  # all new short targets
    List[Tuple[
        int, str, str, str, int, float]],  # reached long: level, direction, target_label, ql_start, asset_index, price
    List[Tuple[
        int, str, str, str, int, float]],  # reached short: level, direction, target_label, ql_start, asset_index, price
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
    pos_size_usd = pos_size_assets * current
    if pos_size_usd > MAX_ENTRY:
        pos_size_usd = MAX_ENTRY
        pos_size_assets = pos_size_usd / current

    return rr, pos_size_assets, pos_size_usd


def _price_and_pos_size(rr: float, target: float, stop: float) -> Tuple[float, float, float]:
    price = (target + rr * stop) / (1 + rr)
    risk = abs(price - stop)

    pos_size_assets = ONE_RR_IN_USD / risk
    pos_size_usd = pos_size_assets * price
    if pos_size_usd > MAX_ENTRY:
        pos_size_usd = MAX_ENTRY
        pos_size_assets = pos_size_usd / price

    return price, pos_size_assets, pos_size_usd


def _close_trade(trade: SmtPspTrade, close_price: float, time_str: str, reason: str, close_half: bool) -> SmtPspTrade:
    percent_to_close = 100 - trade.percent_closed() if not close_half else (100 - trade.percent_closed()) / 2
    part_to_close = percent_to_close / 100
    trade.closes.append((percent_to_close, close_price, time_str, to_ny_date_str(time_str), reason))
    trade.close_position_fee += part_to_close * (close_price * trade.entry_position_assets *
                                                 MARKET_ORDER_FEE_PERCENT / 100)

    if trade.direction == 'UP':
        pnl = part_to_close * trade.entry_position_assets * (close_price - trade.entry_price)
    else:
        pnl = part_to_close * trade.entry_position_assets * (trade.entry_price - close_price)
    trade.pnl_usd += pnl

    return trade


def _cancel_order(alo: SmtPspTrade, close_price: float, time_str: str, reason: str) -> SmtPspTrade:
    alo.limit_status = "CANCELLED"
    alo.closes.append((100, close_price, time_str, to_ny_date_str(time_str), reason))

    return alo


def _filter_by_psp_change(
        smt_level_filter: int,
        smt_type_filter: List[str],
        psp_change: List[Tuple[int, str, str, str, str, str, str, str]],
        psp_change_filter: List[str],  # 'closed'|'confirmed' etc
        psp_key_filter: List[str]  #
) -> Optional[Tuple[int, str, str, str, str, str, str, str]]:
    my_psp_change = None
    for smt_level, smt_label, smt_type, smt_flags, smt_first_appeared, psp_key, psp_date, change in psp_change:
        if (change not in psp_change_filter or smt_level != smt_level_filter
                or psp_key not in psp_key_filter or smt_type not in smt_type_filter):
            continue
        if my_psp_change is None or int(psp_key[0]) > int(my_psp_change[4][0]):
            my_psp_change = (smt_level, smt_label, smt_type, smt_flags, smt_first_appeared, psp_key, psp_date, change)
        if (int(psp_key[0]) == int(my_psp_change[4][0]) and
                smt_types_is_long[smt_type] != smt_types_is_long[my_psp_change[2]]):
            my_psp_change = None

    return my_psp_change


TargetSorter: TypeAlias = Callable[[Target, Target], Target]


def _closest_targets_sorter(is_long: bool) -> TargetSorter:
    def targets_sorter(prev_t: Target, t: Target) -> Target:
        if is_long:
            if t[4][0] < prev_t[4][0]:  # we choose more conservative target
                return t
            return prev_t
        if t[4][0] > prev_t[4][0]:  # we choose more conservative target
            return t
        return prev_t

    return targets_sorter


def _furthest_targets_sorter(is_long: bool) -> TargetSorter:
    def targets_sorter(prev_t: Target, t: Target) -> Target:
        if is_long:
            if t[4][0] > prev_t[4][0]:  # we choose less conservative target
                return t
            return prev_t
        if t[4][0] < prev_t[4][0]:  # we choose less conservative target
            return t
        return prev_t

    return targets_sorter


def _filter_by_target(
        is_long: bool, allow_half: bool, tc: TargetChange, target_levels: List[int], ts: TargetSorter
) -> Optional[Target]:
    long_directions = ["high", "half_high"] if allow_half else ["high"]
    short_directions = ["low", "half_low"] if allow_half else ["low"]
    my_target: Optional[Target] = None
    targets = tc[2] if is_long else tc[3]
    for tl, direction, label, ql_start, tp_a1, tp_a2, tp_a3 in targets:
        if tl not in target_levels:
            continue
        if is_long:
            if direction not in long_directions:
                continue
            if not my_target:
                my_target = (tl, direction, label, ql_start, tp_a1, tp_a2, tp_a3)
                continue
        else:
            if direction not in short_directions:
                continue
            if not my_target:
                my_target = (tl, direction, label, ql_start, tp_a1, tp_a2, tp_a3)
                continue
        my_target = ts(my_target, (tl, direction, label, ql_start, tp_a1, tp_a2, tp_a3))

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


def _open_market_trade(
        trade_asset: Asset, rr_pos: Tuple[float, float, float], my_target: Target,
        my_psp_change: Tuple[int, str, str, str, str, str, str, str],
        stop: float, psp_extremums: Tuple[float, float, float], take_profit: float,
        tos: List[TrueOpen]
) -> SmtPspTrade:
    smt_level, smt_label, smt_type, smt_flags, smt_first_appeared, psp_key, psp_date, change = my_psp_change

    reason = f"{psp_key} psp for {smt_type} {smt_label} -> {my_target[1]} {my_target[2]}"
    return SmtPspTrade(
        asset=trade_asset.symbol,
        direction="UP" if my_target[1] in ["high", "half_high"] else "DOWN",
        signal_time=trade_asset.snapshot_date_readable,
        signal_time_ny=to_ny_date_str(trade_asset.snapshot_date_readable),
        limit_price_history=None,
        limit_stop=None,
        limit_take_profit=None,
        limit_rr=None,
        limit_position_assets=None,
        limit_position_usd=None,
        limit_status=None,
        limit_chase_to_label=None,
        limit_chase_rr=None,
        entry_order_type="MARKET",
        entry_price=trade_asset.prev_15m_candle[3],
        stop=stop,
        take_profit=take_profit,
        entry_rr=rr_pos[0],
        entry_time=trade_asset.snapshot_date_readable,
        entry_time_ny=to_ny_date_str(trade_asset.snapshot_date_readable),
        entry_position_assets=rr_pos[1],
        entry_position_usd=rr_pos[2],
        entry_position_fee=rr_pos[2] * MARKET_ORDER_FEE_PERCENT / 100,
        entry_reason=reason,
        entry_tos=tos,
        psp_key_used=psp_key,
        psp_date=psp_date,
        smt_type=smt_type,
        smt_level=smt_level,
        smt_label=smt_label,
        smt_flags=smt_flags,
        smt_first_appeared=smt_first_appeared,
        target_level=my_target[0],
        target_direction=my_target[1],
        target_label=my_target[2],
        target_ql_start=my_target[3],
        best_pnl=0,
        best_pnl_time=trade_asset.snapshot_date_readable,
        best_pnl_time_ny=to_ny_date_str(trade_asset.snapshot_date_readable),
        best_pnl_price=trade_asset.prev_15m_candle[3],
        best_pnl_tos=tos,
        best_entry_time=trade_asset.snapshot_date_readable,
        best_entry_time_ny=to_ny_date_str(trade_asset.snapshot_date_readable),
        best_entry_price=trade_asset.prev_15m_candle[3],
        best_entry_rr=rr_pos[0],
        best_entry_tos=tos,
        psp_extremums=psp_extremums,
        deadline_close="",
        targets=(my_target[4][1], my_target[5][1], my_target[6][1]),
        _in_trade_range=None,
        closes=[],
        pnl_usd=0,
        close_position_fee=0
    )


def _open_limit_trade(
        alo: SmtPspTrade, trade_asset: Asset, tos: List[TrueOpen]
) -> SmtPspTrade:
    alo.limit_status = "FILLED"
    alo.entry_order_type = "LIMIT"
    alo.entry_price = alo.limit_price_history[-1]
    alo.stop = alo.limit_stop
    alo.take_profit = alo.limit_take_profit
    alo.entry_rr = alo.limit_rr
    alo.entry_time = trade_asset.snapshot_date_readable
    alo.entry_time_ny = to_ny_date_str(trade_asset.snapshot_date_readable)
    alo.entry_position_assets = alo.limit_position_assets
    alo.entry_position_usd = alo.limit_position_usd
    alo.entry_position_fee = alo.limit_position_usd * LIMIT_ORDER_FEE_PERCENT / 100
    alo.entry_tos = tos
    alo.best_pnl = 0
    alo.best_pnl_time = trade_asset.snapshot_date_readable
    alo.best_pnl_time_ny = to_ny_date_str(trade_asset.snapshot_date_readable)
    alo.best_pnl_price = alo.limit_price_history[-1]
    alo.best_pnl_tos = tos
    alo.best_entry_time = trade_asset.snapshot_date_readable
    alo.best_entry_time_ny = to_ny_date_str(trade_asset.snapshot_date_readable)
    alo.best_entry_price = alo.limit_price_history[-1]
    alo.best_entry_rr = alo.limit_rr
    alo.best_entry_tos = tos

    return alo


def _to_limit_price(direction: str, to_price: float, stop: float, take_profit: float) -> float:
    to_price = to_price - (to_price * 0.0005)  # 0.05% за True Open
    if direction == "UP":
        if not stop < to_price < take_profit:
            to_price = to_price
    else:
        to_price = to_price + (to_price * 0.0005)  # 0.05% за True Open
        if not take_profit < to_price < stop:
            to_price = to_price

    return to_price


def _limit_trade_from_alo(
        alo: SmtPspTrade, trade_asset: Asset, assets: List[Asset], tos: TrueOpens
) -> Tuple[Optional[SmtPspTrade], Optional[SmtPspTrade]]:  # opened_trade, updated_alo

    asset_i = next(i for i, x in enumerate(assets) if x.symbol == trade_asset.symbol)
    trade_tos = tos[asset_i]

    if alo.limit_chase_to_label is not None:  # we're chasing some true open
        my_to = next((to for to in trade_tos if to[0] == alo.limit_chase_to_label), None)
        if not my_to:  # ещё не появилось или наступил первый квартал и наоборот ушло
            return None, None

        to_price = _to_limit_price(alo.direction, my_to[1], alo.limit_stop, alo.limit_take_profit)
        if alo.direction == "UP":
            if not alo.limit_stop < to_price < alo.limit_take_profit:  # появилась, но не в нашем ренже
                return None, alo  # ждём следующего такого True Open
        else:
            if not alo.limit_take_profit < to_price < alo.limit_stop:  # появилась, но не в нашем ренже
                return None, alo  # ждём следующего такого True Open

        def update_limit():
            alo.limit_price_history.append(to_price)  # апдейтим лимитку и ждём
            to_rr_pos = _rr_and_pos_size(to_price, alo.limit_take_profit, alo.limit_stop)
            alo.limit_rr = to_rr_pos[0]
            alo.limit_position_assets = to_rr_pos[1]
            alo.limit_position_usd = to_rr_pos[2]

        if len(alo.limit_price_history) == 0:  # это to не могли поставить целью раньше, а теперь она появилась
            update_limit()
        elif alo.limit_price_history[-1] != to_price:  # это не тот же True Open (прошлого цикла)
            update_limit()

    # не открыли лимитку
    if not trade_asset.prev_15m_candle[2] < alo.limit_price_history[-1] < trade_asset.prev_15m_candle[1]:
        return None, alo

    # пост-проверка кастомная в стратегиях
    if not alo.limit_pre_fill_checker(trade_asset.prev_15m_candle[5]):
        return None, alo

    return _open_limit_trade(alo, trade_asset, trade_tos), None  # открываем сделку


def _open_limit_orders(
        trade_asset: Asset, rr_pos: Tuple[float, float, float], my_target: Target,
        my_psp_change: Tuple[int, str, str, str, str, str, str, str],
        stop: float, psp_extremums: Tuple[float, float, float], take_profit: float,
        tos: List[TrueOpen]
) -> List[SmtPspTrade]:
    smt_level, smt_label, smt_type, smt_flags, smt_first_appeared, psp_key, psp_date, change = my_psp_change

    direction = "UP" if my_target[1] in ["high", "half_high"] else "DOWN"

    median_chase_rr = rr_pos[0] * 1.3
    mean_chase_rr = rr_pos[0] * 2
    if 1.5 < rr_pos[0] < 4.5:
        median_chase_rr = rr_pos[0] * 1.5
        mean_chase_rr = rr_pos[0] * 2.5
    elif rr_pos[0] <= 1.5:
        median_chase_rr = rr_pos[0] * 1.7
        mean_chase_rr = rr_pos[0] * 3.5

    median_price_pos = _price_and_pos_size(median_chase_rr, take_profit, stop)
    mean_price_pos = _price_and_pos_size(mean_chase_rr, take_profit, stop)

    def limit_order(
            reason: str, price: Optional[float], pos_assets: Optional[float], pos_usd: Optional[float],
            chase_rr: Optional[float], chase_to_label: Optional[str]
    ) -> SmtPspTrade:
        return SmtPspTrade(
            asset=trade_asset.symbol,
            direction=direction,
            signal_time=trade_asset.snapshot_date_readable,
            signal_time_ny=to_ny_date_str(trade_asset.snapshot_date_readable),
            limit_price_history=[price] if price is not None else [],
            limit_stop=stop,
            limit_take_profit=take_profit,
            limit_rr=chase_rr,
            limit_position_assets=pos_assets,
            limit_position_usd=pos_usd,
            limit_status="ACTIVE",
            limit_chase_to_label=chase_to_label,
            limit_chase_rr=chase_rr,
            entry_order_type="",
            entry_price=0,
            stop=0,
            take_profit=0,
            entry_rr=0,
            entry_time="",
            entry_time_ny="",
            entry_position_assets=0,
            entry_position_usd=0,
            entry_position_fee=0,
            entry_reason=f"{reason} limit for {psp_key} psp for {smt_type} {smt_label} -> {my_target[1]} {my_target[2]}",
            entry_tos=[],
            psp_key_used=psp_key,
            psp_date=psp_date,
            smt_type=smt_type,
            smt_level=smt_level,
            smt_label=smt_label,
            smt_flags=smt_flags,
            smt_first_appeared=smt_first_appeared,
            target_level=my_target[0],
            target_direction=my_target[1],
            target_label=my_target[2],
            target_ql_start=my_target[3],
            best_pnl=0,
            best_pnl_time="",
            best_pnl_time_ny="",
            best_pnl_price=0,
            best_pnl_tos=[],
            best_entry_time="",
            best_entry_time_ny="",
            best_entry_price=0,
            best_entry_rr=0,
            best_entry_tos=[],
            psp_extremums=psp_extremums,
            deadline_close="",
            targets=(my_target[4][1], my_target[5][1], my_target[6][1]),
            _in_trade_range=None,
            closes=[],
            pnl_usd=0,
            close_position_fee=0
        )

    limit_orders = [
        limit_order(
            "chase_median_rr", median_price_pos[0], median_price_pos[1], median_price_pos[2], median_chase_rr, None
        ),
        limit_order(
            "chase_mean_rr", mean_price_pos[0], mean_price_pos[1], mean_price_pos[2], mean_chase_rr, None
        ),
    ]

    tos_list = [x[0] for x in tos]
    asset_to_i = next(i for i, x in enumerate(tos_list) if x == trade_asset.symbol)
    aside_tos = tos_list[asset_to_i + 1:] if direction == "UP" else tos[:asset_to_i]
    all_tos = ['tyo', 'tmo', 'two', 'tdo', 't90mo']

    for to in tos:
        if to[0] in aside_tos or to[0] == trade_asset.symbol:
            continue
        if direction == "UP":
            if not stop < to[1] < take_profit:
                continue
        else:
            if not take_profit < to[1] < stop:
                continue

        to_price = _to_limit_price(direction, to[1], stop, take_profit)
        to_rr_pos = _rr_and_pos_size(to_price, take_profit, stop)
        limit_orders.append(limit_order(
            f"chase_{to[0]}", to_price, to_rr_pos[1], to_rr_pos[2], to_rr_pos[0], to[0]
        ))

    for to_label in set(all_tos) - set(tos_list):
        limit_orders.append(limit_order(
            f"chase_absent_{to_label}", None, None, None, None, to_label
        ))

    return limit_orders


def strategy01_to_constructor(
        smt_lvl_filter: int, psp_change_filter: str, psp_keys_filter: List[str],
        target_lvl_filter: List[int], allow_half_target: bool, tsg: Callable[[bool], TargetSorter]
) -> TOpener:
    def strategy01_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
        my_psp_change = _filter_by_psp_change(
            smt_lvl_filter, ['high', 'half_high', 'low', 'half_low'], spc[4], [psp_change_filter], psp_keys_filter
        )
        if not my_psp_change:
            return []
        smt_level, smt_label, smt_type, smt_flags, smt_first_appeared, psp_key, psp_date, change = my_psp_change

        my_target = _filter_by_target(
            smt_types_is_long[smt_type], allow_half_target, tc, target_lvl_filter, tsg(smt_types_is_long[smt_type])
        )
        if not my_target:
            return []

        stop_a1, stop_a2, stop_a3 = _psp_extremums(
            [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
            smt_type, psp_key, psp_date)

        if (tr.a1.prev_15m_candle[3] in [my_target[4][0], stop_a1] or
                tr.a2.prev_15m_candle[3] in [my_target[5][0], stop_a2] or
                tr.a3.prev_15m_candle[3] in [my_target[6][0], stop_a3]):
            return []

        rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[4][0], stop_a1),
                                           _rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[5][0], stop_a2),
                                           _rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[6][0], stop_a3))

        smb_to_trade = _filter_by_tos_ratio_and_rr(
            tos, (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
            (tr.a1.prev_15m_candle[3], tr.a2.prev_15m_candle[3], tr.a3.prev_15m_candle[3]),
            (rr_pos_a1[0], rr_pos_a2[0], rr_pos_a3[0]),
            1.5, 0.5, smt_types_is_long[smt_type]
        )
        if smb_to_trade == "":
            return []

        stop_percents = (
            percent_from_current(tr.a1.prev_15m_candle[3], stop_a1),
            percent_from_current(tr.a2.prev_15m_candle[3], stop_a2),
            percent_from_current(tr.a3.prev_15m_candle[3], stop_a3),
        )

        trade_asset = tr.a1
        rr_pos = rr_pos_a1
        stop = stop_a1
        take_profit = my_target[4][0]
        trade_tos = tos[0]

        match smb_to_trade:
            case tr.a2.symbol:
                trade_asset = tr.a2
                rr_pos = rr_pos_a2
                stop = stop_a2
                take_profit = my_target[5][0]
                trade_tos = tos[1]
            case tr.a3.symbol:
                trade_asset = tr.a3
                rr_pos = rr_pos_a3
                stop = stop_a3
                take_profit = my_target[6][0]
                trade_tos = tos[2]

        return [
            _open_market_trade(
                trade_asset, rr_pos, my_target, my_psp_change, stop, stop_percents, take_profit, trade_tos
            ),
            *_open_limit_orders(
                trade_asset, rr_pos, my_target, my_psp_change, stop, stop_percents, take_profit, trade_tos
            ),
        ]

    return strategy01_to


def strategy03_to_constructor(
        smt_lvl_filter: int, psp_change_filter: str, psp_keys_filter: List[str],
        target_lvl_filter: List[int], allow_half_target: bool, tsg: Callable[[bool], TargetSorter]
) -> TOpener:
    def strategy03_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
        my_psp_change = _filter_by_psp_change(
            smt_lvl_filter, ['high', 'half_high', 'low', 'half_low'], spc[4], [psp_change_filter], psp_keys_filter
        )
        if not my_psp_change:
            return []
        smt_level, smt_label, smt_type, smt_flags, smt_first_appeared, psp_key, psp_date, change = my_psp_change

        my_target = _filter_by_target(
            smt_types_is_long[smt_type], allow_half_target, tc, target_lvl_filter, tsg(smt_types_is_long[smt_type])
        )
        if not my_target:
            return []

        stop_a1, stop_a2, stop_a3 = _psp_extremums(
            [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
            smt_type, psp_key, psp_date)

        if (tr.a1.prev_15m_candle[3] in [my_target[4][0], stop_a1] or
                tr.a2.prev_15m_candle[3] in [my_target[5][0], stop_a2] or
                tr.a3.prev_15m_candle[3] in [my_target[6][0], stop_a3]):
            return []

        rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[4][0], stop_a1),
                                           _rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[5][0], stop_a2),
                                           _rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[6][0], stop_a3))

        smb_to_trade = _filter_by_tos_ratio_and_rr(
            tos, (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
            (tr.a1.prev_15m_candle[3], tr.a2.prev_15m_candle[3], tr.a3.prev_15m_candle[3]),
            (rr_pos_a1[0], rr_pos_a2[0], rr_pos_a3[0]),
            1.5, 0.5, smt_types_is_long[smt_type]
        )
        if smb_to_trade == "":
            return []

        stop_percents = (
            percent_from_current(tr.a1.prev_15m_candle[3], stop_a1),
            percent_from_current(tr.a2.prev_15m_candle[3], stop_a2),
            percent_from_current(tr.a3.prev_15m_candle[3], stop_a3),
        )

        trade_asset = tr.a1
        rr_pos = rr_pos_a1
        stop = stop_a1
        take_profit = my_target[4][0]
        trade_tos = tos[0]

        trades = []
        for smb in (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol):
            match smb:
                case tr.a2.symbol:
                    trade_asset = tr.a2
                    rr_pos = rr_pos_a2
                    stop = stop_a2
                    take_profit = my_target[5][0]
                    trade_tos = tos[1]
                case tr.a3.symbol:
                    trade_asset = tr.a3
                    rr_pos = rr_pos_a3
                    stop = stop_a3
                    take_profit = my_target[6][0]
                    trade_tos = tos[2]

            trades.append(
                _open_market_trade(
                    trade_asset, rr_pos, my_target, my_psp_change, stop, stop_percents, take_profit, trade_tos
                )
            )
            trades.extend(
                _open_limit_orders(
                    trade_asset, rr_pos, my_target, my_psp_change, stop, stop_percents, take_profit, trade_tos
                ),
            )
        return trades

    return strategy03_to


def strategy04_to_constructor(
        smt_lvl_filter: int, psp_change_filter: str, psp_keys_filter: List[str],
        target_lvl_filter: List[int], allow_half_target: bool, tsg: Callable[[bool], TargetSorter],
        tos_aside_filter_all: List[str], tos_aside_filter_my: List[str], rr_all_min: float, rr_all_max: float
) -> TOpener:
    def strategy04_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
        my_psp_change = _filter_by_psp_change(
            smt_lvl_filter, ['high', 'half_high', 'low', 'half_low'], spc[4], [psp_change_filter], psp_keys_filter
        )
        if not my_psp_change:
            return []
        smt_level, smt_label, smt_type, smt_flags, smt_first_appeared, psp_key, psp_date, change = my_psp_change

        my_target = _filter_by_target(
            smt_types_is_long[smt_type], allow_half_target, tc, target_lvl_filter, tsg(smt_types_is_long[smt_type])
        )
        if not my_target:
            return []

        stop_a1, stop_a2, stop_a3 = _psp_extremums(
            [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
            smt_type, psp_key, psp_date)

        if (tr.a1.prev_15m_candle[3] in [my_target[4][0], stop_a1] or
                tr.a2.prev_15m_candle[3] in [my_target[5][0], stop_a2] or
                tr.a3.prev_15m_candle[3] in [my_target[6][0], stop_a3]):
            return []

        rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[4][0], stop_a1),
                                           _rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[5][0], stop_a2),
                                           _rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[6][0], stop_a3))

        symbols_to_trade_d = _filter_by_tos(
            tos, (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
            (tr.a1.prev_15m_candle[3], tr.a2.prev_15m_candle[3], tr.a3.prev_15m_candle[3]),
            smt_types_is_long[smt_type],
            tos_aside_filter_all,
            tos_aside_filter_my,
            # ['two', 'tdo'],
            # ['two', 'tdo', 't90mo'],
        )
        if not symbols_to_trade_d:
            return []
        symbol_max_rr = _filter_by_rr(
            (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol),
            (rr_pos_a1[0], rr_pos_a2[0], rr_pos_a3[0]),
            rr_all_min, rr_all_max
            # 3.2, 80
        )
        if symbol_max_rr == "" or symbol_max_rr not in symbols_to_trade_d:
            return []

        stop_percents = (
            percent_from_current(tr.a1.prev_15m_candle[3], stop_a1),
            percent_from_current(tr.a2.prev_15m_candle[3], stop_a2),
            percent_from_current(tr.a3.prev_15m_candle[3], stop_a3),
        )

        trade_asset = tr.a1
        rr_pos = rr_pos_a1
        stop = stop_a1
        take_profit = my_target[4][0]
        trade_tos = tos[0]

        match symbol_max_rr:
            case tr.a2.symbol:
                trade_asset = tr.a2
                rr_pos = rr_pos_a2
                stop = stop_a2
                take_profit = my_target[5][0]
                trade_tos = tos[1]
            case tr.a3.symbol:
                trade_asset = tr.a3
                rr_pos = rr_pos_a3
                stop = stop_a3
                take_profit = my_target[6][0]
                trade_tos = tos[2]

        return [
            _open_market_trade(
                trade_asset, rr_pos, my_target, my_psp_change, stop, stop_percents, take_profit, trade_tos
            ),
            *_open_limit_orders(
                trade_asset, rr_pos, my_target, my_psp_change, stop, stop_percents, take_profit, trade_tos
            ),
        ]

    return strategy04_to


def strategy07_to_constructor(
        direction_filter: str,
        smt_lvl_filter: int, psp_change_filter: str, psp_keys_filter: List[str],
        target_lvl_filter: List[int], allow_half_target: bool, tsg: Callable[[bool], TargetSorter],
        post_filterer: Callable[[List[SmtPspTrade]], List[SmtPspTrade]]
) -> TOpener:
    def strategy07_to(tr: Triad, tos: TrueOpens, spc: SmtPspChange, tc: TargetChange) -> List[SmtPspTrade]:
        my_psp_change = _filter_by_psp_change(
            smt_lvl_filter, ['high', 'half_high', 'low', 'half_low'], spc[4], [psp_change_filter], psp_keys_filter
        )
        if not my_psp_change:
            return []

        smt_level, smt_label, smt_type, smt_flags, smt_first_appeared, psp_key, psp_date, change = my_psp_change
        if (direction_filter == "UP" and not smt_types_is_long[smt_type] or
                direction_filter == "DOWN" and smt_types_is_long[smt_type]):
            return []

        my_target = _filter_by_target(
            smt_types_is_long[smt_type], allow_half_target, tc, target_lvl_filter, tsg(smt_types_is_long[smt_type])
        )
        if not my_target:
            return []

        stop_a1, stop_a2, stop_a3 = _psp_extremums(
            [x for x in spc[1] if x[0] == smt_level and x[1] == smt_label][0][2],
            smt_type, psp_key, psp_date)

        if (tr.a1.prev_15m_candle[3] in [my_target[4][0], stop_a1] or
                tr.a2.prev_15m_candle[3] in [my_target[5][0], stop_a2] or
                tr.a3.prev_15m_candle[3] in [my_target[6][0], stop_a3]):
            return []

        rr_pos_a1, rr_pos_a2, rr_pos_a3 = (_rr_and_pos_size(tr.a1.prev_15m_candle[3], my_target[4][0], stop_a1),
                                           _rr_and_pos_size(tr.a2.prev_15m_candle[3], my_target[5][0], stop_a2),
                                           _rr_and_pos_size(tr.a3.prev_15m_candle[3], my_target[6][0], stop_a3))

        stop_percents = (
            percent_from_current(tr.a1.prev_15m_candle[3], stop_a1),
            percent_from_current(tr.a2.prev_15m_candle[3], stop_a2),
            percent_from_current(tr.a3.prev_15m_candle[3], stop_a3),
        )

        trade_asset = tr.a1
        rr_pos = rr_pos_a1
        stop = stop_a1
        take_profit = my_target[4][0]
        trade_tos = tos[0]

        trades = []
        for smb in (tr.a1.symbol, tr.a2.symbol, tr.a3.symbol):
            match smb:
                case tr.a2.symbol:
                    trade_asset = tr.a2
                    rr_pos = rr_pos_a2
                    stop = stop_a2
                    take_profit = my_target[5][0]
                    trade_tos = tos[1]
                case tr.a3.symbol:
                    trade_asset = tr.a3
                    rr_pos = rr_pos_a3
                    stop = stop_a3
                    take_profit = my_target[6][0]
                    trade_tos = tos[2]

            trades.append(
                _open_market_trade(
                    trade_asset, rr_pos, my_target, my_psp_change, stop, stop_percents, take_profit, trade_tos
                )
            )
            trades.extend(
                _open_limit_orders(
                    trade_asset, rr_pos, my_target, my_psp_change, stop, stop_percents, take_profit, trade_tos
                ),
            )
        return post_filterer(trades)

    return strategy07_to


def strategy_10_to_pf(trades: List[SmtPspTrade]) -> List[SmtPspTrade]:
    res = []

    def abs_tdo_1h_checker(self: SmtPspTrade, _: str) -> bool:
        return 1.1 < self.limit_rr < 2.3

    def t90mo_1h_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.5 < self.limit_rr < 2

    def median_rr_1h_checker(self: SmtPspTrade, _: str) -> bool:
        return 12 < self.limit_rr < 20

    def chase_two_tmo_4h_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.3 < self.limit_rr < 3

    def mean_rr_4h_checker(self: SmtPspTrade, entry_time: str) -> bool:
        if not 12 < self.limit_rr < 25:
            return False
        if self.asset == "ETHUSDT":  # TODO hardcode
            return False
        return (quarters_by_time(self.signal_time)[3] in [DayQuarter.DQ1_Asia, DayQuarter.DQ2_London] and
                quarters_by_time(entry_time)[3] in [DayQuarter.DQ1_Asia])

    def median_rr_4h_checker(self: SmtPspTrade, entry_time: str) -> bool:
        if not 5 < self.limit_rr < 11:
            return False
        return (quarters_by_time(entry_time)[3] in [DayQuarter.DQ2_London] and
                quarters_by_time(self.signal_time)[3] in [DayQuarter.DQ2_London])

    for t in trades:
        limit_reason = t.entry_reason.split()[0]
        if t.psp_key_used == '1h':
            if t.entry_order_type == "MARKET":
                if 12 < t.entry_rr < 20:
                    res.append(t)
                    continue
            match limit_reason:
                case "chase_absent_tdo":
                    t.limit_pre_fill_checker = MethodType(abs_tdo_1h_checker, t)
                    res.append(t)
                case "chase_t90mo":
                    t.limit_pre_fill_checker = MethodType(t90mo_1h_checker, t)
                    res.append(t)
                case "chase_median_rr":
                    t.limit_pre_fill_checker = MethodType(median_rr_1h_checker, t)
                    res.append(t)
        elif t.psp_key_used == '4h':
            match limit_reason:
                case "chase_tmo" | "chase_two":
                    t.limit_pre_fill_checker = MethodType(chase_two_tmo_4h_checker, t)
                    res.append(t)
                case "chase_mean_rr":
                    t.limit_pre_fill_checker = MethodType(mean_rr_4h_checker, t)
                    res.append(t)
                case "chase_median_rr":
                    t.limit_pre_fill_checker = MethodType(median_rr_4h_checker, t)
                    res.append(t)

    return res


def strategy_12_to_pf(trades: List[SmtPspTrade]) -> List[SmtPspTrade]:
    res = []

    def abs_tdo_1h_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.3 < self.limit_rr < 2

    def two_1h_checker(self: SmtPspTrade, _: str) -> bool:
        return 3 < self.limit_rr < 7

    def t90mo_1h_checker(self: SmtPspTrade, entry_time: str) -> bool:
        if not 0.5 < self.limit_rr < 2.45:
            return False
        return quarters_by_time(self.signal_time)[3] in [DayQuarter.DQ1_Asia]

    def median_rr_1h_checker(self: SmtPspTrade, _: str) -> bool:
        if not 6 < self.limit_rr < 85:
            return False
        return quarters_by_time(self.signal_time)[3] in [DayQuarter.DQ1_Asia]

    def absent_two_2h_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.3 < self.limit_rr < 50

    def t90mo_2h_checker(self: SmtPspTrade, entry_time: str) -> bool:
        if not 4 < self.limit_rr < 9:
            return False
        return quarters_by_time(self.signal_time)[3] in [DayQuarter.DQ1_Asia, DayQuarter.DQ3_NYAM]

    def absent_tdo_4h_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.3 < self.limit_rr < 3.6

    for t in trades:
        limit_reason = t.entry_reason.split()[0]
        if limit_reason == 'chase_tyo':
            res.append(t)
            continue
        if t.psp_key_used == '1h':
            if t.entry_order_type == "MARKET":
                if (4 < t.entry_rr < 11 and t.target_level == 4 and
                        quarters_by_time(t.signal_time)[3] in [DayQuarter.DQ1_Asia]):
                    res.append(t)
                    continue
            match limit_reason:
                case "chase_absent_tdo":
                    if t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(abs_tdo_1h_checker, t)
                    res.append(t)
                case "chase_two":
                    if t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(two_1h_checker, t)
                    res.append(t)
                case "chase_t90mo":
                    if t.target_level == 5:
                        t.limit_pre_fill_checker = MethodType(t90mo_1h_checker, t)
                    res.append(t)
                case "chase_median_rr":
                    if t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(median_rr_1h_checker, t)
                    res.append(t)
        elif t.psp_key_used == '2h':
            match limit_reason:
                case "chase_absent_two":
                    if t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(absent_two_2h_checker, t)
                    res.append(t)
                case "chase_t90mo":
                    if t.target_level == 5:
                        t.limit_pre_fill_checker = MethodType(t90mo_2h_checker, t)
                    res.append(t)
        elif t.psp_key_used == '4h':
            match limit_reason:
                case "chase_absent_tdo":
                    if t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(absent_tdo_4h_checker, t)
                    res.append(t)

    return res


def strategy_14_to_pf(trades: List[SmtPspTrade]) -> List[SmtPspTrade]:
    res = []

    def abs_tdo_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.3 < self.limit_rr < 1.5

    def tdo_4h_checker(self: SmtPspTrade, _: str) -> bool:
        return 4 < self.limit_rr < 9

    def two_4h_checker(self: SmtPspTrade, _: str) -> bool:
        return 1.5 < self.limit_rr < 95

    def med_rr_1d_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.3 < self.limit_rr < 1

    def t90mo_1d_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.8 < self.limit_rr < 2.3

    for t in trades:
        limit_reason = t.entry_reason.split()[0]
        if limit_reason in ['chase_tyo', 'chase_absent_tyo']:
            res.append(t)
            continue
        if limit_reason == 'abs_tdo_checker':
            t.limit_pre_fill_checker = MethodType(abs_tdo_checker, t)
            res.append(t)
            continue
        if t.psp_key_used == '4h':
            match limit_reason:
                case "chase_tdo":
                    t.limit_pre_fill_checker = MethodType(tdo_4h_checker, t)
                    res.append(t)
                case "chase_two":
                    t.limit_pre_fill_checker = MethodType(two_4h_checker, t)
                    res.append(t)
        elif t.psp_key_used == '1d':
            match limit_reason:
                case "chase_median_rr":
                    t.limit_pre_fill_checker = MethodType(med_rr_1d_checker, t)
                    res.append(t)
                case "chase_t90mo":
                    t.limit_pre_fill_checker = MethodType(t90mo_1d_checker, t)
                    res.append(t)

    return res


def strategy_16_to_pf(trades: List[SmtPspTrade]) -> List[SmtPspTrade]:
    res = []

    def abs_two_checker(self: SmtPspTrade, _: str) -> bool:
        return self.limit_rr < 3

    def abs_two_two_1d_checker(self: SmtPspTrade, _: str) -> bool:
        return 1.5 < self.limit_rr

    def med_rr_1d_target4_checker(self: SmtPspTrade, _: str) -> bool:
        return 4 < self.limit_rr < 6

    def med_rr_1d_target3_checker(self: SmtPspTrade, _: str) -> bool:
        return 2 < self.limit_rr < 10

    def abs_tdo_1d_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.5 < self.limit_rr < 1.5

    def abs_t90mo_1d_target3_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.5 < self.limit_rr < 5

    def abs_t90mo_1d_target4_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.4 < self.limit_rr < 1.3

    def t90mo_1d_target3_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.4 < self.limit_rr < 150

    def t90mo_1d_target4_checker(self: SmtPspTrade, _: str) -> bool:
        return 1 < self.limit_rr < 40

    def mean_rr_1d_target4_checker(self: SmtPspTrade, _: str) -> bool:
        return 1 < self.limit_rr < 40

    def tdo_4h_target3_checker(self: SmtPspTrade, _: str) -> bool:
        return 2.5 < self.limit_rr < 50

    def tdo_4h_target4_checker(self: SmtPspTrade, _: str) -> bool:
        return 3 < self.limit_rr < 12

    for t in trades:
        limit_reason = t.entry_reason.split()[0]
        if limit_reason in ['chase_tyo', 'chase_absent_tyo']:
            res.append(t)
            continue
        if t.psp_key_used == '1d':
            if t.entry_order_type == "MARKET":
                if 0.5 < t.entry_rr < 40 and t.target_level == 3:
                    res.append(t)
                    continue
            match limit_reason:
                case "chase_two" | "chase_absent_two":
                    t.limit_pre_fill_checker = MethodType(abs_two_two_1d_checker, t)
                    res.append(t)
                case "chase_median_rr":
                    if t.target_level == 3:
                        t.limit_pre_fill_checker = MethodType(med_rr_1d_target3_checker, t)
                    elif t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(med_rr_1d_target4_checker, t)
                    res.append(t)
                case "chase_absent_tdo":
                    if t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(abs_tdo_1d_checker, t)
                    res.append(t)
                case "chase_absent_t90mo":
                    if t.target_level == 3:
                        t.limit_pre_fill_checker = MethodType(abs_t90mo_1d_target3_checker, t)
                    elif t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(abs_t90mo_1d_target4_checker, t)
                    res.append(t)
                case "chase_t90mo":
                    if t.target_level == 3:
                        t.limit_pre_fill_checker = MethodType(t90mo_1d_target3_checker, t)
                    elif t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(t90mo_1d_target4_checker, t)
                    res.append(t)
                case "chase_mean_rr":
                    if t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(mean_rr_1d_target4_checker, t)
                    res.append(t)
        elif t.psp_key_used == '4h':
            match limit_reason:
                case "chase_tdo":
                    if t.target_level == 3:
                        t.limit_pre_fill_checker = MethodType(tdo_4h_target3_checker, t)
                    elif t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(tdo_4h_target4_checker, t)
                    res.append(t)
        if limit_reason in ['chase_two']:
            if t.target_level == 4:
                res.append(t)
            continue
        if limit_reason in ['chase_absent_two']:
            if t.target_level == 4:
                res.append(t)
            else:
                t.limit_pre_fill_checker = MethodType(abs_two_checker, t)
                res.append(t)
            continue

    return res


def strategy_18_to_pf(trades: List[SmtPspTrade]) -> List[SmtPspTrade]:
    res = []

    def t90mo_4h_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.8 < self.limit_rr < 3.5

    def abs_t90mo_1d_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.6 < self.limit_rr

    for t in trades:
        limit_reason = t.entry_reason.split()[0]
        if t.psp_key_used == '4h':
            match limit_reason:
                case "chase_t90mo":
                    t.limit_pre_fill_checker = MethodType(t90mo_4h_checker, t)
                    res.append(t)
        elif t.psp_key_used == '1d':
            if t.entry_order_type == "MARKET":
                if 0.5 < t.entry_rr:
                    res.append(t)
                    continue
            match limit_reason:
                case "chase_absent_tdo" | "chase_tmo":
                    res.append(t)
                case "chase_absent_t90mo":
                    t.limit_pre_fill_checker = MethodType(abs_t90mo_1d_checker, t)
                    res.append(t)

    return res


def strategy_30_to_pf(trades: List[SmtPspTrade]) -> List[SmtPspTrade]:
    res = []

    def tdo_2h_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.2 < self.limit_rr < 0.9

    def tdo_4h_checker(self: SmtPspTrade, _: str) -> bool:
        return 1.5 < self.limit_rr < 6.5

    def t90mo_4h_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.5 < self.limit_rr < 2.5

    for t in trades:
        limit_reason = t.entry_reason.split()[0]
        if t.psp_key_used == '1h':
            match limit_reason:
                case "chase_absent_tdo":
                    if t.target_level == 4:
                        res.append(t)
                        continue
        elif t.psp_key_used == '2h':
            if t.entry_order_type == "MARKET":
                if t.target_level == 5 and 4.5 < t.entry_rr:
                    res.append(t)
                    continue
            match limit_reason:
                case "chase_tdo":
                    if t.target_level == 5:
                        t.limit_pre_fill_checker = MethodType(tdo_2h_checker, t)
                        res.append(t)
                        continue
                case "chase_two":
                    if t.target_level == 5:
                        res.append(t)
                        continue
        elif t.psp_key_used == '4h':
            match limit_reason:
                case "chase_two":
                    if t.target_level == 4:
                        res.append(t)
                        continue
                case "chase_tdo":
                    if t.target_level == 5:
                        t.limit_pre_fill_checker = MethodType(tdo_4h_checker, t)
                        res.append(t)
                        continue
                case "chase_t90mo":
                    if t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(t90mo_4h_checker, t)
                        res.append(t)
                        continue

    return res


def strategy_32_to_pf(trades: List[SmtPspTrade]) -> List[SmtPspTrade]:
    res = []

    def t90mo_1d_checker(self: SmtPspTrade, _: str) -> bool:
        return self.limit_rr < 0.42

    def abs_tdo_1d_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.3 < self.limit_rr < 0.8

    def med_rr_1d_checker(self: SmtPspTrade, _: str) -> bool:
        return 0.5 < self.limit_rr < 2

    for t in trades:
        limit_reason = t.entry_reason.split()[0]
        if t.psp_key_used == '1d':
            match limit_reason:
                case "chase_t90mo":
                    if t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(t90mo_1d_checker, t)
                        res.append(t)
                case "chase_absent_tdo":
                    if t.target_level == 3:
                        t.limit_pre_fill_checker = MethodType(abs_tdo_1d_checker, t)
                        res.append(t)
                case "chase_median_rr":
                    if t.target_level == 4:
                        t.limit_pre_fill_checker = MethodType(med_rr_1d_checker, t)
                        res.append(t)

    return res


def _close_by_tp_sl_deadlines(at: SmtPspTrade, trade_asset: Asset, stop_after: str) -> Optional[SmtPspTrade]:
    if to_utc_datetime(trade_asset.snapshot_date_readable) >= to_utc_datetime(stop_after):
        return _close_trade(
            at, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "strategy_stop", False)
    if (at.deadline_close and
            to_utc_datetime(trade_asset.snapshot_date_readable) >= to_utc_datetime(at.deadline_close)):
        return _close_trade(
            at, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "deadline", False)
    if at.direction == 'UP':
        if trade_asset.prev_15m_candle[2] <= at.stop:
            return _close_trade(at, at.stop, trade_asset.snapshot_date_readable, "stop", False)
        if trade_asset.prev_15m_candle[1] >= at.take_profit:
            return _close_trade(
                at, at.take_profit, trade_asset.snapshot_date_readable, "take_profit", False)
    if at.direction == 'DOWN':
        if trade_asset.prev_15m_candle[1] >= at.stop:
            return _close_trade(at, at.stop, trade_asset.snapshot_date_readable, "stop", False)
        if trade_asset.prev_15m_candle[2] <= at.take_profit:
            return _close_trade(
                at, at.take_profit, trade_asset.snapshot_date_readable, "take_profit", False)
    return None


def _cancel_by_tp_sl_deadlines(alo: SmtPspTrade, trade_asset: Asset, stop_after: str) -> Optional[SmtPspTrade]:
    if to_utc_datetime(trade_asset.snapshot_date_readable) >= to_utc_datetime(stop_after):
        return _cancel_order(
            alo, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "strategy_stop")
    if (alo.deadline_close and
            to_utc_datetime(trade_asset.snapshot_date_readable) >= to_utc_datetime(alo.deadline_close)):
        return _cancel_order(
            alo, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "deadline")
    if alo.direction == 'UP':
        if trade_asset.prev_15m_candle[2] <= alo.limit_stop:
            return _cancel_order(alo, alo.limit_stop, trade_asset.snapshot_date_readable, "limit_stop")
        if trade_asset.prev_15m_candle[1] >= alo.limit_take_profit:
            return _cancel_order(
                alo, alo.limit_take_profit, trade_asset.snapshot_date_readable, "limit_take_profit")
    if alo.direction == 'DOWN':
        if trade_asset.prev_15m_candle[1] >= alo.limit_stop:
            return _cancel_order(alo, alo.limit_stop, trade_asset.snapshot_date_readable, "limit_stop")
        if trade_asset.prev_15m_candle[2] <= alo.limit_take_profit:
            return _cancel_order(
                alo, alo.limit_take_profit, trade_asset.snapshot_date_readable, "limit_take_profit")
    return None


def _cancel_by_other_reached_target(alo: SmtPspTrade, trade_asset: Asset, tc: TargetChange) -> Optional[SmtPspTrade]:
    for target_level, target_direction, target_label, ql_start, _, _ in tc[4] + tc[5]:
        if (target_level == alo.target_level and target_direction == alo.target_direction
                and target_label == alo.target_label):
            return _cancel_order(
                alo, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "limit_someone_reached_target"
            )

    return None


def _cancel_by_smt_psp_change(alo: SmtPspTrade, trade_asset: Asset, spc: SmtPspChange) -> Optional[SmtPspTrade]:
    antagonist_smt_types = {
        'high': ['low', 'half_low'],
        'half_high': ['low', 'half_low'],
        'low': ['high', 'half_high'],
        'half_low': ['high', 'half_high'],
    }
    for new_smt_level, new_smt_label, new_smt in spc[2]:
        if new_smt_level >= alo.smt_level and new_smt.type in antagonist_smt_types[alo.smt_type]:
            return _cancel_order(
                alo, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "limit_antagonist_smt"
            )

    for old_smt_level, old_smt_label, old_smt in spc[3]:
        if old_smt_label == alo.smt_label and old_smt.type == alo.smt_type:
            return _cancel_order(
                alo, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "limit_smt_cancelled"
            )

    for _, _, _, _, _, psp_key, psp_date, psp_change in spc[4]:  # possible|closed|confirmed|swept
        if psp_change == 'swept' and psp_date == alo.psp_date and psp_key == alo.psp_key_used:
            return _cancel_order(
                alo, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "limit_psp_swept"
            )
    return None


def _close_by_other_swept_psp(at: SmtPspTrade, trade_asset: Asset, spc: SmtPspChange) -> Optional[SmtPspTrade]:
    for _, _, _, _, _, psp_key, psp_date, psp_change in spc[4]:  # possible|closed|confirmed|swept
        if psp_change == 'swept' and psp_date == at.psp_date and psp_key == at.psp_key_used:
            return _close_trade(
                at, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable, "someone_psp_swept", True
            )


def _close_by_other_reached_target(at: SmtPspTrade, trade_asset: Asset, tc: TargetChange) -> Optional[SmtPspTrade]:
    for target_level, target_direction, target_label, ql_start, _, _ in tc[4] + tc[5]:
        if (target_level == at.target_level and target_direction == at.target_direction
                and target_label == at.target_label and ql_start == at.target_ql_start):
            return _close_trade(
                at, trade_asset.prev_15m_candle[3], trade_asset.snapshot_date_readable,
                "someone_reached_target", True
            )


def _with_best(at: SmtPspTrade, tr: Triad, tos: TrueOpens) -> SmtPspTrade:
    smb_assets_map = {}
    trade_tos = []
    for i, a in enumerate([tr.a1, tr.a2, tr.a3]):
        smb_assets_map[a.symbol] = a
        if a.symbol == at.asset:
            trade_tos = tos[i]
    asset = smb_assets_map[at.asset]
    at._in_trade_range = asset.prev_15m_candle if not at._in_trade_range else as_1_candle(
        [at._in_trade_range, asset.prev_15m_candle]
    )

    if at.direction == 'UP':
        candle_max_pnl = at.entry_position_usd / at.entry_price * (asset.prev_15m_candle[1] - at.entry_price)
        if candle_max_pnl > at.best_pnl:
            at.best_pnl = candle_max_pnl
            at.best_pnl_time = asset.snapshot_date_readable
            at.best_pnl_time_ny = to_ny_date_str(asset.snapshot_date_readable)
            at.best_pnl_price = asset.prev_15m_candle[1]
            at.best_pnl_tos = []
            for label, price, perc in trade_tos:
                if label == asset.symbol:
                    at.best_pnl_tos.append((label, asset.prev_15m_candle[1], 0))
                else:
                    at.best_pnl_tos.append((label, price, percent_from_current(asset.prev_15m_candle[1], price)))
        if asset.prev_15m_candle[2] < at.best_entry_price:
            at.best_entry_time = asset.snapshot_date_readable
            at.best_entry_time_ny = to_ny_date_str(asset.snapshot_date_readable)
            at.best_entry_price = asset.prev_15m_candle[2]
            at.best_entry_rr = _rr_and_pos_size(asset.prev_15m_candle[2], at.take_profit, at.stop)[0]
            at.best_entry_tos = []
            for label, price, perc in trade_tos:
                if label == asset.symbol:
                    at.best_entry_tos.append((label, asset.prev_15m_candle[2], 0))
                else:
                    at.best_entry_tos.append((label, price, percent_from_current(asset.prev_15m_candle[2], price)))
    elif at.direction == 'DOWN':
        candle_max_pnl = at.entry_position_usd / at.entry_price * (at.entry_price - asset.prev_15m_candle[1])
        if candle_max_pnl > at.best_pnl:
            at.best_pnl = candle_max_pnl
            at.best_pnl_time = asset.snapshot_date_readable
            at.best_pnl_time_ny = to_ny_date_str(asset.snapshot_date_readable)
            at.best_pnl_price = asset.prev_15m_candle[1]
            at.best_pnl_tos = []
            for label, price, perc in trade_tos:
                if label == asset.symbol:
                    at.best_pnl_tos.append((label, asset.prev_15m_candle[1], 0))
                else:
                    at.best_pnl_tos.append((label, price, percent_from_current(asset.prev_15m_candle[1], price)))
        if asset.prev_15m_candle[1] > at.best_entry_price:
            at.best_entry_time = asset.snapshot_date_readable
            at.best_entry_time_ny = to_ny_date_str(asset.snapshot_date_readable)
            at.best_entry_price = asset.prev_15m_candle[1]
            at.best_entry_rr = _rr_and_pos_size(asset.prev_15m_candle[1], at.take_profit, at.stop)[0]
            at.best_entry_tos = []
            for label, price, perc in trade_tos:
                if label == asset.symbol:
                    at.best_entry_tos.append((label, asset.prev_15m_candle[1], perc))
                else:
                    at.best_entry_tos.append((label, price, percent_from_current(asset.prev_15m_candle[1], price)))

    at.best_entry_tos = sorted(at.best_entry_tos, key=lambda x: x[1], reverse=True)
    at.best_pnl_tos = sorted(at.best_pnl_tos, key=lambda x: x[1], reverse=True)
    return at


def strategy01_th(
        stop_after: str, active_trades: List[SmtPspTrade], tr: Triad,
        tos: TrueOpens, spc: SmtPspChange, tc: TargetChange
) -> Tuple[List[SmtPspTrade], List[SmtPspTrade]]:
    closed_trades = []
    new_active_trades = []
    assets = [tr.a1, tr.a2, tr.a3]
    assets_d = {asset.symbol: asset for asset in assets}

    opened_trades = [x for x in active_trades if x.entry_time != ""]
    active_limit_orders = [x for x in active_trades if x.limit_status == "ACTIVE"]

    for alo in active_limit_orders:
        trade_asset = assets_d[alo.asset]

        opened_trade, updated_alo = _limit_trade_from_alo(alo, trade_asset, assets, tos)
        if opened_trade:
            opened_trades.append(opened_trade)
            continue

        if updated_alo:
            alo = updated_alo

        by_tp_sp_deadline = _cancel_by_tp_sl_deadlines(alo, trade_asset, stop_after)
        if by_tp_sp_deadline:
            closed_trades.append(by_tp_sp_deadline)
            continue

        by_smt_psp_change = _cancel_by_smt_psp_change(alo, trade_asset, spc)
        if by_smt_psp_change:
            closed_trades.append(by_smt_psp_change)
            continue

        by_other_reached_target = _cancel_by_other_reached_target(alo, trade_asset, tc)
        if by_other_reached_target:
            closed_trades.append(by_other_reached_target)
            continue

        new_active_trades.append(alo)

    for at in opened_trades:
        trade_asset = assets_d[at.asset]

        by_tp_sp_deadline = _close_by_tp_sl_deadlines(at, trade_asset, stop_after)
        if by_tp_sp_deadline:
            if by_tp_sp_deadline.percent_closed() == 100:
                closed_trades.append(by_tp_sp_deadline)
                continue
            else:
                at = by_tp_sp_deadline

        by_other_swept_psp = _close_by_other_swept_psp(at, trade_asset, spc)
        if by_other_swept_psp:
            if by_other_swept_psp.percent_closed() == 100:
                closed_trades.append(by_other_swept_psp)
                continue
            else:
                at = by_other_swept_psp

        by_other_reached_target = _close_by_other_reached_target(at, trade_asset, tc)
        if by_other_reached_target:
            if by_other_reached_target.percent_closed() == 100:
                closed_trades.append(by_other_reached_target)
                continue
            else:
                at = by_other_reached_target

        new_active_trades.append(_with_best(at, tr, tos))

    return (
        new_active_trades,
        closed_trades
    )


# триггер - закрытие 1h/2h/4h PSP в рамках недельного SMT
# цель - ближайший неснятый high/low азии/лондона и пр (не half)
# вход по маркету от точки закрытия prev 15m свечи
# to во всех 3 активах мы ниже/выше больше половины из тру опенов
# минимальный RR (в любом из 3 активов) - 1/1.5
# выбор актива из трёх: сначала по to, потом по RR
# закрытие до стопа: любой из 3 снял наш PSP
# закрытие до тейка: любой из 3 дошёл до нашей цели
strategy01 = SmtPspStrategy(
    name="01. Trigger cPSP in wd smt - closest dq no-half target - medium v1 TO+RR filter",
    trade_opener=strategy01_to_constructor(4, 'closed', ['1h', '2h', '4h'],
                                           [5], False, _closest_targets_sorter),
    trades_handler=strategy01_th
)

# всё как в strategy01, но если есть 2-3 неснятых high/low дня, то берём который с большим RR
strategy02 = SmtPspStrategy(
    name="02. Trigger cPSP in wd smt - furthest dq no-half target - medium v1 TO+RR filter",
    trade_opener=strategy01_to_constructor(4, 'closed', ['1h', '2h', '4h'],
                                           [5], False, _furthest_targets_sorter),
    trades_handler=strategy01_th
)

# триггер цель и вход как в strategy01, но входим во все 3 актива
strategy03 = SmtPspStrategy(
    name="03. Trigger cPSP in wd smt - closest dq no-half target - medium v1 TO+RR filter",
    trade_opener=strategy03_to_constructor(4, 'closed', ['1h', '2h', '4h'],
                                           [5], False, _closest_targets_sorter),
    trades_handler=strategy01_th
)

# триггер цель и вход как в strategy01
# RR от 3.2 до 80, фильтр на aside two tdo и t90mo
strategy04 = SmtPspStrategy(
    name="04. Trigger cPSP in wd smt - closest dq no-half target - medium v2 all TO+RR filter",
    trade_opener=strategy04_to_constructor(
        4, 'closed', ['1h', '2h', '4h'], [5], False,
        _closest_targets_sorter, ['two', 'tdo'], ['two', 'tdo', 't90mo'], 3.2, 80
    ),
    trades_handler=strategy01_th
)

# всё как в strategy02, но входим сразу в 3 актива
strategy05 = SmtPspStrategy(
    name="05. Trigger cPSP in wd smt - closest dq no-half target - medium v1 TO+RR filter",
    trade_opener=strategy03_to_constructor(4, 'closed', ['1h', '2h', '4h'],
                                           [5], False, _furthest_targets_sorter),
    trades_handler=strategy01_th
)

# всё как в strategy04
# RR от 3.2 до 80, фильтр на aside two tdo и t90mo только в выбранном активе, а не во всех
strategy06 = SmtPspStrategy(
    name="06. Trigger cPSP in wd smt - closest dq no-half target - medium v2 asset TO+RR filter",
    trade_opener=strategy04_to_constructor(
        4, 'closed', ['1h', '2h', '4h'], [5], False,
        _closest_targets_sorter, [], ['two', 'tdo', 't90mo'], 3.2, 80
    ),
    trades_handler=strategy01_th
)

# closed wd smt closest dq target, half-targets banned
strategy07 = SmtPspStrategy(
    name="07. Trigger cPSP in wd smt - closest dq no-half target",
    trade_opener=strategy07_to_constructor(
        "", 4, 'closed', ['1h', '2h', '4h'], [5], False,
        _closest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# closed wd smt closest dq target
strategy09 = SmtPspStrategy(
    name="09. Trigger cPSP in wd smt - closest dq target",
    trade_opener=strategy07_to_constructor(
        "", 4, 'closed', ['1h', '2h', '4h'], [5], True,
        _closest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# based on strategy09 2024 - sep 2025 what's good on backtest:
# 1h candles abs_tdo rr 1.1-2.3
# 1h candles t90 rr 0.5-2
# 1h candles med rr 12-20
# 4h candles chase_tmo/chase_two rr 0.3-3.0
# 4h candles mean rr 12-25
# 4h candles med rr 5-11 with entry in London (signal also in London, usually 15min between signal and entry)
strategy10 = SmtPspStrategy(
    name="10. Trigger cPSP in wd smt - closest dq target, post-filter from strategy09 backtest",
    trade_opener=strategy07_to_constructor(
        "", 4, 'closed', ['1h', '4h'], [5], True,
        _closest_targets_sorter, strategy_10_to_pf
    ),
    trades_handler=strategy01_th
)

# closed wd smt closest dq+wd target
strategy11 = SmtPspStrategy(
    name="11. Trigger cPSP in wd smt - closest dq+wd target",
    trade_opener=strategy07_to_constructor(
        "", 4, 'closed', ['1h', '2h', '4h'], [4, 5], True,
        _closest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# based on strategy11 2024 - sep 2025 what's good on backtest:
# chase_tyo
# chase_absent_tdo psp_key == '1h' and target_level == 4 and 0.3 < entry_rr < 2
# chase_absent_tdo psp_key == '4h' and target_level == 4 and 0.3 < entry_rr < 3.6
# chase_absent_two psp_key == '2h' and target_level == 4 and 0.3 < entry_rr < 50
# chase_two psp_key == '1h' and target_level == 4 and 3 < entry_rr < 7
# chase_t90mo signal dq Asia psp_key == '1h' and target_level == 5 and 0.5 < entry_rr < 2.45
# chase_t90mo signal asia or nyam psp_key == '2h' and target_level == 5 and 4 < entry_rr < 9
# chase_median_rr signal asia psp_key == '1h' and target_level == 4 and 6 < entry_rr < 85
# market signal asia and pm psp_key == '1h' and target_level == 4 and 4 < entry_rr < 11
strategy12 = SmtPspStrategy(
    name="12. Trigger cPSP in wd smt - closest dq+wd target - post-filtered on backtest data",
    trade_opener=strategy07_to_constructor(
        "", 4, 'closed', ['1h', '2h', '4h'], [4, 5], True,
        _closest_targets_sorter, strategy_12_to_pf
    ),
    trades_handler=strategy01_th,
)

# closed mw smt closest wd target
strategy13 = SmtPspStrategy(
    name="13. Trigger cPSP in mw smt - closest wd target",
    trade_opener=strategy07_to_constructor(
        "", 3, 'closed', ['4h', '1d'], [4], True,
        _closest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# based on strategy13 2024 - sep 2025 what's good on backtest:
# chase_absent_tyo, chase_tyo (everything)
# chase_median_rr psp_key == '1d' and 0.3 < entry_rr < 1
# chase_t90mo psp_key == '1d' and 0.8 < entry_rr < 2.3
# chase_tdo psp_key == '4h' and 4 < entry_rr < 9
# chase_two psp_key == '4h' and 1.5 < entry_rr < 95
# chase_absent_tdo and 0.3 < entry_rr < 1.5
strategy14 = SmtPspStrategy(
    name="14. Trigger cPSP in mw smt - closest wd target- post-filtered on 2024-2025 backtest results",
    trade_opener=strategy07_to_constructor(
        "", 3, 'closed', ['4h', '1d'], [4], True,
        _closest_targets_sorter, strategy_14_to_pf
    ),
    trades_handler=strategy01_th
)

# всё как в strategy11, но mw smt и mw+wd цели
strategy15 = SmtPspStrategy(
    name="15. Trigger cPSP in mw smt - closest dq+wd target",
    trade_opener=strategy07_to_constructor(
        "", 3, 'closed', ['4h', '1d'], [3, 4], True,
        _closest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# based on strategy15 2024 - sep 2025 what's good on backtest:
# chase_absent_tyo, chase_tyo (everything)
# chase_two, chase_absent_two target 4
# chase_two, chase_absent_two 1d entry > 1.5
# chase_median_rr psp_key == '1d' and target_level == 4 and 1 < entry_rr < 6
# chase_median_rr psp_key == '1d' and target_level == 3 and 2 < entry_rr < 10
# chase_tdo psp_key == '4h' and target_level == 3 and 2.5 < entry_rr < 50
# chase_tdo "psp_key == '4h' and target_level == 4 and 3 < entry_rr < 12",
# chase_absent_tdo psp_key == '1d' and target_level == 4 and 0.5 < entry_rr < 1.5
# chase_absent_t90mo psp_key == '1d' and target_level == 3 and 0.5 < entry_rr < 5
# chase_absent_t90mo psp_key == '1d' and target_level == 4 and 0.4 < entry_rr < 1.3
# chase_t90mo psp_key == '1d' and target_level == 3 and 0.4 < entry_rr < 150
# chase_t90mo psp_key == '1d' and target_level == 4 and 1 < entry_rr < 40
# market psp_key == '1d' and target_level == 3 and 0.5 < entry_rr < 40
# chase_absent_two 3 < entry_rr
# chase_mean_rr psp_key == '1d' and target_level == 4 and 4.3 < entry_rr < 50
strategy16 = SmtPspStrategy(
    name="16. Trigger cPSP in mw smt - closest dq+wd target - post-filtered on 2024-2025 backtest results",
    trade_opener=strategy07_to_constructor(
        "", 3, 'closed', ['4h', '1d'], [3, 4], True,
        _closest_targets_sorter, strategy_16_to_pf
    ),
    trades_handler=strategy01_th
)

# всё как в strategy13, но confirmed свечки
strategy17 = SmtPspStrategy(
    name="17. Trigger CPSP in mw smt - closest wd target",
    trade_opener=strategy07_to_constructor(
        "", 3, 'confirmed', ['4h', '1d'], [4], True,
        _closest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# based on strategy17 2024 - sep 2025 what's good on backtest:
# chase_t90mo psp_key == '4h' and 0.8 < entry_rr < 3.5
# chase_absent_tdo psp_key == '1d'
# chase_tmo psp_key == '1d'
# market psp_key == '1d' and entry > 0.5
# chase_absent_t90mo psp_key == '1d' and entry > 0.6
strategy18 = SmtPspStrategy(
    name="18. Trigger CPSP in mw smt - closest wd target - post-filtered on 2024-2025 backtest results",
    trade_opener=strategy07_to_constructor(
        "", 3, 'confirmed', ['4h', '1d'], [4], True,
        _closest_targets_sorter, strategy_18_to_pf
    ),
    trades_handler=strategy01_th
)

# closed mw smt и mw furhtest цели
strategy19 = SmtPspStrategy(
    name="19. Trigger cPSP in mw smt - furthest mw target",
    trade_opener=strategy07_to_constructor(
        "", 3, 'closed', ['4h', '1d'], [3], True,
        _furthest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# based on strategy17 2024 - sep 2025 what's good on backtest:
# chase_tyo, chase_absent_tyo
# chase_absent_two 4h
# chase_two 1d
# chase_mean_rr psp_key == '1d' and 8 < entry_rr < 25
#
# strategy20 = SmtPspStrategy(
#     name="20. Trigger cPSP in mw smt - furthest mw target - post-filtered on 2024-2025 backtest results",
#     trade_opener=strategy07_to_constructor(
#         "", 3, 'closed', ['4h', '1d'], [3], True,
#         _furthest_targets_sorter, lambda x: x
#     ),
#     trades_handler=strategy01_th
# )

# closed mw smt и mw closest цели
strategy21 = SmtPspStrategy(
    name="21. Trigger cPSP in mw smt - closest mw target",
    trade_opener=strategy07_to_constructor(
        "", 3, 'closed', ['4h', '1d'], [3], True,
        _closest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# confirmed mw smt и mw closest цели
strategy23 = SmtPspStrategy(
    name="23. Trigger CPSP in mw smt - closest mw target",
    trade_opener=strategy07_to_constructor(
        "", 3, 'confirmed', ['4h', '1d'], [3], True,
        _closest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# confirmed mw smt и mw furhtest цели
strategy25 = SmtPspStrategy(
    name="25. Trigger CPSP in mw smt - furthest mw target",
    trade_opener=strategy07_to_constructor(
        "", 3, 'confirmed', ['4h', '1d'], [3], True,
        _furthest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# confirmed wd smt и wd closest цели
strategy27 = SmtPspStrategy(
    name="27. Trigger 4h cPSP in wd smt - furthest dq+wd target",
    trade_opener=strategy07_to_constructor(
        "", 4, 'confirmed', ['1h', '2h', '4h'], [4], True,
        _closest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# confirmed wd 1h 2h 4h свечки, furthest wd + dq цели
strategy29 = SmtPspStrategy(
    name="29. Trigger 1h 2h 4h CPSP in wd smt - furthest dq+wd target",
    trade_opener=strategy07_to_constructor(
        "", 4, 'confirmed', ['1h', '2h', '4h'], [4, 5], True,
        _furthest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# based on strategy29 2024 - sep 2025 what's good on backtest:
# wd targets + 1h candles chase_abs_tdo
# dq targets + 2h candles chase_tdo with rr 0.2 - 0.9
# dq targets + 2h candles chase_two
# dq targets + 2h candles market with rr >4.5
# wd targets + 4h candles chase_two
# dq targets + 4h candles chase_tdo with rr 1.5 - 6.5
# wd targets + 4h candles chase_t90mo with rr 0.5 - 2.5
strategy30 = SmtPspStrategy(
    name="30. Trigger 1h 2h 4h CPSP in wd smt - furthest dq+wd target, post-filter from 2024-2025 backtests",
    trade_opener=strategy07_to_constructor(
        "", 4, 'confirmed', ['1h', '2h', '4h'], [4, 5], True,
        _furthest_targets_sorter, strategy_30_to_pf
    ),
    trades_handler=strategy01_th
)

# mw confirmed 4h + 1d свечки, furthest mw + wd цели
strategy31 = SmtPspStrategy(
    name="31. Trigger 4h + 1d CPSP in mw smt - furthest mw+wd target",
    trade_opener=strategy07_to_constructor(
        "", 3, 'confirmed', ['4h', '1d'], [3, 4], True,
        _furthest_targets_sorter, lambda x: x
    ),
    trades_handler=strategy01_th
)

# based on strategy31 2024 - sep 2025 what's good on backtest:
# - dq targets + 1d psp + t90lo + rr < 0.42
# - mw targets + 1d psp + abs_tdo + rr 0.3-0.8
# - dq targets + 1d psp + med_rr + rr 0.5-2
strategy32 = SmtPspStrategy(
    name="32. Trigger 4h + 1d CPSP in mw smt - furthest mw+wd target + post-filter from strategy31 2024-2025 backtests",
    trade_opener=strategy07_to_constructor(
        "", 3, 'confirmed', ['4h', '1d'], [3, 4], True,
        _furthest_targets_sorter, strategy_32_to_pf
    ),
    trades_handler=strategy01_th
)
