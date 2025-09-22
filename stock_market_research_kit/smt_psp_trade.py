import json
from dataclasses import dataclass, asdict, fields
from typing import List, Tuple, Optional, Dict

from stock_market_research_kit.asset import Trends
from stock_market_research_kit.candle import InnerCandle
from stock_market_research_kit.triad import TrueOpen

ACCOUNT_MARGIN_USD = 10000  # TODO сделать SmtPspStrategy stateful, хранить в ней margin_used
FREE_SAFETY_MARGIN_PERCENT = 20  # TODO 20% маржи оставляем неиспользованной чтобы не поймать ликвидацию на slippage
MAX_LEVERAGE = 50  # TODO и не открывать сделки если кончилась маржа (и сами сделки ограничивать по RR таким образом)
MAX_ENTRY = 0.25 * ACCOUNT_MARGIN_USD * MAX_LEVERAGE
ONE_RR_IN_USD = 100
MARKET_ORDER_FEE_PERCENT = 0.045  # Binance default
LIMIT_ORDER_FEE_PERCENT = 0.018  # Binance default


@dataclass
class SmtPspTrade:
    asset: str
    direction: str  # UP or DOWN
    signal_time: str
    signal_time_ny: str
    signal_trends: Trends

    limit_price_history: Optional[List[float]]  # if chasing to_label - can be changed to new to, latest is active
    limit_stop: Optional[float]
    limit_take_profit: Optional[float]
    # limit_deadline: Optional[str]  # TODO
    # limit_deadline_ny: Optional[str]
    limit_rr: Optional[float]
    limit_position_assets: Optional[float]
    limit_position_usd: Optional[float]
    limit_status: Optional[str]  # ACTIVE, FILLED, CANCELLED
    limit_chase_to_label: Optional[str]  # t90mo | tdo | etc
    limit_chase_rr: Optional[float]  # rr to chase

    entry_time: str
    entry_time_ny: str
    entry_price: float
    entry_order_type: str  # MARKET or LIMIT
    stop: float
    take_profit: float
    entry_rr: float
    entry_position_assets: float
    entry_position_usd: float
    entry_position_fee: float

    entry_reason: str
    entry_tos: List[TrueOpen]
    psp_key_used: str
    psp_date: str
    smt_type: str
    smt_level: int
    smt_label: str
    smt_first_appeared: str
    smt_flags: str
    target_level: int
    target_direction: str
    target_label: str
    target_ql_start: str
    entry_trends: Trends

    best_pnl: float
    best_pnl_time: str
    best_pnl_time_ny: str
    best_pnl_price: float
    best_pnl_tos: List[TrueOpen]
    best_pnl_trends: Trends

    best_entry_time: str
    best_entry_time_ny: str
    best_entry_price: float
    best_entry_rr: float
    best_entry_tos: List[TrueOpen]
    best_entry_trends: Trends

    deadline_close: str  # optional

    psp_extremums: Tuple[float, float, float]
    targets: Tuple[float, float, float]

    in_trade_range: Optional[InnerCandle]

    closes: List[
        Tuple[int, float, str, str, str, Trends]
    ]  # list of closes in (position_percent, price, time, time_ny, reason, trends) format
    pnl_usd: float
    close_position_fee: float

    def limit_pre_fill_checker(self, entry_time: str) -> bool:
        if self.asset == "":
            return False
        return True

    def percent_closed(self) -> int:
        return sum([x[0] for x in self.closes])

    def _pnl(self, percent_close: float, close_price: float) -> float:
        part_to_close = percent_close / 100
        if self.direction == 'UP':
            return part_to_close * self.entry_position_assets * (close_price - self.entry_price)
        else:
            return part_to_close * self.entry_position_assets * (self.entry_price - close_price)

    def pnls_per_closes(self) -> List[float]:
        result = []
        for close in self.closes:
            result.append(self._pnl(close[0], close[1]))
        return result

    def pnl_if_full_preclose(self) -> Tuple[float, float]:  # pnl, close_fee
        pnl = self._pnl(100, self.closes[0][1])
        close_price = self.closes[0][1]
        if len(self.closes) == 1:
            pnl = self._pnl(100, self.closes[-1][1])
            close_price = self.closes[-1][1]
        return pnl, self.entry_position_assets * close_price * (MARKET_ORDER_FEE_PERCENT / 100)

    def pnl_if_full_tp_sl(self) -> Tuple[float, float]:  # pnl, close_fee
        close_price = self.closes[-1][1]
        return (self._pnl(100, close_price),
                self.entry_position_assets * close_price * (MARKET_ORDER_FEE_PERCENT / 100))


def smt_psp_trade_decoder(dct: dict):
    field_names = {f.name for f in fields(SmtPspTrade)}
    filtered = {k: v for k, v in dct.items() if k in field_names}
    if "signal_trends" in filtered:
        filtered["signal_trends"] = Trends.from_dict(filtered["signal_trends"])
    if "entry_trends" in filtered:
        filtered["entry_trends"] = Trends.from_dict(filtered["entry_trends"])
    if "best_pnl_trends" in filtered:
        filtered["best_pnl_trends"] = Trends.from_dict(filtered["best_pnl_trends"])
    if "best_entry_trends" in filtered:
        filtered["best_entry_trends"] = Trends.from_dict(filtered["best_entry_trends"])
    if "closes" in filtered:
        filtered["closes"] = [(x[0], x[1], x[2], x[3], x[4], Trends.from_dict(x[5])) for x in filtered["closes"]]
    return SmtPspTrade(**filtered)


def _to_safe_dict(trade: SmtPspTrade) -> Dict[any, any]:
    d = asdict(trade)
    d.pop("limit_pre_fill_checker", None)
    return d


def json_from_smt_psp_trade(trade: SmtPspTrade) -> str:
    return json.dumps(_to_safe_dict(trade), ensure_ascii=False, indent=4)


def json_from_smt_psp_trades(trades: List[SmtPspTrade]) -> str:
    return json.dumps([_to_safe_dict(trade) for trade in trades], ensure_ascii=False, indent=4)


def smt_psp_trades_from_json(json_str: str) -> List[SmtPspTrade]:
    return [smt_psp_trade_decoder(x) for x in json.loads(json_str)]
