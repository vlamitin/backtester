import json
from dataclasses import dataclass, asdict, fields
from typing import List, Tuple, TypeAlias, Optional, Callable, Dict

from stock_market_research_kit.candle import InnerCandle
from stock_market_research_kit.triad import TrueOpen


@dataclass
class SmtPspTrade:
    asset: str
    direction: str  # UP or DOWN
    signal_time: str
    signal_time_ny: str

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

    best_pnl: float
    best_pnl_time: str
    best_pnl_time_ny: str
    best_pnl_price: float
    best_pnl_tos: List[TrueOpen]

    best_entry_time: str
    best_entry_time_ny: str
    best_entry_price: float
    best_entry_rr: float
    best_entry_tos: List[TrueOpen]

    deadline_close: str  # optional

    psp_extremums: Tuple[float, float, float]
    targets: Tuple[float, float, float]

    _in_trade_range: Optional[InnerCandle]

    closes: List[
        Tuple[int, float, str, str, str]
    ]  # list of closes in (position_percent, price, time, time_ny, reason) format
    pnl_usd: float
    close_position_fee: float

    def limit_pre_fill_checker(self, entry_time: str) -> bool:
        if self.asset == "":
            return False
        return True

    def percent_closed(self) -> int:
        return sum([x[0] for x in self.closes])

    def pnls_per_closes(self) -> List[float]:
        result = []
        for close in self.closes:
            part_to_close = close[0] / 100
            if self.direction == 'UP':
                result.append(part_to_close * self.entry_position_assets * (close[1] - self.entry_price))
            else:
                result.append(part_to_close * self.entry_position_assets * (self.entry_price - close[1]))
        return result


def smt_psp_trade_decoder(dct: dict):
    field_names = {f.name for f in fields(SmtPspTrade)}
    filtered = {k: v for k, v in dct.items() if k in field_names}
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
