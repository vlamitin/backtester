import json
from dataclasses import dataclass, asdict, fields
from typing import List, Tuple, TypeAlias, Optional

from stock_market_research_kit.candle import InnerCandle
from stock_market_research_kit.triad import TrueOpen


@dataclass
class SmtPspTrade:
    asset: str
    direction: str  # UP or DOWN
    entry_price: float
    stop: float
    take_profit: float
    entry_rr: float
    entry_time: str
    entry_time_ny: str
    entry_position_assets: float
    entry_position_usd: float
    entry_position_fee: float
    entry_reason: str
    entry_tos: List[TrueOpen]
    psp_key_used: str
    smt_type: str
    smt_label: str
    smt_flags: str

    best_entry_time: str
    best_entry_time_ny: str
    best_entry_price: float
    best_entry_rr: float
    best_entry_tos: List[TrueOpen]

    psp_extremums: Tuple[float, float, float]

    deadline_close: str  # optional

    targets: Tuple[float, float, float]

    in_trade_range: Optional[InnerCandle]

    closes: List[
        Tuple[float, float, str, str, str]
    ]  # list of closes in (position_percent, price, time, time_ny, reason) format
    pnl_usd: float
    close_position_fee: float


def smt_psp_trade_decoder(dct: dict):
    field_names = {f.name for f in fields(SmtPspTrade)}
    filtered = {k: v for k, v in dct.items() if k in field_names}
    return SmtPspTrade(**filtered)


def json_from_smt_psp_trade(trade: SmtPspTrade) -> str:
    return json.dumps(asdict(trade), ensure_ascii=False, indent=4)


def json_from_smt_psp_trades(trades: List[SmtPspTrade]) -> str:
    return json.dumps([trade.__dict__ for trade in trades], ensure_ascii=False, indent=4)


def smt_psp_trades_from_json(json_str: str) -> List[SmtPspTrade]:
    return [smt_psp_trade_decoder(x) for x in json.loads(json_str)]
