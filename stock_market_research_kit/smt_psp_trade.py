import json
from dataclasses import dataclass, asdict
from typing import List, Tuple


@dataclass
class SmtPspTrade:
    asset: str
    entry_time: str
    entry_time_ny: str
    entry_price: float
    entry_position_assets: float
    entry_position_usd: float
    entry_position_fee: float
    entry_rr: float
    entry_reason: str
    psp_key_used: str

    direction: str  # UP or DOWN

    stop: float
    psp_extremums: Tuple[float, float, float]

    deadline_close: str  # optional

    take_profit: float
    targets: Tuple[float, float, float]

    closes: List[
        Tuple[float, float, str, str, str]
    ]  # list of closes in (position_percent, price, time, time_ny, reason) format
    pnl_usd: float
    close_position_fee: float


def smt_psp_trade_decoder(dct):
    return SmtPspTrade(**dct)


def json_from_smt_psp_trade(trade: SmtPspTrade) -> str:
    return json.dumps(asdict(trade), ensure_ascii=False, indent=4)


def json_from_smt_psp_trades(trades: List[SmtPspTrade]) -> str:
    return json.dumps([trade.__dict__ for trade in trades], ensure_ascii=False, indent=4)


def smt_psp_trades_from_json(json_str: str) -> List[SmtPspTrade]:
    return [smt_psp_trade_decoder(x) for x in json.loads(json_str)]
