import json
from typing import Tuple

from scripts.run_smt_psp_fronttester import fronttest
from stock_market_research_kit.asset import Candles15mGenerator, TriadCandles15mGenerator
from stock_market_research_kit.db_layer import select_full_days_candles_15m, select_candles_15m
from stock_market_research_kit.smt_psp_strategy import strategy01_wq_smt_conservative, \
    strategy02_wq_smt_moderate_conservative, strategy03_wq_smt_conservative_3_assets, \
    strategy04_wq_smt_conservative_moderate_rr_to_filters, strategy06_wq_smt_conservative_moderate_rr_to_filters, \
    strategy05_wq_smt_conservative_moderate_3_assets, strategy07_wq_smt_conservative_3_assets
from stock_market_research_kit.smt_psp_trade import json_from_smt_psp_trades
from stock_market_research_kit.triad import new_triad

strategy01_2024_snapshot = "scripts/test_snapshots/strategy_1_2024_btc_eth_sol.json"
strategy02_2024_snapshot = "scripts/test_snapshots/strategy_2_2024_btc_eth_sol.json"
strategy03_2024_snapshot = "scripts/test_snapshots/strategy_3_2024_btc_eth_sol.json"
strategy04_2024_snapshot = "scripts/test_snapshots/strategy_4_2024_btc_eth_sol.json"
strategy05_2024_snapshot = "scripts/test_snapshots/strategy_5_2024_btc_eth_sol.json"
strategy06_2024_snapshot = "scripts/test_snapshots/strategy_6_2024_btc_eth_sol.json"
strategy07_2024_snapshot = "scripts/test_snapshots/strategy_7_2024_btc_eth_sol.json"

strategy01_2025_snapshot = "scripts/test_snapshots/strategy_1_2025_btc_eth_sol.json"
strategy02_2025_snapshot = "scripts/test_snapshots/strategy_2_2025_btc_eth_sol.json"
strategy03_2025_snapshot = "scripts/test_snapshots/strategy_3_2025_btc_eth_sol.json"
strategy04_2025_snapshot = "scripts/test_snapshots/strategy_4_2025_btc_eth_sol.json"


def candles_generator_reverse(symbol: str, from_year: int, to_year: int, to_: str) -> Candles15mGenerator:
    candles_year = select_full_days_candles_15m(from_year, symbol)
    first_candles_next = select_candles_15m(
        to_year, symbol, f'{to_year}-01-01 00:00', to_
    )
    candles = candles_year + first_candles_next

    index = len(candles) - 1

    while True:
        yield candles[index]
        index = index - 1


def candles_generator(symbols: Tuple[str, str, str], year: int, from_: str, to_: str) -> TriadCandles15mGenerator:
    candles_a1 = select_candles_15m(year, symbols[0], from_, to_)
    candles_a2 = select_candles_15m(year, symbols[1], from_, to_)
    candles_a3 = select_candles_15m(year, symbols[2], from_, to_)

    index = 0

    while True:
        yield candles_a1[index], candles_a2[index], candles_a3[index]
        index = index + 1


def backtest_strategy_full_2024():
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    triad = new_triad(
        symbols,
        (
            candles_generator_reverse(symbols[0], 2023, 2024, '2024-01-01 00:00'),
            candles_generator_reverse(symbols[1], 2023, 2024, '2024-01-01 00:00'),
            candles_generator_reverse(symbols[2], 2023, 2024, '2024-01-01 00:00'),
        )
    )

    closed_trades = fronttest(
        triad,
        [
            strategy03_wq_smt_conservative_3_assets,
            strategy05_wq_smt_conservative_moderate_3_assets,
            strategy07_wq_smt_conservative_3_assets,
        ],
        candles_generator(symbols, 2024, '2024-01-01 00:00', '2025-01-01 00:00'),
        '2025-01-01 00:00'
    )

    strategy03_trades = closed_trades[strategy03_wq_smt_conservative_3_assets.name]
    strategy05_trades = closed_trades[strategy05_wq_smt_conservative_moderate_3_assets.name]
    strategy07_trades = closed_trades[strategy07_wq_smt_conservative_3_assets.name]

    with open(strategy03_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy03_trades))
    with open(strategy05_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy05_trades))
    with open(strategy07_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy07_trades))


if __name__ == "__main__":
    try:
        backtest_strategy_full_2024()

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
