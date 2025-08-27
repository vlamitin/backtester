from typing import Tuple

from scripts.run_smt_psp_fronttester import fronttest
from stock_market_research_kit.asset import Candles15mGenerator, TriadCandles15mGenerator
from stock_market_research_kit.db_layer import select_full_days_candles_15m, select_candles_15m
from stock_market_research_kit.smt_psp_strategy import strategy01, \
    strategy02, strategy03, \
    strategy04, strategy06, \
    strategy05, \
    strategy07, \
    strategy08, strategy09, strategy10, strategy11, strategy12, strategy13, strategy14, strategy15, strategy16
from stock_market_research_kit.smt_psp_trade import json_from_smt_psp_trades
from stock_market_research_kit.triad import new_triad

strategy01_2024_snapshot = "scripts/test_snapshots/strategy_01_2024_btc_eth_sol.json"
strategy02_2024_snapshot = "scripts/test_snapshots/strategy_02_2024_btc_eth_sol.json"
strategy03_2024_snapshot = "scripts/test_snapshots/strategy_03_2024_btc_eth_sol.json"
strategy04_2024_snapshot = "scripts/test_snapshots/strategy_04_2024_btc_eth_sol.json"
strategy05_2024_snapshot = "scripts/test_snapshots/strategy_05_2024_btc_eth_sol.json"
strategy06_2024_snapshot = "scripts/test_snapshots/strategy_06_2024_btc_eth_sol.json"
strategy07_2024_snapshot = "scripts/test_snapshots/strategy_07_2024_btc_eth_sol.json"
strategy08_2024_snapshot = "scripts/test_snapshots/strategy_08_2024_btc_eth_sol.json"
strategy09_2024_snapshot = "scripts/test_snapshots/strategy_09_2024_btc_eth_sol.json"
strategy10_2024_snapshot = "scripts/test_snapshots/strategy_10_2024_btc_eth_sol.json"
strategy11_2024_snapshot = "scripts/test_snapshots/strategy_11_2024_btc_eth_sol.json"
strategy12_2024_snapshot = "scripts/test_snapshots/strategy_12_2024_btc_eth_sol.json"
strategy13_2024_snapshot = "scripts/test_snapshots/strategy_13_2024_btc_eth_sol.json"
strategy14_2024_snapshot = "scripts/test_snapshots/strategy_14_2024_btc_eth_sol.json"
strategy15_2024_snapshot = "scripts/test_snapshots/strategy_15_2024_btc_eth_sol.json"
strategy16_2024_snapshot = "scripts/test_snapshots/strategy_16_2024_btc_eth_sol.json"

strategy01_2025_snapshot = "scripts/test_snapshots/strategy_01_2025_btc_eth_sol.json"
strategy02_2025_snapshot = "scripts/test_snapshots/strategy_02_2025_btc_eth_sol.json"
strategy03_2025_snapshot = "scripts/test_snapshots/strategy_03_2025_btc_eth_sol.json"
strategy04_2025_snapshot = "scripts/test_snapshots/strategy_04_2025_btc_eth_sol.json"


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
            strategy01,
            strategy02,
            strategy03,
            strategy04,
            strategy05,
            strategy06,
            strategy07,
            strategy08,
            strategy09,
            strategy10,
            strategy11,
            strategy12,
            strategy13,
            strategy14,
            strategy15,
            strategy16,
        ],
        candles_generator(symbols, 2024, '2024-01-01 00:00', '2025-01-01 00:00'),
        '2025-01-01 00:00'
    )

    strategy01_trades = closed_trades[strategy01.name]
    strategy02_trades = closed_trades[strategy02.name]
    strategy03_trades = closed_trades[strategy03.name]
    strategy04_trades = closed_trades[strategy04.name]
    strategy05_trades = closed_trades[strategy05.name]
    strategy06_trades = closed_trades[strategy06.name]
    strategy07_trades = closed_trades[strategy07.name]
    strategy08_trades = closed_trades[strategy08.name]
    strategy09_trades = closed_trades[strategy09.name]
    strategy10_trades = closed_trades[strategy10.name]
    strategy11_trades = closed_trades[strategy11.name]
    strategy12_trades = closed_trades[strategy12.name]
    strategy13_trades = closed_trades[strategy13.name]
    strategy14_trades = closed_trades[strategy14.name]
    strategy15_trades = closed_trades[strategy15.name]
    strategy16_trades = closed_trades[strategy16.name]

    with open(strategy01_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy01_trades))
    with open(strategy02_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy02_trades))
    with open(strategy03_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy03_trades))
    with open(strategy04_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy04_trades))
    with open(strategy05_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy05_trades))
    with open(strategy06_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy06_trades))
    with open(strategy07_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy07_trades))
    with open(strategy08_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy08_trades))
    with open(strategy09_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy09_trades))
    with open(strategy10_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy10_trades))
    with open(strategy11_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy11_trades))
    with open(strategy12_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy12_trades))
    with open(strategy13_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy13_trades))
    with open(strategy14_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy14_trades))
    with open(strategy15_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy15_trades))
    with open(strategy16_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy16_trades))


if __name__ == "__main__":
    try:
        backtest_strategy_full_2024()

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
