from typing import Tuple

from scripts.run_smt_psp_fronttester import fronttest
from stock_market_research_kit.asset import Candles15mGenerator, TriadCandles15mGenerator
from stock_market_research_kit.db_layer import select_full_days_candles_15m, select_candles_15m
from stock_market_research_kit.smt_psp_strategy import strategy01, \
    strategy02, strategy03, \
    strategy04, strategy06, \
    strategy05, \
    strategy07, strategy17, strategy19, strategy21, strategy23, strategy25, strategy27, strategy29, strategy31, \
    strategy09, strategy11, strategy13, strategy15
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
strategy17_2024_snapshot = "scripts/test_snapshots/strategy_17_2024_btc_eth_sol.json"
strategy18_2024_snapshot = "scripts/test_snapshots/strategy_18_2024_btc_eth_sol.json"
strategy19_2024_snapshot = "scripts/test_snapshots/strategy_19_2024_btc_eth_sol.json"
strategy20_2024_snapshot = "scripts/test_snapshots/strategy_20_2024_btc_eth_sol.json"
strategy21_2024_snapshot = "scripts/test_snapshots/strategy_21_2024_btc_eth_sol.json"
strategy22_2024_snapshot = "scripts/test_snapshots/strategy_22_2024_btc_eth_sol.json"
strategy23_2024_snapshot = "scripts/test_snapshots/strategy_23_2024_btc_eth_sol.json"
strategy24_2024_snapshot = "scripts/test_snapshots/strategy_24_2024_btc_eth_sol.json"
strategy25_2024_snapshot = "scripts/test_snapshots/strategy_25_2024_btc_eth_sol.json"
strategy26_2024_snapshot = "scripts/test_snapshots/strategy_26_2024_btc_eth_sol.json"
strategy27_2024_snapshot = "scripts/test_snapshots/strategy_27_2024_btc_eth_sol.json"
strategy28_2024_snapshot = "scripts/test_snapshots/strategy_28_2024_btc_eth_sol.json"
strategy29_2024_snapshot = "scripts/test_snapshots/strategy_29_2024_btc_eth_sol.json"
strategy30_2024_snapshot = "scripts/test_snapshots/strategy_30_2024_btc_eth_sol.json"
strategy31_2024_snapshot = "scripts/test_snapshots/strategy_31_2024_btc_eth_sol.json"
strategy32_2024_snapshot = "scripts/test_snapshots/strategy_32_2024_btc_eth_sol.json"

strategy01_2025_snapshot = "scripts/test_snapshots/strategy_01_2025_btc_eth_sol.json"
strategy02_2025_snapshot = "scripts/test_snapshots/strategy_02_2025_btc_eth_sol.json"
strategy03_2025_snapshot = "scripts/test_snapshots/strategy_03_2025_btc_eth_sol.json"
strategy04_2025_snapshot = "scripts/test_snapshots/strategy_04_2025_btc_eth_sol.json"
strategy05_2025_snapshot = "scripts/test_snapshots/strategy_05_2025_btc_eth_sol.json"
strategy06_2025_snapshot = "scripts/test_snapshots/strategy_06_2025_btc_eth_sol.json"
strategy07_2025_snapshot = "scripts/test_snapshots/strategy_07_2025_btc_eth_sol.json"
strategy08_2025_snapshot = "scripts/test_snapshots/strategy_08_2025_btc_eth_sol.json"
strategy09_2025_snapshot = "scripts/test_snapshots/strategy_09_2025_btc_eth_sol.json"
strategy10_2025_snapshot = "scripts/test_snapshots/strategy_10_2025_btc_eth_sol.json"
strategy11_2025_snapshot = "scripts/test_snapshots/strategy_11_2025_btc_eth_sol.json"
strategy12_2025_snapshot = "scripts/test_snapshots/strategy_12_2025_btc_eth_sol.json"
strategy13_2025_snapshot = "scripts/test_snapshots/strategy_13_2025_btc_eth_sol.json"
strategy14_2025_snapshot = "scripts/test_snapshots/strategy_14_2025_btc_eth_sol.json"
strategy15_2025_snapshot = "scripts/test_snapshots/strategy_15_2025_btc_eth_sol.json"
strategy16_2025_snapshot = "scripts/test_snapshots/strategy_16_2025_btc_eth_sol.json"
strategy17_2025_snapshot = "scripts/test_snapshots/strategy_17_2025_btc_eth_sol.json"
strategy18_2025_snapshot = "scripts/test_snapshots/strategy_18_2025_btc_eth_sol.json"
strategy19_2025_snapshot = "scripts/test_snapshots/strategy_19_2025_btc_eth_sol.json"
strategy20_2025_snapshot = "scripts/test_snapshots/strategy_20_2025_btc_eth_sol.json"
strategy21_2025_snapshot = "scripts/test_snapshots/strategy_21_2025_btc_eth_sol.json"
strategy22_2025_snapshot = "scripts/test_snapshots/strategy_22_2025_btc_eth_sol.json"
strategy23_2025_snapshot = "scripts/test_snapshots/strategy_23_2025_btc_eth_sol.json"
strategy24_2025_snapshot = "scripts/test_snapshots/strategy_24_2025_btc_eth_sol.json"
strategy25_2025_snapshot = "scripts/test_snapshots/strategy_25_2025_btc_eth_sol.json"
strategy26_2025_snapshot = "scripts/test_snapshots/strategy_26_2025_btc_eth_sol.json"
strategy27_2025_snapshot = "scripts/test_snapshots/strategy_27_2025_btc_eth_sol.json"
strategy28_2025_snapshot = "scripts/test_snapshots/strategy_28_2025_btc_eth_sol.json"
strategy29_2025_snapshot = "scripts/test_snapshots/strategy_29_2025_btc_eth_sol.json"
strategy30_2025_snapshot = "scripts/test_snapshots/strategy_30_2025_btc_eth_sol.json"
strategy31_2025_snapshot = "scripts/test_snapshots/strategy_31_2025_btc_eth_sol.json"
strategy32_2025_snapshot = "scripts/test_snapshots/strategy_32_2025_btc_eth_sol.json"


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
            strategy09,
            strategy11,
            strategy13,
            strategy15,
            strategy17,
            strategy19,
            strategy21,
            strategy23,
            strategy25,
            strategy27,
            strategy29,
            strategy31,
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
    strategy09_trades = closed_trades[strategy09.name]
    strategy11_trades = closed_trades[strategy11.name]
    strategy13_trades = closed_trades[strategy13.name]
    strategy15_trades = closed_trades[strategy15.name]
    strategy17_trades = closed_trades[strategy17.name]
    strategy19_trades = closed_trades[strategy19.name]
    strategy21_trades = closed_trades[strategy21.name]
    strategy23_trades = closed_trades[strategy23.name]
    strategy25_trades = closed_trades[strategy25.name]
    strategy27_trades = closed_trades[strategy27.name]
    strategy29_trades = closed_trades[strategy29.name]
    strategy31_trades = closed_trades[strategy31.name]

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
    with open(strategy09_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy09_trades))
    with open(strategy11_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy11_trades))
    with open(strategy13_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy13_trades))
    with open(strategy15_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy15_trades))
    with open(strategy17_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy17_trades))
    with open(strategy19_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy19_trades))
    with open(strategy21_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy21_trades))
    with open(strategy23_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy23_trades))
    with open(strategy25_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy25_trades))
    with open(strategy27_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy27_trades))
    with open(strategy29_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy29_trades))
    with open(strategy31_2024_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy31_trades))


def backtest_strategy_full_2025():
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    triad = new_triad(
        symbols,
        (
            candles_generator_reverse(symbols[0], 2024, 2025, '2025-01-01 00:00'),
            candles_generator_reverse(symbols[1], 2024, 2025, '2025-01-01 00:00'),
            candles_generator_reverse(symbols[2], 2024, 2025, '2025-01-01 00:00'),
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
            strategy09,
            strategy11,
            strategy13,
            strategy15,
            strategy17,
            strategy19,
            strategy21,
            strategy23,
            strategy25,
            strategy27,
            strategy29,
            strategy31,
        ],
        candles_generator(symbols, 2025, '2025-01-01 00:00', '2025-09-02 00:00'),
        '2025-09-02 00:00'
    )

    strategy01_trades = closed_trades[strategy01.name]
    strategy02_trades = closed_trades[strategy02.name]
    strategy03_trades = closed_trades[strategy03.name]
    strategy04_trades = closed_trades[strategy04.name]
    strategy05_trades = closed_trades[strategy05.name]
    strategy06_trades = closed_trades[strategy06.name]
    strategy07_trades = closed_trades[strategy07.name]
    strategy09_trades = closed_trades[strategy09.name]
    strategy11_trades = closed_trades[strategy11.name]
    strategy13_trades = closed_trades[strategy13.name]
    strategy15_trades = closed_trades[strategy15.name]
    strategy17_trades = closed_trades[strategy17.name]
    strategy19_trades = closed_trades[strategy19.name]
    strategy21_trades = closed_trades[strategy21.name]
    strategy23_trades = closed_trades[strategy23.name]
    strategy25_trades = closed_trades[strategy25.name]
    strategy27_trades = closed_trades[strategy27.name]
    strategy29_trades = closed_trades[strategy29.name]
    strategy31_trades = closed_trades[strategy31.name]

    with open(strategy01_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy01_trades))
    with open(strategy02_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy02_trades))
    with open(strategy03_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy03_trades))
    with open(strategy04_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy04_trades))
    with open(strategy05_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy05_trades))
    with open(strategy06_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy06_trades))
    with open(strategy07_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy07_trades))
    with open(strategy09_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy09_trades))
    with open(strategy11_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy11_trades))
    with open(strategy13_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy13_trades))
    with open(strategy15_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy15_trades))
    with open(strategy17_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy17_trades))
    with open(strategy19_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy19_trades))
    with open(strategy21_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy21_trades))
    with open(strategy23_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy23_trades))
    with open(strategy25_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy25_trades))
    with open(strategy27_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy27_trades))
    with open(strategy29_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy29_trades))
    with open(strategy31_2025_snapshot, "w", encoding="utf-8") as f:
        f.write(json_from_smt_psp_trades(strategy31_trades))


if __name__ == "__main__":
    try:
        backtest_strategy_full_2024()
        # backtest_strategy_full_2025()

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
