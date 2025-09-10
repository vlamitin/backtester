import time
from datetime import timedelta
from typing import Tuple, List

from scripts.run_smt_psp_fronttester import fronttest
from stock_market_research_kit.asset import Candles15mGenerator, TriadCandles15mGenerator
from stock_market_research_kit.db_layer import select_full_days_candles_15m, select_candles_15m
from stock_market_research_kit.smt_psp_strategy import strategy01, \
    strategy02, strategy03, \
    strategy04, strategy06, \
    strategy05, \
    strategy07, strategy17, strategy19, strategy21, strategy23, strategy25, strategy27, strategy29, strategy31, \
    strategy09, strategy11, strategy13, strategy15, strategy30, strategy32, strategy10, strategy12, strategy14, \
    strategy16, strategy18
from stock_market_research_kit.smt_psp_trade import json_from_smt_psp_trades
from stock_market_research_kit.triad import new_triad
from utils.date_utils import log_warn_ny, to_utc_datetime, now_utc_datetime


def snapshot_file(strategy: str, year: int, symbols: List[str]) -> str:
    smb = '_'.join([x.replace('USDT', '').lower() for x in symbols])
    return f"scripts/test_snapshots/strategy_{strategy}_{year}_{smb}.json"


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
    candles_a1 = []
    candles_a2 = []
    candles_a3 = []

    def update():
        nonlocal candles_a1
        nonlocal candles_a2
        nonlocal candles_a3
        candles_a1 = select_candles_15m(year, symbols[0], from_, to_)
        candles_a2 = select_candles_15m(year, symbols[1], from_, to_)
        candles_a3 = select_candles_15m(year, symbols[2], from_, to_)

    update()
    index = 0

    while True:
        if len(candles_a1) <= index:
            last_candle_date = to_utc_datetime(candles_a1[index - 1][5])
            now_utc = now_utc_datetime()
            sleep_seconds = (last_candle_date + timedelta(minutes=15, seconds=5) - now_utc).total_seconds()
            if sleep_seconds < 0:
                sleep_seconds = 15 * 60
            log_warn_ny(
                f"Sleeping {sleep_seconds} seconds to try one more time to get new candle")
            time.sleep(sleep_seconds)
            update()
        else:
            yield candles_a1[index], candles_a2[index], candles_a3[index]
            index = index + 1


def backtest_strategies(year: int, from_: str, to_: str, symbols: Tuple[str, str, str]):
    triad = new_triad(
        symbols,
        (
            candles_generator_reverse(symbols[0], year - 1, year, from_),
            candles_generator_reverse(symbols[1], year - 1, year, from_),
            candles_generator_reverse(symbols[2], year - 1, year, from_),
        )
    )

    strategies = [
        strategy01,
        strategy02,
        strategy03,
        strategy04,
        strategy05,
        strategy06,
        strategy07,
        strategy09,
        strategy10,
        strategy11,
        strategy12,
        strategy13,
        strategy14,
        strategy15,
        strategy16,
        strategy17,
        strategy18,
        strategy19,
        strategy21,
        strategy23,
        strategy25,
        strategy27,
        strategy29,
        strategy30,
        strategy31,
        strategy32,
    ]

    closed_trades = fronttest(
        triad,
        strategies,
        candles_generator(symbols, year, from_, to_),
        to_
    )

    for s_name in closed_trades:
        with open(snapshot_file(s_name[0:2], year, list(symbols)), "w", encoding="utf-8") as f:
            f.write(json_from_smt_psp_trades(closed_trades[s_name]))


if __name__ == "__main__":
    try:
        backtest_strategies(2023, '2023-01-01 00:00', '2024-01-01 00:00', ('BTCUSDT', 'ETHUSDT', 'SOLUSDT'))
        # backtest_strategies(2024, '2024-01-01 00:00', '2025-01-01 00:00', ('BTCUSDT', 'ETHUSDT', 'SOLUSDT'))
        # backtest_strategies(2025, '2025-01-01 00:00', '2025-09-08 00:00', ('BTCUSDT', 'ETHUSDT', 'SOLUSDT'))
        print('results!')

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
