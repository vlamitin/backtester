import time
from datetime import datetime, timedelta
from typing import Generator, List, Tuple
from zoneinfo import ZoneInfo

from scripts.run_series_raw_loader import update_candle_from_binance
from stock_market_research_kit.db_layer import last_candle_15m, select_full_days_candles_15m, select_candles_15m
from stock_market_research_kit.triad import Reverse15mGenerator, new_triad, Triad
from utils.date_utils import log_info_ny, now_ny_datetime, now_utc_datetime, to_ny_datetime, to_utc_datetime, \
    to_date_str, ny_zone


def update_candles(symbols):
    for symbol in symbols:
        update_candle_from_binance(symbol)


def handle_new_candle(triad: Triad):
    prev_smt_psp = triad.actual_smt_psp()
    last_date = to_utc_datetime(triad.a1.snapshot_date_readable)
    last_a1_candle = last_candle_15m(triad.a1.symbol, last_date.year)  # TODO проверить при смене года
    last_a2_candle = last_candle_15m(triad.a2.symbol, last_date.year)
    last_a3_candle = last_candle_15m(triad.a3.symbol, last_date.year)

    triad.a1.plus_15m(last_a1_candle)
    triad.a2.plus_15m(last_a2_candle)
    triad.a3.plus_15m(last_a3_candle)
    smt_psp = triad.actual_smt_psp()



def time_15m_generator(start_date: datetime) -> Generator[datetime, None, None]:
    current_date = start_date
    while True:
        current_date += timedelta(minutes=15)
        now_utc = now_utc_datetime()

        if current_date > now_utc:
            time.sleep((current_date - now_utc).total_seconds())
        yield current_date


def reverse_candles_generator(symbol, last_date: datetime) -> Reverse15mGenerator:
    candles_prev_year = select_full_days_candles_15m(last_date.year - 1, symbol)
    candles_this_year = select_candles_15m(
        last_date.year, symbol,
        to_date_str(last_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)),
        to_date_str(last_date + timedelta(minutes=15)),
    )
    candles = candles_prev_year + candles_this_year

    index = len(candles) - 1

    while True:
        yield candles[index]
        index = index - 1


def run_notifier(a1, a2, a3: str):
    log_info_ny(f"Starting SMT/PSP Notifier for triad: {a1}-{a2}-{a3}")
    start_time = time.perf_counter()
    update_candles([a1, a2, a3])
    last_a1_candle = last_candle_15m(a1, now_utc_datetime().year)

    triad = new_triad(
        (a1, a2, a3),
        (
            reverse_candles_generator(a1, to_utc_datetime(last_a1_candle[5])),
            reverse_candles_generator(a2, to_utc_datetime(last_a1_candle[5])),
            reverse_candles_generator(a3, to_utc_datetime(last_a1_candle[5])),
        )
    )

    gen = time_15m_generator(
        # datetime(2025, 5, 20, 6, 58, tzinfo=ZoneInfo("America/New_York")).astimezone(ZoneInfo("UTC")),
        to_utc_datetime(last_a1_candle[5]),
    )

    log_info_ny(f"SMT/PSP Notifier starting took {(time.perf_counter() - start_time):.6f} seconds")

    while True:
        next_dt_utc = next(gen)
        print(
            f"handling next_dt_utc {to_date_str(next_dt_utc)} ({to_date_str(next_dt_utc.astimezone(ny_zone))})")
        handling_start = time.perf_counter()
        update_candles([a1, a2, a3])
        handle_new_candle(triad)

        log_info_ny(f"next_dt_utc handling took {(time.perf_counter() - handling_start):.6f} seconds")


if __name__ == "__main__":
    try:
        run_notifier("BTCUSDT", "ETHUSDT", "SOLUSDT")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
