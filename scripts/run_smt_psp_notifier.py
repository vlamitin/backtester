import time
from datetime import datetime, timedelta
from typing import Generator, List, Tuple
from zoneinfo import ZoneInfo

from scripts.run_series_raw_loader import update_candle_from_binance
from stock_market_research_kit.db_layer import last_candle_15m, select_full_days_candles_15m, select_candles_15m
from stock_market_research_kit.tg_notifier import TelegramThrottler
from stock_market_research_kit.triad import Reverse15mGenerator, new_triad, Triad, smt_dict_new_smt_found, \
    smt_dict_psp_changed, smt_dict_readable, smt_readable
from utils.date_utils import log_info_ny, now_ny_datetime, now_utc_datetime, to_ny_datetime, to_utc_datetime, \
    to_date_str, ny_zone, to_ny_date_str


def update_candles(symbols, last_date_utc):
    for symbol in symbols:
        update_candle_from_binance(symbol, last_date_utc)


def handle_new_candle(triad: Triad):
    prev_smt_psp = triad.actual_smt_psp()
    last_date = to_utc_datetime(triad.a1.snapshot_date_readable)
    last_a1_candle = last_candle_15m(last_date.year, triad.a1.symbol)  # TODO проверить при смене года
    last_a2_candle = last_candle_15m(last_date.year, triad.a2.symbol)
    last_a3_candle = last_candle_15m(last_date.year, triad.a3.symbol)

    triad.a1.plus_15m(last_a1_candle)
    triad.a2.plus_15m(last_a2_candle)
    triad.a3.plus_15m(last_a3_candle)
    smt_psp = triad.actual_smt_psp()

    new_smt_d = smt_dict_new_smt_found(prev_smt_psp, smt_psp)
    psp_changed = smt_dict_psp_changed(prev_smt_psp, smt_psp)
    if len(new_smt_d) == 0 and len(psp_changed) == 0:
        return

    snap_date, candle_date = to_ny_date_str(triad.a1.snapshot_date_readable), to_ny_date_str(triad.a1.prev_15m_candle[5])
    message = f"Now <b>{snap_date}</b> (last 15m candle is {candle_date})*\n"
    if len(new_smt_d) > 0:
        for key in new_smt_d:
            smt_tuple = new_smt_d.get(key, None)
            if smt_tuple is None:
                continue
            high, half, low = smt_tuple
            for smt in [high, half, low]:
                if smt is not None:
                    message += f"\nNew {smt_readable(smt, key, triad)}"
        message += "\n"

    for p_change in psp_changed:
        dt = to_ny_date_str(p_change[3])
        message += f"\n{p_change[4].capitalize()} {p_change[2]} PSP on {dt} for {p_change[1]} {p_change[0]}"
    if len(psp_changed) > 0:
        message += "\n"

    message += f"""
<blockquote>{smt_dict_readable(smt_psp, triad)}</blockquote>

* <code>all time here and below is in NY timezone</code>
"""

    print('TODO diff!')
    TelegramThrottler().send_signal_message(message)


def time_15m_generator(start_date: datetime) -> Generator[datetime, None, None]:
    current_date = start_date
    while True:
        current_date += timedelta(minutes=15)
        now_utc = now_utc_datetime()

        if current_date > now_utc:
            time.sleep((current_date - now_utc).total_seconds())
        yield current_date


def reverse_candles_generator(symbol, last_date_utc: datetime) -> Reverse15mGenerator:
    candles_prev_year = select_full_days_candles_15m(last_date_utc.year - 1, symbol)
    candles_this_year = select_candles_15m(
        last_date_utc.year, symbol,
        to_date_str(last_date_utc.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)),
        to_date_str(last_date_utc + timedelta(minutes=15)),
    )
    candles = candles_prev_year + candles_this_year

    index = len(candles) - 1

    while True:
        yield candles[index]
        index = index - 1


def run_notifier(a1, a2, a3: str, last_date_utc: datetime):
    log_info_ny(f"Starting SMT/PSP Notifier for triad: {a1}-{a2}-{a3}")
    start_time = time.perf_counter()
    update_candles([a1, a2, a3], last_date_utc)
    last_a1_candle = last_candle_15m(last_date_utc.year, a1)

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
        to_utc_datetime(last_a1_candle[5]) + timedelta(minutes=15),
    )

    log_info_ny(f"SMT/PSP Notifier starting took {(time.perf_counter() - start_time):.6f} seconds")

    while True:
        next_dt_utc = next(gen)
        log_info_ny(
            f"handling next_dt_utc {to_date_str(next_dt_utc)} UTC ({to_ny_date_str(to_date_str(next_dt_utc))} NY)")
        handling_start = time.perf_counter()
        update_candles([a1, a2, a3], next_dt_utc)
        handle_new_candle(triad)

        log_info_ny(f"next_dt_utc handling took {(time.perf_counter() - handling_start):.6f} seconds")


if __name__ == "__main__":
    try:
        last_date = to_utc_datetime(last_candle_15m(2025, "BTCUSDT")[5]) + timedelta(minutes=15)
        run_notifier("BTCUSDT", "ETHUSDT", "SOLUSDT", last_date)
    except KeyboardInterrupt:
        log_info_ny(f"KeyboardInterrupt, exiting ...")
        quit(0)
