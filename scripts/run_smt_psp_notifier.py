import time
from datetime import datetime, timedelta
from typing import Generator, List, Tuple
from zoneinfo import ZoneInfo

from scripts.run_series_raw_loader import update_candle_from_binance
from stock_market_research_kit.db_layer import last_candle_15m, select_full_days_candles_15m, select_candles_15m
from stock_market_research_kit.tg_notifier import TelegramThrottler
from stock_market_research_kit.triad import Candles15mGenerator, new_triad, Triad, new_smt_found, \
    smt_dict_psp_changed, smt_dict_readable, smt_readable, smt_dict_old_smt_cancelled, \
    targets_readable, targets_reached, true_opens_readable, targets_new_appeared
from utils.date_utils import log_info_ny, now_ny_datetime, now_utc_datetime, to_ny_datetime, to_utc_datetime, \
    to_date_str, ny_zone, to_ny_date_str


def update_candles(symbols, last_date_utc):
    for symbol in symbols:
        update_candle_from_binance(symbol, last_date_utc)


def handle_new_candle(triad: Triad):
    prev_smt_psp = triad.actual_smt_psp()
    prev_long_targets = triad.long_targets()
    prev_short_targets = triad.short_targets()
    last_date_utc = to_utc_datetime(triad.a1.snapshot_date_readable)
    last_a1_candle = last_candle_15m(last_date_utc.year, triad.a1.symbol)  # TODO проверить при смене года
    last_a2_candle = last_candle_15m(last_date_utc.year, triad.a2.symbol)
    last_a3_candle = last_candle_15m(last_date_utc.year, triad.a3.symbol)

    triad.a1.plus_15m(last_a1_candle)
    triad.a2.plus_15m(last_a2_candle)
    triad.a3.plus_15m(last_a3_candle)
    smt_psp = triad.actual_smt_psp()

    long_targets = triad.long_targets()
    short_targets = triad.short_targets()

    new_smts = new_smt_found(prev_smt_psp, smt_psp)
    cancelled_smts = smt_dict_old_smt_cancelled(prev_smt_psp, smt_psp)
    psp_changed = smt_dict_psp_changed(prev_smt_psp, smt_psp)
    reached_short_targets = targets_reached(
        (triad.a1.prev_15m_candle, triad.a2.prev_15m_candle, triad.a3.prev_15m_candle),
        prev_short_targets, short_targets)
    reached_long_targets = targets_reached(
        (triad.a1.prev_15m_candle, triad.a2.prev_15m_candle, triad.a3.prev_15m_candle),
        prev_long_targets, long_targets)

    new_long_targets = targets_new_appeared(prev_long_targets, long_targets)
    new_short_targets = targets_new_appeared(prev_short_targets, short_targets)
    tos = triad.true_opens()

    nothing_changed = (len(new_smts) == 0 and len(psp_changed) == 0 and len(cancelled_smts) == 0 and
                       len(reached_short_targets) == 0 and len(reached_long_targets) == 0 and
                       len(new_long_targets) == 0 and len(new_short_targets) == 0)
    if nothing_changed:
        log_info_ny("nothing changed")
        return

    snap_date, candle_date = to_ny_date_str(
        triad.a1.snapshot_date_readable), to_ny_date_str(triad.a1.prev_15m_candle[5])
    header = f"Now <b>{snap_date}</b> (last 15m candle is {candle_date})*\n"

    symbols = (triad.a1.symbol, triad.a2.symbol, triad.a3.symbol)
    if len(reached_long_targets) > 0:
        for _, direction, label, asset_index, price in reached_long_targets:
            header += f"\n✅↗{symbols[asset_index]} {direction} {label} ({round(price, 3)}) target reached"
        header += "\n"

    if len(reached_short_targets) > 0:
        for _, direction, label, asset_index, price in reached_short_targets:
            header += f"\n✅↘{symbols[asset_index]} {direction} {label} ({round(price, 3)}) target reached"
        header += "\n"

    if len(new_short_targets) > 0:
        header += f"""
↘🎯 New short targets:
{targets_readable(new_short_targets)}        
"""

    if len(new_long_targets) > 0:
        header += f"""
↗🎯 New long targets:
{targets_readable(new_long_targets)}        
"""

    smt_emoji_dict = {
        'high': '↘',
        'half_high': '↘',
        'low': '↗️',
        'half_low': '↗️'
    }
    if len(new_smts) > 0:
        for _, label, smt in new_smts:
            header += f"\n🆕{smt_emoji_dict[smt.type]} New {smt_readable(smt, label, triad)}"
        header += "\n"

    if len(cancelled_smts) > 0:
        for _, label, smt in cancelled_smts:
            header += f"\n🚫{smt_emoji_dict[smt.type]} Cancelled {smt.type.capitalize()} {label}"
        header += "\n"

    if len(psp_changed) > 0:
        psp_emoji_dict = {
            'possible': '❓',
            'closed': '🔚',
            'confirmed': '☑️️',
            'swept': '🛑'
        }
        grouped_psp = {}
        for _, smt_key, smt_type, psp_key, psp_date, change in psp_changed:
            if change + psp_key + psp_date not in grouped_psp:
                grouped_psp[change + psp_key + psp_date] = []
            grouped_psp[change + psp_key + psp_date].append((smt_key, smt_type, psp_key, psp_date, change))

        for key in sorted(grouped_psp.keys()):  # we sort for 'possible' to be in the end
            _, smt_type, psp_key, psp_date, change = grouped_psp[key][0]
            dt = to_ny_date_str(psp_date)
            msg = f"\n{psp_emoji_dict[change]}{smt_emoji_dict[smt_type]} {change.capitalize()} {psp_key} PSP on {dt} for "
            msg += " and ".join([f"{x[1]} {x[0]}" for x in grouped_psp[key]])
            header += msg
        header += "\n"

    smt_short, smt_long = smt_dict_readable(smt_psp, triad)
    short_targets_msg = f"""<blockquote>↘🎯 <b>Short targets</b> for {triad.a1.symbol}-{triad.a2.symbol}-{triad.a3.symbol}:

{targets_readable(short_targets)}</blockquote>"""
    short_smts_msg = f"""<blockquote>↘🔀 <b>Short SMTs:</b>

{smt_short}</blockquote>"""

    long_targets_msg = f"""<blockquote>↗🎯 <b>Long targets</b> for {triad.a1.symbol}-{triad.a2.symbol}-{triad.a3.symbol}:

{targets_readable(long_targets)}</blockquote>"""
    long_smts_msg = f"""<blockquote>↗🔀 <b>Long SMTs:</b>

{smt_long}</blockquote>"""

    footer = """
* <code>all time here and below is in NY timezone</code>
    """

    messages = [""]
    messages[-1] += header
    messages[-1] += f"""
<b>True opens:</b>
<pre>{true_opens_readable(tos)}</pre>"""

    if len(messages[-1]) + len(short_targets_msg) > 4096 - 8:
        messages.append("")
    else:
        messages[-1] += "\n"
    messages[-1] += short_targets_msg

    if len(messages[-1]) + len(short_smts_msg) > 4096 - 8:
        messages.append("")
    else:
        messages[-1] += "\n"
    messages[-1] += short_smts_msg

    if len(messages[-1]) + len(long_targets_msg) > 4096 - 8:
        messages.append("")
    else:
        messages[-1] += "\n"
        messages[-1] += "\n"
    messages[-1] += long_targets_msg

    if len(messages[-1]) + len(long_smts_msg) > 4096 - 8:
        messages.append("")
    else:
        messages[-1] += "\n"
        messages[-1] += "\n"
    messages[-1] += long_smts_msg

    if len(messages[-1]) + len(footer) > 4096 - 8:
        messages.append("")
    else:
        messages[-1] += "\n"
    messages[-1] += footer

    for msg in messages:
        TelegramThrottler().send_signal_message(msg)


def time_15m_generator(start_date: datetime) -> Generator[datetime, None, None]:
    current_date = start_date
    while True:
        current_date += timedelta(minutes=15)
        now_utc = now_utc_datetime()

        if current_date > now_utc:
            sleep_seconds = (current_date - now_utc).total_seconds()
            log_info_ny(
                f"Sleeping {sleep_seconds} seconds until {to_date_str(current_date)} UTC ({to_ny_date_str(to_date_str(current_date))} NY)")
            time.sleep(sleep_seconds)
        yield current_date


def reverse_candles_generator(symbol, last_date_utc: datetime) -> Candles15mGenerator:
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
