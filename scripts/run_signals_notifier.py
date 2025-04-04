import sqlite3
import time
from datetime import datetime, timedelta
from time import sleep
from typing import List
from zoneinfo import ZoneInfo

from scripts.run_day_markuper import markup_days, as_1_candle, london_from_to
from scripts.run_series_raw_loader import update_candle_from_binance
from scripts.run_sessions_backtester import look_for_entry_backtest, look_for_close_backtest, get_backtested_profiles
from scripts.run_sessions_typifier import typify_sessions, typify_session
from scripts.setup_db import connect_to_db
from stock_market_research_kit.notifier_strategy import btc_naive_strategy, NotifierStrategy
from stock_market_research_kit.session import get_next_session_mock
from stock_market_research_kit.session_trade import SessionTrade, session_trade_from_json, json_from_session_trade
from stock_market_research_kit.tg_notifier import post_signal_notification
from utils.date_utils import now_ny_datetime, now_utc_datetime, \
    start_of_day, to_date_str, log_warn, to_utc_datetime, log_info_ny


def get_last_day_candles(symbol):
    now_year = now_utc_datetime().year  # TODO не будет работать при смене года

    connection = connect_to_db(now_year)
    c = connection.cursor()

    # time_from = to_date_str(start_of_day(now_utc_datetime()) - timedelta(hours=2))
    # time_to = to_date_str(now_utc_datetime())

    c.execute("""WITH last_row_date_ts AS (
    SELECT date_ts last_row_date FROM raw_candles
    WHERE symbol = ? AND period = ?
    ORDER BY strftime('%s', date_ts) DESC
    LIMIT 1
),
     time_range AS (
         SELECT
             (strftime('%s', last_row_date) - 86400) / 86400 * 86400 + 22 * 3600 AS start_time_sec,
             strftime('%s', last_row_date) AS end_time_sec
         FROM last_row_date_ts
     )
SELECT open, high, low, close, volume, date_ts FROM raw_candles
WHERE strftime('%s', date_ts) + 0 BETWEEN (SELECT start_time_sec FROM time_range) AND (SELECT end_time_sec FROM time_range)
    AND symbol = ? AND period = ?""", (symbol, "15m", symbol, "15m"))
    rows_15m = c.fetchall()
    connection.close()
    if len(rows_15m) == 0:
        print(f"Symbol {symbol} 15m not found in DB")
        return []

    return [[x[0], x[1], x[2], x[3], x[4], x[5]] for x in rows_15m]


def to_trade_profile(backtest_profile_key):
    sessions_str, stats_str = backtest_profile_key.split(': ')
    typed_sessions = sessions_str.split(' -> ')

    win_rate_abs, win_rate_rel = stats_str.split(' ')
    win, trades_count = map(int, win_rate_abs.split('/'))

    predict_session, predict_type = typed_sessions[-1].split('__')

    return predict_session, predict_type, tuple([*typed_sessions[:-1], (win, trades_count, win_rate_rel)])


def look_for_new_trade(sorted_profiles, profiles, symbol, strategy: NotifierStrategy):
    candles_15m = get_last_day_candles(symbol)
    if len(candles_15m) == 0:
        log_warn("0 candles 15m in look_for_new_trade")
        return

    days = markup_days(candles_15m)

    day_sessions = typify_sessions([days[-1]])
    if len(day_sessions) == 0:
        log_warn("len(day_sessions) == 0!")
        return

    predicted_session_mock = get_next_session_mock(day_sessions[-1].name, days[-1].date_readable)
    if not predicted_session_mock:
        log_warn("no predicted_session_mock!")
        return

    predicted_session_mock.open = candles_15m[-1][3]  # TODO не будет норм раб, если вызывается в конце сессии в while

    trade_profiles = [
        to_trade_profile(x['profile']) for x in sorted_profiles
        if x['win'] / len(x['trades']) > strategy.backtest_min_win_rate
           and x['pnl'] / len(x['trades']) > strategy.backtest_min_pnl_per_trade
           and to_trade_profile(x['profile'])[0] == predicted_session_mock.name.value
    ]

    profiles_map = {}
    for trade_profile in trade_profiles:
        if trade_profile[0] not in profiles_map:
            profiles_map[trade_profile[0]] = {}
        if trade_profile[1] not in profiles_map[trade_profile[0]]:
            profiles_map[trade_profile[0]][trade_profile[1]] = []
        profiles_map[trade_profile[0]][trade_profile[1]].append(trade_profile[2])

    new_trades = look_for_entry_backtest([*day_sessions, predicted_session_mock], profiles_map, strategy.sl_percent,
                                         strategy.tp_percent)

    if len(new_trades) == 0:
        log_info_ny(f"no new trades for {symbol}")
        return

    row_ids = upsert_to_db(strategy.name, symbol, new_trades)

    for i, tr in enumerate(new_trades):
        profile = [x for x in sorted_profiles if x['profile'] == tr.entry_profile_key][0]
        post_signal_notification(f"""New trade #{row_ids[i]}:

    Symbol: {symbol},
    Entry time: {tr.entry_time} (UTC),
    Entry price: {tr.entry_price},
    Position: {tr.entry_position_usd}$.
    
    Hunting session: {tr.hunting_session.value}, hunting type: {tr.hunting_type.value},
    Predict direction: {tr.predict_direction}.
    
    Stop price: {tr.initial_stop},
    Position close deadline: {tr.deadline_close} (UTC),
    Take profit: {tr.take_profit}.
    
    Strategy: {strategy.name},
    Profile: {profile['profile']} ({profile['year']} year),
    Profile backtest result: {profile['win']}/{len(profile['trades'])} {str(round(profile['win'] / len(profile['trades']) * 100, 2))}% ({', '.join([str(x) for x in strategy.backtest_years if x != profile['year']])} years) with pnl {round(profile['pnl'], 2)}$.
""")


def stat_for_a_closed_trade(strategy_name: str, open_date_utc: str, symbol: str):
    now_year = datetime.now(ZoneInfo("UTC")).year  # TODO не будет работать при смене года

    connection = connect_to_db(now_year)
    c = connection.cursor()

    c.execute("""SELECT ROWID, strategy_name, open_date_utc, symbol, session_trade FROM notifier_trades
            WHERE full_close_date_utc != '' ORDER BY strftime('%s', open_date_utc)""")
    rows = c.fetchall()
    connection.close()

    if len(rows) > 10000:
        log_warn("TODO будут проблемы в памяти всё считать, когда сделок тут много станет")

    if len(rows) == 0:
        log_warn("no trades yet!")
        return

    result = {
        'trade_win': False,
        'trade_pnl': 0,
        'trade_guessed': False,
        'total_trades': 0,
        'total_win_trades': 0,
        'total_pnl': 0,
        'total_guessed': 0,
        'strategy_trades': 0,
        'strategy_win_trades': 0,
        'strategy_pnl': 0,
        'strategy_guessed': 0,
        'symbol_trades': 0,
        'symbol_win_trades': 0,
        'symbol_pnl': 0,
        'symbol_guessed': 0,
        'symbol_strategy_trades': 0,
        'symbol_strategy_win_trades': 0,
        'symbol_strategy_pnl': 0,
        'symbol_strategy_guessed': 0,
    }

    for row in rows:
        row_id, row_strategy_name, row_open_date_utc, row_symbol, row_trade_json = row
        session_trade = session_trade_from_json(row_trade_json)

        result['total_trades'] = result['total_trades'] + 1
        result['total_pnl'] = result['total_pnl'] + session_trade.pnl_usd
        if strategy_name == row_strategy_name:
            result['strategy_trades'] = result['strategy_trades'] + 1
            result['strategy_pnl'] = result['strategy_pnl'] + session_trade.pnl_usd
            if symbol == row_symbol:
                result['symbol_strategy_trades'] = result['symbol_strategy_trades'] + 1
                result['symbol_strategy_pnl'] = result['symbol_strategy_pnl'] + session_trade.pnl_usd
                if open_date_utc == row_open_date_utc:
                    result['trade_pnl'] = session_trade.pnl_usd
        if symbol == row_symbol:
            result['symbol_trades'] = result['symbol_trades'] + 1
            result['symbol_pnl'] = result['symbol_pnl'] + session_trade.pnl_usd

        if session_trade.pnl_usd > 0:
            result['total_win_trades'] = result['total_win_trades'] + 1
            if strategy_name == row_strategy_name:
                result['strategy_win_trades'] = result['strategy_win_trades'] + 1
                if symbol == row_symbol:
                    result['symbol_strategy_win_trades'] = result['symbol_strategy_win_trades'] + 1
                    if open_date_utc == row_open_date_utc:
                        result['trade_win'] = True
            if symbol == row_symbol:
                result['symbol_win_trades'] = result['symbol_win_trades'] + 1

        if session_trade.hunting_type == session_trade.result_type:
            result['total_guessed'] = result['total_guessed'] + 1
            if strategy_name == row_strategy_name:
                result['strategy_guessed'] = result['strategy_guessed'] + 1
                if symbol == row_symbol:
                    result['symbol_strategy_guessed'] = result['symbol_strategy_guessed'] + 1
                    if open_date_utc == row_open_date_utc:
                        result['trade_guessed'] = True
            if symbol == row_symbol:
                result['symbol_guessed'] = result['symbol_guessed'] + 1

    return result


def handle_open_trades(strategies: List[NotifierStrategy]):
    now_year = datetime.now(ZoneInfo("UTC")).year  # TODO не будет работать при смене года

    connection = connect_to_db(now_year)
    c = connection.cursor()

    c.execute(f"""SELECT ns.ROWID, ns.strategy_name, ns.open_date_utc, ns.symbol, ns.session_trade,
        rc.open, rc.high, rc.low, rc.close, rc.volume, rc.date_ts FROM notifier_trades ns
    JOIN raw_candles rc ON ns.symbol = rc.symbol
        AND strftime('%s', rc.date_ts) >= strftime('%s', ns.open_date_utc)
        AND strftime('%s', rc.date_ts) < strftime('%s', ns.deadline_close)
    WHERE ns.full_close_date_utc = ''
        AND strategy_name IN ({','.join(['?'] * len(strategies))})
    ORDER BY strftime('%s', rc.date_ts)""", [x.name for x in strategies])

    rows = c.fetchall()
    trades_with_candles = []

    for row in rows:
        if len(trades_with_candles) > 0 and row[0] == trades_with_candles[-1][0][0]:
            trades_with_candles[-1].append(row)
        else:
            trades_with_candles.append([row])

    for rows in trades_with_candles:

        row_id, row_strategy_name, row_trade_open_date_utc, row_symbol, row_trade_json = rows[0][0:5]
        session_trade = session_trade_from_json(row_trade_json)
        strategy = [x for x in strategies if x.name == row_strategy_name][0]
        candles_15m = [x[5:] for x in rows]
        last_mock_candle = [candles_15m[-1][3], candles_15m[-1][3], candles_15m[-1][3], candles_15m[-1][3], 0,
                            to_date_str(to_utc_datetime(candles_15m[-1][5]) + timedelta(minutes=15))]
        closed_trade = look_for_close_backtest([*candles_15m, last_mock_candle], session_trade)
        closed_trade.result_type = typify_session(as_1_candle(candles_15m), strategy.session_thresholds)
        upsert_to_db(strategy.name, row_symbol, [closed_trade])
        stats = stat_for_a_closed_trade(strategy.name, row_trade_open_date_utc, row_symbol)
        post_signal_notification(f"""Close trade #{row_id} with reason '{closed_trade.closes[-1][3]}':

    Symbol: {row_symbol},
    Time open/close: {closed_trade.entry_time} / {closed_trade.closes[-1][2]} (UTC),
    Price open/close: {closed_trade.entry_price} / {closed_trade.closes[-1][1]},
    PNL: {round(closed_trade.pnl_usd, 2)}$.
    
    Session {closed_trade.hunting_session} predicted {closed_trade.hunting_type} ({closed_trade.predict_direction}), but instead got {closed_trade.result_type.value}.
    
    WIN/GUESSED/TRADES/PNL
    Total: {stats['total_win_trades']}/{stats['total_guessed']}/{stats['total_trades']}/{round(stats['total_pnl'], 2)}$,
    Strategy: {stats['strategy_win_trades']}/{stats['strategy_guessed']}/{stats['strategy_trades']}/{round(stats['strategy_pnl'], 2)}$,
    {row_symbol}: {stats['symbol_win_trades']}/{stats['symbol_guessed']}/{stats['symbol_trades']}/{round(stats['symbol_pnl'], 2)}$,
    {row_symbol}+Strategy: {stats['symbol_strategy_win_trades']}/{stats['symbol_strategy_guessed']}/{stats['symbol_strategy_trades']}/{round(stats['symbol_strategy_pnl'], 2)}$.
    
    Strategy: {strategy.name}.
""")
        print('hello')

    connection.close()

    # 'trade_win': False,
    # 'trade_pnl': 0,
    # 'trade_guessed': False,
    # 'total_trades': 0,
    # 'total_win_trades': 0,
    # 'total_pnl': 0,
    # 'total_guessed': 0,
    # 'strategy_trades': 0,
    # 'strategy_win_trades': 0,
    # 'strategy_pnl': 0,
    # 'strategy_guessed': 0,
    # 'symbol_trades': 0,
    # 'symbol_win_trades': 0,
    # 'symbol_pnl': 0,
    # 'symbol_guessed': 0,
    # 'symbol_strategy_trades': 0,
    # 'symbol_strategy_win_trades': 0,
    # 'symbol_strategy_pnl': 0,
    # 'symbol_strategy_guessed': 0,


def to_db_format(strategy_name: str, symbol: str, session_trade: SessionTrade):
    return (
        strategy_name, session_trade.entry_time, symbol, session_trade.pnl_usd, session_trade.deadline_close,
        json_from_session_trade(session_trade),
        "" if len(session_trade.closes) == 0 else session_trade.closes[-1][2]  # TODO bug with partial close
    )


# TODO returns row_ids only for inserts!
def upsert_to_db(strategy_name: str, symbol: str, session_trades: List[SessionTrade]) -> List[int]:
    now_year = datetime.now(ZoneInfo("UTC")).year  # TODO не будет работать при смене года
    conn = connect_to_db(now_year)

    rows = [to_db_format(strategy_name, symbol, tr) for tr in session_trades]

    try:
        c = conn.cursor()
        c.executemany(
            """INSERT INTO notifier_trades (strategy_name, open_date_utc, symbol, pnl, deadline_close, session_trade, full_close_date_utc)
VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (strategy_name, open_date_utc, symbol) DO UPDATE
    SET pnl = excluded.pnl, session_trade = excluded.session_trade, full_close_date_utc = excluded.full_close_date_utc
RETURNING ROWID""",
            rows
        )
        conn.commit()
        print(f"Success inserting {len(session_trades)} notifier_trades for symbol {symbol}")
        c.execute("""SELECT ROWID FROM notifier_trades ORDER BY ROWID DESC LIMIT ?""", (len(session_trades),))
        result = c.fetchall()
        return [x[0] for x in result]
    except sqlite3.ProgrammingError as e:
        print(f"Error inserting {len(session_trades)} notifier_trades for symbol {symbol}: {e}")
        return []
    finally:
        conn.close()


def maybe_open_new_trades(profiles, symbols, strategies: List[NotifierStrategy]):
    for i, x in enumerate(symbols):
        look_for_new_trade(profiles[i][0], profiles[i][1], x, strategies[i])


def update_candles(symbols):
    for symbol in symbols:
        update_candle_from_binance(symbol)


def run_notifier(symbols_with_strategy):
    log_info_ny("Starting Notifier")
    start_time = time.perf_counter()

    symbols = [x[0] for x in symbols_with_strategy]
    strategies = [x[1] for x in symbols_with_strategy]
    profiles = [get_backtested_profiles(x[0], x[0], x[1]) for x in symbols_with_strategy]

    log_info_ny(f"Notifier preparation took {(time.perf_counter() - start_time):.6f} seconds")

    prev_time = now_ny_datetime()

    first_iteration = True

    prev_time = datetime(prev_time.year, prev_time.month, prev_time.day, 9, 30, tzinfo=ZoneInfo("America/New_York"))
    while True:
        if not first_iteration:
            sleep(60 - now_ny_datetime().second)

        first_iteration = False
        now_time = now_ny_datetime()
        if now_time.minute % 15 == 0:
            log_info_ny(f"heartbeat at {now_time}")

        if now_time.isoweekday() in [6, 7]:
            prev_time = now_time
            continue

        start_early = datetime(now_time.year, now_time.month, now_time.day, 7, 0, tzinfo=ZoneInfo("America/New_York"))
        end_early = datetime(now_time.year, now_time.month, now_time.day, 8, 0, tzinfo=ZoneInfo("America/New_York"))
        end_pre = datetime(now_time.year, now_time.month, now_time.day, 9, 30, tzinfo=ZoneInfo("America/New_York"))
        end_open = datetime(now_time.year, now_time.month, now_time.day, 10, 0, tzinfo=ZoneInfo("America/New_York"))
        end_nyam = datetime(now_time.year, now_time.month, now_time.day, 12, 0, tzinfo=ZoneInfo("America/New_York"))
        end_lunch = datetime(now_time.year, now_time.month, now_time.day, 13, 0, tzinfo=ZoneInfo("America/New_York"))
        end_nypm = datetime(now_time.year, now_time.month, now_time.day, 15, 0, tzinfo=ZoneInfo("America/New_York"))
        end_close = datetime(now_time.year, now_time.month, now_time.day, 16, 0, tzinfo=ZoneInfo("America/New_York"))

        if (prev_time < start_early <= now_time
                or prev_time < end_early <= now_time
                or prev_time < end_pre <= now_time
                or prev_time < end_open <= now_time
                or prev_time < end_nyam <= now_time
                or prev_time < end_lunch <= now_time
                or prev_time < end_nypm <= now_time
                or prev_time < end_close <= now_time):
            start_time = time.perf_counter()
            update_candles(symbols)
            handle_open_trades(strategies)
            maybe_open_new_trades(profiles, symbols, strategies)
            log_info_ny(f"Candles&Trades handling took {(time.perf_counter() - start_time):.6f} seconds")

        prev_time = now_time


if __name__ == "__main__":
    try:
        update_candles(["BTCUSDT"])
        update_candles(["AAVEUSDT"])
        update_candles(["AVAXUSDT"])
        update_candles(["CRVUSDT"])
        # look_for_new_trade([], None, "BTCUSDT")
        # print("done")
        run_notifier([
            ("BTCUSDT", btc_naive_strategy),
            ("AAVEUSDT", btc_naive_strategy),
            ("AVAXUSDT", btc_naive_strategy),
            ("CRVUSDT", btc_naive_strategy),
        ])
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
