import sqlite3
from typing import List, Optional

from scripts.setup_db import connect_to_db
from stock_market_research_kit.candle import InnerCandle
from stock_market_research_kit.day import Day, json_from_day, day_from_json
from stock_market_research_kit.session_trade import SessionTrade, json_from_session_trade, json_from_session_trades, \
    session_trades_from_json
from utils.date_utils import log_warn_ny


def upsert_profiles_to_db(strategy_name: str, smb: str, profiles: List[dict]):
    conn = connect_to_db(2025)  # hardcoded 2025

    rows = [(strategy_name, p['profile'], smb, p['year'], p['win'], p['lose'], p['guessed'], p['missed'], p['pnl'],
             json_from_session_trades(p['trades'])) for p in profiles]

    try:
        c = conn.cursor()
        c.executemany("""INSERT INTO
backtested_profiles (strategy_name, profile_key, profile_symbol, profile_year, win, lose, guessed, missed, pnl, trades)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
ON CONFLICT (strategy_name, profile_key, profile_symbol, profile_year)
DO UPDATE SET win = excluded.win, lose = excluded.lose, guessed = excluded.guessed, missed = excluded.missed, pnl = excluded.pnl, trades = excluded.trades""",
                      rows)
        conn.commit()
        print(f"Success upsert {len(rows)} backtested_profiles for symbol {smb} in strategy '{strategy_name[0:2]}...'")
        return True
    except sqlite3.ProgrammingError as e:
        print(
            f"Error upsert {len(rows)} backtested_profiles for symbol {smb} in strategy '{strategy_name[0:2]}...': {e}")
        return False


def select_sorted_profiles(strategy_name: str, symbol: str):
    conn = connect_to_db(2025)  # hardcoded 2025
    c = conn.cursor()
    c.execute("""SELECT profile_year, profile_key, win, lose, guessed, missed, pnl, trades FROM backtested_profiles
WHERE strategy_name = ? AND profile_symbol = ? ORDER BY pnl DESC""", (strategy_name, symbol))
    rows = c.fetchall()
    conn.close()
    return [{
        'year': x[0],
        'profile': x[1],
        'win': x[2],
        'lose': x[3],
        'guessed': x[4],
        'missed': x[5],
        'pnl': x[6],
        'trades': session_trades_from_json(x[7]),
    } for x in rows]


def upsert_days_to_db(year: int, symbol: str, days: List[Day]):
    rows = [(symbol, day.date_readable, json_from_day(day)) for day in days]
    conn = connect_to_db(year)

    try:
        c = conn.cursor()
        c.executemany("""INSERT INTO
            days (symbol, date_ts, data) VALUES (?, ?, ?) ON CONFLICT (symbol, date_ts) DO UPDATE SET data = excluded.data""",
                      rows)
        conn.commit()
        print(f"Success inserting {len(rows)} days for symbol {symbol}")
        return True
    except sqlite3.ProgrammingError as e:
        print(f"Error inserting days {len(days)} symbol {symbol}: {e}")
        return False
    finally:
        conn.close()


def select_days(year: int, symbol: str) -> List[Day]:
    conn = connect_to_db(year)
    c = conn.cursor()
    c.execute("""SELECT data FROM days WHERE symbol = ?""", (symbol,))
    days_rows = c.fetchall()

    conn.close()
    return [day_from_json(x[0]) for x in days_rows]


def select_full_days_candles_15m(year, symbol) -> List[InnerCandle]:
    conn = connect_to_db(year)
    c = conn.cursor()
    # TODO will only work with 15m candles because of 85500!!
    c.execute("""WITH last_row_date_ts AS (
        SELECT date_ts last_row_date FROM raw_candles
        WHERE symbol = ? AND period = ?
        ORDER BY strftime('%s', date_ts) DESC
        LIMIT 1
    ),
         last_full_day AS (
             SELECT
                 (strftime('%s', last_row_date) - 85500) / 86400 * 86400 + 86400 AS end_of_day
             FROM last_row_date_ts
         )
    SELECT open, high, low, close, volume, date_ts FROM raw_candles
    WHERE strftime('%s', date_ts) + 0 < (SELECT end_of_day FROM last_full_day)
      AND symbol = ? AND period = ?
      ORDER BY strftime('%s', date_ts)""", (symbol, "15m", symbol, "15m"))
    rows_15m = c.fetchall()
    conn.close()

    if len(rows_15m) == 0:
        print(f"Symbol {symbol} 15m not found in DB")
        return []

    return [(x[0], x[1], x[2], x[3], x[4], x[5]) for x in rows_15m]


def raw_candle_to_db_format(symbol, period, inner_candle):
    return (symbol, inner_candle[5], period,
            inner_candle[0], inner_candle[1], inner_candle[2], inner_candle[3], inner_candle[4])


def update_stock_data(year: int, inner_candles, symbol, period):
    conn = connect_to_db(year)
    rows = [raw_candle_to_db_format(symbol, period, candle) for candle in inner_candles]

    try:
        c = conn.cursor()
        c.executemany(
            """INSERT INTO raw_candles (symbol, date_ts, period, open, high, low, close, volume) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
                ON CONFLICT (symbol, date_ts, period) DO UPDATE
                SET open = excluded.open, 
                    high = excluded.high, 
                    low = excluded.low, 
                    close = excluded.close,
                    volume = excluded.volume""",
            rows
        )
        conn.commit()
        print(f"Success inserting {len(rows)} raw_candles {period} data for symbol {symbol}")
        return True
    except sqlite3.ProgrammingError as e:
        print(f"Error inserting raw_candles {period} data for symbol {symbol}: {e}")
        return False
    finally:
        conn.close()


def last_candle_15m(year, symbol) -> Optional[InnerCandle]:
    connection = connect_to_db(year)
    c = connection.cursor()

    c.execute("""SELECT open, high, low, close, volume, date_ts
                FROM raw_candles WHERE symbol = ? AND period = ? ORDER BY date_ts DESC LIMIT 1""", (symbol, "15m"))
    rows_15m = c.fetchall()
    connection.close()

    if len(rows_15m) != 1:
        return None

    return rows_15m[0][0], rows_15m[0][1], rows_15m[0][2], rows_15m[0][3], rows_15m[0][4], rows_15m[0][5]


def session_trade_to_db_format(strategy_name: str, symbol: str, session_trade: SessionTrade):
    return (
        strategy_name, session_trade.entry_time, symbol, session_trade.pnl_usd, session_trade.deadline_close,
        json_from_session_trade(session_trade),
        "" if len(session_trade.closes) == 0 else session_trade.closes[-1][2]  # TODO bug with partial close
    )


# TODO returns row_ids only for inserts!
def upsert_trades_to_db(year: int, strategy_name: str, symbol: str, session_trades) -> List[int]:
    conn = connect_to_db(year)

    rows = [session_trade_to_db_format(strategy_name, symbol, tr) for tr in session_trades]

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
        print(f"Success inserting {len(session_trades)} notifier_trades for symbol {symbol} for strategy {strategy_name[0:2]}...")
        c.execute("""SELECT ROWID FROM notifier_trades ORDER BY ROWID DESC LIMIT ?""", (len(session_trades),))
        result = c.fetchall()
        return [x[0] for x in result]
    except sqlite3.ProgrammingError as e:
        print(f"Error inserting {len(session_trades)} notifier_trades for symbol {symbol}: {e}")
        return []
    finally:
        conn.close()


def select_closed_trades(year: int):
    connection = connect_to_db(year)
    c = connection.cursor()

    c.execute("""SELECT ROWID, strategy_name, open_date_utc, symbol, session_trade FROM notifier_trades
                WHERE full_close_date_utc != '' ORDER BY strftime('%s', open_date_utc)""")
    rows = c.fetchall()
    connection.close()

    return rows


def select_open_trades_by_strategies(year: int, strategy_names: List[str]):
    connection = connect_to_db(year)
    c = connection.cursor()

    c.execute(f"""SELECT ns.ROWID, ns.strategy_name, ns.open_date_utc, ns.symbol, ns.session_trade,
            rc.open, rc.high, rc.low, rc.close, rc.volume, rc.date_ts FROM notifier_trades ns
        JOIN raw_candles rc ON ns.symbol = rc.symbol
            AND strftime('%s', rc.date_ts) >= strftime('%s', ns.open_date_utc)
            AND strftime('%s', rc.date_ts) < strftime('%s', ns.deadline_close)
        WHERE ns.full_close_date_utc = ''
            AND strategy_name IN ({','.join(['?'] * len(strategy_names))})
        ORDER BY ns.ROWID""", strategy_names)

    rows = c.fetchall()
    connection.close()

    return rows


def select_last_day_candles(year, time_till: str, symbol):
    connection = connect_to_db(year)
    c = connection.cursor()

    # time_from = to_date_str(start_of_day(now_utc_datetime()) - timedelta(hours=2))
    # time_to = to_date_str(now_utc_datetime())

    c.execute("""WITH last_row_date_ts AS (
    SELECT date_ts last_row_date FROM raw_candles
    WHERE symbol = ? AND period = ? AND strftime('%s', date_ts) + 0 < strftime('%s', ?) + 0
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
WHERE strftime('%s', date_ts) + 0 BETWEEN (SELECT start_time_sec FROM time_range) + 0 AND (SELECT end_time_sec FROM time_range) + 0
    AND symbol = ? AND period = ?""", (symbol, "15m", time_till, symbol, "15m"))
    rows_15m = c.fetchall()
    connection.close()
    if len(rows_15m) > 96:
        log_warn_ny(f"For symbol {symbol} {len(rows_15m)} 15m items found!")
    if len(rows_15m) == 0:
        print(f"Symbol {symbol} 15m not found in DB")
        return []

    return [[x[0], x[1], x[2], x[3], x[4], x[5]] for x in rows_15m]


if __name__ == "__main__":
    try:
        res_btc = select_sorted_profiles(
            "#2 Same as #1, but sessions thresholds are now calculated per-session based on quantiles for 2024 year",
            "BTCUSDT"
        )
        print(res_btc)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
