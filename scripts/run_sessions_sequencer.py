import sqlite3
import time

from stock_market_research_kit.candle_tree import tree_from_sessions
from stock_market_research_kit.session import SessionType, SessionName, session_from_json

DATABASE_PATH = "stock_market_research.db"
conn = sqlite3.connect(DATABASE_PATH)
c = conn.cursor()


def main(symbol):
    c.execute("""SELECT data FROM sessions WHERE symbol = ? ORDER BY session_ts""", (symbol,))
    sessions_rows = c.fetchall()

    if len(sessions_rows) == 0:
        print(f"Symbol {symbol} not found in sessions table")
        return

    sessions = [session_from_json(x[0]) for x in sessions_rows]
    print(f"Found {len(sessions)} {symbol} rows")

    start_time1 = time.perf_counter()
    tree1 = tree_from_sessions(
        "all_sessions_ordered",
        sessions,
        [
            SessionName.CME, SessionName.ASIA, SessionName.LONDON, SessionName.EARLY, SessionName.PRE,
            SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )
    end_time1 = time.perf_counter()
    elapsed_time1 = end_time1 - start_time1
    print(f"all_sessions_ordered {tree1.root.count} items tree built for {elapsed_time1:.6f} seconds")

    start_time2 = time.perf_counter()
    tree2 = tree_from_sessions(
        "ny_close_ordered",
        sessions,
        [
            SessionName.NY_CLOSE, SessionName.NY_PM, SessionName.NY_LUNCH, SessionName.NY_AM,
            SessionName.NY_OPEN, SessionName.PRE, SessionName.EARLY, SessionName.LONDON, SessionName.ASIA,
            SessionName.CME],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )
    end_time2 = time.perf_counter()
    elapsed_time2 = end_time2 - start_time2
    print(f"ny_close_ordered {tree2.root.count} items tree built for {elapsed_time2:.6f} seconds")

    start_time3 = time.perf_counter()
    tree3 = tree_from_sessions(
        "ny_close_only_directional_reverse",
        sessions,
        [
            SessionName.NY_CLOSE, SessionName.NY_PM, SessionName.NY_LUNCH, SessionName.NY_AM,
            SessionName.NY_OPEN, SessionName.PRE, SessionName.EARLY, SessionName.LONDON, SessionName.ASIA,
            SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )
    end_time3 = time.perf_counter()
    elapsed_time3 = end_time3 - start_time3
    print(f"ny_close_directional_reverse {tree3.root.count} items tree built for {elapsed_time3:.6f} seconds")

    conn.close()


if __name__ == "__main__":
    try:
        main("BTCUSDT")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
