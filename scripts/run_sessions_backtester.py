import sqlite3
from datetime import datetime
from typing import List

from scripts.run_sessions_sequencer import fill_profiles, get_successful_profiles
from scripts.run_sessions_typifier import typify_sessions
from stock_market_research_kit.day import Day, day_from_json
from stock_market_research_kit.session import SessionName, SessionType
from stock_market_research_kit.session_trade import SessionTrade

DATABASE_PATH = "stock_market_research.db"
conn = sqlite3.connect(DATABASE_PATH)
c = conn.cursor()


def backtest(days: List[Day], profiles, symbol):
    closed_trades = []
    active_trades = []
    for day in days:
        active_trades.extend(look_for_entry(day, profiles, symbol))
        for trade in active_trades:
            closed_trade = look_for_close(day, trade)
            if closed_trade:
                closed_trades.append(closed_trade)
                active_trades = [x for x in active_trades if x.entry_strategy != closed_trade.entry_strategy]

    print("pnl all", sum([x.pnl_usd for x in closed_trades]))
    print("pnl loss", sum([x.pnl_usd for x in closed_trades if x.pnl_usd < 0]))
    print("pnl prof", sum([x.pnl_usd for x in closed_trades if x.pnl_usd > 0]))
    print("len trades", len(closed_trades))
    print("win rate", len([x.pnl_usd for x in closed_trades if x.pnl_usd > 0]) / len(closed_trades))

    return closed_trades


def look_for_close(day: Day, trade: SessionTrade):
    for candle in day.candles_15m:
        if datetime.strptime(candle[5], "%Y-%m-%d %H:%M") < datetime.strptime(trade.entry_time, "%Y-%m-%d %H:%M"):
            continue
        if datetime.strptime(candle[5], "%Y-%m-%d %H:%M") >= datetime.strptime(trade.deadline_close, "%Y-%m-%d %H:%M"):
            return close_trade(trade, candle[0], trade.deadline_close, "deadline")
        if trade.predict_direction == 'UP' and candle[2] < trade.stop:
            return close_trade(trade, trade.stop, candle[5], "stop")
        if trade.predict_direction == 'DOWN' and candle[1] > trade.stop:
            return close_trade(trade, trade.stop, candle[5], "stop")

    return None


def close_trade(trade: SessionTrade, close_price: float, time: str, reason: str):
    pnl = 0
    if trade.predict_direction == 'UP':
        pnl = trade.entry_position_usd / trade.entry_price * (close_price - trade.entry_price)
    else:
        pnl = trade.entry_position_usd / trade.entry_price * (trade.entry_price - close_price)

    trade.pnl_usd = pnl
    trade.closes.append((100, close_price, time, reason))

    return trade


def look_for_entry(day: Day, profiles, symbol):
    trades: List[SessionTrade] = []
    day_sessions = typify_sessions([day], symbol)

    for session in profiles:
        for candle_type in profiles[session]:
            for profile in profiles[session][candle_type]:
                if is_sublist(list(profile[0:-1]), [f"{x.name.value}__{x.type.value}" for x in day_sessions]):
                    hunting_session = [x for x in day_sessions if x.name.value == session][0]
                    predict_direction = 'UP'
                    stop = hunting_session.open - hunting_session.open * 0.8 / 100
                    take_profit = hunting_session.open + hunting_session.open * 3 / 100

                    if candle_type in [SessionType.BEAR.value, SessionType.FLASH_CRASH.value]:
                        predict_direction = 'DOWN'
                        stop = hunting_session.open + hunting_session.open * 0.8 / 100
                        take_profit = hunting_session.open - hunting_session.open * 3 / 100

                    trades.append(SessionTrade(
                        entry_time=hunting_session.session_date,
                        entry_price=hunting_session.open,
                        entry_position_usd=1000,
                        position_usd=1000,
                        hunting_session=hunting_session.name,
                        predict_direction=predict_direction,
                        entry_strategy=f"{session}__{candle_type}: {str(profile)}",
                        initial_stop=stop,
                        stop=stop,
                        deadline_close=hunting_session.session_end_date,
                        take_profit=take_profit,
                        closes=[],
                        pnl_usd=0
                    ))

    return trades


def is_sublist(sub, main):
    sub_length = len(sub)
    return any(main[i:i + sub_length] == sub for i in range(len(main) - sub_length + 1))


SYMBOL = "BTCUSDT"
if __name__ == "__main__":
    try:
        c.execute("""SELECT data FROM days WHERE symbol = ?""", (SYMBOL,))
        rows = c.fetchall()

        if len(rows) == 0:
            print(f"Symbol {SYMBOL} not found in days table")
            quit(0)

        fill_profiles(SYMBOL)
        successful_profiles = get_successful_profiles(
            [x.value for x in [SessionName.EARLY, SessionName.PRE, SessionName.NY_OPEN, SessionName.NY_AM,
                               SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE]],
            2,
            40
        )

        backtest([day_from_json(x[0]) for x in rows], successful_profiles, SYMBOL)

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
