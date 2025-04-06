import time
from datetime import datetime
from typing import List, Optional

from scripts.run_sessions_sequencer import fill_profiles, get_successful_profiles
from scripts.run_sessions_typifier import typify_sessions
from scripts.setup_db import connect_to_db
from stock_market_research_kit.backtest import Backtest, StrategyTrades
from stock_market_research_kit.day import Day, day_from_json
from stock_market_research_kit.notifier_strategy import NotifierStrategy, btc_naive_strategy, btc_naive_strategy_copy
from stock_market_research_kit.session import SessionName, SessionType, Session, sessions_in_order
from stock_market_research_kit.session_trade import SessionTrade
from utils.date_utils import to_utc_datetime


def backtest(days: List[Day], profiles, bt: Backtest):
    result = bt

    closed_trades = []
    active_trades = []
    for day in days:
        day_sessions = typify_sessions([day])
        if len(day_sessions) == 0:
            continue
        active_trades.extend(look_for_entry_backtest(day_sessions, profiles, bt.sl_percent, bt.tp_percent))
        for trade in active_trades:
            closed_trade = look_for_close_backtest(day.candles_15m, trade)
            if closed_trade:
                closed_trade.result_type = [x.type for x in day_sessions if x.name == closed_trade.hunting_session][0]
                closed_trades.append(closed_trade)
                active_trades = [x for x in active_trades if x.entry_profile_key != closed_trade.entry_profile_key]

    result.trades = len(closed_trades)
    result.win = len([x.pnl_usd for x in closed_trades if x.pnl_usd > 0])
    result.lose = result.trades - result.win
    result.win_rate = 0 if result.trades == 0 else result.win / result.trades
    result.sessions_guessed = len([x for x in closed_trades if x.hunting_type == x.result_type])
    result.sessions_missed = result.trades - result.sessions_guessed
    result.guess_rate = 0 if result.trades == 0 else result.sessions_guessed / result.trades
    result.pnl_all = sum([x.pnl_usd for x in closed_trades])
    result.pnl_prof = sum([x.pnl_usd for x in closed_trades if x.pnl_usd > 0])
    result.pnl_loss = sum([x.pnl_usd for x in closed_trades if x.pnl_usd < 0])

    result.all_trades = closed_trades

    for trade in closed_trades:
        if trade.entry_profile_key not in result.trades_by_strategy:
            result.trades_by_strategy[trade.entry_profile_key] = StrategyTrades(win=0, lose=0, pnl=0, trades=[])
        result.trades_by_strategy[trade.entry_profile_key].trades.append(trade)
        result.trades_by_strategy[trade.entry_profile_key].pnl += trade.pnl_usd
        if trade.pnl_usd > 0:
            result.trades_by_strategy[trade.entry_profile_key].win = result.trades_by_strategy[
                                                                      trade.entry_profile_key].win + 1
        else:
            result.trades_by_strategy[trade.entry_profile_key].lose = result.trades_by_strategy[
                                                                       trade.entry_profile_key].lose + 1

    return result


def look_for_close_backtest(candles_15m, trade: SessionTrade) -> Optional[SessionTrade]:
    for candle in candles_15m:
        if to_utc_datetime(candle[5]) < to_utc_datetime(trade.entry_time):
            continue
        if to_utc_datetime(candle[5]) >= to_utc_datetime(trade.deadline_close):
            return close_trade_backtest(trade, candle[0], trade.deadline_close, "deadline")
        if trade.predict_direction == 'UP':
            if candle[2] < trade.stop:
                return close_trade_backtest(trade, trade.stop, candle[5], "stop")
            if candle[1] > trade.take_profit:
                return close_trade_backtest(trade, trade.take_profit, candle[5], "take_profit")
        if trade.predict_direction == 'DOWN':
            if candle[1] > trade.stop:
                return close_trade_backtest(trade, trade.stop, candle[5], "stop")
            if candle[2] < trade.take_profit:
                return close_trade_backtest(trade, trade.take_profit, candle[5], "take_profit")

    return None


def close_trade_backtest(trade: SessionTrade, close_price: float, time_str: str, reason: str):
    if trade.predict_direction == 'UP':
        pnl = trade.entry_position_usd / trade.entry_price * (close_price - trade.entry_price)
    else:
        pnl = trade.entry_position_usd / trade.entry_price * (trade.entry_price - close_price)

    trade.pnl_usd = pnl
    trade.closes.append((100, close_price, time_str, reason))

    return trade


def look_for_entry_backtest(day_sessions: List[Session], profiles, sl, tp) -> List[SessionTrade]:
    trades: List[SessionTrade] = []

    for session in profiles:
        for candle_type in profiles[session]:
            for profile in profiles[session][candle_type]:
                for x in day_sessions:
                    if not x.type or not x.name:
                        print("hello there")
                if is_sublist(list(profile[0]), [f"{x.name.value}__{x.type.value}" for x in day_sessions]):
                    hunting_sessions = [x for x in day_sessions if x.name.value == session]
                    if len(hunting_sessions) == 0:
                        # this triggers if fn called from backtest with current not-closed day
                        print("hello there1")
                        continue
                    hunting_session = hunting_sessions[0]
                    predict_direction = 'UP'
                    stop = round(hunting_session.open - hunting_session.open * sl / 100, 4)
                    take_profit = round(hunting_session.open + hunting_session.open * tp / 100, 4)

                    if candle_type in [SessionType.BEAR.value, SessionType.FLASH_CRASH.value]:
                        predict_direction = 'DOWN'
                        stop = round(hunting_session.open + hunting_session.open * sl / 100, 4)
                        take_profit = round(hunting_session.open - hunting_session.open * tp / 100, 4)

                    trades.append(SessionTrade(
                        entry_time=hunting_session.session_date,
                        entry_price=hunting_session.open,
                        entry_position_usd=1000,
                        position_usd=1000,
                        hunting_session=SessionName(session),
                        hunting_type=SessionType(candle_type),
                        predict_direction=predict_direction,
                        entry_profile_key=f"{' -> '.join(profile[0])} -> {session}__{candle_type}: {profile[1][0]}/{profile[1][1]} {profile[1][2]}",
                        initial_stop=stop,
                        stop=stop,
                        deadline_close=hunting_session.session_end_date,
                        take_profit=take_profit,
                        closes=[],
                        result_type=SessionType.UNSPECIFIED,
                        pnl_usd=0
                    ))

    return trades


def is_sublist(sub, main):
    sub_length = len(sub)
    return any(main[i:i + sub_length] == sub for i in range(len(main) - sub_length + 1))


PREDICT_PROFILES = [x.value for x in [SessionName.EARLY, SessionName.PRE, SessionName.NY_OPEN, SessionName.NY_AM,
                                      SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE]]


def run_backtest(profiles_symbol: str, profiles_year: int, profiles_min_chance: int, profiles_min_times: int,
                 test_symbol: str, test_year: int, sl_percent: float, tp_percent: float, profiles):
    start_time = time.perf_counter()

    conn = connect_to_db(test_year)
    c = conn.cursor()

    c.execute("""SELECT data FROM days WHERE symbol = ?""", (test_symbol,))
    days_rows = c.fetchall()

    if len(days_rows) == 0:
        print(f"Symbol {test_symbol} not found in days table")
        quit(0)

    successful_profiles = get_successful_profiles(PREDICT_PROFILES, profiles_min_times, profiles_min_chance, profiles)

    bt = Backtest(
        profiles_symbol=profiles_symbol,
        profiles_year=profiles_year,
        profiles_min_chance=profiles_min_chance,
        profiles_min_times=profiles_min_times,
        test_symbol=test_symbol,
        test_year=test_year,
        sl_percent=sl_percent,
        tp_percent=tp_percent,
        trades=0,
        win=0,
        lose=0,
        win_rate=0,
        sessions_guessed=0,
        sessions_missed=0,
        guess_rate=0,
        pnl_all=0,
        pnl_prof=0,
        pnl_loss=0,
        all_trades=[],
        trades_by_strategy={}
    )

    conn.close()
    res = backtest([day_from_json(x[0]) for x in days_rows], successful_profiles, bt)

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"{test_symbol}/{test_year} test on {profiles_symbol}/{profiles_year} took {elapsed_time:.6f} seconds")

    return res


def get_backtested_profiles(profiles_symbol: str, test_symbol: str, strategy: NotifierStrategy, symbol_year_profiles):
    profiles_by_year = {
        2021: {},
        2022: {},
        2023: {},
        2024: {},
        2025: {},
    }
    for profile_year in strategy.profile_years:
        for test_year in strategy.backtest_years:
            if not strategy.include_profile_year_to_backtest and test_year == profile_year:
                continue

            test = run_backtest(
                profiles_symbol, profile_year, strategy.profiles_min_chance, strategy.profiles_min_times,
                test_symbol, test_year, strategy.sl_percent, strategy.tp_percent,
                symbol_year_profiles[f"{profiles_symbol}__{profile_year}"][2]
            )
            for profile in test.trades_by_strategy:
                if profile not in profiles_by_year[profile_year]:
                    profiles_by_year[profile_year][profile] = {'win': 0, 'lose': 0, 'pnl': 0}
                profiles_by_year[profile_year][profile]['win'] += test.trades_by_strategy[profile].win
                profiles_by_year[profile_year][profile]['lose'] += test.trades_by_strategy[profile].lose
                profiles_by_year[profile_year][profile]['pnl'] += test.trades_by_strategy[profile].pnl
                profiles_by_year[profile_year][profile][test_year] = test.trades_by_strategy[profile]

    unsorted_profiles = []
    for profile_year in strategy.profile_years:
        for profile in profiles_by_year[profile_year]:
            unsorted_profile = {
                'year': profile_year,
                'profile': profile,
                'win': profiles_by_year[profile_year][profile]['win'],
                'lose': profiles_by_year[profile_year][profile]['lose'],
                'pnl': profiles_by_year[profile_year][profile]['pnl'],
                'trades': []
            }

            for test_year in strategy.backtest_years:
                if test_year in profiles_by_year[profile_year][profile]:
                    unsorted_profile['trades'].extend(profiles_by_year[profile_year][profile][test_year].trades)

            unsorted_profiles.append(unsorted_profile)

    sorted_profiles = sorted(unsorted_profiles, key=lambda x: x['pnl'], reverse=True)

    return sorted_profiles, profiles_by_year


if __name__ == "__main__":
    try:
        results = []
        for x in [
            ("BTCUSDT", btc_naive_strategy),
            # ("AAVEUSDT", btc_naive_strategy),
            # ("AVAXUSDT", btc_naive_strategy),
            # ("CRVUSDT", btc_naive_strategy),
        ]:
            # [(x[0], [[trade.entry_time for trade in profile['trades']] for profile in x[1][0]]) for x in results]
            # [(x[0], [[trade.entry_time for trade in profile['trades'] if trade.entry_time.startswith('2025-04')] for profile in x[1][0]]) for x in results]
            results.append((x[0], get_backtested_profiles(x[0], x[0], x[1])))
        pnls = [(x[0], sum([profile['pnl'] for profile in x[1][0]])) for x in results]
        april_dates = [(x[0], [[trade.entry_time for trade in profile['trades'] if trade.entry_time.startswith('2025-04')] for profile in x[1][0]]) for x in results]

        strategy_profiles = [
            (x[0], [profile for profile in x[1][0]
                    if profile['win'] / len(profile['trades']) > btc_naive_strategy.backtest_min_win_rate
                    and profile['pnl'] / len(profile['trades']) > btc_naive_strategy.backtest_min_pnl_per_trade
                    ])
            for x in results]
        strategy_pnls = [(x[0], sum([profile['pnl'] for profile in x[1]])) for x in strategy_profiles]
        strategy_april_dates = [(x[0], [[trade.entry_time for trade in profile['trades'] if trade.entry_time.startswith('2025-04')] for profile in x[1]]) for x in strategy_profiles]

        print("done!")

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)

    #  TODO:
    #   - сохранять топ профили (по какому-то трешхолду) в БД
    #   - по каждому из профилей прогнать разбивку по месяцам - с каким win rate и pnl они бы работали
