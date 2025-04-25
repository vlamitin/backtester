import time
from datetime import timedelta
from typing import List, Optional

from scripts.run_sessions_sequencer import fill_profiles, get_successful_profiles
from scripts.run_sessions_typifier import typify_sessions
from stock_market_research_kit.backtest import Backtest, StrategyTrades
from stock_market_research_kit.day import Day
from stock_market_research_kit.db_layer import upsert_profiles_to_db, select_days, select_closed_trades
from stock_market_research_kit.notifier_strategy import NotifierStrategy, \
    thr2024_strict_p30_safe_stops_strategy09, thr2024_strict_p70_safe_stops_strategy10, \
    thr2024_p30_safe_stops_strategy07, thr2024_p70_safe_stops_strategy05, \
    thr2024_loose_p70_safe_stops_strategy06, thr2024_loose_p30_safe_stops_strategy08, \
    btc_naive_p30_safe_stops_strategy_strategy11, btc_naive_p70_safe_stops_strategy_strategy12
from stock_market_research_kit.session import SessionName, SessionType, Session
from stock_market_research_kit.session_quantiles import quantile_session_year_thr
from stock_market_research_kit.session_thresholds import ThresholdsGetter, SGetter
from stock_market_research_kit.session_trade import SessionTrade, session_trade_from_json
from utils.date_utils import to_utc_datetime, to_date_str, start_of_day, now_utc_datetime


def backtest(days: List[Day], profiles, bt: Backtest, thr_getter: ThresholdsGetter):
    result = bt

    closed_trades = []
    active_trades = []
    for day in days:
        day_sessions = typify_sessions([day], thr_getter)
        if len(day_sessions) == 0:
            continue
        active_trades.extend(look_for_entry_backtest(day_sessions, profiles, bt.slg, bt.tpg))
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
            result.trades_by_strategy[trade.entry_profile_key] = StrategyTrades(
                win=0, lose=0, guessed=0, missed=0, pnl=0, trades=[]
            )
        result.trades_by_strategy[trade.entry_profile_key].trades.append(trade)
        result.trades_by_strategy[trade.entry_profile_key].pnl += trade.pnl_usd
        if trade.pnl_usd > 0:
            result.trades_by_strategy[trade.entry_profile_key].win = result.trades_by_strategy[
                                                                         trade.entry_profile_key].win + 1
        else:
            result.trades_by_strategy[trade.entry_profile_key].lose = result.trades_by_strategy[
                                                                          trade.entry_profile_key].lose + 1
        if trade.hunting_type == trade.result_type:
            result.trades_by_strategy[trade.entry_profile_key].guessed = result.trades_by_strategy[
                                                                             trade.entry_profile_key].guessed + 1
        else:
            result.trades_by_strategy[trade.entry_profile_key].missed = result.trades_by_strategy[
                                                                            trade.entry_profile_key].missed + 1

    return result


def look_for_close_backtest(candles_15m, trade: SessionTrade) -> Optional[SessionTrade]:
    for candle in candles_15m:
        candle_close = to_date_str(to_utc_datetime(candle[5]) + timedelta(minutes=14))
        if to_utc_datetime(candle[5]) < to_utc_datetime(trade.entry_time):
            continue
        if to_utc_datetime(candle[5]) >= to_utc_datetime(trade.deadline_close):
            return close_trade_backtest(trade, candle[0], trade.deadline_close, "deadline")
        if trade.predict_direction == 'UP':
            if candle[2] < trade.stop:
                return close_trade_backtest(trade, trade.stop, candle_close, "stop")
            if candle[1] > trade.take_profit:
                return close_trade_backtest(trade, trade.take_profit, candle_close, "take_profit")
        if trade.predict_direction == 'DOWN':
            if candle[1] > trade.stop:
                return close_trade_backtest(trade, trade.stop, candle_close, "stop")
            if candle[2] < trade.take_profit:
                return close_trade_backtest(trade, trade.take_profit, candle_close, "take_profit")

    return None


def close_trade_backtest(trade: SessionTrade, close_price: float, time_str: str, reason: str):
    if trade.predict_direction == 'UP':
        pnl = trade.entry_position_usd / trade.entry_price * (close_price - trade.entry_price)
    else:
        pnl = trade.entry_position_usd / trade.entry_price * (trade.entry_price - close_price)

    trade.pnl_usd = pnl
    trade.closes.append((100, close_price, time_str, reason))

    return trade


def open_trade(price: float, date: str, deadline: str, session_name: SessionName, predict_type: SessionType,
               slg: SGetter, tpg: SGetter, profile_key: str):
    predict_direction = 'DOWN' if predict_type in [SessionType.BEAR, SessionType.FLASH_CRASH] else 'UP'
    sl = slg(session_name, (price, price, price, price, 0, date), predict_direction)
    tp = tpg(session_name, (price, price, price, price, 0, date), predict_direction)

    stop_loss = round(price - price * sl / 100, 4)
    take_profit = round(price + price * tp / 100, 4)

    if predict_direction == 'DOWN':
        stop_loss = round(price + price * sl / 100, 4)
        take_profit = round(price - price * tp / 100, 4)

    return SessionTrade(
        entry_time=date,
        entry_price=price,
        entry_position_usd=1000,
        position_usd=1000,
        hunting_session=session_name,
        hunting_type=predict_type,
        predict_direction=predict_direction,
        entry_profile_key=profile_key,
        initial_stop=stop_loss,
        stop=stop_loss,
        deadline_close=deadline,
        take_profit=take_profit,
        closes=[],
        result_type=SessionType.UNSPECIFIED,
        pnl_usd=0
    )


def look_for_entry_backtest(day_sessions: List[Session], profiles, slg: SGetter, tpg: SGetter) -> List[SessionTrade]:
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
                    trade = open_trade(
                        hunting_session.open, hunting_session.session_date, hunting_session.session_end_date,
                        SessionName(session), SessionType(candle_type), slg, tpg,
                        f"{' -> '.join(profile[0])} -> {session}__{candle_type}: {profile[1][0]}/{profile[1][1]} {profile[1][2]}"
                    )
                    trades.append(trade)

    return trades


def is_sublist(sub, main):
    sub_length = len(sub)
    return any(main[i:i + sub_length] == sub for i in range(len(main) - sub_length + 1))


PREDICT_PROFILES = [x.value for x in [SessionName.EARLY, SessionName.PRE, SessionName.NY_OPEN, SessionName.NY_AM,
                                      SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE]]


def run_backtest(profiles_symbol: str, profiles_year: int, profiles_min_chance: int, profiles_min_times: int,
                 test_symbol: str, test_year: int, slg: SGetter, tpg: SGetter, profiles,
                 thr_getter: ThresholdsGetter):
    days = select_days(test_year, test_symbol)

    if len(days) == 0:
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
        slg=slg,
        tpg=tpg,
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

    res = backtest(days, successful_profiles, bt, thr_getter)

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

            start_time = time.perf_counter()
            test = run_backtest(
                profiles_symbol, profile_year, strategy.profiles_min_chance, strategy.profiles_min_times,
                test_symbol, test_year, strategy.slg, strategy.tpg,
                symbol_year_profiles[f"{profiles_symbol}__{profile_year}"][2],
                strategy.thresholds_getter
            )

            for profile in test.trades_by_strategy:
                if profile not in profiles_by_year[profile_year]:
                    profiles_by_year[profile_year][profile] = {'win': 0, 'lose': 0, 'pnl': 0, 'guessed': 0, 'missed': 0}
                profiles_by_year[profile_year][profile]['win'] += test.trades_by_strategy[profile].win
                profiles_by_year[profile_year][profile]['lose'] += test.trades_by_strategy[profile].lose
                profiles_by_year[profile_year][profile]['guessed'] += test.trades_by_strategy[profile].guessed
                profiles_by_year[profile_year][profile]['missed'] += test.trades_by_strategy[profile].missed
                profiles_by_year[profile_year][profile]['pnl'] += test.trades_by_strategy[profile].pnl
                profiles_by_year[profile_year][profile][test_year] = test.trades_by_strategy[profile]

            print(
                f"Strategy '{strategy.name[0:3]}...' testing profile {profiles_symbol}/{profile_year} on {test_symbol}/{test_year} took {time.perf_counter() - start_time:.6f} seconds")

    unsorted_profiles = []
    for profile_year in strategy.profile_years:
        for profile in profiles_by_year[profile_year]:
            unsorted_profile = {
                'year': profile_year,
                'profile': profile,
                'win': profiles_by_year[profile_year][profile]['win'],
                'lose': profiles_by_year[profile_year][profile]['lose'],
                'guessed': profiles_by_year[profile_year][profile]['guessed'],
                'missed': profiles_by_year[profile_year][profile]['missed'],
                'pnl': profiles_by_year[profile_year][profile]['pnl'],
                'trades': []
            }

            for test_year in strategy.backtest_years:
                if test_year in profiles_by_year[profile_year][profile]:
                    unsorted_profile['trades'].extend(profiles_by_year[profile_year][profile][test_year].trades)

            unsorted_profiles.append(unsorted_profile)

    sorted_profiles = sorted(unsorted_profiles, key=lambda x: x['pnl'], reverse=True)

    return sorted_profiles, profiles_by_year


def test_notifier_trades():
    strategy_12 = {
        # 'BTCUSDT': session_thr2024_p30_safe_stops_strategy(quantile_session_year_thr("BTCUSDT", 2024)),
        # 'AAVEUSDT': session_thr2024_p30_safe_stops_strategy(quantile_session_year_thr("AAVEUSDT", 2024)),
        # 'AVAXUSDT': session_thr2024_p30_safe_stops_strategy(quantile_session_year_thr("AVAXUSDT", 2024)),
        # 'CRVUSDT': session_thr2024_p30_safe_stops_strategy(quantile_session_year_thr("CRVUSDT", 2024)),
        'BTCUSDT': btc_naive_p70_safe_stops_strategy_strategy12(quantile_session_year_thr("BTCUSDT", 2024)),
        'AAVEUSDT': btc_naive_p70_safe_stops_strategy_strategy12(quantile_session_year_thr("AAVEUSDT", 2024)),
        'AVAXUSDT': btc_naive_p70_safe_stops_strategy_strategy12(quantile_session_year_thr("AVAXUSDT", 2024)),
        'CRVUSDT': btc_naive_p70_safe_stops_strategy_strategy12(quantile_session_year_thr("CRVUSDT", 2024)),
    }

    strategy_1_trades = []
    strategy_12_trades = []

    trades_rows = select_closed_trades(2025)

    for row in trades_rows:
        row_id, row_strategy_name, row_open_date_utc, row_symbol, row_trade_json = row
        session_trade = session_trade_from_json(row_trade_json)
        if row_strategy_name[0:3] == '#1 ' \
                and start_of_day(to_utc_datetime(row_open_date_utc)) != start_of_day(now_utc_datetime()):
            strategy_1_trades.append((row_symbol, session_trade))

    days = {
        'BTCUSDT': select_days(2025, 'BTCUSDT'),
        'AAVEUSDT': select_days(2025, 'AAVEUSDT'),
        'AVAXUSDT': select_days(2025, 'AVAXUSDT'),
        'CRVUSDT': select_days(2025, 'CRVUSDT'),
    }
    for symbol, trade in strategy_1_trades:
        day = [x for x in days[symbol] if
               to_date_str(start_of_day(to_utc_datetime(trade.entry_time))) == x.date_readable][0]
        session_candles = day.candles_by_session(trade.hunting_session)
        new_trade = open_trade(
            trade.entry_price, trade.entry_time, trade.deadline_close, trade.hunting_session,
            trade.hunting_type, strategy_12[symbol].slg, strategy_12[symbol].tpg, trade.entry_profile_key
        )
        last_mock_candle = [session_candles[-1][3], session_candles[-1][3], session_candles[-1][3], session_candles[-1][3], 0,
                            to_date_str(to_utc_datetime(session_candles[-1][5]) + timedelta(minutes=15))]
        closed_trade = look_for_close_backtest([*session_candles, last_mock_candle], new_trade)
        strategy_12_trades.append((symbol, closed_trade))
        print('hee')

    strategy_1_trades_pnl = sum(x[1].pnl_usd for x in strategy_1_trades)
    strategy_12_trades_pnl = sum(x[1].pnl_usd for x in strategy_12_trades)

    print('session trades')


if __name__ == "__main__":
    try:
        test_notifier_trades()
        # session_2024_strats = {
        #     "BTCUSDT": session_btc_naive_p70_safe_stops_strategy_strategy(quantile_session_year_thr("BTCUSDT", 2024)),
        #     "AAVEUSDT": session_btc_naive_p70_safe_stops_strategy_strategy(quantile_session_year_thr("AAVEUSDT", 2024)),
        #     "AVAXUSDT": session_btc_naive_p70_safe_stops_strategy_strategy(quantile_session_year_thr("AVAXUSDT", 2024)),
        #     "CRVUSDT": session_btc_naive_p70_safe_stops_strategy_strategy(quantile_session_year_thr("CRVUSDT", 2024)),
        #     # "BTCUSDT": btc_naive_strategy,
        #     # "AAVEUSDT": btc_naive_strategy,
        #     # "AVAXUSDT": btc_naive_strategy,
        #     # "CRVUSDT": btc_naive_strategy,
        # }
        #
        # symbol_year_profiles = {}
        # for symbol, strategy in [
        #     ("BTCUSDT", session_2024_strats["BTCUSDT"]),
        #     ("AAVEUSDT", session_2024_strats["AAVEUSDT"]),
        #     ("AVAXUSDT", session_2024_strats["AVAXUSDT"]),
        #     ("CRVUSDT", session_2024_strats["CRVUSDT"]),
        # ]:
        #     for year in strategy.profile_years:
        #         key = f"{symbol}__{year}"
        #         if key not in symbol_year_profiles:
        #             symbol_year_profiles[key] = fill_profiles(symbol, year, strategy.thresholds_getter)
        #
        # results = []
        # for symbol, strategy in [
        #     ("BTCUSDT", session_2024_strats["BTCUSDT"]),
        #     ("AAVEUSDT", session_2024_strats["AAVEUSDT"]),
        #     ("AVAXUSDT", session_2024_strats["AVAXUSDT"]),
        #     ("CRVUSDT", session_2024_strats["CRVUSDT"]),
        # ]:
        #     backtested_results = get_backtested_profiles(symbol, symbol, strategy, symbol_year_profiles)
        #     results.append((symbol, backtested_results))
        #     upsert_profiles_to_db(strategy.name, symbol, backtested_results[0])
        #
        # pnls = [(x[0], sum([profile['pnl'] for profile in x[1][0]])) for x in results]
        # guessed = [(x[0], sum([profile['guessed'] for profile in x[1][0]])) for x in results]
        # missed = [(x[0], sum([profile['missed'] for profile in x[1][0]])) for x in results]
        # april_dates = [(x[0],
        #                 sorted([ts for sublist in [
        #                     [trade.entry_time for trade in profile['trades'] if trade.entry_time.startswith('2025-04')]
        #                     for
        #                     profile in x[1][0]] for ts in sublist])) for x in results]
        #
        # strategy_profiles = [
        #     (x[0], [profile for profile in x[1][0]
        #             if profile['win'] / len(profile['trades']) > session_2024_strats["BTCUSDT"].backtest_min_win_rate
        #             and profile['pnl'] / len(profile['trades']) > session_2024_strats[
        #                 "BTCUSDT"].backtest_min_pnl_per_trade
        #             ])
        #     for x in results]
        # strategy_pnls = [(x[0], sum([profile['pnl'] for profile in x[1]])) for x in strategy_profiles]
        # strategy_april_dates = [(x[0], sorted([ts for sublist in [
        #     [trade.entry_time for trade in profile['trades'] if trade.entry_time.startswith('2025-04')] for profile in
        #     x[1]] for ts in sublist])) for x in strategy_profiles]
        #
        # print("done!")

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)

    #  TODO:
    #   - сохранять топ профили (по какому-то трешхолду) в БД
    #   - по каждому из профилей прогнать разбивку по месяцам - с каким win rate и pnl они бы работали
