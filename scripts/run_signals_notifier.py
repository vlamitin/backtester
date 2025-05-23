import time
from datetime import datetime, timedelta, time as dtime
from typing import List, Tuple
from zoneinfo import ZoneInfo

from scripts.run_day_markuper import markup_days
from scripts.run_series_raw_loader import update_candle_from_binance
from scripts.run_sessions_backtester import look_for_entry_backtest, look_for_close_backtest
from scripts.run_sessions_typifier import typify_sessions, typify_session
from stock_market_research_kit.candle import as_1_candle
from stock_market_research_kit.db_layer import upsert_trades_to_db, select_closed_trades, select_last_day_candles, \
    select_open_trades_by_strategies, select_sorted_profiles
from stock_market_research_kit.notifier_strategy import btc_naive_strategy01, NotifierStrategy, \
    thr2024_p70_safe_stops_strategy05, thr2024_strict_strategy03, thr2024_loose_strategy04, \
    btc_naive_p30_safe_stops_strategy_strategy11, thr2024_p30_safe_stops_strategy07, \
    thr2024_strict_p70_safe_stops_strategy10, thr2024_loose_p70_safe_stops_strategy06, tdo_thr2024_loose_strategy14
from stock_market_research_kit.session import get_next_session_mock
from stock_market_research_kit.session_quantiles import quantile_session_year_thr
from stock_market_research_kit.session_trade import session_trade_from_json
from stock_market_research_kit.tg_notifier import post_signal_notification, TelegramThrottler
from utils.date_utils import to_date_str, log_warn, to_utc_datetime, log_info_ny, log_warn_ny


def to_trade_profile(backtest_profile_key):
    sessions_str, stats_str = backtest_profile_key.split(': ')
    typed_sessions = sessions_str.split(' -> ')

    win_rate_abs, win_rate_rel = stats_str.split(' ')
    win, trades_count = map(int, win_rate_abs.split('/'))

    predict_session, predict_type = typed_sessions[-1].split('__')

    return {
        predict_session: {
            predict_type: (
                typed_sessions[:-1],
                (win, trades_count, win_rate_rel),
                []
            )
        }
    }


def look_for_new_trade(symbol, time_till: datetime, strategy: NotifierStrategy):
    now_year = time_till.year  # TODO не будет работать при смене года

    candles_15m = select_last_day_candles(now_year, to_date_str(time_till), symbol)
    if len(candles_15m) == 0:
        log_warn("0 candles 15m in look_for_new_trade")
        return

    days = markup_days(candles_15m)

    day_sessions = typify_sessions([days[-1]], strategy.thresholds_getter)
    if len(day_sessions) == 0:
        log_warn_ny("len(day_sessions) == 0!")
        return

    predicted_session_mock = get_next_session_mock(day_sessions[-1].name, days[-1].date_readable)
    if not predicted_session_mock:
        log_warn_ny("no predicted_session_mock!")
        return

    predicted_session_mock.open = candles_15m[-1][3]  # TODO не будет норм раб, если вызывается в конце сессии в while

    sorted_profiles = select_sorted_profiles(strategy.name, symbol)
    if len(sorted_profiles) == 0:
        log_warn_ny("no sorted_profiles!!")
        return
    trade_profiles = [
        to_trade_profile(x['profile']) for x in sorted_profiles
        if x['win'] / len(x['trades']) > strategy.backtest_min_win_rate
           and x['pnl'] / len(x['trades']) > strategy.backtest_min_pnl_per_trade
           and list(to_trade_profile(x['profile']).keys())[0] == predicted_session_mock.name.value
    ]

    profiles_map = {}
    for sub_map in trade_profiles:
        predict_session = list(sub_map.keys())[0]
        if predict_session not in profiles_map:
            profiles_map[predict_session] = {}

        predict_type = list(sub_map[predict_session].keys())[0]
        if predict_type not in profiles_map[predict_session]:
            profiles_map[predict_session][predict_type] = []
        profiles_map[predict_session][predict_type].append(sub_map[predict_session][predict_type])

    new_trades = look_for_entry_backtest([*day_sessions, predicted_session_mock], days[-1], profiles_map, strategy)

    if len(new_trades) == 0:
        log_info_ny(f"no new trades for {symbol} in {predicted_session_mock.name.value} for '{strategy.name[0:3]}...'")
        return

    # TODO пока выбираю из всех найденных сделку с наивысшим guess_rate
    max_guess_rate, trade = 0, None
    for tr in new_trades:
        profile = [x for x in sorted_profiles if x['profile'] == tr.entry_profile_key][0]
        guess_rate = profile['guessed'] / (profile['guessed'] + profile['missed'])
        if guess_rate >= max_guess_rate:
            max_guess_rate, trade = guess_rate, tr

    # print('trade')

    row_ids = upsert_trades_to_db(now_year, strategy.name, symbol, [trade])

    for i, tr in enumerate([trade]):
        profile = [x for x in sorted_profiles if x['profile'] == tr.entry_profile_key][0]
        TelegramThrottler().send_signal_message(f"""New trade #{row_ids[i]}:

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

    rows = select_closed_trades(now_year)

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


def handle_open_trades(symbols_with_strategies: List[Tuple[str, NotifierStrategy]]):
    unique_strategies = list(set([x[1].name for x in symbols_with_strategies]))

    now_year = datetime.now(ZoneInfo("UTC")).year  # TODO не будет работать при смене года

    open_trades_rows = select_open_trades_by_strategies(now_year, unique_strategies)

    trades_with_candles = []
    for row in open_trades_rows:
        if len(trades_with_candles) > 0 and row[0] == trades_with_candles[-1][0][0]:
            trades_with_candles[-1].append(row)
        else:
            trades_with_candles.append([row])
    trades_with_candles = sorted(trades_with_candles, key=lambda x: x[0][0])
    for rows in trades_with_candles:
        row_id, row_strategy_name, row_trade_open_date_utc, row_symbol, row_trade_json = rows[0][0:5]
        session_trade = session_trade_from_json(row_trade_json)
        strategy = [x[1] for x in symbols_with_strategies if x[0] == row_symbol and x[1].name == row_strategy_name][0]
        candles_15m = [x[5:] for x in rows]
        last_mock_candle = [candles_15m[-1][3], candles_15m[-1][3], candles_15m[-1][3], candles_15m[-1][3], 0,
                            to_date_str(to_utc_datetime(candles_15m[-1][5]) + timedelta(minutes=15))]
        closed_trade = look_for_close_backtest([*candles_15m, last_mock_candle], session_trade)
        if not closed_trade:
            continue

        closed_trade.result_type = typify_session(
            session_trade.hunting_session, as_1_candle(candles_15m), strategy.thresholds_getter
        )
        upsert_trades_to_db(now_year, strategy.name, row_symbol, [closed_trade])
        stats = stat_for_a_closed_trade(strategy.name, row_trade_open_date_utc, row_symbol)
        TelegramThrottler().send_signal_message(f"""Close trade #{row_id} with reason '{closed_trade.closes[-1][3]}':

    Symbol: {row_symbol},
    Time open/close: {closed_trade.entry_time} / {closed_trade.closes[-1][2]} (UTC),
    Price open/close: {closed_trade.entry_price} / {closed_trade.closes[-1][1]},
    PNL: {round(closed_trade.pnl_usd, 2)}$.

    Session {closed_trade.hunting_session.name} predicted {closed_trade.hunting_type.name} ({closed_trade.predict_direction}), but instead got {closed_trade.result_type.value}.

    WIN/GUESSED/TRADES/PNL
    Total: {stats['total_win_trades']}/{stats['total_guessed']}/{stats['total_trades']}/{round(stats['total_pnl'], 2)}$,
    Strategy: {stats['strategy_win_trades']}/{stats['strategy_guessed']}/{stats['strategy_trades']}/{round(stats['strategy_pnl'], 2)}$,
    {row_symbol}: {stats['symbol_win_trades']}/{stats['symbol_guessed']}/{stats['symbol_trades']}/{round(stats['symbol_pnl'], 2)}$,
    {row_symbol}+Strategy: {stats['symbol_strategy_win_trades']}/{stats['symbol_strategy_guessed']}/{stats['symbol_strategy_trades']}/{round(stats['symbol_strategy_pnl'], 2)}$.

    Strategy: {strategy.name}.
""")


# def maybe_post_session_stat(symbols, symbol_year_profiles):
#     now_year = datetime.now(ZoneInfo("UTC")).year  # TODO не будет работать при смене года
#
#     for symbol in symbols:
#         candles_15m = select_last_day_candles(now_year, symbol)
#         if len(candles_15m) == 0:
#             log_warn("0 candles 15m in look_for_new_trade")
#             return
#
#         days = markup_days(candles_15m)
#
#         day_sessions = typify_sessions([days[-1]])
#         if len(day_sessions) == 0:
#             log_warn("len(day_sessions) == 0!")
#             continue
#
#         chances = get_next_session_chances(
#             [f"{x.name.value}__{x.type.value}" for x in day_sessions],
#             symbol_year_profiles[f"{symbol}__2024"][0]
#         )
#         if len(chances['variants']) == 0:
#             continue
#
#         from_to = get_from_to(SessionName(chances['next_session']), to_date_str(start_of_day(now_utc_datetime())))
#         variants = "\n".join(chances['variants'])
#         post_stat_notification(f"""Next <b>{symbol}</b> session is <b>{chances['next_session']}</b>
# (from {from_to[0]} to {from_to[1]} UTC), chances based on 2024:
#
# {variants}
# """)


def update_candles(symbols):
    for symbol in symbols:
        update_candle_from_binance(symbol)


def ny_time_generator(start_date: datetime, times_ny: list[dtime]):
    current_date = start_date.astimezone(ZoneInfo("America/New_York"))
    while True:
        if current_date.isoweekday() not in [6, 7]:
            for t in times_ny:
                dt_ny = datetime.combine(current_date.date(), t, tzinfo=ZoneInfo("America/New_York"))
                if dt_ny < start_date:
                    continue
                dt_utc = dt_ny.astimezone(ZoneInfo("UTC"))
                now_utc = datetime.now(ZoneInfo("UTC"))
                if dt_utc > now_utc:
                    seconds_to_wait = (dt_utc - now_utc).total_seconds()
                    time.sleep(seconds_to_wait)
                yield dt_utc
        current_date += timedelta(days=1)


def run_notifier(symbols_with_strategy):
    unique_symbols = list(set([x[0] for x in symbols_with_strategy]))

    times_of_day = [
        dtime(7, 0),  # early open
        dtime(8, 0),  # pre open
        dtime(9, 30),  # ny open
        dtime(10, 0),  # ny am open
        dtime(12, 0),  # ny lunch start
        dtime(13, 0),  # ny pm open
        dtime(15, 0),  # ny close start
        dtime(16, 0),  # ny close
    ]
    gen = ny_time_generator(
        datetime(2025, 5, 6, 9, 28, tzinfo=ZoneInfo("America/New_York")),
        # datetime.now(ZoneInfo("America/New_York")),
        times_of_day
    )

    log_info_ny(f"Notifier starting took {(time.perf_counter() - start_time):.6f} seconds")

    while True:
        next_dt_utc = next(gen)
        print(f"handling next_dt_utc {next_dt_utc}")
        handling_start = time.perf_counter()
        update_candles(unique_symbols)
        handle_open_trades(symbols_with_strategy)
        for symbol, strategy in symbols_with_strategy:
            look_for_new_trade(symbol, next_dt_utc, strategy)
        # maybe_post_session_stat(symbol_strategy_backtested_profiles_tuples)
        log_info_ny(f"Candles&Trades handling took {(time.perf_counter() - handling_start):.6f} seconds")


if __name__ == "__main__":
    try:
        log_info_ny("Starting Notifier")
        start_time = time.perf_counter()
        update_candles(["BTCUSDT"])
        update_candles(["AAVEUSDT"])
        update_candles(["AVAXUSDT"])
        update_candles(["CRVUSDT"])
        run_notifier([
            ("BTCUSDT", btc_naive_p30_safe_stops_strategy_strategy11(quantile_session_year_thr("BTCUSDT", 2024))),
            ("AAVEUSDT", btc_naive_p30_safe_stops_strategy_strategy11(quantile_session_year_thr("AAVEUSDT", 2024))),
            ("AVAXUSDT", btc_naive_p30_safe_stops_strategy_strategy11(quantile_session_year_thr("AVAXUSDT", 2024))),
            ("CRVUSDT", btc_naive_p30_safe_stops_strategy_strategy11(quantile_session_year_thr("CRVUSDT", 2024))),
            ("BTCUSDT", thr2024_p30_safe_stops_strategy07(quantile_session_year_thr("BTCUSDT", 2024))),
            ("AAVEUSDT", thr2024_p30_safe_stops_strategy07(quantile_session_year_thr("AAVEUSDT", 2024))),
            ("AVAXUSDT", thr2024_p30_safe_stops_strategy07(quantile_session_year_thr("AVAXUSDT", 2024))),
            ("CRVUSDT", thr2024_p30_safe_stops_strategy07(quantile_session_year_thr("CRVUSDT", 2024))),
            ("BTCUSDT", thr2024_strict_p70_safe_stops_strategy10(quantile_session_year_thr("BTCUSDT", 2024))),
            ("AAVEUSDT", thr2024_strict_p70_safe_stops_strategy10(quantile_session_year_thr("AAVEUSDT", 2024))),
            ("AVAXUSDT", thr2024_strict_p70_safe_stops_strategy10(quantile_session_year_thr("AVAXUSDT", 2024))),
            ("CRVUSDT", thr2024_strict_p70_safe_stops_strategy10(quantile_session_year_thr("CRVUSDT", 2024))),
            ("BTCUSDT", thr2024_loose_p70_safe_stops_strategy06(quantile_session_year_thr("BTCUSDT", 2024))),
            ("AAVEUSDT", thr2024_loose_p70_safe_stops_strategy06(quantile_session_year_thr("AAVEUSDT", 2024))),
            ("AVAXUSDT", thr2024_loose_p70_safe_stops_strategy06(quantile_session_year_thr("AVAXUSDT", 2024))),
            ("CRVUSDT", thr2024_loose_p70_safe_stops_strategy06(quantile_session_year_thr("CRVUSDT", 2024))),
            ("BTCUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("BTCUSDT", 2024))),
            ("AAVEUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("AAVEUSDT", 2024))),
            ("CRVUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("CRVUSDT", 2024))),
            ("AVAXUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("AVAXUSDT", 2024))),
            ("1INCHUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("1INCHUSDT", 2024))),
            ("COMPUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("COMPUSDT", 2024))),
            ("LINKUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("LINKUSDT", 2024))),
            ("LTCUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("LTCUSDT", 2024))),
            ("SUSHIUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("SUSHIUSDT", 2024))),
            ("UNIUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("UNIUSDT", 2024))),
            ("XLMUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("XLMUSDT", 2024))),
            ("XMRUSDT", tdo_thr2024_loose_strategy14(quantile_session_year_thr("XMRUSDT", 2024))),
        ])
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
