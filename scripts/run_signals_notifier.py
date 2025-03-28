from datetime import datetime
from time import sleep
from zoneinfo import ZoneInfo

from scripts.run_sessions_backtester import get_backtested_profiles
from scripts.run_sessions_typifier import typify_sessions
from stock_market_research_kit.binance_fetcher import get_today
from stock_market_research_kit.session import sessions_in_order, SessionName
from stock_market_research_kit.tg_notifier import post_signal_notification


def look_for_signal(sorted_profiles, profiles, symbol) -> str:
    today = get_today(symbol)
    if not today:
        return ""

    day_sessions = typify_sessions([today])
    if len(day_sessions) == 0:
        return ""

    last_session_index = sessions_in_order.index(day_sessions[-1].name)
    if last_session_index == len(sessions_in_order) - 1:
        return ""
    # session_to_predict = sessions_in_order[last_session_index + 1]
    synthetic_day_sessions = [f"{x.name.value}__{x.type.value}" for x in day_sessions]
    print('synthetic_day_sessions', synthetic_day_sessions)

    for profile in sorted_profiles:
        profile_sessions = [item.strip() for item in profile['profile'].split(': ')[0].split(" -> ")]

        if len(synthetic_day_sessions) < len(profile_sessions[:-1]):
            continue

        synthetic_day_sessions = synthetic_day_sessions[-(len(profile_sessions) + 1):]

        # print('profile_sessions', profile_sessions)
        if synthetic_day_sessions != profile_sessions[:-1]:
            continue

        return f"Predict {symbol} {' -> '.join(profile_sessions)}: {profile['year']} {profile['profile'].split(': ')[1]}, other years backtest pnl: ${round(profile['pnl'], 2)}, {profile['win']}/{profile['win'] + profile['lose']} {round(profile['win'] / (profile['win'] + profile['lose']) * 100, 2)}%"

    return ""


def maybe_send_signals(profiles, symbols):
    signals = [look_for_signal(profiles[i][0], profiles[i][1], x) for i, x in enumerate(symbols)]
    signals = [x for x in signals if x != ""]

    for signal in signals:
        post_signal_notification(signal)


def run_notifier(symbols):
    profiles = [get_backtested_profiles(x, x) for x in symbols]

    prev_time = datetime.now(ZoneInfo("America/New_York"))
    prev_time = datetime(prev_time.year, prev_time.month, prev_time.day, 11, 58, tzinfo=ZoneInfo("America/New_York"))
    while True:
        now_time = datetime.now(ZoneInfo("America/New_York"))
        sleep(60 - now_time.second)

        now_time = datetime.now(ZoneInfo("America/New_York"))
        print('now_time', now_time)

        if now_time.isoweekday() in [6, 7]:
            prev_time = now_time
            continue

        end_london = datetime(now_time.year, now_time.month, now_time.day, 5, 0, tzinfo=ZoneInfo("America/New_York"))
        end_early = datetime(now_time.year, now_time.month, now_time.day, 8, 0, tzinfo=ZoneInfo("America/New_York"))
        end_pre = datetime(now_time.year, now_time.month, now_time.day, 9, 30, tzinfo=ZoneInfo("America/New_York"))
        end_open = datetime(now_time.year, now_time.month, now_time.day, 10, 0, tzinfo=ZoneInfo("America/New_York"))
        end_nyam = datetime(now_time.year, now_time.month, now_time.day, 12, 0, tzinfo=ZoneInfo("America/New_York"))
        end_lunch = datetime(now_time.year, now_time.month, now_time.day, 13, 0, tzinfo=ZoneInfo("America/New_York"))
        end_nypm = datetime(now_time.year, now_time.month, now_time.day, 15, 0, tzinfo=ZoneInfo("America/New_York"))
        end_close = datetime(now_time.year, now_time.month, now_time.day, 16, 0, tzinfo=ZoneInfo("America/New_York"))

        if prev_time < end_london <= now_time:
            maybe_send_signals(profiles, symbols)
        if prev_time < end_early <= now_time:
            maybe_send_signals(profiles, symbols)
        if prev_time < end_pre <= now_time:
            maybe_send_signals(profiles, symbols)
        if prev_time < end_open <= now_time:
            maybe_send_signals(profiles, symbols)
        if prev_time < end_nyam <= now_time:
            maybe_send_signals(profiles, symbols)
        if prev_time < end_lunch <= now_time:
            maybe_send_signals(profiles, symbols)
        if prev_time < end_nypm <= now_time:
            maybe_send_signals(profiles, symbols)
        if prev_time < end_close <= now_time:
            maybe_send_signals(profiles, symbols)

        prev_time = now_time


if __name__ == "__main__":
    # TODO
    #   - делаем 2 TG канала
    #   - первый - чисто шансы на следующую сессию
    #   - второй - сигналы, каждый сигнал в БД записываем, в конце сессии пишем результат в PNL и общую стату
    try:
        run_notifier([
            "BTCUSDT",
            "AAVEUSDT"
        ])
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
