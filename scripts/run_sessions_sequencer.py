import time
from itertools import pairwise
from typing import List

from scripts.run_sessions_typifier import typify_sessions
from scripts.setup_db import connect_to_db
from stock_market_research_kit.candle_tree import tree_from_sessions, Tree
from stock_market_research_kit.day import day_from_json
from stock_market_research_kit.session import SessionType, SessionName, Session, sessions_in_order, \
    SessionImpact


def fill_trees(sessions: List[Session]):
    ordered_trees = {}
    directional_trees = {}

    ordered_trees[SessionName.CME.value] = tree_from_sessions(
        "tree_01_cme_ordered",
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

    ordered_trees[SessionName.ASIA.value] = tree_from_sessions(
        "tree_02_asia_ordered",
        sessions,
        [SessionName.ASIA, SessionName.LONDON, SessionName.EARLY, SessionName.PRE,
         SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )

    directional_trees[SessionName.ASIA.value] = tree_from_sessions(
        "tree_02_asia_directional",
        sessions,
        [SessionName.ASIA, SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )

    ordered_trees[SessionName.LONDON.value] = tree_from_sessions(
        "tree_03_london_ordered",
        sessions,
        [SessionName.LONDON, SessionName.EARLY, SessionName.PRE,
         SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )

    directional_trees[SessionName.LONDON.value] = tree_from_sessions(
        "tree_03_london_directional",
        sessions,
        [SessionName.LONDON, SessionName.ASIA, SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )

    ordered_trees[SessionName.EARLY.value] = tree_from_sessions(
        "tree_04_early_ordered",
        sessions,
        [SessionName.EARLY, SessionName.PRE,
         SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )
    directional_trees[SessionName.EARLY.value] = tree_from_sessions(
        "tree_04_early_directional",
        sessions,
        [SessionName.EARLY, SessionName.LONDON, SessionName.ASIA, SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )

    ordered_trees[SessionName.PRE.value] = tree_from_sessions(
        "tree_05_pre_ordered",
        sessions,
        [SessionName.PRE,
         SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )
    directional_trees[SessionName.PRE.value] = tree_from_sessions(
        "tree_05_pre_directional",
        sessions,
        [SessionName.PRE, SessionName.EARLY, SessionName.LONDON, SessionName.ASIA, SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )

    ordered_trees[SessionName.NY_OPEN.value] = tree_from_sessions(
        "tree_06_open_ordered",
        sessions,
        [SessionName.NY_OPEN, SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM,
         SessionName.NY_CLOSE],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )
    directional_trees[SessionName.NY_OPEN.value] = tree_from_sessions(
        "tree_06_open_directional",
        sessions,
        [SessionName.NY_OPEN, SessionName.PRE, SessionName.EARLY, SessionName.LONDON, SessionName.ASIA,
         SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )

    ordered_trees[SessionName.NY_AM.value] = tree_from_sessions(
        "tree_07_nyam_ordered",
        sessions,
        [SessionName.NY_AM, SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )
    directional_trees[SessionName.NY_AM.value] = tree_from_sessions(
        "tree_07_nyam_directional",
        sessions,
        [SessionName.NY_AM, SessionName.NY_OPEN, SessionName.PRE, SessionName.EARLY, SessionName.LONDON,
         SessionName.ASIA, SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )

    ordered_trees[SessionName.NY_LUNCH.value] = tree_from_sessions(
        "tree_08_lunch_ordered",
        sessions,
        [SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )
    directional_trees[SessionName.NY_LUNCH.value] = tree_from_sessions(
        "tree_08_lunch_directional",
        sessions,
        [SessionName.NY_LUNCH, SessionName.NY_AM, SessionName.NY_OPEN, SessionName.PRE,
         SessionName.EARLY, SessionName.LONDON, SessionName.ASIA, SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )

    ordered_trees[SessionName.NY_PM.value] = tree_from_sessions(
        "tree_09_nypm_ordered",
        sessions,
        [SessionName.NY_PM, SessionName.NY_CLOSE],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )
    directional_trees[SessionName.NY_PM.value] = tree_from_sessions(
        "tree_09_nypm_directional",
        sessions,
        [SessionName.NY_PM, SessionName.NY_LUNCH, SessionName.NY_AM, SessionName.NY_OPEN,
         SessionName.PRE, SessionName.EARLY, SessionName.LONDON, SessionName.ASIA, SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )

    ordered_trees[SessionName.NY_CLOSE.value] = tree_from_sessions(
        "tree_10_close_ordered",
        sessions,
        [SessionName.NY_CLOSE],
        {
            SessionType.COMPRESSION, SessionType.DOJI, SessionType.INDECISION, SessionType.BULL,
            SessionType.TO_THE_MOON,
            SessionType.STB, SessionType.REJECTION_BULL, SessionType.HAMMER, SessionType.BEAR, SessionType.FLASH_CRASH,
            SessionType.BTS, SessionType.REJECTION_BEAR, SessionType.BEAR_HAMMER,
            SessionType.V_SHAPE, SessionType.PUMP_AND_DUMP}
    )
    directional_trees[SessionName.NY_CLOSE.value] = tree_from_sessions(
        "tree_10_close_directional",
        sessions,
        [SessionName.NY_CLOSE, SessionName.NY_PM, SessionName.NY_LUNCH, SessionName.NY_AM,
         SessionName.NY_OPEN, SessionName.PRE, SessionName.EARLY, SessionName.LONDON, SessionName.ASIA,
         SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )

    return ordered_trees, directional_trees


def to_sorted_distr(distr, ttl):
    res = []
    for key in distr:
        if key != SessionImpact.UNSPECIFIED.value:
            res.append((key, (distr[key], ttl, f"{str(round(distr[key] / ttl * 100, 2))}%")))
    return sorted(res, key=lambda x: x[1][0], reverse=True)


def directional_profiles(session_ordered_tree, session_filtered_tree, ordered_trees):
    result = {
        SessionType.BULL.value: [
            # ([], (36, 304, '11.8%'), {}),
            # (['ASIA__INDECISION'], (12, 98, '12.2%'), {}),
            # (['CME__COMPRESSION', 'ASIA__INDECISION'], (9, 69, '13%'), {}),
            # (['CME__BEAR', 'ASIA__INDECISION'], (2, 3, '66.6%'), {}),
        ],
        SessionType.TO_THE_MOON.value: [],
        SessionType.BEAR.value: [],
        SessionType.FLASH_CRASH.value: []
    }

    bull, ttm, bear, fc, total = (0, {}), (0, {}), (0, {}), (0, {}), 0
    for node in session_ordered_tree.root.children:
        total += node.count

        [_, candle_type] = node.key.split('__')
        if candle_type == SessionType.BULL.value:
            bull = node.count, node.distribution
        elif candle_type == SessionType.TO_THE_MOON.value:
            ttm = node.count, node.distribution
        elif candle_type == SessionType.BEAR.value:
            bear = node.count, node.distribution
        elif candle_type == SessionType.FLASH_CRASH.value:
            fc = node.count, node.distribution

    bull_distr = to_sorted_distr(bull[1], total)
    ttm_distr = to_sorted_distr(ttm[1], total)
    bear_distr = to_sorted_distr(bear[1], total)
    fc_distr = to_sorted_distr(fc[1], total)

    result[SessionType.BULL.value].append((
        [], (bull[0], total, f"{str(round(bull[0] / total * 100, 2))}%"), bull_distr
    ))
    result[SessionType.TO_THE_MOON.value].append((
        [], (ttm[0], total, f"{str(round(ttm[0] / total * 100, 2))}%"), ttm_distr
    ))
    result[SessionType.BEAR.value].append((
        [], (bear[0], total, f"{str(round(bear[0] / total * 100, 2))}%"), bear_distr
    ))
    result[SessionType.FLASH_CRASH.value].append((
        [], (fc[0], total, f"{str(round(fc[0] / total * 100, 2))}%"), fc_distr
    ))

    for node in session_filtered_tree.root.children:
        for path in node.get_paths():
            if len(path) <= 1:
                continue

            ordered_path = ['total']
            ordered_path.extend(reversed([x[0] for x in path][1:]))
            ordered_node = ordered_trees[SessionName.CME.value].find_by_path(ordered_path)
            predicted_ordered_node = ordered_trees[SessionName.CME.value].find_by_path([*ordered_path, path[0][0]])

            if ordered_node is None:
                [session_name, _] = ordered_path[1].split('__')
                ordered_node = ordered_trees[session_name].find_by_path(ordered_path)
                predicted_ordered_node = ordered_trees[session_name].find_by_path([*ordered_path, path[0][0]])

            [_, candle_type] = path[0][0].split('__')

            if not predicted_ordered_node or not ordered_node:
                print('hello there!')

            chances = (
                ordered_path[1:],
                (predicted_ordered_node.count, ordered_node.count,
                 f"{str(round(predicted_ordered_node.count / ordered_node.count * 100, 2))}%"),
                to_sorted_distr(predicted_ordered_node.distribution, ordered_node.count)
            )
            result[candle_type].append(chances)

    return result


def fill_profiles(symbol, year):
    start_time = time.perf_counter()
    conn = connect_to_db(year)
    c = conn.cursor()

    c.execute("""SELECT data FROM days WHERE symbol = ?""", (symbol,))
    days_rows = c.fetchall()

    if len(days_rows) == 0:
        print(f"Symbol {symbol} not found in days table")
        return

    sessions = typify_sessions([day_from_json(x[0]) for x in days_rows])
    ordered_trees, directional_trees = fill_trees(sessions)

    profiles = {}
    profiles[SessionName.CME.value] = {}
    profiles[SessionName.ASIA.value] = directional_profiles(ordered_trees[SessionName.ASIA.value],
                                                            directional_trees[SessionName.ASIA.value], ordered_trees)
    profiles[SessionName.LONDON.value] = directional_profiles(ordered_trees[SessionName.LONDON.value],
                                                              directional_trees[SessionName.LONDON.value],
                                                              ordered_trees)
    profiles[SessionName.EARLY.value] = directional_profiles(ordered_trees[SessionName.EARLY.value],
                                                             directional_trees[SessionName.EARLY.value], ordered_trees)
    profiles[SessionName.PRE.value] = directional_profiles(ordered_trees[SessionName.PRE.value],
                                                           directional_trees[SessionName.PRE.value], ordered_trees)
    profiles[SessionName.NY_OPEN.value] = directional_profiles(ordered_trees[SessionName.NY_OPEN.value],
                                                               directional_trees[SessionName.NY_OPEN.value],
                                                               ordered_trees)
    profiles[SessionName.NY_AM.value] = directional_profiles(ordered_trees[SessionName.NY_AM.value],
                                                             directional_trees[SessionName.NY_AM.value], ordered_trees)
    profiles[SessionName.NY_LUNCH.value] = directional_profiles(ordered_trees[SessionName.NY_LUNCH.value],
                                                                directional_trees[SessionName.NY_LUNCH.value],
                                                                ordered_trees)
    profiles[SessionName.NY_PM.value] = directional_profiles(ordered_trees[SessionName.NY_PM.value],
                                                             directional_trees[SessionName.NY_PM.value], ordered_trees)
    profiles[SessionName.NY_CLOSE.value] = directional_profiles(ordered_trees[SessionName.NY_CLOSE.value],
                                                                directional_trees[SessionName.NY_CLOSE.value],
                                                                ordered_trees)

    print(
        f"Filling profiles from {len(sessions)} {year} {symbol} sessions took {(time.perf_counter() - start_time):.6f} seconds")
    conn.close()
    return ordered_trees, directional_trees, profiles


#  returns {str (session_name.value): {str (session_type.value): tuple(session_name, ..., (win,all,pnl))[]}}
def get_successful_profiles(session_names: List[str], min_times: int, min_chance: float, profiles):
    result = {}
    for session_name in profiles:
        if session_name not in session_names:
            continue
        for candle_type in profiles[session_name]:
            for profile in profiles[session_name][candle_type]:
                if profile[1][0] < min_times or float(profile[1][2].split('%')[0]) < min_chance:
                    continue
                if session_name not in result:
                    result[session_name] = {}
                if candle_type not in result[session_name]:
                    result[session_name][candle_type] = []
                result[session_name][candle_type].append(profile)

    return result


def get_next_session_chances(typed_sessions: List[str], ordered_trees):
    result = {
        'next_session': '',
        'variants': []
    }
    if len(typed_sessions) == 0 or SessionName(typed_sessions[-1].split('__')[0]) == SessionName.NY_CLOSE:
        return result
    result['next_session'] = dict(pairwise(sessions_in_order)).get(SessionName(typed_sessions[-1].split('__')[0])).value

    for sequence in [typed_sessions[i:] for i in range(len(typed_sessions))]:
        sequence_node = ordered_trees[sequence[0].split('__')[0]].find_by_path(['total', *sequence])
        if not sequence_node:
            continue
        next_session_top = sorted(
            sequence_node.children,
            key=lambda x: x.count, reverse=True
        )
        next_session_sum = sum([x.count for x in next_session_top])
        for node in next_session_top:
            sorted_distr = to_sorted_distr(node.distribution, next_session_sum)
            distr_str = ""
            if len(sorted_distr) > 0:
                distr_str = ", ".join([f"{x[0]} {x[1][0]}/{x[1][1]} {x[1][2]}" for x in sorted_distr])
                distr_str = f" ({distr_str})"
            result['variants'].append((
                f"{' -> '.join(sequence)} -> <b>{node.key}: {node.count}/{next_session_sum} {round(node.count / next_session_sum * 100, 2)}%</b>{distr_str}"
            ))

    next_session_top = sorted(
        ordered_trees[result['next_session']].root.children, key=lambda x: x.count, reverse=True)
    next_session_sum = sum([x.count for x in ordered_trees[result['next_session']].root.children])
    for node in next_session_top:
        sorted_distr = to_sorted_distr(node.distribution, next_session_sum)
        distr_str = ""
        if len(sorted_distr) > 0:
            distr_str = ", ".join([f"{x[0]} {x[1][0]}/{x[1][1]} {x[1][2]}" for x in sorted_distr])
            distr_str = f" ({distr_str})"
        result['variants'].append((
            f"<b>{node.key}: {node.count}/{next_session_sum} {round(node.count / next_session_sum * 100, 2)}%</b>{distr_str}"
        ))
    return result


if __name__ == "__main__":
    try:
        ot, dt, pp = fill_profiles("BTCUSDT", 2024)
        get_next_session_chances(['CME Open__COMPRESSION', 'Asia Open__INDECISION', 'London Open__BEAR'], ot)
        successful_profiles = get_successful_profiles(
            [x.value for x in [SessionName.EARLY, SessionName.PRE, SessionName.NY_OPEN, SessionName.NY_AM,
                               SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE]],
            2,
            40,
            pp
        )
        print('len(successful_profiles)', len(successful_profiles))
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
