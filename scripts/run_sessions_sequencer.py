import time
from itertools import pairwise
from typing import List

from scripts.run_sessions_typifier import typify_sessions
from scripts.setup_db import connect_to_db
from stock_market_research_kit.candle_tree import tree_from_sessions, Tree
from stock_market_research_kit.day import day_from_json
from stock_market_research_kit.session import SessionType, SessionName, session_from_json, Session, sessions_in_order

ordered_trees = {
    SessionName.CME.value: Tree('tree_01_cme_ordered', 'total', 0),
    SessionName.ASIA.value: Tree('tree_02_asia_ordered', 'total', 0),
    SessionName.LONDON.value: Tree('tree_03_london_ordered', 'total', 0),
    SessionName.EARLY.value: Tree('tree_04_early_ordered', 'total', 0),
    SessionName.PRE.value: Tree('tree_05_pre_ordered', 'total', 0),
    SessionName.NY_OPEN.value: Tree('tree_06_open_ordered', 'total', 0),
    SessionName.NY_AM.value: Tree('tree_07_nyam_ordered', 'total', 0),
    SessionName.NY_LUNCH.value: Tree('tree_08_lunch_ordered', 'total', 0),
    SessionName.NY_PM.value: Tree('tree_09_nypm_ordered', 'total', 0),
    SessionName.NY_CLOSE.value: Tree('tree_10_close_ordered', 'total', 0),
}

tree_02_asia_directional = Tree('tree_02_asia_directional', 'total', 0)
tree_03_london_directional = Tree('tree_03_london_directional', 'total', 0)
tree_04_early_directional = Tree('tree_04_early_directional', 'total', 0)
tree_05_pre_directional = Tree('tree_05_pre_directional', 'total', 0)
tree_06_open_directional = Tree('tree_06_open_directional', 'total', 0)
tree_07_nyam_directional = Tree('tree_07_nyam_directional', 'total', 0)
tree_08_lunch_directional = Tree('tree_08_lunch_directional', 'total', 0)
tree_09_nypm_directional = Tree('tree_09_nypm_directional', 'total', 0)
tree_10_close_directional = Tree('tree_10_close_directional', 'total', 0)

profiles = {
    SessionName.CME.value: {},
    SessionName.ASIA.value: {},
    SessionName.LONDON.value: {},
    SessionName.EARLY.value: {},
    SessionName.PRE.value: {},
    SessionName.NY_OPEN.value: {},
    SessionName.NY_AM.value: {},
    SessionName.NY_LUNCH.value: {},
    SessionName.NY_PM.value: {},
    SessionName.NY_CLOSE.value: {},
}


def fill_trees(sessions: List[Session]):
    global tree_02_asia_directional
    global tree_03_london_directional
    global tree_04_early_directional
    global tree_05_pre_directional
    global tree_06_open_directional
    global tree_07_nyam_directional
    global tree_08_lunch_directional
    global tree_09_nypm_directional
    global tree_10_close_directional

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

    tree_02_asia_directional = tree_from_sessions(
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

    tree_03_london_directional = tree_from_sessions(
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
    tree_04_early_directional = tree_from_sessions(
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
    tree_05_pre_directional = tree_from_sessions(
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
    tree_06_open_directional = tree_from_sessions(
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
    tree_07_nyam_directional = tree_from_sessions(
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
    tree_08_lunch_directional = tree_from_sessions(
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
    tree_09_nypm_directional = tree_from_sessions(
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
    tree_10_close_directional = tree_from_sessions(
        "tree_10_close_directional",
        sessions,
        [SessionName.NY_CLOSE, SessionName.NY_PM, SessionName.NY_LUNCH, SessionName.NY_AM,
         SessionName.NY_OPEN, SessionName.PRE, SessionName.EARLY, SessionName.LONDON, SessionName.ASIA,
         SessionName.CME],
        {SessionType.BULL, SessionType.TO_THE_MOON, SessionType.BEAR, SessionType.FLASH_CRASH}
    )


def directional_profiles(session_ordered_tree, session_filtered_tree):
    result = {
        SessionType.BULL.value: [
            # (None, (36, 304, '11.8%')),
            # ('ASIA__INDECISION', (12, 98, '12.2%')),
            # ('CME__COMPRESSION', 'ASIA__INDECISION', (9, 69, '13%')),
            # ('CME__BEAR', 'ASIA__INDECISION', (2, 3, '66.6%')),
        ],
        SessionType.TO_THE_MOON.value: [],
        SessionType.BEAR.value: [],
        SessionType.FLASH_CRASH.value: []
    }

    bull, ttm, bear, fc, total = 0, 0, 0, 0, 0
    for node in session_ordered_tree.root.children:
        total += node.count

        [_, candle_type] = node.key.split('__')
        if candle_type == SessionType.BULL.value:
            bull = node.count
        elif candle_type == SessionType.TO_THE_MOON.value:
            ttm = node.count
        elif candle_type == SessionType.BEAR.value:
            bear = node.count
        elif candle_type == SessionType.FLASH_CRASH.value:
            fc = node.count

    result[SessionType.BULL.value].append((None, (bull, total, f"{str(round(bull / total * 100, 2))}%")))
    result[SessionType.TO_THE_MOON.value].append((None, (ttm, total, f"{str(round(ttm / total * 100, 2))}%")))
    result[SessionType.BEAR.value].append((None, (bear, total, f"{str(round(bear / total * 100, 2))}%")))
    result[SessionType.FLASH_CRASH.value].append((None, (fc, total, f"{str(round(fc / total * 100, 2))}%")))

    for node in session_filtered_tree.root.children:
        for path in node.get_paths():
            if len(path) <= 1:
                continue

            ordered_path = ['total']
            ordered_path.extend(reversed([x[0] for x in path][1:]))
            ordered_node = ordered_trees[SessionName.CME.value].find_by_path(ordered_path)

            if ordered_node is None:
                [session_name, _] = ordered_path[1].split('__')
                ordered_node = ordered_trees[session_name].find_by_path(ordered_path)

            [_, candle_type] = path[0][0].split('__')

            chances = tuple([x for x in ordered_path[1:]])
            chances = (
                *chances,
                (path[-1][1], ordered_node.count, f"{str(round(path[-1][1] / ordered_node.count * 100, 2))}%")
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

    fill_trees(sessions)

    profiles[SessionName.ASIA.value] = directional_profiles(ordered_trees[SessionName.ASIA.value],
                                                            tree_02_asia_directional)
    profiles[SessionName.LONDON.value] = directional_profiles(ordered_trees[SessionName.LONDON.value],
                                                              tree_03_london_directional)
    profiles[SessionName.EARLY.value] = directional_profiles(ordered_trees[SessionName.EARLY.value],
                                                             tree_04_early_directional)
    profiles[SessionName.PRE.value] = directional_profiles(ordered_trees[SessionName.PRE.value],
                                                           tree_05_pre_directional)
    profiles[SessionName.NY_OPEN.value] = directional_profiles(ordered_trees[SessionName.NY_OPEN.value],
                                                               tree_06_open_directional)
    profiles[SessionName.NY_AM.value] = directional_profiles(ordered_trees[SessionName.NY_AM.value],
                                                             tree_07_nyam_directional)
    profiles[SessionName.NY_LUNCH.value] = directional_profiles(ordered_trees[SessionName.NY_LUNCH.value],
                                                                tree_08_lunch_directional)
    profiles[SessionName.NY_PM.value] = directional_profiles(ordered_trees[SessionName.NY_PM.value],
                                                             tree_09_nypm_directional)
    profiles[SessionName.NY_CLOSE.value] = directional_profiles(ordered_trees[SessionName.NY_CLOSE.value],
                                                                tree_10_close_directional)

    print(f"Filling profiles from {len(sessions)} {year} {symbol} sessions took {(time.perf_counter() - start_time):.6f} seconds")
    conn.close()


#  returns {str (session_name.value): {str (session_type.value): tuple(session_name, ..., (win,all,pnl))[]}}
def get_successful_profiles(session_names: List[str], min_times: int, min_chance: float):
    result = {}
    for session_name in profiles:
        if session_name not in session_names:
            continue
        for candle_type in profiles[session_name]:
            for profile in profiles[session_name][candle_type]:
                if profile[-1][0] < min_times or float(profile[-1][2].split('%')[0]) < min_chance:
                    continue
                if session_name not in result:
                    result[session_name] = {}
                if candle_type not in result[session_name]:
                    result[session_name][candle_type] = []
                result[session_name][candle_type].append(profile)

    return result


def get_next_session_chances(typed_sessions: List[str]):
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
            result['variants'].append((
                f"{' -> '.join(sequence)} -> {node.key}: {node.count}/{next_session_sum} {round(node.count / next_session_sum * 100, 2)}%"
            ))

    next_session_top = sorted(
        ordered_trees[result['next_session']].root.children, key=lambda x: x.count, reverse=True)
    next_session_sum = sum([x.count for x in ordered_trees[result['next_session']].root.children])
    for node in next_session_top:
        result['variants'].append((
            f"{node.key}: {node.count}/{next_session_sum} {round(node.count / next_session_sum * 100, 2)}%"
        ))
    return result


if __name__ == "__main__":
    try:
        fill_profiles("BTCUSDT", 2024)
        get_next_session_chances(['CME Open__COMPRESSION', 'Asia Open__INDECISION', 'London Open__BEAR'])
        # successful_profiles = get_successful_profiles(
        #     [x.value for x in [SessionName.EARLY, SessionName.PRE, SessionName.NY_OPEN, SessionName.NY_AM,
        #                        SessionName.NY_LUNCH, SessionName.NY_PM, SessionName.NY_CLOSE]],
        #     2,
        #     40
        # )
        # print('len(successful_profiles)', len(successful_profiles))
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
