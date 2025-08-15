import copy
from datetime import timedelta

from stock_market_research_kit.db_layer import select_full_days_candles_15m, select_candles_15m
from stock_market_research_kit.triad import Reverse15mGenerator, new_triad, triad_from_json, json_from_triad, Triad
from utils.date_utils import to_utc_datetime

snapshot_triad_23_15 = open("stock_market_research_kit/test_snapshots/triad_btc-eth-sol_08_08_2025_23_15.json", "r",
                            encoding="utf-8").read()
snapshot_triad_23_30 = open("stock_market_research_kit/test_snapshots/triad_btc-eth-sol_08_08_2025_23_30.json", "r",
                            encoding="utf-8").read()
snapshot_triad_23_45 = open("stock_market_research_kit/test_snapshots/triad_btc-eth-sol_08_08_2025_23_45.json", "r",
                            encoding="utf-8").read()
snapshot_triad_00_00 = open("stock_market_research_kit/test_snapshots/triad_btc-eth-sol_08_09_2025_00_00.json", "r",
                            encoding="utf-8").read()
snapshot_triad_00_15 = open("stock_market_research_kit/test_snapshots/triad_btc-eth-sol_08_09_2025_00_15.json", "r",
                            encoding="utf-8").read()


def triad_from_snapshot(snap: str) -> Triad:
    t = triad_from_json(snap)

    t.a1.candles_15m = select_candles_15m(
        2024, t.a1.symbol,
        "2024-01-01 00:00", t.a1.snapshot_date_readable
    ) + select_candles_15m(
        2025, t.a1.symbol,
        "2025-01-01 00:00", t.a1.snapshot_date_readable
    )

    t.a2.candles_15m = select_candles_15m(
        2024, t.a2.symbol,
        "2024-01-01 00:00", t.a2.snapshot_date_readable
    ) + select_candles_15m(
        2025, t.a2.symbol,
        "2025-01-01 00:00", t.a2.snapshot_date_readable
    )

    t.a3.candles_15m = select_candles_15m(
        2024, t.a3.symbol,
        "2024-01-01 00:00", t.a3.snapshot_date_readable
    ) + select_candles_15m(
        2025, t.a3.symbol,
        "2025-01-01 00:00", t.a3.snapshot_date_readable
    )

    return t


def reverse_09_aug_2025_test_generator(symbol) -> Reverse15mGenerator:
    candles_2024 = select_full_days_candles_15m(2024, symbol)
    candles_2025 = [x for x in select_full_days_candles_15m(2025, symbol) if
                    to_utc_datetime(x[5]) < to_utc_datetime('2025-08-10 00:00')]
    candles = candles_2024 + candles_2025[:-99]

    index = len(candles) - 1

    while True:
        yield candles[index]
        index = index - 1


def test_09_aug_2025_smt():
    candles_2025_btc = select_full_days_candles_15m(2025, "BTCUSDT")
    candles_2025_eth = select_full_days_candles_15m(2025, "ETHUSDT")
    candles_2025_sol = select_full_days_candles_15m(2025, "SOLUSDT")
    triad_23_15 = triad_from_snapshot(snapshot_triad_23_15)

    actual_smt = triad_23_15.actual_smt_psp()

    triad_23_30 = triad_from_snapshot(snapshot_triad_23_30)


def test_09_aug_2025_snapshot_diff():
    triad_23_15 = triad_from_json(snapshot_triad_23_15)
    smt_psp_23_15 = triad_23_15.actual_smt_psp()

    triad_23_30 = triad_from_json(snapshot_triad_23_30)
    smt_psp_23_30 = triad_23_30.actual_smt_psp()

    diff_23_30 = Triad.diff_smt_psp(smt_psp_23_15, smt_psp_23_30)

    triad_23_45 = triad_from_json(snapshot_triad_23_45)
    smt_psp_23_45 = triad_23_45.actual_smt_psp()

    triad_00_00 = triad_from_json(snapshot_triad_00_00)
    smt_psp_00_00 = triad_00_00.actual_smt_psp()

    triad_00_15 = triad_from_json(snapshot_triad_00_15)
    smt_psp_00_15 = triad_00_15.actual_smt_psp()


def test_09_aug_2025():
    triad = new_triad(
        ("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        (
            reverse_09_aug_2025_test_generator("BTCUSDT"),
            reverse_09_aug_2025_test_generator("ETHUSDT"),
            reverse_09_aug_2025_test_generator("SOLUSDT"),
        )
    )  # TODO
    # triad = triad_from_json(snapshot_triad_1)
    # triad.a1.candles_15m = select_candles_15m(2024, triad.a1.symbol, "2024-01-01 00:00",
    #                                           triad.a1.snapshot_date_readable)
    # smt_psp = triad.actual_smt_psp()

    # j_triad = json_from_triad(triad)

    triad_copy = copy.deepcopy(triad)
    triad_copy.a1.candles_15m = []
    triad_copy.a2.candles_15m = []
    triad_copy.a3.candles_15m = []
    assert json_from_triad(triad_copy) + '\n' == snapshot_triad_23_15
    assert triad.a1.candles_15m[0][5] == "2024-01-01 00:00"
    assert triad.a1.candles_15m[-1][5] == "2025-08-08 23:00"

    candles_2025_btc = select_full_days_candles_15m(2025, "BTCUSDT")
    candles_2025_eth = select_full_days_candles_15m(2025, "ETHUSDT")
    candles_2025_sol = select_full_days_candles_15m(2025, "SOLUSDT")

    triad.a1.plus_15m(candles_2025_btc[-99])
    triad.a2.plus_15m(candles_2025_eth[-99])
    triad.a3.plus_15m(candles_2025_sol[-99])
    # smt_psp = triad.actual_smt_psp()

    triad_copy = copy.deepcopy(triad)
    triad_copy.a1.candles_15m = []
    triad_copy.a2.candles_15m = []
    triad_copy.a3.candles_15m = []
    assert json_from_triad(triad_copy) + '\n' == snapshot_triad_23_30
    assert triad.a2.candles_15m[0][5] == "2024-01-01 00:00"
    assert triad.a2.candles_15m[-1][5] == "2025-08-08 23:15"

    triad.a1.plus_15m(candles_2025_btc[-98])
    triad.a2.plus_15m(candles_2025_eth[-98])
    triad.a3.plus_15m(candles_2025_sol[-98])
    # smt_psp = triad.actual_smt_psp()

    # j_triad_3 = json_from_triad(triad)

    assert json_from_triad(triad) + '\n' == snapshot_triad_23_45

    triad.a1.plus_15m(candles_2025_btc[-97])
    triad.a2.plus_15m(candles_2025_eth[-97])
    triad.a3.plus_15m(candles_2025_sol[-97])
    smt_psp = triad.actual_smt_psp()

    # j_triad_4 = json_from_triad(triad)

    assert json_from_triad(triad) + '\n' == snapshot_triad_00_00

    triad.a1.plus_15m(candles_2025_btc[-96])
    triad.a2.plus_15m(candles_2025_eth[-96])
    triad.a3.plus_15m(candles_2025_sol[-96])
    smt_psp = triad.actual_smt_psp()

    # j_triad_4 = json_from_triad(triad)

    assert json_from_triad(triad) + '\n' == snapshot_triad_00_15


if __name__ == "__main__":
    try:
        test_09_aug_2025()

    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
