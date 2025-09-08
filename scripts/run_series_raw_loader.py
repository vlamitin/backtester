import csv
import os
from datetime import datetime, timedelta
from typing import List, Tuple, TypeAlias
from zoneinfo import ZoneInfo

from stock_market_research_kit.binance_fetcher import fetch_15m_candles
from stock_market_research_kit.candle import InnerCandle, as_1_candle
from stock_market_research_kit.db_layer import update_stock_data, last_candle_15m, select_multiyear_candles_15m, \
    update_multiyear_stock_data
from utils.date_utils import to_utc_datetime, to_date_str, to_timestamp, \
    get_all_days_between, end_of_day, log_warn_ny


def load_from_binance_csv(symbol, period, year) -> List[InnerCandle]:
    # Folder path
    folder_path = f"./data/csv/{symbol}/{period}"

    # Get all CSV files and sort them alphabetically
    csv_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".csv") if str(year) in f])

    rows = load_asset_csvs([os.path.join(folder_path, x) for x in csv_files])
    return [to_inner_candle(row) for row in rows]


def load_asset_csvs(file_names: List[str]):
    all_data = []

    for file_name in file_names:
        with open(file_name, newline='', encoding='utf-8') as f:
            print(f"Read {file_name}")
            reader = csv.reader(f)
            data = list(reader)  # Read all rows into a list
            all_data.append((file_name, data))  # Store filename and content

    rows = []
    for filename, data in all_data:
        for row in data:
            if not row[0].isdigit():
                continue
            rows.append(row)

    return rows


def load_total3_csv():
    candles2h_total3 = [to_inner_candle(x) for x in load_asset_csvs(["./data/csv/CRYPTOCAP_TOTAL3, 120.csv"])]
    candles1h_total3 = [to_inner_candle(x) for x in load_asset_csvs(["./data/csv/CRYPTOCAP_TOTAL3, 60.csv"])]
    candles15m_total3 = [to_inner_candle(x) for x in load_asset_csvs(["./data/csv/CRYPTOCAP_TOTAL3, 15.csv"])]

    for candles, pp in [(candles2h_total3[:-1], timedelta(hours=2)), (candles1h_total3[:-1], timedelta(hours=1))]:
        from_, to_ = candles[0][5], candles[-1][5]

        last_15m = to_date_str(to_utc_datetime(to_) + pp)
        sol_candles15m = select_multiyear_candles_15m('SOLUSDT', from_, last_15m)
        if sol_candles15m[-1][5] != to_date_str(to_utc_datetime(to_) + pp - timedelta(minutes=15)):
            log_warn_ny('15m candles mismatch!')
            exit(1)

        populated = populate_15m(candles, sol_candles15m)

        update_multiyear_stock_data(populated, 'TOTAL3', '15m')
    update_multiyear_stock_data(candles15m_total3, 'TOTAL3', '15m')


def populate_15m(
        target_candles: List[InnerCandle],
        reference_candles_15m: List[InnerCandle],
) -> List[InnerCandle]:  # 15m target candles
    res: List[InnerCandle] = []

    candles_15m_in_parent = int((to_utc_datetime(target_candles[0][5]) - to_utc_datetime(target_candles[1][5])
                                 ).total_seconds() / (to_utc_datetime(reference_candles_15m[0][5]) - to_utc_datetime(
        reference_candles_15m[1][5])).total_seconds())

    # обходим окна 2h
    for i in range(len(target_candles)):
        target_candle_parent = target_candles[i]

        # берем все 15m свечи внутри окна
        ref_candles_15m = reference_candles_15m[
                          i * candles_15m_in_parent:i * candles_15m_in_parent + candles_15m_in_parent]
        ref_15m = ref_candles_15m[0]
        if ref_15m[5] != target_candle_parent[5] or len(ref_candles_15m) != candles_15m_in_parent:
            log_warn_ny(f"mismatch at {ref_15m[5]}!")
            exit(1)

        ref_rel_candles = to_relative_candles(as_1_candle(ref_candles_15m), ref_candles_15m)
        target_candles_15m = from_relative_candles(target_candle_parent, ref_rel_candles)
        res.extend(target_candles_15m)

    return res


def to_inner_candle(binance_candle) -> InnerCandle:
    date = to_date_str(datetime.fromtimestamp(int(str(binance_candle[0])[:10]), tz=ZoneInfo("UTC")))
    open_price = float(binance_candle[1])
    high = float(binance_candle[2])
    low = float(binance_candle[3])
    close = float(binance_candle[4])
    volume = float(binance_candle[5])

    return open_price, high, low, close, volume, date


def fill_year_from_csv(year, symbol):
    series_15m = load_from_binance_csv(symbol, "15m", year)
    update_stock_data(year, series_15m, symbol, "15m")

    print(f"done loading {year} year for {symbol}")


def update_candle_from_binance(symbol, last_date_utc: datetime):
    now_year = last_date_utc.year  # TODO не будет работать при смене года

    last_15m_candle = last_candle_15m(now_year, symbol)
    if not last_15m_candle:
        print(f"Symbol {symbol} {now_year} year 15m not found in DB")
        return

    iteration_days = get_all_days_between(
        to_utc_datetime(last_15m_candle[5]) + timedelta(minutes=15),
        last_date_utc
    )

    if len(iteration_days) > 32:
        raise ValueError("More than a month passed! Download a CSV!")

    for i, date in enumerate(iteration_days):
        update_day_from_binance(symbol, date, last_date_utc)


def update_day_from_binance(symbol: str, from_date: datetime, to_date: datetime):
    start_time = to_timestamp(from_date)
    end_time = min(to_timestamp(end_of_day(from_date)), to_timestamp(to_date) - 1)

    if start_time >= end_time:
        return

    candles_15m = [to_inner_candle(x) for x in fetch_15m_candles(symbol, start_time, end_time)]
    update_stock_data(from_date.year, candles_15m, symbol, "15m")


CandlePosition: TypeAlias = Tuple[str, float]  # body|front_wick|back_wick, part from 1 (0 is lowest)
# open, front_extremum (high for bull candles), back_extremum, close, volume, date
RelativeCandle: TypeAlias = Tuple[CandlePosition, CandlePosition, CandlePosition, CandlePosition, float, str]


def to_relative_candles(parent_candle: InnerCandle, child_candles: List[InnerCandle]) -> List[RelativeCandle]:
    p_body_start_end = parent_candle[0], parent_candle[3]
    front_wick_start_end = parent_candle[3], parent_candle[1]
    back_wick_start_end = parent_candle[0], parent_candle[2]
    if parent_candle[0] > parent_candle[3]:
        front_wick_start_end = parent_candle[3], parent_candle[2]
        back_wick_start_end = parent_candle[0], parent_candle[1]

    def pos_in_parent(price: float) -> CandlePosition:
        if min(p_body_start_end) <= price <= max(p_body_start_end):
            if p_body_start_end[1] == p_body_start_end[0]:
                return 'body', 0
            return 'body', abs(price - p_body_start_end[0]) / abs(p_body_start_end[1] - p_body_start_end[0])
        if min(front_wick_start_end) <= price <= max(front_wick_start_end):
            if front_wick_start_end[1] == front_wick_start_end[0]:
                return 'front_wick', 0
            return 'front_wick', abs(price - front_wick_start_end[0]) / abs(
                front_wick_start_end[1] - front_wick_start_end[0])
        if min(back_wick_start_end) <= price <= max(back_wick_start_end):
            if back_wick_start_end[1] == back_wick_start_end[0]:
                return 'back_wick', 0
            return 'back_wick', abs(price - back_wick_start_end[0]) / abs(
                back_wick_start_end[1] - back_wick_start_end[0])

    res = []
    for c in child_candles:
        open_, front_ex, back_ex, close_ = pos_in_parent(c[0]), pos_in_parent(c[1]), pos_in_parent(c[2]), pos_in_parent(
            c[3])
        res.append((open_, front_ex, back_ex, close_, 0, c[5]))

    return res


def from_relative_candles(parent_candle: InnerCandle, child_candles: List[RelativeCandle]) -> List[InnerCandle]:
    p_body_start_end = parent_candle[0], parent_candle[3]
    front_wick_start_end = parent_candle[3], parent_candle[1]
    back_wick_start_end = parent_candle[0], parent_candle[2]
    if parent_candle[0] > parent_candle[3]:
        front_wick_start_end = parent_candle[3], parent_candle[2]
        back_wick_start_end = parent_candle[0], parent_candle[1]

    def price(pos_in_parent: CandlePosition) -> float:
        match pos_in_parent[0]:
            case 'body':
                if parent_candle[0] < parent_candle[3]:
                    return p_body_start_end[0] + pos_in_parent[1] * (p_body_start_end[1] - p_body_start_end[0])
                return p_body_start_end[0] - pos_in_parent[1] * (p_body_start_end[0] - p_body_start_end[1])
            case 'front_wick':
                if parent_candle[0] < parent_candle[3]:
                    return front_wick_start_end[0] + pos_in_parent[1] * (
                            front_wick_start_end[1] - front_wick_start_end[0])
                return front_wick_start_end[0] - pos_in_parent[1] * (front_wick_start_end[0] - front_wick_start_end[1])
            case 'back_wick':
                if parent_candle[0] < parent_candle[3]:
                    return back_wick_start_end[0] - pos_in_parent[1] * (back_wick_start_end[0] - back_wick_start_end[1])
                return back_wick_start_end[0] + pos_in_parent[1] * (back_wick_start_end[1] - back_wick_start_end[0])

    res = []
    for c in child_candles:
        open_, high, low, close_ = price(c[0]), price(c[1]), price(c[2]), price(c[3])
        res.append((open_, max(high, low), min(high, low), close_, 0, c[5]))

    return res


if __name__ == "__main__":
    try:
        # update_candle_from_binance("BTCUSDT")
        load_total3_csv()
        # for smb in [
        #     # "1INCHUSDT",
        #     # "AAVEUSDT",
        #     # "AVAXUSDT",
        #     # "BTCUSDT",
        #     # "COMPUSDT",
        #     # "CRVUSDT",
        #     "ETHUSDT",
        #     # "LINKUSDT",
        #     # "LTCUSDT",
        #     "SOLUSDT",
        #     # "SUSHIUSDT",
        #     # "UNIUSDT",
        #     # "XLMUSDT",
        #     # "XMRUSDT",
        # ]:
        #     # update_candle_from_binance(smb, datetime.now(ZoneInfo("UTC")))
        #     update_day_from_binance(
        #         "SOLUSDT",
        #         to_utc_datetime('2022-02-28 00:00'),
        #         to_utc_datetime('2022-02-28 23:59')
        #     )
        #     # for series_year in [
        #     #     # 2021,
        #     #     2022,
        #     #     # 2023,
        #     #     # 2024,
        #     #     # 2025
        #     # ]:
        #     #     fill_year_from_csv(series_year, smb)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
