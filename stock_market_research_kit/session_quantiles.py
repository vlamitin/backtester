import time
from typing import Dict

from stock_market_research_kit.db_layer import select_days
from stock_market_research_kit.session import SessionName
from stock_market_research_kit.session_thresholds import SessionThresholds, threshold_session_year


def quantile_per_session_year_thresholds(symbol: str, year: int) -> Dict[SessionName, SessionThresholds]:
    start_time = time.perf_counter()

    days = select_days(year, symbol)

    result = {
        SessionName.CME: threshold_session_year(symbol, SessionName.CME, year,
                                                [x.cme_as_candle for x in days if x.cme_as_candle[5] != ""]),
        SessionName.ASIA: threshold_session_year(symbol, SessionName.ASIA, year,
                                                 [x.asia_as_candle for x in days if x.asia_as_candle[5] != ""]),
        SessionName.LONDON: threshold_session_year(symbol, SessionName.LONDON, year,
                                                   [x.london_as_candle for x in days if x.london_as_candle[5] != ""]),
        SessionName.EARLY: threshold_session_year(symbol, SessionName.EARLY, year,
                                                  [x.early_session_as_candle for x in days if
                                                   x.early_session_as_candle[5] != ""]),
        SessionName.PRE: threshold_session_year(symbol, SessionName.PRE, year,
                                                [x.premarket_as_candle for x in days if
                                                 x.premarket_as_candle[5] != ""]),
        SessionName.NY_OPEN: threshold_session_year(symbol, SessionName.NY_OPEN, year,
                                                    [x.ny_am_open_as_candle for x in days if
                                                     x.ny_am_open_as_candle[5] != ""]),
        SessionName.NY_AM: threshold_session_year(symbol, SessionName.NY_AM, year,
                                                  [x.ny_am_as_candle for x in days if x.ny_am_as_candle[5] != ""]),
        SessionName.NY_LUNCH: threshold_session_year(symbol, SessionName.NY_LUNCH, year,
                                                     [x.ny_lunch_as_candle for x in days if
                                                      x.ny_lunch_as_candle[5] != ""]),
        SessionName.NY_PM: threshold_session_year(symbol, SessionName.NY_PM, year,
                                                  [x.ny_pm_as_candle for x in days if x.ny_pm_as_candle[5] != ""]),
        SessionName.NY_CLOSE: threshold_session_year(symbol, SessionName.NY_CLOSE, year,
                                                     [x.ny_pm_close_as_candle for x in days if
                                                      x.ny_pm_close_as_candle[5] != ""]),
    }

    print(
        f"Calculation quantiles for {year} {symbol} sessions took {(time.perf_counter() - start_time):.6f} seconds")

    return result


if __name__ == "__main__":
    try:
        res_btc = quantile_per_session_year_thresholds("BTCUSDT", 2024)
        res_aave = quantile_per_session_year_thresholds("AAVEUSDT", 2024)
        res_avax = quantile_per_session_year_thresholds("AVAXUSDT", 2024)
        res_crv = quantile_per_session_year_thresholds("CRVUSDT", 2024)
        print(res_crv, res_btc)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
