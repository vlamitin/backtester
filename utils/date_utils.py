from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# returns -1 if checked time is past, 0 if same day and +1 if future
def is_same_day_or_past_or_future(checked_time_string, current_day_string):
    current_day = to_utc_datetime(current_day_string)
    next_day = current_day + timedelta(days=1)
    checked_time = to_utc_datetime(checked_time_string)

    range_from = datetime(current_day.year, current_day.month, current_day.day, 0, 0, tzinfo=ZoneInfo("UTC"))
    range_to = datetime(next_day.year, next_day.month, next_day.day, 0, 0, tzinfo=ZoneInfo("UTC"))

    if range_from > checked_time:
        return -1
    elif checked_time >= range_to:
        return 1
    else:
        return 0


def start_of_day(date: datetime) -> datetime:
    return date.replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(date: datetime) -> datetime:
    return date.replace(hour=23, minute=59, second=59, microsecond=999000)


# it returns start_date, end_date and all days with 0:00 in between
def get_all_days_between(start_date: datetime, end_date: datetime):
    if start_date.date() == end_date.date():
        return [end_date]
    days = [start_date]
    current_date = start_of_day(start_date + timedelta(days=1))

    while current_date < end_date and current_date != start_of_day(end_date):
        days.append(current_date)
        current_date += timedelta(days=1)

    days.append(end_date)

    return days


def to_timestamp(date_str: str) -> int:
    if date_str == "":
        return -1

    return int(to_utc_datetime(date_str).timestamp() * 1000)


def get_previous_candle_15m_close() -> datetime:
    now = datetime.now(ZoneInfo("UTC"))
    rounded = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    return rounded


def now_ts() -> int:
    return int(now_utc_datetime().timestamp() * 1000)


def now_utc_datetime() -> datetime:
    return datetime.now(ZoneInfo("UTC"))


def now_ny_datetime() -> datetime:
    return datetime.now(ZoneInfo("America/New_York"))


def is_some_prev_day_session(checked_time_string, current_day_string, from_ny, to_ny):
    current_day = to_utc_datetime(current_day_string)
    prev_day = current_day - timedelta(days=1)

    if prev_day.isoweekday() in [5, 6]:
        return False

    checked_time = to_utc_datetime(checked_time_string)

    from_h, from_m = [int(x) for x in from_ny.split(":")]
    to_h, to_m = [int(x) for x in to_ny.split(":")]

    range_from = datetime(
        prev_day.year, prev_day.month, prev_day.day, from_h, from_m, tzinfo=ZoneInfo("America/New_York")
    )
    range_to = datetime(
        prev_day.year, prev_day.month, prev_day.day, to_h, to_m, tzinfo=ZoneInfo("America/New_York")
    )

    return range_from <= checked_time < range_to


def is_some_same_day_session(checked_time_string: str, current_day_string: str, from_ny: str, to_ny: str):
    current_day = to_utc_datetime(current_day_string)
    if current_day.isoweekday() in [6, 7]:
        return False

    return is_some_same_day(checked_time_string, current_day_string, from_ny, to_ny)


def is_some_same_day(checked_time_string: str, current_day_string: str, from_ny: str, to_ny: str):
    current_day = to_utc_datetime(current_day_string)
    checked_time = to_utc_datetime(checked_time_string)

    from_h, from_m = [int(x) for x in from_ny.split(":")]
    to_h, to_m = [int(x) for x in to_ny.split(":")]

    range_from = datetime(
        current_day.year, current_day.month, current_day.day, from_h, from_m, tzinfo=ZoneInfo("America/New_York")
    )
    range_to = datetime(
        current_day.year, current_day.month, current_day.day, to_h, to_m, tzinfo=ZoneInfo("America/New_York")
    )

    return range_from <= checked_time < range_to


def to_utc_datetime(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo("UTC"))


def to_date_str(date: datetime) -> str:
    return date.strftime("%Y-%m-%d %H:%M")


def log_warn(message: str):
    print(f"WARN at {to_date_str(now_utc_datetime())} utc: {message}")


def log_warn_ny(message: str):
    print(f"WARN at {to_date_str(now_ny_datetime())} ny: {message}")


def log_info_ny(message: str):
    print(f"INFO at {to_date_str(now_ny_datetime())} ny: {message}")


def cme_open_from_to(day_string) -> (str, str):
    return (
        to_date_str(to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")).replace(
            hour=18, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str(to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")).replace(
            hour=18, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_prev_day_cme_open_time(checked_time_string, current_day_string):
    return is_some_prev_day_session(checked_time_string, current_day_string, "18:00", "19:00")


def asia_from_to(day_string) -> (str, str):
    return (
        to_date_str(to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")).replace(
            hour=19, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str(to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")).replace(
            hour=21, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_asian_time(checked_time_string, current_day_string):
    return is_some_prev_day_session(checked_time_string, current_day_string, "19:00", "22:00")


def london_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=2, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=4, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_london_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "02:00", "05:00")


def early_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=7, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=7, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_early_session_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "07:00", "08:00")


def pre_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=8, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=9, minute=29, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_premarket_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "08:00", "09:30")


def open_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=9, minute=30, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=9, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_ny_am_open_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "09:30", "10:00")


def nyam_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=10, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=11, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_ny_am_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "10:00", "12:00")


def lunch_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=12, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=12, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_ny_lunch_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "12:00", "13:00")


def nypm_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=13, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=14, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_ny_pm_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "13:00", "15:00")


def close_from_to(day_string) -> (str, str):
    return (
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=15, minute=0, second=0, microsecond=0).astimezone(
            ZoneInfo("UTC"))
        ),
        to_date_str((to_utc_datetime(day_string).astimezone(
            ZoneInfo("America/New_York")) + timedelta(days=1)).replace(
            hour=15, minute=59, second=59, microsecond=999000).astimezone(
            ZoneInfo("UTC"))
        )
    )


def is_ny_pm_close_time(checked_time_string, current_day_string):
    return is_some_same_day_session(checked_time_string, current_day_string, "15:00", "16:00")


def get_second_monday(date: datetime) -> datetime:
    first_day = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    days_to_first_monday = (7 - first_day.weekday() + 0) % 7
    first_monday = first_day + timedelta(days=days_to_first_monday)
    return first_monday + timedelta(days=7)


def is_previous_week(date_to_check: datetime, reference_date: datetime) -> bool:
    reference_week_start = reference_date - timedelta(days=reference_date.weekday())
    previous_week_start = reference_week_start - timedelta(weeks=1)
    previous_week_end = reference_week_start - timedelta(days=1)

    return previous_week_start <= date_to_check <= previous_week_end


if __name__ == "__main__":
    try:
        log_warn("test")
        # res = get_all_days_between(
        #     datetime(2024, 3, 1, 15, 0, tzinfo=ZoneInfo("UTC")),
        #     datetime(2024, 3, 5, 10, 0, tzinfo=ZoneInfo("UTC"))
        # )
        res = get_second_monday(datetime.now())
        print('res', res)

        # for smb in [
        #     "BTCUSDT",
        #     "AAVEUSDT",
        #     "CRVUSDT",
        #     "AVAXUSDT",
        # ]:
        #     for series_year in [
        #         2021,
        #         2022,
        #         2023,
        #         2024,
        #         2025
        #     ]:
        #         fill_year_from_csv(series_year, smb)
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
