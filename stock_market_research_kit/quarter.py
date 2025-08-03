from enum import Enum
from typing import Tuple


class YearQuarter(Enum):
    YQ1 = 'First year quarter'
    YQ2 = 'Second year quarter'
    YQ3 = 'Third year quarter'
    YQ4 = 'Fourth year quarter'


class MonthWeek(Enum):
    MW1 = 1
    MW2 = 2
    MW3 = 3
    MW4 = 4
    MW5 = 5


class WeekDay(Enum):
    Mon = 1
    Tue = 2
    Wed = 3
    Thu = 4
    MonThu = 10
    Fri = 5
    MonFri = 15
    Sat = 6
    MonSat = 21
    Sun = 7


class DayQuarter(Enum):
    DQ1_Asia = 'Asia 18:00 - 00:00 NY time'
    DQ2_London = 'London 00:00 - 06:00 NY tim'
    DQ3_NYAM = 'NYAM 06:00 - 12:00 NY tim'
    DQ4_NYPM = 'NYPM 12:00 - 18:00 NY tim'


class Quarter90m(Enum):
    Q1_90m = 'First 90m quarter of 6h'
    Q2_90m = 'Second 90m quarter of 6h'
    Q3_90m = 'Third 90m quarter of 6h'
    Q4_90m = 'Fourth 90m quarter of 6h'
