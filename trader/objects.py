from enum import Enum, IntEnum


class Action(Enum):
    BUY = 1
    SELL = 2
    NEUTRAL = 3

    def __str__(self):
        if self.value == 1: return 'BUY'
        if self.value == 2: return 'SELL'
        if self.value == 3: return 'NEUTRAL'


class WhatToShow(IntEnum):
    def __str__(self):
        if self.value == 1: return 'TRADES'
        if self.value == 2: return 'MIDPOINT'
        if self.value == 3: return 'BID'
        if self.value == 4: return 'ASK'

    TRADES = 1
    MIDPOINT = 2
    BID = 3
    ASK = 4

class ReportType(IntEnum):
    def __str__(self):
        return self.name

    ReportsFinSummary = 1       # Financial summary
    ReportsOwnership = 2        # Company’s ownership
    ReportSnapshot = 3          # Company’s financial overview
    ReportsFinStatements = 4    # Financial Statements
    RESC = 5                    # Analyst Estimates
    CalendarReport = 6          # Company’s calendar

class BarSize(IntEnum):
    Secs1 = 0
    Secs5 = 1
    Secs10 = 2
    Secs15 = 3
    Secs30 = 4
    Mins1 = 5
    Mins2 = 6
    Mins3 = 7
    Mins5 = 8
    Mins10 = 9
    Mins15 = 10
    Mins20 = 11
    Mins30 = 12
    Hours1 = 13
    Hours2 = 14
    Hours3 = 15
    Hours4 = 16
    Hours8 = 17
    Days1 = 18
    Weeks1 = 19
    Months1 = 20

    @staticmethod
    def bar_sizes():
        return [
            '1 secs', '5 secs', '10 secs', '15 secs', '30 secs', '1 min', '2 mins', '3 mins', '5 mins',
            '10 mins', '15 mins', '20 mins', '30 mins', '1 hour', '2 hours', '3 hours', '4 hours', '8 hours',
            '1 day', '1 week', '1 month'
        ]

    @staticmethod
    def parse_str(bar_size_str: str):
        return BarSize(BarSize.bar_sizes().index(bar_size_str))

    def __str__(self):
        return BarSize.bar_sizes()[int(self.value)]
