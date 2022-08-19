from enum import IntEnum

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
