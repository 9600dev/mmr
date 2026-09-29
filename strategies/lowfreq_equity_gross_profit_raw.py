"""Decomposition control D11 (C03, PREREG.md addendum R3): the NUMERATOR of
gross profitability alone - raw as-of TTM gross profit in dollars. Top
quintile (the largest gross profits). Never a champion.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned


class GrossProfitRaw(CoveredTilt):
    def signal(self, close, fields, signals):
        return aligned(fields, close, 'gross_profit_ttm')
