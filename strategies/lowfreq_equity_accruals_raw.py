"""Decomposition control D9 (C03, PREREG.md addendum R3): the NUMERATOR of
`low_accruals` alone - minus the raw dollar accrual (as-of TTM net income
minus TTM cash from operations), unscaled. Top quintile. Never a champion.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned


class LowAccrualsRaw(CoveredTilt):
    def signal(self, close, fields, signals):
        return -(aligned(fields, close, 'net_income_ttm') - aligned(fields, close, 'cash_from_operations_ttm'))
