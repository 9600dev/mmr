"""Decomposition control D6 — low accruals with the numerator frozen at its
first observed value in the window (C03, PREREG.md addendum R2):
-(first observed (net income - cash from operations)) / as-of total assets.

Same knowledge mask as `low_accruals`, the accrual information removed. If it
matches `low_accruals`, that book is an asset-size tilt. Never a champion.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned, first_observed


class LowAccrualsFrozen(CoveredTilt):
    def signal(self, close, fields, signals):
        assets = aligned(fields, close, 'total_assets')
        acc = aligned(fields, close, 'net_income_ttm') - aligned(fields, close, 'cash_from_operations_ttm')
        return -first_observed(acc) / assets.where(assets > 0)
