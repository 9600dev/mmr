"""H7 — cash-flow quality / low accruals (C03, PREREG.md H7; Sloan 1996).

Signal: -(as-of TTM net income - as-of TTM cash from operations) / as-of total
assets. Earnings backed by cash rank high; accrual-heavy earnings rank low.
Scaled by assets, not market cap, so it is not a size proxy. Top quintile.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned


class LowAccrualsQuintile(CoveredTilt):
    def signal(self, close, fields, signals):
        assets = aligned(fields, close, 'total_assets')
        acc = (aligned(fields, close, 'net_income_ttm') - aligned(fields, close, 'cash_from_operations_ttm'))
        return -acc / assets.where(assets > 0)
