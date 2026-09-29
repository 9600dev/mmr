"""Decomposition control D10 (C03, PREREG.md addendum R3): the profitability
COMPONENT of `quality_value_blend` alone - as-of TTM gross profit / as-of
total assets (Novy-Marx 2013). Top quintile. Registered as a control (added
after pass-1 results were seen), never a champion.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned


class GrossProfitabilityQuintile(CoveredTilt):
    def signal(self, close, fields, signals):
        assets = aligned(fields, close, 'total_assets')
        return aligned(fields, close, 'gross_profit_ttm') / assets.where(assets > 0)
