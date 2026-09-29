"""Decomposition control D13 (C03, PREREG.md addendum R3): the NUMERATOR of
`s2p_top_quintile` alone - raw as-of TTM revenue in dollars, top quintile
(the largest-sales names). Never a champion.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned


class RevenueRaw(CoveredTilt):
    def signal(self, close, fields, signals):
        rev = aligned(fields, close, 'revenue_ttm')
        return rev.where(rev > 0)
