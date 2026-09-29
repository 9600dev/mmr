"""Decomposition control D4 — the denominator of the revenue-surprise ratio
(C03, PREREG.md addendum R2): smallest as-of TTM REVENUE quintile.

`pead_revenue` divides the change in revenue by the prior TTM revenue; if it
does not beat this book, it measured a small-revenue tilt, not a surprise.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned


class SmallRevenueQuintile(CoveredTilt):
    def signal(self, close, fields, signals):
        rev = aligned(fields, close, 'revenue_ttm')
        return -rev.where(rev > 0)
