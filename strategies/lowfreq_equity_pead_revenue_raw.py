"""Decomposition control D8 (C03, PREREG.md addendum R3): the NUMERATOR of
`pead_revenue` alone - the raw dollar change of as-of TTM revenue at the
latest filing, unscaled. Top quintile. Never a champion.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned, latest_change


class RevenueSurpriseRaw(CoveredTilt):
    def signal(self, close, fields, signals):
        new, prev = latest_change(aligned(fields, close, 'revenue_ttm'))
        return new - prev
