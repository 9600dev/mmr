"""Decomposition control D7 (C03, PREREG.md addendum R3): the NUMERATOR of
`pead_earnings` alone - the raw dollar change of as-of TTM net income at the
latest filing (Q_t - Q_{t-4}), unscaled. Top quintile. Never a champion.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned, latest_change


class EarningsSurpriseRaw(CoveredTilt):
    def signal(self, close, fields, signals):
        new, prev = latest_change(aligned(fields, close, 'net_income_ttm'))
        return new - prev
