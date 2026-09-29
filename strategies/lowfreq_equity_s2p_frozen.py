"""H1-decomposition (numerator control): sales-to-price with each name's
revenue FROZEN at its first observed as-of value in the window (C03,
PREREG.md D2; the 2026-07-29 standing rule's instrument).

Same knowledge mask as H1, the revenue INFORMATION removed: if this control
matches or beats H1, the numerator was subtracting and what H1 measured is
1/market-cap. Frozen per window (the harness slices the fields to each
window), which is prefix-safe for the lookahead probe.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned, first_observed, market_cap


class SalesToPriceFrozenRevenue(CoveredTilt):
    def signal(self, close, fields, signals):
        rev = first_observed(aligned(fields, close, 'revenue_ttm'))
        return rev / market_cap(close, fields)
