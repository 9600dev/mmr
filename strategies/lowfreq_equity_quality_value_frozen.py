"""Decomposition control D5 — the quality/value blend with BOTH numerators
frozen at their first observed value in the window (C03, PREREG.md addendum
R2): rank(first gross profit / assets) + rank(first revenue / market cap).

Same knowledge mask as `quality_value_blend`, the fundamental information
removed. If it matches the blend, the blend is its denominators (assets and
market cap). Never a champion.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned, first_observed, market_cap, pct_rank


class QualityValueFrozen(CoveredTilt):
    def signal(self, close, fields, signals):
        assets = aligned(fields, close, 'total_assets')
        gp = first_observed(aligned(fields, close, 'gross_profit_ttm')) / assets.where(assets > 0)
        s2p = first_observed(aligned(fields, close, 'revenue_ttm')) / market_cap(close, fields)
        both = gp.notna() & s2p.notna()
        return (pct_rank(gp.where(both)) + pct_rank(s2p.where(both))) / 2.0
