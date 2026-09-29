"""H4 — profitability blended with value (C03, PREREG.md H4).

Signal: the mean of two cross-sectional percentile ranks, gross profitability
(as-of TTM gross profit / as-of total assets; Novy-Marx 2013) and
sales-to-price. Cheap AND productive, not cheap because failing. Top quintile.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned, pct_rank
from strategies.lowfreq_equity_s2p import sales_to_price


class QualityValueBlend(CoveredTilt):
    def signal(self, close, fields, signals):
        assets = aligned(fields, close, 'total_assets')
        gp = aligned(fields, close, 'gross_profit_ttm') / assets.where(assets > 0)
        s2p = sales_to_price(close, fields)
        both = gp.notna() & s2p.notna()
        return (pct_rank(gp.where(both)) + pct_rank(s2p.where(both))) / 2.0
