"""Decomposition control D3 — the denominator of every asset-scaled ratio
(C03, PREREG.md addendum R2): smallest as-of TOTAL ASSETS quintile.

`pead_earnings`, `quality_value_blend` (its gross-profitability half) and
`low_accruals` all divide by total assets. If a candidate does not beat this
book, its numerator added nothing and what it measured is an asset-size tilt.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned


class SmallAssetsQuintile(CoveredTilt):
    def signal(self, close, fields, signals):
        assets = aligned(fields, close, 'total_assets')
        return -assets.where(assets > 0)
