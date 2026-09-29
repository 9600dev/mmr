"""H2 — post-earnings-announcement drift on the bottom line (C03, PREREG.md H2).

Surprise vs a SEASONAL RANDOM WALK, measured from the acceptance instant: the
as-of TTM net income changes only when a new filing is known, and
TTM_t - TTM_prev = Q_t - Q_{t-4}, the quarter's earnings minus the same
quarter a year earlier. Scaled by as-of total assets (net income can be
negative, so the level is not a usable scale). The name carries its latest
scaled surprise until its next filing; the top quintile is held.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned, latest_change


class EarningsSurpriseQuintile(CoveredTilt):
    def signal(self, close, fields, signals):
        new, prev = latest_change(aligned(fields, close, 'net_income_ttm'))
        assets = aligned(fields, close, 'total_assets').ffill()
        return (new - prev) / assets.where(assets > 0)
