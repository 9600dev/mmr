"""H3 — post-announcement drift on the top line (C03, PREREG.md H3).

Seasonal-random-walk revenue surprise from the as-of TTM revenue frame:
(TTM_t - TTM_prev) / |TTM_prev| = (Q_t - Q_{t-4}) / trailing annual revenue,
known from the filing's acceptance instant, carried until the next filing;
the top quintile is held.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned, latest_change


class RevenueSurpriseQuintile(CoveredTilt):
    def signal(self, close, fields, signals):
        new, prev = latest_change(aligned(fields, close, 'revenue_ttm'))
        return (new - prev) / prev.abs().where(prev.abs() > 0)
