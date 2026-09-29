"""H1-decomposition (denominator): the smallest-market-cap quintile of the
covered PIT universe (C03, PREREG.md D1; decision D05).

Signal: -(close x as-of diluted shares). If the sales-to-price book does not
beat this one, its numerator added nothing and the tilt is the size effect.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, market_cap


class SmallCapQuintile(CoveredTilt):
    def signal(self, close, fields, signals):
        return -market_cap(close, fields)
