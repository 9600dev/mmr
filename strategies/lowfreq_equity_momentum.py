"""Decomposition control D12 (C03, PREREG.md addendum R3): the momentum
COMPONENT of `value_momentum_filter` alone - 12-1 price momentum
(close 21 sessions ago / close 252 sessions ago - 1), top quintile of the
covered set (member, priced, momentum computable; price-only, no filing
needed). Never a champion: long-only 12-1 momentum already lost to equal
weight of its universe (RESEARCH_LOG 2026-07-30); it is here so the filter
can be judged against momentum alone.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt


class Momentum121Quintile(CoveredTilt):
    MOM_SKIP = 21
    MOM_LOOKBACK = 252

    def signal(self, close, fields, signals):
        return close.shift(self.MOM_SKIP) / close.shift(self.MOM_LOOKBACK) - 1.0
