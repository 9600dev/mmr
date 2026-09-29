"""H5 — value with a momentum filter (C03, PREREG.md H5).

Universe = covered names with BOTH sales-to-price and 12-1 price momentum
(close 21 sessions ago / close 252 sessions ago - 1; the one price signal that
survived the 2026-07-28 IC scan) computable. The top quintile is drawn from the
top-40% sales-to-price names ordered by momentum: signal = 1 + momentum
percentile rank for a value name, momentum percentile rank (< 1) otherwise, so
every name with both inputs stays COVERED and the benchmark is still the whole
covered set, while the held slice is the momentum-best half of cheap names.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, pct_rank
from strategies.lowfreq_equity_s2p import sales_to_price


class ValueMomentumFilter(CoveredTilt):
    VALUE_TOP_FRACTION = 0.4      # pre-registered: two quintiles of value, halved by momentum
    MOM_SKIP = 21
    MOM_LOOKBACK = 252

    def signal(self, close, fields, signals):
        mom = close.shift(self.MOM_SKIP) / close.shift(self.MOM_LOOKBACK) - 1.0
        s2p = sales_to_price(close, fields)
        both = mom.notna() & s2p.notna()
        value_rank = pct_rank(s2p.where(both))
        mom_rank = pct_rank(mom.where(both))
        is_value = value_rank >= (1.0 - self.VALUE_TOP_FRACTION)
        return mom_rank + is_value.astype(float)
