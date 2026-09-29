"""H6 — low volatility within the covered set (C03, PREREG.md H6).

Signal: -(standard deviation of daily close-to-close returns over the past
VOL_WINDOW sessions, at least MIN_OBS of them). Backward-looking rolling
window: row t uses rows <= t. Bottom-volatility quintile held.

Coverage is exactly the common definition (member, priced, signal
computable): a price-only signal needs no filing, so no fundamentals field
narrows the candidate's or the benchmark's universe (PREREG addendum R2).
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt


class LowVolatilityQuintile(CoveredTilt):
    VOL_WINDOW = 126
    MIN_OBS = 100

    def signal(self, close, fields, signals):
        rets = close.pct_change(fill_method=None)
        return -rets.rolling(self.VOL_WINDOW, min_periods=self.MIN_OBS).std()
