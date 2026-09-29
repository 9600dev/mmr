"""H6 — low volatility within the covered set (C03, PREREG.md H6).

Signal: -(standard deviation of daily close-to-close returns over the past
VOL_WINDOW sessions, at least MIN_OBS of them). Backward-looking rolling
window: row t uses rows <= t. Bottom-volatility quintile held.
"""
from __future__ import annotations

from strategies.lowfreq_equity_base import CoveredTilt, aligned, market_cap


class LowVolatilityQuintile(CoveredTilt):
    VOL_WINDOW = 126
    MIN_OBS = 100

    def signal(self, close, fields, signals):
        rets = close.pct_change(fill_method=None)
        vol = rets.rolling(self.VOL_WINDOW, min_periods=self.MIN_OBS).std()
        # covered = priced with an as-of filing, like every other candidate
        covered = market_cap(close, fields).notna()
        return -vol.where(covered)
