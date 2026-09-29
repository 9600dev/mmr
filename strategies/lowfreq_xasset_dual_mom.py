"""H3 (PREREG.md): dual momentum - relative ranking plus an absolute filter.

Rank eligible risk assets by their return from t-LOOKBACK_DAYS to t-SKIP_DAYS
(the 12-1 convention); hold the top ``TOP_N`` at 1/TOP_N each, except that a
top-N name whose excess return over BIL on that span is <= 0 fails the
absolute filter (Antonacci 2012) and its slot goes to BIL. D04: monthly.
"""
from __future__ import annotations

import pandas as pd

from strategies.lowfreq_xasset_base import XAssetBase, trailing_return


class DualMomentum(XAssetBase):
    TOP_N = 5                 # grid {5, 10}
    LOOKBACK_DAYS = 252       # grid {126, 252}
    SKIP_DAYS = 21            # fixed: skip the latest month

    def risk_weights(self, close: pd.DataFrame, risk: pd.DataFrame) -> pd.DataFrame:
        span = self.LOOKBACK_DAYS - self.SKIP_DAYS
        lagged = risk.shift(self.SKIP_DAYS)
        ret = trailing_return(lagged, span)
        excess = ret
        if self.RISK_OFF in close.columns:
            bil = trailing_return(close[[self.RISK_OFF]].shift(self.SKIP_DAYS), span)[self.RISK_OFF]
            excess = ret.sub(bil, axis=0)
        eligible = ret.notna() & risk.notna()
        rank = ret.where(eligible).rank(axis=1, ascending=False, method='first')
        on = (rank <= self.TOP_N) & (excess > 0)
        return on.astype(float) / float(self.TOP_N)
