"""H7 (PREREG.md): trend-filtered 60/40.

IEF is always 0.40. SPY is 0.60 while its ``TREND_DAYS`` excess return over BIL
is positive; otherwise that 0.60 goes to ``OFF_ASSET`` (BIL via the base's
remainder rule, or IEF explicitly). D04: monthly, gross <= 1.
"""
from __future__ import annotations

import pandas as pd

from strategies.lowfreq_xasset_base import XAssetBase


class Trend6040(XAssetBase):
    EQUITY = 'SPY'
    BOND = 'IEF'
    EQUITY_WEIGHT = 0.6
    TREND_DAYS = 252          # grid {126, 252}
    OFF_ASSET = 'BIL'         # grid {BIL, IEF}

    def risk_weights(self, close: pd.DataFrame, risk: pd.DataFrame) -> pd.DataFrame:
        for t in (self.EQUITY, self.BOND):
            if t not in risk.columns:
                raise ValueError(f'Trend6040 needs {t} in the panel')
        if self.OFF_ASSET not in (self.RISK_OFF, self.BOND):
            raise ValueError(f'OFF_ASSET must be {self.RISK_OFF} or {self.BOND}, got {self.OFF_ASSET!r}')
        excess = self.excess_return(close, risk[[self.EQUITY]], self.TREND_DAYS)[self.EQUITY]
        on = (excess > 0).astype(float)                    # NaN (no history) -> OFF
        eq = self.EQUITY_WEIGHT * on
        bond = pd.Series(1.0 - self.EQUITY_WEIGHT, index=risk.index)
        if self.OFF_ASSET == self.BOND:
            bond = bond + self.EQUITY_WEIGHT * (1.0 - on)
        return pd.DataFrame({self.EQUITY: eq, self.BOND: bond}, index=risk.index)
