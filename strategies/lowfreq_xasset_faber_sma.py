"""H2 (PREREG.md): Faber (2007) moving-average timing of an equal-weight book.

Each eligible risk asset has base weight 1/N_eligible and is held only while
its total-return close is above its ``SMA_DAYS`` simple moving average; the
OFF share goes to BIL (``XAssetBase``). D04: monthly, gross <= 1.
"""
from __future__ import annotations

import pandas as pd

from strategies.lowfreq_xasset_base import XAssetBase


class FaberSma(XAssetBase):
    SMA_DAYS = 210            # grid {126, 168, 210, 252}; Faber's 10 months

    def risk_weights(self, close: pd.DataFrame, risk: pd.DataFrame) -> pd.DataFrame:
        sma = risk.rolling(self.SMA_DAYS, min_periods=self.SMA_DAYS).mean()
        eligible = sma.notna() & risk.notna()
        n = eligible.sum(axis=1)
        on = (risk > sma) & eligible
        return on.astype(float).div(n.where(n > 0), axis=0)
