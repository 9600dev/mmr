"""H1 (PREREG.md): time-series momentum on an inverse-volatility base.

A risk asset is ON when its ``LOOKBACK_DAYS`` excess return over BIL is
positive (Moskowitz-Ooi-Pedersen 2012). Base weights are 1/sigma normalised
over ALL eligible risk assets (Hurst-Ooi-Pedersen 2013 vol scaling, unlevered);
OFF names' base weight goes to BIL via ``XAssetBase``. D04: monthly, gross <= 1.
"""
from __future__ import annotations

import pandas as pd

from strategies.lowfreq_xasset_base import XAssetBase, inverse_vol_base


class TsMomRiskParity(XAssetBase):
    LOOKBACK_DAYS = 252       # grid {63, 126, 252}
    VOL_DAYS = 63             # fixed (PREREG C0)

    def risk_weights(self, close: pd.DataFrame, risk: pd.DataFrame) -> pd.DataFrame:
        base = inverse_vol_base(risk, self.VOL_DAYS)
        excess = self.excess_return(close, risk, self.LOOKBACK_DAYS)
        eligible = base.notna() & excess.notna()
        base = base.where(eligible)
        base = base.div(base.sum(axis=1, min_count=1), axis=0)     # renormalise over the eligible set
        on = (excess > 0) & eligible
        return base.where(on, 0.0)
