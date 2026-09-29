"""H4 (PREREG.md): unlevered inverse-volatility over every eligible risk asset
(Asness-Frazzini-Pedersen 2012), fully invested, no filter - the control that
isolates the trend filter in H1. D04: monthly, gross <= 1.
"""
from __future__ import annotations

import pandas as pd

from strategies.lowfreq_xasset_base import XAssetBase, inverse_vol_base


class RiskParity(XAssetBase):
    VOL_DAYS = 63             # grid {21, 63, 252}

    def risk_weights(self, close: pd.DataFrame, risk: pd.DataFrame) -> pd.DataFrame:
        return inverse_vol_base(risk, self.VOL_DAYS)
