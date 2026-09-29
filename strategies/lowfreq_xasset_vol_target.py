"""H5 (PREREG.md): volatility-managed equity exposure (Moreira-Muir 2017).

SPY weight = min(1, TARGET_VOL / realised vol over the last ``VOL_DAYS``
sessions); the remainder is BIL (``XAssetBase``). Unlevered by D01, so only
the de-risking half of the paper's rule exists here. D04: monthly.
"""
from __future__ import annotations

import pandas as pd

from strategies.lowfreq_xasset_base import XAssetBase, realised_vol


class VolTarget(XAssetBase):
    EQUITY = 'SPY'
    TARGET_VOL = 0.12         # grid {0.10, 0.12, 0.15}
    VOL_DAYS = 21             # fixed: last month's realised vol

    def risk_weights(self, close: pd.DataFrame, risk: pd.DataFrame) -> pd.DataFrame:
        if self.EQUITY not in risk.columns:
            raise ValueError(f'VolTarget needs {self.EQUITY} in the panel')
        sigma = realised_vol(risk[[self.EQUITY]], self.VOL_DAYS)[self.EQUITY]
        exposure = (self.TARGET_VOL / sigma).where(sigma > 0).clip(upper=1.0)
        return pd.DataFrame({self.EQUITY: exposure}, index=risk.index)
