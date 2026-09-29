"""H6 (PREREG.md): carry via the trailing 12-month distribution yield.

yield_i(t) = (TR_t / TR_{t-YIELD_DAYS}) / (P_t / P_{t-YIELD_DAYS}) - 1, where TR
is the total-return close (the panel) and P the PRICE close, recovered exactly
as dollar_volume / volume from the harness's per-name fields (Koijen-
Moskowitz-Pedersen-Vrugt 2018: carry = the return if prices do not change).
Hold the top ``TOP_N`` yielders at 1/TOP_N, no filter. D04: monthly.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.lowfreq_xasset_base import XAssetBase


class Carry(XAssetBase):
    TOP_N = 10                # grid {5, 10, 15}
    YIELD_DAYS = 252          # fixed: trailing twelve months

    def price_close(self, risk: pd.DataFrame) -> pd.DataFrame:
        fields = (self.research_data or {}).get('fields') or {}
        dv, vol = fields.get('dollar_volume'), fields.get('volume')
        if dv is None or vol is None:
            raise ValueError('Carry needs dollar_volume and volume fields (set_research_data) to recover price closes')
        dv = dv.reindex(index=risk.index, columns=risk.columns).astype(float)
        vol = vol.reindex(index=risk.index, columns=risk.columns).astype(float)
        price = (dv / vol.where(vol > 0)).replace([np.inf, -np.inf], np.nan)
        return price.where(price > 0)

    def risk_weights(self, close: pd.DataFrame, risk: pd.DataFrame) -> pd.DataFrame:
        price = self.price_close(risk)
        tr_growth = risk / risk.shift(self.YIELD_DAYS)
        px_growth = price / price.shift(self.YIELD_DAYS)
        yld = tr_growth / px_growth - 1.0
        eligible = yld.notna() & risk.notna()
        rank = yld.where(eligible).rank(axis=1, ascending=False, method='first')
        on = rank <= self.TOP_N
        return on.astype(float) / float(self.TOP_N)
