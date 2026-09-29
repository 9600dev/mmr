"""H6 - purchase value relative to market cap (C04 PREREG.md; D11, D14).

Qualify: trailing-63-session officer/director purchase value (1/n_owners
weighted) divided by market cap (close x as-of diluted shares) >=
``VALUE_TO_MCAP_MIN`` = 0.2 basis points - the calibration median of that
ratio among purchasers on the dev panel (1.7e-5, counts only, see
PREREG.md), so about half of C0's qualifiers qualify. Dollar conviction
scaled by the size of the firm; a name without an as-of share count cannot
qualify.
"""
from __future__ import annotations

import pandas as pd

from strategies.lowfreq_insider_base import InsiderOverlay, market_cap
from trader.research.lowfreq import insider_signals as ins


class ValueToMcapOverlay(InsiderOverlay):
    VALUE_TO_MCAP_MIN = 2e-5          # 0.2 basis points of market cap (PREREG.md calibration)

    def qualifies(self, frames, close, fields) -> pd.DataFrame:
        ratio = ins.trailing_sum(frames['p_value'], self.LOOKBACK) / market_cap(close, fields)
        return ratio >= self.VALUE_TO_MCAP_MIN
