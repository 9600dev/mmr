"""H5 - net purchase ratio, Lakonishok & Lee (2001) (C04 PREREG.md; D11, D14).

Qualify: over the trailing 126 sessions of usable officer/director events,
NPR = (P$ - S$) / (P$ + S$) >= 0.5 with P$ >= $25,000, dollar values
1/n_owners-weighted. Sales are dominated by scheduled and option-related
selling, so a name only qualifies when purchases clearly outweigh them.
"""
from __future__ import annotations

from typing import Dict

import pandas as pd

from strategies.lowfreq_insider_base import InsiderOverlay
from trader.research.lowfreq import insider_signals as ins


class NetBuyRatioOverlay(InsiderOverlay):
    LOOKBACK = 126
    NPR_MIN = 0.5

    def qualifies(self, frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        p = ins.trailing_sum(frames['p_value'], self.LOOKBACK)
        s = ins.trailing_sum(frames['s_value'], self.LOOKBACK)
        total = p + s
        npr = (p - s) / total.where(total > 0)
        return (npr >= self.NPR_MIN) & (p >= self.MIN_VALUE_USD)
