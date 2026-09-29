"""H2 - cluster buying by several insiders (C04 PREREG.md; D11, D14).

Qualify: at least ``MIN_BUYERS`` (2) DISTINCT officer/director buyers (a joint
filing counts once) whose purchases became usable within the trailing 63
sessions, and trailing-63-session purchase value of at least $25,000.
"""
from __future__ import annotations

from typing import Dict

import pandas as pd

from strategies.lowfreq_insider_base import InsiderOverlay
from trader.research.lowfreq import insider_signals as ins


class ClusterPurchaseOverlay(InsiderOverlay):
    MIN_BUYERS = 2

    def qualifies(self, frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        buyers = frames[f'buyers_{self.BUYER_WINDOW}']
        value = ins.trailing_sum(frames['p_value'], self.LOOKBACK)
        return (buyers >= self.MIN_BUYERS) & (value >= self.MIN_VALUE_USD)
