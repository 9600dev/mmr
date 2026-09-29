"""H2 - cluster buying by several insiders (C04 PREREG.md; D11, D14).

Qualify: at least ``MIN_BUYERS`` (2) DISTINCT officer/director buyers (a joint
filing counts once) whose purchases became usable within the trailing 63
sessions.
"""
from __future__ import annotations

import pandas as pd

from strategies.lowfreq_insider_base import InsiderOverlay


class ClusterPurchaseOverlay(InsiderOverlay):
    MIN_BUYERS = 2

    def qualifies(self, frames, close, fields) -> pd.DataFrame:
        return frames[f'buyers_{self.BUYER_WINDOW}'] >= self.MIN_BUYERS
