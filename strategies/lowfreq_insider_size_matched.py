"""D1 - size-matched control for the purchase baseline (C04 PREREG.md; D11).

Same construction as C0 (``AnyPurchaseOverlay``), but on every rebalance each
qualifying name is REPLACED by the covered non-qualifying name nearest to it
in trailing-63-session median dollar-volume rank (the PIT universe's own size
variable), ties by ticker, each match used once. What it earns is the part of
C0's active return that comes from the SIZE of the names insiders buy, not
from the buying. A control, never a champion.
"""
from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd

from strategies.lowfreq_insider_any_purchase import AnyPurchaseOverlay
from strategies.lowfreq_insider_base import adv_rank_matches


class SizeMatchedAnyPurchaseOverlay(AnyPurchaseOverlay):
    ADV_WINDOW = 63

    def extra_state(self, close: pd.DataFrame) -> Dict[str, Any]:
        dv = self._field(close, 'dollar_volume')
        if dv is None:
            dv = pd.DataFrame(np.nan, index=close.index, columns=close.columns)
        return {'adv': dv.astype(float).rolling(self.ADV_WINDOW, min_periods=20).median()}

    def select(self, state: Dict[str, Any], index: int, covered: List[str], signal: List[str]) -> List[str]:
        return adv_rank_matches(signal, covered, state['adv'].iloc[index])
