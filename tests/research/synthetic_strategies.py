"""Known-answer strategies for the harness tests."""
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from trader.research.lowfreq.reference_strategies import EtfEqualWeight, LowFreqStrategy


class SameAsBenchmark(EtfEqualWeight):
    """Byte-identical behaviour to the benchmark: active return must be ~0."""


class HoldOne(LowFreqStrategy):
    """Holds one ticker at a fixed weight; the rest is cash earning zero."""
    TICKER = 'SPY'
    WEIGHT = 1.0

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        return {'close': panel['close']}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index):
            return None
        w = {c: 0.0 for c in state['close'].columns}
        w[self.TICKER] = self.WEIGHT
        return w


class PeeksAhead(LowFreqStrategy):
    """Reads t+1's close in precompute_panel - must be refused."""

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close']
        return {'close': close, 'tomorrow': close.shift(-1)}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        row = state['tomorrow'].iloc[index].dropna()
        if len(row) == 0:
            return None
        best = (row / state['close'].iloc[index]).idxmax()
        w = {c: 0.0 for c in state['close'].columns}
        w[best] = 1.0
        return w


class PeeksInOnPanel(LowFreqStrategy):
    """Reads panel.iloc[index + 1] directly - the precompute probe cannot see it."""

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        return {'close': panel['close']}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        nxt = state['close'].iloc[index + 1]
        best = (nxt / state['close'].iloc[index]).dropna().idxmax()
        w = {c: 0.0 for c in state['close'].columns}
        w[best] = 1.0
        return w


class Gridded(HoldOne):
    """A 4-cell grid so the PBO / stability branch runs."""
    TICKER = 'SPY'
    WEIGHT = 1.0
    LOOKBACK = 20
