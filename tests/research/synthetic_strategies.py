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
        if not self.due(index, panel):
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


class CoversTwo(HoldOne):
    """Publishes a coverage frame naming SPY and IEF only; holds SPY."""

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close']
        cov = pd.DataFrame(False, index=close.index, columns=close.columns)
        cov[['SPY', 'IEF']] = True
        return {'close': close, 'coverage': cov}


class EqualWeightTwo(LowFreqStrategy):
    """Plain monthly equal weight of SPY and IEF - the benchmark's answer,
    computed independently - publishing the same two-name coverage."""

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close']
        cov = pd.DataFrame(False, index=close.index, columns=close.columns)
        cov[['SPY', 'IEF']] = True
        return {'close': close, 'coverage': cov}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index, panel):
            return None
        w = {c: 0.0 for c in state['close'].columns}
        w['SPY'] = 0.5
        w['IEF'] = 0.5
        return w


class ChurnsOneName(LowFreqStrategy):
    """Alternates between fully holding TICKER and cash every rebalance, ignoring
    membership - so it BUYS a name after it has left the point-in-time universe."""
    TICKER = 'CCC'

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        return {'close': panel['close']}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index, panel):
            return None
        state['n'] = state.get('n', 0) + 1
        w = {c: 0.0 for c in state['close'].columns}
        w[self.TICKER] = 1.0 if state['n'] % 2 == 1 else 0.0
        return w


class PeeksWhenFlagged(HoldOne):
    """Clean at the default cell (PEEK=0); reads t+1 in the OTHER grid cell.
    The lookahead probes must cover every executable cell, not just the default."""
    PEEK = 0

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close']
        return {'close': close, 'tomorrow': close.shift(-1) if self.PEEK else close}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.PEEK:
            return super().on_panel(panel, state, index)
        row = state['tomorrow'].iloc[index].dropna()
        if len(row) == 0:
            return None
        best = (row / state['close'].iloc[index]).idxmax()
        w = {c: 0.0 for c in state['close'].columns}
        w[best] = 1.0
        return w


class FailsInLateWindows(HoldOne):
    """Raises inside any window whose panel starts on/after FAIL_FROM - i.e. in
    a late walk-forward fold, but not in the full-panel lookahead probes."""
    FAIL_FROM = '2016-10-01'      # fold 1's test window (data from 2016-10-20) fails; train windows and full-panel probes do not

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if str(pd.Timestamp(panel.index[0]).date()) >= self.FAIL_FROM:
            raise RuntimeError('feed died in this window')
        return super().on_panel(panel, state, index)


class NeverTrades(LowFreqStrategy):
    """Returns None forever: no cell of its grid can score on a train window."""
    X = 1

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        return {}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        return None


class CoversByParam(LowFreqStrategy):
    """NAMES=2 covers SPY+IEF and holds SPY; NAMES=3 covers SPY+IEF+GLD and holds
    GLD - so the covered benchmark's universe must follow the CHOSEN cell."""
    NAMES = 2

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close']
        names = ['SPY', 'IEF', 'GLD'][:self.NAMES]
        cov = pd.DataFrame(False, index=close.index, columns=close.columns)
        cov[names] = True
        return {'close': close, 'coverage': cov, 'hold': names[-1]}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index, panel):
            return None
        w = {c: 0.0 for c in state['close'].columns}
        w[state['hold']] = 1.0
        return w
