"""Reference-family strategies: the benchmarks every candidate must beat.

All three are monthly-rebalanced, long-only, gross <= 1.0 (D01), and return
``None`` between rebalances (hold) rather than ``{}`` (flatten). Cash earns
zero (D03); a book that wants a cash yield must hold BIL explicitly.

``LowFreqStrategy`` is the optional base for family strategies: the harness
calls ``set_research_data`` with the panel's extra frames (as-of fundamentals
for the equity universe) before the backtest starts.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from trader.trading.strategy import Strategy


class LowFreqStrategy(Strategy):
    REBALANCE_EVERY = 21          # trading days; ~monthly (D04/D05: no more often than weekly)

    def __init__(self):
        super().__init__()
        self.research_data: Dict[str, Any] = {}

    def set_research_data(self, data: Dict[str, Any]) -> None:
        self.research_data = dict(data)

    def due(self, index: int) -> bool:
        return self.REBALANCE_EVERY <= 1 or index % self.REBALANCE_EVERY == 0


class EtfEqualWeight(LowFreqStrategy):
    """Monthly equal weight of every ETF with a bar today (eligible = listed)."""

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        return {'close': panel['close']}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index):
            return None
        row = state['close'].iloc[index].dropna()
        if len(row) < 2:
            return None
        w = {c: 1.0 / len(row) for c in row.index}
        for c in state['close'].columns:          # names without a bar today: explicitly flat
            w.setdefault(c, 0.0)
        return w


class Etf6040(LowFreqStrategy):
    """60% SPY / 40% IEF, rebalanced monthly."""
    EQUITY = 'SPY'
    BOND = 'IEF'
    EQUITY_WEIGHT = 0.6

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close']
        for t in (self.EQUITY, self.BOND):
            if t not in close.columns:
                raise ValueError(f'Etf6040 needs {t} in the panel')
        return {'close': close}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index):
            return None
        row = state['close'].iloc[index]
        if not (np.isfinite(row.get(self.EQUITY, np.nan)) and np.isfinite(row.get(self.BOND, np.nan))):
            return None
        w = {c: 0.0 for c in state['close'].columns}
        w[self.EQUITY] = self.EQUITY_WEIGHT
        w[self.BOND] = 1.0 - self.EQUITY_WEIGHT
        return w


class EqEwCovered(LowFreqStrategy):
    """Monthly equal weight of the COVERED point-in-time universe (D05).

    Covered = a member with a price today AND an as-of ``shares_diluted`` from
    an accepted filing (the field every fundamental signal needs). A candidate
    may narrow this by publishing a boolean ``coverage`` frame in its
    ``precompute_panel`` state; the harness forwards it via
    ``set_coverage`` so the benchmark owns exactly the names the candidate
    could have chosen from.
    """
    MIN_NAMES = 30

    def __init__(self):
        super().__init__()
        self._coverage: Optional[pd.DataFrame] = None

    def set_coverage(self, frame: pd.DataFrame) -> None:
        self._coverage = frame

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close']
        if self._coverage is not None:
            cov = self._coverage.reindex(index=close.index, columns=close.columns).fillna(False).astype(bool)
        else:
            shares = (self.research_data.get('fields') or {}).get('shares_diluted')
            if shares is None:
                cov = pd.DataFrame(True, index=close.index, columns=close.columns)
            else:
                cov = shares.reindex(index=close.index, columns=close.columns).notna()
        return {'close': close, 'coverage': cov & close.notna()}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index):
            return None
        cov = state['coverage'].iloc[index]
        names = list(cov.index[cov.to_numpy(dtype=bool)])
        if len(names) < self.MIN_NAMES:
            return None
        w = {c: 0.0 for c in state['close'].columns}
        for c in names:
            w[c] = 1.0 / len(names)
        return w
