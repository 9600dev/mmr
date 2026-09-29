"""Reference-family strategies: the benchmarks every candidate must beat.

All three rebalance on the first trading day of each calendar month
(``schedule_mask``), are long-only, gross <= 1.0 (D01), and return ``None``
between rebalances (hold) rather than ``{}`` (flatten). Cash earns
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


def schedule_mask(index, every: str = 'month') -> np.ndarray:
    """True on the first trading day of each calendar ``every`` ('month' or
    'week') present in ``index`` - the rebalance days.

    Calendar-based, so the schedule does not depend on where a window was
    sliced: bar 0 counts only if it IS a period's first business day (its
    previous business day falls in another period). ``index % 21`` looked
    monthly but rebalanced twice in some months, skipped others, and changed
    phase with every warm-up origin (D04/D05: "monthly").
    """
    idx = pd.DatetimeIndex(index)
    if idx.tz is not None:
        idx = idx.tz_convert(None)
    n = len(idx)
    if n == 0:
        return np.zeros(0, dtype=bool)
    if every == 'month':
        def period(t) -> int:
            return int(t.year) * 12 + int(t.month)
    elif every == 'week':
        def period(t) -> int:
            y, w, _ = t.isocalendar()
            return int(y) * 100 + int(w)
    else:
        raise ValueError(f"REBALANCE must be 'month' or 'week', got {every!r}")
    keys = np.asarray([period(t) for t in idx], dtype=int)
    mask = np.empty(n, dtype=bool)
    mask[0] = period(idx[0] - pd.offsets.BDay(1)) != keys[0]
    mask[1:] = keys[1:] != keys[:-1]
    return mask


class LowFreqStrategy(Strategy):
    REBALANCE = 'month'           # calendar month | 'week' (D04/D05: no more often than weekly)

    def __init__(self):
        super().__init__()
        self.research_data: Dict[str, Any] = {}
        self._due_key: Any = None
        self._due_mask: Optional[np.ndarray] = None

    def set_research_data(self, data: Dict[str, Any]) -> None:
        self.research_data = dict(data)

    def due(self, index: int, panel: pd.DataFrame) -> bool:
        """Is ``index`` a rebalance day? Needs the panel the harness passed to
        ``on_panel`` so the calendar is read from the bars actually in play."""
        idx = panel.index
        key = (len(idx), idx[0], idx[-1], self.REBALANCE)
        if self._due_mask is None or self._due_key != key:
            self._due_mask = schedule_mask(idx, self.REBALANCE)
            self._due_key = key
        return bool(self._due_mask[index])


class EtfEqualWeight(LowFreqStrategy):
    """Monthly equal weight of every ETF with a bar today (eligible = listed)."""

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        return {'close': panel['close']}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index, panel):
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
        if not self.due(index, panel):
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
        fields = self.research_data.get('fields') or {}
        if self._coverage is not None:
            cov = self._coverage.reindex(index=close.index, columns=close.columns).fillna(False).astype(bool)
        else:
            shares = fields.get('shares_diluted')
            if shares is None:
                cov = pd.DataFrame(True, index=close.index, columns=close.columns)
            else:
                cov = shares.reindex(index=close.index, columns=close.columns).notna()
        member = fields.get('member')
        if member is not None:                 # D05: never own a name outside the PIT universe
            cov = cov & member.reindex(index=close.index, columns=close.columns).fillna(False).astype(bool)
        return {'close': close, 'coverage': cov & close.notna()}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index, panel):
            return None
        cov = state['coverage'].iloc[index]
        names = list(cov.index[cov.to_numpy(dtype=bool)])
        if len(names) < self.MIN_NAMES:
            return None
        w = {c: 0.0 for c in state['close'].columns}
        for c in names:
            w[c] = 1.0 / len(names)
        return w
