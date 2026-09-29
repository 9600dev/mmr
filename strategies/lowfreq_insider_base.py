"""Shared machinery for the low-frequency INSIDER family (C04; design record D11, D14).

Every candidate in ``research/lowfreq/insider/candidates.yaml`` is an
``InsiderOverlay``: on the first NYSE session of each month it holds the
COVERED point-in-time universe (member AND priced - D11: coverage =
membership) equal-weighted, and OVERLAYS a fixed ``W_NAME`` of extra weight
on every covered name that QUALIFIES on the decision bar under one insider
rule. The overlay ("sleeve") is capped at ``SLEEVE_MAX`` of the book, so when
more than ``SLEEVE_MAX / W_NAME`` names qualify each gets ``SLEEVE_MAX / K``.
Weights: basket ``(1 - S) / N`` for every covered name plus ``S / K`` for
each qualifying name, ``S = min(SLEEVE_MAX, K * W_NAME)``; gross exactly 1.0,
long-only, never fewer than ``MIN_COVERED`` names. Fewer than
``MIN_SIGNAL_NAMES`` qualifiers => the pure equal-weight basket (the book is
then the benchmark itself). This is the D11 overlay: qualifying insider
events are sparse, so the unfilled weight sits in the benchmark basket.

Insider events reach the strategy through
``trader.research.lowfreq.insider_signals`` (C03's loader, D14 timing), as
dense daily frames the subclasses reduce with backward-looking windows only;
the harness's fresh-prefix lookahead probe is what proves it for every
candidate. Nothing here reads a price or an event after the decision bar.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from trader.research.lowfreq import insider_signals as ins
from trader.research.lowfreq.reference_strategies import LowFreqStrategy


def overlay_weights(covered: Sequence[str], signal: Sequence[str], w_name: float, sleeve_max: float,
                    min_signal: int) -> Dict[str, float]:
    """Pure: the D11 overlay weights (see module docstring). ``signal`` must be
    a subset of ``covered``; names are de-duplicated and ordered by ticker."""
    cov = sorted(set(str(c) for c in covered))
    sig = sorted(set(str(s) for s in signal) & set(cov))
    n, k = len(cov), len(sig)
    if n == 0:
        return {}
    sleeve = min(float(sleeve_max), k * float(w_name)) if k >= int(min_signal) and k > 0 else 0.0
    base = (1.0 - sleeve) / n
    w = {c: base for c in cov}
    if sleeve > 0:
        extra = sleeve / k
        for s in sig:
            w[s] += extra
    return w


class InsiderOverlay(LowFreqStrategy):
    REBALANCE = 'month'             # D11: no more often than weekly; monthly like the benchmark
    LOOKBACK = 63                   # sessions of usable events a purchase stays "recent" (~3 months)
    W_NAME = 0.02                   # overlay weight per qualifying name
    SLEEVE_MAX = 0.60               # cap on the overlay's share of the book
    MIN_SIGNAL_NAMES = 5            # below this many qualifiers the book is the plain EW basket
    MIN_COVERED = 30                # D05/D11: hold (None) rather than rebalance a thin universe
    MIN_VALUE_USD = 25_000.0        # trailing purchase value a name needs to qualify
    BUYER_WINDOW = 63               # sessions over which distinct buyers are counted (cluster rule)

    def qualifies(self, frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Boolean (session x ticker): does the name qualify on that bar, reading
        only rows <= t of the daily frames (rolling windows)."""
        raise NotImplementedError

    def _field(self, close: pd.DataFrame, name: str) -> Optional[pd.DataFrame]:
        raw = (self.research_data.get('fields') or {}).get(name)
        if not isinstance(raw, pd.DataFrame):
            return None
        return raw.reindex(index=close.index, columns=close.columns)

    def coverage(self, close: pd.DataFrame) -> pd.DataFrame:
        member = self._field(close, 'member')
        if member is None:                     # a panel without membership (tests): every priced name
            member = pd.DataFrame(True, index=close.index, columns=close.columns)
        return member.fillna(False).astype(bool) & close.notna()

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close'].astype(float)
        cov = self.coverage(close)
        frames = ins.signal_frames(pd.DatetimeIndex(close.index), [str(c) for c in close.columns], self.BUYER_WINDOW)
        q = self.qualifies(frames).reindex(index=close.index, columns=close.columns).fillna(False).astype(bool)
        state: Dict[str, Any] = {'close': close, 'coverage': cov, 'sig': (q & cov).astype(float)}
        state.update(self.extra_state(close))
        return state

    def extra_state(self, close: pd.DataFrame) -> Dict[str, Any]:
        return {}

    def select(self, state: Dict[str, Any], index: int, covered: List[str], signal: List[str]) -> List[str]:
        """The names that receive the overlay (a control may substitute others)."""
        return signal

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index, panel):
            return None
        cov_row = state['coverage'].iloc[index]
        covered = [str(c) for c in cov_row.index[cov_row.to_numpy(dtype=bool)]]
        if len(covered) < self.MIN_COVERED:
            return None
        sig_row = state['sig'].iloc[index]
        signal = [str(c) for c in sig_row.index[sig_row.to_numpy(dtype=float) > 0]]
        chosen = self.select(state, index, covered, signal)
        w = {str(c): 0.0 for c in state['close'].columns}          # explicit flat everywhere else
        w.update(overlay_weights(covered, chosen, self.W_NAME, self.SLEEVE_MAX, self.MIN_SIGNAL_NAMES))
        return w


class ValueRuleOverlay(InsiderOverlay):
    """Qualify when the trailing ``LOOKBACK``-session sum of one dollar measure
    reaches ``MIN_VALUE_USD``."""
    MEASURE = 'p_value'

    def qualifies(self, frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return ins.trailing_sum(frames[self.MEASURE], self.LOOKBACK) >= self.MIN_VALUE_USD


def adv_rank_matches(signal: Sequence[str], covered: Sequence[str], adv: pd.Series) -> List[str]:
    """Pure: for each signal name (ticker order) the covered NON-signal name
    with the nearest trailing-ADV rank not already used (ties: ticker order);
    names without an ADV are skipped. The size-matched control's book."""
    cov = sorted(set(str(c) for c in covered))
    sig = sorted(set(str(s) for s in signal) & set(cov))
    ranks = adv.reindex(cov).astype(float)
    ranks = ranks[ranks.notna()].rank(method='first')
    pool = {t: float(r) for t, r in ranks.items() if t not in sig}
    out: List[str] = []
    for s in sig:
        if s not in ranks.index or not pool:
            continue
        target = float(ranks[s])
        best = min(pool.items(), key=lambda kv: (abs(kv[1] - target), kv[0]))
        out.append(best[0])
        pool.pop(best[0])
    return out
