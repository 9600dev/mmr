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

Rules are COUNT-based unless the hypothesis is about dollars: the baseline
``CountRuleOverlay`` qualifies a name on >= ``MIN_COUNT`` purchase FILINGS
in the trailing ``LOOKBACK`` sessions, and every refined rule is the same
count applied after one filter - so "refined minus baseline" is exactly what
the filter contributed (PREREG.md, decomposition).

``SizeMatchedOverlay`` is the size control: it mirrors a rule's qualifiers
but overlays, for each of them, the covered NON-qualifier nearest in
MARKET CAP (close x as-of diluted shares) instead - what the size of the
bought names earns without the buying.

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


def market_cap(close: pd.DataFrame, fields: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Close x as-of diluted shares (the loader's EDGAR-acceptance as-of frame);
    NaN where unknown or non-positive. A per-row product: prefix-stable."""
    shares = fields.get('shares_diluted')
    if shares is None:
        return pd.DataFrame(np.nan, index=close.index, columns=close.columns)
    mcap = close * shares.reindex(index=close.index, columns=close.columns).astype(float)
    return mcap.where(mcap > 0)


def rank_matches(signal: Sequence[str], covered: Sequence[str], size: pd.Series) -> List[str]:
    """Pure: for each signal name (ticker order) the covered NON-signal name
    with the nearest ``size`` rank not already used (ties: ticker order);
    names without a size are skipped. The size-matched control's book."""
    cov = sorted(set(str(c) for c in covered))
    sig = sorted(set(str(s) for s in signal) & set(cov))
    ranks = size.reindex(cov).astype(float)
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


class InsiderOverlay(LowFreqStrategy):
    REBALANCE = 'month'             # D11: no more often than weekly; monthly like the benchmark
    LOOKBACK = 63                   # sessions of usable events a purchase stays "recent" (~3 months)
    W_NAME = 0.02                   # overlay weight per qualifying name
    SLEEVE_MAX = 0.60               # cap on the overlay's share of the book
    MIN_SIGNAL_NAMES = 5            # below this many qualifiers the book is the plain EW basket
    MIN_COVERED = 30                # D05/D11: hold (None) rather than rebalance a thin universe
    BUYER_WINDOW = 63               # sessions over which distinct buyers are counted (cluster rule)

    def qualifies(self, frames: Dict[str, pd.DataFrame], close: pd.DataFrame,
                  fields: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Boolean (session x ticker): does the name qualify on that bar, reading
        only rows <= t of the daily frames (rolling windows) and of the as-of fields."""
        raise NotImplementedError

    def _fields(self, close: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        raw = self.research_data.get('fields') or {}
        return {k: v.reindex(index=close.index, columns=close.columns) for k, v in raw.items()
                if isinstance(v, pd.DataFrame)}

    def coverage(self, close: pd.DataFrame, fields: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        member = fields.get('member')
        if member is None:                     # a panel without membership (tests): every priced name
            member = pd.DataFrame(True, index=close.index, columns=close.columns)
        return member.fillna(False).astype(bool) & close.notna()

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close'].astype(float)
        fields = self._fields(close)
        cov = self.coverage(close, fields)
        frames = ins.signal_frames(pd.DatetimeIndex(close.index), [str(c) for c in close.columns], self.BUYER_WINDOW)
        q = self.qualifies(frames, close, fields).reindex(index=close.index, columns=close.columns)
        q = q.fillna(False).astype(bool)
        state: Dict[str, Any] = {'close': close, 'coverage': cov, 'sig': (q & cov).astype(float)}
        state.update(self.extra_state(close, fields))
        return state

    def extra_state(self, close: pd.DataFrame, fields: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        return {}

    def select(self, state: Dict[str, Any], index: int, covered: List[str], signal: List[str]) -> List[str]:
        """The names that receive the overlay (a control substitutes others)."""
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


class CountRuleOverlay(InsiderOverlay):
    """Qualify on >= ``MIN_COUNT`` purchase filings (after ``MEASURE``'s filter)
    that became usable in the trailing ``LOOKBACK`` sessions."""
    MEASURE = 'p_filings'
    MIN_COUNT = 1

    def qualifies(self, frames, close, fields) -> pd.DataFrame:
        return ins.trailing_sum(frames[self.MEASURE], self.LOOKBACK) >= self.MIN_COUNT


class SizeMatchedOverlay(InsiderOverlay):
    """The size control of ``RULE``: same qualifiers, but the overlay goes to
    each qualifier's nearest MARKET-CAP neighbour among covered non-qualifiers."""
    RULE: type = CountRuleOverlay

    def qualifies(self, frames, close, fields) -> pd.DataFrame:
        rule = self.RULE()
        rule.research_data = self.research_data
        return rule.qualifies(frames, close, fields)

    def extra_state(self, close, fields) -> Dict[str, Any]:
        return {'mcap': market_cap(close, fields)}

    def select(self, state, index, covered, signal) -> List[str]:
        return rank_matches(signal, covered, state['mcap'].iloc[index])
