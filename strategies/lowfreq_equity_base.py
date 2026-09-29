"""Shared machinery for the low-frequency EQUITY family (C03, design decision D05).

Every candidate in ``research/lowfreq/equity/candidates.yaml`` is a
``CoveredTilt``: on the first NYSE session of each month it ranks the COVERED
point-in-time universe on one signal (higher = more attractive), holds the top
``QUANTILE`` of it equal-weighted, and is flat in everything else. Long-only,
gross exactly 1.0 when invested (D01: no leverage), never fewer than
``MIN_NAMES`` names (D05), rebalance monthly (D05: no more often than weekly).

Coverage (D05 decisive benchmark). ``precompute_panel`` publishes a boolean
``coverage`` frame = point-in-time member AND a stored close AND a computable
signal that day. The harness hands it to ``EqEwCovered`` so the benchmark owns
exactly the names the candidate could have chosen from; a BUY outside it is a
harness error. Weight > 0 only where coverage is True on the decision bar.

As-of fundamentals. The frames in ``research_data['fields']`` are the
loader's EDGAR-acceptance-instant as-of frames (``scripts/fundamental_signals
.build``: a filing is known from the first session close strictly AFTER its
acceptance). A subclass reads ``fields[...].iloc[index]`` only through the
per-row cross-sectional ops below, so nothing here can use a filing before the
loader says it was known, and the harness's fresh-prefix lookahead probe
(``engine.lookahead_check``) is what proves it for every candidate.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from trader.research.lowfreq.reference_strategies import LowFreqStrategy

FIELD_NAMES = ('shares_diluted', 'revenue_ttm', 'net_income_ttm', 'gross_profit_ttm',
               'total_assets', 'cash_from_operations_ttm', 'stockholders_equity')


def aligned(frames: Dict[str, pd.DataFrame], close: pd.DataFrame, name: str) -> pd.DataFrame:
    """``frames[name]`` on the panel's (time x ticker) shape; all-NaN when the
    loader carried no such frame (the signal is then not computable anywhere)."""
    fr = frames.get(name)
    if fr is None:
        return pd.DataFrame(np.nan, index=close.index, columns=close.columns)
    return fr.reindex(index=close.index, columns=close.columns).astype(float)


def market_cap(close: pd.DataFrame, fields: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Price x as-of diluted shares (the denominator of every price ratio)."""
    mcap = close * aligned(fields, close, 'shares_diluted')
    return mcap.where(mcap > 0)


def clean(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.replace([np.inf, -np.inf], np.nan)


def pct_rank(frame: pd.DataFrame) -> pd.DataFrame:
    """Cross-sectional percentile rank per ROW (0..1, higher = larger). A
    per-row op: the value at date t depends on date t's row only, so it is
    identical on any prefix of the panel (the lookahead probe's requirement)."""
    return frame.rank(axis=1, pct=True)


def latest_change(frame: pd.DataFrame) -> tuple:
    """For an as-of step frame (a value that changes only when a new filing is
    known) return ``(new, prev)``: at every date the value as of that date and
    the value it replaced at the most recent change, both forward-filled, NaN
    until a name has shown one change inside the panel. Backward-looking only:
    row t uses rows <= t."""
    cur = frame.ffill()
    shifted = cur.shift(1)
    changed = cur.notna() & shifted.notna() & (cur != shifted)
    prev = shifted.where(changed).ffill()
    new = cur.where(changed).ffill()
    # the pair is only meaningful from the first observed change onwards
    return new.where(prev.notna()), prev


def first_observed(frame: pd.DataFrame) -> pd.DataFrame:
    """Each column frozen at its FIRST observed value in the window, on the
    original knowledge mask (``scripts/fundamental_signals._freeze_per_name``).
    Prefix-safe: a column's first observation is the same in any prefix that
    contains it, and NaN before it in every prefix."""
    first = frame.apply(lambda col: col.dropna().iloc[0] if col.notna().any() else np.nan)
    return frame * 0 + first


class CoveredTilt(LowFreqStrategy):
    """Top-``QUANTILE`` slice of the covered PIT universe on ``signal``, monthly."""
    REBALANCE = 'month'          # D05: no more often than weekly
    QUANTILE = 0.2               # fraction of the covered set held (pre-registered, not fitted)
    MIN_NAMES = 30               # D05: never hold fewer names when invested
    MIN_COVERED = 30             # below this many covered names: hold (None), do not rebalance

    def signal(self, close: pd.DataFrame, fields: Dict[str, pd.DataFrame],
               signals: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Higher = more attractive; NaN = not computable (so not covered)."""
        raise NotImplementedError

    def _frames(self, close: pd.DataFrame, key: str) -> Dict[str, pd.DataFrame]:
        raw = self.research_data.get(key) or {}
        return {k: v.reindex(index=close.index, columns=close.columns) for k, v in raw.items()
                if isinstance(v, pd.DataFrame)}

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close'].astype(float)
        fields = self._frames(close, 'fields')
        signals = self._frames(close, 'signals')
        sig = clean(self.signal(close, fields, signals).reindex(index=close.index, columns=close.columns))
        member = fields.get('member')
        if member is None:                     # a panel without membership (tests): every priced name is a member
            member = pd.DataFrame(True, index=close.index, columns=close.columns)
        cov = member.fillna(False).astype(bool) & close.notna() & sig.notna()
        return {'close': close, 'sig': sig.where(cov), 'coverage': cov}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index, panel):
            return None
        row = state['sig'].iloc[index].dropna()
        if len(row) < max(self.MIN_COVERED, self.MIN_NAMES):
            return None
        n = max(int(round(len(row) * self.QUANTILE)), self.MIN_NAMES)
        n = min(n, len(row))
        # stable ordering: ties broken by ticker so a replay is deterministic
        ranked = row.reset_index()
        ranked.columns = ['ticker', 'value']
        ranked = ranked.sort_values(['value', 'ticker'], ascending=[False, True])
        held = list(ranked['ticker'].iloc[:n])
        w = {c: 0.0 for c in state['close'].columns}          # explicit flat everywhere else
        for c in held:
            w[c] = 1.0 / n
        return w
