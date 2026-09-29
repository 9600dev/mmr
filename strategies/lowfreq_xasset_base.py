"""Shared base for the cross-asset ETF family (C02, D04) - see
``research/lowfreq/xasset/PREREG.md`` section C0 for the conventions it pins.

A subclass implements ``risk_weights(close, risk)`` and returns a (time x
risk-ticker) frame of target weights over the RISK assets only, computed
causally (rolling windows with ``min_periods`` = window). The base then:

* zeroes any weight on a name without a close on the decision bar (D04: no
  ticker is used before its first bar, and a name that stopped printing is
  sold, never bought);
* clips to long-only and scales a row down when its gross exceeds 1.0 (D01);
* puts the unallocated remainder in BIL when BIL prints that day, else leaves
  it in cash (D03: cash earns zero, a cash yield means holding BIL);
* emits every column explicitly (0.0 where flat) on rebalance days and
  ``None`` between them (hold), monthly by default (D04).

Everything in ``precompute_panel`` is prefix-stable, so the harness's
fresh-state lookahead replay (``engine.lookahead_check``) sees identical
values whether or not later bars are hidden.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from trader.research.lowfreq.reference_strategies import LowFreqStrategy

ANNUALISATION = 252.0
GROSS_TOL = 1e-9


def trailing_return(close: pd.DataFrame, days: int) -> pd.DataFrame:
    """``close_t / close_{t-days} - 1`` (NaN until ``days`` bars exist)."""
    return close / close.shift(days) - 1.0


def realised_vol(close: pd.DataFrame, days: int) -> pd.DataFrame:
    """Annualised std of daily returns over ``days`` sessions, defined only
    when the whole window is present (``min_periods = days``)."""
    return close.pct_change().rolling(days, min_periods=days).std() * np.sqrt(ANNUALISATION)


def inverse_vol_base(close: pd.DataFrame, days: int) -> pd.DataFrame:
    """Row-normalised 1/sigma over the names whose sigma is defined and > 0:
    the unlevered risk-parity base (PREREG H1/H4)."""
    sigma = realised_vol(close, days)
    inv = (1.0 / sigma).where(sigma > 0)
    inv = inv.replace([np.inf, -np.inf], np.nan)
    total = inv.sum(axis=1, min_count=1)
    return inv.div(total, axis=0).where(total > 0)


class XAssetBase(LowFreqStrategy):
    RISK_OFF = 'BIL'          # the cash proxy (never a risk asset)

    def risk_weights(self, close: pd.DataFrame, risk: pd.DataFrame) -> pd.DataFrame:
        """Target weights over the risk assets (columns of ``risk``), time x
        ticker, causal. NaN means 0. Row gross may be < 1 (remainder -> BIL)."""
        raise NotImplementedError

    def excess_return(self, close: pd.DataFrame, risk: pd.DataFrame, days: int) -> pd.DataFrame:
        """Risk assets' ``days``-session return minus BIL's over the same span
        (BIL absent from the panel -> raw return)."""
        ret = trailing_return(risk, days)
        if self.RISK_OFF in close.columns:
            return ret.sub(trailing_return(close[[self.RISK_OFF]], days)[self.RISK_OFF], axis=0)
        return ret

    def precompute_panel(self, panel: pd.DataFrame) -> Dict[str, Any]:
        close = panel['close']
        risk_cols = [c for c in close.columns if c != self.RISK_OFF]
        w = self.risk_weights(close, close[risk_cols])
        w = w.reindex(index=close.index, columns=risk_cols).astype(float)
        w = w.replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0.0)
        w = w.where(close[risk_cols].notna(), 0.0)           # D04: no close today -> no weight
        gross = w.sum(axis=1)
        over = gross > 1.0
        if over.any():
            w.loc[over] = w.loc[over].div(gross[over], axis=0)  # D01: gross <= 1, scale down only
            gross = w.sum(axis=1)
        full = w.copy()
        if self.RISK_OFF in close.columns:
            spare = (1.0 - gross).clip(lower=0.0)
            full[self.RISK_OFF] = spare.where(close[self.RISK_OFF].notna(), 0.0)
        full = full.reindex(columns=close.columns).fillna(0.0)
        return {'close': close, 'weights': full}

    def on_panel(self, panel, state, index) -> Optional[Dict[Any, float]]:
        if not self.due(index, panel):
            return None
        row = state['weights'].iloc[index]
        vals = row.to_numpy(dtype=float)
        if not np.all(np.isfinite(vals)) or vals.min() < 0 or vals.sum() > 1.0 + GROSS_TOL:
            raise ValueError(f'{type(self).__name__}: malformed weights at {panel.index[index]}: '
                             f'gross {vals.sum():.6f} min {vals.min():.6f}')
        return {c: float(v) for c, v in row.items()}
