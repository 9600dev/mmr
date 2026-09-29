"""Return statistics for the harness. Daily series in, plain floats out.

Active return = strategy daily return minus decisive-benchmark daily return
over the OOS windows (D01). Its mean is tested with a Newey-West t (the
per-period observations are stitched from several folds and a rebalanced book
is autocorrelated at short lags), its Sharpe is deflated by the family trial
count via ``expected_max_sharpe`` + ``probabilistic_sharpe`` (the same pair
``selection_bias.deflated_sharpe`` composes, with N decoupled from the number
of trials whose Sharpe happens to be recorded).
"""
from __future__ import annotations

import math
from typing import Dict, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from trader.simulation.backtest_stats import probabilistic_sharpe
from trader.simulation.selection_bias import expected_max_sharpe
from trader.simulation.signal_eval import newey_west_variance

BARS_PER_YEAR = 252.0


def _clean(r) -> np.ndarray:
    a = np.asarray(pd.Series(r, dtype=float).to_numpy(), dtype=float)
    return a[np.isfinite(a)]


def ann_sharpe(returns) -> Optional[float]:
    a = _clean(returns)
    if len(a) < 3:
        return None
    sd = float(np.std(a, ddof=1))
    if not (sd > 0):
        return None
    return float(np.mean(a) / sd * math.sqrt(BARS_PER_YEAR))


def ann_mean(returns) -> Optional[float]:
    """Arithmetic annualised mean - the quantity the Newey-West t tests."""
    a = _clean(returns)
    return float(np.mean(a) * BARS_PER_YEAR) if len(a) else None


def cagr(returns) -> Optional[float]:
    a = _clean(returns)
    if len(a) == 0:
        return None
    growth = float(np.prod(1.0 + a))
    if growth <= 0:
        return -1.0
    return growth ** (BARS_PER_YEAR / len(a)) - 1.0


def max_drawdown(returns) -> Optional[float]:
    a = _clean(returns)
    if len(a) == 0:
        return None
    eq = np.cumprod(1.0 + a)
    peak = np.maximum.accumulate(eq)
    return float(np.min(eq / peak - 1.0))


def bartlett_lags(n: int) -> int:
    """Newey-West bandwidth, the usual 4*(n/100)^(2/9) rule."""
    return int(math.floor(4.0 * (max(n, 1) / 100.0) ** (2.0 / 9.0))) if n > 1 else 0


def newey_west_t(returns, lags: Optional[int] = None) -> Tuple[Optional[float], int]:
    a = _clean(returns)
    n = len(a)
    if n < 3:
        return None, 0
    L = bartlett_lags(n) if lags is None else int(lags)
    var = newey_west_variance(tuple(float(x) for x in a), L)
    if var is None or var <= 0:
        return None, L
    return float(np.mean(a) / math.sqrt(var)), L


def per_calendar_year(returns: pd.Series, min_days: int = 60) -> Dict[int, Dict[str, float]]:
    """Arithmetic sum of daily active return per calendar year. Years with
    fewer than ``min_days`` observations are reported but flagged ``counted``
    False (a fortnight of data is not a year of evidence)."""
    s = pd.Series(returns, dtype=float).dropna()
    out: Dict[int, Dict[str, float]] = {}
    if len(s) == 0:
        return out
    years = np.asarray([pd.Timestamp(str(t)).year for t in s.index], dtype=int)
    values = s.to_numpy(dtype=float)
    for year in np.unique(years):
        grp = values[years == year]
        out[int(year)] = {'active_return': float(grp.sum()), 'days': int(len(grp)),
                          'counted': bool(len(grp) >= min_days)}
    return out


def fraction_positive_years(table: Dict[int, Dict[str, float]]) -> Optional[float]:
    counted = [v for v in table.values() if v.get('counted')]
    if not counted:
        return None
    return sum(1 for v in counted if v['active_return'] > 0) / len(counted)


def deflated_sharpe_n(returns, n_trials: int, trial_sharpes_annualised: Sequence[float]) -> Optional[float]:
    """PSR of ``returns`` against the Sharpe the best of ``n_trials`` would
    show under the null. Variance across trials is measured from whichever
    trials have a recorded Sharpe (per-period units); with fewer than two it is
    zero and the number degenerates to plain PSR - stated, not hidden."""
    a = _clean(returns)
    if len(a) < 3:
        return None
    s = np.asarray([x for x in trial_sharpes_annualised if x is not None and math.isfinite(x)], dtype=float)
    per_period = s / math.sqrt(BARS_PER_YEAR)
    variance = float(np.var(per_period, ddof=1)) if len(per_period) > 1 else 0.0
    sr_star = expected_max_sharpe(max(int(n_trials), 1), variance)
    if sr_star is None:
        return None
    return probabilistic_sharpe(a, benchmark_sharpe=sr_star)


def summarise(returns: pd.Series) -> Dict[str, Optional[float]]:
    return {'sharpe': ann_sharpe(returns), 'cagr': cagr(returns), 'ann_mean': ann_mean(returns),
            'max_drawdown': max_drawdown(returns), 'days': int(pd.Series(returns).dropna().shape[0])}
