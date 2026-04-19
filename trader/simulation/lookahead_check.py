"""Walk-forward consistency checker for ``Strategy.precompute``.

A strategy that uses the precompute hook gets the entire OHLCV history in one
shot — which is both the source of the fast path's speed and the source of
its most dangerous failure mode: **lookahead bias**. If a precomputed value
at past index ``k`` depends on bars beyond ``k``, the backtest is using
information that wouldn't have been available at that point in time, and
the resulting P&L is fiction.

This module provides ``assert_no_lookahead(strategy, prices)`` which runs
``precompute`` on the full series AND on progressively-truncated copies
and asserts that the value at every past index is identical across runs.
If hiding the last N bars changes what ``precompute`` returns for bar
``N-5``, that bar's value depended on the future.

What this catches (common real-world mistakes):
  - ``close.shift(-N)`` — explicit future-lookup
  - ``rolling(window, center=True)`` — centered rolling peeks forward
  - Full-series normalization: ``(x - x.mean()) / x.std()`` where mean/std
    span all bars including future ones
  - ``StandardScaler().fit_transform(full_series)`` — classic ML fit-on-train
  - HMM / change-point / regime detection fit on the complete history
  - ``scipy.signal.savgol_filter`` / Kalman smoother with backward pass

What this does NOT catch:
  - Adversarial patterns (e.g. "leak only when hide_bars > 10")
  - Leakage via external mutable state (strategies reading `self.foo`
    that was populated out-of-band)
  - Lookahead in ``on_prices`` or ``on_bar`` itself (those can still
    index ``prices.iloc[index+5]``; we only check ``precompute``)

Usage in a test::

    from trader.simulation.lookahead_check import assert_no_lookahead

    def test_my_strategy_no_lookahead():
        prices = _synthetic_ohlcv(n=200)
        assert_no_lookahead(MyStrategy(), prices)
"""

from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd


class LookaheadDetected(AssertionError):
    """Raised when ``precompute`` output at a past index changes depending
    on whether future bars are visible — i.e. the precomputed value used
    future data."""

    def __init__(
        self,
        *,
        key: str,
        index: int,
        full_value: Any,
        truncated_value: Any,
        hidden_bars: int,
        total_bars: int,
    ) -> None:
        self.key = key
        self.index = index
        self.full_value = full_value
        self.truncated_value = truncated_value
        self.hidden_bars = hidden_bars
        self.total_bars = total_bars

        super().__init__(
            f"Lookahead bias in precompute: state[{key!r}] at bar {index} "
            f"is {full_value!r} when precompute sees all {total_bars} bars, "
            f"but {truncated_value!r} when the last {hidden_bars} bar(s) are "
            f"hidden. A value at a past index must not change when future "
            f"bars are removed — if it does, the computation used future "
            f"information. Common causes: full-series normalization "
            f"(x / x.mean()), centered rolling, shift(-N), fit-on-full-series "
            f"ML models."
        )


def _as_array(v: Any) -> Optional[np.ndarray]:
    """Coerce a state value to a 1-D numpy array we can compare index-wise.
    Returns None for values we can't safely compare (dicts, scalars,
    multi-dim arrays, arbitrary objects)."""
    if isinstance(v, np.ndarray):
        arr = v
    elif isinstance(v, pd.Series):
        arr = v.to_numpy()
    elif isinstance(v, pd.DataFrame):
        # Skip DataFrames — multi-column comparison not well defined here.
        return None
    else:
        return None
    if arr.ndim != 1:
        return None
    return arr


def _values_equal(a: Any, b: Any, tolerance: float) -> bool:
    """Compare two scalar indicator values. NaN-aware: two NaNs are equal."""
    a_nan = isinstance(a, float) and np.isnan(a)
    b_nan = isinstance(b, float) and np.isnan(b)
    if a_nan and b_nan:
        return True
    if a_nan != b_nan:
        return False
    try:
        return abs(float(a) - float(b)) <= tolerance
    except (TypeError, ValueError):
        return a == b


def assert_no_lookahead(
    strategy,
    prices: pd.DataFrame,
    *,
    hide_bars_sequence: Sequence[int] = (1, 2, 5, 10),
    tolerance: float = 1e-9,
) -> None:
    """Walk-forward consistency check for a strategy's precompute hook.

    Calls ``strategy.precompute(prices)`` on the full DataFrame, then again
    on progressively truncated copies (last ``h`` bars hidden, for each
    ``h`` in ``hide_bars_sequence``). Asserts that for every key returned
    by precompute, the value at every past index is identical across runs.

    Arguments:
        strategy: an instantiated ``Strategy`` whose ``precompute`` you want
            to audit. Does not need to be installed via ``install()``.
        prices: the OHLCV DataFrame to test against. Needs enough bars that
            your indicators have fully warmed up — 200+ for most. Real
            market data is fine; deterministic synthetic data is faster and
            reproducible.
        hide_bars_sequence: how many trailing bars to hide in each test run.
            More values = more confidence but slower. Defaults sample both
            short (``1, 2``) and longer (``5, 10``) horizons, which catches
            most realistic bugs without running 100 truncation variants.
        tolerance: float tolerance for comparing past-index values. Defaults
            to 1e-9 (roughly IEEE-754 noise level). Bump slightly if you're
            using float32 anywhere.

    Raises:
        LookaheadDetected: on the first inconsistent (key, index) pair found.
            The exception message names the key, index, and both values so
            you can pinpoint what leaked.
    """
    full_state = strategy.precompute(prices)
    if not full_state:
        # Strategy opted out of precompute (returned empty dict) — nothing
        # to audit. This is fine; on_prices-only strategies can't have this
        # class of lookahead at all.
        return

    total_bars = len(prices)

    for hide in hide_bars_sequence:
        if hide <= 0 or hide >= total_bars:
            continue
        truncated_prices = prices.iloc[:-hide]
        truncated_state = strategy.precompute(truncated_prices)
        if not truncated_state:
            # Strategy returned empty for truncated input — could be because
            # it requires more bars than the truncated series has. Skip
            # this hide value rather than falsely failing.
            continue

        for key in full_state:
            if key not in truncated_state:
                continue

            full_arr = _as_array(full_state[key])
            trunc_arr = _as_array(truncated_state[key])
            if full_arr is None or trunc_arr is None:
                continue  # non-array state; not checkable here

            # truncated_state[key] should be aligned 1:1 with the truncated
            # prices (length total_bars - hide). So index i in trunc_arr
            # corresponds to index i in full_arr.
            n = min(len(full_arr), len(trunc_arr))
            if n == 0:
                continue

            # Vectorized fast-check first; only fall into per-element loop
            # to find the first mismatch for a descriptive error message.
            a = full_arr[:n]
            b = trunc_arr[:n]
            a_nan = np.isnan(a) if np.issubdtype(a.dtype, np.floating) else np.zeros(n, dtype=bool)
            b_nan = np.isnan(b) if np.issubdtype(b.dtype, np.floating) else np.zeros(n, dtype=bool)
            # both-nan is OK; otherwise values must match within tolerance
            mismatch = (a_nan != b_nan)
            if np.issubdtype(a.dtype, np.floating):
                diffs = np.where(a_nan | b_nan, 0.0, np.abs(a - b))
                mismatch = mismatch | (diffs > tolerance)
            else:
                mismatch = mismatch | (a != b)

            if mismatch.any():
                bad_idx = int(np.flatnonzero(mismatch)[0])
                raise LookaheadDetected(
                    key=key,
                    index=bad_idx,
                    full_value=full_arr[bad_idx],
                    truncated_value=trunc_arr[bad_idx],
                    hidden_bars=hide,
                    total_bars=total_bars,
                )
