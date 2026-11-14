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

Truncation happens in two places. The last few bars are hidden (1, 2, 5, 10)
— and the series is ALSO cut at interior points: at day boundaries, inside
and just after each session's opening range, mid-session, near the close.
The interior cuts are what find a PER-DAY leak. ``groupby(day).transform('max')``
broadcasts a whole day's extreme to every bar of that day; hiding the last
ten bars changes it only when that extreme happens to sit in those ten bars
(8 of 40 seeded frames, measured), while a cut in the middle of a session
changes it nearly always (39 of 40).

A check that compares nothing passes, so ``lookahead_report`` also returns
WHAT was compared: an array that is all-NaN or constant cannot reveal a leak
however many times it is re-derived. Callers that gate on this check (the
strategy gauntlet) must read that report rather than treat "no exception" as
"verified".

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

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

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


@dataclass(frozen=True)
class KeyCoverage:
    """What the check actually saw for one array-valued state key."""

    compared: int       # truncated runs whose values were compared to the full run
    informative: bool   # takes at least two distinct values (NaN counts as one)
    all_nan: bool       # not one non-NaN value anywhere


@dataclass(frozen=True)
class LookaheadReport:
    """What ``lookahead_report`` compared on one price frame.

    "No leak found" is only evidence about values that were examined, so this
    records which were: an all-NaN indicator is unchanged by every truncation
    whether or not it would leak on data it responds to.
    """

    total_bars: int
    truncations: Tuple[int, ...] = ()           # prefix lengths that produced state
    keys: Dict[str, KeyCoverage] = field(default_factory=dict)
    uncheckable: Tuple[str, ...] = ()           # state keys that are not 1-D arrays

    @property
    def exercised(self) -> List[str]:
        """Keys that could have revealed a leak: compared at least once and
        not degenerate."""
        return [k for k, c in self.keys.items() if c.compared > 0 and c.informative]

    @property
    def degenerate(self) -> List[str]:
        """Array keys the check could not have learned anything from."""
        return [k for k, c in self.keys.items() if not (c.compared > 0 and c.informative)]


def _coverage_of(arr: np.ndarray) -> Tuple[bool, bool]:
    """``(informative, all_nan)`` for one state array."""
    if arr.size == 0:
        return False, False
    values = pd.Series(arr)
    missing = values.isna()
    if bool(missing.all()):
        return False, True
    try:
        distinct = int(values[~missing].nunique()) + (1 if bool(missing.any()) else 0)
    except TypeError:
        # Unhashable entries: cannot be shown constant, so not called degenerate.
        return True, False
    return distinct >= 2, False


def interior_cut_points(
    index: pd.Index,
    *,
    session_tz: Optional[str] = None,
    open_minute: Optional[int] = None,
    close_minute: Optional[int] = None,
    max_days: int = 8,
) -> List[int]:
    """Prefix lengths at which to truncate a price frame, away from its end.

    Always: every tenth of the frame. For an intraday ``DatetimeIndex``, also
    per calendar day (at most ``max_days`` of them, spread across the frame):
    the day boundary and a spread of points inside the day. When the session
    is given (``open_minute``/``close_minute`` since midnight in
    ``session_tz``; the index's own zone when that is None), the in-day points
    are anchored to it instead: the bar before the open, 5 and 15 minutes into
    the session, 30 and 31 minutes in (either side of a half-hour opening
    range), an hour in, mid-session, half an hour before the close, the last
    session bar, and the close itself.

    A returned value ``c`` means "re-run on ``prices.iloc[:c]``".
    """
    n = len(index)
    if n < 3:
        return []
    cuts = {int(round(n * tenth / 10.0)) for tenth in range(1, 10)}

    if isinstance(index, pd.DatetimeIndex):
        local = index if index.tz is not None else index.tz_localize('UTC')
        if session_tz is not None:
            local = local.tz_convert(session_tz)
        # Wall-clock minutes since the epoch in that zone.
        wall = np.asarray(local.tz_localize(None), dtype='datetime64[m]').astype(np.int64)
        minute, day = wall % (24 * 60), wall // (24 * 60)
        starts = np.flatnonzero(np.r_[True, day[1:] != day[:-1]])
        if len(starts) < n:                                   # more than one bar a day
            ends = np.r_[starts[1:], n]
            chosen: Sequence[int] = range(len(starts))
            if len(starts) > max_days:
                # Evenly spread, always including the first and the last day.
                chosen = sorted({int(round(i * (len(starts) - 1) / max(max_days - 1, 1)))
                                 for i in range(max(max_days, 1))})
            for d in chosen:
                s, e = int(starts[d]), int(ends[d])
                cuts.add(s)
                cuts.update(_cuts_within_day(minute[s:e], s, open_minute, close_minute))

    return sorted(c for c in cuts if 0 < c < n)


def _cuts_within_day(minute: np.ndarray, offset: int, open_minute: Optional[int],
                     close_minute: Optional[int]) -> List[int]:
    """Cut points inside one calendar day whose bars sit at ``minute`` (minutes
    since local midnight) and whose first bar is at position ``offset``."""
    if open_minute is not None and close_minute is not None:
        in_session = np.flatnonzero((minute >= open_minute) & (minute < close_minute))
        if len(in_session):
            first, last = int(in_session[0]), int(in_session[-1]) + 1
            cuts = [first, last, last - 1, first + (last - first) // 2]
            for target in (open_minute + 5, open_minute + 15, open_minute + 30,
                           open_minute + 31, open_minute + 60, close_minute - 30):
                at = np.flatnonzero(minute >= target)
                if len(at):
                    cuts.append(int(at[0]))
            return [offset + c for c in cuts]
    # No session known, or none of its bars on this day: spread through the day.
    return [offset + int(len(minute) * f) for f in (0.02, 0.05, 0.1, 0.25, 0.5, 0.75)]


def lookahead_report(
    strategy,
    prices: pd.DataFrame,
    *,
    hide_bars_sequence: Sequence[int] = (1, 2, 5, 10),
    cut_points: Optional[Sequence[int]] = None,
    tolerance: float = 1e-9,
) -> LookaheadReport:
    """Run the walk-forward consistency check and report what it compared.

    Same check, arguments and exception as ``assert_no_lookahead``; the
    difference is the return value. Use this when the caller must know whether
    a clean result means "verified" or "there was nothing to look at".
    """
    total_bars = len(prices)
    full_state = strategy.precompute(prices)
    if not full_state:
        # Strategy opted out of precompute (returned empty dict) — nothing
        # to audit. The empty report says so.
        return LookaheadReport(total_bars=total_bars)

    full_arrays: Dict[str, np.ndarray] = {}
    uncheckable: List[str] = []
    for key, value in full_state.items():
        arr = _as_array(value)
        if arr is None:
            uncheckable.append(key)     # non-array state; not checkable here
        else:
            full_arrays[key] = arr
    compared = {key: 0 for key in full_arrays}

    if cut_points is None:
        cut_points = interior_cut_points(prices.index)
    # Trailing hides first, in the order given; then the interior cuts from
    # the longest prefix down. Each entry is a prefix length.
    lengths: List[int] = []
    for length in ([total_bars - int(h) for h in hide_bars_sequence]
                   + sorted({int(c) for c in cut_points}, reverse=True)):
        if 0 < length < total_bars and length not in lengths:
            lengths.append(length)

    truncations: List[int] = []
    for length in lengths:
        truncated_state = strategy.precompute(prices.iloc[:length])
        if not truncated_state:
            # Strategy returned empty for truncated input — could be because
            # it requires more bars than the truncated series has. Skip
            # this truncation rather than falsely failing.
            continue
        truncations.append(length)

        for key, full_arr in full_arrays.items():
            if key not in truncated_state:
                continue
            trunc_arr = _as_array(truncated_state[key])
            if trunc_arr is None:
                continue

            # truncated_state[key] should be aligned 1:1 with the truncated
            # prices (``length`` bars). So index i in trunc_arr corresponds
            # to index i in full_arr.
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
                    hidden_bars=total_bars - length,
                    total_bars=total_bars,
                )
            compared[key] += 1

    keys: Dict[str, KeyCoverage] = {}
    for key, arr in full_arrays.items():
        informative, all_nan = _coverage_of(arr)
        keys[key] = KeyCoverage(compared=compared[key], informative=informative,
                                all_nan=all_nan)
    return LookaheadReport(total_bars=total_bars, truncations=tuple(truncations),
                           keys=keys, uncheckable=tuple(uncheckable))


def assert_no_lookahead(
    strategy,
    prices: pd.DataFrame,
    *,
    hide_bars_sequence: Sequence[int] = (1, 2, 5, 10),
    cut_points: Optional[Sequence[int]] = None,
    tolerance: float = 1e-9,
) -> None:
    """Walk-forward consistency check for a strategy's precompute hook.

    Calls ``strategy.precompute(prices)`` on the full DataFrame, then again
    on truncated copies: with the last ``h`` bars hidden for each ``h`` in
    ``hide_bars_sequence``, and cut at each prefix length in ``cut_points``.
    Asserts that for every key returned by precompute, the value at every
    past index is identical across runs.

    Arguments:
        strategy: an instantiated ``Strategy`` whose ``precompute`` you want
            to audit. Does not need to be installed via ``install()``.
        prices: the OHLCV DataFrame to test against. Needs enough bars that
            your indicators have fully warmed up — 200+ for most — and, for
            a strategy that only acts inside a session, bars INSIDE that
            session: on a frame with none, every session-gated array is NaN
            and this check passes without having examined anything. Real
            market data is fine; ``synthetic_markets.session_trending`` and
            friends are deterministic and session-stamped.
        hide_bars_sequence: how many trailing bars to hide in each test run.
            Defaults sample both short (``1, 2``) and longer (``5, 10``)
            horizons.
        cut_points: prefix lengths to truncate at as well. ``None`` (default)
            derives them from the index with ``interior_cut_points`` — day
            boundaries and points inside each day; pass that function's
            session-anchored output when you know the session, or ``()`` to
            hide trailing bars only.
        tolerance: float tolerance for comparing past-index values. Defaults
            to 1e-9 (roughly IEEE-754 noise level). Bump slightly if you're
            using float32 anywhere.

    Raises:
        LookaheadDetected: on the first inconsistent (key, index) pair found.
            The exception message names the key, index, and both values so
            you can pinpoint what leaked.
    """
    lookahead_report(strategy, prices, hide_bars_sequence=hide_bars_sequence,
                     cut_points=cut_points, tolerance=tolerance)


def assert_no_panel_lookahead(
    strategy,
    panel: 'pd.DataFrame',
    *,
    hide_bars_sequence: Sequence[int] = (1, 2, 5, 10),
    tolerance: float = 1e-9,
) -> None:
    """The same walk-forward consistency check, for `precompute_panel`.

    Every piece of cross-sectional research in this repo runs through the panel
    path, and until now nothing checked it for leaks. The per-instrument checker
    cannot: it passes a single OHLCV frame, while `precompute_panel` takes a
    wide two-level (field, conid) panel, so it would fail on the shape long
    before it examined a value.

    The leak this catches is the one that produced a large stable fake IC
    elsewhere today: anything that normalises across TIME rather than across
    names. A cross-sectional z-score is safe because each row uses only that
    row; a time-series z-score divides by a standard deviation computed from
    the whole sample INCLUDING THE FUTURE, and the resulting signal knows how
    volatile the next five years will be. Truncating the panel changes that
    denominator, so a past value shifts - which is exactly what this asserts
    cannot happen.

    Also catches `shift(-1)`, centred rolling windows, `fit_transform` on the
    complete history, and cumulative statistics taken over the full frame.
    """
    import numpy as np
    import pandas as pd

    full = strategy.precompute_panel(panel)
    if not isinstance(full, dict) or not full:
        return                          # nothing precomputed, nothing to leak

    n = len(panel)
    for hide in hide_bars_sequence:
        if hide >= n - 1:
            continue
        truncated = strategy.precompute_panel(panel.iloc[:n - hide])
        for key, full_val in full.items():
            trunc_val = truncated.get(key)
            if trunc_val is None:
                raise AssertionError(
                    f'precompute_panel returned key {key!r} on the full panel '
                    f'but not with the last {hide} bar(s) hidden; the set of '
                    f'outputs must not depend on how much future exists')
            if not hasattr(full_val, 'iloc') or not hasattr(trunc_val, 'iloc'):
                continue                # not frame-like; nothing to align
            common = min(len(full_val), len(trunc_val))
            a = np.asarray(full_val.iloc[:common], dtype=float)
            b = np.asarray(trunc_val.iloc[:common], dtype=float)
            if a.shape != b.shape:
                raise AssertionError(
                    f'precompute_panel key {key!r} changed shape when the last '
                    f'{hide} bar(s) were hidden: {a.shape} vs {b.shape}')
            # NaN in the same places, finite values equal. A value that was NaN
            # with the future hidden and became a number with it visible is a
            # leak even when the number looks reasonable.
            both_nan = np.isnan(a) & np.isnan(b)
            differ = ~both_nan & ~(np.abs(a - b) <= tolerance)
            if differ.any():
                where = np.argwhere(differ)
                first = tuple(int(x) for x in where[0])
                raise AssertionError(
                    f'LOOKAHEAD in precompute_panel key {key!r}: hiding the '
                    f'last {hide} bar(s) changed a PAST value at index {first} '
                    f'({a[first]!r} -> {b[first]!r}). Something is normalising '
                    f'across time rather than across names, or reading forward.')
