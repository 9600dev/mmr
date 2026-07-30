"""How much of a backtest result is selection rather than edge.

WHY THIS EXISTS
    Every other statistic in this codebase answers "is THIS run real?".
    None of them know that the run was CHOSEN. `bt-sweep` over a 27-cell grid
    returns the best of 27 draws, and the best of 27 draws from pure noise has
    a flattering Sharpe by construction. Reporting it as if it were a single
    pre-registered hypothesis is the standard way to deploy noise, and the
    2026-07 roster was selected exactly that way.

    Both tests here are López de Prado's, and they answer different halves:

      * DEFLATED SHARPE (`deflated_sharpe`) — one number, one run. Given that
        you tried N configurations, how much Sharpe would you EXPECT the best
        of them to show under the null of no skill? Deflate by that and ask
        whether what is left is still significant.

      * PBO (`pbo_cscv`) — one number, one whole sweep. Across many
        train/test splits, how often does the configuration that wins IN
        SAMPLE land in the bottom half OUT OF SAMPLE? That is the probability
        that your selection procedure — not any particular result — is
        overfitting. A PBO near 0.5 means the sweep's winner is a coin flip,
        no matter how good its headline numbers are.

    PBO is the more damning of the two because it indicts the METHOD. You can
    re-run a strategy with better data; you cannot re-run a selection process
    that was never informative.

REFERENCES
    Bailey & López de Prado (2014), "The Deflated Sharpe Ratio: Correcting for
    Selection Bias, Backtest Overfitting and Non-Normality", Journal of
    Portfolio Management 40(5).
    Bailey, Borwein, López de Prado & Zhu (2017), "The Probability of Backtest
    Overfitting", Journal of Computational Finance 20(4). CSCV is section 3.

ON CONTRACTS AND CROSSHAIR
    These are `deal`-contracted on their output ranges but are NOT in the
    CrossHair target list, unlike the order kernel. CrossHair executes
    symbolically and cannot see through numpy's C implementations, so it would
    report vacuous success rather than checking anything — a green tick that
    means nothing is worse than no tick. The properties are pinned by
    Hypothesis instead, in `tests/invariants/test_selection_bias.py`, against
    generated return matrices whose right answer is known by construction.
"""

from __future__ import annotations

import itertools
import math

from typing import List, NamedTuple, Optional, Sequence, Tuple

import deal
import numpy as np


# Euler–Mascheroni constant, from the expected-maximum-of-N-gaussians
# approximation. Named rather than inlined because a wrong digit here silently
# mis-deflates every result.
_EULER_MASCHERONI = 0.5772156649015329


# Below this many trials the estimate is too noisy to read as a point value.
# Measured, not assumed: over 12 seeds of a KNOWN-noise family (true PBO 0.5),
# the estimate had sd 0.32 at N=5 and 0.22 at N=12, falling to 0.13 by N=30.
# Crucially the spread does NOT shrink with more observations (sd 0.27 at
# T=100 vs 0.23 at T=1200) — it is driven by the coarseness of the rank grid,
# which has only N+1 positions. So a small sweep reading PBO=0.25 is entirely
# consistent with pure noise, and reporting that bare number as "not overfit"
# would invert its meaning. The caveat travels WITH the result rather than
# living in a docstring, because the number is what gets pasted into a
# decision and the qualification is what gets left behind.
_MIN_TRIALS_FOR_POINT_ESTIMATE = 20


class PBOResult(NamedTuple):
    """Outcome of a CSCV pass over a family of trials."""
    pbo: float                    # P(in-sample winner is below median OOS), [0, 1]
    n_splits: int                 # S — how many blocks the period was cut into
    n_combinations: int           # C(S, S/2) — how many train/test pairs were scored
    n_trials: int                 # N — how many configurations were compared
    n_observations: int           # T — length of each trial's return series
    median_oos_rank: float        # median relative rank of the IS winner, [0, 1]
    logits: List[float]           # per-combination λ, for plotting the distribution
    caveat: Optional[str]         # set when N is too small to read as a point value


@deal.post(lambda r: r is None or r >= 0.0)
def expected_max_sharpe(n_trials: int, trial_sharpe_variance: float) -> Optional[float]:
    """The Sharpe you should EXPECT from the best of ``n_trials``, under the
    null that none of them has any skill.

    This is the whole idea of deflation in one function. If you try 27
    parameter sets on a strategy with zero edge, the best of them does not
    score 0 — it scores the expected maximum of 27 draws from the sampling
    distribution of Sharpe under the null, which for a realistic spread of
    trial results is a comfortably positive number. That value, not zero, is
    the benchmark the winner must beat.

        E[max SR] ≈ √V · [ (1-γ)·Φ⁻¹(1 - 1/N) + γ·Φ⁻¹(1 - 1/(N·e)) ]

    ``trial_sharpe_variance`` (V) is the variance of the Sharpes ACROSS trials
    and must be measured from the sweep, not assumed: it encodes how much the
    configurations actually differ. A grid whose cells all behave identically
    has V≈0 and deflates by almost nothing, which is correct — you did not
    really try 27 things.

    UNITS ARE A TRAP. V must be in the same units as the Sharpe you intend to
    deflate. This function has no way to check that, so the caller carries the
    obligation; `deflated_sharpe` below does the conversion in one place so
    callers do not each get it wrong.

    Returns 0.0 for N ≤ 1 — the expected maximum of a single draw is its mean,
    which under the null is zero, so a single un-selected run deflates by
    nothing and DSR degenerates to PSR. The asymptotic formula cannot express
    that (Φ⁻¹(0) is -∞), so it is special-cased rather than clamped.
    """
    from scipy.stats import norm

    if n_trials <= 1:
        return 0.0
    if not math.isfinite(trial_sharpe_variance) or trial_sharpe_variance < 0:
        return None
    if trial_sharpe_variance == 0.0:
        return 0.0

    n = float(n_trials)
    gamma = _EULER_MASCHERONI
    z1 = float(norm.ppf(1.0 - 1.0 / n))
    z2 = float(norm.ppf(1.0 - 1.0 / (n * math.e)))
    if not (math.isfinite(z1) and math.isfinite(z2)):
        return None
    return math.sqrt(trial_sharpe_variance) * ((1.0 - gamma) * z1 + gamma * z2)


@deal.post(lambda r: r is None or 0.0 <= r <= 1.0)
def deflated_sharpe(
    returns: np.ndarray,
    trial_sharpes_annualised: Sequence[float],
    bars_per_year: float = 252.0,
) -> Optional[float]:
    """Probability the true Sharpe beats what selection alone would produce.

    Same shape as PSR, with a benchmark that is no longer zero: instead of
    "is this better than nothing?", it asks "is this better than the best of N
    coin flips?". A run can have PSR 0.97 and DSR 0.30 — the edge is real
    against a null of zero, and unremarkable against a null of "you searched".

    ``trial_sharpes_annualised`` is the Sharpe of EVERY trial in the family the
    winner was chosen from, INCLUDING the winner, in the annualised units this
    codebase stores (`backtest_runs.sharpe_ratio`). Passing only the survivors
    of a manual cull understates N and V, and returns a number that is too
    kind — the count that matters is how many you LOOKED at, not how many you
    kept.

    The de-annualisation is the reason this wrapper exists at all. PSR reads
    per-period returns and forms a per-period Sharpe, so a benchmark built from
    annualised trial Sharpes would be √252 ≈ 16× too large and would deflate
    every strategy ever written to zero. Doing it here means callers cannot get
    the units wrong individually.
    """
    from trader.simulation.backtest_stats import probabilistic_sharpe

    r = np.asarray(returns, dtype=float)
    r = r[np.isfinite(r)]
    if len(r) < 3:
        return None

    sharpes = np.asarray(list(trial_sharpes_annualised), dtype=float)
    sharpes = sharpes[np.isfinite(sharpes)]
    n_trials = len(sharpes)
    if n_trials == 0:
        return None
    if not (math.isfinite(bars_per_year) and bars_per_year > 0):
        return None

    # Annualised -> per-period, matching the scale PSR works in.
    per_period = sharpes / math.sqrt(bars_per_year)
    variance = float(np.var(per_period, ddof=1)) if n_trials > 1 else 0.0

    sr_star = expected_max_sharpe(n_trials, variance)
    if sr_star is None:
        return None
    return probabilistic_sharpe(r, benchmark_sharpe=sr_star)


def _block_moments(matrix: np.ndarray, n_splits: int):
    """Per-block (count, sum, sum-of-squares) for every trial.

    CSCV scores C(S, S/2) train/test pairs, which is 12,870 at the default
    S=16. Recomputing a Sharpe over half the rows each time would be
    O(C·T·N) and slow enough that nobody would run this. Pooling from
    per-block aggregates is O(C·S·N) and gives numerically identical means
    and variances, because both are exact functions of (n, Σx, Σx²).
    """
    T = matrix.shape[0]
    bounds = np.linspace(0, T, n_splits + 1).astype(int)
    counts, sums, sumsqs = [], [], []
    for i in range(n_splits):
        block = matrix[bounds[i]:bounds[i + 1], :]
        counts.append(block.shape[0])
        sums.append(block.sum(axis=0))
        sumsqs.append((block ** 2).sum(axis=0))
    return (np.array(counts, dtype=float),
            np.array(sums, dtype=float),
            np.array(sumsqs, dtype=float))


def _pooled_sharpe(counts, sums, sumsqs, chosen) -> np.ndarray:
    """Per-trial Sharpe over the union of the ``chosen`` blocks."""
    n = counts[list(chosen)].sum()
    s = sums[list(chosen), :].sum(axis=0)
    ss = sumsqs[list(chosen), :].sum(axis=0)
    if n < 2:
        return np.zeros(sums.shape[1])
    mean = s / n
    var = (ss - n * mean ** 2) / (n - 1.0)

    # CATASTROPHIC CANCELLATION. For a near-constant column, `ss` and
    # `n * mean**2` are nearly equal large numbers, so their difference is
    # floating-point noise - tiny, but often positive. sqrt of that noise is a
    # denominator close to zero, and the Sharpe explodes: a genuinely constant
    # column of 0.005 returned 38,214,751 before this guard. Such a trial then
    # wins the in-sample argmax in every CSCV split and corrupts the PBO.
    #
    # The docstring's claim that this pooled form is "numerically identical" to
    # the direct computation was therefore FALSE precisely where it mattered;
    # numpy's std() uses a stable two-pass method and does not cancel. Rather
    # than abandon the aggregates - CSCV needs them to be runnable at all - the
    # variance is compared against the noise floor its own inputs imply: if it
    # is negligible relative to the mean square, it is unresolvable and the
    # column carries no information, which is 0.0 in a ranking.
    scale = ss / n
    with np.errstate(invalid='ignore', divide='ignore'):
        resolvable = var > 1e-10 * np.maximum(scale, np.finfo(float).tiny)
        sd = np.sqrt(np.maximum(var, 0.0))
        sharpe = np.where(resolvable & (sd > 0), mean / sd, 0.0)
    return np.nan_to_num(sharpe, nan=0.0, posinf=0.0, neginf=0.0)


@deal.post(lambda r: r is None or 0.0 <= r.pbo <= 1.0)
def pbo_cscv(
    returns_matrix: np.ndarray,
    n_splits: int = 16,
) -> Optional[PBOResult]:
    """Probability of Backtest Overfitting, by Combinatorially Symmetric
    Cross-Validation.

    ``returns_matrix`` is (T observations × N trials): one column per
    configuration tried, one row per period, ALIGNED — row *t* must be the
    same instant in every column. Misaligned columns make this silently
    meaningless rather than wrong-looking, which is why the caller that builds
    the matrix from stored equity curves intersects on timestamps rather than
    assuming equal lengths.

    The procedure, per Bailey et al. (2017) §3:

      1. cut the period into S disjoint blocks;
      2. for each of the C(S, S/2) ways to choose half the blocks as training,
         take the remaining half as testing;
      3. find the trial that wins in training;
      4. record where THAT trial ranks among all trials in testing;
      5. PBO is the fraction of splits where the training winner ranked in the
         bottom half out of sample.

    The symmetry in the name is why it is trustworthy: every block serves as
    training exactly as often as testing, so the estimate cannot be an artefact
    of which period you happened to call "in sample". A single 70/30 split
    can be lucky; 12,870 symmetric splits cannot all be.

    Interpretation, and it is blunt: PBO ≈ 0.5 means the sweep's winner is
    indistinguishable from picking a cell at random. That is not "weak
    evidence of edge", it is evidence of NO selection skill, and it holds
    however good the winner's headline Sharpe looks.

    Returns ``None`` when the input cannot support the test: fewer than 2
    trials (a rank among one thing is meaningless), fewer than 2 observations
    per block, or an odd/degenerate ``n_splits``.
    """
    m = np.asarray(returns_matrix, dtype=float)
    if m.ndim != 2:
        return None
    T, N = m.shape
    if N < 2 or T < 4:
        return None
    if n_splits < 2 or n_splits % 2 != 0:
        return None
    # Every block must hold enough rows for a variance. Shrink to the largest
    # even S that does, rather than refusing: a short daily series is the
    # normal case, not an error.
    while n_splits > 2 and T // n_splits < 2:
        n_splits -= 2
    if T // n_splits < 2:
        return None

    m = np.nan_to_num(m, nan=0.0, posinf=0.0, neginf=0.0)
    counts, sums, sumsqs = _block_moments(m, n_splits)

    all_blocks = set(range(n_splits))
    logits: List[float] = []
    ranks: List[float] = []
    for train in itertools.combinations(range(n_splits), n_splits // 2):
        test = tuple(sorted(all_blocks - set(train)))
        is_perf = _pooled_sharpe(counts, sums, sumsqs, train)
        oos_perf = _pooled_sharpe(counts, sums, sumsqs, test)

        winner = int(np.argmax(is_perf))
        # Relative rank of the winner's OOS performance among all trials.
        # 1-based ascending: 1 = worst OOS, N = best. Ties resolve to the
        # average rank so a family of identical trials lands at 0.5 rather
        # than being decided by column order.
        oos_w = oos_perf[winner]
        better = float((oos_perf < oos_w).sum())
        equal = float((oos_perf == oos_w).sum())
        rank = better + (equal + 1.0) / 2.0
        omega = rank / (N + 1.0)
        omega = min(max(omega, 1e-9), 1.0 - 1e-9)
        ranks.append(omega)
        logits.append(math.log(omega / (1.0 - omega)))

    if not logits:
        return None
    pbo = float(np.mean([1.0 if x <= 0.0 else 0.0 for x in logits]))
    caveat = None
    if N < _MIN_TRIALS_FOR_POINT_ESTIMATE:
        caveat = (
            f'only {N} trials — at this width the estimate has a standard '
            f'deviation around 0.2-0.3 even when the true value is 0.5, so '
            f'read it as a direction, not a number. Values between 0.2 and '
            f'0.8 do not distinguish a real edge from noise here.')
    return PBOResult(
        pbo=pbo,
        n_splits=n_splits,
        n_combinations=len(logits),
        n_trials=N,
        n_observations=T,
        median_oos_rank=float(np.median(ranks)),
        logits=logits,
        caveat=caveat,
    )


@deal.post(lambda r: r is None or r > 0.0)
def infer_periods_per_year(timestamps: Sequence[str]) -> Optional[float]:
    """How many observations per year this series actually carries.

    NOT derivable from the run's ``bar_size``, and assuming otherwise is a live
    bug this function exists to fix. Equity curves are decimated on write
    (default: one point per day), so a 1-minute backtest stores ~251 DAILY
    points while its ``bar_size`` still says '1 min'. Annualising those daily
    returns with the 1-minute factor of 98,280 overstates every Sharpe built
    from the blob by √(98280/252) ≈ 19.7×.

    Counting observations against the calendar span they cover is immune to
    that, because it measures the series in front of you rather than the
    metadata describing how it was generated. It also handles a decimation
    setting that changed between runs, which no bar-size lookup can.
    """
    import datetime as _dt

    parsed = []
    for t in timestamps:
        try:
            parsed.append(_dt.datetime.fromisoformat(str(t)))
        except (TypeError, ValueError):
            continue
    if len(parsed) < 3:
        return None
    parsed.sort()
    span_days = (parsed[-1] - parsed[0]).total_seconds() / 86400.0
    if span_days <= 0:
        return None
    return float(len(parsed) / (span_days / 365.25))


def align_equity_curves(
    curves: Sequence[Sequence[dict]],
) -> Optional[Tuple[np.ndarray, int, int]]:
    """Turn stored equity curves into an aligned (T × N) matrix of returns.

    Each curve is the parsed ``equity_curve_json``: a list of
    ``{'timestamp': str, 'value': float}``. Runs in one sweep cover the same
    window but need not have identical index length — a strategy that never
    traded a given symbol can produce a shorter curve, and daily decimation
    can land differently. So this INTERSECTS on timestamp rather than
    truncating to the shortest, because truncation would silently compare
    different periods across columns and CSCV would then be measuring the
    misalignment.

    Alignment is on the DATE, not the full timestamp. Two reasons, both found
    the hard way on real sweeps: decimation appends the curve's true final
    point, so each run ends on its own last intraday minute and a
    whole-timestamp match loses the last row of every column; and pooling
    across venues compares an ASX curve stamped +10:00 against a US one
    stamped -04:00, whose instants never coincide at all.

    A curve that does not cover most of the common window is DROPPED rather
    than allowed to shrink the intersection. Without that rule, one short or
    foreign-calendar run silently truncates every other column: a pooled ORB
    sweep of 495 runs collapsed from 250 usable rows to 12, and still returned
    a confident-looking PBO of 20%. Losing a column is visible in the returned
    count; losing 95% of the rows is not.

    Returns ``(matrix, n_used, n_dropped)``, or ``None`` if fewer than 2
    curves survive or the common index is too short to split.
    """
    parsed: List[dict] = []
    for curve in curves:
        if not curve:
            continue
        points: dict = {}
        for p in curve:
            try:
                # Date only — see the docstring. Last-write-wins within a day,
                # which for a decimated curve is the day's closing value.
                points[str(p['timestamp'])[:10]] = float(p['value'])
            except (KeyError, TypeError, ValueError):
                continue
        if len(points) >= 3:
            parsed.append(points)
    if len(parsed) < 2:
        return None

    # The window most runs actually share, taken as the modal index rather than
    # the intersection of all — so a single outlier cannot define it.
    from collections import Counter
    day_counts: Counter = Counter()
    for p in parsed:
        day_counts.update(p.keys())
    quorum = max(2, int(0.8 * len(parsed)))
    index = sorted(d for d, c in day_counts.items() if c >= quorum)
    if len(index) < 5:
        return None

    kept = [p for p in parsed if sum(1 for d in index if d in p) >= 0.9 * len(index)]
    n_dropped = len(parsed) - len(kept)
    if len(kept) < 2:
        return None

    # A kept curve may still be missing up to 10% of the index (a halted day,
    # a venue holiday the others traded). Carry the last known equity forward
    # rather than dropping the row for everyone: a missing mark is "no change
    # recorded", and forcing an exact match here would reintroduce the
    # intersection collapse this function exists to prevent.
    rows: List[List[float]] = []
    for p in kept:
        series: List[float] = []
        last = None
        for d in index:
            last = p.get(d, last)
            series.append(float(last) if last is not None else float('nan'))
        rows.append(series)

    values = np.array(rows, dtype=float).T
    # Leading gaps (a curve that starts after the index does) have no prior
    # value to carry; backfill from the first real observation.
    for j in range(values.shape[1]):
        col = values[:, j]
        good = np.flatnonzero(np.isfinite(col))
        if len(good):
            col[:good[0]] = col[good[0]]

    with np.errstate(divide='ignore', invalid='ignore'):
        rets = np.diff(values, axis=0) / values[:-1, :]
    return np.nan_to_num(rets, nan=0.0, posinf=0.0, neginf=0.0), len(kept), n_dropped
