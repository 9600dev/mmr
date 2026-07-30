"""Does this signal rank instruments in a way that predicts forward returns?

WHY THIS EXISTS, AND WHY IT COMES BEFORE A BACKTESTER
    A backtest is an instrument for checking that a KNOWN signal survives costs
    and execution. It is a bad instrument for discovering whether the signal
    exists at all, because it throws away most of the evidence: it trades a
    selected subset of names on a selected subset of days, and the 2026-07
    research got 20-44 trades out of ten months that way — enough to detect a
    per-trade Sharpe above 0.30 and nothing smaller.

    Asking the question directly uses everything. With 500 names over 10 years,
    ranking every name every day against its own forward return is ~1.25M
    observations and ~2,500 independent periods. If the answer is "no
    information", you have learned that in milliseconds and without building a
    portfolio simulator to lose money slowly with.

WHAT IT MEASURES
    * INFORMATION COEFFICIENT — the rank correlation, within each period,
      between the signal and the return that FOLLOWS it. One number per
      period; what matters is the mean and whether it is distinguishable from
      zero across periods. Rank correlation rather than Pearson because a
      signal only has to get the ORDER right; being linearly proportional to
      returns is a much stronger claim nobody needs.

    * QUANTILE SPREAD — bucket by signal, average the forward return in each
      bucket. A real signal is roughly MONOTONE across buckets. A significant
      IC with a non-monotone spread usually means one extreme bucket is doing
      all the work, which is a different and more fragile claim.

    * TURNOVER — how much the ranking changes between periods. Costs scale
      with this, and a signal with a good IC that reshuffles completely every
      day can be unprofitable at any realistic spread. Reported alongside so
      the comparison is never IC-in-isolation.

THE ONE PROPERTY THAT MATTERS
    Row *t* of the forward-return frame must depend only on prices at *t* and
    later. Getting that off by one turns the whole exercise into a measurement
    of the present against itself, and — as with walk-forward folds — the
    output looks completely normal when it is wrong: a plausible IC, a
    plausible spread, a number someone builds a strategy on. It is asserted in
    `tests/invariants/test_signal_eval.py` rather than left to review.
"""

from __future__ import annotations

import math

from typing import Any, NamedTuple, Optional

import deal


class ICSummary(NamedTuple):
    """Whether the per-period ICs are distinguishable from zero."""
    n_periods: int
    mean_ic: Optional[float]
    std_ic: Optional[float]
    t_stat: Optional[float]        # Newey-West corrected when horizon > 1
    naive_t_stat: Optional[float]  # uncorrected, kept only to show the gap
    hit_rate: Optional[float]      # fraction of periods with IC > 0
    ir: Optional[float]            # mean / std — the "information ratio" of the signal


@deal.pure
@deal.pre(lambda values, lags: lags >= 0)
def newey_west_variance(values: tuple[float, ...], lags: int) -> Optional[float]:
    """Variance of the mean, corrected for serial correlation (Newey-West).

    REQUIRED whenever the forward horizon exceeds the sampling interval. At
    horizon h sampled daily, consecutive observations share h-1 days of the
    same forward return, so they are nowhere near independent and the naive
    standard error is too small by roughly sqrt(h). That inflates every
    t-statistic by the same factor and turns noise into a discovery.

    This is not hypothetical here: the pure-noise control in
    `scripts/signal_scan.py` scored t = 2.07 at horizon 21 under the naive
    formula — "significant" — while its true t was around 0.45. The control
    existed precisely to catch that, and did.

    Bartlett weights (1 - l/(lags+1)) keep the estimate positive
    semi-definite. Returns None if the corrected variance comes out
    non-positive, which the weighting makes unlikely but not impossible in
    small samples; None means "cannot be computed", never zero.
    """
    n = len(values)
    if n < 2:
        return None
    # The requested bandwidth is CLAMPED to what the sample can support, and
    # the Bartlett weights are computed from the clamped value. Using the
    # requested one instead makes every weight approach 1.0 when the bandwidth
    # exceeds the sample - asking for 99 lags on four observations weights the
    # single computable lag at 0.99 instead of 0.75, over-correcting by 25x on
    # this example. Found by writing a test that assumed the two agreed; they
    # did not, and the requested-bandwidth version was the wrong one.
    effective = min(lags, n - 1)
    mean = sum(values) / n
    dev = [v - mean for v in values]
    gamma0 = sum(d * d for d in dev) / n
    total = gamma0
    for lag in range(1, effective + 1):
        cov = sum(dev[i] * dev[i + lag] for i in range(n - lag)) / n
        total += 2.0 * (1.0 - lag / (effective + 1.0)) * cov
    if not math.isfinite(total) or total <= 0:
        return None
    return total / n


@deal.pure
@deal.pre(lambda mean, std, n: n >= 0)
def ic_t_statistic(mean: float, std: float, n: int) -> Optional[float]:
    """t-statistic of the mean IC against zero.

    Returns None rather than 0.0 when it cannot be computed (n < 2, zero or
    non-finite dispersion). A t-stat of 0.0 reads as "measured, and there is
    nothing there"; None reads as "not measured". Collapsing those is how a
    signal with one usable period gets reported as definitively worthless.
    """
    if n < 2 or not (math.isfinite(mean) and math.isfinite(std)) or std <= 0:
        return None
    # std can be denormal, in which case std/sqrt(n) UNDERFLOWS to exactly
    # zero and the division below raises. CrossHair found it at
    # (6.5e-319, 5e-324, 4) — the same denormal class it caught in
    # order_math. A dispersion that small is not a measurement anyway.
    se = std / math.sqrt(n)
    if not (math.isfinite(se) and se > 0):
        return None
    return mean / se


@deal.pure
@deal.post(lambda r: r is None or r >= 0.0)
def periods_needed_for_significance(mean_ic: float, std_ic: float,
                                    target_t: float = 2.0) -> Optional[float]:
    """How many periods it would take for an IC this size to reach ``target_t``.

    The honesty check on a weak result. An IC of 0.005 with a dispersion of
    0.10 is not "a small edge" — it needs ~1,600 periods to become
    distinguishable from noise, and if you have 250 you have not measured
    anything. Reporting this next to the t-stat stops a promising-looking mean
    from being read as evidence when the sample cannot support it.
    """
    if not (math.isfinite(mean_ic) and math.isfinite(std_ic)):
        return None
    # target_t was unvalidated: a NaN target returned NaN, which is neither a
    # period count nor None and silently violated the postcondition. Found by
    # CrossHair at target_t=nan.
    if not (math.isfinite(target_t) and target_t > 0):
        return None
    if mean_ic == 0.0 or std_ic <= 0:
        return None
    result = (target_t * std_ic / abs(mean_ic)) ** 2
    return result if math.isfinite(result) else None


# --------------------------------------------------------------------------
# Panel-level computations. pandas-backed, so NOT in the CrossHair target list
# (it cannot see through pandas any more than through numpy) — the properties
# are pinned by Hypothesis and by hand-built frames whose answer is known.
# --------------------------------------------------------------------------


def forward_returns(prices, horizon: int = 1):
    """Return from *t* to *t+horizon*, aligned so row *t* holds it.

    ``prices`` is a wide frame: index = period, columns = instrument.

    THE ALIGNMENT IS THE WHOLE POINT. Row *t* must be computable only from
    prices at *t* and later, and must be paired with a signal computed from
    data up to *t*. The last ``horizon`` rows are NaN because their forward
    return has not happened yet — they are deliberately not filled, since a
    filled value would be a fabricated observation at exactly the end of the
    sample, where it has the most leverage over the result.
    """
    if horizon < 1:
        raise ValueError(f'horizon must be >= 1, got {horizon}')
    return prices.shift(-horizon) / prices - 1.0


def period_ic(signal_row, forward_row, method: str = 'spearman') -> Optional[float]:
    """Rank correlation between one period's signal and its forward returns.

    Names missing EITHER side are dropped for that period rather than filled.
    A missing signal is not a neutral signal, and a missing forward return is
    not a zero return — an instrument that stopped trading is absent, not flat.
    Filling either would put a fabricated observation in the middle of the
    cross-section.

    Returns None when fewer than 5 names survive: a rank correlation over 3
    names is noise with a decimal point on it.
    """
    import pandas as pd

    pair = pd.concat([signal_row, forward_row], axis=1).dropna()
    if len(pair) < 5:
        return None
    a, b = pair.iloc[:, 0], pair.iloc[:, 1]
    if a.nunique() < 2 or b.nunique() < 2:
        return None          # no ordering to correlate
    value = a.corr(b, method=method)
    return float(value) if value is not None and math.isfinite(value) else None


def information_coefficient(signal, prices, horizon: int = 1,
                            method: str = 'spearman'):
    """Per-period IC series for a signal against forward returns."""
    import pandas as pd

    fwd = forward_returns(prices, horizon)
    common_idx = signal.index.intersection(fwd.index)
    common_cols = signal.columns.intersection(fwd.columns)
    out = {}
    for ts in common_idx:
        out[ts] = period_ic(signal.loc[ts, common_cols],
                            fwd.loc[ts, common_cols], method=method)
    return pd.Series(out, dtype=float).dropna()


def summarise_ic(ic_series, horizon: int = 1) -> ICSummary:
    """Reduce the per-period ICs to 'is this distinguishable from zero'.

    ``horizon`` drives the Newey-West lag. Pass the SAME horizon the forward
    returns were built with — at horizon h sampled daily, each observation
    overlaps the next h-1, and ignoring that inflates every t-statistic by
    about sqrt(h).
    """
    import numpy as np

    vals = np.asarray(ic_series.dropna().to_numpy(), dtype=float)
    n = len(vals)
    if n == 0:
        return ICSummary(0, None, None, None, None, None, None)
    mean = float(vals.mean())
    std = float(vals.std(ddof=1)) if n > 1 else None
    naive = ic_t_statistic(mean, std, n) if std is not None else None

    t = naive
    if horizon > 1 and n > 2:
        var = newey_west_variance(tuple(float(v) for v in vals), horizon - 1)
        t = (mean / math.sqrt(var)) if var else None
    return ICSummary(
        n_periods=n,
        mean_ic=mean,
        std_ic=std,
        t_stat=t,
        naive_t_stat=naive,
        hit_rate=float((vals > 0).mean()),
        ir=(mean / std) if std and std > 0 else None,
    )


def quantile_returns(signal, prices, horizon: int = 1, n_buckets: int = 5):
    """Mean forward return per signal bucket, per period.

    Returns a frame: index = period, columns = bucket 0..n-1 (0 = lowest
    signal). A real signal is roughly monotone across buckets; a good IC with
    a non-monotone spread usually means one extreme is doing all the work,
    which is a more fragile claim than the IC alone suggests.
    """
    import pandas as pd

    fwd = forward_returns(prices, horizon)
    rows = {}
    for ts in signal.index.intersection(fwd.index):
        pair = pd.concat([signal.loc[ts], fwd.loc[ts]], axis=1).dropna()
        if len(pair) < n_buckets * 2:
            continue          # too thin to bucket meaningfully
        s, f = pair.iloc[:, 0], pair.iloc[:, 1]
        try:
            buckets = pd.qcut(s.rank(method='first'), n_buckets,
                              labels=False, duplicates='drop')
        except ValueError:
            continue
        rows[ts] = f.groupby(buckets).mean()
    return pd.DataFrame(rows).T if rows else pd.DataFrame()


def rank_turnover(signal):
    """Fraction of the cross-section whose rank moved, period to period.

    Costs scale with this. A signal with a strong IC that reshuffles the book
    every period can be unprofitable at any realistic spread, so this is
    reported beside the IC rather than left for the backtest to discover.
    """
    import numpy as np
    import pandas as pd

    ranks = signal.rank(axis=1, pct=True)
    delta = (ranks - ranks.shift(1)).abs()
    arr = delta.to_numpy()
    # The first row is all-NaN by construction (nothing precedes it). nanmean
    # warns on an all-NaN slice rather than erroring, so mask it explicitly
    # instead of letting a legitimate boundary emit noise on every call.
    usable = ~np.isnan(arr).all(axis=1)
    out = np.full(len(arr), np.nan)
    if usable.any():
        out[usable] = np.nanmean(arr[usable], axis=1)
    return pd.Series(out, index=signal.index).dropna()


def evaluate(signal, prices, horizon: int = 1, n_buckets: int = 5) -> dict:
    """One call: IC summary, quantile spread, turnover, and the honesty check.

    ``periods_needed`` is the number of periods this IC would require to reach
    t=2. If it exceeds what you have, the result is not a weak edge — it is an
    unmeasured one, and the distinction decides whether more data would help
    or whether the idea is dead.
    """
    ic = information_coefficient(signal, prices, horizon)
    summary = summarise_ic(ic, horizon=horizon)
    q = quantile_returns(signal, prices, horizon, n_buckets)
    turn = rank_turnover(signal)

    needed = None
    if summary.mean_ic is not None and summary.std_ic:
        needed = periods_needed_for_significance(summary.mean_ic,
                                                 summary.std_ic)
    spread = None
    if len(q.columns) >= 2:
        spread = float(q.iloc[:, -1].mean() - q.iloc[:, 0].mean())

    return {
        'horizon': horizon,
        'n_periods': summary.n_periods,
        'mean_ic': summary.mean_ic,
        'ic_t_stat': summary.t_stat,
        'ic_t_stat_naive': summary.naive_t_stat,
        'ic_hit_rate': summary.hit_rate,
        'ic_ir': summary.ir,
        'periods_needed_for_t2': needed,
        'periods_available': summary.n_periods,
        'top_minus_bottom': spread,
        'bucket_means': (q.mean().to_dict() if len(q) else {}),
        'mean_turnover': float(turn.mean()) if len(turn) else None,
    }
