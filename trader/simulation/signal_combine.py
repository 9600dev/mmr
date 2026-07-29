"""Combining weak signals, and stripping the bets nobody chose.

WHY COMBINE
    A single cross-sectional signal here scores IC 0.018, which converts to a
    Sharpe around 0.31 after costs. That is not a failure; it is the normal
    size of a real anomaly in liquid US equities. Books that work stack many
    weak, weakly-correlated signals rather than hunting for one strong one.

    The arithmetic is why. Combining k signals of average IC c with average
    pairwise correlation rho gives approximately

        IC_combined  ~  c * sqrt( k / (1 + (k-1) * rho) )

    Five signals at IC 0.018 with rho = 0.2 land near 0.030 - a ~65% uplift
    for no new data. The correlation term is what does the work, which is why
    ADDING A SIGNAL THAT CORRELATES 0.9 WITH ONE YOU HAVE IS WORTH ALMOST
    NOTHING, and why the diagnostic below reports pairwise correlation
    alongside the combination.

WHY NEUTRALISE
    Equal-weighting within quantiles makes sector, size and beta bets nobody
    chose. A momentum book over 2016-2026 was structurally long technology and
    was paid for that exposure rather than for the signal. Neutralising does
    not add return; it removes variance you were never compensated for, which
    is the same thing to a Sharpe ratio and a very different thing to a
    drawdown.

THE LOOKAHEAD TRAP IN Z-SCORING
    Standardisation must be CROSS-SECTIONAL - across names within one period -
    never across time. A time-series z-score at row t divides by a standard
    deviation computed from the whole sample, including the future, and the
    resulting "signal" knows how volatile the next five years will be. It is
    the same class of error as a misaligned forward return and just as
    invisible: the output is a well-behaved frame of plausible numbers.
"""

from __future__ import annotations

import math

from typing import Any, Dict, Mapping, Optional

import deal


@deal.pure
@deal.post(lambda r: r is None or r >= 0.0)
def combined_ic_estimate(mean_ic: float, n_signals: int,
                         mean_correlation: float) -> Optional[float]:
    """Expected IC of an equal-weighted combination, from the standard formula.

    A planning tool, not a measurement: it says what k signals COULD be worth
    before you build the combination, so effort goes to uncorrelated
    additions rather than to another momentum variant.

    Returns None when the inputs cannot support the estimate. A correlation of
    exactly -1/(k-1) drives the denominator to zero, which is the algebraic
    edge of a perfectly hedged set rather than an infinite IC.
    """
    if n_signals < 1 or not math.isfinite(mean_ic):
        return None
    if not math.isfinite(mean_correlation) or not (-1.0 <= mean_correlation <= 1.0):
        return None
    denom = 1.0 + (n_signals - 1) * mean_correlation
    if denom <= 1e-12:
        return None
    return abs(mean_ic) * math.sqrt(n_signals / denom)


def cross_sectional_zscore(frame: Any, min_names: int = 5) -> Any:
    """Standardise WITHIN each period, across names.

    Never across time - see the module docstring. Rows with fewer than
    ``min_names`` usable values are emptied rather than standardised, because
    a z-score over three names is not a ranking, it is noise with a mean
    subtracted from it.
    """
    import numpy as np

    mean = frame.mean(axis=1)
    std = frame.std(axis=1, ddof=1)
    counts = frame.notna().sum(axis=1)
    z = frame.sub(mean, axis=0).div(std.replace(0.0, np.nan), axis=0)
    z[counts < min_names] = np.nan
    return z


def combine(signals: Mapping[str, Any],
            weights: Optional[Mapping[str, float]] = None,
            min_names: int = 5) -> Any:
    """Weighted sum of cross-sectionally standardised signals.

    Each signal is z-scored within its own period BEFORE weighting, so a
    signal measured in percent and one measured in dollars contribute on the
    same scale. Weighting raw signals instead would let units decide the
    combination.

    A name missing from one signal but present in others keeps its
    contribution from the others rather than dropping out of the combination
    entirely, because a missing input is not a bearish view. The count of
    contributing signals is what the mean divides by.
    """
    import pandas as pd

    if not signals:
        return None
    w = dict(weights) if weights else {k: 1.0 for k in signals}
    total = 0.0
    acc = None
    denom = None
    for name, frame in signals.items():
        wt = float(w.get(name, 0.0))
        if wt == 0.0 or not math.isfinite(wt):
            continue
        z = cross_sectional_zscore(frame, min_names=min_names) * wt
        present = z.notna().astype(float) * abs(wt)
        acc = z.fillna(0.0) if acc is None else acc.add(z.fillna(0.0),
                                                        fill_value=0.0)
        denom = present if denom is None else denom.add(present, fill_value=0.0)
        total += abs(wt)
    if acc is None or denom is None:
        return None
    import numpy as np
    out = acc.div(denom.replace(0.0, np.nan))
    return out


def signal_correlations(signals: Mapping[str, Any]) -> Dict[str, float]:
    """Mean cross-sectional rank correlation between each pair of signals.

    The number that decides whether a new signal is worth adding. Two signals
    correlating 0.9 are one signal; the combination formula's benefit comes
    almost entirely from the correlation term, so this belongs beside any
    claim that a set of signals is diversified.
    """
    import numpy as np
    import pandas as pd

    names = list(signals)
    out: Dict[str, float] = {}
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            fa, fb = signals[a], signals[b]
            idx = fa.index.intersection(fb.index)
            cols = fa.columns.intersection(fb.columns)
            vals = []
            for ts in idx[::5]:          # every 5th period: this is a summary
                pair = pd.concat([fa.loc[ts, cols], fb.loc[ts, cols]],
                                 axis=1).dropna()
                if len(pair) >= 10 and pair.iloc[:, 0].nunique() > 1 \
                        and pair.iloc[:, 1].nunique() > 1:
                    vals.append(pair.iloc[:, 0].corr(pair.iloc[:, 1],
                                                     method='spearman'))
            if vals:
                out[f'{a}|{b}'] = float(np.nanmean(vals))
    return out


def neutralise(signal: Any, groups: Mapping[int, str]) -> Any:
    """Demean the signal within each group, so the book stops betting on it.

    After this, the average signal inside every sector is zero: the strategy
    can still prefer one technology name over another, but it can no longer
    prefer technology. Names with no group are left untouched rather than
    pooled into a synthetic "other" bucket, which would create a group whose
    only shared property is missing metadata.
    """
    import pandas as pd

    if not groups:
        return signal

    def _group(col):
        # A column label that is not a conid has no sector, which is the same
        # situation as a conid with no metadata: leave it alone. Raising here
        # would take down the whole book because two instruments in a
        # 490-name panel happen to be stored under their ticker rather than
        # their conid, which is a store inconsistency and not this function's
        # business.
        try:
            return groups.get(int(col))
        except (TypeError, ValueError):
            return None

    labels = pd.Series({c: _group(c) for c in signal.columns})
    known = labels.dropna()
    if known.empty:
        return signal
    out = signal.copy()
    for label in known.unique():
        cols = [c for c in signal.columns if _group(c) == label]
        if len(cols) < 2:
            continue          # a group of one cannot be demeaned meaningfully
        block = signal[cols]
        out[cols] = block.sub(block.mean(axis=1), axis=0)
    return out
