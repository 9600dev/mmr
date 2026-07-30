"""Walk-forward selection — choose parameters on the past, report on the future.

WHY THIS EXISTS
    A sweep evaluates every configuration over the whole period and you keep
    the best. Selection and reporting then happen on the SAME data, so the
    number you write down is the maximum of N draws and not something you
    could ever have earned. Measured on this codebase's own history
    (2026-07-28), that procedure had a Probability of Backtest Overfitting of
    58-70% for the ORB family across four independent sweeps: the in-sample
    winner landed in the BOTTOM half out of sample more often than not.

    PBO and the deflated Sharpe DETECT that. Walk-forward PREVENTS it. At every
    step the parameters are chosen using only data that precedes the window
    they are judged on, so the stitched result is out-of-sample by
    construction. There is nothing left to deflate, because nothing was
    selected on the data being reported: the trial count is one — the
    procedure — however many cells it considered along the way.

WHAT IS ACTUALLY BEING MEASURED
    Not "how good is the best configuration", which is the question a sweep
    answers and the question that has no honest answer. Instead: "how good is
    the RULE 'refit on the last N days, trade the next M'". That rule is
    deployable. A specific cell chosen with hindsight is not.

    So a walk-forward result can be much worse than the sweep's headline and
    still be the more useful number — it is the one that survives contact with
    not knowing the future.

DESIGN
    The fold planner below is pure and works in integer day offsets rather than
    dates. That is deliberate: the property that matters (a fold's test window
    never begins before its training window ends) is arithmetic, and stating it
    over ints makes it symbolically checkable by CrossHair instead of merely
    sampled. Timezones, calendars and session boundaries are a separate concern
    handled at the edge, where getting them wrong costs a misaligned window
    rather than silent lookahead.
"""

from __future__ import annotations

import datetime as dt

from typing import Any, Dict, List, NamedTuple, Optional, Sequence

import deal


class FoldOffsets(NamedTuple):
    """One train/test split, in whole days from the start of the period.

    Half-open throughout: ``[train_start, train_end)`` and
    ``[test_start, test_end)``. Half-open is what makes ``test_start ==
    train_end`` mean "no overlap" rather than "shares one day", and a
    one-day overlap is exactly the leak that would be invisible in a summary.
    """
    index: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int


class Fold(NamedTuple):
    """A FoldOffsets resolved onto the calendar.

    ``train_data_start`` and ``test_data_start`` are where DATA LOADING should
    begin, which is earlier than the window being measured whenever the
    strategy needs history to warm up. They exist because getting this wrong
    produced a silently empty result: a 252-day lookback in a 252-day test
    window never reaches its first tradeable index, and three of seven folds
    came back at exactly 0.00% having made no trades at all. The fold planner
    was correct; the CALLER had to remember, and did not.

    The measured window is unchanged by warm-up. That separation is the whole
    point - a warm-up that shifted ``test_start`` would quietly change what is
    being reported on.
    """
    index: int
    train_start: dt.date
    train_end: dt.date
    test_start: dt.date
    test_end: dt.date
    train_data_start: dt.date
    test_data_start: dt.date


class FoldResult(NamedTuple):
    """What one fold decided, and what that decision then earned."""
    fold: Fold
    chosen: Optional[Dict[str, Any]]   # the winning parameter cell, or None
    train_score: Optional[float]       # its score in-sample (why it was picked)
    test_metrics: Dict[str, Any]       # how it actually did out-of-sample


@deal.pure
@deal.pre(lambda total_days, train_days, test_days, anchored=False:
          total_days >= 0 and train_days > 0 and test_days > 0)
@deal.ensure(
    lambda _: all(f.test_start >= f.train_end for f in _.result),
    message='LEAKAGE: a fold would be tested on data it was fitted on')
@deal.ensure(
    lambda _: all(f.train_start < f.train_end and f.test_start < f.test_end
                  for f in _.result),
    message='every window must be non-empty')
@deal.ensure(
    lambda _: all(f.test_end <= _.total_days for f in _.result),
    message='no fold may extend past the data')
@deal.ensure(
    lambda _: all(a.test_end <= b.test_start
                  for a, b in zip(_.result, _.result[1:])),
    message='test windows must not overlap — each day is judged at most once')
def plan_fold_offsets(
    total_days: int,
    train_days: int,
    test_days: int,
    anchored: bool = False,
) -> List[FoldOffsets]:
    """Split ``total_days`` into successive train/test folds.

    ``anchored=False`` (default) rolls a fixed-length training window forward,
    which lets the procedure forget regimes that have ended. ``anchored=True``
    expands the window from a fixed start, using all history to date — more
    data, at the cost of averaging over regimes that may no longer apply.
    Neither is universally right, which is why it is a parameter rather than a
    decision baked in here.

    The contracts above are the whole point of this function. The first one —
    ``test_start >= train_end`` — is the property whose violation would make
    every downstream number a lie while changing nothing visible about the
    output, so it is asserted at runtime rather than trusted to review.

    Returns an empty list when the period cannot hold even one fold. That is
    not an error: it is the honest answer to "what can you tell me
    out-of-sample from 40 days with a 60-day training window", and inventing a
    fold by shrinking the window would answer a different question than the
    caller asked.
    """
    folds: List[FoldOffsets] = []
    i = 0
    while True:
        if anchored:
            train_start = 0
            train_end = train_days + i * test_days
        else:
            train_start = i * test_days
            train_end = train_start + train_days
        test_start = train_end
        test_end = test_start + test_days
        if test_end > total_days:
            break
        folds.append(FoldOffsets(i, train_start, train_end,
                                 test_start, test_end))
        i += 1
    return folds


def resolve_folds(start: dt.date, offsets: Sequence[FoldOffsets],
                  warmup_days: int = 0) -> List[Fold]:
    """Put integer offsets onto the calendar, with optional warm-up.

    ``warmup_days`` is how much history the strategy needs before it can act -
    its lookback, in CALENDAR days, so roughly 1.45x a trading-day count. It
    moves only where data loading starts. The train and test windows themselves
    are untouched, so a result measured with warm-up covers the same period as
    one measured without it, and the two are comparable.

    Deliberately trivial otherwise: all the logic that can be wrong lives in
    the pure planner above.
    """
    if warmup_days < 0:
        raise ValueError(f'warmup_days must be >= 0, got {warmup_days}')

    def d(n: int) -> dt.date:
        return start + dt.timedelta(days=n)
    warm = dt.timedelta(days=warmup_days)
    return [Fold(f.index, d(f.train_start), d(f.train_end),
                 d(f.test_start), d(f.test_end),
                 d(f.train_start) - warm, d(f.test_start) - warm)
            for f in offsets]


@deal.pure
def selection_stability(results: Sequence[FoldResult]) -> Optional[float]:
    """Fraction of consecutive folds that kept the same parameter cell.

    A diagnostic worth as much as the performance number. If the procedure
    picks a different winner nearly every fold, it has not found a parameter
    that works — it has found noise, and the fact that its out-of-sample
    equity happens to be positive is luck rather than evidence. Conversely a
    procedure that keeps choosing the same cell has located something stable,
    even if the returns are modest.

    Returns None with fewer than two folds, where the question is meaningless.
    """
    picks = [r.chosen for r in results if r.chosen is not None]
    if len(picks) < 2:
        return None
    same = sum(1 for a, b in zip(picks, picks[1:]) if a == b)
    return same / (len(picks) - 1)


def run_walk_forward(
    folds: Sequence[Fold],
    cells: Sequence[Dict[str, Any]],
    run_backtest,
    score,
    higher_is_better: bool = True,
    on_fold=None,
) -> List[FoldResult]:
    """Drive the procedure: fit on each train window, judge on the next test one.

    ``run_backtest(cell, start, end)`` returns a metrics dict or None; ``score``
    maps that dict to a comparable number or None. Both are injected rather
    than imported so this sequencing — the part where a leak would hide — can
    be tested without a Backtester, a store, or any market data.

    The one rule enforced here beyond the planner's arithmetic: the winning
    cell is chosen from TRAIN results only, and the test window is run
    afterwards for that cell alone. Scoring every cell on the test window and
    then reporting the best would be the original sin with extra steps.
    """
    out: List[FoldResult] = []
    for fold in folds:
        scored = []
        for cell in cells:
            # Data from train_data_start so the strategy can warm up; the
            # window being fitted is still [train_start, train_end).
            metrics = run_backtest(cell, fold.train_data_start, fold.train_end)
            scored.append((cell, score(metrics) if metrics else None))
        chosen = pick_best(scored, higher_is_better=higher_is_better)

        train_score = None
        for cell, sc in scored:
            if chosen is not None and cell == chosen:
                train_score = sc
                break

        test_metrics: Dict[str, Any] = {}
        if chosen is not None:
            test_metrics = run_backtest(
                chosen, fold.test_data_start, fold.test_end) or {}

        result = FoldResult(fold, chosen, train_score, test_metrics)
        out.append(result)
        if on_fold is not None:
            on_fold(result)
    return out


@deal.pure
def pick_best(
    scored: Sequence[tuple],
    higher_is_better: bool = True,
) -> Optional[Dict[str, Any]]:
    """Choose a parameter cell from ``(cell, score)`` pairs.

    Cells whose score is missing are DROPPED rather than treated as zero. A
    backtest that failed or produced no trades has not told you the cell is
    mediocre; it has told you nothing, and scoring it as 0.0 would rank it
    above every genuinely losing cell and hand the procedure a configuration
    that was never evaluated.

    Ties resolve to the first cell in the given order, so the caller controls
    determinism by controlling grid order — two runs over the same grid must
    make the same choice or the walk-forward result stops being reproducible.
    """
    usable = [(cell, score) for cell, score in scored if score is not None]
    if not usable:
        return None
    best = usable[0]
    for cell, score in usable[1:]:
        if (score > best[1]) if higher_is_better else (score < best[1]):
            best = (cell, score)
    return dict(best[0]) if isinstance(best[0], dict) else best[0]
