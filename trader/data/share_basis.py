"""Share counts a market cap can be computed from.

A market capitalisation is ``price x shares``, and it is only a market cap
when the two numbers are on the SAME basis. Three things stood between the
stored fundamentals and that, and until 2026-11-15 nothing in the research
path knew about any of them (docs/RESEARCH_LOG.md, 2026-11-15):

SPLIT BASIS
    ``pit_daily_bars`` prices are split-ADJUSTED (the vendor's ``adjusted=True``
    as of the day they were fetched): NVDA's stored close for 2021-07-01 is
    20.21, a fortieth of the 808 it traded at. A filing's share count is AS
    REPORTED on the day it was filed: 632M for that same date, before the 4:1
    and the 10:1. Multiplied together they understate the market cap by the
    cumulative ratio of every LATER split - 40x here - and every price ratio
    built on it (sales-to-price, book-to-price, 1/marketcap) is overstated by
    the same factor for exactly the stocks that went on to split, which are
    the ones that went up. That is lookahead: it ranks 2019's universe by
    which names would have split by 2026.

    The repair is one multiplication. A filing accepted on day f reports
    shares on the basis of day f (a split between the period end and the
    filing is already in the filing - ASC 260 requires it), so

        shares_on_price_basis = shares_reported x K(f),
        K(f) = product of split_to / split_from over splits executed after f

    and ``adjusted_price(t) x shares_on_price_basis`` is the true market cap
    at any t at or after f. Only splits the stored prices actually reflect may
    enter K: one executed after the last stored day is not in any price yet,
    and the vendor's adjusted series is occasionally missing one (the price
    then still shows the raw jump on the execution day).

FISCAL-YEAR-END ROWS
    The retired statements endpoint derived a fiscal Q4 by subtraction from
    the annual figures. That is right for a flow (revenue for the year minus
    nine months) and meaningless for a weighted-average share count: what it
    stored is ``annual average - mean(Q1..Q3)``, a small number of either
    sign (AAPL FY2025: -47,029,000). Negative for a company buying back
    stock, so the name dropped out of coverage for a quarter; small and
    POSITIVE for a company issuing stock, so its market cap collapsed and it
    went to the top of every price ratio. Those values are not share counts
    and are never used; the count known at a 10-K is the last 10-Q's.

UNITS
    About 1% of the quarterly share counts are off by a power of a thousand
    (MCD 2024-03-31: 725.9, reported in millions). ``plausible_share_counts``
    is a causal filter for that: it never looks at a later filing.

Everything here is a pure function of its arguments.
"""

from __future__ import annotations

import datetime as dt
import math

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

SplitEvent = Tuple[dt.date, float]          # (execution date, split_to / split_from)

# A level is "the same level" within this factor. Share counts move a few
# percent a quarter; a merger can double one; a unit error is a factor of 1000.
PLAUSIBLE_FACTOR = 3.0
# How many accepted counts the reference median is taken over.
REFERENCE_FILINGS = 4


def _as_date(value: Any) -> Optional[dt.date]:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def split_events(splits: Iterable[Mapping[str, Any]]) -> Dict[str, List[SplitEvent]]:
    """``ticker -> [(execution date, ratio), ...]`` in date order.

    ``ratio`` is ``split_to / split_from``: 4.0 for a 4:1 split, 0.1 for a
    1-for-10 reverse split. A row that is not a split - no ticker, no date, a
    ratio that is not a positive finite number, or exactly 1 - is dropped: it
    cannot change a share basis, and guessing what it meant could.
    """
    out: Dict[str, List[SplitEvent]] = {}
    for row in splits:
        ticker = str(row.get('ticker') or '').strip()
        day = _as_date(row.get('execution_date'))
        split_to, split_from = row.get('split_to'), row.get('split_from')
        if split_to is None or split_from is None:
            continue
        try:
            ratio = float(split_to) / float(split_from)
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        if not ticker or day is None or not math.isfinite(ratio) or ratio <= 0 or ratio == 1.0:
            continue
        out.setdefault(ticker, []).append((day, ratio))
    for events in out.values():
        events.sort()
    return out


def split_factor_after(events: Sequence[SplitEvent], filed_on: dt.date,
                       through: Optional[dt.date] = None) -> float:
    """K: the product of the ratios of splits executed AFTER ``filed_on`` and
    no later than ``through`` (None = no upper bound).

    Strictly after: a split executed on the filing day is already in the
    filing. ``through`` is the last day the prices are adjusted to; a split
    after it is in no stored price and must not be in the share count either.
    """
    factor = 1.0
    for day, ratio in events:
        if day > filed_on and (through is None or day <= through):
            factor *= ratio
    return factor


def split_reflected_in_prices(before: Optional[float], on: Optional[float], ratio: float) -> Optional[bool]:
    """Do stored prices already carry this split?

    ``before`` / ``on`` are the stored closes of the last session before the
    execution date and of the execution date. In an ADJUSTED series the two
    are an ordinary day apart; in a series the split was never applied to,
    the price moves by ``1 / ratio`` overnight (a 2:1 split halves it). The
    answer is whichever of those the observed move is nearer to, in logs.

    None when it cannot be told: a price missing or not positive, or a ratio
    so close to 1 (a 1.2% stock dividend) that an ordinary day's move looks
    like either.
    """
    try:
        if before is None or on is None:
            return None
        before, on, ratio = float(before), float(on), float(ratio)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(before) and math.isfinite(on) and math.isfinite(ratio)):
        return None
    if before <= 0 or on <= 0 or ratio <= 0 or abs(math.log(ratio)) < 0.2:
        return None
    move = math.log(on / before)
    return abs(move) <= abs(move + math.log(ratio))


def plausible_share_counts(counts: Sequence[Optional[float]],
                           factor: float = PLAUSIBLE_FACTOR,
                           reference: int = REFERENCE_FILINGS) -> List[Optional[float]]:
    """Each count in filing order, or None where it cannot be believed.

    CAUSAL: the verdict on a count uses only the counts before it, so the
    answer for a filing never changes when later filings arrive (an as-of
    read must not depend on the future).

    A count is kept when it is within ``factor`` of the median of the last
    ``reference`` kept counts. One that is not is held back - it may be a
    unit error (a count in millions among counts in units) or it may be a
    real change of level (a merger, the first correct figure after a run of
    wrong ones). The NEXT count decides: if it agrees with the held one, the
    level has moved and it is kept (the held one stays rejected - at the time
    it could not be told apart from an error); if it agrees with the old
    level, the held one was the error.

    A count that is None, not finite or not positive is never a share count.
    The first usable count has nothing to be compared with and is kept; if it
    was the error, the second and third correct it by the rule above.
    """
    kept: List[float] = []
    held: Optional[float] = None
    out: List[Optional[float]] = []
    for raw in counts:
        try:
            value = float(raw) if raw is not None else float('nan')
        except (TypeError, ValueError):
            value = float('nan')
        if not math.isfinite(value) or value <= 0:
            out.append(None)
            continue
        if not kept:
            kept.append(value)
            out.append(value)
            continue
        recent = sorted(kept[-reference:])
        middle = len(recent) // 2
        median = recent[middle] if len(recent) % 2 else (recent[middle - 1] + recent[middle]) / 2.0
        if median / factor <= value <= median * factor:
            kept.append(value)
            held = None
            out.append(value)
        elif held is not None and held / factor <= value <= held * factor:
            kept = [value]              # the level moved; the old one is history
            held = None
            out.append(value)
        else:
            held = value
            out.append(None)
    return out


def is_late_filing_notice(form_type: Any) -> bool:
    """``NT 10-K`` / ``NT 10-Q``: a notification that the report will be LATE.

    It names the period and carries no statements. A row dated to one says
    the numbers were public before they were filed - by days or weeks, for
    exactly the companies with something to delay.
    """
    return str(form_type or '').strip().upper().startswith('NT')


def is_annual_report(form_type: Any) -> bool:
    """``10-K`` and its amendments (not a late-filing notice of one)."""
    return str(form_type or '').strip().upper().startswith('10-K')
