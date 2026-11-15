"""Is every stored daily bar of one ticker on the same split basis?

``pit_daily_bars`` is written from the vendor's grouped-daily endpoint with
``adjusted=True``: each bar is split-adjusted AS OF THE DAY IT WAS FETCHED, and
days are appended weeks or months apart. A split that executes between two
fetches therefore leaves the bars fetched before it on the old basis and the
bars fetched after it on the new one. The stored series then has a split-sized
jump at the JUNCTION between the two fetches - not on the split's execution
date, and recorded nowhere (MNST, 1:2 executed 2026-08-11: stored close 97.74
on 2026-07-28 and 48.62 on 2026-07-29).

The junction is not recorded, so it is MEASURED: the vendor's adjusted daily
series for the ticker, fetched now (the REFERENCE), is on one basis throughout,
and ``stored close / reference close`` is, day by day,

    1                                   the day is on the current basis
    the product of the LAST m splits    the day was fetched before those m
                                        splits executed

and nothing else. That is the whole candidate set, deliberately: a day fetched
at time F lacks exactly the splits executed after F, which is a suffix of the
ticker's split list. Allowing any SUBSET instead would let a ticker with 17
reverse splits "explain" almost any wrong price (131,072 products), and such
tickers are where the wrong prices are (WHLR: eleven stored days are off by
1/72, which 39 of the 65,536 subsets of its later splits produce and no suffix
does).

Measured 2026-11-15 on 395,383 stored days of 1,367 tickers with a split: the
quotient is within 1e-5 of a candidate on 99.6% of days and the largest
disagreement that is still the same bar is 0.00096 (a close the vendor revised
by 0.1%). ``TOLERANCE`` is twice that.

What is refused rather than repaired: a day whose quotient is no candidate
(the stored prices carry a split the vendor has since withdrawn; a second
split on one date that the split table cannot hold; an older split missing
while later ones are applied; a restructuring that is not a split), a stored
day the reference has no bar for, and a split whose presence in the reference
cannot be established. One wrong day is not a reason to leave a real stale
segment in place, so an ISOLATED day that is clearly on its neighbours' basis
and off by an ordinary amount is given that basis and reported as a price
discrepancy - its difference from the reference is kept exactly as it was,
since no split explains it.

Two things the reference cannot be taken at its word for:

A SPLIT IT DOES NOT CARRY (PSTV 25:1 reverse, 2026-04-02: the vendor's own
adjusted series goes from 0.1558 to 3.56 overnight). The quotient cannot show
it; ``split_reflected_in_prices`` on the reference finds it, and - because a
real move of that size on an execution date looks the same (GCTK doubled on
the day of its 2:1 reverse, 2026-10-26, in a series that DOES apply the
split) - it is believed only when the vendor's UNADJUSTED series shows that
the adjusted one does not apply the split. Such a split is missing from the
reference on every day before it, and from a stored day either likewise (the
ratio is then applied here) or not at all (the stored bar was fetched while
the vendor still applied it, as PSTV's were - nothing to do).

A SPLIT EXECUTED AFTER ITS LAST BAR (NLTX 4:1 reverse on 2023-12-19, the day
after the ticker's last session). There is no later price to show whether the
series applies it, so both readings are tried and the ticker is refused if
they ask for different changes.

Everything here is a pure function of its arguments.
"""

from __future__ import annotations

import bisect
import datetime as dt
import itertools
import math

from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from trader.data.share_basis import SplitEvent, split_reflected_in_prices

# |log(stored / reference) - log(candidate)| at or below this is "the same".
TOLERANCE = 0.002
# An isolated day may differ from its basis by this much (in logs: about 10%)
# and still be recognisably on it ...
PRICE_NOTE_MAX = 0.10
# ... provided no other candidate is nearer than this. 0.2 is the size of split
# share_basis.split_reflected_in_prices will tell from an ordinary day's move.
PRICE_NOTE_SEPARATION = 0.20
# More splits than this missing from the reference itself is not a data error
# to repair by arithmetic; more than this after its last bar are not all tried.
MAX_REFERENCE_GAPS = 4
MAX_TRAILING_SPLITS = 4
# The diagnosis of a refusal tries 3**n combinations.
_HINT_MAX_SPLITS = 8

CLEAN = 'clean'
ADJUST = 'adjust'
REFUSED = 'refused'
NEEDS_UNADJUSTED = 'needs_unadjusted'

_DISTINCT = 1e-9          # two candidate products further apart than this differ

_EXPLAINED, _NOTE, _UNEXPLAINED, _NO_REFERENCE, _BAD_CLOSE = range(5)


@dataclass(frozen=True)
class Segment:
    """Consecutive stored days that lack the same splits.

    ``factor`` is ``stored price / price on the current basis``: prices are
    DIVIDED by it and volume MULTIPLIED by it. ``vendor_lacks`` are the splits
    of ``splits`` that the vendor's own adjusted series does not carry either.
    ``first_close`` is the stored close of ``first_day`` when the plan was made,
    so the write can tell the rows are still the ones that were examined.
    """
    first_day: dt.date
    last_day: dt.date
    rows: int
    factor: float
    splits: Tuple[SplitEvent, ...]
    vendor_lacks: Tuple[SplitEvent, ...]
    first_close: float


@dataclass(frozen=True)
class PriceNote:
    """One isolated day whose close differs from the reference by an amount no
    split explains. ``residual`` is ``log(stored / (reference x factor))``."""
    day: dt.date
    stored_close: float
    reference_close: float
    factor: float
    residual: float


@dataclass(frozen=True)
class TickerPlan:
    ticker: str
    verdict: str
    stored_days: int
    segments: Tuple[Segment, ...] = ()
    price_notes: Tuple[PriceNote, ...] = ()
    reason: str = ''
    detail: str = ''
    # execution dates of splits that some stored day is not verified to reflect
    unverified_splits: Tuple[dt.date, ...] = ()
    # splits whose presence in the reference needs the unadjusted series
    needs_unadjusted: Tuple[SplitEvent, ...] = ()

    @property
    def rows_to_change(self) -> int:
        return sum(s.rows for s in self.segments)


@dataclass(frozen=True)
class AppliedAdjustment:
    """One row of the audit log, as far as the double-application guard needs it."""
    first_day: dt.date
    last_day: dt.date
    splits: Tuple[SplitEvent, ...]
    applied_at: str = ''


@dataclass(frozen=True)
class BasisDecision:
    adjusted_through: Optional[dt.date]
    explanation: str


@dataclass(frozen=True)
class _Days:
    """One reading of every stored day, in day order."""
    states: Tuple[int, ...]
    missing: Tuple[Tuple[SplitEvent, ...], ...]
    residuals: Tuple[float, ...]
    log_quotients: Tuple[float, ...]

    @property
    def bad(self) -> List[int]:
        return [i for i, s in enumerate(self.states) if s in (_UNEXPLAINED, _NO_REFERENCE, _BAD_CLOSE)]


def _usable(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(value) and value > 0


def relevant_events(events: Sequence[SplitEvent], first_stored_day: dt.date,
                    run_date: dt.date) -> List[SplitEvent]:
    """The splits that can be missing from a stored bar, in date order.

    One executed on or before the first stored day is in every stored price
    already (it is in the raw price). One dated after ``run_date`` has not
    happened: an announced split is in no price.
    """
    return sorted(e for e in events if first_stored_day < e[0] <= run_date)


def describe_split(event: SplitEvent) -> str:
    day, ratio = event
    if ratio >= 1:
        text = f'1:{ratio:g}'
    else:
        text = f'{1 / ratio:g}:1 reverse'
    return f'{day.isoformat()} {text}'


def _product(events: Sequence[SplitEvent]) -> float:
    out = 1.0
    for _, ratio in events:
        out *= ratio
    return out


def _diagnosis(log_quotient: float, later: Sequence[SplitEvent], tolerance: float) -> str:
    """Words for a quotient that IS a product of this ticker's splits, only not
    of its most recent ones: the fewest splits, each missing from the stored
    price or carried by it alone, that produce it. A diagnosis for the person
    reading the refusal, never a licence to change anything."""
    if not later or len(later) > _HINT_MAX_SPLITS:
        return ''
    logs = [math.log(r) for _, r in later]
    best: Optional[Tuple[int, Tuple[int, ...]]] = None
    for exponents in itertools.product((0, 1, -1), repeat=len(later)):
        cost = sum(abs(e) for e in exponents)
        if cost == 0 or (best is not None and cost >= best[0]):
            continue
        if abs(log_quotient - sum(e * value for e, value in zip(exponents, logs))) <= tolerance:
            best = (cost, exponents)
    if best is None:
        return ''
    lack = [describe_split(ev) for ev, e in zip(later, best[1]) if e == 1]
    carry = [describe_split(ev) for ev, e in zip(later, best[1]) if e == -1]
    parts = []
    if carry:
        parts.append('carry ' + ', '.join(carry) + ', which the vendor\'s adjusted series does not')
    if lack:
        parts.append('lack ' + ', '.join(lack) + ' while carrying a later split')
    return 'it would be explained if the stored prices ' + ' and '.join(parts)


def _refused(ticker: str, stored_days: int, reason: str, detail: str,
             unverified: Sequence[dt.date]) -> TickerPlan:
    return TickerPlan(ticker=ticker, verdict=REFUSED, stored_days=stored_days, reason=reason,
                      detail=detail, unverified_splits=tuple(sorted(set(unverified))))


def _classify(days: Sequence[Tuple[dt.date, float]], ref: Mapping[dt.date, float],
              lacks: Sequence[SplitEvent], core: Sequence[SplitEvent], tolerance: float) -> _Days:
    """Read every stored day against ``core`` (the splits the reference carries:
    a stored day lacks the last m of them) and ``lacks`` (the splits the
    reference does not carry: a stored day lacks each of them or not)."""
    core_dates = [d for d, _ in core]
    suffix_logs = [0.0]
    for _, ratio in reversed(core):
        suffix_logs.append(suffix_logs[-1] + math.log(ratio))

    states: List[int] = []
    missing: List[Tuple[SplitEvent, ...]] = []
    residuals: List[float] = []
    log_quotients: List[float] = []
    for day, close in days:
        reference_close = ref.get(day)
        if not _usable(close) or reference_close is None:
            states.append(_BAD_CLOSE if not _usable(close) else _NO_REFERENCE)
            missing.append(())
            residuals.append(float('nan'))
            log_quotients.append(float('nan'))
            continue
        gaps = [e for e in lacks if e[0] > day]
        # stored / TRUE price: the true price is reference / (ratios it lacks)
        log_q = math.log(float(close) / reference_close) + sum(math.log(r) for _, r in gaps)
        n_later = len(core) - bisect.bisect_right(core_dates, day)
        # Of the candidates within tolerance, the one that lacks the FEWEST
        # splits: "nothing is missing" wins whenever it fits. Taking the
        # nearest instead rescaled SLG by 0.999997 on 18 days, for a 1:1.0306
        # split and its reversal a month later whose product is not quite 1.
        # With none within tolerance, the nearest (for the one-wrong-day rule).
        best: Optional[Tuple[bool, float, float, float, Tuple[SplitEvent, ...]]] = None
        values: List[float] = []
        for m in range(0, n_later + 1):
            for k in range(0, len(gaps) + 1):
                for chosen in itertools.combinations(gaps, k):
                    value = suffix_logs[m] + sum(math.log(r) for _, r in chosen)
                    values.append(value)
                    distance = abs(log_q - value)
                    fits = distance <= tolerance
                    key = (not fits, float(m + k) if fits else distance, distance)
                    if best is None or key < best[:3]:
                        best = (key[0], key[1], distance, value,
                                tuple(sorted(tuple(core[len(core) - m:]) + chosen)))
        assert best is not None
        _, _, distance, best_value, best_missing = best
        runner_up = min((abs(log_q - v) for v in values if abs(v - best_value) > _DISTINCT), default=math.inf)
        if distance <= tolerance:
            states.append(_EXPLAINED)
        elif distance <= PRICE_NOTE_MAX and runner_up >= PRICE_NOTE_SEPARATION:
            states.append(_NOTE)          # provisional: must also be isolated
        else:
            states.append(_UNEXPLAINED)
        missing.append(best_missing)
        residuals.append(log_q - best_value)
        log_quotients.append(log_q)

    # An off day counts as "on its basis" only between explained neighbours.
    n_days = len(days)
    provisional = list(states)
    for i, state in enumerate(provisional):
        if state != _NOTE:
            continue
        neighbours = [j for j in (i - 1, i + 1) if 0 <= j < n_days]
        isolated = bool(neighbours) and all(provisional[j] == _EXPLAINED for j in neighbours)
        # the same fetch basis as the day before (which lacks at least as much)
        # or as the day after (which lacks at most as much)
        same_basis = (
            (i > 0 and tuple(e for e in missing[i - 1] if e[0] > days[i][0]) == missing[i])
            or (i + 1 < n_days and tuple(e for e in missing[i] if e[0] > days[i + 1][0]) == missing[i + 1]))
        if not (isolated and same_basis):
            states[i] = _UNEXPLAINED
    return _Days(tuple(states), tuple(missing), tuple(residuals), tuple(log_quotients))


def _segments(days: Sequence[Tuple[dt.date, float]], reading: _Days,
              lacking: Sequence[SplitEvent]) -> Tuple[Segment, ...]:
    out: List[Segment] = []
    start = 0
    for i in range(1, len(days) + 1):
        if i < len(days) and reading.missing[i] == reading.missing[start]:
            continue
        lacked = reading.missing[start]
        if lacked:
            out.append(Segment(first_day=days[start][0], last_day=days[i - 1][0], rows=i - start,
                               factor=_product(lacked), splits=lacked,
                               vendor_lacks=tuple(e for e in lacked if e in lacking),
                               first_close=float(days[start][1])))
        start = i
    return tuple(out)


def plan_ticker(ticker: str,
                bars: Sequence[Tuple[dt.date, float]],
                events: Sequence[SplitEvent],
                reference: Mapping[dt.date, float],
                unadjusted: Optional[Mapping[dt.date, float]] = None,
                tolerance: float = TOLERANCE) -> TickerPlan:
    """What to do with one ticker's stored bars.

    ``bars``        ``(day, stored close)`` for every stored day of the ticker.
    ``events``      the ticker's splits, ``(execution date, split_to/split_from)``,
                    none dated after the run date (``relevant_events``).
    ``reference``   ``day -> close`` of the vendor's ADJUSTED daily series,
                    fetched now.
    ``unadjusted``  the same from the vendor's UNADJUSTED series. Only needed
                    when the reference moves by a split's size on its execution
                    date; the verdict is then ``NEEDS_UNADJUSTED`` and the
                    caller fetches it and asks again.

    ``CLEAN``: every stored day is on the current basis. ``ADJUST``: the
    segments say which days to rescale and by what. ``REFUSED``: change
    nothing - ``reason`` / ``detail`` say why.
    """
    days: List[Tuple[dt.date, float]] = sorted(bars)
    n_days = len(days)
    if not days:
        return TickerPlan(ticker=ticker, verdict=CLEAN, stored_days=0)
    splits: List[SplitEvent] = sorted(e for e in events if e[0] > days[0][0])
    if not splits:
        return TickerPlan(ticker=ticker, verdict=CLEAN, stored_days=n_days)
    all_dates = [d for d, _ in splits]

    ref: Dict[dt.date, float] = {d: float(c) for d, c in reference.items() if _usable(c)}
    if not ref:
        return _refused(ticker, n_days, 'no_reference',
                        'the vendor returned no adjusted daily bars for this ticker, so its '
                        f'{n_days} stored days cannot be checked', all_dates)
    ref_days = sorted(ref)

    # -- which splits does the reference itself not carry? --------------------
    lacks: List[SplitEvent] = []
    needs: List[SplitEvent] = []
    for event in splits:
        day, ratio = event
        i = bisect.bisect_left(ref_days, day)
        if i == 0 or i >= len(ref_days):
            continue                      # no session before it, or none since
        before_day, on_day = ref_days[i - 1], ref_days[i]
        if split_reflected_in_prices(ref[before_day], ref[on_day], ratio) is not False:
            continue
        # The reference moves by about 1/ratio on the execution date: either it
        # does not carry the split, or the stock really moved that much. Only
        # the unadjusted series can say which.
        if unadjusted is None:
            needs.append(event)
            continue
        raw_before, raw_on = unadjusted.get(before_day), unadjusted.get(on_day)
        if raw_before is None or raw_on is None or not (_usable(raw_before) and _usable(raw_on)):
            return _refused(ticker, n_days, 'undecidable_split',
                            f'the vendor\'s adjusted series moves by the size of {describe_split(event)} on '
                            f'that date ({ref[before_day]:g} -> {ref[on_day]:g}) and its unadjusted series has '
                            'no bar to show whether the split is applied', all_dates)
        step = math.log((float(raw_before) / ref[before_day]) / (float(raw_on) / ref[on_day]))
        if abs(step - math.log(ratio)) <= tolerance:
            continue                      # applied: the move is the stock's own
        if abs(step) <= tolerance:
            lacks.append(event)           # not applied, and the raw jump is there
            continue
        return _refused(ticker, n_days, 'undecidable_split',
                        f'across {describe_split(event)} the vendor\'s unadjusted/adjusted ratio changes by '
                        f'{math.exp(step):.6g}: neither 1 (split not applied) nor {ratio:g} (applied)', all_dates)
    if needs:
        return TickerPlan(ticker=ticker, verdict=NEEDS_UNADJUSTED, stored_days=n_days,
                          needs_unadjusted=tuple(needs))
    if len(lacks) > MAX_REFERENCE_GAPS:
        return _refused(ticker, n_days, 'undecidable_split',
                        f'the vendor\'s adjusted series is missing {len(lacks)} of this ticker\'s splits '
                        f'({", ".join(describe_split(e) for e in lacks)}); it is not a reference', all_dates)

    # -- read the stored days --------------------------------------------------
    # A split executed after the reference's last bar may or may not be applied
    # by it; nothing in the series can show which. Every reading is tried, the
    # one that takes the vendor at its word (all applied) first.
    settled = [e for e in splits if e not in lacks and e[0] <= ref_days[-1]]
    trailing = [e for e in splits if e[0] > ref_days[-1]]
    if len(trailing) > MAX_TRAILING_SPLITS:
        hypotheses: List[Tuple[SplitEvent, ...]] = [tuple(trailing)]
    else:
        hypotheses = [chosen for k in range(len(trailing), -1, -1)
                      for chosen in itertools.combinations(trailing, k)]
    readings = [_classify(days, ref, lacks, sorted(settled + list(applied)), tolerance)
                for applied in hypotheses]
    explained = [(applied, reading) for applied, reading in zip(hypotheses, readings) if not reading.bad]

    if not explained:
        reading = readings[0]
        bad = reading.bad
        first_unverified = min(i for i, s in enumerate(reading.states)
                               if s not in (_EXPLAINED, _NOTE) or reading.missing[i])
        unverified = [d for d in all_dates if d > days[first_unverified][0]]
        bad_close = [i for i in bad if reading.states[i] == _BAD_CLOSE]
        if bad_close:
            i = bad_close[0]
            return _refused(ticker, n_days, 'bad_stored_close',
                            f'{len(bad_close)} of {n_days} stored days have no usable close '
                            f'(first {days[i][0].isoformat()}: {days[i][1]!r})', unverified)
        no_reference = [i for i in bad if reading.states[i] == _NO_REFERENCE]
        if no_reference:
            i, j = no_reference[0], no_reference[-1]
            return _refused(ticker, n_days, 'reference_gap',
                            f'{len(no_reference)} of {n_days} stored days have no bar in the vendor\'s adjusted '
                            f'series ({days[i][0].isoformat()} .. {days[j][0].isoformat()}; the series covers '
                            f'{ref_days[0].isoformat()} .. {ref_days[-1].isoformat()}), so their basis cannot be '
                            'checked', unverified)
        # the quotient most of the unexplained days share
        counts: Dict[float, List[int]] = {}
        for i in bad:
            counts.setdefault(round(reading.log_quotients[i], 3), []).append(i)
        common = max(counts.values(), key=len)
        i, j, c = bad[0], bad[-1], common[0]
        hint = _diagnosis(reading.log_quotients[c], [e for e in splits if e[0] > days[c][0]], tolerance)
        detail = (f'{len(bad)} of {n_days} stored days differ from the vendor\'s adjusted series by a factor that '
                  f'is not the product of this ticker\'s most recent splits ({days[i][0].isoformat()} .. '
                  f'{days[j][0].isoformat()}; {len(common)} of them by {math.exp(reading.log_quotients[c]):.6g}, '
                  f'e.g. {days[c][0].isoformat()} stored {days[c][1]:g} vs {ref[days[c][0]]:g})')
        if hint:
            detail += '; ' + hint
        return _refused(ticker, n_days, 'unexplained_quotient', detail, unverified)

    plans = [(_segments(days, reading, lacks), reading) for _, reading in explained]

    def changes(segments: Sequence[Segment]) -> Tuple[Tuple[dt.date, dt.date, float], ...]:
        return tuple((s.first_day, s.last_day, round(math.log(s.factor), 9)) for s in segments)

    if any(changes(segments) != changes(plans[0][0]) for segments, _ in plans[1:]):
        return _refused(ticker, n_days, 'undecidable_split',
                        f'{", ".join(describe_split(e) for e in trailing)} executed after the last bar of the '
                        f'vendor\'s adjusted series ({ref_days[-1].isoformat()}), which therefore cannot show '
                        'whether it is applied, and the two readings ask for different changes', all_dates)
    segments, reading = plans[0]
    notes = tuple(PriceNote(day=days[i][0], stored_close=float(days[i][1]), reference_close=ref[days[i][0]],
                            factor=_product(reading.missing[i]), residual=reading.residuals[i])
                  for i, state in enumerate(reading.states) if state == _NOTE)
    return TickerPlan(ticker=ticker, verdict=ADJUST if segments else CLEAN, stored_days=n_days,
                      segments=segments, price_notes=notes)


def already_applied(segment: Segment, prior: Sequence[AppliedAdjustment]) -> Optional[AppliedAdjustment]:
    """The audit row that says this segment's days were already given one of
    its splits, or None.

    A split can be applied to a day once. If the comparison asks for it again,
    something other than the stored prices has changed (the reference, the
    split table, this code) and repeating the division would compound: 2x, 4x,
    8x, one night at a time.
    """
    for entry in prior:
        if entry.last_day < segment.first_day or entry.first_day > segment.last_day:
            continue
        for day, ratio in segment.splits:
            if any(day == d and abs(ratio - r) <= 1e-9 * max(1.0, abs(r)) for d, r in entry.splits):
                return entry
    return None


def refuse_repeat(plan: TickerPlan, prior: Sequence[AppliedAdjustment]) -> TickerPlan:
    """``plan``, or a refusal if any of its segments repeats an applied one."""
    for segment in plan.segments:
        hit = already_applied(segment, prior)
        if hit is not None:
            shared = [e for e in segment.splits if any(e[0] == d for d, _ in hit.splits)]
            return _refused(
                plan.ticker, plan.stored_days, 'already_applied',
                f'{segment.first_day.isoformat()} .. {segment.last_day.isoformat()} would be divided by '
                f'{segment.factor:g} for {", ".join(describe_split(e) for e in shared)}, which the audit log '
                f'says was applied to {hit.first_day.isoformat()} .. {hit.last_day.isoformat()}'
                + (f' on {hit.applied_at}' if hit.applied_at else '')
                + '; repeating it would compound', [e[0] for s in plan.segments for e in s.splits])
    return plan


def basis_to_record(claim: dt.date, previous: Optional[dt.date],
                    new_refused_splits: Sequence[dt.date], scope_gap: str = '') -> BasisDecision:
    """The day ``pit_daily_bars`` may be recorded as uniformly adjusted through.

    ``claim``               the day the reference was read on (or the day the
                            split table is current to, if earlier).
    ``previous``            the basis already on record, if any.
    ``new_refused_splits``  execution dates of the unverified splits of tickers
                            refused in THIS run that no earlier run reported.
    ``scope_gap``           non-empty when the run did not look at every split
                            the claim would cover; nothing is recorded then.

    A refusal holds the basis back in the run that first reports it: nothing is
    recorded past the day before the earliest such split. A refusal an earlier
    run already recorded is an exception readers have been told about, and no
    longer holds it back - otherwise one ticker that can never be explained
    (SDRL: a 2018 restructuring that is not a split) would pin the basis at
    2018 while every other ticker is adjusted through today. The basis never
    moves backwards, and a held-back value is never the FIRST one recorded.
    """
    if scope_gap:
        return BasisDecision(None, f'not recorded: {scope_gap}')
    if new_refused_splits:
        earliest = min(new_refused_splits)
        cap = earliest - dt.timedelta(days=1)
        if cap < claim:
            if previous is None:
                return BasisDecision(None, (
                    f'not recorded: a split executed {earliest.isoformat()} was refused for the first time '
                    f'in this run and there is no basis on record yet (it would be held at {cap.isoformat()} '
                    f'while the adjusted tickers are on the basis of {claim.isoformat()})'))
            if cap <= previous:
                return BasisDecision(None, (
                    f'not advanced past {previous.isoformat()}: a split executed {earliest.isoformat()} '
                    'was refused for the first time in this run'))
            return BasisDecision(cap, (
                f'held at {cap.isoformat()}, not {claim.isoformat()}: a split executed '
                f'{earliest.isoformat()} was refused for the first time in this run'))
    if previous is not None and claim <= previous:
        return BasisDecision(None, f'already recorded through {previous.isoformat()}')
    return BasisDecision(claim, f'adjusted through {claim.isoformat()}')
