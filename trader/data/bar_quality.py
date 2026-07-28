"""Pure quality rules for a stored OHLCV series.

WHY THIS EXISTS
    Every gate in this system verifies CODE against a spec. Nothing verified
    the DATA. The order path is mutation-tested, contract-checked and
    symbolically verified, and it is fed by strategies fitted to bars nobody
    had audited. On 2026-07-28 the refresh pipeline was found failing silently
    in two different ways on the same day, which is a poor prior for the bars
    it produced.

    A one-off audit is worth little: data changes nightly. So the rules live
    here as PURE PREDICATES over a series, which means the same machinery that
    guards the trading kernel guards them (deal contracts, Hypothesis
    properties, CrossHair, mutation testing), and the CLI that runs them can be
    cron'd and can fail loudly.

WHAT IS AND IS NOT IN SCOPE
    In scope: statements that are true of any correct OHLCV series regardless
    of venue, instrument or source. A violation is a defect in the data,
    upstream of any interpretation.

    NOT in scope: anything needing a calendar, a corporate-actions feed or a
    second source. "This day is missing" and "this gap is an unadjusted split"
    are real problems and are deliberately not decided here, because deciding
    them needs inputs this module does not take. Session coverage IS reported,
    but as a measurement, not a verdict.

DESIGN NOTE
    Findings are DATA, not exceptions. A quality run over a whole universe must
    report everything wrong rather than stopping at the first problem, and the
    caller decides what is fatal. The severity split exists for the same
    reason: `error` means the bar cannot be true of any real market, `warn`
    means it is suspicious and may be legitimate.
"""

from __future__ import annotations

import math

from typing import Any, Dict, List, NamedTuple, Optional, Sequence

import deal


class Finding(NamedTuple):
    """One thing wrong with a series, addressed to whoever must fix it."""
    rule: str                 # stable identifier, safe to baseline against
    severity: str             # 'error' (impossible) | 'warn' (suspicious)
    index: Optional[int]      # position in the series, when it localises
    detail: str               # human-readable, names the offending values


class Bar(NamedTuple):
    """One OHLCV row, as the quality rules see it.

    Deliberately not a DataFrame row: these rules are pure and total, and a
    NamedTuple keeps them checkable by CrossHair and cheap to generate with
    Hypothesis.
    """
    ts: float                 # epoch seconds, UTC
    open: float
    high: float
    low: float
    close: float
    volume: float


def _finite(x: Any) -> bool:
    try:
        return math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


@deal.has()
@deal.pure
@deal.ensure(
    lambda _: all(f.severity in ('error', 'warn') for f in _.result),
    message='every finding must carry a known severity')
def check_bar(bar: Bar, index: int) -> List[Finding]:
    """Rules that a SINGLE bar must satisfy to describe a real market.

    Each of these is impossible in correct data, so all are errors. A high
    below the open is not an unusual market, it is a broken row, and a strategy
    fitted through it has been fitted to something that never happened.
    """
    out: List[Finding] = []
    fields = (('open', bar.open), ('high', bar.high),
              ('low', bar.low), ('close', bar.close))
    missing = [name for name, value in fields if not _finite(value)]

    # A bar with NO prices at all is a PLACEHOLDER, not a corrupt bar. The
    # ingestion path writes one for a date the source had nothing for, and on
    # ASX daily series roughly a fifth of rows are weekend placeholders (their
    # stored date is shifted a day by the venue offset, so they look like
    # Fridays). Reporting those as errors would leave the gate permanently red,
    # and a permanently red gate gets switched off. They are still worth
    # surfacing: they inflate coverage counts and anything that forgets to drop
    # them will compute through a hole.
    #
    # A bar with SOME prices and some missing is different, and always wrong.
    if len(missing) == len(fields):
        out.append(Finding('empty_bar', 'warn', index,
                           'no OHLC values (placeholder for a date the source '
                           'had nothing for)'))
        return out
    if missing:
        out.append(Finding('ohlc_partially_missing', 'error', index,
                           f'missing {sorted(missing)} but not all — a bar with '
                           f'some prices and some holes is corrupt'))
    if not _finite(bar.volume):
        out.append(Finding('volume_not_finite', 'error', index,
                           f'volume={bar.volume!r}'))
    # Everything below compares numbers; a non-finite field already reported
    # would produce noise on top of the real finding.
    if out:
        return out

    for name, value in fields:
        if value <= 0:
            out.append(Finding('non_positive_price', 'error', index,
                               f'{name}={value!r}'))
    if bar.volume < 0:
        out.append(Finding('negative_volume', 'error', index,
                           f'volume={bar.volume!r}'))
    if out:
        return out

    if bar.high < bar.low:
        out.append(Finding('high_below_low', 'error', index,
                           f'high={bar.high!r} < low={bar.low!r}'))
    if bar.high < max(bar.open, bar.close):
        out.append(Finding('high_not_highest', 'error', index,
                           f'high={bar.high!r} < max(open={bar.open!r}, '
                           f'close={bar.close!r})'))
    if bar.low > min(bar.open, bar.close):
        out.append(Finding('low_not_lowest', 'error', index,
                           f'low={bar.low!r} > min(open={bar.open!r}, '
                           f'close={bar.close!r})'))
    return out


@deal.has()
@deal.pure
@deal.ensure(
    lambda _: all(f.severity in ('error', 'warn') for f in _.result),
    message='every finding must carry a known severity')
def check_series(bars: Sequence[Bar]) -> List[Finding]:
    """Rules about the sequence rather than any single bar.

    Ordering and duplication are errors: a backtest walks this series in order
    and a repeated or out-of-order timestamp means it replays or rewinds time,
    which silently changes what every indicator computed.
    """
    out: List[Finding] = []
    for i, bar in enumerate(bars):
        out.extend(check_bar(bar, i))

    seen: Dict[float, int] = {}
    previous: Optional[float] = None
    for i, bar in enumerate(bars):
        if not _finite(bar.ts):
            out.append(Finding('timestamp_not_finite', 'error', i,
                               f'ts={bar.ts!r}'))
            continue
        if previous is not None and bar.ts < previous:
            out.append(Finding('timestamps_out_of_order', 'error', i,
                               f'ts={bar.ts!r} follows {previous!r}'))
        if bar.ts in seen:
            out.append(Finding('duplicate_timestamp', 'error', i,
                               f'ts={bar.ts!r} first seen at index {seen[bar.ts]}'))
        else:
            seen[bar.ts] = i
        previous = bar.ts
    return out


@deal.has()
@deal.pure
def spacing_findings(bars: Sequence[Bar], expected_seconds: float) -> List[Finding]:
    """Gaps that are not a multiple of the bar interval.

    A WARNING, not an error, and the distinction matters. Real series are full
    of legitimate gaps: overnight, weekends, holidays, halts, and (for the
    intraday feeds here) the boundary between one session's last bar and the
    next session's first. Calling those errors would produce thousands of
    findings nobody reads, which is how a quality gate gets switched off.

    What this catches is a gap that is not a whole number of intervals at all,
    e.g. a 90-second step in a 1-minute series. That cannot be a missing bar;
    it means the timestamps themselves are wrong, which no calendar excuses.
    """
    out: List[Finding] = []
    if expected_seconds <= 0 or not _finite(expected_seconds):
        return out
    for i in range(1, len(bars)):
        prev, cur = bars[i - 1].ts, bars[i].ts
        if not (_finite(prev) and _finite(cur)) or cur <= prev:
            continue
        delta = cur - prev
        remainder = delta % expected_seconds
        # Tolerate float dust; anything else is a genuinely fractional step.
        off = min(remainder, expected_seconds - remainder)
        if off > 1e-6:
            out.append(Finding('non_multiple_gap', 'warn', i,
                               f'{delta:g}s step is not a multiple of '
                               f'{expected_seconds:g}s'))
    return out


@deal.has()
@deal.pure
@deal.ensure(
    lambda _: all(f.severity == 'warn' for f in _.result),
    message='a jump is suspicious, never impossible — it must not be an error')
def unexplained_jumps(bars: Sequence[Bar], threshold: float = 0.25) -> List[Finding]:
    """Bar-to-bar close moves beyond ``threshold``, as a fraction.

    A WARNING. The purpose is to surface the signature of an unadjusted
    corporate action: a 2-for-1 split looks like a clean -50% overnight move
    with no unusual volume, and one of those in a symbol's history can invent
    or destroy an entire apparent edge. This module cannot tell a split from a
    genuine crash, because that needs a corporate-actions feed, so it reports
    and does not judge.
    """
    out: List[Finding] = []
    if not (_finite(threshold) and threshold > 0):
        return out
    for i in range(1, len(bars)):
        prev, cur = bars[i - 1].close, bars[i].close
        if not (_finite(prev) and _finite(cur)) or prev <= 0:
            continue
        move = abs(cur - prev) / prev
        if move > threshold:
            out.append(Finding('large_jump', 'warn', i,
                               f'close moved {move:.1%} ({prev:g} -> {cur:g})'))
    return out
