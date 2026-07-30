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


@deal.pure
@deal.pre(lambda move, split_to, split_from, tolerance=0.15:
          split_to > 0 and split_from > 0 and tolerance > 0)
def explained_by_split(move: float, split_to: float, split_from: float,
                       tolerance: float = 0.15) -> bool:
    """Is this price move what the given split ratio would produce?

    `unexplained_jumps` reports every large move as a WARNING and declines to
    judge, on the stated grounds that telling an unadjusted split from a real
    re-rating "needs a corporate-actions feed this module does not take". That
    was true when written and is no longer: 799 warnings sat permanently
    accepted in the baseline because nobody could classify them.

    A k-for-1 split multiplies the price by 1/k, so the fractional move is
    ``split_from/split_to - 1``: a 4:1 split shows as -75%, a 1:10 reverse split
    as +900%. Matched within ``tolerance`` because the split day carries genuine
    trading on top of the mechanical adjustment, so an exact equality would
    reject nearly every real case.

    Returns False rather than raising for a nonsense ratio, because "this move
    is not explained" is the safe answer - it keeps the finding visible instead
    of dismissing it on bad reference data.
    """
    if not (math.isfinite(move) and math.isfinite(split_to)
            and math.isfinite(split_from)):
        return False
    expected = split_from / split_to - 1.0
    if expected == 0:
        return False
    # Relative comparison: a -75% expectation should not be matched by the same
    # absolute slack as a +900% one.
    return abs(move - expected) <= tolerance * abs(expected)


@deal.has()
@deal.pure
@deal.ensure(
    lambda _: all(f.severity == 'error' for f in _.result),
    message='a round-tripping spike cannot be a real price move')
def price_spikes(bars: Sequence[Bar], threshold: float = 0.6,
                 tolerance: float = 0.35) -> List[Finding]:
    """A move that REVERSES the next bar: a bad print, not a market.

    Distinct from `unexplained_jumps`, which reports any large move as a
    warning because a crash and an unadjusted split look identical from
    prices alone. This rule is narrower and therefore an ERROR: a price that
    leaps and comes back to within ``tolerance`` of where it started did not
    go anywhere. Real moves of that size do not round-trip.

    Found in the live daily store on 2026-07-28: 11 such days across 5
    instruments, including +606% followed by -85.8% (net x1.00) on consecutive
    days spanning a year boundary. They are invisible to every per-bar rule,
    because each bar is internally coherent - it is the SEQUENCE that cannot
    have happened. One of them was enough to make an equal-weight
    buy-and-hold benchmark report a CAGR of 116%.

    Deliberately does NOT flag a sustained level shift (a jump that stays).
    That is the signature of an unadjusted split, which needs a
    corporate-actions feed to tell from a real re-rating, and guessing would
    delete genuine history.
    """
    out: List[Finding] = []
    if not (_finite(threshold) and threshold > 0):
        return out
    if not (_finite(tolerance) and tolerance > 0):
        return out
    for i in range(1, len(bars) - 1):
        prev, cur, nxt = bars[i - 1].close, bars[i].close, bars[i + 1].close
        if not all(_finite(x) and x > 0 for x in (prev, cur, nxt)):
            continue
        up = cur / prev - 1.0
        back = nxt / cur - 1.0
        if abs(up) < threshold:
            continue
        # Did the round trip return to roughly the starting level?
        net = (1.0 + up) * (1.0 + back) - 1.0
        if abs(net) <= tolerance:
            out.append(Finding(
                'price_spike', 'error', i,
                f'close {prev:g} -> {cur:g} -> {nxt:g}: a {up:+.1%} move that '
                f'reversed to {net:+.1%} net cannot be a real price'))
    return out


# --------------------------------------------------------------------------
# Vectorised counterpart, for the write path.
#
# The rules above are the SPEC: pure, contracted, CrossHair-checked, and they
# build a NamedTuple per bar. That is far too slow for ingestion, which writes
# frames of tens of thousands of rows. So the write path uses the mask below.
#
# Two implementations of one rule is exactly the shape that produced the
# flip-residual bug (a gate and a splitter answering different questions), so
# they are not allowed to drift: `tests/invariants/test_bar_quality.py` asserts
# the mask selects precisely the rows `check_bar` calls errors, over generated
# frames. The spec stays the source of truth; this is an optimisation of it.
# --------------------------------------------------------------------------

# Exchange TEST securities, published on the consolidated tape and not
# tradeable. ZWZZT closed at 199,999.00 in this store, which would dominate any
# cross-sectional ranking it touched. Defined HERE rather than in each consumer
# so the writer and the reader cannot disagree - filtering only at read means
# every future consumer has to remember, and one that forgets gets a 200,000
# dollar phantom in its top decile.
EXCHANGE_TEST_TICKERS = frozenset({
    'ZWZZT', 'ZVZZT', 'ZXZZT', 'ZJZZT', 'ZAZZT', 'ZBZZT', 'ZCZZT',
    'ZEXIT', 'ZIEXT', 'ZTEST', 'ZTST', 'IBM.TEST', 'CBO', 'CBX', 'TESTA',
})


@deal.has()
@deal.pure
def is_test_ticker(symbol: str) -> bool:
    """True for an exchange test security, which is not a tradeable instrument.

    Matched on the exact set rather than a 'Z...ZZT' pattern: a pattern would
    also swallow real tickers, and there is no recovering a name you silently
    refused to store.
    """
    return str(symbol).strip().upper() in EXCHANGE_TEST_TICKERS


_OHLC = ('open', 'high', 'low', 'close')


def impossible_mask(df) -> Any:
    """Boolean Series: True for rows that cannot describe a real market.

    ERRORS only. An all-empty bar is a placeholder, not an impossibility, and
    is deliberately NOT selected: those rows may be load-bearing for the
    incremental refresh (they record "we asked and the source had nothing"),
    and dropping them could send it re-fetching the same empty range forever.
    """
    import numpy as np
    import pandas as pd

    if df is None or len(df) == 0:
        return pd.Series([], dtype=bool)
    if not all(c in df.columns for c in _OHLC):
        # Nothing to judge against; refusing here would block legitimate
        # non-OHLC writes rather than protect anything.
        return pd.Series(False, index=df.index)

    cols = {c: pd.to_numeric(df[c], errors='coerce').to_numpy(dtype=float)
            for c in _OHLC}
    finite = {c: np.isfinite(v) for c, v in cols.items()}
    n_finite = sum(finite[c].astype(int) for c in _OHLC)

    all_missing = n_finite == 0
    some_missing = (n_finite > 0) & (n_finite < len(_OHLC))

    o, h, l, c = (cols['open'], cols['high'], cols['low'], cols['close'])
    with np.errstate(invalid='ignore'):
        body_hi = np.fmax(o, c)
        body_lo = np.fmin(o, c)
        non_positive = (
            (np.nan_to_num(o, nan=1.0) <= 0) | (np.nan_to_num(h, nan=1.0) <= 0)
            | (np.nan_to_num(l, nan=1.0) <= 0) | (np.nan_to_num(c, nan=1.0) <= 0))
        incoherent = (h < l) | (h < body_hi) | (l > body_lo)

    bad = some_missing | (~all_missing & (non_positive | incoherent))

    if 'volume' in df.columns:
        vol = pd.to_numeric(df['volume'], errors='coerce').to_numpy(dtype=float)
        # A missing volume on an otherwise complete bar is corrupt; a missing
        # volume on a placeholder row is just part of the placeholder.
        bad = bad | (~all_missing & (~np.isfinite(vol) | (vol < 0)))

    return pd.Series(bad, index=df.index)
