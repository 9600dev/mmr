"""Share counts a market cap can be computed from.

A market capitalisation is ``price x shares``, and it is only a market cap
when the two numbers are on the SAME basis and belong to the SAME company.
Five things stood between the stored fundamentals and that, and until
2026-11-15 nothing in the research path knew about any of them
(docs/RESEARCH_LOG.md, 2026-11-15 and its addendum):

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
    enter K: one executed after the day the prices are adjusted through is
    not in any price yet, and the vendor's adjusted series is occasionally
    missing one (the price then still shows the raw jump on the execution
    day). WHICH splits a stored series reflects is measured, never assumed
    (``trader/data/pit_adjustment.py``): the vendor applies splits by ticker
    symbol, including a later company's to an earlier company's bars (PARA
    2025: 13.05 traded, 2,610 "adjusted"), and adds records late. Price and
    count then carry the same factors and the product is still the cap.

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
    Filers report share counts in units, in thousands or in millions, and the
    stored number is the one printed: about 1.2% of quarterly counts are below
    a million (MCD from 2024: 725.9, 722.0, 720.0 - millions; OKE 2012:
    105,926 - thousands; TXN 2011: 1,194 - millions again), some printed in
    thousands are large enough to pass for a count (COP 2018-2020:
    1,084,561), a handful are a thousand times too large, and the first
    figure of a series is as often wrong as any (TEVA 2,000,000, XOM
    4,000,000). ``plausible_share_counts`` reads the scale, believes a level
    when two consecutive filings agree on it, and is causal: it never looks
    at a later filing.

ONE TICKER, SEVERAL COMPANIES
    The point-in-time tables are keyed by ticker, and tickers change hands: B
    was Barnes Group (51M shares) until January 2025 and is Barrick since
    May; CTRA was Contura Energy (19M) and is Coterra (800M). The old
    company's last count priced the new one - for as long as nothing replaced
    it, which for a foreign filer is for ever. ``MAX_DAYS_BETWEEN_FILINGS`` bounds
    both how long a filing is the latest word and how far apart two filings
    of one issuer can be.

THE TAPE
    Whatever the rules above believed, a market cap smaller than two typical
    days of the name's own trading is not one (``unbelievable_cap``).

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
# What a share count of a listed company in this universe can be. Below a
# million the number is in thousands or millions (the smallest real counts in
# a liquid universe are BRK.A's 1.4M and NVR's 3M; a company with fewer - SEB,
# 0.97M - gets no count here, which is the cost of the rule). Above two hundred
# billion it is a thousand times too large (the largest real one, on a
# split-adjusted basis, is about 25 billion).
MIN_SHARES = 1e6
MAX_SHARES = 2e11


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


def share_count_readings(value: float) -> List[float]:
    """What a printed share count can mean, the natural reading first.

    Filers print in units, thousands or millions, so the true count is the
    printed number or LARGER by a power of a thousand - never smaller. Below a
    thousand the natural reading is millions; below a million, thousands; from
    a million up, the number itself. The larger readings follow, because the
    natural one is not always right: TXN's 1,194 is millions, and COP's
    1,084,561 is thousands although it could be a share count as it stands.

    A number is read DOWN only when it cannot be a share count at all (above
    ``MAX_SHARES``: a thousand or a million times too large). Reading a
    possible count down to fit the filings around it is how a wrong first
    figure takes over a series: XOM's first stored count is a wrong
    4,000,000, and every correct 4.7 billion after it fitted that as 4.7
    million.

    Only readings that are a possible share count are returned, so the list
    is empty for a number no scale makes sense of (0.01).
    """
    if value > MAX_SHARES:
        scales: Tuple[float, ...] = (1e-3, 1e-6)
    elif value < 1e3:
        scales = (1e6, 1e3)
    elif value < MIN_SHARES:
        scales = (1e3, 1e6)
    else:
        scales = (1.0, 1e3, 1e6)
    return [value * scale for scale in scales if MIN_SHARES <= value * scale <= MAX_SHARES]


def plausible_share_counts(counts: Sequence[Optional[float]],
                           factor: float = PLAUSIBLE_FACTOR,
                           reference: int = REFERENCE_FILINGS,
                           basis: Optional[Sequence[float]] = None) -> List[Optional[float]]:
    """The share count each filing reports, in filing order - in UNITS,
    whatever scale it was printed in - or None where it cannot be believed.

    ``basis``, when given, is one multiplier per count (its split factor,
    ``split_factor_after``): the scale is read from the number AS PRINTED, the
    comparison with earlier filings is made on the common basis, and the
    result is on that basis. Without it a 4:1 split looks like a change of
    level; with the multiplier applied before reading the scale, 725.9 x 2
    looks like a count in thousands.

    CAUSAL: the verdict on a count uses only the counts before it, so the
    answer for a filing never changes when later filings arrive (an as-of
    read must not depend on the future).

    Each printed number has a few possible readings
    (``share_count_readings``). With counts already believed, the reading
    within ``factor`` of the median of the last ``reference`` of them is the
    count. If none is, the number is held back - it may be an error, or a
    real change of level (a merger; the first right figure after a run of
    wrong ones). The NEXT filing decides: a reading of it that agrees with a
    reading of the held one means the level has moved, and it is kept (the
    held one stays rejected - when it arrived it could not be told from an
    error); one that agrees with the old level means the held one was the
    error.

    The FIRST number of a series is held back the same way, for the same
    reason: nothing says yet whether it is a count. TEVA's first stored
    figure is 2,000,000 and XOM's 4,000,000, against a billion and four
    billion in every filing after. So a level is believed when two
    consecutive filings agree on it, at the start of a series as at a change
    of level, and a series costs one filing before it has a count.

    A value that is None, not finite, not positive, or has no possible
    reading is never a share count and is skipped: it neither confirms nor
    contradicts.

    What this cannot see is a level that is wrong in every filing the same
    way (a series printed in thousands from its first row, each number large
    enough to be a count as it stands). ``unbelievable_cap`` is for that.
    """
    kept: List[float] = []
    held: List[float] = []              # the readings of a number not yet believed
    out: List[Optional[float]] = []
    for position, raw in enumerate(counts):
        try:
            value = float(raw) if raw is not None else float('nan')
        except (TypeError, ValueError):
            value = float('nan')
        readings = share_count_readings(value) if math.isfinite(value) and value > 0 else []
        if basis is not None:
            multiplier = float(basis[position])
            readings = ([r * multiplier for r in readings]
                        if math.isfinite(multiplier) and multiplier > 0 else [])
        if not readings:
            out.append(None)
            continue
        if kept:
            recent = sorted(kept[-reference:])
            middle = len(recent) // 2
            median = recent[middle] if len(recent) % 2 else (recent[middle - 1] + recent[middle]) / 2.0
            same_level = next((r for r in readings if median / factor <= r <= median * factor), None)
            if same_level is not None:
                kept.append(same_level)
                held = []
                out.append(same_level)
                continue
        confirmed = next((r for r in readings
                          if any(h / factor <= r <= h * factor for h in held)), None)
        if confirmed is not None:
            kept = [confirmed]          # two filings agree: this is the level now
            held = []
            out.append(confirmed)
        else:
            held = readings
            out.append(None)
    return out


# Consecutive periodic reports of one issuer are at most about 140 days apart
# (a 10-Q is due 40-45 days after its quarter, a 10-K 60-90 days after the
# year). Measured over 78,279 consecutive filings in the store: median 91
# days, 90% within 116, a second mode at 180-200 (one report missing, 1.6%),
# 1.1% longer. Past this many days two things stop being true: that the
# newest filing still describes the company, and that the next filing under
# the ticker is the same company's. Tickers change hands: B was Barnes Group
# until January 2025 and is Barrick since May; CTRA was Contura Energy (19 M
# shares) and is Coterra (800 M); EXE, BKR and STRC likewise.
MAX_DAYS_BETWEEN_FILINGS = 200

# A market cap smaller than this many days of the name's own trading is not a
# market cap. A liquid stock turns over 0.2% to 3% of its value on a typical
# day, and a count wrong by a factor of a hundred or a thousand turns that
# into 50% to 3,000%. The few names that really trade more than this for
# weeks (KODK in August 2020, AMC in early 2021, a micro-cap issuing stock
# faster than it reports) are the ones whose last filed count no longer
# describes them either.
MAX_DAILY_TURNOVER = 0.5


def unbelievable_cap(market_cap: Optional[float], typical_dollar_volume: Optional[float],
                     max_turnover: float = MAX_DAILY_TURNOVER) -> bool:
    """Is this "market cap" too small for the trading that goes on in the
    name? True when a typical day's dollar volume exceeds ``max_turnover`` of
    it - the last line of defence against a share count that every filter
    believed (a level that was wrong in every filing, a count overtaken by
    issuance). False when either number is unknown: this refuses a cap that
    is contradicted, it does not demand proof."""
    if market_cap is None or typical_dollar_volume is None:
        return False
    try:
        cap, volume = float(market_cap), float(typical_dollar_volume)
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(cap) and math.isfinite(volume)) or cap <= 0 or volume <= 0:
        return False
    return volume > max_turnover * cap


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
