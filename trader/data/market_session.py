"""Which minutes of the clock an instrument is actually tradeable in.

WHY THIS EXISTS
    The live runtime built OHLCV bars out of any tick it received and dispatched
    them to strategies, with nothing anywhere asking whether the exchange was
    open. ``normalize_ticker`` falls back to the bid/ask MIDPOINT when there is
    no trade price, so an out-of-hours quote carries a non-NaN close, forms a
    bar in ``resample_ticks_to_bars`` (which drops only NaN-close bars), and is
    handed to ``strategy.on_prices`` as if it were market data.

    Observed 2026-07-26: three quote ticks for WDS at 11:45 PDT on a SUNDAY —
    the ASX shut since Friday — produced a dispatched bar and dropped the
    pulse's ``bar_age_s`` from 218,093s to 263s. See AUDIT_ROADMAP G8.

EXTENDED HOURS ARE IN-SESSION, DELIBERATELY
    This is NOT an RTH filter. US pre/post-market bars must reach strategies:
    the 1-min history the roster was validated on spans 04:00-20:00 ET, and 23%
    of GOOGL's stored 1-min bars have zero volume — those ARE the extended-hours
    minutes (0% zero-volume inside RTH, 42-54% outside it). Filtering to RTH, or
    to bars containing a trade, would silently change the series every US
    strategy computes on.

WHY pandas_market_calendars AND NOT exchange_calendars
    ``exchange_calendars`` (already a dependency, used by the data service and
    the sweep freshness guard) models REGULAR hours only. Verified on 4.13.1:
    ``XNYS`` reports open=09:30 close=16:00, ``ExchangeCalendar`` has no
    ``market_times`` attribute, and no file in the package mentions pre/post
    market at all. It is the right tool for "is this a trading day" and the
    wrong one for this question.

    ``pandas_market_calendars`` models it directly — NYSE/NASDAQ carry ``pre``
    (04:00 ET) and ``post`` (20:00 ET) alongside open/close, plus break
    start/end for the venues with a lunch break. Cross-checked against our own
    stored history, and it agrees exactly on both venues we trade:
      * NYSE 2026-07-24 → pre 08:00 UTC, post 24:00 UTC. GOOGL's stored 1-min
        bars run 08:00-23:59 UTC. Exact match.
      * ASX 2026-07-24 → open 00:00 UTC, close 06:10 UTC (the closing auction
        is included). WDS's last real bar is the 06:10 UTC auction print.
        Exact match.

FAIL OPEN, ON PURPOSE
    An unknown venue, an unparseable timestamp, or a calendar lookup that raises
    all resolve to IN-SESSION. The bug this guards against costs a slightly
    polluted moving average; the failure mode of guarding too eagerly is a
    strategy silently starved of bars, which costs trades. Those are not
    symmetric, so the uncertain answer is the permissive one — and it is logged
    once per venue so the gap is visible rather than assumed.
"""

from __future__ import annotations

import datetime as dt
import math
import threading

from typing import Any, Dict, NamedTuple, Optional, Tuple

import pandas as pd

from trader.common.logging_helper import setup_logging

logging = setup_logging(module_name='market_session')


# IB exchange / primaryExchange codes → pandas_market_calendars names.
# IB reports the listing venue (ARCA, BATS, NYSE, NASDAQ, ...) while the
# calendar is per-market, so several codes collapse onto one calendar.
_EXCHANGE_CALENDARS: Dict[str, str] = {
    # United States — all share the NYSE session incl. 04:00-20:00 ET extended
    'NYSE': 'NYSE', 'NASDAQ': 'NASDAQ', 'NASDAQ.NMS': 'NASDAQ', 'ARCA': 'NYSE',
    'AMEX': 'NYSE', 'BATS': 'NYSE', 'IEX': 'NYSE', 'ISLAND': 'NASDAQ',
    'PINK': 'NYSE', 'SMART': 'NYSE', 'NYSENAT': 'NYSE',
    # Australia — ASX (pmc's 'ASX' closes 16:10, i.e. it includes the auction)
    'ASX': 'ASX',
    # Rest of the venues the scanners/universes can reach
    # IB's 'TSE' is TORONTO; Tokyo is 'TSEJ'. 'TSE' was mapped to Tokyo here,
    # which is not a fail-open gap but a wrong answer: a Toronto listing's bars
    # were judged against Tokyo's session and suppressed all day.
    'TSEJ': 'XTKS', 'JPX': 'JPX',
    'SEHK': 'HKEX', 'HKFE': 'HKEX',
    'LSE': 'LSE', 'LSEETF': 'LSE',
    'IBIS': 'XFRA', 'FWB': 'XFRA', 'SBF': 'XPAR', 'AEB': 'XAMS',
    'EBS': 'SIX', 'VSE': 'XWBO', 'BVME': 'XMIL', 'BM': 'XMAD',
    'TSE': 'TSX', 'TSX': 'TSX', 'VENTURE': 'TSXV',
    'SGX': 'XSES', 'NSE': 'NSE', 'BSE': 'XBOM',
    'KSE': 'XKRX', 'TASE': 'TASE', 'JSE': 'XJSE',
}

# Instruments that trade around the clock — no session gate applies.
_ALWAYS_OPEN_SEC_TYPES = frozenset({'CASH', 'CRYPTO'})

Window = Tuple[pd.Timestamp, pd.Timestamp]


class SessionLookup(NamedTuple):
    """Tri-state answer, because two-state was silently fail-CLOSED.

    ``window is None`` is ambiguous on its own: it means both "the venue is
    legitimately shut that day" and "we could not find out". The first must
    suppress a bar; the second must NOT, per this module's stated policy. With a
    bare ``Optional`` they collapsed, and mutation testing found the collapse —
    a calendar error logged "treating as open" and then returned False, which
    would have starved every strategy of bars from a single library exception.

    Same ``*_evaluable`` shape ``RiskInputs`` and ``PortfolioState`` use, and for
    the same reason: "could not read it" is not a value.
    """
    window: Optional[Window]
    evaluable: bool
    intervals: Tuple[Window, ...] = ()


_lock = threading.Lock()
_calendar_cache: Dict[str, object] = {}
_schedule_cache: Dict[Tuple[str, dt.date], SessionLookup] = {}
_warned_unknown: set = set()


def calendar_name_for(exchange: str, primary_exchange: str = '') -> Optional[str]:
    """Map IB venue codes to a calendar name. primaryExchange wins — `exchange`
    is frequently the routing destination (SMART), not the listing venue."""
    for code in (primary_exchange, exchange):
        key = str(code or '').strip().upper()
        if key in _EXCHANGE_CALENDARS:
            return _EXCHANGE_CALENDARS[key]
    return None


def _calendar(name: str):
    with _lock:
        if name not in _calendar_cache:
            import pandas_market_calendars as mcal
            _calendar_cache[name] = mcal.get_calendar(name)
        return _calendar_cache[name]


def _utc_stamps(row, columns, names) -> list:
    """UTC timestamps for whichever of *names* the schedule actually has.

    Venues without extended hours simply have no ``pre``/``post`` column, and a
    column can hold NaT on an irregular day — both are skipped rather than
    assumed, so a half-populated row degrades to the regular window instead of
    poisoning the comparison with NaT.
    """
    out = []
    for name in names:
        if name not in columns:
            continue
        value = row[name]
        if value is None or pd.isna(value):
            continue
        stamp = pd.Timestamp(value)
        if not isinstance(stamp, pd.Timestamp):   # NaT is not a Timestamp
            continue
        out.append(stamp.tz_convert('UTC') if stamp.tz is not None
                   else stamp.tz_localize('UTC'))
    return out


def session_window(calendar_name: str, day: dt.date) -> SessionLookup:
    """The full tradeable window (UTC) for *day*, extended hours INCLUDED.

    ``SessionLookup(window=None, evaluable=True)`` = the venue has no session
    that day (weekend, holiday) — a bar then really is out of session.
    ``evaluable=False`` = the lookup failed and the caller must fail OPEN.

    Successful lookups are cached per (calendar, day); FAILURES ARE NOT. Caching
    an error would pin that day to the wrong answer for the life of the process,
    turning one transient exception into a permanent outage for that instrument.
    """
    key = (calendar_name, day)
    with _lock:
        if key in _schedule_cache:
            return _schedule_cache[key]
    window: Optional[Window] = None
    intervals: Tuple[Window, ...] = ()
    try:
        cal = _calendar(calendar_name)
        stamp = day.isoformat()
        # market_times='all' adds pre/post where the venue has them; venues
        # without extended hours simply return open/close.
        sched = cal.schedule(start_date=stamp, end_date=stamp, market_times='all')
        if len(sched):
            row = sched.iloc[0]
            starts = _utc_stamps(row, sched.columns, ('pre', 'market_open'))
            ends = _utc_stamps(row, sched.columns, ('post', 'market_close'))
            if not starts or not ends:
                # A session row we cannot read is not evidence of a closed
                # market — do not cache it and do not suppress on it.
                logging.warning(
                    'market_session: %s has a session on %s but no readable '
                    'open/close (%s) — treating as open', calendar_name, day,
                    list(sched.columns))
                return SessionLookup(None, False)
            window = (min(starts), max(ends))
            intervals = (window,)
            break_starts = _utc_stamps(row, sched.columns, ('break_start',))
            break_ends = _utc_stamps(row, sched.columns, ('break_end',))
            if break_starts and break_ends:
                pause_start, pause_end = break_starts[0], break_ends[0]
                if window[0] <= pause_start < pause_end <= window[1]:
                    intervals = ((window[0], pause_start), (pause_end, window[1]))
    except Exception as ex:
        logging.warning(
            'market_session: could not read %s schedule for %s (%s) — treating '
            'as open, NOT caching', calendar_name, day, ex)
        return SessionLookup(None, False)
    lookup = SessionLookup(window, True, intervals)
    with _lock:
        _schedule_cache[key] = lookup
    return lookup


def session_intervals(calendar_name: str, day: dt.date) -> SessionLookup:
    """Session envelope and tradable intervals; lunch breaks are excluded.

    Internal interval ends are exclusive; the final session close is inclusive
    to preserve exchange closing-auction prints. ``window`` remains the outer
    envelope for consumers that only need the session date/open/close.
    """
    return session_window(calendar_name, day)


def in_session(
    ts,
    exchange: str,
    primary_exchange: str = '',
    sec_type: str = '',
) -> bool:
    """Is *ts* inside the venue's tradeable window, extended hours included?

    FAILS OPEN. An unmapped venue, an unreadable timestamp or a calendar error
    all return True: starving a strategy of real bars is worse than admitting a
    few synthetic ones (see the module docstring).
    """
    if str(sec_type or '').strip().upper() in _ALWAYS_OPEN_SEC_TYPES:
        return True

    name = calendar_name_for(exchange, primary_exchange)
    if name is None:
        key = f'{primary_exchange}/{exchange}'
        with _lock:
            first_time = key not in _warned_unknown
            _warned_unknown.add(key)
        if first_time:
            logging.warning(
                'market_session: no calendar mapped for exchange %r (primary %r) — '
                'bars for it are NOT session-gated. Add it to _EXCHANGE_CALENDARS.',
                exchange, primary_exchange)
        return True

    try:
        stamp = pd.Timestamp(ts)
        # pd.Timestamp(None) is NaT — it does NOT raise. Without this check the
        # NaT flows on, every schedule lookup fails, and the function returns
        # False: fail CLOSED, the exact opposite of the documented policy.
        # Caught by tests/test_market_session.py, not by reading the code.
        if stamp is pd.NaT or pd.isna(stamp):
            return True
        stamp = stamp.tz_localize('UTC') if stamp.tz is None else stamp.tz_convert('UTC')
    except Exception:
        return True

    # A session can straddle UTC midnight in both directions (NYSE post-market
    # ends at 00:00 UTC the NEXT day; the ASX session starts at 00:00 UTC on the
    # session date). Check the neighbouring days too rather than assuming the
    # UTC date and the session date agree.
    day = stamp.date()
    if not isinstance(day, dt.date):        # NaT.date() is NaT, not a date
        return True

    # A session can straddle UTC midnight in both directions (NYSE post-market
    # ends at 00:00 UTC the NEXT day; the ASX session starts at 00:00 UTC on the
    # session date), so check the neighbouring days rather than assuming the UTC
    # date and the session date agree.
    unevaluable = False
    for offset in (-1, 0, 1):
        lookup = session_window(name, day + dt.timedelta(days=offset))
        if not lookup.evaluable:
            unevaluable = True
            continue
        intervals = lookup.intervals or ((lookup.window,) if lookup.window else ())
        for start, end in intervals:
            if start <= stamp < end or (lookup.window and stamp == end == lookup.window[1]):
                return True
    if unevaluable:
        # Every relevant day answered "unknown" — fail OPEN. Returning False
        # here is what the two-state version did, and it would have suppressed
        # every bar for this instrument off one library exception.
        return True
    return False


def early_close(calendar_name: str, day: dt.date) -> Optional[pd.Timestamp]:
    """The regular-session close (UTC) on *day* IF the venue closes early then.

    ``None`` means an ordinary session or no session at all. A lookup that
    fails RAISES: this answers "is today unusual?", and "could not find out" is
    not "no" (the same reason ``SessionLookup`` is tri-state).

    The strategies carry their own session close (16:00 for a US class), which
    is exactly what an early close breaks: an entry after 13:00 was held by IB
    until the next session and an end-of-day flatten fired three hours late
    (AUDIT_2026-11-14 B3.5). ``mmr verify`` / ``mmr preflight`` use this to
    announce the day, and :func:`early_close_shift` / :func:`close_by_on` use
    it to move a close-by time (the executor and the backtester).
    """
    calendar: Any = _calendar(calendar_name)
    stamp = day.isoformat()
    schedule = calendar.schedule(start_date=stamp, end_date=stamp)
    if not len(schedule):
        return None
    early = calendar.early_closes(schedule)
    if not len(early):
        return None
    close = pd.Timestamp(early.iloc[0]['market_close'])
    if not isinstance(close, pd.Timestamp) or pd.isna(close):      # NaT is not a Timestamp
        raise ValueError(f'{calendar_name} reports an early close on {day} with no readable time')
    return close.tz_localize('UTC') if close.tz is None else close.tz_convert('UTC')


# ---------------------------------------------------------------------------
# The REGULAR session: where a market order actually executes.
#
# Everything above is about which bars a strategy may SEE, and deliberately
# includes extended hours. An order is a different question. IB holds a market
# order sent outside the regular session until the next one opens: an entry
# decided on Friday's 15:59 bar, or at 13:30 on a 13:00-close day, fills on
# Monday morning on a signal nobody would still act on, and a 15:45 flatten on
# that half day fires three hours after the close (AUDIT_2026-11-14 B1.11 /
# B3.5). The two helpers below are what the executor and the backtester use to
# refuse the first and move the second.
# ---------------------------------------------------------------------------

_regular_cache: Dict[Tuple[str, dt.date], SessionLookup] = {}


def regular_session(calendar_name: str, day: dt.date) -> SessionLookup:
    """The regular trading intervals (UTC) on *day*: no pre- or post-market, a
    lunch break excluded, an early close as early as it is.

    Same tri-state as :func:`session_window`: ``window=None, evaluable=True``
    is a day with no session; ``evaluable=False`` is a lookup that failed (and
    is not cached).
    """
    key = (calendar_name, day)
    with _lock:
        if key in _regular_cache:
            return _regular_cache[key]
    try:
        calendar: Any = _calendar(calendar_name)
        stamp = day.isoformat()
        schedule = calendar.schedule(start_date=stamp, end_date=stamp)
        window: Optional[Window] = None
        intervals: Tuple[Window, ...] = ()
        if len(schedule):
            row = schedule.iloc[0]
            opens = _utc_stamps(row, schedule.columns, ('market_open',))
            closes = _utc_stamps(row, schedule.columns, ('market_close',))
            if not opens or not closes or not opens[0] < closes[0]:
                raise ValueError(f'unreadable regular hours in {list(schedule.columns)}')
            window = (opens[0], closes[0])
            intervals = (window,)
            pause_starts = _utc_stamps(row, schedule.columns, ('break_start',))
            pause_ends = _utc_stamps(row, schedule.columns, ('break_end',))
            if pause_starts and pause_ends and window[0] < pause_starts[0] < pause_ends[0] < window[1]:
                intervals = ((window[0], pause_starts[0]), (pause_ends[0], window[1]))
    except Exception as ex:
        logging.warning('market_session: could not read the regular %s session for %s (%s) — NOT caching',
                        calendar_name, day, ex)
        return SessionLookup(None, False)
    lookup = SessionLookup(window, True, intervals)
    with _lock:
        _regular_cache[key] = lookup
    return lookup


def _judge_regular(order_at, calendar_name: str) -> Tuple[Optional[bool], str]:
    """(inside?, why not). ``None`` = could not be determined; the text then
    says what failed. One judgement behind both readings below, so "refuse an
    open" and "an exit waits" can never disagree about where the session is."""
    try:
        instant = pd.Timestamp(order_at)
        if not isinstance(instant, pd.Timestamp) or pd.isna(instant):
            raise ValueError('no timestamp')
        zone = getattr(_calendar(calendar_name), 'tz')
        instant = instant.tz_localize(zone) if instant.tz is None else instant
        instant = instant.tz_convert('UTC')
        local = instant.tz_convert(zone)
    except Exception as ex:
        return None, f'regular session not evaluable for {calendar_name} ({type(ex).__name__}: {ex})'
    day = local.date()
    lookup = regular_session(calendar_name, day)
    if not lookup.evaluable:
        return None, f'regular session not evaluable for {calendar_name} on {day}'
    if lookup.window is None:
        return False, (f'{calendar_name} has no regular session on {day}: '
                       f'a market order sent now is held until the next one')
    for start, end in lookup.intervals:
        if start <= instant < end:
            return True, ''
    opening, closing = lookup.window
    at = f'{local:%H:%M} {zone}'
    if instant < opening:
        return False, (f'{calendar_name} regular session has not opened ({opening.tz_convert(zone):%H:%M}); '
                       f'a market order sent at {at} is held until it does')
    if instant >= closing:
        return False, (f'{calendar_name} regular session closed at {closing.tz_convert(zone):%H:%M}; '
                       f'a market order sent at {at} is held until the next session')
    return False, f'{calendar_name} is in its trading break at {at}; a market order sent now is held until it ends'


def regular_session_refusal(order_at, calendar_name: Optional[str]) -> Optional[str]:
    """Why a market order sent at *order_at* would NOT execute in the venue's
    regular session — or None when it would.

    ``order_at`` is the instant the order goes out: for a bar-driven decision,
    the END of the bar (a 15:59 one-minute bar is acted on at 16:00:00, which
    is the close). The session is half-open, ``open <= order_at < close``, and
    a lunch break is outside it.

    ``calendar_name`` empty/None means the venue has no calendar here (an
    unmapped exchange, or an instrument that trades round the clock): the rule
    does not apply and the answer is None. Every other uncertainty is a refusal
    — an unreadable instant, a calendar that cannot answer — because this
    guards OPENS only, where refusing is the benign direction.

    A timestamp WITHOUT a zone is read as the venue's own wall clock, the
    reading the other time-of-day rules already give it (``check_time_exit``
    takes ``bar_ts.time()``; the backtester treats a naive frame as session
    wall time). The live runtime always supplies an aware instant.
    """
    if not calendar_name:
        return None
    inside, why = _judge_regular(order_at, calendar_name)
    if inside is None:
        return f'{why} — refusing to open (fail-closed)'
    return None if inside else why


def in_regular_session(instant, calendar_name: Optional[str]) -> Optional[bool]:
    """Is *instant* inside the venue's regular session? ``None`` when that
    cannot be said: no calendar for the venue, an unreadable instant, a
    calendar that cannot answer.

    The three-valued reading of the judgement :func:`regular_session_refusal`
    makes, for callers that must NOT turn "unknown" into "outside" — an exit
    is delayed only by a session known to be shut.
    """
    if not calendar_name:
        return None
    return _judge_regular(instant, calendar_name)[0]


# IB routing destinations: where an order is SENT, not where the instrument is
# listed. `_EXCHANGE_CALENDARS` maps SMART to the NYSE session so that bar
# gating has a default (and fails open); an order rule that refuses must not
# guess a listing from a router — an ASX name routed SMART is not on New York
# hours.
_ROUTERS = frozenset({'SMART', 'IBKRATS', 'BEST'})


def listing_calendar(exchange: str, primary_exchange: str = '') -> Optional[str]:
    """The session calendar of the venue an instrument is LISTED on, or None
    when only a routing destination is known or the venue is not mapped."""
    for code in (primary_exchange, exchange):
        key = str(code or '').strip().upper()
        if key and key not in _ROUTERS and key in _EXCHANGE_CALENDARS:
            return _EXCHANGE_CALENDARS[key]
    return None


def order_calendar(exchange: str, primary_exchange: str = '', sec_type: str = '') -> Optional[str]:
    """The calendar an ORDER for this instrument is judged on, or None when the
    regular-session rules do not apply to it: an instrument that trades round
    the clock (``CASH`` / ``CRYPTO``), or a listing venue that is unknown or
    has no calendar here."""
    if str(sec_type or '').strip().upper() in _ALWAYS_OPEN_SEC_TYPES:
        return None
    return listing_calendar(exchange, primary_exchange)


def bar_open_refusal(bar_ts, bar_seconds, calendar_name: Optional[str]) -> Optional[str]:
    """Why an OPEN decided on the bar starting at *bar_ts* could not execute
    in the regular session of *calendar_name* — or None.

    The order goes out when the bar completes, so the instant judged is
    ``bar_ts + bar_seconds`` (see :func:`regular_session_refusal`). A
    daily-or-slower bar is outside the rule (it is decided after the close by
    design and fills at the next open), and so is an instrument with no
    calendar (:func:`order_calendar`). An interval that is not a positive
    finite number is a refusal: without it there is no order instant.

    The live executor and the backtester both call this, with the same two
    numbers, which is what makes a validated entry one the executor will take.
    """
    try:
        seconds = float(bar_seconds)
    except (TypeError, ValueError):
        seconds = float('nan')
    if not math.isfinite(seconds) or seconds <= 0:
        return ('bar interval unknown — cannot tell when the order would be sent; '
                'refusing to open (fail-closed)')
    if seconds >= 86400:
        return None
    if not calendar_name:
        return None
    try:
        order_at = pd.Timestamp(bar_ts) + pd.Timedelta(seconds=seconds)
    except Exception as ex:
        return (f'regular session not evaluable for {calendar_name} ({type(ex).__name__}: {ex}) — '
                f'refusing to open (fail-closed)')
    return regular_session_refusal(order_at, calendar_name)


def intraday_open_refusal(bar_ts, bar_seconds, exchange: str, primary_exchange: str = '',
                          sec_type: str = '') -> Optional[str]:
    """:func:`bar_open_refusal` for an instrument named by its venue fields.

    A routing destination such as SMART is not a listing: with nothing else to
    go on the rule does not apply.
    """
    return bar_open_refusal(bar_ts, bar_seconds, order_calendar(exchange, primary_exchange, sec_type))


_early_shift_cache: Dict[Tuple[str, dt.date], dt.timedelta] = {}


def early_close_shift(calendar_name: str, day: dt.date) -> dt.timedelta:
    """How much earlier than usual the venue's regular session ends on *day*:
    three hours for a 13:00 New York close, zero on an ordinary day and on a
    day with no session. Never negative.

    A calendar that cannot answer RAISES (and nothing is remembered); an
    answer is remembered, because the backtester asks once per bar.
    """
    key = (calendar_name, day)
    with _lock:
        if key in _early_shift_cache:
            return _early_shift_cache[key]
    shift = dt.timedelta(0)
    actual = early_close(calendar_name, day)
    if actual is not None:
        calendar: Any = _calendar(calendar_name)
        usual: dt.time = calendar.close_time
        # Wall-clock arithmetic in the venue's own zone: both closes are on `day`.
        usual_at = dt.datetime.combine(day, usual.replace(tzinfo=None))
        actual_at = actual.tz_convert(calendar.tz).to_pydatetime().replace(tzinfo=None)
        shift = max(usual_at - actual_at, dt.timedelta(0))
    with _lock:
        _early_shift_cache[key] = shift
    return shift


def close_by_on(close_by: dt.time, calendar_name: Optional[str], day: dt.date) -> dt.time:
    """The time of day a holding that asked to be closed by *close_by* must be
    closed by ON *day*.

    On an ordinary day, and for a venue with no calendar here, that is
    *close_by*. On a day the venue closes early it is earlier by exactly as
    much as the close is — a 15:45 flatten on a 13:00 close (16:00 usually)
    becomes 12:45, keeping the strategy's own margin before the bell. It never
    moves later, and never earlier than midnight. A calendar that cannot
    answer RAISES; the caller keeps the unshifted time (an exit is never held
    up by a lookup).
    """
    if not calendar_name:
        return close_by
    shift = early_close_shift(calendar_name, day)
    if shift <= dt.timedelta(0):
        return close_by
    moved = dt.datetime.combine(day, close_by.replace(tzinfo=None)) - shift
    if moved.date() != day:
        return dt.time(0, 0)
    return moved.time()
