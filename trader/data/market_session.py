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
import threading

from typing import Dict, Optional, Tuple

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
    'TSE': 'XTKS', 'TSEJ': 'XTKS', 'JPX': 'JPX',
    'SEHK': 'HKEX', 'HKFE': 'HKEX',
    'LSE': 'LSE', 'LSEETF': 'LSE',
    'IBIS': 'XFRA', 'FWB': 'XFRA', 'SBF': 'XPAR', 'AEB': 'XAMS',
    'EBS': 'SIX', 'VSE': 'XWBO', 'BVME': 'XMIL', 'BM': 'XMAD',
    'TSX': 'TSX', 'VENTURE': 'TSXV',
    'SGX': 'XSES', 'NSE': 'NSE', 'BSE': 'XBOM',
    'KSE': 'XKRX', 'TASE': 'TASE', 'JSE': 'XJSE',
}

# Instruments that trade around the clock — no session gate applies.
_ALWAYS_OPEN_SEC_TYPES = frozenset({'CASH', 'CRYPTO'})

_lock = threading.Lock()
_calendar_cache: Dict[str, object] = {}
_schedule_cache: Dict[Tuple[str, dt.date], Optional[Tuple[pd.Timestamp, pd.Timestamp]]] = {}
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


def session_window(
    calendar_name: str,
    day: dt.date,
) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    """The full tradeable window (UTC) for *day*, extended hours INCLUDED.

    ``None`` means the venue has no session that day — a weekend or a holiday.
    Cached per (calendar, day): the schedule call is not cheap and the runtime
    asks this on every dispatched bar.
    """
    key = (calendar_name, day)
    with _lock:
        if key in _schedule_cache:
            return _schedule_cache[key]
    window: Optional[Tuple[pd.Timestamp, pd.Timestamp]] = None
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
            if starts and ends:
                window = (min(starts), max(ends))
    except Exception as ex:
        logging.warning(
            'market_session: could not read %s schedule for %s (%s) — treating as open',
            calendar_name, day, ex)
        window = None
        with _lock:
            _schedule_cache[key] = None
        return None
    with _lock:
        _schedule_cache[key] = window
    return window


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
    for offset in (-1, 0, 1):
        window = session_window(name, day + dt.timedelta(days=offset))
        if window and window[0] <= stamp <= window[1]:
            return True
    return False
