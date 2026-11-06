"""How far behind is a stored bar series?

One answer, used by both the `data status` display and the sweep freshness
guard. They previously computed it separately and made the same two mistakes,
which is the usual result of two call sites answering the same question.

MISTAKE ONE: dropping a timezone instead of converting it. Daily bars are
stored tz-aware (``2026-07-26 21:00:00-07:00``). Both call sites did
``last.replace(tzinfo=None)``, which discards the offset and keeps the wall
clock, so the instant silently moves by the size of the offset. One of them
then compared that against ``datetime.now(timezone.utc)``. Converting is
``astimezone``; ``replace`` is for when you know the value was UTC all along.

MISTAKE TWO: counting calendar days. Daily bars only appear on trading days, so
on a Monday morning a series that is perfectly current through Friday's close
reads as three days stale. A freshness signal that cries wolf every Monday is a
freshness signal people stop reading.

Weekends are excluded here; public holidays are NOT. That under-counts nothing
and over-counts a little, which is the safe direction: this feeds a guard that
refuses to run sweeps on stale data, so over-reporting staleness fails closed.
Per-exchange holiday calendars would need a venue for each series, which this
function deliberately does not take.

KNOWN LIMITATION, same reason. A session's bar is stamped at a venue-relative
instant, so its UTC date does not always equal the exchange's session date. A
US daily bar lands on the session date in UTC; an ASX one lands a day earlier,
because Sydney is ahead of UTC and the bar sits at local midnight. So ASX
series read one session staler than they are. Fixing that needs the venue for
each series, and the error is in the fail-closed direction, so it is recorded
here rather than papered over with a guess.
"""

from __future__ import annotations

import datetime as dt

from typing import Optional


def _as_utc(value) -> Optional[dt.datetime]:
    """A datetime-like value as an aware UTC datetime, or None.

    A naive value is ASSUMED to be UTC rather than local: every producer in
    this system stores UTC, and guessing local would move the instant by the
    host's offset, which is the bug this module exists to stop repeating.
    """
    if value is None:
        return None
    to_py = getattr(value, 'to_pydatetime', None)
    if callable(to_py):          # pandas.Timestamp
        try:
            value = to_py()
        except Exception:
            return None
    if not isinstance(value, dt.datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _calendar_sessions_after(last: dt.datetime, current: dt.datetime, calendar) -> Optional[int]:
    """Sessions of ``calendar`` dated strictly after ``last`` up to and
    including ``current``'s date, both dated in the venue's timezone. None when
    the calendar cannot answer for this span."""
    import pandas as pd

    tz = getattr(calendar, 'tz', None)
    if tz is None:
        return None
    start = last.astimezone(tz).date()
    end = current.astimezone(tz).date()
    if end <= start:
        return 0
    first = (pd.Timestamp(start) + pd.Timedelta(days=1))
    last_day = pd.Timestamp(end)
    try:
        lo = max(first, pd.Timestamp(calendar.first_session).tz_localize(None))
        hi = min(last_day, pd.Timestamp(calendar.last_session).tz_localize(None))
        if lo != first or hi != last_day:
            return None          # span leaves the calendar: don't half-answer
        return int(len(calendar.sessions_in_range(lo, hi)))
    except Exception:
        return None


def sessions_behind(last_bar, now=None, calendar=None) -> Optional[int]:
    """Weekday sessions between ``last_bar`` and ``now``, or None if unknown.

    With an ``exchange_calendars`` ``calendar`` the count is of that venue's
    real sessions, with both instants converted to the VENUE's timezone before
    taking their dates. That removes both limitations recorded above: holidays
    stop counting as missed sessions (the Friday after Thanksgiving no longer
    reads every US series as stale), and an ASX bar stamped at Sydney midnight
    is dated by its own session rather than the previous UTC day. When the
    calendar cannot answer (a date outside its bounds) this falls back to the
    weekday count rather than guessing.

    ``0`` means "current": the most recent bar is from today, or from the most
    recent weekday when today is a weekend. A series that stops on Friday reads
    as 0 all weekend and 1 on Monday, which is what an operator means by
    "a day behind".

    A bar dated in the FUTURE returns 0 rather than a negative number. That
    happens legitimately, because a daily bar carries a session timestamp that
    can be ahead of the wall clock in some venues, and "less than current" is
    not a meaningful state.
    """
    last = _as_utc(last_bar)
    if last is None:
        return None
    current = _as_utc(now) if now is not None else dt.datetime.now(dt.timezone.utc)
    if current is None:
        return None

    if calendar is not None:
        counted = _calendar_sessions_after(last, current, calendar)
        if counted is not None:
            return counted

    start, end = last.date(), current.date()
    if end <= start:
        return 0

    # Count weekdays strictly after the bar's date, up to and including today.
    # Iterating is fine: this is only ever called on a series that is behind,
    # and a series years behind is a data problem, not a performance one.
    days = 0
    cursor = start + dt.timedelta(days=1)
    while cursor <= end:
        if cursor.weekday() < 5:      # Mon-Fri
            days += 1
        cursor += dt.timedelta(days=1)
    return days
