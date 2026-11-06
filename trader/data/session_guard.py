"""Refuse daily-or-larger bars for sessions that have not closed yet.

Field finding 2026-11-05: `mmr history ib --symbol AAPL --bar_size "1 day"
--prev_days 15`, run at 14:30 ET, stored that day's still-forming session as a
complete daily bar (volume 9.8M, roughly a third of a normal day). Every
consumer then treats it as final: missing() sees the day as covered and never
re-fetches it, so the truncated high/low/close/volume are permanent, and a
backtest or ATR sizing reads a session that ended mid-afternoon.

The vendors are not wrong to return the bar — IB, TwelveData and Massive all
serve the in-progress session on request. What is wrong is persisting it. So
the rule lives at the one chokepoint every writer passes through
(``Data.write``), not in each downloader: the IB worker, the Massive and
TwelveData workers, `data download`, and the strategy runtime's history
priming (whose window deliberately runs to the end of today) all write through
it.

A bar is UNCLOSED when any session inside its period closes after ``now``:

* ``1 day`` — the bar's own session;
* ``1 week`` / ``1 month`` — every session from the bar's date to the end of
  its week / calendar month.

Sessions and closes come from the venue's ``exchange_calendars`` calendar, and
the bar is dated in the VENUE's timezone (daily bars are keyed at venue
midnight). A row on a non-session day (a weekend placeholder) is not a session
and is left alone, unless it is dated after today, which nothing legitimate is.

Without a calendar the session close is unknown, so this fails CLOSED: a bar
whose period reaches today (in the bar's own timezone) or later is refused.
The cost is that a bar fetched after the close arrives on the next run rather
than this one; the alternative is guessing a close time.

KNOWN IMPRECISION: "closed" means the REGULAR session's close. An IB daily bar
requested with useRTH=False can still accrue after-hours volume after that.
"""

from __future__ import annotations

import datetime as dt
from typing import Optional

import numpy as np
import pandas as pd

DAILY_OR_LARGER = ('1 day', '1 week', '1 month')


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _ts(value) -> pd.Timestamp:
    """A Timestamp, refusing NaT: a bar with no time cannot be placed in a
    session, and guessing would decide whether it gets persisted."""
    ts = pd.Timestamp(value)
    if not isinstance(ts, pd.Timestamp):
        raise ValueError(f'bar timestamp is not a time: {value!r}')
    return ts


def _as_utc(now: Optional[dt.datetime]) -> pd.Timestamp:
    ts = _ts(now if now is not None else _utcnow())
    return _ts(ts.tz_localize('UTC') if ts.tzinfo is None else ts.tz_convert('UTC'))


def _period_end(day: dt.date, bar_size: str) -> dt.date:
    if bar_size == '1 week':
        return day + dt.timedelta(days=6)
    if bar_size == '1 month':
        nxt = (day.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        return nxt - dt.timedelta(days=1)
    return day


def _local_date(ts: pd.Timestamp, tz) -> dt.date:
    # A naive stamp is taken as the venue's wall clock: daily bars are keyed at
    # venue midnight, and reading a naive midnight as UTC would move it to the
    # previous evening in every venue west of Greenwich.
    if ts.tzinfo is None or tz is None:
        return ts.date()
    return ts.tz_convert(tz).date()


def _unclosed_with_calendar(day: dt.date, end: dt.date, calendar, now: pd.Timestamp) -> bool:
    today = now.tz_convert(calendar.tz).date()
    if day > today:
        return True                      # dated in the future
    first = _ts(calendar.first_session).date()
    last = _ts(calendar.last_session).date()
    if end < first:
        return False                     # older than the calendar: long closed
    if day > last:
        return True                      # beyond the calendar: cannot be closed
    lo, hi = max(day, first), min(end, last)
    sessions = calendar.sessions_in_range(_ts(lo), _ts(hi))
    if len(sessions) == 0:
        return False                     # no session in the period (placeholder)
    close = _ts(calendar.session_close(sessions[-1]))
    close = _ts(close.tz_localize('UTC') if close.tzinfo is None else close.tz_convert('UTC'))
    return bool(close > now)


def _unclosed_without_calendar(ts: pd.Timestamp, day: dt.date, end: dt.date,
                               now: pd.Timestamp) -> bool:
    tz = ts.tzinfo
    today = now.tz_convert(tz).date() if tz is not None else now.date()
    return end >= today


def unclosed_session_mask(frame: pd.DataFrame, default_bar_size: str = '',
                          calendar=None, now: Optional[dt.datetime] = None) -> np.ndarray:
    """Boolean mask over ``frame`` rows: True where the row is a daily-or-larger
    bar whose period contains a session that has not closed at ``now``.

    The bar size comes from the row's ``bar_size`` column when present, else
    ``default_bar_size``. Rows of intraday bar size are never masked.
    """
    n = len(frame)
    mask = np.zeros(n, dtype=bool)
    if n == 0:
        return mask
    if 'bar_size' in frame.columns:
        sizes = frame['bar_size'].astype(object).where(frame['bar_size'].notna(), default_bar_size)
        sizes = [str(s) for s in sizes]
    else:
        sizes = [default_bar_size] * n
    if not any(s in DAILY_OR_LARGER for s in sizes):
        return mask
    if 'date' in frame.columns:
        stamps = [_ts(v) for v in frame['date']]
    else:
        stamps = [_ts(v) for v in frame.index]
    current = _as_utc(now)
    # Every venue's session closes within a day of its date, so a period that
    # ended more than two days before today (UTC) is closed everywhere. This
    # keeps a 10-year daily frame from paying a calendar lookup per row.
    horizon = current.date() - dt.timedelta(days=2)
    tz = getattr(calendar, 'tz', None) if calendar is not None else None
    for i, (ts, size) in enumerate(zip(stamps, sizes)):
        if size not in DAILY_OR_LARGER:
            continue
        day = _local_date(ts, tz if calendar is not None else ts.tzinfo)
        end = _period_end(day, size)
        if end < horizon:
            continue
        if calendar is not None:
            mask[i] = _unclosed_with_calendar(day, end, calendar, current)
        else:
            mask[i] = _unclosed_without_calendar(ts, day, end, current)
    return mask


def calendar_for(contract) -> object:
    """The exchange calendar for a SecurityDefinition / Contract, or None.

    primaryExchange first (SMART is a router, not a venue), then exchange.
    """
    import exchange_calendars
    for attr in ('primaryExchange', 'exchange'):
        name = getattr(contract, attr, None)
        if not name:
            continue
        try:
            return exchange_calendars.get_calendar(name)
        except Exception:
            continue
    return None
