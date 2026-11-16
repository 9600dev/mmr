"""IB no-data markers cover whole empty days only — never sit beside real intraday bars."""
import datetime as dt

import pandas as pd

from trader.listeners.ib_history_worker import missing_session_days

NY = 'America/New_York'


def _ts(s):
    return pd.Timestamp(s, tz=NY)


# The tests below are about WHICH past days get a marker; "now" is pinned well
# after them so the clock of the machine running the suite does not matter.
LATER = pd.Timestamp('2026-11-10 12:00', tz=NY)


def test_intraday_days_with_bars_get_no_marker():
    # Real 1-min bars Mon+Tue, first bar 04:01 (no bar at exactly 04:00).
    idx = pd.DatetimeIndex([_ts('2026-11-02 04:01'), _ts('2026-11-02 15:59'),
                            _ts('2026-11-03 04:05'), _ts('2026-11-03 16:00')])
    out = missing_session_days(idx, _ts('2026-11-02 04:01').to_pydatetime(),
                               _ts('2026-11-03 20:00').to_pydatetime(), NY, now=LATER)
    assert list(out) == []      # old code injected a NULL bar at Tue 04:01


def test_intraday_weekend_marked_at_midnight():
    idx = pd.DatetimeIndex([_ts('2026-10-30 09:30'), _ts('2026-11-02 09:30')])
    out = missing_session_days(idx, _ts('2026-10-30 09:30').to_pydatetime(),
                               _ts('2026-11-02 16:00').to_pydatetime(), NY, now=LATER)
    assert list(out) == [_ts('2026-10-31'), _ts('2026-11-01')]


def test_daily_gap_marked():
    idx = pd.DatetimeIndex([_ts('2026-11-02'), _ts('2026-11-03'), _ts('2026-11-05')])
    out = missing_session_days(idx, _ts('2026-11-02').to_pydatetime(),
                               _ts('2026-11-05').to_pydatetime(), NY, now=LATER)
    assert list(out) == [_ts('2026-11-04')]


def test_empty_result_marks_window_days():
    end = _ts('2026-11-05 00:00').to_pydatetime()
    out = missing_session_days(pd.DatetimeIndex([]), end - dt.timedelta(days=1), end, NY, now=LATER)
    assert list(out) == [_ts('2026-11-04'), _ts('2026-11-05')]


def test_bars_keyed_in_other_tz_count_for_exchange_day():
    # A 21:00 ET bar is 02:00 UTC next day — still the ET day it traded.
    idx = pd.DatetimeIndex([pd.Timestamp('2026-11-03 02:00', tz='UTC')])
    out = missing_session_days(idx, _ts('2026-11-02 20:00').to_pydatetime(),
                               _ts('2026-11-02 21:00').to_pydatetime(), NY, now=LATER)
    assert list(out) == []


# ── a day that is not over has not had "no data" ────────────────────────────

SYDNEY = 'Australia/Sydney'


def _syd(s):
    return pd.Timestamp(s, tz=SYDNEY)


def test_the_exchanges_current_day_is_never_marked():
    """2026-11-16: strategy_service restarted at 06:17 in Sydney, before the
    ASX opened, and asked IB for 1-min history up to "now". Monday had no bar
    yet, so it was marked as a day the source had nothing for - and the marker
    stayed beside the 372 real bars that arrived that evening."""
    friday = pd.DatetimeIndex([_syd('2026-11-13 10:00'), _syd('2026-11-13 16:10')])
    asked_at = _syd('2026-11-16 06:17')
    out = missing_session_days(friday, _syd('2026-11-13 10:00').to_pydatetime(), asked_at.to_pydatetime(),
                               SYDNEY, now=asked_at)
    assert list(out) == [_syd('2026-11-14'), _syd('2026-11-15')]        # the weekend, and not Monday


def test_a_day_is_marked_once_it_is_over():
    friday = pd.DatetimeIndex([_syd('2026-11-13 10:00'), _syd('2026-11-13 16:10')])
    window = (_syd('2026-11-13 10:00').to_pydatetime(), _syd('2026-11-16 23:59').to_pydatetime())
    still_monday = missing_session_days(friday, *window, SYDNEY, now=_syd('2026-11-16 23:59:59'))
    assert _syd('2026-11-16') not in list(still_monday)
    tuesday = missing_session_days(friday, *window, SYDNEY, now=_syd('2026-11-17 00:00'))
    assert list(tuesday) == [_syd('2026-11-14'), _syd('2026-11-15'), _syd('2026-11-16')]


def test_today_is_the_exchanges_today_not_the_callers():
    """18:00 on Sunday in New York is 10:00 on Monday in Sydney: Monday has
    begun there, so it is the day that cannot be marked; in New York's terms
    Sunday is still running."""
    now = pd.Timestamp('2026-11-15 18:00', tz=NY)
    window = (_syd('2026-11-13 00:00').to_pydatetime(), _syd('2026-11-16 12:00').to_pydatetime())
    assert list(missing_session_days(pd.DatetimeIndex([]), *window, SYDNEY, now=now)) == [
        _syd('2026-11-13'), _syd('2026-11-14'), _syd('2026-11-15')]
    ny_window = (_ts('2026-11-13 00:00').to_pydatetime(), _ts('2026-11-16 12:00').to_pydatetime())
    assert list(missing_session_days(pd.DatetimeIndex([]), *ny_window, NY, now=now)) == [
        _ts('2026-11-13'), _ts('2026-11-14')]


def test_a_window_that_runs_into_the_future_marks_nothing_there():
    now = _ts('2026-11-10 09:00')
    out = missing_session_days(pd.DatetimeIndex([]), _ts('2026-11-08').to_pydatetime(),
                               _ts('2026-11-12').to_pydatetime(), NY, now=now)
    assert list(out) == [_ts('2026-11-08'), _ts('2026-11-09')]


def test_without_a_clock_argument_it_uses_the_real_one():
    """Long ago is over; a day far in the future is not."""
    past = missing_session_days(pd.DatetimeIndex([]), _ts('2020-01-04').to_pydatetime(),
                                _ts('2020-01-05').to_pydatetime(), NY)
    assert list(past) == [_ts('2020-01-04'), _ts('2020-01-05')]
    future = missing_session_days(pd.DatetimeIndex([]), _ts('2099-01-04').to_pydatetime(),
                                  _ts('2099-01-05').to_pydatetime(), NY)
    assert list(future) == []
