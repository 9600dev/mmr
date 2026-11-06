"""IB no-data markers cover whole empty days only — never sit beside real intraday bars."""
import datetime as dt

import pandas as pd

from trader.listeners.ib_history_worker import missing_session_days

NY = 'America/New_York'


def _ts(s):
    return pd.Timestamp(s, tz=NY)


def test_intraday_days_with_bars_get_no_marker():
    # Real 1-min bars Mon+Tue, first bar 04:01 (no bar at exactly 04:00).
    idx = pd.DatetimeIndex([_ts('2026-11-02 04:01'), _ts('2026-11-02 15:59'),
                            _ts('2026-11-03 04:05'), _ts('2026-11-03 16:00')])
    out = missing_session_days(idx, _ts('2026-11-02 04:01').to_pydatetime(),
                               _ts('2026-11-03 20:00').to_pydatetime(), NY)
    assert list(out) == []      # old code injected a NULL bar at Tue 04:01


def test_intraday_weekend_marked_at_midnight():
    idx = pd.DatetimeIndex([_ts('2026-10-30 09:30'), _ts('2026-11-02 09:30')])
    out = missing_session_days(idx, _ts('2026-10-30 09:30').to_pydatetime(),
                               _ts('2026-11-02 16:00').to_pydatetime(), NY)
    assert list(out) == [_ts('2026-10-31'), _ts('2026-11-01')]


def test_daily_gap_marked():
    idx = pd.DatetimeIndex([_ts('2026-11-02'), _ts('2026-11-03'), _ts('2026-11-05')])
    out = missing_session_days(idx, _ts('2026-11-02').to_pydatetime(),
                               _ts('2026-11-05').to_pydatetime(), NY)
    assert list(out) == [_ts('2026-11-04')]


def test_empty_result_marks_window_days():
    end = _ts('2026-11-05 00:00').to_pydatetime()
    out = missing_session_days(pd.DatetimeIndex([]), end - dt.timedelta(days=1), end, NY)
    assert list(out) == [_ts('2026-11-04'), _ts('2026-11-05')]


def test_bars_keyed_in_other_tz_count_for_exchange_day():
    # A 21:00 ET bar is 02:00 UTC next day — still the ET day it traded.
    idx = pd.DatetimeIndex([pd.Timestamp('2026-11-03 02:00', tz='UTC')])
    out = missing_session_days(idx, _ts('2026-11-02 20:00').to_pydatetime(),
                               _ts('2026-11-02 21:00').to_pydatetime(), NY)
    assert list(out) == []
