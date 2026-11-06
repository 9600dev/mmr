"""IB history requests are sized to the requested window, not a fixed 10-year chunk."""
import datetime as dt

from trader.listeners.ib_history_worker import window_duration

E = dt.datetime(2026, 11, 5, 16, 0, tzinfo=dt.timezone.utc)


def test_short_daily_window_requests_days_not_ten_years():
    assert window_duration('10 Y', E - dt.timedelta(days=15), E) == '17 D'


def test_long_daily_window_keeps_chunk():
    assert window_duration('10 Y', E - dt.timedelta(days=900), E) == '10 Y'
    assert window_duration('10 Y', E - dt.timedelta(days=364), E) == '10 Y'  # 366 D > IB's 365 cap


def test_intraday_chunks():
    assert window_duration('1 W', E - dt.timedelta(days=2), E) == '4 D'
    assert window_duration('1 W', E - dt.timedelta(days=30), E) == '1 W'
    assert window_duration('86400 S', E - dt.timedelta(hours=3), E) == '86400 S'


def test_unparseable_step_unchanged():
    assert window_duration('weird', E - dt.timedelta(days=1), E) == 'weird'
