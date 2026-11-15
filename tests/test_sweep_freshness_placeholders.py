"""The sweep freshness guard dates a series by its last REAL bar.

docs/AUDIT_2026-11-14.md B5.8: `_freshness_check` took `df.index.max()` over
every stored row. The IB history worker writes an all-NULL "the source had
nothing for this day" marker row for days without a bar, so a series whose
last real daily bar is weeks old but which carries a marker for yesterday read
as current, and `sweep run` went ahead on stale data. `data status` already
excludes those rows (`DuckDBDataStore.last_bars`); this guard did not.

Dates are relative to the wall clock because `_freshness_check` reads it (as
does the unclosed-session guard in `TickData.write`, which is why nothing here
is dated today).
"""
from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import trader.mmr_cli as cli
from trader.data.bar_staleness import sessions_behind
from trader.data.data_access import TickStorage
from trader.objects import BarSize

NY = 'America/New_York'
YESTERDAY = pd.Timestamp.now(tz=NY).normalize() - pd.Timedelta(days=1)
LONG_AGO = YESTERDAY - pd.Timedelta(days=40)


def _daily(days, placeholder=False):
    index = pd.DatetimeIndex(days, name='date')
    value = float('nan') if placeholder else 10.0
    return pd.DataFrame({'open': value, 'high': value, 'low': value, 'close': value,
                         'volume': value, 'average': value,
                         'bar_count': float('nan') if placeholder else 5.0,
                         'bar_size': '1 day'}, index=index)


def _weekdays_ending(last, count):
    return list(pd.bdate_range(end=last, periods=count, tz=NY))


def _markers(count=6):
    """One no-data marker per calendar day, the newest dated yesterday."""
    return _daily([YESTERDAY - pd.Timedelta(days=n) for n in range(count)], placeholder=True)


def _day_that_is_behind(sessions):
    """The newest weekday whose daily bar is exactly `sessions` behind now."""
    for back in range(1, 30):
        day = YESTERDAY - pd.Timedelta(days=back)
        if day.dayofweek < 5 and sessions_behind(day) == sessions:
            return day
    raise AssertionError(f'no weekday is {sessions} sessions behind')


@pytest.fixture
def history(tmp_path, monkeypatch):
    path = str(tmp_path / 'history.duckdb')
    from trader.container import Container
    monkeypatch.setattr(Container, 'instance', classmethod(
        lambda cls: SimpleNamespace(config=lambda: {'history_duckdb_path': path})))
    return TickStorage(path).get_tickdata(BarSize.Days1)


def test_markers_after_the_last_real_bar_do_not_make_a_series_fresh(history):
    real = _weekdays_ending(LONG_AGO, 20)
    assert history.write(111, _daily(real)) == 20
    assert history.write(111, _markers()) == 6
    # The stored series really does end in marker rows, the newest yesterday.
    stored = history.read(111)
    assert stored.index.max().date() == YESTERDAY.date() and stored['close'].isna().sum() == 6

    stale = cli._freshness_check([111])

    assert [row['conid'] for row in stale] == [111]
    assert stale[0]['last_bar'] == str(real[-1].date())
    assert 'behind' in stale[0]['reason']


def test_a_series_of_markers_only_has_no_data(history):
    assert history.write(222, _markers()) == 6
    stale = cli._freshness_check([222])
    assert [(row['conid'], row['last_bar']) for row in stale] == [(222, None)]
    assert 'no data' in stale[0]['reason']


def test_a_current_series_is_still_fresh_with_markers_inside_it(history):
    """Markers are not themselves a finding: a weekend marker between two real
    bars changes nothing."""
    real = _weekdays_ending(YESTERDAY, 10)
    history.write(333, _daily(real))
    weekend = [day for day in pd.date_range(real[0], real[-1], tz=NY) if day.dayofweek >= 5]
    assert history.write(333, _daily(weekend, placeholder=True)) == len(weekend) > 0
    assert cli._freshness_check([333]) == []


def test_each_conid_is_judged_on_its_own_bars(history):
    history.write(1, _daily(_weekdays_ending(YESTERDAY, 5)))
    history.write(2, _daily(_weekdays_ending(LONG_AGO, 5)))
    history.write(2, _markers())
    history.write(3, _markers())
    stale = {row['conid']: row for row in cli._freshness_check([1, 2, 3, 4])}
    assert sorted(stale) == [2, 3, 4]
    assert stale[4]['last_bar'] is None and stale[3]['last_bar'] is None
    assert stale[2]['last_bar'] == str(_weekdays_ending(LONG_AGO, 1)[0].date())


def test_staleness_limit_is_unchanged_for_real_bars(history):
    """Three sessions behind passes and four does not, exactly as before —
    measured on the last real bar, with or without markers after it."""
    at_limit, past_limit = _day_that_is_behind(3), _day_that_is_behind(4)
    history.write(5, _daily(_weekdays_ending(at_limit, 5)))
    history.write(6, _daily(_weekdays_ending(past_limit, 5)))
    history.write(7, _daily(_weekdays_ending(past_limit, 5)))
    history.write(7, _markers(count=2))
    stale = cli._freshness_check([5, 6, 7])
    assert [row['conid'] for row in stale] == [6, 7]
    assert {row['last_bar'] for row in stale} == {str(past_limit.date())}
    assert all('4 trading session(s) behind (limit 3)' in row['reason'] for row in stale)
