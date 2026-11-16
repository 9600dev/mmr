"""scripts/remove_stale_markers.py: a no-data marker beside real bars of its
own venue day is not true, and is the only thing the tool removes."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

from trader.data.duckdb_store import DuckDBConnection

REPO = Path(__file__).resolve().parents[1]
SYDNEY, NEW_YORK = 'Australia/Sydney', 'America/New_York'
BHP, CBA, SPY = '4036812', '4036818', '756733'
ZONES = {BHP: SYDNEY, CBA: SYDNEY, SPY: NEW_YORK}


def _load():
    spec = importlib.util.spec_from_file_location('remove_stale_markers', REPO / 'scripts' / 'remove_stale_markers.py')
    mod = importlib.util.module_from_spec(spec)
    sys.modules['remove_stale_markers'] = mod
    spec.loader.exec_module(mod)
    return mod


tool = _load()


@pytest.fixture
def db(tmp_path):
    conn = DuckDBConnection(str(tmp_path / 'hist.duckdb'))
    conn.execute('CREATE TABLE tick_data(symbol VARCHAR, date TIMESTAMPTZ, open DOUBLE, high DOUBLE, low DOUBLE, '
                 'close DOUBLE, volume DOUBLE, average DOUBLE, bar_count INTEGER, bar_size VARCHAR, '
                 'what_to_show INTEGER)', fetch='none')
    return conn


def _at(text, zone):
    return pd.Timestamp(text, tz=zone).tz_convert('UTC').to_pydatetime()


def _bar(db, symbol, text, zone, close=10.0, bar_size='1 min'):
    db.execute('INSERT INTO tick_data VALUES (?, ?, ?, ?, ?, ?, 100, ?, 5, ?, 1)',
               [symbol, _at(text, zone), close, close, close, close, close, bar_size], fetch='none')


def _marker(db, symbol, text, zone, bar_size='1 min'):
    db.execute('INSERT INTO tick_data VALUES (?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, 1)',
               [symbol, _at(text, zone), bar_size], fetch='none')


def _count(db):
    return db.execute('SELECT count(*) FROM tick_data', fetch='one')[0]


def _stale(db, zones=ZONES):
    return [(row[0], row[1], row[3]) for row in tool.stale_markers(db, zones)[0]]


def _monday(db, symbol=BHP):
    """2026-11-16 in Sydney: the marker written before the open, then the session."""
    _marker(db, symbol, '2026-11-16 00:00', SYDNEY)
    _bar(db, symbol, '2026-11-16 10:00', SYDNEY)
    _bar(db, symbol, '2026-11-16 16:10', SYDNEY)


def test_a_marker_on_a_day_its_instrument_traded_is_stale(db):
    _monday(db)
    assert _stale(db) == [(BHP, '1 min', 2)]
    assert _count(db) == 3                                             # finding it changes nothing


def test_apply_removes_the_marker_and_only_the_marker(db):
    _monday(db)
    _monday(db, CBA)
    stale, unjudged, before, after = tool.remove_stale_markers(db, ZONES)
    assert (len(stale), unjudged, before, after) == (2, [], 6, 4)
    assert db.execute('SELECT count(*) FROM tick_data WHERE close IS NULL', fetch='one')[0] == 0
    assert _stale(db) == []
    assert tool.remove_stale_markers(db, ZONES) == ([], [], 4, 4)      # a second run is a no-op


def test_a_weekend_marker_is_true_and_is_kept(db):
    _bar(db, BHP, '2026-11-13 16:10', SYDNEY)
    _marker(db, BHP, '2026-11-14 00:00', SYDNEY)
    _marker(db, BHP, '2026-11-15 00:00', SYDNEY)
    _bar(db, BHP, '2026-11-16 10:00', SYDNEY)
    assert _stale(db) == []
    assert tool.remove_stale_markers(db, ZONES)[0] == []
    assert _count(db) == 4


def test_the_day_is_the_venues_day_not_the_next_24_hours(db):
    """Markers written before 2026-11-06 are stamped at a time of day. A Sunday
    marker stamped 04:00 New York has Monday's pre-market bars within 24 hours
    of it and is still true of Sunday. (The first version of this tool would
    have deleted 190 such markers from the live store.)"""
    _marker(db, SPY, '2026-11-01 04:00', NEW_YORK)                     # Sunday
    _bar(db, SPY, '2026-11-02 04:00', NEW_YORK)                        # Monday, 24 h later to the minute...
    _bar(db, SPY, '2026-11-02 03:59', NEW_YORK)                        # ...and inside the 24 h
    assert _stale(db) == []
    # the same marker on a day that did trade is stale wherever in the day it is stamped
    _marker(db, SPY, '2026-11-02 09:30', NEW_YORK)
    assert _stale(db) == [(SPY, '1 min', 2)]


def test_a_daily_marker_is_judged_against_that_days_daily_bar(db):
    _marker(db, CBA, '2026-11-15 13:00', SYDNEY, bar_size='1 day')     # Sunday, stamped mid-day
    _bar(db, CBA, '2026-11-16 00:00', SYDNEY, bar_size='1 day')        # Monday's bar, 11 hours later
    assert _stale(db) == []
    _marker(db, CBA, '2026-11-16 09:00', SYDNEY, bar_size='1 day')     # a marker on Monday itself
    assert _stale(db) == [(CBA, '1 day', 1)]


def test_another_instruments_or_bar_sizes_bars_do_not_make_a_marker_stale(db):
    _marker(db, BHP, '2026-11-16 00:00', SYDNEY)
    _bar(db, CBA, '2026-11-16 10:00', SYDNEY)                          # CBA traded; says nothing of BHP
    _bar(db, BHP, '2026-11-16 00:00', SYDNEY, bar_size='1 day')        # BHP's daily bar; says nothing of 1 min
    assert _stale(db) == []


def test_the_bounds_of_the_venue_day(db):
    _marker(db, BHP, '2026-11-16 00:00', SYDNEY)
    _bar(db, BHP, '2026-11-15 23:59', SYDNEY)                          # the day before
    _bar(db, BHP, '2026-11-17 00:00', SYDNEY)                          # the day after
    assert _stale(db) == []
    _bar(db, BHP, '2026-11-16 23:59', SYDNEY)                          # the last minute of the day
    assert _stale(db) == [(BHP, '1 min', 1)]


@pytest.mark.parametrize('day, hours', [('2026-03-08', 23), ('2026-11-01', 25), ('2026-11-02', 24)])
def test_a_venue_day_is_23_or_25_hours_when_the_clocks_change(day, hours):
    start, end = tool.venue_day(pd.Timestamp(f'{day} 12:00', tz=NEW_YORK), NEW_YORK)
    assert start == pd.Timestamp(day, tz=NEW_YORK)
    assert end == pd.Timestamp(day, tz=NEW_YORK) + pd.Timedelta(hours=hours)
    assert (end.hour, end.minute) == (0, 0)


def test_two_markers_do_not_vouch_for_each_other(db):
    _marker(db, BHP, '2026-11-16 00:00', SYDNEY)
    _marker(db, BHP, '2026-11-16 10:00', SYDNEY)
    assert _stale(db) == []


def test_an_instrument_with_no_known_venue_is_not_judged(db):
    _monday(db, '999001')
    _monday(db)
    stale, unjudged = tool.stale_markers(db, ZONES)
    assert [row[0] for row in stale] == [BHP] and unjudged == ['999001']
    stale, unjudged, before, after = tool.remove_stale_markers(db, ZONES)
    assert (len(stale), unjudged, before - after) == (1, ['999001'], 1)
    assert db.execute("SELECT count(*) FROM tick_data WHERE symbol = '999001'", fetch='one')[0] == 3


def test_a_marker_stored_twice_goes_twice_and_the_count_still_adds_up(db):
    _monday(db)
    _marker(db, BHP, '2026-11-16 00:00', SYDNEY)                       # the same key again
    stale, _, before, after = tool.remove_stale_markers(db, ZONES)
    assert (len(stale), before, after) == (1, 4, 2)


def test_nothing_to_judge_is_not_an_error(db):
    assert tool.stale_markers(db, ZONES) == ([], [])
    assert tool.remove_stale_markers(db, ZONES) == ([], [], 0, 0)
