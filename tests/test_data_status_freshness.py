"""`data status` reports the STALEST member of a job, not the freshest.

Field finding 2026-11-05: `mmr data status` showed the US 1-min job as
"514/514 coverage, last bar 2026-11-05, stale 0d". In truth 13 symbols had
today's bars and 469 had ended on 2026-10-22. The handler computed per-symbol
last bars and then reported max() of them, so ONE fresh member made the whole
universe read current. Coverage also counted members whose only rows were
all-NULL placeholders.
"""
from __future__ import annotations

import datetime as dt
import json
from types import SimpleNamespace

import exchange_calendars
import pandas as pd
import pytest
from ib_async import Contract, ContractDetails

import trader.mmr_cli as cli
from trader.data.bar_staleness import sessions_behind
from trader.data.data_access import SecurityDefinition
from trader.data.duckdb_store import DuckDBDataStore
from trader.data.universe import Universe, UniverseAccessor

UTC = dt.timezone.utc
# Thursday 2026-11-05 14:30 ET, mid-session (EST, UTC-5).
NOW = dt.datetime(2026, 11, 5, 19, 30, tzinfo=UTC)


def _definition(conid, symbol, primary='NASDAQ'):
    return SecurityDefinition.from_contract_details(ContractDetails(contract=Contract(
        conId=conid, exchange='SMART', primaryExchange=primary, secType='STK',
        symbol=symbol, currency='USD')))


def _minute_bars(day, n=3, placeholder=False):
    idx = pd.date_range(f'{day} 09:30', periods=n, freq='min', tz='America/New_York')
    value = float('nan') if placeholder else 10.0
    df = pd.DataFrame({'open': value, 'high': value, 'low': value, 'close': value,
                       'volume': value, 'bar_size': '1 min'}, index=idx)
    df.index.name = 'date'
    return df


def _xnys(sd):
    return exchange_calendars.get_calendar('XNYS')


class TestJobFreshnessIsPerMember:
    def _shape(self):
        """The 2026-11-05 shape in miniature: 2 current, 5 stuck on 10-22."""
        defs = [_definition(i, f'S{i}') for i in range(1, 8)]
        last = {}
        for i in (1, 2):
            last[str(i)] = dt.datetime(2026, 11, 5, 19, 29, tzinfo=UTC)
        for i in range(3, 8):
            last[str(i)] = dt.datetime(2026, 10, 22, 23, 59, tzinfo=UTC)
        return defs, last

    def test_the_stale_majority_is_reported(self):
        defs, last = self._shape()
        row = cli._job_freshness(defs, last, '1 min', NOW, calendar_for=_xnys)
        assert (row['covered'], row['total']) == (7, 7)
        assert row['last_bar_newest'].startswith('2026-11-05')
        assert row['last_bar_oldest'].startswith('2026-10-22')
        assert row['stale_count'] == 5
        assert {m['symbol'] for m in row['stale_members']} == {f'S{i}' for i in range(3, 8)}
        # 10-23 .. 11-05 is ten XNYS sessions
        assert row['stale_days_max'] == 10
        assert row['stale_days_newest'] == 0

    def test_a_universe_that_is_all_current_has_no_stale_members(self):
        defs = [_definition(1, 'A'), _definition(2, 'B')]
        # Yesterday's last bar during today's session is the steady state.
        last = {'1': dt.datetime(2026, 11, 4, 21, 0, tzinfo=UTC),
                '2': dt.datetime(2026, 11, 5, 19, 0, tzinfo=UTC)}
        row = cli._job_freshness(defs, last, '1 min', NOW, calendar_for=_xnys)
        assert row['stale_count'] == 0 and row['stale_members'] == []

    def test_members_without_data_are_uncovered_not_fresh(self):
        defs = [_definition(1, 'A'), _definition(2, 'B')]
        last = {'1': dt.datetime(2026, 11, 5, 19, 0, tzinfo=UTC)}
        row = cli._job_freshness(defs, last, '1 day', NOW, calendar_for=_xnys)
        assert (row['covered'], row['uncovered'], row['total']) == (1, 1, 2)
        assert row['uncovered_members'] == [{'conId': 2, 'symbol': 'B'}]

    def test_empty_universe(self):
        row = cli._job_freshness([], {}, '1 day', NOW)
        assert row['covered'] == row['total'] == row['stale_count'] == 0
        assert row['last_bar_oldest'] is None and row['stale_days_max'] is None

    @pytest.mark.parametrize('bar_size,threshold', [
        ('1 min', 1), ('5 mins', 1), ('1 hour', 1), ('1 day', 1), ('1 week', 5), ('1 month', 23)])
    def test_threshold_by_bar_size(self, bar_size, threshold):
        assert cli._staleness_threshold_sessions(bar_size) == threshold


class TestCalendarSessions:
    def test_holiday_is_not_a_missed_session(self):
        """Thanksgiving 2026 is Thu 11-26. Wed's bar read on Fri is current
        on the XNYS calendar; the weekday fallback calls it 2 behind."""
        wed = dt.datetime(2026, 11, 25, 5, 0, tzinfo=UTC)   # Wed 00:00 ET daily bar
        fri = dt.datetime(2026, 11, 27, 15, 0, tzinfo=UTC)
        cal = exchange_calendars.get_calendar('XNYS')
        assert sessions_behind(wed, fri, calendar=cal) == 1
        assert sessions_behind(wed, fri) == 2

    def test_asx_bar_is_dated_in_sydney(self):
        """An ASX daily bar at Sydney midnight is the PREVIOUS day in UTC."""
        cal = exchange_calendars.get_calendar('XASX')
        bar = pd.Timestamp('2026-11-05 00:00', tz='Australia/Sydney').to_pydatetime()
        later_same_day = pd.Timestamp('2026-11-05 17:00', tz='Australia/Sydney').to_pydatetime()
        assert sessions_behind(bar, later_same_day, calendar=cal) == 0

    def test_out_of_calendar_span_falls_back_to_weekdays(self):
        cal = exchange_calendars.get_calendar('XNYS')
        ancient = dt.datetime(1990, 1, 2, tzinfo=UTC)
        assert sessions_behind(ancient, NOW, calendar=cal) == sessions_behind(ancient, NOW)


def test_handler_end_to_end_on_tmp_db(tmp_path, monkeypatch, capsys):
    history = str(tmp_path / 'history.duckdb')
    main = str(tmp_path / 'main.duckdb')
    store = DuckDBDataStore(history)
    store.write('1', _minute_bars('2026-11-05'))
    store.write('2', _minute_bars('2026-10-22'))
    store.write('3', _minute_bars('2026-10-22'))
    # 4 has only a placeholder for today: not evidence of freshness.
    store.write('4', _minute_bars('2026-11-05', placeholder=True))
    accessor = UniverseAccessor(main, 'Universes')
    accessor.update(Universe('us', [_definition(i, f'S{i}') for i in (1, 2, 3, 4)]))

    yaml_path = tmp_path / 'data_refresh.yaml'
    yaml_path.write_text('jobs:\n  us_1min:\n    universe: us\n    bar_size: "1 min"\n    days: 30\n')
    monkeypatch.setenv('DATA_REFRESH_CONFIG', str(yaml_path))
    from trader.container import Container
    monkeypatch.setattr(Container, 'instance', classmethod(
        lambda cls: SimpleNamespace(config=lambda: {
            'duckdb_path': main, 'history_duckdb_path': history,
            'universe_library': 'Universes'})))

    monkeypatch.setattr(cli, '_json_mode', True)
    cli._handle_data_status(now=NOW)

    row = json.loads(capsys.readouterr().out)['data'][0]
    assert row['job'] == 'us_1min'
    assert (row['covered'], row['uncovered'], row['total']) == (3, 1, 4)
    assert row['last_bar_newest'].startswith('2026-11-05')
    assert row['last_bar_oldest'].startswith('2026-10-22')
    assert row['stale_count'] == 2
    assert row['stalest_symbol'] in ('S2', 'S3')
    assert [m['symbol'] for m in row['uncovered_members']] == ['S4']
    for key in ('stale_members', 'stale_threshold_sessions', 'stale_days_max',
                'stale_days_newest'):
        assert key in row


def test_rich_table_shows_stalest_date_and_stale_count(tmp_path, monkeypatch):
    history = str(tmp_path / 'history.duckdb')
    main = str(tmp_path / 'main.duckdb')
    store = DuckDBDataStore(history)
    store.write('1', _minute_bars('2026-10-22'))
    accessor = UniverseAccessor(main, 'Universes')
    accessor.update(Universe('us', [_definition(1, 'S1')]))
    yaml_path = tmp_path / 'data_refresh.yaml'
    yaml_path.write_text('jobs:\n  j:\n    universe: us\n    bar_size: "1 min"\n')
    monkeypatch.setenv('DATA_REFRESH_CONFIG', str(yaml_path))
    from trader.container import Container
    monkeypatch.setattr(Container, 'instance', classmethod(
        lambda cls: SimpleNamespace(config=lambda: {
            'duckdb_path': main, 'history_duckdb_path': history})))
    monkeypatch.setattr(cli, '_json_mode', False)
    printed = []
    monkeypatch.setattr(cli.console, 'print', lambda obj, *a, **k: printed.append(obj))
    cli._handle_data_status(now=NOW)
    table = printed[0]
    headers = [c.header for c in table.columns]
    assert 'stalest bar' in headers and 'stale' in headers
    cells = {c.header: list(c.cells) for c in table.columns}
    assert '2026-10-22' in cells['stalest bar'][0]
    assert cells['stale'][0].count('1 (>1)') == 1
