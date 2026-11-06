"""`history massive|twelvedata|ib` under --json, and why one symbol enqueues two.

Field finding 2026-11-05: `mmr --json history ib --symbol AAPL --bar_size
"1 day" --prev_days 15` printed plain text ("Pulling IB history via
data_service... Done: 2 enqueued, 2 completed, 0 failed") and the count looked
like a duplicate job for one symbol and one bar size.

It is not a duplicate: DataService enqueues one task per CONTIGUOUS missing
date range, and AAPL's window had two gaps. What was wrong is that nothing
said so. The summary now lists each task with its range.

Also pinned here: the window used to be computed from the host's naive local
clock, so on a host west of the venue "yesterday 23:59:59" local landed on
TODAY in New York — which is how a still-forming daily bar got requested.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time
from types import SimpleNamespace

import pandas as pd
import pytest
from ib_async import Contract, ContractDetails

import trader.mmr_cli as cli
from trader.common.helpers import dateify, timezoneify
from trader.data.data_access import SecurityDefinition, TickStorage
from trader.data_service import DataService
from trader.objects import BarSize

UTC = dt.timezone.utc
DURING = dt.datetime(2026, 11, 5, 19, 30, tzinfo=UTC)   # Thu 14:30 ET


def _definition(conid=265598, symbol='AAPL'):
    return SecurityDefinition.from_contract_details(ContractDetails(contract=Contract(
        conId=conid, exchange='SMART', primaryExchange='NASDAQ', secType='STK',
        symbol=symbol, currency='USD'), timeZoneId='US/Eastern'))


def _daily(days):
    idx = pd.DatetimeIndex([pd.Timestamp(d, tz='America/New_York') for d in days])
    df = pd.DataFrame({'open': 1.0, 'high': 1.0, 'low': 1.0, 'close': 1.0, 'volume': 1.0,
                       'bar_size': '1 day'}, index=idx)
    df.index.name = 'date'
    return df


@pytest.fixture
def svc(tmp_path):
    path = str(tmp_path / 'history.duckdb')
    return DataService(duckdb_path=str(tmp_path / 'main.duckdb'), history_duckdb_path=path), path


class TestPlanning:
    def test_two_gaps_are_two_tasks_not_a_duplicate(self, svc):
        service, path = svc
        td = TickStorage(path).get_tickdata(BarSize.Days1)
        # Stored: everything in the window except 10-26..10-27 and 11-03..11-04
        sessions = [d.date().isoformat() for d in pd.bdate_range('2026-10-19', '2026-11-04')]
        held = [d for d in sessions if d not in ('2026-10-26', '2026-10-27', '2026-11-03', '2026-11-04')]
        td.write(_definition(), _daily(held), now=DURING)
        plan = service._plan_downloads([_definition()], td, prev_days=15, now=DURING)
        ranges = [(str(dr.start), str(dr.end)) for _, dr in plan]
        assert ranges == [('2026-10-26', '2026-10-27'), ('2026-11-03', '2026-11-04')]

    def test_window_never_reaches_today_whatever_the_host_timezone(self, svc, monkeypatch):
        service, path = svc
        td = TickStorage(path).get_tickdata(BarSize.Days1)
        for host_tz in ('America/Vancouver', 'UTC', 'America/New_York', 'Australia/Sydney'):
            monkeypatch.setenv('TZ', host_tz)
            time.tzset()
            plan = service._plan_downloads([_definition()], td, prev_days=3, now=DURING)
            assert plan, host_tz
            assert max(str(dr.end) for _, dr in plan) == '2026-11-04', host_tz
        monkeypatch.delenv('TZ')
        time.tzset()

    def test_the_old_host_local_window_did_reach_today_on_a_pacific_host(self, monkeypatch):
        """Root cause pin: the expression pull_* used before."""
        monkeypatch.setenv('TZ', 'America/Vancouver')
        time.tzset()
        try:
            host_now = DURING.astimezone().replace(tzinfo=None)   # naive local, as dt.datetime.now()
            end_date = dateify(host_now - dt.timedelta(days=1), make_eod=True)
            tz_end = timezoneify(end_date, timezone='US/Eastern')
            assert tz_end.date() == dt.date(2026, 11, 5)   # today in New York
        finally:
            monkeypatch.delenv('TZ')
            time.tzset()

    def test_naive_now_is_refused(self, svc):
        service, path = svc
        td = TickStorage(path).get_tickdata(BarSize.Days1)
        with pytest.raises(ValueError):
            service._plan_downloads([_definition()], td, 3, now=dt.datetime(2026, 11, 5))


def test_pull_reports_symbols_and_per_task_ranges(svc, monkeypatch):
    import asyncio
    service, path = svc
    monkeypatch.setattr(service, '_resolve_symbols', lambda s, u: [_definition()])

    async def fake_one(sem, security, dr, bs, td, client_id):
        return {'symbol': security.symbol, 'start': str(dr.start), 'end': str(dr.end),
                'rows': 2, 'ok': True, 'client_id': client_id}
    monkeypatch.setattr(service, '_download_ib_one', fake_one)
    monkeypatch.setattr(service, '_plan_downloads', lambda secs, td, days, now=None: [
        (secs[0], SimpleNamespace(start=dt.date(2026, 10, 26), end=dt.date(2026, 10, 27))),
        (secs[0], SimpleNamespace(start=dt.date(2026, 11, 3), end=dt.date(2026, 11, 4)))])
    result = asyncio.run(service.pull_ib(symbols=['AAPL'], bar_size='1 day', prev_days=15,
                                         ib_client_id=10))
    assert (result['enqueued'], result['completed'], result['failed']) == (2, 2, 0)
    assert result['symbols'] == ['AAPL']
    assert [t['client_id'] for t in result['tasks']] == [10, 11]     # unique per task
    assert [t['start'] for t in result['tasks']] == ['2026-10-26', '2026-11-03']


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def _result(**kw):
    base = {'enqueued': 2, 'completed': 2, 'failed': 0, 'errors': [], 'symbols': ['AAPL'],
            'tasks': [{'symbol': 'AAPL', 'start': '2026-10-26', 'end': '2026-10-27', 'rows': 2, 'ok': True},
                      {'symbol': 'AAPL', 'start': '2026-11-03', 'end': '2026-11-04', 'rows': 2, 'ok': True}]}
    base.update(kw)
    return base


def _args(action, **kw):
    ns = SimpleNamespace(hist_action=action, symbol='AAPL', universe=None, bar_size='1 day',
                         prev_days=15, ib_client_id=10)
    for k, v in kw.items():
        setattr(ns, k, v)
    return ns


@pytest.fixture
def json_cli(monkeypatch):
    monkeypatch.setattr(cli, '_json_mode', True)
    monkeypatch.setattr(cli, '_exit_code', 0)


@pytest.mark.parametrize('action,method', [
    ('ib', 'pull_ib'), ('massive', 'pull_massive'), ('twelvedata', 'pull_twelvedata'),
    ('td', 'pull_twelvedata')])
def test_json_mode_prints_one_json_document(json_cli, capsys, action, method):
    mmr = SimpleNamespace(**{method: lambda **kw: _result()})
    cli._handle_history(mmr, _args(action))
    out = capsys.readouterr().out
    doc = json.loads(out)                      # the whole stdout is one JSON document
    assert 'Pulling' not in out and 'Done:' not in out
    assert doc['success'] is True
    assert doc['source'] == ('twelvedata' if action == 'td' else action)
    assert (doc['enqueued'], doc['completed'], doc['failed']) == (2, 2, 0)
    assert doc['symbols'] == ['AAPL'] and doc['rows_written'] == 4
    assert len(doc['tasks']) == 2
    assert cli._exit_code == 0


def test_json_failure_sets_success_false_and_exit_code(json_cli, capsys):
    mmr = SimpleNamespace(pull_ib=lambda **kw: _result(
        enqueued=0, completed=0, errors=['no securities resolved'], symbols=[], tasks=[]))
    cli._handle_history(mmr, _args('ib'))
    doc = json.loads(capsys.readouterr().out)
    assert doc['success'] is False and 'no securities resolved' in doc['message']
    assert cli._exit_code == 1


def test_plain_mode_still_prints_progress_and_ranges(monkeypatch):
    monkeypatch.setattr(cli, '_json_mode', False)
    printed = []
    monkeypatch.setattr(cli.console, 'print', lambda obj, *a, **k: printed.append(str(obj)))
    cli._handle_history(SimpleNamespace(pull_ib=lambda **kw: _result()), _args('ib'))
    text = '\n'.join(printed)
    assert 'Pulling IB history' in text and 'Done: 2 enqueued' in text
    assert '2026-10-26' in text and '2026-11-03' in text
