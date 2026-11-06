"""`mmr --json data query` rows carry their timestamp; placeholders are hidden.

Field finding 2026-11-05: `mmr --json data query AAPL --bar-size "1 day"
--days 30` returned rows with no date at all — the bar time lived in the
DataFrame index and `to_json(orient='records')` drops the index — and among
them weekend placeholder rows whose OHLCV were all NULL, which read as garbage
and made `--tail 1` return a row with no close. `what_to_show` was a raw int.
"""
from __future__ import annotations

import datetime as dt
import json
from types import SimpleNamespace

import pandas as pd
import pytest

import trader.mmr_cli as cli
from trader.data.duckdb_store import DuckDBDataStore


def _daily(days):
    idx = pd.DatetimeIndex([pd.Timestamp(d, tz='America/New_York') for d in days])
    df = pd.DataFrame({'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.5,
                       'volume': 1000.0, 'bar_size': '1 day', 'what_to_show': 1}, index=idx)
    df.index.name = 'date'
    return df


@pytest.fixture
def query_env(tmp_path, monkeypatch):
    history = str(tmp_path / 'history.duckdb')
    main = str(tmp_path / 'main.duckdb')
    today = dt.date.today()
    # last week: Mon..Sun, Sat/Sun as all-NULL placeholders (IB worker shape)
    monday = today - dt.timedelta(days=today.weekday() + 7)
    days = [monday + dt.timedelta(days=i) for i in range(7)]
    df = _daily([d.isoformat() for d in days])
    df.loc[df.index[5:], ['open', 'high', 'low', 'close', 'volume']] = float('nan')
    DuckDBDataStore(history).write('ZZQ', df)
    from trader.container import Container
    monkeypatch.setattr(Container, 'instance', classmethod(
        lambda cls: SimpleNamespace(config=lambda: {
            'duckdb_path': main, 'history_duckdb_path': history,
            'universe_library': 'Universes'})))
    monkeypatch.setattr(cli, '_json_mode', True)
    return days


def _run(capsys, **kw):
    args = SimpleNamespace(symbol='ZZQ', bar_size='1 day', days=30, tail=None,
                           include_placeholders=False)
    for k, v in kw.items():
        setattr(args, k, v)
    cli._handle_data_query(args)
    # the last line: rich logging may share stdout under pytest
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_every_json_row_has_an_iso_timestamp_with_offset(query_env, capsys):
    out = _run(capsys)
    assert out['data'], 'expected rows'
    for row in out['data']:
        ts = dt.datetime.fromisoformat(row['date'])
        assert ts.tzinfo is not None
    weekdays = [d.isoformat() for d in query_env[:5]]
    assert [r['date'][:10] for r in out['data']] == weekdays
    assert list(out['data'][0])[0] == 'date'


def test_placeholders_are_dropped_by_default_and_counted(query_env, capsys):
    out = _run(capsys)
    assert len(out['data']) == 5
    assert out['placeholders_dropped'] == 2
    assert all(r['close'] is not None for r in out['data'])


def test_tail_one_returns_a_real_bar(query_env, capsys):
    """The mmr-skill helper reads the last close via `--tail 1`; a trailing
    weekend placeholder made it fall back to a paid vendor call."""
    out = _run(capsys, tail=1)
    assert out['data'][0]['close'] == 10.5
    assert out['data'][0]['date'][:10] == query_env[4].isoformat()


def test_include_placeholders_keeps_them(query_env, capsys):
    out = _run(capsys, include_placeholders=True)
    assert len(out['data']) == 7
    assert out['placeholders_dropped'] == 0
    assert [r['close'] for r in out['data'][5:]] == [None, None]


def test_what_to_show_is_rendered_as_enum_name(query_env, capsys):
    out = _run(capsys)
    assert {r['what_to_show'] for r in out['data']} == {'TRADES'}


@pytest.mark.parametrize('value,expected', [
    (1, 'TRADES'), (2, 'MIDPOINT'), (3, 'BID'), (4, 'ASK'), (1.0, 'TRADES'),
    (None, None), (float('nan'), None), (9, 'UNKNOWN(9)')])
def test_what_to_show_name(value, expected):
    assert cli._what_to_show_name(value) == expected


def test_a_row_with_only_some_ohlc_missing_is_not_a_placeholder():
    idx = pd.DatetimeIndex([pd.Timestamp('2026-11-02', tz='America/New_York')])
    df = pd.DataFrame({'open': [float('nan')], 'high': [1.0], 'low': [1.0],
                       'close': [1.0], 'volume': [1.0]}, index=idx)
    out, dropped = cli._query_output_frame(df)
    assert (len(out), dropped) == (1, 0)


def test_parser_accepts_include_placeholders():
    parser = cli.build_parser() if hasattr(cli, 'build_parser') else None
    if parser is None:
        pytest.skip('no parser factory exported')
    args = parser.parse_args(['data', 'query', 'AAPL', '--include-placeholders'])
    assert args.include_placeholders is True
