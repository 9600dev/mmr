"""`data summary` is ONE grouped query, and it is correct per bar size.

Field finding 2026-11-05 (28 GB history DB, 1,144 series): `mmr data summary`
took 5m25s. It looped every bar size x every symbol and, per pair, issued
min_date + max_date — each its own DuckDB connection open/close — plus a
universe re-read per conId. It was also WRONG: list_symbols() and
min_date()/max_date() never filtered on bar size, so a symbol holding only
daily bars was listed under "1 min" with daily extents (AUDIT_2026-07).

The oracle here is an independent per-(symbol, bar_size) computation over the
same tmp DuckDB, not a restatement of the grouped SQL.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd
import pytest
from ib_async import Contract, ContractDetails

import trader.mmr_cli as cli
from trader.data.data_access import SecurityDefinition, TickStorage
from trader.data.duckdb_store import DuckDBConnection, DuckDBDataStore
from trader.data.universe import Universe, UniverseAccessor


def _definition(conid, symbol, sec_type='STK'):
    return SecurityDefinition.from_contract_details(ContractDetails(contract=Contract(
        conId=conid, exchange='SMART', primaryExchange='NASDAQ', secType=sec_type,
        symbol=symbol, currency='USD')))


def _bars(start, periods, freq, bar_size, placeholders_at=()):
    idx = pd.date_range(start, periods=periods, freq=freq, tz='America/New_York')
    df = pd.DataFrame({
        'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.5, 'volume': 100.0,
        'bar_size': bar_size,
    }, index=idx)
    df.index.name = 'date'
    for i in placeholders_at:
        df.iloc[i, df.columns.get_indexer(['open', 'high', 'low', 'close', 'volume'])] = float('nan')
    return df


@pytest.fixture
def history(tmp_path):
    path = str(tmp_path / 'history.duckdb')
    store = DuckDBDataStore(path)
    # conId 1001: daily + 1-min; trailing daily placeholder must not set `end`.
    store.write('1001', _bars('2026-10-01', 10, 'D', '1 day', placeholders_at=(0, 9)))
    store.write('1001', _bars('2026-10-22 09:30', 30, 'min', '1 min'))
    # conId 1002: daily only. Must NOT appear under 1 min (the old bug).
    store.write('1002', _bars('2026-09-01', 5, 'D', '1 day'))
    # A raw-ticker key from a pre-registration `data download`.
    store.write('ZZZT', _bars('2026-08-03', 3, 'D', '1 day'))
    # A conId the catalogue does not know.
    store.write('999', _bars('2026-08-03', 2, 'D', '1 day'))
    main = str(tmp_path / 'main.duckdb')
    accessor = UniverseAccessor(main, 'Universes')
    accessor.update(Universe('us', [_definition(1001, 'AAA'), _definition(1002, 'BBB')]))
    return path, main, store, accessor


def _oracle(store, symbol, bar_size):
    rows = store._db.execute(
        'SELECT MIN(date), MAX(date), COUNT(*), COUNT(close) FROM tick_data '
        'WHERE symbol = ? AND bar_size = ? AND close IS NOT NULL',
        [symbol, bar_size], fetch='one')
    total = store._db.execute(
        'SELECT COUNT(*) FROM tick_data WHERE symbol = ? AND bar_size = ?',
        [symbol, bar_size], fetch='one')[0]
    return pd.Timestamp(rows[0]).tz_convert('UTC'), pd.Timestamp(rows[1]).tz_convert('UTC'), total, rows[3]


def test_grouped_inventory_matches_per_series_oracle(history):
    path, _, store, _ = history
    inv = TickStorage(path).series_summary()
    pairs = {(r.symbol, r.bar_size) for r in inv.itertuples()}
    assert pairs == {('1001', '1 day'), ('1001', '1 min'), ('1002', '1 day'),
                     ('ZZZT', '1 day'), ('999', '1 day')}
    for r in inv.itertuples():
        start, end, total, bars = _oracle(store, r.symbol, r.bar_size)
        assert (r.start, r.end, r.rows, r.bars) == (start, end, total, bars)


def test_daily_only_symbol_is_not_listed_under_one_minute(history):
    """The fabricated-coverage bug: 1002 has no 1-min bars at all."""
    path, main, _, accessor = history
    df = cli._data_summary_frame(TickStorage(path), accessor)
    assert df[(df['conId'] == '1002')]['bar_size'].tolist() == ['1 day']
    one_min = df[df['bar_size'] == '1 min']
    assert one_min['symbol'].tolist() == ['AAA']
    # and its extent is the 1-min extent, not the daily one
    assert one_min['start'].iloc[0] == pd.Timestamp('2026-10-22 09:30', tz='America/New_York')
    assert one_min['end'].iloc[0] == pd.Timestamp('2026-10-22 09:59', tz='America/New_York')


def test_placeholders_do_not_set_the_extent_but_are_counted(history):
    path, _, _, accessor = history
    df = cli._data_summary_frame(TickStorage(path), accessor)
    row = df[(df['conId'] == '1001') & (df['bar_size'] == '1 day')].iloc[0]
    assert row['start'] == pd.Timestamp('2026-10-02', tz='America/New_York')
    assert row['end'] == pd.Timestamp('2026-10-09', tz='America/New_York')
    assert (row['rows'], row['placeholders']) == (10, 2)


def test_symbol_resolution_covers_conids_tickers_and_unknowns(history):
    path, _, _, accessor = history
    df = cli._data_summary_frame(TickStorage(path), accessor)
    by_key = {(r.conId, r.symbol) for r in df.itertuples()}
    assert ('1001', 'AAA') in by_key and ('1002', 'BBB') in by_key
    assert ('', 'ZZZT') in by_key          # ticker key: shown, conId empty
    assert ('999', '999') in by_key        # unknown conId: shown as-is


def test_symbol_map_prefers_stk_like_resolve_symbol(tmp_path):
    accessor = UniverseAccessor(str(tmp_path / 'm.duckdb'), 'Universes')
    accessor.update(Universe('a', [_definition(7, 'SEVEN_OPT', 'OPT')]))
    accessor.update(Universe('b', [_definition(7, 'SEVEN', 'STK')]))
    expected = accessor.resolve_symbol(7, first_only=True)[0].symbol
    assert cli._conid_symbol_map(accessor)[7] == expected == 'SEVEN'


def _count_connections(monkeypatch):
    calls = []
    real = DuckDBConnection.execute_atomic

    def counting(self, fn):
        calls.append(self.db_path)
        return real(self, fn)
    monkeypatch.setattr(DuckDBConnection, 'execute_atomic', counting)
    return calls


def test_query_count_does_not_grow_with_the_number_of_series(tmp_path, monkeypatch):
    """5m25s came from O(series) connection round-trips. Prove O(1)."""
    def run(n_series):
        path = str(tmp_path / f'h{n_series}.duckdb')
        store = DuckDBDataStore(path)
        defs = []
        for i in range(n_series):
            store.write(str(5000 + i), _bars('2026-10-01', 3, 'D', '1 day'))
            store.write(str(5000 + i), _bars('2026-10-01 09:30', 3, 'min', '1 min'))
            defs.append(_definition(5000 + i, f'S{i}'))
        accessor = UniverseAccessor(str(tmp_path / f'm{n_series}.duckdb'), 'Universes')
        accessor.update(Universe('u', defs))
        storage = TickStorage(path)
        calls = _count_connections(monkeypatch)
        df = cli._data_summary_frame(storage, accessor)
        monkeypatch.undo()
        assert len(df) == 2 * n_series
        return len(calls)

    small, large = run(2), run(40)
    assert small == large
    assert large <= 6


def test_json_output_carries_offsets_and_row_counts(history, monkeypatch, capsys):
    path, main, _, _ = history
    from trader.container import Container
    monkeypatch.setattr(Container, 'instance', classmethod(
        lambda cls: SimpleNamespace(config=lambda: {
            'duckdb_path': main, 'history_duckdb_path': path,
            'universe_library': 'Universes'})))
    monkeypatch.setattr(cli, '_json_mode', True)
    cli._handle_data_summary()
    out = json.loads(capsys.readouterr().out)
    rows = {(r['conId'] or r['symbol'], r['bar_size']): r for r in out['data']}
    r = rows[('1001', '1 min')]
    assert set(r) == {'conId', 'symbol', 'bar_size', 'start', 'end', 'rows', 'placeholders'}
    assert r['start'] == '2026-10-22T09:30:00-04:00'
    assert pd.Timestamp(r['end']).tzinfo is not None
    assert r['rows'] == 30


def test_empty_history_db_prints_empty_table(tmp_path, monkeypatch, capsys):
    from trader.container import Container
    path = str(tmp_path / 'empty.duckdb')
    monkeypatch.setattr(Container, 'instance', classmethod(
        lambda cls: SimpleNamespace(config=lambda: {
            'duckdb_path': str(tmp_path / 'main.duckdb'), 'history_duckdb_path': path})))
    monkeypatch.setattr(cli, '_json_mode', True)
    cli._handle_data_summary()
    assert json.loads(capsys.readouterr().out)['data'] == []
