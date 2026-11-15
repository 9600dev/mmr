"""A universe that cannot be READ is never replaced by a new one.

Three CLI paths register a listing in the `downloads` universe like this:

    try:
        universe = accessor.get('downloads')
    except Exception:
        universe = None
    if universe is None:
        universe = Universe('downloads', [the one new definition])
    accessor.update(universe)

`accessor.get` returns an EMPTY universe for a name that does not exist — it
never raises for that — so the `except` only ever fires when the stored
universe could not be read, and what follows replaces it with a universe of
one. `downloads` holds every listing the nightly refresh fills.

That was reachable before only through a database error. It matters more now
that the object store refuses a blob holding a type it does not write (see
tests/test_object_store_restricted_load.py): the refusal must stay a refusal,
not become the reason the stored universe is overwritten.
"""
import argparse
import dataclasses
from pathlib import Path
from types import SimpleNamespace

import dill
import numpy as np
import pandas as pd
import pytest

import trader.mmr_cli as cli
from trader.data.data_access import SecurityDefinition
from trader.data.duckdb_store import DuckDBConnection, DuckDBDataStore, DuckDBObjectStore
from trader.data.universe import Universe, UniverseAccessor
from trader.messaging.clientserver import DillDeserializationError


def _definition(con_id, symbol):
    values = {}
    for field in dataclasses.fields(SecurityDefinition):
        name = field.type if isinstance(field.type, str) else field.type.__name__
        values[field.name] = {'str': '', 'int': 0, 'float': 0.0, 'bool': False}[name]
    values.update(conId=con_id, symbol=symbol, secType='STK', exchange='SMART',
                  primaryExchange='NASDAQ', currency='USD')
    return SecurityDefinition(**values)


AAPL = _definition(265598, 'AAPL')


def _store_unreadable_downloads(db_path) -> bytes:
    """A `downloads` blob the store refuses: one definition's conId is a
    numpy scalar. Placed as a row, as an older writer or a restore would."""
    members = [_definition(n, f'S{n}') for n in range(1, 40)] + [_definition(np.int64(4391), 'AMD')]
    blob = dill.dumps(Universe('downloads', members))
    DuckDBObjectStore(db_path)
    DuckDBConnection.get_instance(db_path).execute(
        'INSERT OR REPLACE INTO object_store (key, data) VALUES (?, ?)', ['downloads', blob], fetch='none')
    with pytest.raises(DillDeserializationError):
        UniverseAccessor(db_path, 'Universes').get('downloads')
    return blob


def _stored_downloads(db_path) -> bytes:
    row = DuckDBConnection.get_instance(db_path).execute(
        "SELECT data FROM object_store WHERE key = 'downloads'", fetch='one')
    return bytes(row[0])


@pytest.fixture
def databases(tmp_path, monkeypatch):
    main, history = str(tmp_path / 'main.duckdb'), str(tmp_path / 'history.duckdb')
    from trader.container import Container
    monkeypatch.setattr(Container, 'instance', classmethod(lambda cls: SimpleNamespace(config=lambda: {
        'duckdb_path': main, 'history_duckdb_path': history, 'universe_library': 'Universes',
        'zmq_rpc_server_address': 'tcp://127.0.0.1', 'zmq_rpc_server_port': 1})))
    monkeypatch.setattr(cli, '_json_mode', True)
    return SimpleNamespace(main=main, history=history)


class _Service:
    """Stands in for the trader_service RPC client the handlers open."""

    def __init__(self, *args, **kwargs):
        self._rpc = SimpleNamespace(rpc=lambda: SimpleNamespace(resolve_contract=lambda contract: [AAPL]))

    def connect(self):
        pass

    def close(self):
        pass

    def resolve(self, symbol):
        return [AAPL]

    def discover_listings(self, symbol, exchange='', currency=''):
        return [AAPL]


# --------------------------------------------------------------------------
# `data download SYMBOL` (the resolver every ad-hoc download goes through)
# --------------------------------------------------------------------------

def test_download_listing_does_not_replace_a_universe_it_could_not_read():
    class _Accessor:
        updated = []

        def resolve_symbol(self, symbol):
            return []

        def get(self, name):
            raise DillDeserializationError('object_store key downloads was not loaded')

        def update(self, universe):
            self.updated.append(universe)

    accessor = _Accessor()
    with pytest.raises(DillDeserializationError):
        cli._resolve_download_listing('AAPL', 'twelvedata', accessor, _Service())
    assert accessor.updated == []


def test_download_listing_on_a_real_store(databases):
    before = _store_unreadable_downloads(databases.main)
    accessor = UniverseAccessor(databases.main, 'Universes')
    with pytest.raises(DillDeserializationError):
        cli._resolve_download_listing('AAPL', 'twelvedata', accessor, _Service())
    assert _stored_downloads(databases.main) == before


def test_download_listing_still_creates_and_extends_a_readable_universe(databases):
    """What the swallowed exception was never needed for."""
    accessor = UniverseAccessor(databases.main, 'Universes')
    found, refusal, registered = cli._resolve_download_listing('AAPL', 'twelvedata', accessor, _Service())
    assert (found, refusal, registered) == (AAPL, '', True)
    assert [d.conId for d in accessor.get('downloads').security_definitions] == [265598]

    other = _definition(272093, 'MSFT')
    service = _Service()
    service.discover_listings = lambda symbol, exchange='', currency='': [other]
    cli._resolve_download_listing('MSFT', 'twelvedata', accessor, service)
    assert [d.conId for d in accessor.get('downloads').security_definitions] == [265598, 272093]


# --------------------------------------------------------------------------
# `data migrate-symbols`
# --------------------------------------------------------------------------

def _ticker_keyed_bars(history):
    index = pd.date_range('2026-11-02', periods=3, freq='D', tz='America/New_York', name='date')
    DuckDBDataStore(history).write('AAPL', pd.DataFrame(
        {'open': 270.0, 'high': 271.0, 'low': 269.0, 'close': 270.5, 'volume': 1e6, 'bar_size': '1 day'},
        index=index))


def _tick_symbols(history):
    rows = DuckDBConnection.get_instance(history).execute(
        'SELECT DISTINCT symbol FROM tick_data ORDER BY symbol', fetch='all')
    return [row[0] for row in rows]


def test_migrate_symbols_does_not_replace_a_universe_it_could_not_read(databases, monkeypatch, capsys):
    import trader.sdk
    monkeypatch.setattr(trader.sdk, 'MMR', _Service)
    before = _store_unreadable_downloads(databases.main)
    _ticker_keyed_bars(databases.history)

    cli._handle_data_migrate_symbols(argparse.Namespace())

    assert _stored_downloads(databases.main) == before, 'the unreadable universe was overwritten'
    # The symbol is reported as not migrated, and its bars are left as they were.
    assert '"skipped": 1' in capsys.readouterr().out
    assert _tick_symbols(databases.history) == ['AAPL']


def test_migrate_symbols_still_registers_into_a_readable_universe(databases, monkeypatch, capsys):
    import trader.sdk
    monkeypatch.setattr(trader.sdk, 'MMR', _Service)
    UniverseAccessor(databases.main, 'Universes').update(Universe('downloads', [_definition(272093, 'MSFT')]))
    _ticker_keyed_bars(databases.history)

    cli._handle_data_migrate_symbols(argparse.Namespace())

    assert '"migrated": 1' in capsys.readouterr().out
    assert _tick_symbols(databases.history) == ['265598']
    members = UniverseAccessor(databases.main, 'Universes').get('downloads').security_definitions
    assert [d.conId for d in members] == [272093, 265598]


# --------------------------------------------------------------------------
# `strategies deploy --conids N` (auto-registration of an unknown conId)
# --------------------------------------------------------------------------

STRATEGY = '''
from trader.trading.strategy import Strategy


class Quiet(Strategy):
    def on_prices(self, prices):
        return None
'''


def _deploy(tmp_path, monkeypatch):
    import trader.sdk
    monkeypatch.setattr(trader.sdk, 'MMR', _Service)
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    monkeypatch.setattr(cli, '_gauntlet_db_path', lambda: str(tmp_path / 'gauntlet.duckdb'))
    module = tmp_path / 'mods' / 'quiet.py'
    module.parent.mkdir()
    module.write_text(STRATEGY)
    cli._handle_strategy_deploy(argparse.Namespace(
        name='quiet', conids=[265598], universe=None, bar_size='1 min', days=5, paper_only=False,
        auto_execute=False, module=str(module), class_name='Quiet', params=None))


def test_deploy_does_not_replace_a_universe_it_could_not_read(databases, tmp_path, monkeypatch):
    before = _store_unreadable_downloads(databases.main)
    _deploy(tmp_path, monkeypatch)
    assert _stored_downloads(databases.main) == before, 'the unreadable universe was overwritten'


def test_deploy_still_registers_into_a_readable_universe(databases, tmp_path, monkeypatch):
    UniverseAccessor(databases.main, 'Universes').update(Universe('downloads', [_definition(272093, 'MSFT')]))
    _deploy(tmp_path, monkeypatch)
    members = UniverseAccessor(databases.main, 'Universes').get('downloads').security_definitions
    assert [d.conId for d in members] == [272093, 265598]
