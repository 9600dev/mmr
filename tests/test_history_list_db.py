"""`history list` reads bars from the HISTORY database.

MMR.history_list() queried tick_data in ``duckdb_path`` (the main DB, which
holds the universe catalogue) instead of ``history_duckdb_path``, where every
downloader writes bars. With the two split — the shipped default — it listed
whatever stray rows the main DB held, usually nothing.
"""
from unittest.mock import MagicMock

import pandas as pd
from ib_async import Contract, ContractDetails

from trader.data.data_access import SecurityDefinition
from trader.data.duckdb_store import DuckDBDataStore
from trader.data.universe import Universe, UniverseAccessor
from trader.sdk import MMR


def _bars(n=3):
    idx = pd.date_range('2026-10-01', periods=n, freq='D', tz='America/New_York')
    return pd.DataFrame({'open': 1.0, 'high': 1.0, 'low': 1.0, 'close': 1.0,
                         'volume': 1.0, 'bar_size': '1 day'}, index=idx)


def test_history_list_reads_the_history_db_and_names_from_main(tmp_path):
    main = str(tmp_path / 'main.duckdb')
    history = str(tmp_path / 'history.duckdb')
    DuckDBDataStore(history).write('265598', _bars())
    DuckDBDataStore(main).write('111', _bars(1))        # stray row in the main DB
    UniverseAccessor(main, 'Universes').update(Universe('us', [
        SecurityDefinition.from_contract_details(ContractDetails(contract=Contract(
            conId=265598, symbol='AAPL', secType='STK', exchange='SMART',
            primaryExchange='NASDAQ', currency='USD')))]))

    mmr = MMR.__new__(MMR)
    container = MagicMock()
    container.config.return_value = {'duckdb_path': main, 'history_duckdb_path': history,
                                     'universe_library': 'Universes'}
    mmr._container = container
    df = mmr.history_list()
    assert df[['symbol', 'name', 'rows']].to_dict('records') == [
        {'symbol': '265598', 'name': 'AAPL', 'rows': 3}]
