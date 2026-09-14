"""The resolver cache must not pin an EMPTY result for the process lifetime (2026-09-14).

``UniverseAccessor.resolve_universe`` cached complete query results per key
so a first-only lookup could not hide a conflicting definition. It also cached
MISSES, and an accessor only invalidates on its OWN mutations — so a symbol
inserted through another accessor (the CLI's ``universe add``, the trader's
portfolio update) stayed unresolvable in every other accessor until restart.
"""
from ib_async import Contract, ContractDetails

from trader.data.data_access import SecurityDefinition
from trader.data.universe import UniverseAccessor


def _definition(conid, symbol):
    return SecurityDefinition.from_contract_details(ContractDetails(contract=Contract(
        conId=conid, exchange='SMART', primaryExchange='NASDAQ', secType='STK',
        symbol=symbol, currency='USD')))


def test_miss_is_not_cached_so_another_accessors_insert_becomes_visible(tmp_path):
    db = str(tmp_path / 'catalogue.duckdb')
    reader = UniverseAccessor(db, 'Universes')
    writer = UniverseAccessor(db, 'Universes')

    assert reader.resolve_universe(265598) == []
    assert reader.resolve_symbol('AAPL') == []
    writer.insert('portfolio', _definition(265598, 'AAPL'))

    assert [d.conId for d in reader.resolve_symbol(265598)] == [265598]
    assert [d.symbol for d in reader.resolve_symbol('AAPL')] == ['AAPL']


def test_hits_are_still_cached(tmp_path):
    db = str(tmp_path / 'catalogue.duckdb')
    accessor = UniverseAccessor(db, 'Universes')
    accessor.insert('portfolio', _definition(1, 'ONE'))
    first = accessor.resolve_universe(1)
    assert len(first) == 1
    calls = []
    accessor.get_all = lambda: calls.append(True) or []  # would return nothing if consulted
    assert accessor.resolve_universe(1) == first
    assert calls == [], 'a non-empty result is served from the cache'
    assert accessor.resolve_universe(2) == [] and calls == [True], 'a miss re-queries'
    assert accessor.resolve_universe(2) == [] and calls == [True, True], 'and is not remembered'
