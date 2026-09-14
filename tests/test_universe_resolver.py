"""Resolver-cache must honour exchange/sec_type/universe filters on a HIT.

Regression for the precedence bug where a cache hit skipped the exchange check
when a universe was given and skipped sec_type entirely.
"""
from trader.data.universe import UniverseAccessor


def _accessor_with_cached_asx_stk():
    # Prime through the public resolver; the cache's private representation is
    # allowed to change while all existing request/filter assertions remain.
    from pathlib import Path
    from tempfile import TemporaryDirectory
    from ib_async import Contract, ContractDetails
    from trader.data.data_access import SecurityDefinition
    from trader.data.universe import Universe

    temporary = TemporaryDirectory(prefix='mmr-resolver-test-')
    acc = UniverseAccessor(str(Path(temporary.name)/'catalogue.duckdb'), 'mine')
    acc._test_temporary = temporary
    sd = SecurityDefinition.from_contract_details(ContractDetails(contract=Contract(
        conId=1, exchange='ASX', primaryExchange='ASX', secType='STK',
        symbol='BHP', currency='AUD')))
    u = Universe('mine', [sd])
    # Stable object identity preserves these original unit assertions; the
    # integration cases in test_sdk exercise actual persisted catalogue reads.
    acc.get_all = lambda: [u]
    acc.get = lambda name: u if name == 'mine' else Universe(name, [])
    acc.resolve_universe(1)
    return acc, u, sd


class TestResolverCacheHit:
    def test_matching_exchange_hits(self):
        acc, u, sd = _accessor_with_cached_asx_stk()
        assert acc.resolve_universe(1, exchange='ASX') == [(u, sd)]

    def test_wrong_exchange_does_not_hit(self):
        acc, u, sd = _accessor_with_cached_asx_stk()
        assert acc.resolve_universe(1, exchange='NYSE') == []

    def test_wrong_sectype_does_not_hit(self):
        acc, u, sd = _accessor_with_cached_asx_stk()
        assert acc.resolve_universe(1, sec_type='OPT') == []

    def test_matching_sectype_hits(self):
        acc, u, sd = _accessor_with_cached_asx_stk()
        assert acc.resolve_universe(1, sec_type='STK') == [(u, sd)]

    def test_universe_filter_respected(self):
        acc, u, sd = _accessor_with_cached_asx_stk()
        assert acc.resolve_universe(1, universe='mine') == [(u, sd)]
        assert acc.resolve_universe(1, universe='other') == []


def test_invalidate_clears_cache():
    acc, u, sd = _accessor_with_cached_asx_stk()
    acc.invalidate_resolver_cache()
    assert acc._resolver_cache == {}
