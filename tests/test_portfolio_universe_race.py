"""Concurrent portfolio-universe updates must not lose (and re-add) entries."""
import asyncio
import copy
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import trader.trading.trading_runtime as tr
from trader.trading.trading_runtime import Trader


class _FakeAccessor:
    """Round-trips like the dill-backed store: get() returns a COPY, slowly."""

    def __init__(self):
        self.stored = SimpleNamespace(name='portfolio', security_definitions=[])
        self.writes = 0

    def get(self, name):
        time.sleep(0.02)                       # widen the read->write window
        return copy.deepcopy(self.stored)

    def update(self, universe):
        time.sleep(0.01)
        self.writes += 1
        self.stored = copy.deepcopy(universe)


def _trader(monkeypatch):
    t = object.__new__(Trader)
    t._known_portfolio_conids = set()
    t.universe_accessor = _FakeAccessor()

    async def details(contract):
        await asyncio.sleep(0.01)
        return [SimpleNamespace(conId=contract.conId)]

    t.client = SimpleNamespace(get_contract_details_async=AsyncMock(side_effect=details))
    monkeypatch.setattr(tr.SecurityDefinition, 'from_contract_details',
                        staticmethod(lambda d: SimpleNamespace(conId=d.conId)))
    return t


def _item(conid):
    return SimpleNamespace(contract=SimpleNamespace(conId=conid))


def test_concurrent_new_conids_all_persist_once(monkeypatch):
    t = _trader(monkeypatch)

    async def burst():
        ids = [101, 102, 103, 104, 105]
        await asyncio.gather(*(t.update_portfolio_universe(_item(c)) for c in ids))
        # a second burst (price-mark refresh) must be a no-op
        await asyncio.gather(*(t.update_portfolio_universe(_item(c)) for c in ids))
        return ids

    ids = asyncio.run(burst())
    persisted = [d.conId for d in t.universe_accessor.stored.security_definitions]
    assert sorted(persisted) == ids, f'lost or duplicated entries: {persisted}'
    assert t.universe_accessor.writes == len(ids)
    assert t._known_portfolio_conids == set(ids)


def test_duplicate_events_for_same_new_conid_write_once(monkeypatch):
    t = _trader(monkeypatch)

    async def burst():
        await asyncio.gather(*(t.update_portfolio_universe(_item(7)) for _ in range(6)))

    asyncio.run(burst())
    assert [d.conId for d in t.universe_accessor.stored.security_definitions] == [7]
    assert t.client.get_contract_details_async.await_count == 1
