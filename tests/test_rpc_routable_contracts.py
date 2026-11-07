"""RPC entry points route blank-exchange position contracts (IB 321 otherwise)."""
import asyncio
import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock

from ib_async.contract import Contract

from trader.messaging.trader_service_api import TraderServiceApi
from trader.trading.trading_runtime import Trader


def _api():
    trader = SimpleNamespace(routable_contract=Trader.routable_contract,
                             client=SimpleNamespace(get_snapshot=AsyncMock(return_value='tick')),
                             get_snapshots_batch=AsyncMock(return_value=[]))
    api = object.__new__(TraderServiceApi)
    api.trader = trader
    return api, trader


def _position_contract():
    return Contract(secType='STK', conId=9599491, symbol='F', exchange='', primaryExchange='NYSE', currency='USD')


def test_get_snapshot_routes_blank_exchange():
    api, trader = _api()
    asyncio.run(api.get_snapshot(_position_contract(), False))
    sent = trader.client.get_snapshot.call_args.kwargs['contract']
    assert sent.exchange == 'SMART' and sent.conId == 9599491


def test_batch_routes_each_contract_and_keeps_explicit_venue():
    api, trader = _api()
    asx = Contract(secType='STK', conId=4036812, symbol='BHP', exchange='ASX', currency='AUD')
    asyncio.run(api.get_snapshots_batch([_position_contract(), asx], False))
    sent = trader.get_snapshots_batch.call_args.args[0]
    assert [c.exchange for c in sent] == ['SMART', 'ASX']


def test_already_routable_contract_keeps_identity():
    api, trader = _api()
    c = Contract(secType='STK', conId=265598, symbol='AAPL', exchange='SMART', currency='USD')
    asyncio.run(api.get_snapshot(c, False))
    assert trader.client.get_snapshot.call_args.kwargs['contract'] is c


def test_every_order_and_quote_entry_normalises():
    for name in ('place_order_simple', 'place_expressive_order', 'resize_position',
                 'place_standalone_order', 'check_order_margin', 'get_snapshot',
                 'get_market_depth', 'get_snapshots_batch'):
        src = inspect.getsource(getattr(TraderServiceApi, name))
        assert '_routed(self.trader' in src, name
