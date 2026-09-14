"""Server-side emergency reductions must place an order-routable contract (review 2026-09-11).

``ib.positions()`` reports stock contracts with ``exchange=''``; IB rejects
``placeOrder`` on that shape with error 321. Every client path normalises via
``sdk._to_contract``; ``emergency_close_position`` placed the raw record and
the executor retried the same failing shape forever while the position sat
unprotected.
"""
from types import SimpleNamespace

import pytest
from ib_async import Contract, Position, Stock

from trader.common.reactivex import SuccessFail
from trader.messaging.trader_service_api import TraderServiceApi
from trader.trading.trading_runtime import Trader


def _position_record():
    return Contract(secType='STK', conId=4391, symbol='AMD', exchange='', primaryExchange='NASDAQ', currency='USD')


def test_stock_position_record_routes_smart_and_keeps_identity():
    routed = Trader.routable_contract(_position_record())
    assert (routed.exchange, routed.conId, routed.primaryExchange, routed.symbol) == ('SMART', 4391, 'NASDAQ', 'AMD')


def test_an_existing_exchange_is_preserved():
    assert Trader.routable_contract(Stock('AMD', 'NASDAQ', 'USD', conId=4391)).exchange == 'NASDAQ'


def test_cash_and_listed_derivatives_route_to_their_venues():
    assert Trader.routable_contract(Contract(secType='CASH', symbol='EUR', currency='USD')).exchange == 'IDEALPRO'
    future = Contract(secType='FUT', symbol='ES', primaryExchange='CME', currency='USD')
    assert Trader.routable_contract(future).exchange == 'CME'
    unknown = Contract(secType='FUT', symbol='ES', currency='USD')
    assert Trader.routable_contract(unknown).exchange == '', 'nothing is invented for an unknown venue'


def test_the_original_record_is_not_mutated():
    record = _position_record()
    Trader.routable_contract(record)
    assert record.exchange == ''


@pytest.mark.asyncio
async def test_emergency_close_places_a_routable_contract():
    record = _position_record()
    placed = []

    async def place_expressive_order(contract, action, quantity, spec, **kwargs):
        placed.append((contract, action, quantity, spec, kwargs))
        return SuccessFail.success(obj=[])

    trader = SimpleNamespace(
        ib_account='DU_FAKE',
        get_positions=lambda: [Position('DU_FAKE', record, 3.0, 100.0)],
        place_expressive_order=place_expressive_order,
        routable_contract=Trader.routable_contract)

    result = await TraderServiceApi(trader).emergency_close_position(4391, 3.0, 'orb_amd', 'emergency-1')

    assert result.is_success(), result.error
    (contract, action, quantity, spec, kwargs), = placed
    assert contract.exchange == 'SMART' and contract.conId == 4391
    assert (action, quantity, spec) == ('SELL', 3.0, {'order_type': 'MARKET'})
    assert kwargs['allow_open'] is False and kwargs['algo_name'] == 'orb_amd'
    assert kwargs['client_intent_id'] == 'emergency-1'
