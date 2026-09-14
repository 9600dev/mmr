"""Working orders are an input to two safety decisions; they must be READ.

1. ``aggregate_position_value`` (concentration) summed ``orderStatus.remaining``,
   which a native ``PendingSubmit`` reports as 0 until IB acknowledges it, so a
   just-submitted same-direction open was invisible to the very check that
   bounds it. It now uses the same ``max(total - filled, remaining)`` reading
   the reduction coordinator already used.
2. ``working_trades`` swallowed ``(AttributeError, TypeError)`` from
   ``ib.openTrades()`` and continued with an EMPTY set — the one answer that
   lets a second executable close be sent against the same shares (the O04
   hazard) and lets concentration under-count. It now fails loud, and callers
   treat that as unevaluable: an open is refused, a reduction is deferred.
"""
import datetime as dt
from types import SimpleNamespace

import pytest
from ib_async import Contract, LimitOrder, MarketOrder, OrderStatus, Position, Trade

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.objects import Action
from trader.trading.executioner import WorkingOrdersUnreadableError
from trader.trading.risk_gate import RiskGate, RiskLimits
from trader.trading.trading_runtime import Trader


def _stock_trader(held, submitted):
    t = object.__new__(Trader)
    t.ib_account = 'DU1'
    t.get_positions = lambda: [Position('DU1', _stock(), held, 10.0)]
    t._submitted_trades = list(submitted)
    t.client = SimpleNamespace(ib=SimpleNamespace(openTrades=lambda: []))
    return t


def _pending_submit_buy(quantity):
    order = MarketOrder('BUY', quantity, account='DU1', orderId=41)
    return Trade(_stock(), order, OrderStatus(orderId=41, status='PendingSubmit', filled=0, remaining=0))


def test_pending_submit_open_counts_toward_aggregate_exposure():
    """Held 100 @ $10, a working PendingSubmit BUY 50 (remaining reads 0),
    adding BUY 1 valued at $10 -> $1,510 of exposure, not $1,010."""
    trader = _stock_trader(100, [_pending_submit_buy(50)])

    assert trader.aggregate_position_value(_stock(), 'BUY', 1, 10.0) == pytest.approx(1510.0)


def test_partially_filled_working_open_counts_its_unfilled_remainder():
    trade = _pending_submit_buy(50)
    trade.orderStatus.status = 'Submitted'
    trade.orderStatus.filled = 20
    trade.orderStatus.remaining = 30
    trader = _stock_trader(100, [trade])
    assert trader.aggregate_position_value(_stock(), 'BUY', 1, 10.0) == pytest.approx(1310.0)


def test_working_trades_fail_loud_when_the_broker_set_is_unreadable():
    trader = _stock_trader(100, [])
    trader.client.ib = SimpleNamespace()  # no openTrades at all
    with pytest.raises(WorkingOrdersUnreadableError, match='UNKNOWN: working orders unreadable'):
        trader.working_trades(_stock())
    trader.client.ib = SimpleNamespace(openTrades=lambda: object())  # not iterable
    with pytest.raises(WorkingOrdersUnreadableError):
        trader.working_trades(_stock())
    with pytest.raises(WorkingOrdersUnreadableError):
        trader.aggregate_position_value(_stock(), 'BUY', 1, 10.0)


def _unreadable_broker(trader):
    def boom():
        raise ConnectionError('socket closed')
    trader.client.ib.openTrades = boom


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['expressive', 'simple'])
async def test_open_is_refused_when_working_orders_are_unreadable(tmp_path, path):
    trader = _coordinated_trader(tmp_path, held=0)
    trader.risk_gate = RiskGate(RiskLimits(), trader.event_store)
    trader.startup_time = dt.datetime.now()  # trader_exception() reads it for context
    _unreadable_broker(trader)
    try:
        if path == 'expressive':
            result = await trader.place_expressive_order(_stock(), 'BUY', 1, {'order_type': 'LIMIT', 'limit_price': 10})
            error = result.error
            assert not result.is_success()
        else:
            observable = await trader.place_order_simple(_stock(), Action.BUY, None, 1, None, True, 0.0)
            with pytest.raises(Exception) as raised:
                await observable
            error = str(raised.value)
        assert 'working orders unreadable' in error, error
        assert trader.placed == []
    finally:
        trader.order_tracker.close(timeout=1)


@pytest.mark.asyncio
async def test_reduction_is_deferred_not_refused_when_working_orders_are_unreadable(tmp_path):
    trader = _coordinated_trader(tmp_path, held=100)
    _unreadable_broker(trader)
    try:
        result = await trader.place_expressive_order(_stock(), 'SELL', 40, {'order_type': 'MARKET'},
                                                     client_intent_id='deferred-close')
        assert not result.is_success()
        assert result.error.startswith('DEFERRED:') or 'DEFERRED: SELL 40 not placed' in result.error, result.error
        assert 'working orders unreadable' in result.error
        assert 'retry once the broker order book is readable' in result.error
        assert trader.placed == [], 'nothing was sent'
        claim = trader.server_order_journal().get('deferred-close')
        assert claim['status'] in {'RETRYABLE', 'REJECTED'} and claim['orders'] == []

        # Once the broker book is readable again the same close proceeds.
        trader.client.ib.openTrades = lambda: []
        resumed = await trader.place_expressive_order(_stock(), 'SELL', 40, {'order_type': 'MARKET'},
                                                      client_intent_id='deferred-close-2')
        assert resumed.is_success(), resumed.error
    finally:
        trader.order_tracker.close(timeout=1)


@pytest.mark.asyncio
async def test_protective_stop_is_deferred_when_working_orders_are_unreadable(tmp_path):
    trader = _coordinated_trader(tmp_path, held=100)
    _unreadable_broker(trader)
    try:
        result = await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8)
        assert not result.is_success()
        assert 'DEFERRED' in str(result.error or result.exception)
        assert trader.placed == []
    finally:
        trader.order_tracker.close(timeout=1)


def test_trader_place_order_bypass_is_gone():
    """Trader.place_order called executioner.place_order outside the order
    lock and outside any durable intent, with no callers."""
    assert not hasattr(Trader, 'place_order')
