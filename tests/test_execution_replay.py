"""Completed-order status is not a substitute for missing execution quantity."""
import asyncio
import datetime as dt
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from ib_async import Contract
from ib_async.objects import CommissionReport, Execution, Fill
from ib_async.order import Order, OrderStatus, Trade

from trader.data.event_store import EventStore, EventType
from trader.strategy.auto_executor import AutoExecutor
from trader.trading.execution_replay import replay_execution_history
from trader.trading.order_lifecycle import OrderLifecycleTracker


def completed(*, status='Filled', order_id=17, intent_id='replay-intent'):
    return Trade(
        Contract(conId=1111, symbol='OWN', secType='STK', currency='USD'),
        Order(orderId=order_id, permId=1717, clientId=7, account='paper',
              totalQuantity=100, action='BUY', orderRef='owner|mmr:' + intent_id),
        OrderStatus(status=status))


def replay(tracker, trade, fills=()):
    ib = SimpleNamespace(reqCompletedOrdersAsync=AsyncMock(return_value=[trade]),
                         reqExecutionsAsync=AsyncMock(return_value=list(fills)),
                         trades=lambda: [])
    asyncio.run(replay_execution_history(ib, tracker))
    assert tracker.flush()
    assert tracker.mark_replay_complete()


def fill_events(store):
    return sorted(store.query_since(dt.datetime(2000, 1, 1), EventType.ORDER_FILLED),
                  key=lambda event: event.timestamp)


@pytest.mark.parametrize('status', ['Filled', 'Cancelled'])
@pytest.mark.parametrize('order_id', [0, 17])
def test_status_only_completed_order_never_proves_zero_fill(tmp_path, status, order_id):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    first = OrderLifecycleTracker(store)
    try:
        replay(first, completed(status=status, order_id=order_id))
        row, = first.snapshot()
        assert row['status'] == 'Unknown'
        assert row['brokerStatus'] == status
        assert row['filled'] == 0  # a lower bound, not a final zero-fill result
        assert row['fillQuantityKnown'] is False
        assert row['remaining'] is None
        assert fill_events(store) == []
        assert asyncio.run(first.wait_decisive(order_id, timeout=.01)) == 'timeout'
    finally:
        first.close()
    second = OrderLifecycleTracker(store)
    try:
        row, = second.snapshot()
        assert row['status'] == 'Unknown'
        assert row['fillQuantityKnown'] is False
    finally:
        second.close()


@pytest.mark.parametrize('known_filled', [0, 40, 100])
def test_completed_replay_preserves_reservation_and_only_known_ownership(tmp_path, known_filled):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    path = str(tmp_path / 'executor.duckdb')
    sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: {'complete': True, 'orders': tracker.snapshot()})
    executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    intent = executor.intents.create('owner', 1111, 'OPEN',
        {'bar_ts': '2026-09-08T10:00:00+00:00', 'quantity': 100}, status='UNKNOWN')
    first = OrderLifecycleTracker(store)
    if known_filled:
        earlier = completed(intent_id=intent['intent_id'])
        earlier.orderStatus = OrderStatus(status='Filled' if known_filled == 100 else 'Submitted',
                                          filled=known_filled, avgFillPrice=10)
        first.on_trade(earlier)
    first.close()
    tracker = OrderLifecycleTracker(store)
    try:
        replay(tracker, completed(intent_id=intent['intent_id']))
        executor._reconcile_intent(intent)
        restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        restarted._reconcile_intents()
        row, = restarted.intents.all(kind='OPEN')
        position = restarted.state.open_position('owner', 1111)
        if known_filled == 100:
            assert row['status'] == 'FILLED'
            assert not restarted.intents.all(kind='OPEN', active=True)
        else:
            assert row['status'] == 'UNKNOWN'
            assert restarted.intents.all(kind='OPEN', active=True)[0]['intent_id'] == intent['intent_id']
            # A new bar cannot allocate another OPEN reservation on this instrument.
            assert restarted.intents.create('owner', 1111, 'OPEN', {})['intent_id'] == intent['intent_id']
        assert (position['quantity'] if position else 0) == known_filled
        assert sum(event.quantity for event in fill_events(store)) == known_filled
    finally:
        tracker.close()


def test_partial_receipt_cannot_complete_filled_history_with_missing_remainder(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    trade = completed(order_id=0)
    trade.order.clientId = 0  # IB completedOrder omits both scoped numeric IDs.
    execution = Execution(execId='fill-40', orderId=17, clientId=7, permId=1717,
                          acctNumber='paper', side='BOT', shares=40, cumQty=40,
                          price=10, avgPrice=10, orderRef=trade.order.orderRef)
    fill = Fill(trade.contract, execution, CommissionReport(), dt.datetime.now(dt.timezone.utc))
    try:
        replay(tracker, trade, [fill])
        row, = tracker.snapshot()
        assert row['orderId'] == 17
        assert row['clientId'] == 7
        assert row['filled'] == 40
        assert row['fillQuantityKnown'] is False
        assert row['status'] == 'Unknown'
        assert sum(event.quantity for event in fill_events(store)) == 40
        execution.execId, execution.shares, execution.cumQty = 'fill-100', 60, 100
        replay(tracker, trade, [fill])
        row, = tracker.snapshot()
        assert row['status'] == 'Filled'
        assert row['fillQuantityKnown'] is True
        assert row['filled'] == 100
        assert sum(event.quantity for event in fill_events(store)) == 100
    finally:
        tracker.close()


@pytest.mark.parametrize('status,quantity', [('Cancelled', 0), ('Cancelled', 40), ('Filled', 100)])
def test_completed_order_explicit_filled_quantity_is_execution_evidence(tmp_path, status, quantity):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    trade = completed(status=status)
    trade.order.filledQuantity = quantity
    try:
        replay(tracker, trade)
        row, = tracker.snapshot()
        assert row['status'] == status
        assert row['fillQuantityKnown'] is True
        assert row['filled'] == quantity
        assert sum(event.quantity for event in fill_events(store)) == quantity
        assert all(event.metadata['price_evaluable'] is False for event in fill_events(store))
    finally:
        tracker.close()


@pytest.mark.parametrize('known_filled', [0, 40])
def test_prior_cancel_cannot_prove_quantity_of_later_filled_status(tmp_path, known_filled):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    earlier = completed(status='Cancelled')
    earlier.orderStatus.filled = known_filled
    earlier.orderStatus.avgFillPrice = 10
    try:
        tracker.on_trade(earlier)
        replay(tracker, completed())
        row, = tracker.snapshot()
        assert row['status'] == 'Unknown'
        assert row['filled'] == known_filled
        assert row['fillQuantityKnown'] is False
        assert sum(event.quantity for event in fill_events(store)) == known_filled
    finally:
        tracker.close()


def test_legacy_zero_filled_checkpoint_is_unknown_after_restart(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    first = OrderLifecycleTracker(store)
    replay(first, completed())
    row, = first.snapshot()
    first.close()
    row['status'] = 'Filled'
    row.pop('fillQuantityKnown')
    row.pop('brokerStatus')
    with first._journal.transaction() as conn:
        conn.execute("UPDATE broker_order_progress SET terminal='filled',snapshot=? WHERE identity=?",
                     [json.dumps(row), row['identity']])
    second = OrderLifecycleTracker(store)
    try:
        restored, = second.snapshot()
        assert restored['status'] == 'Unknown'
        assert restored['fillQuantityKnown'] is False
        replay(second, completed())
        assert fill_events(store) == []
        with second._journal.transaction() as conn:
            assert conn.execute('SELECT terminal FROM broker_order_progress').fetchone()[0] is None
    finally:
        second.close()


def test_missing_previous_cost_cannot_fabricate_next_fill_price(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    trade = completed(status='Cancelled')
    trade.order.filledQuantity = 40
    try:
        replay(tracker, trade)
        trade.orderStatus = OrderStatus(status='Submitted', filled=100, avgFillPrice=10)
        tracker.on_trade(trade)
        events = fill_events(store)
        assert [event.quantity for event in events] == [40, 60]
        assert all(event.metadata['price_evaluable'] is False for event in events)
        # The latest cumulative cost is now known, so later incremental
        # quantity can again have an exact price.
        trade.order.totalQuantity = 140
        trade.orderStatus = OrderStatus(status='Filled', filled=140, avgFillPrice=11)
        tracker.on_trade(trade)
        final = fill_events(store)[-1]
        assert final.quantity == 40
        assert final.price == 13.5
        assert final.metadata['price_evaluable'] is True
    finally:
        tracker.close()


def test_late_price_receipt_restores_cost_checkpoint_without_recounting_quantity(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    trade = completed(status='Cancelled')
    trade.order.filledQuantity = 40
    receipt = Execution(execId='late-price', orderId=17, clientId=7, permId=1717,
                        acctNumber='paper', shares=40, cumQty=40, price=10, avgPrice=10)
    fill = Fill(trade.contract, receipt, CommissionReport(), dt.datetime.now(dt.timezone.utc))
    try:
        replay(tracker, trade)
        replay(tracker, trade, [fill])
        assert len(fill_events(store)) == 1
        trade.orderStatus = OrderStatus(status='Filled', filled=100, avgFillPrice=11)
        tracker.on_trade(trade)
        latest = fill_events(store)[-1]
        assert latest.quantity == 60
        assert latest.price == pytest.approx(700 / 60)
        assert latest.metadata['price_evaluable'] is True
        assert sum(event.quantity for event in fill_events(store)) == 100
    finally:
        tracker.close()
