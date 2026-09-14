"""Recovery callbacks can arrive before Trader attaches its durable journal."""
import asyncio
import datetime as dt
import json
from types import SimpleNamespace

import pytest

from trader.data.event_store import EventStore, EventType
from trader.strategy.auto_executor import AutoExecutor
from trader.trading.order_lifecycle import OrderLifecycleTracker


def _trade(filled=0, status='Submitted', *, average=0, perm_id=1717,
           intent_id='startup-intent'):
    return SimpleNamespace(
        order=SimpleNamespace(orderId=17, clientId=7, permId=perm_id,
                              account='paper', orderRef='owner|mmr:' + intent_id,
                              orderType='MKT', action='BUY', totalQuantity=100),
        orderStatus=SimpleNamespace(status=status, filled=filled, avgFillPrice=average),
        contract=SimpleNamespace(conId=123, symbol='OWN'))


def _receipt(cumulative, *, shares=20, price=110, average=None):
    return SimpleNamespace(execution=SimpleNamespace(
        execId='startup-fill-' + str(cumulative), orderId=17, clientId=7,
        permId=1717, acctNumber='paper', shares=shares, cumQty=cumulative,
        price=price, avgPrice=average or price,
        time=dt.datetime.now(dt.timezone.utc)))


def _send(tracker, trade, fill=None, *, completed=False, flush=True):
    async def callback():
        if fill is None:
            tracker.on_trade(trade, completed=completed)
        else:
            tracker.on_execution(trade, fill, completed=completed)
    asyncio.run(callback())
    if flush:
        assert tracker.flush(timeout=1), tracker.health


def _events(store, kind=EventType.ORDER_FILLED):
    return sorted(store.query_since(dt.datetime(2000, 1, 1), kind),
                  key=lambda event: event.timestamp)


def _seed(store, trade):
    tracker = OrderLifecycleTracker(store)
    try:
        _send(tracker, trade)
        return tracker.snapshot()[0]
    finally:
        tracker.close(timeout=.2)


@pytest.mark.parametrize('prior_status,expected_status,known', [
    ('Submitted', 'Unknown', False), ('Filled', 'Filled', True),
])
@pytest.mark.parametrize('completed_status', ['Cancelled', 'Filled'])
@pytest.mark.parametrize('completed_filled', [0, 20])
def test_late_attach_preserves_durable_lower_bound_and_final_quantity_certainty(
        tmp_path, prior_status, expected_status, known, completed_status, completed_filled):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    original = _seed(store, _trade(40, prior_status, average=100))
    tracker = OrderLifecycleTracker()
    try:
        # IB completedOrder supplies status with a default zero when its
        # bounded execution replay no longer contains the actual fills.
        _send(tracker, _trade(completed_filled, status=completed_status), completed=True, flush=False)
        tracker.set_event_store(store)
        assert tracker.flush(timeout=1), tracker.health
        for restart in (False, True):
            if restart:
                tracker.close(timeout=.2)
                tracker = OrderLifecycleTracker(store)
            row, = tracker.snapshot([17])
            assert row['identity'] == original['identity']
            assert row['filled'] == 40
            assert row['avgFillPrice'] == 100
            assert row['brokerStatus'] == completed_status
            assert row['status'] == expected_status
            assert row['fillQuantityKnown'] is known
            assert row['remaining'] == (60 if known else None)
            assert tracker.latest_status(17) == expected_status
        assert [(event.quantity, event.price) for event in _events(store)] == [(40, 100)]
        assert _events(store, EventType.ORDER_CANCELLED) == []
    finally:
        tracker.close(timeout=.2)


def test_late_attach_partial_history_keeps_open_reservation_and_confirmed_ownership(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker()
    sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: {
        'complete': True, 'orders': tracker.snapshot()})
    path = str(tmp_path / 'executor.duckdb')
    executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    intent = executor.intents.create('owner', 123, 'OPEN',
        {'bar_ts': '2026-09-08T10:00:00+00:00', 'quantity': 100}, status='UNKNOWN')
    _seed(store, _trade(40, average=100, intent_id=intent['intent_id']))
    try:
        _send(tracker, _trade(status='Cancelled', intent_id=intent['intent_id']),
              completed=True, flush=False)
        tracker.set_event_store(store)
        assert tracker.flush(timeout=1)
        executor._reconcile_intent(intent)
        restored = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        restored._reconcile_intents()
        row, = restored.intents.all(kind='OPEN', active=True)
        assert row['status'] == 'UNKNOWN'
        assert restored.state.open_position('owner', 123)['quantity'] == 40
        assert restored.intents.create('owner', 123, 'OPEN', {})['intent_id'] == intent['intent_id']
    finally:
        tracker.close(timeout=.2)


def test_buffered_lower_snapshot_cannot_erase_cost_or_price_a_future_delta(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    _seed(store, _trade(40, average=100))
    tracker = OrderLifecycleTracker()
    try:
        _send(tracker, _trade(20, average=50), flush=False)
        tracker.set_event_store(store)
        assert tracker.flush(timeout=1)
        assert tracker.snapshot()[0]['filled'] == 40
        assert tracker.snapshot()[0]['avgFillPrice'] == 100
        _send(tracker, _trade(60, average=6200 / 60))
        fills = _events(store)
        assert [event.quantity for event in fills] == [40, 20]
        assert [event.price for event in fills] == pytest.approx([100, 110])
        tracker.close(timeout=.2)
        tracker = OrderLifecycleTracker(store)
        _send(tracker, _trade(60, average=6200 / 60))
        assert len(_events(store)) == 2
    finally:
        tracker.close(timeout=.2)


@pytest.mark.parametrize('prior_perm,buffered_perm', [(0, 1717), (1717, 0)])
def test_buffered_identity_promotion_replays_fifo_against_durable_checkpoint_across_midnight(
        tmp_path, monkeypatch, prior_perm, buffered_perm):
    from trader.trading import order_lifecycle

    observed_at = [dt.datetime(2026, 9, 1, 23, 59, 59, tzinfo=dt.timezone.utc)]

    class ObservationClock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return observed_at[0].astimezone(tz) if tz else observed_at[0].replace(tzinfo=None)

    monkeypatch.setattr(order_lifecycle, 'dt', SimpleNamespace(
        datetime=ObservationClock, timezone=dt.timezone))
    store = EventStore(str(tmp_path / 'events.duckdb'))
    original = _seed(store, _trade(40, average=100, perm_id=prior_perm))
    observed_at[0] += dt.timedelta(seconds=2)
    tracker = OrderLifecycleTracker()
    try:
        _send(tracker, _trade(status='Cancelled', perm_id=buffered_perm),
              completed=True, flush=False)
        # This receipt proves a new lower bound, not the final cancelled
        # quantity. The status omitted fills; this receipt confirms the delta.
        fill = _receipt(60, average=6200 / 60)
        _send(tracker, _trade(status='Cancelled'), fill, completed=True, flush=False)
        # Later average must not be used when persisting the earlier receipt.
        _send(tracker, _trade(100, 'Filled', average=112, perm_id=0), flush=False)
        tracker.set_event_store(store)
        assert tracker.flush(timeout=1), tracker.health
        row, = tracker.snapshot([17])
        assert row['identity'] == original['identity']
        assert (row['permId'], row['filled'], row['status']) == (1717, 100, 'Filled')
        fills = _events(store)
        assert [event.quantity for event in fills] == [40, 20, 40]
        assert [event.price for event in fills] == pytest.approx([100, 110, 125])
        assert all(event.metadata['price_evaluable'] for event in fills)
        assert len(tracker.execution_receipts([17])) == 1
        assert _events(store, EventType.ORDER_CANCELLED) == []
        tracker.close(timeout=.2)
        observed_at[0] += dt.timedelta(days=1)
        tracker = OrderLifecycleTracker(store)
        _send(tracker, _trade(status='Cancelled', perm_id=0), completed=True)
        _send(tracker, _trade(status='Cancelled'), fill, completed=True)
        row, = tracker.snapshot([17])
        assert row['filled'] == 100
        assert row['status'] == 'Filled'
        assert len(_events(store)) == 3
        assert len(tracker.execution_receipts([17])) == 1
    finally:
        tracker.close(timeout=.2)


@pytest.mark.parametrize('stale_terminal', [None, 'filled'])
def test_legacy_snapshot_below_durable_checkpoint_recovers_only_confirmed_lower_bound(
        tmp_path, stale_terminal):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    _seed(store, _trade(40, average=100))
    tracker = OrderLifecycleTracker(store)
    tracker.close(timeout=.2)
    # Reproduce a saved journal from the old late-attach overwrite: numeric
    # cumulative/cost survive but JSON was replaced by a default-zero row.
    with tracker._journal.transaction() as conn:
        payload = json.loads(conn.execute('SELECT snapshot FROM broker_order_progress').fetchone()[0])
        payload.update(filled=0, avgFillPrice=0, status='Cancelled', brokerStatus='Cancelled',
                       fillQuantityKnown=True, remaining=100)
        conn.execute('UPDATE broker_order_progress SET snapshot=?, terminal=?',
                     [json.dumps(payload), stale_terminal])
    tracker = OrderLifecycleTracker(store)
    try:
        row, = tracker.snapshot([17])
        assert (row['filled'], row['avgFillPrice']) == (40, 100)
        assert row['status'] == 'Unknown'
        assert row['fillQuantityKnown'] is False
        assert row['remaining'] is None
        _send(tracker, _trade(status='Cancelled'), completed=True)
        _send(tracker, _trade(status='Cancelled'), _receipt(60, average=6200 / 60), completed=True)
        row, = tracker.snapshot([17])
        assert row['filled'] == 60
        assert row['status'] == 'Unknown'
        assert row['fillQuantityKnown'] is False
        fills = _events(store)
        assert [event.quantity for event in fills] == [40, 20]
        assert [event.price for event in fills] == pytest.approx([100, 110])
        tracker.close(timeout=.2)
        tracker = OrderLifecycleTracker(store)
        row, = tracker.snapshot([17])
        assert (row['filled'], row['status'], row['fillQuantityKnown']) == (60, 'Unknown', False)
    finally:
        tracker.close(timeout=.2)


@pytest.mark.parametrize('attach_after_replay', [False, True])
def test_status_permanent_id_links_later_completed_order_without_scoped_id(
        tmp_path, attach_after_replay):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    earlier = _trade(40, average=100, perm_id=0)
    # ib_async.orderStatus updates OrderStatus.permId independently of Order.
    earlier.orderStatus.permId = 1717
    _seed(store, earlier)
    tracker = OrderLifecycleTracker(None if attach_after_replay else store)
    try:
        completed = _trade(status='Cancelled')
        completed.order.orderId = 0
        _send(tracker, completed, completed=True, flush=not attach_after_replay)
        if attach_after_replay:
            tracker.set_event_store(store)
            assert tracker.flush(timeout=1)
        row, = tracker.snapshot()
        assert (row['orderId'], row['permId'], row['filled']) == (17, 1717, 40)
        assert (row['status'], row['fillQuantityKnown']) == ('Unknown', False)
        assert [(event.quantity, event.price) for event in _events(store)] == [(40, 100)]
    finally:
        tracker.close(timeout=.2)


@pytest.mark.parametrize('conflict_source', ['status', 'execution'])
def test_contradictory_permanent_ids_require_replay_without_guessing(
        tmp_path, monkeypatch, conflict_source):
    from trader.trading import order_lifecycle

    errors = []
    monkeypatch.setattr(order_lifecycle.logging, 'exception', lambda *args: errors.append(args))
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        _send(tracker, _trade(40, average=100))
        contradictory = _trade(60, average=6200 / 60)
        fill = None
        if conflict_source == 'status':
            contradictory.orderStatus.permId = 2727
        else:
            fill = _receipt(60)
            fill.execution.permId = 2727
        _send(tracker, contradictory, fill)
        assert tracker.health['replay_required'] is True
        assert tracker.health['healthy'] is False
        assert errors
        row, = tracker.snapshot()
        assert (row['permId'], row['filled']) == (1717, 40)
        assert [(event.quantity, event.price) for event in _events(store)] == [(40, 100)]
        assert tracker.execution_receipts() == []
    finally:
        tracker.close(timeout=.2)
