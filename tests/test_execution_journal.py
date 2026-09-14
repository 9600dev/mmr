"""Execution audit journal replay, partial fills, and callback isolation."""
import asyncio
import datetime as dt
import json
import threading
from types import SimpleNamespace

from trader.data.event_store import EventStore, EventType
from trader.trading.order_lifecycle import OrderLifecycleTracker


def trade(filled, status='Submitted', *, perm_id=4001, average=100):
    return SimpleNamespace(
        order=SimpleNamespace(orderId=1, permId=perm_id, clientId=7, account='paper',
                              totalQuantity=100, orderRef='owned', action='BUY'),
        orderStatus=SimpleNamespace(status=status, filled=filled, avgFillPrice=average),
        contract=SimpleNamespace(conId=123, symbol='OWN'),
    )


def fills(store):
    return store.query_since(dt.datetime(2000, 1, 1), EventType.ORDER_FILLED)


def test_partial_fill_checkpoint_replays_across_tracker_restart(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    first = OrderLifecycleTracker(store)
    first.on_trade(trade(40))
    first.close()
    second = OrderLifecycleTracker(store)
    try:
        second.on_trade(trade(40))
        second.on_trade(trade(100, 'Filled', average=106))
        assert sum(event.quantity for event in fills(store)) == 100
        assert sum(event.quantity * event.price for event in fills(store)) == 10600
    finally:
        second.close()


def test_event_store_ack_crash_cannot_duplicate_durable_fill(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    first = OrderLifecycleTracker(store)
    first.on_trade(trade(100, 'Filled'))
    first.close()
    # Crash window: DuckDB committed, SQLite did not acknowledge delivery.
    with first._journal.transaction() as conn:
        conn.execute('UPDATE broker_event_outbox SET delivered=0')
    second = OrderLifecycleTracker(store)
    try:
        assert second.flush()
        assert len(fills(store)) == 1
        assert fills(store)[0].quantity == 100
    finally:
        second.close()


def test_execution_id_replay_is_persistent_and_does_not_duplicate_status_fill(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    execution = SimpleNamespace(execId='execution-1', acctNumber='paper', permId=4001,
                                shares=40, cumQty=40, price=100, avgPrice=100,
                                time=dt.datetime.now(dt.timezone.utc))
    fill = SimpleNamespace(execution=execution)
    first = OrderLifecycleTracker(store)
    first.on_trade(trade(40))
    first.on_execution(trade(40), fill)
    first.close()
    second = OrderLifecycleTracker(store)
    try:
        second.on_execution(trade(40), fill)
        assert len(second.execution_receipts()) == 1
        assert sum(event.quantity for event in fills(store)) == 40
    finally:
        second.close()


def test_reused_order_id_with_new_broker_identity_records_both_orders(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        tracker.on_trade(trade(100, 'Filled', perm_id=4001))
        tracker.on_trade(trade(100, 'Filled', perm_id=4002))
        assert sum(event.quantity for event in fills(store)) == 200
        assert len(tracker.snapshot([1])) == 2
    finally:
        tracker.close()


def test_audit_store_block_does_not_block_callback_or_durable_ingestion(tmp_path):
    entered, release = threading.Event(), threading.Event()

    class BlockedAudit:
        duckdb_path = str(tmp_path / 'events.duckdb')

        def __init__(self):
            self.events = []

        def append(self, event):
            entered.set()
            assert release.wait(5), 'test failed to release simulated bulk database writer'
            self.events.append(event)

    store = BlockedAudit()
    tracker = OrderLifecycleTracker(store)

    async def observe_while_audit_blocked():
        tracker.on_trade(trade(40))
        assert await asyncio.to_thread(entered.wait, 2)
        # This callback must return while the first audit append is blocked.
        tracker.on_trade(trade(100, 'Filled'))
        for _ in range(200):
            with tracker._journal.transaction() as conn:
                pending = conn.execute('SELECT count(*) FROM broker_event_outbox WHERE delivered=0').fetchone()[0]
            if pending == 2:
                break
            await asyncio.sleep(.005)
        assert pending == 2, 'second execution was not journalled independently of blocked DuckDB'
        release.set()
        assert await asyncio.to_thread(tracker.flush)

    try:
        asyncio.run(observe_while_audit_blocked())
        assert sum(event.quantity for event in store.events) == 100
    finally:
        release.set()
        tracker.close()
