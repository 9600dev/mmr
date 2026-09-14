"""A broker permanent ID enriches one order; it does not create another order.

IB's local cancelOrder emits orderStatusEvent before an acknowledgment can
supply permId, so a provisional PendingCancel observation is reachable.
All broker facts here are synthetic; the tracker and persistence are real.
"""
import datetime as dt
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from trader.data.event_store import EventStore, EventType
from trader.data.execution_journal import ExecutionJournal
from trader.trading.order_lifecycle import OrderLifecycleTracker


def _trade(filled, status='Submitted', *, perm_id=0, average=100.0,
           account='paper', client_id=7, reference='identity-owner|mmr:identity-intent',
           conid=123, action='BUY'):
    return SimpleNamespace(
        order=SimpleNamespace(
            orderId=17, permId=perm_id, clientId=client_id, account=account,
            totalQuantity=100, orderRef=reference, action=action, orderType='LMT',
        ),
        orderStatus=SimpleNamespace(status=status, filled=filled, avgFillPrice=average),
        contract=SimpleNamespace(conId=conid, symbol='OWN'),
    )


def _events(store, kind):
    return store.query_since(dt.datetime(2000, 1, 1), kind)


@pytest.mark.parametrize('restart_before_ack', [False, True])
def test_pre_ack_cancel_becomes_one_terminal_order_across_restart(tmp_path, restart_before_ack):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        tracker.on_trade(_trade(0, 'PendingCancel'))
        assert tracker.flush()
        assert tracker.latest_status(17) == 'PendingCancel'
        if restart_before_ack:
            tracker.close()
            tracker = OrderLifecycleTracker(store)

        tracker.on_trade(_trade(0, 'Cancelled', perm_id=1717))
        assert tracker.flush()
        row, = tracker.snapshot([17])
        assert row['permId'] == 1717
        assert row['status'] == 'Cancelled'
        assert row['filled'] == 0
        assert row['fillQuantityKnown'] is True
        assert row.get('identityAmbiguous') is not True
        assert tracker.latest_status(17) == 'Cancelled'

        tracker.close()
        tracker = OrderLifecycleTracker(store)
        row, = tracker.snapshot([17])
        assert row['status'] == 'Cancelled'
        assert tracker.latest_status(17) == 'Cancelled'
        tracker.on_trade(_trade(0, 'Cancelled', perm_id=1717))
        assert tracker.flush()
        assert len(_events(store, EventType.ORDER_CANCELLED)) == 1
        assert _events(store, EventType.ORDER_FILLED) == []
    finally:
        tracker.close()


def test_permanent_id_ack_across_utc_midnight_preserves_provisional_fill(tmp_path, monkeypatch):
    from trader.trading import order_lifecycle

    observed_at = [dt.datetime(2026, 9, 1, 23, 59, 59, tzinfo=dt.timezone.utc)]

    class ObservationClock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return observed_at[0].astimezone(tz) if tz else observed_at[0].replace(tzinfo=None)

    # Isolate the lifecycle module's observation clock, not datetime globally.
    monkeypatch.setattr(order_lifecycle, 'dt', SimpleNamespace(
        datetime=ObservationClock, timezone=dt.timezone))
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        tracker.on_trade(_trade(40))
        assert tracker.flush()
        tracker.close()
        observed_at[0] += dt.timedelta(seconds=2)
        tracker = OrderLifecycleTracker(store)
        tracker.on_trade(_trade(40, 'Cancelled', perm_id=1717))
        assert tracker.flush()

        row, = tracker.snapshot([17])
        assert row['permId'] == 1717
        assert row['status'] == 'Cancelled'
        assert row['filled'] == 40
        assert sum(e.quantity for e in _events(store, EventType.ORDER_FILLED)) == 40
        assert len(_events(store, EventType.ORDER_CANCELLED)) == 1
    finally:
        tracker.close()


@pytest.mark.parametrize('restart_before_omission', [False, True])
def test_missing_perm_after_positive_identity_keeps_same_order(tmp_path, restart_before_omission):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        tracker.on_trade(_trade(40, perm_id=1717))
        assert tracker.flush()
        if restart_before_omission:
            tracker.close()
            tracker = OrderLifecycleTracker(store)
        tracker.on_trade(_trade(40, 'PendingCancel'))
        tracker.on_trade(_trade(40, 'Cancelled'))
        assert tracker.flush()

        row, = tracker.snapshot([17])
        assert row['permId'] == 1717
        assert row['status'] == 'Cancelled'
        assert row['filled'] == 40
        assert tracker.latest_status(17) == 'Cancelled'
        assert sum(e.quantity for e in _events(store, EventType.ORDER_FILLED)) == 40
        assert len(_events(store, EventType.ORDER_CANCELLED)) == 1
    finally:
        tracker.close()


def test_provisional_id_collision_with_another_durable_reference_requires_replay(tmp_path, monkeypatch):
    from trader.trading import order_lifecycle

    errors = []
    monkeypatch.setattr(order_lifecycle.logging, 'exception', lambda *args, **kwargs: errors.append(args))
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        tracker.on_trade(_trade(40, reference='identity-owner|mmr:first-intent'))
        assert tracker.flush()
        tracker.on_trade(_trade(20, reference='identity-owner|mmr:other-intent'))
        assert tracker.flush()

        assert tracker.health['healthy'] is False
        assert tracker.health['replay_required'] is True
        assert errors
        row, = tracker.snapshot([17])
        assert row['clientIntentId'] == 'first-intent'
        assert row['filled'] == 40
        assert sum(e.quantity for e in _events(store, EventType.ORDER_FILLED)) == 40
    finally:
        tracker.close()


def test_two_proven_perm_ids_do_not_guess_which_omitted_identity_changed(tmp_path, monkeypatch):
    from trader.trading import order_lifecycle

    errors = []
    monkeypatch.setattr(order_lifecycle.logging, 'exception', lambda *args, **kwargs: errors.append(args))
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        tracker.on_trade(_trade(40, perm_id=1717))
        tracker.on_trade(_trade(20, perm_id=2727))
        assert tracker.flush()
        tracker.close()
        tracker = OrderLifecycleTracker(store)
        tracker.on_trade(_trade(60))
        assert tracker.flush()

        assert tracker.health['healthy'] is False
        assert tracker.health['replay_required'] is True
        assert errors
        assert {(r['permId'], r['filled']) for r in tracker.snapshot([17])} == {
            (1717, 40), (2727, 20),
        }
        assert sum(e.quantity for e in _events(store, EventType.ORDER_FILLED)) == 60
    finally:
        tracker.close()


@pytest.mark.parametrize('restart_before_ack', [False, True])
def test_identity_promotion_keeps_one_cumulative_fill_checkpoint(tmp_path, restart_before_ack):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        tracker.on_trade(_trade(40))
        assert tracker.flush()
        if restart_before_ack:
            tracker.close()
            tracker = OrderLifecycleTracker(store)

        # The first permanent-ID observation repeats the SAME 40-share fill.
        tracker.on_trade(_trade(40, perm_id=1717))
        assert tracker.flush()
        assert sum(e.quantity for e in _events(store, EventType.ORDER_FILLED)) == 40
        row, = tracker.snapshot([17])
        assert row['permId'] == 1717
        assert row['filled'] == 40

        # Only the subsequent 60 shares are new. The broker cumulative price
        # implies total cost 10,600, of which the earlier fill cost 4,000.
        tracker.on_trade(_trade(100, 'Filled', perm_id=1717, average=106))
        assert tracker.flush()
        fills = _events(store, EventType.ORDER_FILLED)
        assert sorted(e.quantity for e in fills) == [40, 60]
        assert sum(e.quantity * e.price for e in fills) == pytest.approx(10600)

        tracker.close()
        tracker = OrderLifecycleTracker(store)
        tracker.on_trade(_trade(100, 'Filled', perm_id=1717, average=106))
        assert tracker.flush()
        row, = tracker.snapshot([17])
        assert row['status'] == 'Filled'
        assert row['filled'] == 100
        assert tracker.latest_status(17) == 'Filled'
        assert sum(e.quantity for e in _events(store, EventType.ORDER_FILLED)) == 100
    finally:
        tracker.close()


def test_execution_receipt_does_not_recount_provisional_status_fill(tmp_path):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    receipt = SimpleNamespace(execution=SimpleNamespace(
        execId='identity-promotion-fill', orderId=17, clientId=7,
        acctNumber='paper', permId=1717, shares=40, cumQty=40,
        price=100, avgPrice=100, time=dt.datetime.now(dt.timezone.utc),
    ))
    tracker = OrderLifecycleTracker(store)
    try:
        tracker.on_trade(_trade(40))
        assert tracker.flush()
        tracker.on_execution(_trade(40, perm_id=1717), receipt)
        assert tracker.flush()
        assert sum(e.quantity for e in _events(store, EventType.ORDER_FILLED)) == 40

        tracker.close()
        tracker = OrderLifecycleTracker(store)
        tracker.on_execution(_trade(40, perm_id=1717), receipt)
        assert tracker.flush()
        assert len(tracker.execution_receipts([17])) == 1
        row, = tracker.snapshot([17])
        assert row['filled'] == 40
        assert row['permId'] == 1717
        assert sum(e.quantity for e in _events(store, EventType.ORDER_FILLED)) == 40
    finally:
        tracker.close()


def test_saved_legacy_duplicate_stays_unhealthy_without_rewriting_audit(tmp_path):
    """Replay cannot decide which old audit fill was already double-counted."""
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        tracker.on_trade(_trade(40))
        assert tracker.flush()
        provisional, = tracker.snapshot([17])
        original_fill, = _events(store, EventType.ORDER_FILLED)
    finally:
        tracker.close()

    # Reproduce the exact persisted shape written by an older tracker. New
    # observations should never create this second logical order again.
    permanent = dict(provisional, identity='paper:perm:1717', permId=1717)
    journal = ExecutionJournal(store.duckdb_path)
    try:
        with journal.transaction() as conn:
            conn.execute('INSERT INTO broker_order_progress VALUES (?, ?, ?, ?, ?)',
                         [permanent['identity'], 40, 4000, None, json.dumps(permanent)])
    finally:
        journal.close()
    store.append(replace(original_fill, id=None,
                         metadata={**original_fill.metadata, 'broker_identity': permanent['identity']}))
    old_fills = _events(store, EventType.ORDER_FILLED)
    assert len(old_fills) == 2
    assert sum(e.quantity for e in old_fills) == 80

    tracker = OrderLifecycleTracker(store)
    try:
        assert tracker.health['healthy'] is False
        assert tracker.health['replay_required'] is True
        assert {(r['permId'], r['filled']) for r in tracker.snapshot([17])} == {(0, 40), (1717, 40)}
        assert all(r.get('identityAmbiguous') is True for r in tracker.snapshot([17]))
        tracker.begin_replay()
        tracker.on_trade(_trade(40, perm_id=1717))
        assert tracker.flush()
        assert tracker.mark_replay_complete() is False
        assert tracker.health['healthy'] is False
        assert tracker.health['replay_required'] is True
        assert all(r.get('identityAmbiguous') is True for r in tracker.snapshot([17]))
        assert _events(store, EventType.ORDER_FILLED) == old_fills

        tracker.close()
        tracker = OrderLifecycleTracker(store)
        assert tracker.mark_replay_complete() is False
        assert tracker.health['healthy'] is False
        assert tracker.health['replay_required'] is True
        assert {(r['permId'], r['filled']) for r in tracker.snapshot([17])} == {(0, 40), (1717, 40)}
        assert all(r.get('identityAmbiguous') is True for r in tracker.snapshot([17]))
        assert _events(store, EventType.ORDER_FILLED) == old_fills
    finally:
        tracker.close()


@pytest.mark.parametrize('other_account,other_client,other_conid,other_action,other_perm', [
    ('other-paper', 7, 123, 'BUY', 0),
    ('paper', 8, 123, 'BUY', 0),
    ('paper', 7, 456, 'BUY', 2727),
    ('paper', 7, 123, 'SELL', 2727),
])
def test_promotion_preserves_other_scope_with_same_order_id(
        tmp_path, other_account, other_client, other_conid, other_action, other_perm):
    store = EventStore(str(tmp_path / 'events.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        tracker.on_trade(_trade(40))
        # A different proven permanent ID makes numeric order-ID reuse
        # explicit even where the provisional account/client/day key collides.
        tracker.on_trade(_trade(20, account=other_account, client_id=other_client,
                                conid=other_conid, action=other_action, perm_id=other_perm))
        tracker.on_trade(_trade(40, perm_id=1717))
        assert tracker.flush()

        rows = tracker.snapshot([17])
        assert len(rows) == 2
        assert {(r['account'], r['clientId'], r['conId'], r['action'], r['permId'], r['filled'])
                for r in rows} == {
            ('paper', 7, 123, 'BUY', 1717, 40),
            (other_account, other_client, other_conid, other_action, other_perm, 20),
        }
        assert sum(e.quantity for e in _events(store, EventType.ORDER_FILLED)) == 60
    finally:
        tracker.close()
