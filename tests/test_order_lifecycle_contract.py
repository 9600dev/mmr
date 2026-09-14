"""Broker snapshot, receipt and recovery contracts beyond order acceptance.

These regressions assert consumer-visible evidence and recovery behavior. They
use temporary databases and synthetic broker observations, never a broker.
"""
import asyncio
import datetime as dt
import json
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.trading.order_lifecycle import OrderLifecycleTracker


STAMP = dt.datetime(2026, 9, 8, 14, 30, tzinfo=dt.timezone.utc)


def _trade(*, status='Submitted', filled=0, quantity=4, average=0,
           order_id=17, client_id=7, perm_id=1717, account='paper-A',
           order_ref='owner|mmr:intent-A', order_type='MKT', action='BUY',
           completed_quantity=None):
    return SimpleNamespace(
        order=SimpleNamespace(orderId=order_id, clientId=client_id, permId=perm_id,
                              account=account, orderRef=order_ref, orderType=order_type,
                              action=action, totalQuantity=quantity,
                              filledQuantity=completed_quantity),
        orderStatus=SimpleNamespace(status=status, filled=filled, avgFillPrice=average),
        contract=SimpleNamespace(conId=123, symbol='OWN'))


def _fill(*, exec_id='receipt-A', order_id=17, client_id=7, perm_id=1717,
          account='paper-A', order_ref='owner|mmr:intent-A', shares=1,
          cumulative=1, price=.5, average=.5):
    return SimpleNamespace(execution=SimpleNamespace(
        execId=exec_id, orderId=order_id, clientId=client_id, permId=perm_id,
        acctNumber=account, orderRef=order_ref, shares=shares, cumQty=cumulative,
        price=price, avgPrice=average, time=STAMP))


def _send(tracker, trade, fill=None, *, completed=None, flush=True):
    # Model the IB callback context: persistence may never block its loop.
    async def observe():
        kwargs = {} if completed is None else {'completed': completed}
        if fill is None:
            tracker.on_trade(trade, **kwargs)
        else:
            tracker.on_execution(trade, fill, **kwargs)
    asyncio.run(observe())
    if flush:
        assert tracker.flush(timeout=1), tracker.health


def _events(store, kind=EventType.ORDER_FILLED):
    return sorted(store.query_since(dt.datetime(2000, 1, 1), kind),
                  key=lambda event: event.timestamp)


@pytest.fixture
def lifecycle(tmp_path):
    store = EventStore(str(tmp_path / 'audit.duckdb'))
    tracker = OrderLifecycleTracker(store)
    try:
        yield tracker, store
    finally:
        tracker.close(timeout=.2)


def test_receipt_and_snapshot_preserve_execution_identity_and_complete_wire_fields(lifecycle):
    tracker, store = lifecycle
    trade = _trade(order_id=0, client_id=99, perm_id=0, account='', order_ref='',
                   order_type='STP', action='SELL')
    fill = _fill(client_id=0)
    _send(tracker, trade, fill)
    row, = tracker.snapshot([17])
    observed = dt.datetime.fromisoformat(row.pop('observed_at'))
    assert observed.utcoffset() == dt.timedelta(0)
    receipt = dict(execId='receipt-A', orderId=17, shares=1, cumQty=1, price=.5,
                   account='paper-A', permId=1717, time=STAMP.isoformat())
    assert row == dict(orderId=17, clientId=0, permId=1717, account='paper-A',
                       identity='paper-A:perm:1717', status='Submitted', brokerStatus='Submitted',
                       filled=1, fillQuantityKnown=True, remaining=3, totalQuantity=4,
                       avgFillPrice=.5, orderRef='owner', brokerOrderRef='owner|mmr:intent-A',
                       clientIntentId='intent-A', action='SELL', orderType='STP',
                       conId=123, symbol='OWN', execution=receipt)
    assert tracker.execution_receipts([17]) == [receipt]
    assert tracker.execution_receipts([]) == []
    assert tracker.execution_receipts([999]) == []
    event, = _events(store)
    assert event.metadata == dict(status='Submitted', cumulative_filled=1, remaining=3,
                                  broker_identity='paper-A:perm:1717', broker_status='Submitted',
                                  fill_quantity_known=True, price_evaluable=True, execId='receipt-A')
    # The same execution ID in another account is distinct broker evidence.
    _send(tracker, _trade(order_id=18, perm_id=1818, account='paper-B'),
          _fill(order_id=18, perm_id=1818, account='paper-B'))
    assert len(tracker.execution_receipts()) == 2
    assert tracker.execution_receipts([17]) == [receipt]
    assert len(tracker.execution_receipts([18])) == 1


@pytest.mark.parametrize('status,filled', [('Filled', 1), ('Submitted', 0), ('Cancelled', 0)])
def test_restart_restores_status_without_misclassifying_small_or_unfilled_orders(tmp_path, status, filled):
    store = EventStore(str(tmp_path / 'restart.duckdb'))
    first = OrderLifecycleTracker(store)
    _send(first, _trade(status=status, filled=filled, quantity=1, average=.5))
    first.close(timeout=.2)
    restored = OrderLifecycleTracker(store)
    try:
        assert restored.latest_status(17) == status
        row, = restored.snapshot([17])
        assert row['status'] == status
        assert row['filled'] == filled
        assert row['remaining'] == 1 - filled
        assert row['fillQuantityKnown'] is True
        expected = {'Filled': 'filled', 'Submitted': 'accepted', 'Cancelled': 'rejected'}[status]
        assert asyncio.run(restored.wait_decisive(17, timeout=.01)) == expected
    finally:
        restored.close(timeout=.2)


def test_status_only_replay_retains_scoped_ids_average_and_known_remaining_quantity(tmp_path):
    store = EventStore(str(tmp_path / 'replay.duckdb'))
    first = OrderLifecycleTracker(store)
    _send(first, _trade(filled=1, quantity=4, average=.5, client_id=23))
    first.close(timeout=.2)
    restored = OrderLifecycleTracker(store)
    try:
        _send(restored, _trade(status='Cancelled', order_id=0, client_id=0,
                               completed_quantity=1), completed=True)
        row, = restored.snapshot([17])
        assert (row['orderId'], row['clientId'], row['permId']) == (17, 23, 1717)
        assert row['status'] == row['brokerStatus'] == 'Cancelled'
        assert (row['filled'], row['avgFillPrice'], row['remaining']) == (1, .5, 3)
        assert restored.latest_status(17) == 'Cancelled'
        event, = _events(store, EventType.ORDER_CANCELLED)
        assert event.price == .5
        assert event.quantity == 4
        assert event.metadata == dict(status='Cancelled', cumulative_filled=1,
                                      remaining=3, broker_identity='paper-A:perm:1717')
    finally:
        restored.close(timeout=.2)


def test_legacy_known_partial_terminal_fill_survives_cancel_noise(tmp_path):
    store = EventStore(str(tmp_path / 'legacy.duckdb'))
    first = OrderLifecycleTracker(store)
    _send(first, _trade(status='Filled', filled=1, quantity=4, average=.5))
    first.close(timeout=.2)
    # Pre-upgrade checkpoints have no explicit fillQuantityKnown field.
    with first._journal.transaction() as conn:
        payload = json.loads(conn.execute('SELECT snapshot FROM broker_order_progress').fetchone()[0])
        payload.pop('fillQuantityKnown')
        conn.execute('UPDATE broker_order_progress SET snapshot=?', [json.dumps(payload)])
    restored = OrderLifecycleTracker(store)
    try:
        _send(restored, _trade(status='Cancelled', filled=1, quantity=4), completed=True)
        row, = restored.snapshot([17])
        assert row['status'] == 'Filled'
        assert row['brokerStatus'] == 'Cancelled'
        assert row['fillQuantityKnown'] is True
        assert row['filled'] == 1
        assert row['avgFillPrice'] == .5
        assert restored.latest_status(17) == 'Filled'
        assert asyncio.run(restored.wait_decisive(17, timeout=.01)) == 'filled'
        assert _events(store, EventType.ORDER_CANCELLED) == []
    finally:
        restored.close(timeout=.2)


def test_legacy_zero_fill_checkpoint_retains_unknown_broker_terminal_metadata(tmp_path):
    store = EventStore(str(tmp_path / 'legacy-zero.duckdb'))
    first = OrderLifecycleTracker(store)
    _send(first, _trade(status='Filled'))
    first.close(timeout=.2)
    with first._journal.transaction() as conn:
        payload = json.loads(conn.execute('SELECT snapshot FROM broker_order_progress').fetchone()[0])
        payload.pop('fillQuantityKnown')
        payload.pop('brokerStatus')
        payload.update(status='Filled', remaining=4)
        conn.execute("UPDATE broker_order_progress SET snapshot=?,terminal='filled'", [json.dumps(payload)])
    restored = OrderLifecycleTracker(store)
    try:
        row, = restored.snapshot([17])
        assert row['status'] == 'Unknown'
        assert row['brokerStatus'] == 'Filled'
        assert row['fillQuantityKnown'] is False
        assert row['remaining'] is None
        assert restored.latest_status(17) == 'Unknown'
        assert _events(store) == []
    finally:
        restored.close(timeout=.2)


def test_completed_label_can_finish_a_prior_full_cumulative_checkpoint(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(filled=1, quantity=1, average=.5))
    _send(tracker, _trade(status='Filled', quantity=1), completed=True)
    row, = tracker.snapshot([17])
    assert (row['status'], row['filled'], row['remaining']) == ('Filled', 1, 0)
    assert row['fillQuantityKnown'] is True
    assert len(_events(store)) == 1


@pytest.mark.parametrize('status,quantity,known', [
    ('Cancelled', 4, False), ('Filled', 4, False),
    ('Cancelled', .5, True), ('Filled', .5, True),
])
def test_completed_receipt_proves_only_its_observed_cumulative_quantity(lifecycle, status, quantity, known):
    tracker, store = lifecycle
    _send(tracker, _trade(status=status, quantity=quantity),
          _fill(shares=.5, cumulative=.5), completed=True)
    row, = tracker.snapshot([17])
    assert row['brokerStatus'] == status
    assert row['status'] == (status if known else 'Unknown')
    assert row['fillQuantityKnown'] is known
    assert row['filled'] == .5
    assert row['remaining'] == (0 if known else None)


@pytest.mark.parametrize('quantity,completed_quantity,status,filled,known', [
    (4, 0, 'Unknown', 0, False),
    (0, None, 'Unknown', 0, False),
    (4, .5, 'Filled', .5, True),
])
def test_explicit_completed_quantity_never_fabricates_requested_or_zero_fills(
        lifecycle, quantity, completed_quantity, status, filled, known):
    tracker, store = lifecycle
    _send(tracker, _trade(status='Filled', quantity=quantity,
                          completed_quantity=completed_quantity), completed=True)
    row, = tracker.snapshot([17])
    assert row['status'] == status
    assert row['filled'] == filled
    assert row['fillQuantityKnown'] is known
    assert sum(event.quantity for event in _events(store)) == filled


def test_live_cancel_receipt_is_not_treated_as_status_only_completed_replay(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(status='Cancelled'), _fill())
    row, = tracker.snapshot([17])
    assert row['status'] == 'Cancelled'
    assert row['fillQuantityKnown'] is True
    assert row['remaining'] == 3


@pytest.mark.parametrize('quantity,price', [(.5, .25), (1, .01), (1, 1)])
def test_small_fill_has_exact_positive_quantity_price_and_evaluability(lifecycle, quantity, price):
    tracker, store = lifecycle
    _send(tracker, _trade(status='Filled', filled=quantity, quantity=quantity, average=price))
    event, = _events(store)
    assert (event.quantity, event.price) == (quantity, price)
    assert event.metadata['price_evaluable'] is True
    assert event.metadata['cumulative_filled'] == quantity
    assert event.metadata['remaining'] == 0


def test_zero_or_regressing_status_price_cannot_erase_known_cumulative_cost(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(filled=1, average=.5))
    _send(tracker, _trade(filled=.5, average=99))
    assert tracker.snapshot([17])[0]['avgFillPrice'] == .5
    _send(tracker, _trade(filled=1, average=0))
    assert tracker.snapshot([17])[0]['avgFillPrice'] == .5
    _send(tracker, _trade(filled=2, average=.75))
    assert [(event.quantity, event.price) for event in _events(store)] == [(1, .5), (1, 1)]


@pytest.mark.parametrize('shares,cumulative,price,expected', [
    (20, 60, 12, 12), (1, 60, 99, 0), (20, 59, 99, 0),
    (20, 60, float('nan'), 0),
])
def test_only_matching_receipt_can_price_delta_after_unknown_prior_cost(
        lifecycle, shares, cumulative, price, expected):
    tracker, store = lifecycle
    _send(tracker, _trade(status='Cancelled', quantity=100, completed_quantity=40), completed=True)
    _send(tracker, _trade(filled=60, quantity=100, average=640 / 60),
          _fill(shares=shares, cumulative=cumulative, price=price, average=640 / 60))
    event = _events(store)[-1]
    assert event.quantity == 20
    assert event.price == expected
    assert event.metadata['price_evaluable'] is (expected > 0)
    assert tracker.snapshot([17])[0]['avgFillPrice'] == pytest.approx(640 / 60)


def test_receipt_price_supplies_missing_cumulative_average(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(), _fill(average=0, price=.5))
    row, = tracker.snapshot([17])
    assert row['avgFillPrice'] == .5
    event, = _events(store)
    assert event.price == .5 and event.metadata['price_evaluable'] is True


def test_replay_completion_requires_durable_queue_drain_and_explicit_acknowledgment(tmp_path):
    tracker = OrderLifecycleTracker(None)
    store = EventStore(str(tmp_path / 'late-store.duckdb'))
    try:
        tracker.begin_replay()
        assert tracker.health['replay_required'] is True
        assert tracker.health['healthy'] is False
        _send(tracker, _trade(status='Filled', filled=1, quantity=1, average=.5), flush=False)
        assert tracker.health['pending'] == 1
        assert tracker.mark_replay_complete() is False
        tracker.set_event_store(store)
        assert tracker.flush(timeout=1)
        assert tracker.health['replay_required'] is True
        assert tracker.mark_replay_complete() is True
        assert tracker.health['replay_required'] is False
        assert tracker.health['healthy'] is True
        assert len(_events(store)) == 1
    finally:
        tracker.close(timeout=.2)


def test_unreadable_broker_observation_requires_replay_even_with_empty_queue():
    tracker = OrderLifecycleTracker(None)
    _send(tracker, _trade(order_id='not-a-broker-id'), flush=False)
    assert tracker.health['pending'] == 0
    assert tracker.health['error']
    assert tracker.health['replay_required'] is True
    assert tracker.health['healthy'] is False
    assert tracker.mark_replay_complete() is False


def test_queue_overflow_retains_replay_requirement_and_reports_loss():
    tracker = OrderLifecycleTracker(None)
    tracker._queue = queue.Queue(maxsize=1)
    _send(tracker, _trade(order_id=17), flush=False)
    _send(tracker, _trade(order_id=18, perm_id=1818), flush=False)
    assert tracker.health['pending'] == 1
    assert tracker.health['error']
    assert tracker.health['replay_required'] is True
    assert tracker.health['healthy'] is False


class _AuditSink:
    """Fast idempotent audit endpoint; the tracker still uses real SQLite."""
    def __init__(self, path):
        self.duckdb_path = str(path)
        self.events = {}
        self.delivered = threading.Event()
        self._lock = threading.Lock()

    def append_once(self, event, identity):
        with self._lock:
            self.events.setdefault(identity, event)
        self.delivered.set()


def _submission():
    return TradingEvent(EventType.ORDER_SUBMITTED, STAMP, strategy_name='owner',
                        order_id=17, quantity=1, price=.5,
                        metadata={'submission_identity': 'submission:paper-A:7:intent-A:17'})


def test_already_delivered_submission_retry_needs_no_new_audit_write(tmp_path):
    audit = _AuditSink(tmp_path / 'attempts')
    tracker = OrderLifecycleTracker(audit)
    try:
        tracker.record_submission(_submission(), timeout=1)
        assert tracker.health['pending_submissions'] == 0
        def unavailable(*args):
            raise OSError('audit endpoint is now unavailable')
        audit.append_once = unavailable
        tracker.record_submission(_submission(), timeout=.05)
        assert len(audit.events) == 1
        assert tracker.health['pending_submissions'] == 0
        assert tracker.health['healthy'] is True
    finally:
        tracker.close(timeout=.2)


@pytest.mark.parametrize('journal_was_started', [False, True])
def test_unavailable_or_closed_tracker_cannot_acknowledge_new_submission(tmp_path, journal_was_started):
    tracker = OrderLifecycleTracker(_AuditSink(tmp_path / 'closed') if journal_was_started else None)
    if journal_was_started:
        tracker.close(timeout=.2)
    with pytest.raises(RuntimeError):
        tracker.record_submission(_submission(), timeout=.02)


def test_positive_subsecond_submission_budget_waits_for_audit_ack(tmp_path):
    audit = _AuditSink(tmp_path / 'delayed-ack')
    release, waiting = threading.Event(), threading.Event()
    append = audit.append_once
    def delayed(event, identity):
        assert release.wait(2)
        append(event, identity)
    audit.append_once = delayed
    tracker = OrderLifecycleTracker(audit)
    class ObservedWait(threading.Event):
        def wait(self, timeout=None):
            waiting.set()
            return super().wait(timeout)
    tracker._submission_wake = ObservedWait()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(tracker.record_submission, _submission(), .75)
            try:
                assert waiting.wait(.3), 'a positive budget was refused before waiting'
                assert not result.done()
            finally:
                release.set()
            result.result(timeout=1)
        assert tracker.health['pending_submissions'] == 0
    finally:
        release.set()
        tracker.close(timeout=.2)


@pytest.mark.parametrize('failure_stage', ['ingestion', 'audit'])
def test_transient_storage_failure_retries_without_another_broker_callback(tmp_path, failure_stage):
    audit = _AuditSink(tmp_path / failure_stage)
    tracker = OrderLifecycleTracker(audit)
    failure_seen, available = threading.Event(), threading.Event()
    attempts = 0
    original = tracker._persist_observation if failure_stage == 'ingestion' else audit.append_once
    def temporary_outage(*args):
        nonlocal attempts
        attempts += 1
        if not available.is_set():
            if attempts >= 2:
                failure_seen.set()
            raise OSError('temporary execution storage outage')
        return original(*args)
    if failure_stage == 'ingestion':
        tracker._persist_observation = temporary_outage
    else:
        audit.append_once = temporary_outage
    try:
        _send(tracker, _trade(status='Filled', filled=1, quantity=1, average=.5), flush=False)
        assert failure_seen.wait(.5)
        assert tracker.health['healthy'] is False
        assert tracker.health['error']
        available.set()  # deliberately do not wake the tracker or submit another event
        assert audit.delivered.wait(.75), 'quiescent recovery needs an independent retry'
        assert tracker.flush(timeout=.5)
        assert tracker.health['healthy'] is True
        assert tracker.health['error'] == ''
        assert len(audit.events) == 1
    finally:
        available.set()
        tracker.close(timeout=.2)


def test_outbox_drains_multiple_pages_after_audit_unblocks_without_new_observations(tmp_path):
    audit = _AuditSink(tmp_path / 'pages')
    entered, release = threading.Event(), threading.Event()
    append = audit.append_once
    def blocked(event, identity):
        entered.set()
        assert release.wait(5)
        append(event, identity)
    audit.append_once = blocked
    tracker = OrderLifecycleTracker(audit)
    async def observations():
        for index in range(300):
            tracker.on_trade(_trade(order_id=index + 1, perm_id=index + 1000,
                                     status='Filled', filled=1, quantity=1, average=.5))
        for _ in range(400):
            if tracker.health['pending'] == 0:
                return
            await asyncio.sleep(.005)
        pytest.fail('ingestion did not finish independently of blocked audit delivery')
    try:
        asyncio.run(observations())
        assert entered.wait(.5)
        assert tracker.flush(timeout=.02) is False
        release.set()
        assert tracker.flush(timeout=2)
        assert len(audit.events) == 300
        assert sum(event.quantity for event in audit.events.values()) == 300
        assert tracker.health['healthy'] is True
    finally:
        release.set()
        tracker.close(timeout=.2)
