"""Broker evidence and recovery examples staged outside the active oracle."""

import asyncio
import datetime as dt
import json
import math
import sys
import threading
from types import SimpleNamespace

import pytest
from ib_async import Contract, Execution, Order, OrderStatus, Trade

from trader.data.event_store import EventStore, EventType
from trader.trading.order_lifecycle import OrderLifecycleTracker


def _trade(*, order_id=17, perm_id=1717, status="Submitted", filled=0.0,
           total=4.0, average=0.0, account="paper-A", reference="owner|mmr:entry-A",
           action="BUY", conid=123):
    return Trade(
        contract=Contract(conId=conid, symbol="OWN"),
        order=Order(orderId=order_id, clientId=7, permId=perm_id, account=account,
                    orderRef=reference, action=action, orderType="MKT", totalQuantity=total),
        orderStatus=OrderStatus(status=status, filled=filled, avgFillPrice=average, permId=perm_id),
    )


def _fill(*, exec_id="receipt-A", cumulative=1.0, shares=1.0, price=0.5,
          average=0.5, order_id=17, perm_id=1717, account="paper-A"):
    return SimpleNamespace(execution=Execution(
        execId=exec_id, orderId=order_id, clientId=7, permId=perm_id,
        acctNumber=account, orderRef="owner|mmr:entry-A", cumQty=cumulative,
        shares=shares, price=price, avgPrice=average,
        time=dt.datetime(2026, 9, 9, 12, tzinfo=dt.timezone.utc),
    ))


def _without(record, field):
    """A legacy/minimal adapter, distinct from the native dataclass controls."""
    return SimpleNamespace(**{key: value for key, value in vars(record).items() if key != field})


def _send(tracker, trade, fill=None, *, completed=False, flush=True):
    async def callback():
        if fill is None:
            tracker.on_trade(trade, completed=completed)
        else:
            tracker.on_execution(trade, fill, completed=completed)
    asyncio.run(callback())
    if flush:
        assert tracker.flush(timeout=1.0), tracker.health


def _events(store, kind=EventType.ORDER_FILLED):
    return sorted(store.query_since(dt.datetime(2000, 1, 1), kind), key=lambda event: event.timestamp)


@pytest.fixture
def lifecycle(tmp_path):
    store = EventStore(str(tmp_path / "events.duckdb"))
    tracker = OrderLifecycleTracker(store)
    try:
        yield tracker, store
    finally:
        tracker.close(timeout=0.1)


def test_unattached_tracker_has_boolean_readiness_empty_flush_and_safe_close():
    tracker = OrderLifecycleTracker()
    assert tracker.health["replay_required"] is False
    assert tracker.health["healthy"] is True
    assert tracker.flush(timeout=0.01) is True
    tracker.close(timeout=0.0)


def test_unattached_snapshot_filter_does_not_stop_at_an_earlier_unrequested_order():
    tracker = OrderLifecycleTracker()
    try:
        _send(tracker, _trade(order_id=1, perm_id=101), flush=False)
        _send(tracker, _trade(order_id=2, perm_id=102), flush=False)
        row, = tracker.snapshot([2])
        assert row["orderId"] == 2
        assert row["permId"] == 102
    finally:
        tracker.close(timeout=0.0)


def test_one_timed_out_waiter_does_not_remove_a_live_waiter_for_the_same_order():
    tracker = OrderLifecycleTracker()

    async def scenario():
        short = asyncio.create_task(tracker.wait_decisive(17, timeout=0.01))
        long = asyncio.create_task(tracker.wait_decisive(17, timeout=1.0))
        assert await short == "timeout"
        tracker.on_trade(_trade(status="Submitted"))
        assert await long == "accepted"

    try:
        asyncio.run(scenario())
    finally:
        tracker.close(timeout=0.0)


def test_small_positive_completed_quantity_remains_known(lifecycle):
    tracker, _ = lifecycle
    _send(tracker, _trade(status="Cancelled", filled=0.5, average=0.25), completed=True)
    row, = tracker.snapshot()
    assert row["status"] == "Cancelled"
    assert row["filled"] == 0.5
    assert row["fillQuantityKnown"] is True
    assert row["remaining"] == 3.5


def test_live_filled_status_with_only_partial_receipt_remains_unknown(lifecycle):
    tracker, _ = lifecycle
    _send(tracker, _trade(status="Filled"), _fill(cumulative=0.5, shares=0.5))
    row, = tracker.snapshot()
    assert row["brokerStatus"] == "Filled"
    assert row["status"] == "Unknown"
    assert row["filled"] == 0.5
    assert row["fillQuantityKnown"] is False
    assert row["remaining"] is None


@pytest.mark.parametrize("total", [0.0, sys.float_info.max])
def test_unknown_total_never_turns_a_receipt_lower_bound_into_final_quantity(lifecycle, total):
    tracker, _ = lifecycle
    _send(tracker, _trade(status="Cancelled", total=total), _fill(), completed=True)
    row, = tracker.snapshot()
    assert row["totalQuantity"] == 0.0
    assert row["filled"] == 1.0
    assert row["fillQuantityKnown"] is False
    assert row["status"] == "Unknown"


def test_zero_execution_cumulative_does_not_fabricate_one_share(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(status="Filled"), _fill(cumulative=0.0, shares=0.0), completed=True)
    row, = tracker.snapshot()
    assert row["filled"] == 0.0
    assert row["fillQuantityKnown"] is False
    assert _events(store) == []


def test_known_zero_cancellation_survives_quantity_omitted_completed_replay(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(status="Cancelled"))
    _send(tracker, _trade(status="Cancelled"), completed=True)
    row, = tracker.snapshot()
    assert row["status"] == "Cancelled"
    assert row["fillQuantityKnown"] is True
    assert row["filled"] == 0.0
    assert len(_events(store, EventType.ORDER_CANCELLED)) == 1


def test_native_empty_account_action_and_instrument_do_not_become_invented_values(lifecycle):
    tracker, _ = lifecycle
    _send(tracker, _trade(account="", action="", conid=0), _fill(account=""))
    row, = tracker.snapshot()
    assert row["account"] == ""
    assert row["action"] == ""
    assert row["conId"] == 0


@pytest.mark.parametrize("conflict", ["permanent", "provisional"])
def test_identity_refusal_keeps_the_actual_cause_in_readiness(conflict, monkeypatch):
    from trader.trading import order_lifecycle

    class ObservationClock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            stamp = dt.datetime(2026, 9, 9, 12, tzinfo=dt.timezone.utc)
            return stamp.astimezone(tz) if tz else stamp.replace(tzinfo=None)

    monkeypatch.setattr(order_lifecycle, "dt", SimpleNamespace(datetime=ObservationClock, timezone=dt.timezone))
    tracker = OrderLifecycleTracker()
    monkeypatch.setattr(order_lifecycle.logging, "exception", lambda *args, **kwargs: None)
    try:
        if conflict == "permanent":
            trade = _trade()
            trade.orderStatus.permId = 2727
            _send(tracker, trade, flush=False)
            cause = "contradict"
        else:
            _send(tracker, _trade(perm_id=0, reference="first"), flush=False)
            # A changed durable reference cannot silently reuse the provisional
            # identity in the same observation session.
            _send(tracker, _trade(perm_id=0, reference="different"), flush=False)
            cause = "ambig"
        health = tracker.health
        assert health["healthy"] is False
        assert health["replay_required"] is True
        assert cause in health["error"].casefold()
        assert "identit" in health["error"].casefold()
    finally:
        tracker.close(timeout=0.0)


@pytest.mark.parametrize("missing_id", [False, True])
def test_empty_or_missing_execution_id_is_not_a_shared_durable_receipt(lifecycle, missing_id):
    tracker, store = lifecycle
    for cumulative in (1.0, 2.0):
        fill = _fill(exec_id="", cumulative=cumulative)
        if missing_id:
            fill.execution = _without(fill.execution, "execId")
        _send(tracker, _trade(filled=cumulative, average=0.5), fill)
    assert tracker.execution_receipts() == []
    assert sum(event.quantity for event in _events(store)) == 2.0


@pytest.mark.parametrize(
    "component,field,wire_key,expected",
    [("order", "orderId", "orderId", 0),
     ("order", "action", "action", ""),
     ("order", "account", "account", ""),
     ("contract", "conId", "conId", 0)],
)
def test_legacy_adapter_missing_identity_field_stays_unknown(
    lifecycle, component, field, wire_key, expected,
):
    tracker, _ = lifecycle
    trade = _trade()
    setattr(trade, component, _without(getattr(trade, component), field))
    _send(tracker, trade)
    row, = tracker.snapshot()
    assert row[wire_key] == expected


def test_missing_legacy_status_does_not_invent_a_broker_observation(lifecycle):
    tracker, _ = lifecycle
    trade = _trade()
    trade.orderStatus = _without(trade.orderStatus, "status")
    _send(tracker, trade)
    assert tracker.snapshot() == []


@pytest.mark.parametrize("missing", ["avgFillPrice", "avgPrice", "price", "shares", "cumQty"])
def test_legacy_adapter_missing_price_or_receipt_fact_is_never_one(lifecycle, missing):
    tracker, _ = lifecycle
    trade = _trade(filled=1.0, average=0.0)
    fill = _fill(price=0.0, average=0.0)
    if missing == "avgFillPrice":
        trade.orderStatus = _without(trade.orderStatus, missing)
        _send(tracker, trade)
        row, = tracker.snapshot()
        assert row["avgFillPrice"] == 0.0
    else:
        fill.execution = _without(fill.execution, missing)
        _send(tracker, trade, fill)
        row, = tracker.snapshot()
        if missing in ("avgPrice", "price"):
            assert row["avgFillPrice"] == 0.0
        if missing in ("price", "shares", "cumQty"):
            assert row["execution"][missing] == 0.0


def test_an_older_receipt_cannot_replace_a_newer_cumulative_cost(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(filled=40.0, total=100.0, average=100.0),
          _fill(cumulative=40.0, shares=40.0, price=100.0, average=100.0))
    _send(tracker, _trade(filled=100.0, total=100.0, average=106.0),
          _fill(cumulative=40.0, shares=40.0, price=100.0, average=100.0))
    fills = _events(store)
    assert [(event.quantity, event.price) for event in fills] == [(40.0, 100.0), (60.0, 110.0)]
    assert tracker.snapshot()[0]["avgFillPrice"] == 106.0


@pytest.mark.parametrize("bad_receipt_price", [0.0, sys.float_info.max])
def test_invalid_receipt_price_cannot_erase_an_independently_known_incremental_price(lifecycle, bad_receipt_price):
    tracker, store = lifecycle
    _send(tracker, _trade(filled=1.0, average=10.0))
    _send(tracker, _trade(filled=2.0, average=15.0),
          _fill(cumulative=2.0, price=bad_receipt_price, average=15.0))
    last = _events(store)[-1]
    assert last.quantity == 1.0
    assert last.price == 20.0
    assert last.metadata["price_evaluable"] is True


def test_a_matching_penny_receipt_prices_its_delta_when_earlier_cost_is_unknown(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(filled=1.0, average=0.0))
    _send(tracker, _trade(filled=2.0, average=0.0),
          _fill(cumulative=2.0, shares=1.0, price=0.5, average=0.0))
    first, last = _events(store)
    assert first.metadata["price_evaluable"] is False
    assert last.quantity == 1.0
    assert last.price == 0.5
    assert last.metadata["price_evaluable"] is True
    assert tracker.snapshot()[0]["avgFillPrice"] == 0.0
    with tracker._journal.transaction() as conn:
        checkpoint = conn.execute("SELECT cumulative,notional FROM broker_order_progress").fetchall()
    assert [tuple(row) for row in checkpoint] == [(2.0, 0.0)]


@pytest.mark.parametrize("first_price", [0.0, 0.25])
def test_late_small_cost_evidence_is_preserved_for_the_next_delta(lifecycle, first_price):
    tracker, store = lifecycle
    _send(tracker, _trade(filled=1.0, average=first_price))
    _send(tracker, _trade(filled=1.0, average=0.5))
    assert tracker.snapshot()[0]["avgFillPrice"] == 0.5
    _send(tracker, _trade(filled=2.0, average=0.75))
    fills = _events(store)
    assert len(fills) == 2
    assert fills[-1].quantity == 1.0
    assert fills[-1].price == 1.0
    assert fills[-1].metadata["price_evaluable"] is True


@pytest.mark.parametrize("quantity,average", [(1.0, -1.0), (2.0, 1e308)])
def test_invalid_or_overflowed_cost_stays_unknown_and_has_a_finite_durable_checkpoint(
    lifecycle, quantity, average,
):
    tracker, store = lifecycle
    _send(tracker, _trade(filled=quantity, average=average))
    event, = _events(store)
    assert event.quantity == quantity
    assert event.price == 0.0
    assert event.metadata["price_evaluable"] is False
    with tracker._journal.transaction() as conn:
        notional, = conn.execute("SELECT notional FROM broker_order_progress").fetchone()
    assert math.isfinite(notional) and notional == 0.0


def test_unset_average_is_not_a_real_evaluable_fill_price(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(filled=1.0, average=sys.float_info.max))
    event, = _events(store)
    assert event.price == 0.0
    assert event.metadata["price_evaluable"] is False


def test_cancel_then_pending_then_fill_retains_the_cancellation_race_audit(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(status="Cancelled", total=1.0))
    _send(tracker, _trade(status="Submitted", total=1.0))
    _send(tracker, _trade(status="Filled", total=1.0, filled=1.0, average=10.0))
    event, = _events(store)
    assert event.quantity == 1.0
    assert event.metadata["superseded"] == "Cancelled"
    assert len(_events(store, EventType.ORDER_CANCELLED)) == 1


def test_a_superseded_cancellation_is_not_reannounced_on_every_later_fill_receipt(lifecycle):
    tracker, store = lifecycle
    _send(tracker, _trade(status="Cancelled"))
    # Execution receipts can increase a cumulative checkpoint after the first
    # terminal status was observed. Only the first fill supersedes cancellation.
    _send(tracker, _trade(status="Filled", filled=1.0, average=10.0))
    _send(tracker, _trade(status="Filled", filled=2.0, average=10.0))
    first, second = _events(store)
    assert [first.quantity, second.quantity] == [1.0, 1.0]
    assert first.metadata["superseded"] == "Cancelled"
    assert "superseded" not in second.metadata


@pytest.mark.parametrize("average,expected", [(0.25, 0.25), (0.0, 0.0), (sys.float_info.max, 0.0)])
def test_legacy_lower_snapshot_restores_broker_terminality_and_honest_price(tmp_path, average, expected):
    store = EventStore(str(tmp_path / "legacy.duckdb"))
    first = OrderLifecycleTracker(store)
    quantity = 1.0 if average == sys.float_info.max else 2.0
    _send(first, _trade(status="Cancelled", filled=quantity, average=average))
    first.close(timeout=0.1)
    before = [(event.quantity, event.price) for event in _events(store)]
    # Saved state from the former late-attachment bug: numeric checkpoint is
    # authoritative, while its JSON is older and lacks the brokerStatus field.
    with first._journal.transaction() as conn:
        identity, payload = conn.execute("SELECT identity,snapshot FROM broker_order_progress").fetchone()
        row = json.loads(payload)
        row.update(filled=0.0, avgFillPrice=0.0)
        row.pop("brokerStatus")
        conn.execute("UPDATE broker_order_progress SET snapshot=? WHERE identity=?", [json.dumps(row), identity])
    restored = OrderLifecycleTracker(store)
    try:
        row, = restored.snapshot()
        assert row["brokerStatus"] == "Cancelled"
        assert row["status"] == "Unknown"
        assert row["filled"] == quantity
        assert row["fillQuantityKnown"] is False
        assert row["remaining"] is None
        assert row["avgFillPrice"] == expected
        assert [(event.quantity, event.price) for event in _events(store)] == before
    finally:
        restored.close(timeout=0.1)


def test_same_quantity_missing_legacy_json_price_does_not_erase_durable_cost(tmp_path):
    store = EventStore(str(tmp_path / "legacy-price.duckdb"))
    first = OrderLifecycleTracker(store)
    _send(first, _trade(filled=2.0, average=10.0))
    first.close(timeout=0.1)
    with first._journal.transaction() as conn:
        identity, payload = conn.execute("SELECT identity,snapshot FROM broker_order_progress").fetchone()
        row = json.loads(payload)
        row["avgFillPrice"] = 0.0
        conn.execute("UPDATE broker_order_progress SET snapshot=? WHERE identity=?", [json.dumps(row), identity])
    restored = OrderLifecycleTracker(store)
    try:
        _send(restored, _trade(filled=2.0, average=0.0))
        _send(restored, _trade(filled=3.0, average=10.0))
        fills = _events(store)
        assert [(event.quantity, event.price) for event in fills] == [(2.0, 10.0), (1.0, 10.0)]
        assert fills[-1].metadata["price_evaluable"] is True
    finally:
        restored.close(timeout=0.1)


def test_legacy_reference_separates_scoped_ids_after_restart(tmp_path):
    store = EventStore(str(tmp_path / "legacy-ref.duckdb"))
    first = OrderLifecycleTracker(store)
    _send(first, _trade(perm_id=1717, reference="owner-A", filled=1.0, average=10.0))
    _send(first, _trade(perm_id=2727, reference="owner-B", filled=2.0, average=20.0))
    first.close(timeout=0.1)
    with first._journal.transaction() as conn:
        for identity, payload in conn.execute("SELECT identity,snapshot FROM broker_order_progress").fetchall():
            row = json.loads(payload)
            row.pop("brokerOrderRef")
            conn.execute("UPDATE broker_order_progress SET snapshot=? WHERE identity=?", [json.dumps(row), identity])
    restored = OrderLifecycleTracker(store)
    try:
        _send(restored, _trade(perm_id=0, reference="owner-A", filled=1.0, average=10.0))
        assert len(restored.snapshot()) == 2
        assert sum(row["filled"] for row in restored.snapshot()) == 3.0
        assert sum(event.quantity for event in _events(store)) == 3.0
    finally:
        restored.close(timeout=0.1)


def test_original_receipt_key_is_still_idempotent_when_replayed(lifecycle):
    tracker, _ = lifecycle
    # This is an immutable pre-upgrade receipt identity, not a key constructed
    # by the implementation whose compatibility is being checked.
    receipt = dict(execId="old-receipt", orderId=17, shares=1.0, cumQty=1.0,
                   price=0.5, account="paper-A", permId=1717,
                   time="2026-09-09T12:00:00+00:00")
    with tracker._journal.transaction() as conn:
        conn.execute("INSERT INTO broker_execution_receipts VALUES (?, ?)",
                     ["paper-A:old-receipt", json.dumps(receipt)])
    _send(tracker, _trade(filled=1.0, average=0.5), _fill(exec_id="old-receipt"))
    assert tracker.execution_receipts() == [receipt]


def test_buffered_identity_error_stays_replay_required_after_unrelated_success(tmp_path, monkeypatch):
    from trader.trading import order_lifecycle

    class ObservationClock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            stamp = dt.datetime(2026, 9, 9, 12, tzinfo=dt.timezone.utc)
            return stamp.astimezone(tz) if tz else stamp.replace(tzinfo=None)

    monkeypatch.setattr(order_lifecycle, "dt", SimpleNamespace(datetime=ObservationClock, timezone=dt.timezone))
    store = EventStore(str(tmp_path / "buffered.duckdb"))
    first = OrderLifecycleTracker(store)
    _send(first, _trade(perm_id=0, reference="owner|mmr:old"))
    first.close(timeout=0.1)
    buffered = OrderLifecycleTracker()
    errors_before_worker_start = []

    class ObservingThread(threading.Thread):
        def start(self):
            errors_before_worker_start.append(buffered.health["error"])
            super().start()

    monkeypatch.setattr(order_lifecycle, "threading", SimpleNamespace(
        RLock=threading.RLock, Lock=threading.Lock, Event=threading.Event,
        Thread=ObservingThread,
    ))
    monkeypatch.setattr(order_lifecycle.logging, "exception", lambda *args, **kwargs: None)
    try:
        _send(buffered, _trade(perm_id=0, reference="owner|mmr:new"), flush=False)
        _send(buffered, _trade(order_id=18, perm_id=1818, reference="unrelated"), flush=False)
        buffered.set_event_store(store)
        assert "ambig" in errors_before_worker_start[0].casefold()
        assert "identit" in errors_before_worker_start[0].casefold()
        assert buffered.flush(timeout=1.0), buffered.health
        assert buffered.health["replay_required"] is True
        assert buffered.health["healthy"] is False
        assert {row["orderRef"] for row in buffered.snapshot()} == {"owner", "unrelated"}
        assert buffered.snapshot([17])[0]["clientIntentId"] == "old"
    finally:
        buffered.close(timeout=0.1)


@pytest.mark.parametrize(
    "reported,status_average,cumulative,shares,price,execution_average,expected_average",
    [
        pytest.param(1.0, 10.0, 2.0, 1.0, 20.0, 0.0, 0.0, id="lagging-status-partial-receipt"),
        pytest.param(2.0, 15.0, 2.0, 1.0, 20.0, 0.0, 15.0, id="matching-status-average"),
        pytest.param(2.0, 15.0, 2.0, 1.0, 20.0, 12.0, 12.0, id="known-execution-average"),
        pytest.param(0.0, 0.0, 2.0, 2.0, 12.0, 0.0, 12.0, id="full-cumulative-receipt"),
        pytest.param(3.0, 17.0, 2.0, 1.0, 20.0, 10.0, 17.0, id="older-execution-receipt"),
        pytest.param(0.0, 0.0, 2.0, 1.0, 20.0, 0.0, 0.0, id="missing-earlier-cost"),
    ],
)
def test_cumulative_average_requires_price_evidence_for_the_same_endpoint(
        lifecycle, reported, status_average, cumulative, shares, price,
        execution_average, expected_average):
    tracker, store = lifecycle
    _send(tracker, _trade(filled=reported, average=status_average),
          _fill(cumulative=cumulative, shares=shares, price=price, average=execution_average))
    rows = tracker.snapshot()
    assert len(rows) == 1
    row = rows[0]
    endpoint = max(reported, cumulative)
    assert row["filled"] == endpoint
    assert row["avgFillPrice"] == expected_average
    assert row["execution"]["price"] == price
    assert row["execution"]["shares"] == shares
    events = _events(store)
    assert len(events) == 1
    assert (events[0].quantity, events[0].price) == (endpoint, expected_average)
    assert events[0].metadata["price_evaluable"] is (expected_average > 0)
    with tracker._journal.transaction() as conn:
        durable = conn.execute("SELECT cumulative,notional FROM broker_order_progress").fetchall()
    assert [tuple(row) for row in durable] == [(endpoint, endpoint * expected_average)]


@pytest.mark.parametrize(
    "reported,expected_average",
    [pytest.param(1.0, 0.0, id="status-average-is-older"),
     pytest.param(2.0, 10.0, id="status-average-is-aligned")],
)
def test_completed_quantity_does_not_move_an_older_status_average_forward(
        lifecycle, reported, expected_average):
    tracker, store = lifecycle
    trade = _trade(status="Cancelled", total=3.0, filled=reported, average=10.0)
    trade.order.filledQuantity = 2.0
    _send(tracker, trade, completed=True)
    rows = tracker.snapshot()
    assert len(rows) == 1
    row = rows[0]
    assert (row["filled"], row["remaining"], row["fillQuantityKnown"]) == (2.0, 1.0, True)
    assert row["avgFillPrice"] == expected_average
    events = _events(store)
    assert len(events) == 1
    assert (events[0].quantity, events[0].price) == (2.0, expected_average)
    assert events[0].metadata["price_evaluable"] is (expected_average > 0)


def test_subdollar_partial_receipt_average_prices_its_cumulative_endpoint(lifecycle):
    tracker, store = lifecycle
    trade = _trade(total=4.0, filled=0.0, average=0.0)
    fill = _fill(cumulative=2.0, shares=1.0, price=0.75, average=0.5)
    # The latest share cost $0.75; the broker's cumulative average proves
    # that both shares together cost $1.00. A sub-dollar average is valid.
    _send(tracker, trade, fill)
    rows = tracker.snapshot()
    assert len(rows) == 1
    assert rows[0]["filled"] == 2.0
    assert rows[0]["avgFillPrice"] == 0.5
    assert (rows[0]["execution"]["shares"], rows[0]["execution"]["price"]) == (1.0, 0.75)
    fills = _events(store)
    assert [(event.quantity, event.price) for event in fills] == [(2.0, 0.5)]
    assert fills[0].metadata["price_evaluable"] is True
    with tracker._journal.transaction() as conn:
        checkpoint = conn.execute("SELECT cumulative,notional FROM broker_order_progress").fetchall()
    assert [tuple(row) for row in checkpoint] == [(2.0, 1.0)]
    receipts = tracker.execution_receipts()
    assert len(receipts) == 1
    assert (receipts[0]["cumQty"], receipts[0]["shares"], receipts[0]["price"]) == (2.0, 1.0, 0.75)
    _send(tracker, trade, fill)
    assert [(event.quantity, event.price) for event in _events(store)] == [(2.0, 0.5)]
    assert tracker.execution_receipts() == receipts
    assert tracker.snapshot()[0]["filled"] == 2.0
    assert tracker.snapshot()[0]["avgFillPrice"] == 0.5


@pytest.mark.parametrize(
    "sentinel_source",
    [pytest.param("status-average", id="status-average"),
     pytest.param("execution-average", id="execution-average"),
     pytest.param("full-receipt-price", id="full-receipt-price")],
)
def test_native_unset_price_cannot_replace_known_endpoint_cost(lifecycle, sentinel_source):
    tracker, store = lifecycle
    _send(tracker, _trade(filled=1.0, average=0.5))
    assert [(event.quantity, event.price) for event in _events(store)] == [(1.0, 0.5)]
    if sentinel_source == "status-average":
        _send(tracker, _trade(filled=1.0, average=sys.float_info.max))
    elif sentinel_source == "execution-average":
        _send(tracker, _trade(filled=1.0, average=0.0),
              _fill(cumulative=1.0, shares=1.0, price=0.5, average=sys.float_info.max))
    else:
        _send(tracker, _trade(filled=1.0, average=0.0),
              _fill(cumulative=1.0, shares=1.0, price=sys.float_info.max, average=0.0))
    rows = tracker.snapshot()
    assert len(rows) == 1
    assert rows[0]["filled"] == 1.0
    assert rows[0]["avgFillPrice"] == 0.5
    with tracker._journal.transaction() as conn:
        checkpoint = conn.execute("SELECT cumulative,notional FROM broker_order_progress").fetchall()
    assert [tuple(row) for row in checkpoint] == [(1.0, 0.5)]
    assert [(event.quantity, event.price) for event in _events(store)] == [(1.0, 0.5)]
    receipts = tracker.execution_receipts()
    assert len(receipts) == (0 if sentinel_source == "status-average" else 1)
    # The new cumulative endpoint is $1.50. Retaining the first share's
    # valid $0.50 cost makes the newly observed share cost exactly $1.00.
    advanced = _trade(filled=2.0, average=0.75)
    _send(tracker, advanced)
    fills = _events(store)
    assert [(event.quantity, event.price) for event in fills] == [(1.0, 0.5), (1.0, 1.0)]
    assert all(event.metadata["price_evaluable"] is True for event in fills)
    assert tracker.snapshot()[0]["filled"] == 2.0
    assert tracker.snapshot()[0]["avgFillPrice"] == 0.75
    with tracker._journal.transaction() as conn:
        checkpoint = conn.execute("SELECT cumulative,notional FROM broker_order_progress").fetchall()
    assert [tuple(row) for row in checkpoint] == [(2.0, 1.5)]
    _send(tracker, advanced)
    assert [(event.quantity, event.price) for event in _events(store)] == [(1.0, 0.5), (1.0, 1.0)]
    assert tracker.execution_receipts() == receipts
