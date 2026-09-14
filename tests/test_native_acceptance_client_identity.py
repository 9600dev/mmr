"""An older broker client's rejection cannot cancel a newly accepted order.

The real coordinator, lifecycle tracker and durable journal use the existing
in-memory broker. A late historical Trade arrives at the callback boundary.
"""

from ib_async import Order, OrderStatus, Stock, Trade
import pytest

from review.test_review_order_contract import _coordinated_trader, _stock


@pytest.mark.asyncio
async def test_foreign_client_rejection_does_not_cancel_current_accepted_order(tmp_path):
    trader = _coordinated_trader(tmp_path, held=100)
    acknowledged = trader.client.subscribe_place_order.side_effect

    async def receive_historical_status_after_current_ack(contract, order):
        observation = await acknowledged(contract, order)
        assert trader.placed[-1].orderStatus.status == "Submitted"
        historical = Trade(
            Stock("HISTORY", "SMART", "USD", conId=202),
            Order(orderId=order.orderId, clientId=99, permId=99001,
                  account=trader.ib_account, action="BUY", totalQuantity=5,
                  filledQuantity=0, orderType="LMT",
                  orderRef="other-owner|mmr:old-client-order"),
            OrderStatus(orderId=order.orderId, clientId=99, permId=99001,
                        status="Cancelled", filled=0, remaining=5),
        )
        # Completed history supplies its own Trade without advancing the
        # native allocator. Explicit filledQuantity=0 proves final zero;
        # completedOrder's default status.filled=0 alone would be unknown.
        trader.order_tracker.on_trade(historical, completed=True)
        return observation

    trader.client.subscribe_place_order.side_effect = receive_historical_status_after_current_ack
    try:
        result = await trader.place_expressive_order(
            _stock(), "SELL", 10, {"order_type": "MARKET"},
            algo_name="current-owner", client_intent_id="current-client-order")

        assert result.is_success(), result.error
        current, = trader.placed
        assert current.order.clientId == 7
        assert current.orderStatus.status == "Submitted"
        trader.client.ib.cancelOrder.assert_not_called()
        assert trader.order_tracker.flush(timeout=1)
        rows = {row["clientId"]: row for row in trader.order_tracker.snapshot()}
        assert set(rows) == {7, 99}
        assert rows[7]["orderId"] == rows[99]["orderId"] == current.order.orderId
        assert rows[7]["clientIntentId"] == "current-client-order"
        assert rows[7]["status"] == "Submitted"
        assert rows[99]["status"] == "Cancelled" and rows[99]["filled"] == 0
        assert trader.inventory == 100, "acceptance is not a fill"
    finally:
        trader.order_tracker.close(timeout=1)
        journal = getattr(trader, "_server_order_journal", None)
        if journal is not None:
            journal.journal.close()




# These controls exercise native callback acceptance without a persistence worker.
# They do not place orders or manufacture lifecycle snapshots/receipt mappings.
import asyncio

from trader.trading.order_lifecycle import OrderLifecycleTracker


def _acceptance_domain_trade(action, client_id, *, order_id=17, con_id=100,
                             permanent_id=0, status_permanent_id=None,
                             status="PendingSubmit"):
    if status_permanent_id is None:
        status_permanent_id = permanent_id
    return Trade(
        Stock("ACCEPT", "SMART", "USD", conId=con_id),
        Order(orderId=order_id, clientId=client_id, permId=permanent_id,
              account="TEST-ACCEPTANCE", action=action, totalQuantity=10,
              orderType="MKT", orderRef="manual-acceptance-domain"),
        OrderStatus(orderId=order_id, clientId=client_id, permId=status_permanent_id,
                    status=status, filled=0, remaining=10),
    )


async def _close_acceptance_domain_tracker(tracker, waiter):
    if waiter is not None:
        if not waiter.done():
            waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
    # No event store was attached, so no worker or journal needs draining.
    tracker.close(timeout=0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action,client_id,order_id,con_id,order_permanent,status_permanent",
    [
        pytest.param("BUY", 0, 17, 100, 1002, 1002, id="buy-client-zero"),
        pytest.param("SELL", 0, 17, 100, 1002, 1002, id="sell-client-zero"),
        pytest.param("BUY", 7, 17, 100, 1002, 1002, id="buy-client-seven"),
        pytest.param("SELL", 7, 17, 100, 1002, 1002, id="sell-client-seven"),
        pytest.param("BUY", 7, 1, 1, 0, 1002, id="status-permanent-positive-minima"),
        pytest.param("SELL", 0, 1, 1, 1002, 0, id="order-permanent-positive-minima"),
    ],
)
async def test_native_happy_acknowledgment_matches_exact_trade(
        action, client_id, order_id, con_id, order_permanent, status_permanent):
    tracker = OrderLifecycleTracker()
    current = _acceptance_domain_trade(
        action, client_id, order_id=order_id, con_id=con_id,
        permanent_id=order_permanent, status_permanent_id=status_permanent)
    waiter = None
    try:
        tracker.on_trade(current)
        waiter = asyncio.create_task(
            tracker.wait_decisive(order_id, timeout=1, trade=current))
        await asyncio.sleep(0)
        assert not waiter.done(), "PendingSubmit does not establish broker acceptance"

        current.orderStatus.status = "Submitted"
        tracker.on_trade(current)
        assert await asyncio.wait_for(waiter, timeout=2) == "accepted"
        assert tracker.latest_status(order_id, trade=current) == "Submitted"
        row, = tracker.snapshot()
        assert (row["account"], row["clientId"], row["orderId"], row["permId"],
                row["conId"], row["action"]) == (
                    "TEST-ACCEPTANCE", client_id, order_id, 1002, con_id, action)
        assert row["filled"] == 0 and row["remaining"] == 10
    finally:
        await _close_acceptance_domain_tracker(tracker, waiter)


class _AcceptanceDomainIdentityOrder(set):
    """Choose traversal order without changing any producer-created member."""

    def __init__(self, members, *, old_identity, old_first):
        super().__init__(members)
        self.old_identity = old_identity
        self.old_first = old_first

    def __iter__(self):
        members = tuple(set.__iter__(self))
        old = tuple(identity for identity in members if identity == self.old_identity)
        others = tuple(identity for identity in members if identity != self.old_identity)
        return iter(old + others if self.old_first else others + old)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["BUY", "SELL"])
@pytest.mark.parametrize("old_first", [True, False], ids=["old-first", "new-first"])
async def test_native_provisional_promotion_accepts_either_identity_order(action, old_first):
    tracker = OrderLifecycleTracker()
    # Native history may contain an older positive permanent identity in a
    # reused numeric/manual-reference scope. This is not a repeated durable
    # server intent and does not assume every reconnect resets order IDs.
    old = _acceptance_domain_trade(action, 7, permanent_id=1001, status="Cancelled")
    current = _acceptance_domain_trade(action, 7)
    waiter = None
    try:
        tracker.on_trade(old)
        old_row, = tracker.snapshot()
        tracker.on_trade(current)
        waiter = asyncio.create_task(tracker.wait_decisive(17, timeout=1, trade=current))
        await asyncio.sleep(0)
        assert not waiter.done(), "old terminal history cannot answer the new Trade's waiter"
        assert tracker.latest_status(17, trade=current) is None
        assert tracker.latest_status(17, trade=old) == "Cancelled"

        # The set was populated only by the real observations above. Keep its
        # exact members and inherited add/membership operations; choose only
        # which permitted iteration order the next real callback encounters.
        with tracker._lock:
            scope, = tracker._scoped_identities
            original_members = tracker._scoped_identities[scope]
            ordered = _AcceptanceDomainIdentityOrder(
                original_members, old_identity=old_row["identity"], old_first=old_first)
            assert set(set.__iter__(ordered)) == original_members == {old_row["identity"]}
            tracker._scoped_identities[scope] = ordered

        # wait_decisive already froze a zero-permanent selector for this exact
        # object. Its native acknowledgment supplies the distinct positive ID.
        current.order.permId = current.orderStatus.permId = 1002
        current.orderStatus.status = "Submitted"
        tracker.on_trade(current)
        rows = {row["permId"]: row for row in tracker.snapshot()}
        assert set(rows) == {1001, 1002}
        expected_members = {row["identity"] for row in rows.values()}
        assert set(set.__iter__(ordered)) == expected_members
        old_identity, new_identity = rows[1001]["identity"], rows[1002]["identity"]
        assert list(ordered) == (
            [old_identity, new_identity] if old_first else [new_identity, old_identity])

        assert await asyncio.wait_for(waiter, timeout=2) == "accepted"
        assert tracker.latest_status(17, trade=current) == "Submitted"
        assert tracker.latest_status(17, trade=old) == "Cancelled"
        assert tracker.latest_status(17) is None, "numeric-only lookup cannot pick between two orders"
        assert all(row["filled"] == 0 and row["remaining"] == 10 for row in rows.values())
    finally:
        await _close_acceptance_domain_tracker(tracker, waiter)


@pytest.mark.parametrize(
    "requested_id, changes",
    [
        pytest.param(102, {}, id="different-requested-order"),
        pytest.param(0, {"order.orderId": 0, "orderStatus.orderId": 0}, id="unallocated-order"),
        pytest.param(101.0, {}, id="float-requested-order"),
        pytest.param(True, {"order.orderId": 1, "orderStatus.orderId": 1}, id="boolean-requested-order"),
        pytest.param(101, {"order.orderId": 101.0}, id="float-order-identity"),
        pytest.param(101, {"order.clientId": 7.0}, id="float-client-identity"),
        pytest.param(101, {"order.clientId": -1, "orderStatus.clientId": 0}, id="negative-client-identity"),
        pytest.param(101, {"contract.conId": 0}, id="unqualified-instrument"),
        pytest.param(101, {"contract.conId": -202}, id="negative-instrument"),
        pytest.param(101, {"contract.conId": 202.0}, id="float-instrument-identity"),
        pytest.param(101, {"orderStatus.orderId": 102}, id="contradictory-status-order"),
        pytest.param(101, {"orderStatus.clientId": 8}, id="contradictory-status-client"),
        pytest.param(101, {"orderStatus.orderId": 101.0}, id="float-status-order"),
        pytest.param(101, {"orderStatus.clientId": 7.0}, id="float-status-client"),
        pytest.param(101, {"order.account": ""}, id="missing-account"),
        pytest.param(101, {"order.account": 77}, id="nonstring-account"),
        pytest.param(101, {"order.orderRef": None}, id="nonstring-reference"),
        pytest.param(101, {"order.action": "HOLD"}, id="invalid-action"),
        pytest.param(101, {"order.permId": -1, "orderStatus.permId": 0}, id="negative-order-permanent-id"),
        pytest.param(101, {"order.permId": 0, "orderStatus.permId": -1}, id="negative-status-permanent-id"),
        pytest.param(101, {"order.permId": 700.0}, id="float-order-permanent-id"),
        pytest.param(101, {"orderStatus.permId": 700.0}, id="float-status-permanent-id"),
        pytest.param(101, {"orderStatus.permId": 701}, id="contradictory-permanent-id"),
        pytest.param(101, {"order": None}, id="missing-order-object"),
    ],
)
@pytest.mark.asyncio
async def test_unproven_native_selector_is_unknown_without_waiting(requested_id, changes):
    """Unproven queries cannot borrow an earlier real acknowledgment or wait for one.

    Only the query object changes after an ordinary native callback. The
    tracker's broker observations, identity maps and receipt references are
    never fabricated or replaced. Immediate completion is checked by one
    event-loop turn, not by a machine-specific wall-clock threshold.
    """
    import asyncio

    from trader.trading.order_lifecycle import OrderLifecycleTracker

    tracker = OrderLifecycleTracker()
    task = None
    trade = Trade(
        Stock("QUERY", "SMART", "USD", conId=202),
        Order(orderId=101, clientId=7, permId=700,
              account="DU_TEST", action="SELL", totalQuantity=10,
              orderType="LMT", orderRef="query-owner|mmr:query-order"),
        OrderStatus(orderId=101, clientId=7, permId=700,
                    status="Submitted", filled=0, remaining=10),
    )
    try:
        tracker.on_trade(trade)
        assert tracker.latest_status(101, trade=trade) == "Submitted"
        observed_history = tracker.snapshot()

        for path, value in changes.items():
            parent, separator, attribute = path.rpartition(".")
            target = getattr(trade, parent) if separator else trade
            setattr(target, attribute, value)

        assert tracker.latest_status(requested_id, trade=trade) is None
        task = asyncio.create_task(
            tracker.wait_decisive(requested_id, timeout=30.0, trade=trade))
        await asyncio.sleep(0)
        assert task.done(), "an already-unproven selector must not wait for broker evidence"
        assert await task == "timeout"
        assert tracker.snapshot() == observed_history
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        tracker.close(timeout=0)




# These controls extend native_acceptance_domain.append.py. All in-memory
# acceptance state is produced by on_trade; only the explicitly labelled old
# journal fixture below seeds historical rows on disk.
import datetime as dt
import json
from types import SimpleNamespace

from trader.data.event_store import EventStore, EventType
from trader.data.execution_journal import ExecutionJournal


async def _close_acceptance_recovery_tracker(tracker):
    if tracker is None:
        return
    # close() drains/joins workers but does not close its persistent journal.
    await asyncio.to_thread(tracker.close, timeout=2)
    if tracker._journal is not None:
        tracker._journal.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["BUY", "SELL"])
async def test_same_native_trade_remains_accepted_during_pre_callback_promotion(action):
    tracker = OrderLifecycleTracker()
    current = _acceptance_domain_trade(action, 7, status="Submitted")
    try:
        tracker.on_trade(current)
        before, = tracker.snapshot()
        assert before["permId"] == 0 and before["status"] == "Submitted"

        # Native orderStatus updates its fields before emitting its event.
        # The exact same object is still the receipt for the copied perm0 row.
        current.orderStatus.permId = 1002
        assert tracker.snapshot()[0]["permId"] == 0
        assert tracker.latest_status(17, trade=current) == "Submitted"
        assert await tracker.wait_decisive(17, timeout=0, trade=current) == "accepted"

        tracker.on_trade(current)
        after, = tracker.snapshot()
        assert after["permId"] == 1002 and after["identity"] == before["identity"]
        assert after["filled"] == 0 and after["remaining"] == 10
    finally:
        await _close_acceptance_domain_tracker(tracker, None)


@pytest.mark.asyncio
async def test_waiter_for_old_reference_cannot_borrow_updated_reference_acknowledgment():
    tracker = OrderLifecycleTracker()
    current = _acceptance_domain_trade("SELL", 7, permanent_id=1001)
    waiter = None
    try:
        tracker.on_trade(current)
        before, = tracker.snapshot()
        waiter = asyncio.create_task(tracker.wait_decisive(17, timeout=1, trade=current))
        await asyncio.sleep(0)
        assert not waiter.done()

        # openOrder can update an existing Trade's reference. The existing
        # waiter already captured the old full scope; positive permId alone
        # cannot make the new reference answer that older request.
        current.order.orderRef = "manual-acceptance-reference-updated"
        current.orderStatus.status = "Submitted"
        tracker.on_trade(current)
        after, = tracker.snapshot()
        assert after["identity"] == before["identity"]
        assert after["brokerOrderRef"] == current.order.orderRef
        assert tracker.latest_status(17, trade=current) == "Submitted"
        assert await asyncio.wait_for(waiter, timeout=2) == "timeout"
        assert tracker.health["healthy"] is True
    finally:
        await _close_acceptance_domain_tracker(tracker, waiter)


@pytest.mark.asyncio
@pytest.mark.parametrize("old_first", [True, False], ids=["obsolete-first", "current-first"])
async def test_updated_reference_alias_does_not_hide_current_exact_acknowledgment(old_first):
    tracker = OrderLifecycleTracker()
    old = _acceptance_domain_trade("BUY", 7, permanent_id=1001, status="Submitted")
    current = _acceptance_domain_trade("BUY", 7, permanent_id=1002, status="Submitted")
    try:
        tracker.on_trade(old)
        old_before, = tracker.snapshot()
        old.order.orderRef = "manual-acceptance-reference-updated"
        tracker.on_trade(old)
        tracker.on_trade(current)
        rows = {row["permId"]: row for row in tracker.snapshot()}
        assert set(rows) == {1001, 1002}
        assert rows[1001]["identity"] == old_before["identity"]
        assert rows[1001]["brokerOrderRef"] != rows[1002]["brokerOrderRef"]

        # Both memberships came from real callbacks. Preserve them exactly,
        # choosing only the two allowed orders of the existing scope set.
        scope = (current.order.account, current.order.clientId, current.order.orderId,
                 current.order.orderRef, current.contract.conId, current.order.action)
        with tracker._lock:
            original_members = tracker._scoped_identities[scope]
            ordered = _AcceptanceDomainIdentityOrder(
                original_members, old_identity=old_before["identity"], old_first=old_first)
            expected = {rows[1001]["identity"], rows[1002]["identity"]}
            assert set(set.__iter__(ordered)) == original_members == expected
            tracker._scoped_identities[scope] = ordered
        assert list(ordered) == (
            [rows[1001]["identity"], rows[1002]["identity"]] if old_first
            else [rows[1002]["identity"], rows[1001]["identity"]])
        assert tracker.latest_status(17, trade=current) == "Submitted"
        assert await tracker.wait_decisive(17, timeout=0, trade=current) == "accepted"
        assert tracker.latest_status(17, trade=old) == "Submitted"
        assert tracker.latest_status(17) is None
        assert tracker.health["healthy"] is True
    finally:
        await _close_acceptance_domain_tracker(tracker, None)


@pytest.mark.asyncio
async def test_numeric_acceptance_recovers_only_after_each_unproven_object_is_identified():
    tracker = OrderLifecycleTracker()
    known = _acceptance_domain_trade("SELL", 7, permanent_id=1001, status="Submitted")
    first = _acceptance_domain_trade("SELL", 7)
    second = _acceptance_domain_trade("SELL", 7)
    try:
        tracker.on_trade(known)
        tracker.on_trade(first)
        tracker.on_trade(second)
        assert tracker.health["healthy"] is True
        assert tracker.latest_status(17) is None
        assert tracker.latest_status(17, trade=known) == "Submitted"
        assert tracker.latest_status(17, trade=first) is None
        assert tracker.latest_status(17, trade=second) is None

        # These are two reconstructed callback objects, later positively
        # identified as the same known physical order. This does not submit
        # another order or assume a duplicate durable intent can be resent.
        first.order.permId = first.orderStatus.permId = 1001
        first.orderStatus.status = "Submitted"
        tracker.on_trade(first)
        assert tracker.health["healthy"] is True
        assert tracker.latest_status(17, trade=first) == "Submitted"
        assert tracker.latest_status(17) is None, "the second object still lacks physical proof"

        second.order.permId = second.orderStatus.permId = 1001
        second.orderStatus.status = "Submitted"
        tracker.on_trade(second)
        assert tracker.health["healthy"] is True
        assert tracker.latest_status(17) == "Submitted"
        assert await tracker.wait_decisive(17, timeout=0) == "accepted"
        row, = tracker.snapshot()
        assert row["permId"] == 1001 and row["filled"] == 0 and row["remaining"] == 10
    finally:
        await _close_acceptance_domain_tracker(tracker, None)


@pytest.mark.asyncio
async def test_restart_does_not_treat_a_provisional_checkpoint_as_fresh_object_proof(tmp_path):
    store = EventStore(str(tmp_path / "native-provisional-acceptance.duckdb"))
    first = restarted = None
    original = _acceptance_domain_trade("BUY", 7, status="Submitted")
    current = _acceptance_domain_trade("BUY", 7, permanent_id=1002)
    try:
        first = OrderLifecycleTracker(store)
        first.on_trade(original)
        assert await asyncio.to_thread(first.flush, timeout=2)
        await _close_acceptance_recovery_tracker(first)
        first = None

        restarted = OrderLifecycleTracker(store)
        row, = restarted.snapshot()
        assert row["permId"] == 0 and row["status"] == "Submitted"
        # Before this object's first callback, no weak receipt from the old
        # process exists. A positive ID does not identify that saved perm0 row.
        assert restarted.latest_status(17, trade=current) is None
        assert await restarted.wait_decisive(17, timeout=0, trade=current) == "timeout"
        restarted.on_trade(current)
        assert restarted.latest_status(17, trade=current) == "PendingSubmit"
        assert await restarted.wait_decisive(17, timeout=0, trade=current) == "timeout"
        current.orderStatus.status = "Submitted"
        restarted.on_trade(current)
        assert await restarted.wait_decisive(17, timeout=0, trade=current) == "accepted"
        assert await asyncio.to_thread(restarted.flush, timeout=2)
        assert restarted.health["healthy"] is True
        assert store.query_since(dt.datetime(2000, 1, 1), EventType.ORDER_FILLED) == []
    finally:
        await _close_acceptance_recovery_tracker(restarted)
        await _close_acceptance_recovery_tracker(first)


@pytest.mark.asyncio
async def test_ambiguous_dated_legacy_checkpoints_cannot_supply_exact_acceptance(tmp_path, monkeypatch):
    from trader.trading import order_lifecycle

    observed_at = [dt.datetime(2026, 9, 1, 12, tzinfo=dt.timezone.utc)]

    class ObservationClock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return observed_at[0].astimezone(tz) if tz else observed_at[0].replace(tzinfo=None)

    monkeypatch.setattr(order_lifecycle, "dt", SimpleNamespace(
        datetime=ObservationClock, timezone=dt.timezone))
    store = EventStore(str(tmp_path / "dated-legacy-acceptance.duckdb"))
    first = normalization_only = restarted = None
    current = _acceptance_domain_trade("SELL", 7, permanent_id=1002, status="Submitted")
    try:
        first = OrderLifecycleTracker(store)
        original = _acceptance_domain_trade("SELL", 7, status="Submitted")
        first.on_trade(original)
        assert await asyncio.to_thread(first.flush, timeout=2)
        provisional, = first.snapshot()
        original.order.permId = original.orderStatus.permId = 1002
        first.on_trade(original)
        assert await asyncio.to_thread(first.flush, timeout=2)
        promoted, = first.snapshot()
        assert promoted["identity"] == provisional["identity"]
        assert promoted["permId"] == 1002
        await _close_acceptance_recovery_tracker(first)
        first = None

        # An old journal may contain separate dated provisional checkpoints
        # for the same scope. Normalize the second row through real ingress,
        # then seed that explicit historical duplicate on disk. The current
        # writer is not claimed to produce this ambiguous pair itself.
        observed_at[0] += dt.timedelta(days=1)
        normalization_only = OrderLifecycleTracker()
        other = _acceptance_domain_trade("SELL", 7, status="Submitted")
        normalization_only.on_trade(other)
        duplicate, = normalization_only.snapshot()
        assert duplicate["permId"] == 0
        assert duplicate["identity"] != promoted["identity"]
        assert duplicate["identity"].startswith("TEST-ACCEPTANCE:2026-09-02:")
        assert promoted["identity"].startswith("TEST-ACCEPTANCE:2026-09-01:")
        await _close_acceptance_domain_tracker(normalization_only, None)
        normalization_only = None
        journal = ExecutionJournal(store.duckdb_path)
        try:
            with journal.transaction() as conn:
                conn.execute("INSERT INTO broker_order_progress VALUES (?, ?, ?, ?, ?)",
                             [duplicate["identity"], 0.0, 0.0, None, json.dumps(duplicate)])
        finally:
            journal.close()

        restarted = OrderLifecycleTracker(store)
        rows = restarted.snapshot()
        expected_keys = {provisional["identity"], duplicate["identity"]}
        assert {row["identity"] for row in rows} == expected_keys
        assert {row["permId"] for row in rows} == {0, 1002}
        assert all(row.get("identityAmbiguous") is True for row in rows)
        assert restarted.health["healthy"] is False
        assert restarted.health["replay_required"] is True
        restarted.on_trade(current)
        assert await asyncio.to_thread(restarted.flush, timeout=2)
        assert restarted.latest_status(17, trade=current) is None
        assert await restarted.wait_decisive(17, timeout=0, trade=current) == "timeout"
        assert restarted.mark_replay_complete() is False
        assert {row["identity"] for row in restarted.snapshot()} == expected_keys
        assert all(row.get("identityAmbiguous") is True for row in restarted.snapshot())
        assert store.query_since(dt.datetime(2000, 1, 1), EventType.ORDER_FILLED) == []
    finally:
        await _close_acceptance_recovery_tracker(restarted)
        await _close_acceptance_recovery_tracker(first)
        if normalization_only is not None:
            await _close_acceptance_domain_tracker(normalization_only, None)




@pytest.mark.asyncio
async def test_nonweak_adapter_acknowledgment_cannot_reuse_native_provisional_receipt():
    """A minimal adapter has numeric evidence, not the earlier Trade's proof."""
    from types import SimpleNamespace

    tracker = OrderLifecycleTracker()
    current = _acceptance_domain_trade("SELL", 7)
    try:
        tracker.on_trade(current)
        assert tracker.latest_status(17, trade=current) == "PendingSubmit"

        # SimpleNamespace is the supported minimal, non-weakref adapter shape.
        # Its observation retains native order fields but is a distinct object.
        adapter = SimpleNamespace(
            contract=current.contract,
            order=current.order,
            orderStatus=OrderStatus(orderId=17, clientId=7, permId=0,
                                    status="Submitted", filled=0, remaining=10),
        )
        tracker.on_trade(adapter)
        assert tracker.latest_status(17) == "Submitted"
        assert await tracker.wait_decisive(17, timeout=0) == "accepted"
        assert tracker.latest_status(17, trade=current) is None
        assert await tracker.wait_decisive(17, timeout=0, trade=current) == "timeout"
        row, = tracker.snapshot()
        assert row["status"] == "Submitted" and row["permId"] == 0
        assert row["filled"] == 0 and row["remaining"] == 10
    finally:
        tracker.close(timeout=0)


def test_expired_native_fifo_receipts_preserve_unresolved_numeric_scope(tmp_path):
    """Losing a Python object cannot identify a different provisional receipt."""
    import gc
    import weakref

    from trader.data.event_store import EventStore

    tracker = OrderLifecycleTracker()
    try:
        def stage_native_callbacks():
            confirmed = _acceptance_domain_trade(
                "SELL", 7, permanent_id=1001, status="Submitted")
            provisional = _acceptance_domain_trade("SELL", 7)
            tracker.on_trade(confirmed)
            tracker.on_trade(provisional)
            # A repeated positive receipt proves this permanent order only;
            # it does not identify the separate provisional Trade object.
            tracker.on_trade(confirmed)
            return weakref.ref(confirmed), weakref.ref(provisional)

        confirmed_ref, provisional_ref = stage_native_callbacks()
        gc.collect()
        assert confirmed_ref() is None and provisional_ref() is None
        assert tracker.latest_status(17) is None

        # Exercise the actual pre-attachment FIFO replay, including each
        # observation's now-expired native weakref. No maps/rows are injected.
        tracker.set_event_store(EventStore(str(tmp_path / "weakref-events.duckdb")))
        assert tracker.flush(timeout=5)
        assert tracker.health["healthy"], tracker.health
        assert tracker.latest_status(17) is None

        # The positive physical identity remains usable. Only a bare numeric
        # query is unresolved; no separate process-restart durability is claimed.
        exact_order = _acceptance_domain_trade(
            "SELL", 7, permanent_id=1001, status="Submitted")
        assert tracker.latest_status(17, trade=exact_order) == "Submitted"
        row, = tracker.snapshot()
        assert row["permId"] == 1001 and row["status"] == "Submitted"
        assert row["filled"] == 0 and row["remaining"] == 10
    finally:
        tracker.close(timeout=5 if tracker._journal is not None else 0)
        if tracker._journal is not None:
            tracker._journal.close()


# Unavailable totals cannot turn a partial receipt into final fill proof.
#
# This is a defensive native-shape observation, not a claim that an ordinary
# positive-share broker order reports this field combination. The real send
# reserved forty shares. Its later Trade projection has a missing/default total
# and a lagged zero filled field, while an Execution proves ten shares. No broker
# or service is contacted and no acceptance/snapshot dictionary is fabricated.
import copy
import datetime as dt
import sys
import asyncio

from ib_async import CommissionReport, Execution, Fill, OrderStatus, Trade
import pytest

from review.test_review_order_contract import _coordinated_trader, _stock
from test_server_reduction_reservations import uncertain_stop


@pytest.fixture
def incomplete_total_server(tmp_path):
    server = _coordinated_trader(tmp_path, held=40)
    try:
        yield server
    finally:
        try:
            server.order_tracker.close(timeout=1)
        finally:
            try:
                if server.order_tracker._journal is not None:
                    server.order_tracker._journal.close()
            finally:
                journal = getattr(server, "_server_order_journal", None)
                if journal is not None:
                    journal.journal.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("unavailable_total", [0.0, sys.float_info.max],
                         ids=["default-zero", "native-unset"])
async def test_partial_execution_with_unavailable_total_retains_finality_reservation(
        incomplete_total_server, unavailable_total):
    server = incomplete_total_server
    sent = await uncertain_stop(server)
    original_claim = copy.deepcopy(
        server.server_order_journal().reservations(server.ib_account))

    # Preserve the real reserved physical identity. Only the later observation
    # lacks a meaningful total; the immutable original send remains SELL40.
    order = copy.deepcopy(sent)
    order.clientId, order.permId = 7, 1717
    order.totalQuantity = unavailable_total
    trade = Trade(_stock(), order, OrderStatus(
        orderId=order.orderId, clientId=7, permId=1717,
        status="Filled", filled=0, remaining=0))
    stamp = dt.datetime.now(dt.timezone.utc)
    execution = Execution(
        execId="defensive-partial-10", time=stamp,
        orderId=order.orderId, clientId=7, permId=1717,
        acctNumber=server.ib_account, orderRef=order.orderRef,
        side="SLD", shares=10, cumQty=10, price=10, avgPrice=10)
    fill = Fill(trade.contract, execution,
                CommissionReport(execId=execution.execId), stamp)
    server.order_tracker.on_execution(trade, fill)
    server.inventory = 30  # The acknowledged cumulative execution was ten.
    assert await asyncio.to_thread(server.order_tracker.flush, 1)

    proof = await server.execution_snapshot(intent_id="uncertain-stop")
    row, = proof["orders"]
    assert proof["complete"] and not proof["retry_safe"]
    assert row["clientIntentId"] == "uncertain-stop"
    assert row["orderId"] == sent.orderId and row["permId"] == 1717
    assert row["filled"] == 10 and row["brokerStatus"] == "Filled"
    assert row["status"] == "Unknown" and row["fillQuantityKnown"] is False
    assert await server.unobserved_reduction_quantity(_stock(), "SELL") == 40
    assert server.server_order_journal().reservations(server.ib_account) == original_claim
    assert server.placed == []

    # A later complete forty-share status is independent final-quantity proof.
    # It releases the same reservation; partial evidence must not strand it.
    order.totalQuantity = 40
    trade.orderStatus.filled = 40
    trade.orderStatus.avgFillPrice = 10
    server.inventory = 0
    server.order_tracker.on_trade(trade)
    assert await asyncio.to_thread(server.order_tracker.flush, 1)
    final = await server.execution_snapshot(intent_id="uncertain-stop")
    final_row, = final["orders"]
    assert final["complete"] and final_row["status"] == "Filled"
    assert final_row["filled"] == 40 and final_row["fillQuantityKnown"] is True
    assert await server.unobserved_reduction_quantity(_stock(), "SELL") == 0
    assert server.server_order_journal().reservations(server.ib_account) == []
    assert server.placed == []
