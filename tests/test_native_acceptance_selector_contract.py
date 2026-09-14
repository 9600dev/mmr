"""Native acceptance follows the physical order identity.

Positive-ID history may be reused across clients or broker sessions. A status
belongs to the actual Trade/physical identity; it is not an account-wide status
for a bare integer. These are receipt and caller controls, not transport tests.
"""
import asyncio

from ib_async import Order, OrderStatus, Stock, Trade
import pytest

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.trading.order_lifecycle import OrderLifecycleTracker


def _trade(*, status="PendingSubmit", perm_id=0, client_id=7,
           reference="owner|mmr:current", account="DU_SELECTOR_FAKE",
           conid=100, action="SELL", order_id=17):
    return Trade(
        Stock("SELECTOR", "SMART", "USD", conId=conid),
        Order(orderId=order_id, clientId=client_id, permId=perm_id,
              account=account, orderRef=reference, action=action,
              orderType="LMT", totalQuantity=10),
        OrderStatus(orderId=order_id, clientId=client_id, permId=perm_id,
                    status=status, filled=0, remaining=10),
    )


def _completed_zero(tracker, trade):
    # Explicit completedOrder quantity, not its default status.filled=0.
    trade.order.filledQuantity = 0
    tracker.on_trade(trade, completed=True)


async def _cleanup_waiter(waiter):
    if waiter is not None and not waiter.done():
        waiter.cancel()
    if waiter is not None:
        await asyncio.gather(waiter, return_exceptions=True)


@pytest.mark.asyncio
async def test_matching_native_rejection_still_cancels_the_returned_trade(tmp_path):
    trader = _coordinated_trader(tmp_path, held=100)
    acknowledged = trader.client.subscribe_place_order.side_effect

    async def reject_current(contract, order):
        observation = await acknowledged(contract, order)
        current = trader.placed[-1]
        current.orderStatus.status = "Inactive"
        trader.order_tracker.on_trade(current)
        return observation

    trader.client.subscribe_place_order.side_effect = reject_current
    try:
        result = await trader.place_expressive_order(
            _stock(), "SELL", 10, {"order_type": "MARKET"},
            algo_name="current-owner", client_intent_id="matching-rejection")
        assert not result.is_success()
        current, = trader.placed
        trader.client.ib.cancelOrder.assert_called_once_with(current.order)
        assert current.orderStatus.status == "Cancelled"
        assert trader.inventory == 100
    finally:
        trader.order_tracker.close(timeout=1)
        tracker_journal = getattr(trader.order_tracker, "_journal", None)
        if tracker_journal is not None:
            tracker_journal.close()
        journal = getattr(trader, "_server_order_journal", None)
        if journal is not None:
            journal.journal.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("other_client", [7, 99])
async def test_foreign_rejection_waits_for_the_actual_current_ack(other_client):
    tracker = OrderLifecycleTracker()
    current = _trade()
    waiter = None
    try:
        tracker.on_trade(current)
        waiter = asyncio.create_task(tracker.wait_decisive(17, timeout=1, trade=current))
        await asyncio.sleep(0)  # register the consumer before either later receipt
        old = _trade(status="Cancelled", perm_id=9001, client_id=other_client,
                     reference="other-owner|mmr:history")
        _completed_zero(tracker, old)
        await asyncio.sleep(0)
        assert not waiter.done(), "unrelated completed history is not this order's rejection"
        current.orderStatus.status = "Submitted"
        tracker.on_trade(current)
        assert await waiter == "accepted"
        assert tracker.latest_status(17, trade=current) == "Submitted"
        assert tracker.latest_status(17) is None, "a bare number now names two physical histories"
    finally:
        await _cleanup_waiter(waiter)
        tracker.close(timeout=0)


@pytest.mark.asyncio
async def test_provisional_wait_follows_its_actual_trade_permanent_id_promotion():
    tracker = OrderLifecycleTracker()
    current = _trade()
    waiter = None
    try:
        tracker.on_trade(current)
        waiter = asyncio.create_task(tracker.wait_decisive(17, timeout=1, trade=current))
        await asyncio.sleep(0)
        current.order.permId = current.orderStatus.permId = 1001
        current.orderStatus.status = "PreSubmitted"
        tracker.on_trade(current)
        assert await waiter == "accepted"
        row, = tracker.snapshot()
        assert row["permId"] == 1001 and row["status"] == "PreSubmitted"
    finally:
        await _cleanup_waiter(waiter)
        tracker.close(timeout=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["Cancelled", "ApiCancelled", "Inactive"])
async def test_matching_physical_rejection_resolves_a_registered_waiter(status):
    tracker = OrderLifecycleTracker()
    current = _trade(perm_id=1001)
    waiter = None
    try:
        tracker.on_trade(current)
        waiter = asyncio.create_task(tracker.wait_decisive(17, timeout=1, trade=current))
        await asyncio.sleep(0)
        current.orderStatus.status = status
        tracker.on_trade(current)
        assert await waiter == "rejected"
        assert tracker.latest_status(17, trade=current) == status
    finally:
        await _cleanup_waiter(waiter)
        tracker.close(timeout=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("old_permanent", [0, 1001])
async def test_unobserved_provisional_trade_cannot_borrow_reused_scope_rejection(old_permanent):
    tracker = OrderLifecycleTracker()
    old = _trade(status="Cancelled", perm_id=old_permanent)
    current = _trade()  # same complete scope, but no receipt for this Trade
    try:
        _completed_zero(tracker, old)
        assert tracker.latest_status(17, trade=current) is None
        assert await tracker.wait_decisive(17, timeout=.01, trade=current) == "timeout"
        assert tracker.latest_status(17, trade=old) == "Cancelled"
    finally:
        tracker.close(timeout=0)


@pytest.mark.asyncio
async def test_nonzero_permanent_ids_separate_same_client_number_and_reference_reuse():
    tracker = OrderLifecycleTracker()
    old = _trade(status="Cancelled", perm_id=1001)
    current = _trade(status="Submitted", perm_id=1002)
    try:
        tracker.on_trade(current)
        _completed_zero(tracker, old)
        assert await tracker.wait_decisive(17, timeout=.01, trade=current) == "accepted"
        assert await tracker.wait_decisive(17, timeout=.01, trade=old) == "rejected"
        assert tracker.latest_status(17) is None
        assert await tracker.wait_decisive(17, timeout=.01) == "timeout"
        assert {row["permId"] for row in tracker.snapshot()} == {1001, 1002}
    finally:
        tracker.close(timeout=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("different", [
    {"account": "DU_OTHER_FAKE"},
    {"client_id": 99},
    {"reference": "other-owner|mmr:other"},
    {"conid": 202},
    {"action": "BUY"},
])
async def test_a_permanent_id_does_not_override_contradictory_caller_scope(different):
    tracker = OrderLifecycleTracker()
    observed = _trade(status="Cancelled", perm_id=1001)
    other = _trade(perm_id=1001, **different)
    try:
        _completed_zero(tracker, observed)
        # Deliberately conflicting selector input is not a second broker row.
        # No claim is made that two native orders share a permanent identity.
        assert tracker.latest_status(17, trade=other) is None
        assert await tracker.wait_decisive(17, timeout=.01, trade=other) == "timeout"
    finally:
        tracker.close(timeout=0)


@pytest.mark.asyncio
async def test_contradictory_returned_permanent_ids_remain_unknown():
    tracker = OrderLifecycleTracker()
    current = _trade(status="Cancelled", perm_id=1001)
    try:
        _completed_zero(tracker, current)
        current.orderStatus.permId = 1002
        assert tracker.latest_status(17, trade=current) is None
        assert await tracker.wait_decisive(17, timeout=.01, trade=current) == "timeout"
    finally:
        tracker.close(timeout=0)


@pytest.mark.asyncio
async def test_legacy_numeric_status_is_available_only_for_one_physical_history():
    tracker = OrderLifecycleTracker()
    first = _trade(status="Submitted", perm_id=1001)
    try:
        tracker.on_trade(first)
        assert tracker.latest_status(17) == "Submitted"
        assert await tracker.wait_decisive(17, timeout=.01) == "accepted"
        other = _trade(status="Cancelled", perm_id=9001, client_id=99)
        _completed_zero(tracker, other)
        assert tracker.latest_status(17) is None
        assert await tracker.wait_decisive(17, timeout=.01) == "timeout"
    finally:
        tracker.close(timeout=0)


@pytest.mark.asyncio
async def test_completed_zero_order_id_never_answers_a_legacy_numeric_wait():
    tracker = OrderLifecycleTracker()
    historical = _trade(status="Cancelled", perm_id=1001, order_id=0)
    try:
        _completed_zero(tracker, historical)
        assert tracker.latest_status(0) is None
        assert await tracker.wait_decisive(0, timeout=.01) == "timeout"
        row, = tracker.snapshot()
        assert row["permId"] == 1001 and row["status"] == "Cancelled"
    finally:
        tracker.close(timeout=0)
