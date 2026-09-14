"""Startup and reused numeric scopes require trusted acceptance receipts."""
import asyncio

import pytest

from test_native_acceptance_selector_contract import _trade, _cleanup_waiter
from trader.data.event_store import EventStore
from trader.trading.order_lifecycle import OrderLifecycleTracker


def test_pre_attach_provisional_acceptance_keeps_its_actual_receipt_after_fifo_rebind(tmp_path):
    tracker = OrderLifecycleTracker()
    current = _trade(status="Submitted")
    try:
        tracker.on_trade(current)  # receipt exists before a journal is attached
        assert tracker.latest_status(17, trade=current) == "Submitted"
        tracker.set_event_store(EventStore(str(tmp_path / "events.duckdb")))
        assert tracker.flush(timeout=1)
        # No second callback is supplied: attaching persistence must preserve
        # already-observed acceptance for this actual provisional Trade.
        assert tracker.latest_status(17, trade=current) == "Submitted"
        assert asyncio.run(tracker.wait_decisive(17, timeout=.01, trade=current)) == "accepted"
        row, = tracker.snapshot()
        assert row["status"] == "Submitted" and row["permId"] == 0
    finally:
        tracker.close(timeout=1)
        if tracker._journal is not None:
            tracker._journal.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("history_first", [True, False])
async def test_new_provisional_object_cannot_borrow_old_same_scope_filled_status(history_first):
    tracker = OrderLifecycleTracker()
    old = _trade(status="Filled", perm_id=1001)
    old.order.filledQuantity = 10
    old.orderStatus.filled = 10
    old.orderStatus.remaining = 0
    old.orderStatus.avgFillPrice = 10
    current = _trade()
    waiter = None
    try:
        if history_first:
            tracker.on_trade(old, completed=True)
        tracker.on_trade(current)
        if not history_first:
            # Completed history can arrive after a provisional placement
            # receipt without proving these same-scope objects are one order.
            tracker.on_trade(old, completed=True)
        waiter = asyncio.create_task(tracker.wait_decisive(17, timeout=1, trade=current))
        await asyncio.sleep(0)
        assert tracker.latest_status(17, trade=current) is None
        assert tracker.latest_status(17, trade=old) == "Filled"
        assert tracker.latest_status(17) is None
        current.orderStatus.status = "Submitted"
        tracker.on_trade(current)
        await asyncio.sleep(0)
        assert not waiter.done(), "the provisional object still lacks permanent disambiguation"
        assert tracker.latest_status(17, trade=current) is None
        assert tracker.latest_status(17, trade=old) == "Filled"
        current.order.permId = current.orderStatus.permId = 1002
        tracker.on_trade(current)
        assert await waiter == "accepted"
        assert tracker.latest_status(17, trade=current) == "Submitted"
        assert tracker.latest_status(17, trade=old) == "Filled"
        assert tracker.latest_status(17) is None
        assert {row["permId"] for row in tracker.snapshot()} == {1001, 1002}
    finally:
        await _cleanup_waiter(waiter)
        tracker.close(timeout=0)
