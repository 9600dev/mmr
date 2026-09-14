"""Keep unknown broker facts honest and wait for decisive status evidence."""

import asyncio

import pytest

from test_order_lifecycle_contract_gaps import _fill, _send, _trade, _without
from trader.trading.order_lifecycle import OrderLifecycleTracker


def test_native_empty_order_type_remains_unknown():
    tracker = OrderLifecycleTracker()
    try:
        trade = _trade()
        trade.order.orderType = ""
        _send(tracker, trade, flush=False)
        row, = tracker.snapshot()
        assert row["orderType"] == ""
        assert row["orderId"] == trade.order.orderId
        assert tracker.health["healthy"] is True
    finally:
        tracker.close(timeout=0.0)


@pytest.mark.parametrize("component,field,expected", [
    ("order", "orderType", ""),
    ("contract", "symbol", ""),
    ("order", "clientId", 0),
])
def test_minimal_adapter_omitted_metadata_is_not_invented(component, field, expected):
    # These are the same supported minimal-adapter inputs used by the existing
    # legacy identity controls. Native ib_async dataclasses declare the fields.
    tracker = OrderLifecycleTracker()
    try:
        trade = _trade()
        setattr(trade, component, _without(getattr(trade, component), field))
        _send(tracker, trade, flush=False)
        row, = tracker.snapshot()
        assert row[field] == expected
        assert row["orderId"] == 17
        assert row["permId"] == 1717
        assert tracker.health["healthy"] is True
    finally:
        tracker.close(timeout=0.0)


def test_minimal_execution_without_time_keeps_its_confirmed_receipt():
    tracker = OrderLifecycleTracker()
    try:
        fill = _fill()
        fill.execution = _without(fill.execution, "time")
        _send(tracker, _trade(), fill, flush=False)
        row, = tracker.snapshot()
        assert row["filled"] == 1.0
        assert row["execution"]["execId"] == "receipt-A"
        assert row["execution"]["time"] is None
        assert tracker.health["healthy"] is True
    finally:
        tracker.close(timeout=0.0)


def test_pending_submit_does_not_finish_a_positive_id_decisive_waiter():
    tracker = OrderLifecycleTracker()

    async def scenario():
        # These finite timeouts bound a broken test; no elapsed-time or exact
        # response deadline is part of the asserted status contract.
        waiter = asyncio.create_task(tracker.wait_decisive(17, timeout=2.0))
        try:
            await asyncio.sleep(0)  # register the real waiter on this loop
            tracker.on_trade(_trade(status="PendingSubmit"))
            await asyncio.sleep(0)  # allow an erroneous early result to wake it
            assert not waiter.done()
            tracker.on_trade(_trade(status="Submitted"))
            assert await asyncio.wait_for(waiter, timeout=1.0) == "accepted"
        finally:
            if not waiter.done():
                waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)

    try:
        asyncio.run(scenario())
    finally:
        # No event store is attached, so this callback/waiter test starts no
        # persistence threads and has no pending worker join to wait on.
        tracker.close(timeout=0.0)
