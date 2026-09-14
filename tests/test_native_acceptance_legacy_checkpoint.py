"""An old provisional adapter checkpoint is not a new order's acceptance.

This is the supported minimal-adapter/persisted-history boundary, not a claim
that IB natively reports a final fill with no permanent identity. The old
adapter's missing identity must not become new acceptance proof after restart.
"""
from types import SimpleNamespace

import pytest

from test_native_acceptance_selector_contract import _trade
from trader.data.event_store import EventStore
from trader.trading.order_lifecycle import OrderLifecycleTracker


@pytest.mark.asyncio
async def test_restarted_legacy_provisional_fill_does_not_accept_a_new_positive_order(tmp_path):
    store = EventStore(str(tmp_path / "legacy-acceptance.duckdb"))
    first = restarted = None
    # This historical adapter supplied a valid cumulative quantity but omitted
    # permanent IDs. Its exact observed object cannot survive tracker restart.
    legacy = SimpleNamespace(
        order=SimpleNamespace(orderId=17, clientId=7, account="DU_SELECTOR_FAKE",
                              orderRef="owner|mmr:current", action="SELL",
                              orderType="LMT", totalQuantity=10),
        orderStatus=SimpleNamespace(status="Filled", filled=10, remaining=0,
                                    avgFillPrice=10),
        contract=SimpleNamespace(conId=100, symbol="SELECTOR"),
    )
    try:
        first = OrderLifecycleTracker(store)
        first.on_trade(legacy)
        assert first.flush(timeout=1)
        first.close(timeout=1)
        if first._journal is not None:
            first._journal.close()
        first = None
        restarted = OrderLifecycleTracker(store)
        row, = restarted.snapshot()
        assert row["permId"] == 0 and row["status"] == "Filled" and row["filled"] == 10

        current = _trade(perm_id=1002)
        restarted.on_trade(current)
        # A matching numeric scope does not establish continuity with the old
        # adapter object. PendingSubmit must remain non-decisive for this order.
        assert restarted.latest_status(17, trade=current) == "PendingSubmit"
        assert await restarted.wait_decisive(17, timeout=.01, trade=current) == "timeout"
        current.orderStatus.status = "Submitted"
        restarted.on_trade(current)
        assert await restarted.wait_decisive(17, timeout=.01, trade=current) == "accepted"
        assert restarted.latest_status(17, trade=current) == "Submitted"
    finally:
        if restarted is not None:
            restarted.close(timeout=1)
            if restarted._journal is not None:
                restarted._journal.close()
        if first is not None:
            first.close(timeout=1)
            if first._journal is not None:
                first._journal.close()
