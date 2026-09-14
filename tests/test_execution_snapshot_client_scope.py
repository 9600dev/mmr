"""Numeric cancellation probes use the current broker client's ID namespace.

Native lifecycle projection and runtime snapshots are real. Broker I/O is
the existing isolated fixture; completed-order receipts are supplied directly.
"""

import asyncio
from types import SimpleNamespace

from ib_async import Order, OrderStatus, Stock, Trade

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.strategy.auto_executor import AutoExecutor


def test_old_client_unknown_history_cannot_hold_current_client_cancel_pending(tmp_path):
    trader = _coordinated_trader(tmp_path, held=100)
    trader.trading_runtime_ib_client_id = 8
    sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: asyncio.run(
        trader.execution_snapshot(**kwargs)))
    executor = AutoExecutor(str(tmp_path / "executor.duckdb"),
                            paper_trading=True, sdk_factory=lambda: sdk)

    try:
        old_order = Order(orderId=17, clientId=7, permId=7017,
                          account=trader.ib_account, action="SELL", orderType="STP",
                          totalQuantity=10, orderRef="other-owner|mmr:old-client-stop")
        old = Trade(Stock("OTHER", "SMART", "USD", conId=202), old_order,
                    OrderStatus(orderId=17, clientId=7, permId=7017,
                                status="Submitted", filled=0, remaining=10))
        trader.order_tracker.on_trade(old)
        # completedOrder's default status.filled=0 is not a final-quantity
        # receipt. Keep this other instrument's honest UNKNOWN history.
        old.orderStatus.status = "Cancelled"
        trader.order_tracker.on_trade(old, completed=True)
        assert trader.order_tracker.flush(timeout=1)
        historical, = trader.order_tracker.snapshot()
        assert historical["status"] == "Unknown"
        assert historical["brokerStatus"] == "Cancelled"
        assert historical["fillQuantityKnown"] is False

        missing = asyncio.run(trader.execution_snapshot(order_ids=[17]))
        assert missing["complete"] is False
        assert executor._order_is_terminal(17) is False

        current_order = Order(orderId=17, clientId=8, permId=8017,
                              account=trader.ib_account, action="SELL", orderType="STP",
                              totalQuantity=40, orderRef="current-owner|mmr:current-client-stop")
        current = Trade(_stock(), current_order, OrderStatus(
            orderId=17, clientId=8, permId=8017, status="Cancelled", filled=0, remaining=40))
        # This is a real orderStatus-shaped zero-fill confirmation, not the
        # separate completedOrder default-zero case above.
        trader.order_tracker.on_trade(current)
        assert trader.order_tracker.flush(timeout=1)

        global_snapshot = asyncio.run(trader.execution_snapshot())
        assert global_snapshot["complete"]
        assert {(row["clientId"], row["orderId"], row["conId"])
                for row in global_snapshot["orders"]} == {(7, 17, 202), (8, 17, 100)}

        old_scoped = asyncio.run(trader.execution_snapshot(intent_id="old-client-stop"))
        old_row, = old_scoped["orders"]
        assert (old_row["clientId"], old_row["orderId"], old_row["conId"]) == (7, 17, 202)
        assert old_row["status"] == "Unknown"

        scoped = asyncio.run(trader.execution_snapshot(order_ids=[17]))
        assert scoped["complete"]
        row, = scoped["orders"]
        assert (row["clientId"], row["orderId"], row["conId"]) == (8, 17, 100)
        assert row["status"] == "Cancelled" and row["fillQuantityKnown"] is True
        assert executor._order_is_terminal(17) is True

        # The global history remains intact after the scoped cancellation
        # check, including the unrelated order's unresolved final quantity.
        retained = {row["clientId"]: row for row in trader.order_tracker.snapshot()}
        assert retained[7]["status"] == "Unknown"
        assert retained[7]["fillQuantityKnown"] is False
        assert trader.placed == []
        trader.client.ib.cancelOrder.assert_not_called()
    finally:
        executor.intents.journal.close()
        trader.order_tracker.close(timeout=1)
        journal = getattr(trader, "_server_order_journal", None)
        if journal is not None:
            journal.journal.close()
