"""A new broker client may reuse a terminal stop's numeric order ID.

The server coordinator, journals, lifecycle projection and execution snapshot
are real. The broker boundary supplies acknowledged Trades with client-scoped
IDs and distinct permanent IDs; it neither opens a socket nor models fills.
"""

import asyncio
import datetime as dt
from types import SimpleNamespace

import reactivex as rx
from ib_async import OrderStatus, Trade

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.strategy.auto_executor import AutoExecutor


OWNER = "client_change_owner"
CONID = 100


def test_new_client_stop_reusing_numeric_id_preserves_separate_fill_checkpoints(
    tmp_path,
):
    trader = _coordinated_trader(tmp_path, held=140)
    broker = trader.client.ib
    # Each sequential connection starts at the same allowed positive ID.
    # The fixture's original sender hardcodes client 7 and matches only the
    # numeric ID, so replace that boundary with the native client-scoped key.
    broker.client.getReqId = lambda: 17

    async def acknowledged_order(contract, order):
        order.clientId = trader.trading_runtime_ib_client_id
        physical_key = (order.clientId, order.orderId)
        assert all((trade.order.clientId, trade.order.orderId) != physical_key
                   for trade in trader.placed)
        order.permId = 1000 + order.clientId
        trade = Trade(contract, order, OrderStatus(
            orderId=order.orderId, clientId=order.clientId, permId=order.permId,
            status="Submitted", filled=0, remaining=order.totalQuantity,
        ))
        trader.placed.append(trade)
        trader.order_tracker.on_trade(trade)
        return rx.of(trade)

    trader.client.subscribe_place_order.side_effect = acknowledged_order
    path = str(tmp_path / "executor.duckdb")
    sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: asyncio.run(
        trader.execution_snapshot(**kwargs)))
    executors = []

    def restart_executor():
        if executors:
            executors[-1].intents.journal.close()
        executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        executors.append(executor)
        return executor

    def place_stop(intent_id, quantity):
        result = asyncio.run(trader.place_standalone_order(
            _stock(), "SELL", quantity, "STP", aux_price=8,
            order_ref=OWNER, client_intent_id=intent_id))
        assert result.is_success(), result.error
        trade = trader.placed[-1]
        claim = trader.server_order_journal().get(intent_id)
        assert claim["orders"] == [{"orderId": 17, "clientId": trade.order.clientId}]
        return trade

    def receipt(trade, filled, status, inventory):
        trade.orderStatus.status = status
        trade.orderStatus.filled = filled
        trade.orderStatus.remaining = trade.order.totalQuantity - filled
        trade.orderStatus.avgFillPrice = 8
        trader.inventory = inventory
        trader.order_tracker.on_trade(trade)
        assert trader.order_tracker.flush(timeout=1)

    try:
        executor = restart_executor()
        entry = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
        executor.state.record_open(OWNER, CONID, 40, entry, 501, None, None)

        old_stop = place_stop("protective:client-seven", 40)
        receipt(old_stop, 10, "Cancelled", 130)
        executor._reconcile_intents(OWNER, CONID)
        old_intent, = executor.intents.all(kind="PROTECTIVE")
        assert old_intent["status"] == "CANCELLED"
        assert old_intent["payload"]["cumulative_filled"] == 10
        assert executor.state.open_position(OWNER, CONID)["quantity"] == 30

        # A client change is sequential. There is still one authoritative
        # Trader, the old physical order is terminal, and its history persists.
        trader.trading_runtime_ib_client_id = 8
        new_stop = place_stop("protective:client-eight", 30)
        receipt(new_stop, 5, "Submitted", 125)
        snapshot = asyncio.run(trader.execution_snapshot())
        assert snapshot["complete"] and snapshot["positions_complete"]
        rows = {row["clientIntentId"]: row for row in snapshot["orders"]}
        assert set(rows) == {"protective:client-seven", "protective:client-eight"}
        assert {(row["clientId"], row["orderId"], row["permId"])
                for row in rows.values()} == {(7, 17, 1007), (8, 17, 1008)}
        assert all(row["fillQuantityKnown"] and not row.get("identityAmbiguous", False)
                   and row.get("brokerIntentCreatedAt") for row in rows.values())

        # Fresh process state must recover both physical stops. Repeating the
        # complete history must apply only five new fills, never the old ten.
        for _ in range(2):
            executor = restart_executor()
            executor._reconcile_intents(OWNER, CONID)
            adopted = executor.intents.all(kind="PROTECTIVE")
            by_reference = {item["payload"]["broker_intent_id"]: item for item in adopted}
            assert set(by_reference) == set(rows)
            assert by_reference["protective:client-seven"]["intent_id"] == old_intent["intent_id"]
            assert len({item["intent_id"] for item in adopted}) == 2
            _positions, checkpoints = executor.state.ownership_snapshot(adopted)
            assert checkpoints == {
                by_reference["protective:client-seven"]["intent_id"]: 10,
                by_reference["protective:client-eight"]["intent_id"]: 5,
            }
            assert executor.state.open_position(OWNER, CONID)["quantity"] == 25

        assert trader.inventory == 125, "the remaining 100 shares belong to a manual holding"
        assert len(trader.placed) == 2, "reconciliation must not create physical orders"
    finally:
        for executor in executors:
            executor.intents.journal.close()
        trader.order_tracker.close(timeout=1)
        journal = getattr(trader, "_server_order_journal", None)
        if journal is not None:
            journal.journal.close()
