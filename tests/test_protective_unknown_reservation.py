"""An unobserved native protective send retains its executable reservation.

The real native claim, order coordinator, lifecycle, execution snapshot and
executor run against a deterministic IB adapter. No external service is used.
"""
import asyncio
import copy
import datetime as dt
from types import SimpleNamespace

import pytest
from ib_async.order import OrderStatus, Trade

from review.test_review_order_contract import _coordinated_trader, _stock
from review.test_review_strategy_contract import FakeResult
from trader.messaging.trader_service_api import TraderServiceApi
from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_intents import timestamp_text


OWNER, CONID = "missing_protective_receipt", 100


@pytest.fixture
def unknown_protection(tmp_path, monkeypatch):
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "8")
    trader = _coordinated_trader(tmp_path, held=140)
    native_place = trader.client.subscribe_place_order.side_effect
    attempts, proposals, approvals, faults = [], [], [], []
    monkeypatch.setattr("trader.strategy.auto_executor.logging.exception",
                        lambda *args, **kwargs: faults.append(args))

    async def receipt_unavailable(contract, order):
        # The reservation was committed before this boundary. A transport
        # error here cannot prove whether the broker accepted the order.
        attempts.append(copy.deepcopy(order))
        raise ConnectionError("simulated broker send outcome unavailable")

    trader.client.subscribe_place_order.side_effect = receipt_unavailable

    def propose(**kwargs):
        proposals.append(kwargs)
        return len(proposals), None, None

    def approve(proposal_id):
        approvals.append(proposal_id)
        proposal = proposals[proposal_id - 1]
        result = asyncio.run(trader.place_expressive_order(
            _stock(), proposal["action"], proposal["quantity"],
            {"order_type": "LIMIT", "limit_price": 9.5},
            algo_name=OWNER, client_intent_id=proposal["metadata"]["client_intent_id"]))
        ids = []
        if result.is_success():
            for trade in result.obj:
                # This fake broker executes the acknowledged close exactly;
                # inventory, lifecycle receipt and response all report it.
                quantity = float(trade.order.totalQuantity)
                trade.order.filledQuantity = quantity
                trade.orderStatus.status = "Filled"
                trade.orderStatus.filled = quantity
                trade.orderStatus.remaining = 0
                trade.orderStatus.avgFillPrice = 9.5
                trader.inventory -= quantity
                trader.order_tracker.on_trade(trade)
                ids.append(trade.order.orderId)
        return FakeResult(ok=result.is_success(), obj=ids, error=result.error)

    def protection(**kwargs):
        return asyncio.run(trader.place_standalone_order(
            _stock(), kwargs["action"], kwargs["quantity"], kwargs["order_type"],
            aux_price=kwargs["aux_price"], tif=kwargs["tif"],
            order_ref=kwargs["order_ref"], client_intent_id=kwargs["client_intent_id"]))

    sdk = SimpleNamespace(
        execution_snapshot=lambda **kwargs: asyncio.run(trader.execution_snapshot(**kwargs)),
        resolve=lambda symbol, **kwargs: [_stock()] if symbol in (CONID, "AUDIT") else [],
        place_protective_order=protection, propose=propose, approve=approve,
        cancel=lambda order_id: TraderServiceApi(trader).cancel_order(order_id),
        _proposal_store=lambda: SimpleNamespace(get=lambda _pid: None))
    path = str(tmp_path / "executor.duckdb")
    executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    instances = [executor]
    entry = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
    try:
        # Retain the known historical forty-share fill at 10 per share;
        # this is local prior evidence, not another broker submission.
        opening = executor.intents.create(OWNER, CONID, "OPEN", {
            "bar_ts": timestamp_text(entry), "quantity": 40, "proposal_id": 501},
            status="FILLED")
        assert executor.state.apply_fill(opening, 40, cumulative_quote_notional=400) == 40
        assert executor.state.open_position(OWNER, CONID)["avg_cost"] == 10
        executor._ensure_protective(OWNER, CONID)
        protective, = executor.intents.all(kind="PROTECTIVE")
        claim = trader.server_order_journal().get(protective["intent_id"])
        scoped = asyncio.run(trader.execution_snapshot(intent_id=protective["intent_id"]))
        unscoped = asyncio.run(trader.execution_snapshot())
        assert len(attempts) == 1
        assert protective["status"] == claim["status"] == "UNKNOWN"
        assert not protective["payload"].get("order_ids")
        assert len(claim["orders"]) == 1
        assert not scoped["complete"] and not scoped["retry_safe"] and not scoped["orders"]
        assert unscoped["complete"] and unscoped["positions_complete"]
        trader.client.subscribe_place_order.side_effect = native_place
        yield SimpleNamespace(trader=trader, executor=executor, sdk=sdk, path=path,
                              entry=entry, attempts=attempts, proposals=proposals,
                              approvals=approvals, protective=protective,
                              instances=instances, faults=faults)
    finally:
        trader.order_tracker.close(timeout=1)
        trader.server_order_journal().journal.close()
        for instance in instances:
            instance.intents.journal.close()
        assert not faults, "unexpected executor exception must not be hidden by cheap log capture"


def restart(ctx):
    executor = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
    ctx.instances.append(executor)
    return executor


@pytest.mark.parametrize("restart_first", [False, True])
def test_unobserved_protective_send_blocks_an_independent_close(unknown_protection, restart_first):
    ctx = unknown_protection
    executor = restart(ctx) if restart_first else ctx.executor
    executor._snapshot_cache = None

    executor._execute_close(OWNER, CONID, ctx.entry + dt.timedelta(minutes=1),
                            40, "explicit close while protective outcome is unknown")
    executor.manage_positions()
    restarted = restart(ctx)
    restarted.manage_positions()

    assert ctx.proposals == [], "unknown protective capacity cannot authorize another close"
    assert ctx.approvals == []
    assert ctx.trader.placed == []
    assert len(ctx.attempts) == 1, "the unknown protective send itself is never retried"
    assert ctx.trader.inventory == 140
    assert restarted.state.open_position(OWNER, CONID)["quantity"] == 40
    saved, = restarted.intents.all(kind="PROTECTIVE")
    assert saved["intent_id"] == ctx.protective["intent_id"]
    assert saved["status"] == "UNKNOWN" and not saved["payload"].get("order_ids")
    assert restarted.intents.all(kind="CLOSE", active=True)


def test_management_resumes_once_exact_cancelled_zero_fill_proof_arrives(unknown_protection):
    ctx = unknown_protection
    # Persist the same unsent request that survives a restart before its first
    # management cycle; no physical order identity is fabricated for it.
    ctx.executor.intents.create(OWNER, CONID, "CLOSE", {
        "bar_ts": timestamp_text(ctx.entry + dt.timedelta(minutes=1)),
        "quantity": 40, "reason": "retained explicit close", "exit_request_active": True,
        "explicit_exit_scope": ctx.executor._capture_exit_scope(OWNER, CONID)}, status="WAITING")
    order = copy.deepcopy(ctx.attempts[0])
    order.clientId = 7
    order.permId = 1717
    order.filledQuantity = 0
    terminal = Trade(_stock(), order, OrderStatus(
        orderId=order.orderId, permId=order.permId, status="Cancelled",
        filled=0, remaining=0, avgFillPrice=0))
    ctx.trader.order_tracker.on_trade(terminal, completed=True)
    assert ctx.trader.order_tracker.flush(timeout=1)
    proof = asyncio.run(ctx.trader.execution_snapshot(intent_id=ctx.protective["intent_id"]))
    assert proof["complete"]
    row, = proof["orders"]
    assert row["clientIntentId"] == ctx.protective["intent_id"]
    assert row["status"] == "Cancelled" and row["filled"] == 0 and row["fillQuantityKnown"]

    executor = restart(ctx)
    executor.manage_positions()
    executor.manage_positions()
    restarted = restart(ctx)
    restarted.manage_positions()

    assert len(ctx.proposals) == len(ctx.approvals) == len(ctx.trader.placed) == 1
    assert ctx.proposals[0]["action"] == "SELL" and ctx.proposals[0]["quantity"] == 40
    assert ctx.trader.inventory == 100, "only the independently owned forty shares may close"
    assert restarted.state.open_position(OWNER, CONID) is None
    assert not restarted.intents.all(kind="CLOSE", active=True)
    saved, = restarted.intents.all(kind="PROTECTIVE")
    assert saved["intent_id"] == ctx.protective["intent_id"] and saved["status"] == "CANCELLED"


def test_native_terminal_status_without_final_fill_quantity_keeps_close_pending(unknown_protection):
    ctx = unknown_protection
    order = copy.deepcopy(ctx.attempts[0])
    order.clientId = 7
    order.permId = 1717
    # completedOrder's default zero is not an execution receipt. The original
    # Order.filledQuantity remains IB's unset sentinel, so native tracking
    # exposes Unknown quantity even though cancellation itself is observed.
    terminal = Trade(_stock(), order, OrderStatus(
        orderId=order.orderId, permId=order.permId, status="Cancelled",
        filled=0, remaining=0, avgFillPrice=0))
    ctx.trader.order_tracker.on_trade(terminal, completed=True)
    assert ctx.trader.order_tracker.flush(timeout=1)
    proof = asyncio.run(ctx.trader.execution_snapshot(intent_id=ctx.protective["intent_id"]))
    assert proof["complete"]
    row, = proof["orders"]
    assert row["orderId"] > 0 and row["clientIntentId"] == ctx.protective["intent_id"]
    assert row["brokerStatus"] == "Cancelled" and row["status"] == "Unknown"
    assert row["fillQuantityKnown"] is False
    # This terminal receipt is absent from the live order book. The native
    # cancel API cannot manufacture another terminal quantity observation.
    ctx.trader.book.get_order.return_value = None

    ctx.executor._snapshot_cache = None
    ctx.executor._execute_close(OWNER, CONID, ctx.entry + dt.timedelta(minutes=1),
                                40, "exit awaits actual protective fill quantity")
    recovered = restart(ctx)
    recovered.manage_positions()

    assert ctx.proposals == ctx.approvals == ctx.trader.placed == []
    assert ctx.trader.inventory == 140
    assert recovered.state.open_position(OWNER, CONID)["quantity"] == 40
    saved, = recovered.intents.all(kind="PROTECTIVE")
    assert saved["status"] == "UNKNOWN" and saved["payload"]["order_ids"] == [order.orderId]
    assert recovered.intents.all(kind="CLOSE", active=True)


def test_legacy_zero_order_id_is_not_a_resolved_protective_reservation(unknown_protection):
    ctx = unknown_protection
    # Historical metadata can contain a placeholder numeric ID. This is a
    # compatibility control, not a claim that the current writer emits zero.
    ctx.executor.intents.update(ctx.protective, order_ids=[0])

    ctx.executor._snapshot_cache = None
    ctx.executor._execute_close(OWNER, CONID, ctx.entry + dt.timedelta(minutes=1),
                                40, "exit awaits a usable protective identity")
    recovered = restart(ctx)
    recovered.manage_positions()

    assert ctx.proposals == ctx.approvals == ctx.trader.placed == []
    assert ctx.trader.inventory == 140
    assert recovered.state.open_position(OWNER, CONID)["quantity"] == 40
    assert recovered.intents.all(kind="PROTECTIVE", active=True)
    assert recovered.intents.all(kind="CLOSE", active=True)
