"""Completed history without a numeric ID retains unknown execution reservations.

A completedOrder can identify one durable physical leg by permanent ID and
reference while omitting its client order ID and final fill quantity. The
proposal recorder observes the real executor boundary; approval, server claim,
capacity, tracker and snapshots use the actual native implementation.
"""
import asyncio
import copy
import datetime as dt
from types import SimpleNamespace

from ib_async import OrderStatus, Trade
import pytest

from review.test_review_order_contract import _coordinated_trader, _stock
from review.test_review_strategy_contract import FakeResult
from trader.messaging.trader_service_api import TraderServiceApi
from trader.strategy.auto_executor import AutoExecutor


OWNER, CONID = "zero_id_emergency", 100


@pytest.fixture
def native_unknown_emergency(tmp_path, monkeypatch):
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    monkeypatch.delenv("MMR_AUTO_EXECUTE_DISABLED", raising=False)
    trader = _coordinated_trader(tmp_path, held=140)
    managers = []
    try:
        native_place = trader.client.subscribe_place_order.side_effect
        attempts, proposals, approvals = [], [], []
        faults = []
        monkeypatch.setattr("trader.strategy.auto_executor.logging.exception",
                            lambda *args, **kwargs: faults.append(args))

        async def lost_receipt(contract, order):
            attempts.append(copy.deepcopy(order))
            raise ConnectionError("the broker send receipt is unavailable")

        def propose(**kwargs):
            # Record the existing proposal-allocation interface; this fixture does
            # not claim to exercise ProposalStore's FAILED audit transition.
            proposals.append(kwargs)
            return len(proposals), None, None

        def approve(proposal_id, **kwargs):
            approvals.append(proposal_id)
            proposal = proposals[proposal_id - 1]
            result = asyncio.run(trader.place_expressive_order(
                _stock(), proposal["action"], proposal["quantity"],
                {"order_type": "MARKET"}, algo_name=OWNER,
                client_intent_id=proposal["metadata"]["client_intent_id"]))
            ids = []
            if result.is_success():
                for trade in result.obj:
                    quantity = float(trade.order.totalQuantity)
                    trade.order.filledQuantity = quantity
                    trade.orderStatus.status = "Filled"
                    trade.orderStatus.filled = quantity
                    trade.orderStatus.remaining = 0
                    trade.orderStatus.avgFillPrice = 10
                    trader.inventory -= quantity
                    trader.order_tracker.on_trade(trade)
                    ids.append(trade.order.orderId)
            return FakeResult(ok=result.is_success(), obj=ids, error=result.error)

        api = TraderServiceApi(trader)
        sdk = SimpleNamespace(
            execution_snapshot=lambda **kwargs: asyncio.run(trader.execution_snapshot(**kwargs)),
            emergency_close_position=lambda **kwargs: asyncio.run(api.emergency_close_position(**kwargs)),
            resolve=lambda symbol, **kwargs: [_stock()] if symbol in (CONID, "AUDIT") else [],
            propose=propose, approve=approve,
            cancel=lambda order_id: api.cancel_order(order_id),
            _proposal_store=lambda: SimpleNamespace(get=lambda _pid: None),
        )
        path = str(tmp_path / "executor.duckdb")
        executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        managers.append(executor)
        entry = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
        # Existing attributed inventory and its durable ownership epoch are
        # initialized through the production state API; manual100 is separate.
        executor.state.record_open(OWNER, CONID, 40, entry, 501, None, None)
        executor._load_open_view()
        trader.client.subscribe_place_order.side_effect = lost_receipt
        executor._remember_emergency_exit(OWNER, CONID, entry + dt.timedelta(minutes=1),
                                          "retained owned close")
        executor._retry_emergency_exits()
        pending, = executor._emergency_exits.values()
        identity = pending["intent_id"]
        claim = trader.server_order_journal().get(identity)
        assert pending["attempted"] and len(attempts) == 1
        assert claim["status"] == "UNKNOWN" and len(claim["orders"]) == 1
        assert not executor.intents.all(kind="CLOSE")

        # Only completed history is delivered: there was no prior positive-ID
        # tracker receipt to recover that missing ID from. The journal's one
        # exact reserved leg still proves physical association for snapshots.
        order = copy.deepcopy(attempts[0])
        order.orderId = 0
        order.clientId = 7
        order.permId = 1701
        historical = Trade(_stock(), order, OrderStatus(
            orderId=0, clientId=7, permId=1701, status="Cancelled",
            filled=0, remaining=0))
        trader.order_tracker.on_trade(historical, completed=True)
        assert trader.order_tracker.flush(timeout=1)
        proof = asyncio.run(trader.execution_snapshot(intent_id=identity))
        row, = proof["orders"]
        assert proof["complete"] and not proof["retry_safe"]
        assert row["clientIntentId"] == identity and row["orderId"] == 0 and row["permId"] == 1701
        assert row["brokerStatus"] == "Cancelled" and row["status"] == "Unknown"
        assert row["fillQuantityKnown"] is False
        executor._snapshot_cache = None
        executor._retry_emergency_exits()
        restored, = executor.intents.all(kind="CLOSE")
        assert restored["intent_id"] == identity and restored["status"] == "UNKNOWN"
        assert not restored["payload"].get("order_ids") and not restored["payload"].get("proposal_id")
        assert restored["payload"]["ownership_epoch"] == executor.state.open_position(OWNER, CONID)["ownership_epoch"]
        assert not proposals and not approvals and not trader.placed
        yield SimpleNamespace(trader=trader, sdk=sdk, executor=executor, path=path,
                              managers=managers, restored=restored, identity=identity,
                              historical=historical, original_claim=copy.deepcopy(claim),
                              attempts=attempts, proposals=proposals, approvals=approvals,
                              native_place=native_place, faults=faults)
    finally:
        trader.order_tracker.close(timeout=1)
        trader.order_tracker._journal.close()
        journal = getattr(trader, "_server_order_journal", None)
        if journal is not None:
            journal.journal.close()
        for manager in managers:
            manager.intents.journal.close()


@pytest.mark.parametrize("restart_first", [False, True])
def test_native_zero_id_unknown_close_stays_reserved_before_any_new_proposal(
        native_unknown_emergency, restart_first):
    ctx = native_unknown_emergency
    manager = ctx.executor
    if restart_first:
        manager.intents.journal.close()
        manager = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
        ctx.managers.append(manager)
    restored, = manager.intents.all(kind="CLOSE")
    manager._snapshot_cache = None
    manager._advance_close(restored)

    # The durable server claim is an independent boundary: the existing
    # claim's different allow_open fingerprint refuses a new wire send even
    # before the proposed executor fix. Do not call this an oversell proof.
    assert len(ctx.attempts) == 1 and ctx.trader.placed == []
    assert ctx.trader.server_order_journal().get(ctx.identity) == ctx.original_claim
    assert ctx.trader.inventory == 140
    assert manager.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.proposals == [], "unknown physical fill quantity cannot authorize a new proposal"
    assert ctx.approvals == []
    assert ctx.faults == []


def test_exact_native_zero_fill_receipt_releases_a_new_residual_close(native_unknown_emergency):
    ctx = native_unknown_emergency
    ctx.historical.order.filledQuantity = 0
    ctx.trader.order_tracker.on_trade(ctx.historical, completed=True)
    assert ctx.trader.order_tracker.flush(timeout=1)
    ctx.trader.client.subscribe_place_order.side_effect = ctx.native_place
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    ctx.executor.manage_positions()

    old = next(row for row in ctx.executor.intents.all(kind="CLOSE")
               if row["intent_id"] == ctx.identity)
    assert old["status"] == "CANCELLED" and old["payload"]["cumulative_filled"] == 0
    assert len(ctx.proposals) == len(ctx.approvals) == len(ctx.trader.placed) == 1
    proposal, = ctx.proposals
    assert proposal["quantity"] == 40 and proposal["metadata"]["client_intent_id"] != ctx.identity
    assert len(ctx.attempts) == 1
    assert ctx.trader.inventory == 100
    assert ctx.executor.state.open_position(OWNER, CONID) is None
    assert ctx.faults == []
