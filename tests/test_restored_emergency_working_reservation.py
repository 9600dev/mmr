"""Native recovery must settle a prior fill before sizing another reduction.

Only the IB send/receipt transport and proposal-allocation interface are fake.
The executor, local state, server claims, coordinator, tracker and snapshots
are real. The two cases retain versus lose a process-local emergency claim.
Recovery must account for the earlier fill before selling the owned residual.
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


OWNER, CONID = "restored_working_close", 100


@pytest.mark.parametrize("restart_trader", [False, True],
                         ids=["retained_volatile_claim", "lost_volatile_claim"])
def test_scoped_reconciliation_failure_cannot_resubmit_a_recovered_emergency(
        tmp_path, monkeypatch, restart_trader, caplog):
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    monkeypatch.delenv("MMR_AUTO_EXECUTE_DISABLED", raising=False)
    servers, closed_servers = [], set()
    executor = None

    def stop_server(server):
        if id(server) in closed_servers:
            return
        server.order_tracker.close(timeout=1)
        if server.order_tracker._journal is not None:
            server.order_tracker._journal.close()
        journal = getattr(server, "_server_order_journal", None)
        if journal is not None:
            journal.journal.close()
        if server.order_tracker._temporary is not None:
            server.order_tracker._temporary.cleanup()
        closed_servers.add(id(server))

    try:
        server = _coordinated_trader(tmp_path, held=140)
        servers.append(server)
        attempts, proposals, approvals, scoped_failures = [], [], [], []
        scoped_transport_down = False
        # Expected transport errors need no expensive diagnostic rendering.
        monkeypatch.setattr("trader.strategy.auto_executor.logging.exception",
                            lambda *args, **kwargs: None)

        def snapshot(**kwargs):
            if scoped_transport_down and kwargs.get("intent_id"):
                scoped_failures.append(dict(kwargs))
                raise ConnectionError("intent-scoped RPC response unavailable")
            return asyncio.run(server.execution_snapshot(**kwargs))

        def propose(**kwargs):
            # Preserve the actual metadata/client-intent boundary without
            # claiming to exercise ProposalStore's audit state machine.
            proposals.append(copy.deepcopy(kwargs))
            return len(proposals), None, None

        def approve(proposal_id, **kwargs):
            proposal = proposals[proposal_id - 1]
            result = asyncio.run(server.place_expressive_order(
                _stock(), proposal["action"], proposal["quantity"],
                {"order_type": "MARKET"}, algo_name=OWNER,
                client_intent_id=proposal["metadata"]["client_intent_id"]))
            approvals.append((result.is_success(), result.error))
            ids = []
            if result.is_success():
                # The fake broker fills only Trades actually returned by the
                # native approval/coordinator path, never a guessed order.
                for trade in result.obj:
                    quantity = float(trade.order.totalQuantity)
                    trade.order.filledQuantity = quantity
                    trade.orderStatus.status = "Filled"
                    trade.orderStatus.filled = quantity
                    trade.orderStatus.remaining = 0
                    trade.orderStatus.avgFillPrice = 10
                    server.inventory -= quantity
                    server.order_tracker.on_trade(trade)
                    ids.append(trade.order.orderId)
            return FakeResult(ok=result.is_success(), obj=ids, error=result.error)

        sdk = SimpleNamespace(
            execution_snapshot=snapshot,
            emergency_close_position=lambda **kwargs: asyncio.run(
                TraderServiceApi(server).emergency_close_position(**kwargs)),
            resolve=lambda symbol, **kwargs: [_stock()] if symbol in (CONID, "AUDIT") else [],
            propose=propose, approve=approve,
            cancel=lambda order_id: TraderServiceApi(server).cancel_order(order_id),
            _proposal_store=lambda: SimpleNamespace(get=lambda _pid: None),
        )
        executor = AutoExecutor(str(tmp_path / "executor.duckdb"),
                                paper_trading=True, sdk_factory=lambda: sdk)
        entry = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
        executor.state.record_open(OWNER, CONID, 40, entry, 501, None, None)
        executor._load_open_view()
        executor._remember_emergency_exit(
            OWNER, CONID, entry + dt.timedelta(minutes=1), "close the attributed holding")

        # The running process proves its complete prior-reservation census
        # while storage is healthy. An empty uninitialized cache is not proof.
        assert asyncio.run(server.unobserved_reduction_quantity(_stock(), "SELL")) == 0
        durable = server.server_order_journal()
        native_send = server.client.subscribe_place_order.side_effect

        async def lost_acknowledgement(contract, order):
            attempts.append(copy.deepcopy(order))
            raise ConnectionError("send may have reached IB; acknowledgement was lost")

        def unavailable_journal():
            raise OSError("server claim journal temporarily unavailable")

        with monkeypatch.context() as outage:
            outage.setattr(server, "server_order_journal", unavailable_journal)
            server.client.subscribe_place_order.side_effect = lost_acknowledgement
            executor._retry_emergency_exits()
        server.client.subscribe_place_order.side_effect = native_send
        emergency, = executor._emergency_exits.values()
        identity = emergency["intent_id"]
        assert emergency["attempted"] and len(attempts) == 1
        old_claim = copy.deepcopy(server._emergency_order_journal.get(identity))
        assert old_claim["status"] == "UNKNOWN" and len(old_claim["orders"]) == 1
        assert durable.get(identity) is None
        assert executor.intents.all(kind="CLOSE") == []

        if restart_trader:
            # This is an actual new Trader/journal/tracker instance. Only the
            # durable database is reused; volatile claims are not copied.
            stop_server(server)
            server = _coordinated_trader(tmp_path, held=110)
            servers.append(server)
            sequence = iter(range(101, 1000))
            server.client.ib.client.getReqId = lambda: next(sequence)
            assert getattr(server, "_emergency_order_journal", None) is None
            assert server.server_order_journal().get(identity) is None
        else:
            server.inventory = 110

        # IB completedOrder may omit the old numeric ID but retain permanent
        # identity, account, reference and final cumulative fill. There was
        # no positive-ID tracker callback before the lost acknowledgement.
        order = copy.deepcopy(attempts[0])
        order.orderId, order.clientId, order.permId = 0, 0, 1701
        order.filledQuantity = 30
        # Native Wrapper.completedOrder leaves status quantities at defaults;
        # Order.filledQuantity supplies the authoritative completed amount.
        historical = Trade(_stock(), order, OrderStatus(orderId=0, status="Cancelled"))
        server.order_tracker.on_trade(historical, completed=True)
        assert server.order_tracker.flush(timeout=1)
        global_proof = asyncio.run(server.execution_snapshot())
        row, = global_proof["orders"]
        assert global_proof["complete"] and global_proof["positions_complete"]
        assert (row["orderId"], row["permId"], row["clientIntentId"]) == (0, 1701, identity)
        assert row["status"] == "Cancelled" and row["fillQuantityKnown"] is True
        assert row["filled"] == 30 and server.inventory == 110
        assert executor.state.open_position(OWNER, CONID)["quantity"] == 40

        if not restart_trader:
            # Independent server guard: the retained volatile claim refuses
            # the ordinary approval route's same-ID/different fingerprint.
            refused = asyncio.run(server.place_expressive_order(
                _stock(), "SELL", 40, {"order_type": "MARKET"},
                algo_name=OWNER, client_intent_id=identity))
            assert not refused.is_success() and server.placed == []
            assert server._emergency_order_journal.get(identity) == old_claim

        scoped_transport_down = True
        executor._snapshot_cache = None
        executor._retry_emergency_exits()
        restored, = executor.intents.all(kind="CLOSE")
        assert restored["intent_id"] == identity and restored["status"] == "WORKING"
        assert restored["payload"]["emergency"] is True
        assert restored["payload"]["order_ids"] == []
        assert "cumulative_filled" not in restored["payload"]
        assert "proposal_id" not in restored["payload"]
        assert restored["payload"]["ownership_epoch"] == executor.state.open_position(OWNER, CONID)["ownership_epoch"]
        assert scoped_failures and proposals == []

        caplog.clear()
        with caplog.at_level("WARNING", logger="auto_executor"):
            executor._advance_close(restored)
        retry_warnings = [record.getMessage() for record in caplog.records
                          if record.name == "auto_executor"
                          and "retry proof" in record.getMessage().lower()]
        assert any(identity in message and "intent-scoped RPC response unavailable" in message
                   for message in retry_warnings)

        # A global broker balance of110 includes manual100. The old30-share
        # receipt must be attributed before local40 can size another SELL.
        sent_quantities = [float(trade.order.totalQuantity) for trade in server.placed]
        assert sent_quantities == [], (
            f"unreconciled prior fill30 produced native SELLs {sent_quantities}; "
            f"broker inventory became {server.inventory}, approvals={approvals}")
        assert server.inventory == 110
        assert len(attempts) == 1
        assert all(not accepted for accepted, _error in approvals)
        if not restart_trader:
            assert server._emergency_order_journal.get(identity) == old_claim
        assert proposals == approvals == []
        assert restored["status"] == "WORKING"
        assert executor.state.open_position(OWNER, CONID)["quantity"] == 40

        # Once exact history becomes readable, management attributes the old
        # partial fill and closes only the residual under a new physical intent.
        scoped_transport_down = False
        executor.manage_positions()
        old = next(row for row in executor.intents.all(kind="CLOSE")
                   if row["intent_id"] == identity)
        assert old["status"] == "CANCELLED" and old["payload"]["cumulative_filled"] == 30
        assert len(proposals) == len(approvals) == len(server.placed) == 1
        assert proposals[0]["quantity"] == 10
        assert proposals[0]["metadata"]["client_intent_id"] != identity
        assert approvals[0][0] is True
        assert float(server.placed[0].order.totalQuantity) == 10
        assert server.inventory == 100
        assert executor.state.open_position(OWNER, CONID) is None
        assert not executor._emergency_exits
        assert len(attempts) == 1
    finally:
        if executor is not None:
            executor.intents.journal.close()
        for existing in servers:
            stop_server(existing)


def test_completed_zero_id_emergency_keeps_its_identity_when_scoped_reads_recover(
        tmp_path, monkeypatch):
    """A completed native attempt hands residual demand to a fresh identity."""
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    monkeypatch.delenv("MMR_AUTO_EXECUTE_DISABLED", raising=False)
    server = executor = None
    try:
        server = _coordinated_trader(tmp_path, held=140)
        attempts, proposals, approvals = [], [], []
        failed_scopes = []
        scoped_failures_remaining = 0
        monkeypatch.setattr("trader.strategy.auto_executor.logging.exception",
                            lambda *args, **kwargs: None)

        def snapshot(**kwargs):
            nonlocal scoped_failures_remaining
            if kwargs.get("intent_id") and scoped_failures_remaining:
                scoped_failures_remaining -= 1
                failed_scopes.append(dict(kwargs))
                raise ConnectionError("earlier scoped response unavailable")
            return asyncio.run(server.execution_snapshot(**kwargs))

        def propose(**kwargs):
            proposals.append(copy.deepcopy(kwargs))
            return len(proposals), None, None

        def approve(proposal_id, **kwargs):
            proposal = proposals[proposal_id - 1]
            result = asyncio.run(server.place_expressive_order(
                _stock(), proposal["action"], proposal["quantity"],
                {"order_type": "MARKET"}, algo_name=OWNER,
                client_intent_id=proposal["metadata"]["client_intent_id"]))
            approvals.append((result.is_success(), result.error))
            ids = []
            if result.is_success():
                for trade in result.obj:
                    quantity = float(trade.order.totalQuantity)
                    trade.order.filledQuantity = quantity
                    trade.orderStatus.status = "Filled"
                    trade.orderStatus.filled = quantity
                    trade.orderStatus.remaining = 0
                    trade.orderStatus.avgFillPrice = 10
                    server.inventory -= quantity
                    server.order_tracker.on_trade(trade)
                    ids.append(trade.order.orderId)
            return FakeResult(ok=result.is_success(), obj=ids, error=result.error)

        sdk = SimpleNamespace(
            execution_snapshot=snapshot,
            emergency_close_position=lambda **kwargs: asyncio.run(
                TraderServiceApi(server).emergency_close_position(**kwargs)),
            resolve=lambda symbol, **kwargs: [_stock()] if symbol in (CONID, "AUDIT") else [],
            propose=propose, approve=approve,
            cancel=lambda order_id: TraderServiceApi(server).cancel_order(order_id),
            _proposal_store=lambda: SimpleNamespace(get=lambda _pid: None),
        )
        executor = AutoExecutor(str(tmp_path / "executor.duckdb"),
                                paper_trading=True, sdk_factory=lambda: sdk)
        entry = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
        executor.state.record_open(OWNER, CONID, 40, entry, 501, None, None)
        executor._load_open_view()
        executor._remember_emergency_exit(
            OWNER, CONID, entry + dt.timedelta(minutes=1), "close the attributed holding")
        assert asyncio.run(server.unobserved_reduction_quantity(_stock(), "SELL")) == 0
        durable = server.server_order_journal()
        native_send = server.client.subscribe_place_order.side_effect

        async def lose_acknowledgement(contract, order):
            attempts.append(copy.deepcopy(order))
            raise ConnectionError("native send acknowledgement unavailable")

        def unavailable_claim_journal():
            raise OSError("server claim journal temporarily unavailable")

        with monkeypatch.context() as outage:
            outage.setattr(server, "server_order_journal", unavailable_claim_journal)
            server.client.subscribe_place_order.side_effect = lose_acknowledgement
            executor._retry_emergency_exits()
        server.client.subscribe_place_order.side_effect = native_send
        emergency, = executor._emergency_exits.values()
        identity = emergency["intent_id"]
        old_claim = copy.deepcopy(server._emergency_order_journal.get(identity))
        assert emergency["attempted"] and len(attempts) == 1
        assert old_claim["status"] == "UNKNOWN" and len(old_claim["orders"]) == 1
        assert durable.get(identity) is None
        assert executor.intents.all(kind="CLOSE") == []

        # Native completedOrder preserves reference/permanent identity but
        # has no numeric cancellation ID or status-quantity projection.
        order = copy.deepcopy(attempts[0])
        order.orderId, order.clientId, order.permId = 0, 0, 1701
        order.filledQuantity = 30
        server.inventory = 110
        server.order_tracker.on_trade(
            Trade(_stock(), order, OrderStatus(orderId=0, status="Cancelled")), completed=True)
        assert server.order_tracker.flush(timeout=1)
        proof = asyncio.run(server.execution_snapshot())
        receipt, = proof["orders"]
        assert proof["complete"] and proof["positions_complete"]
        assert (receipt["orderId"], receipt["permId"], receipt["clientIntentId"]) == (0, 1701, identity)
        assert receipt["filled"] == 30 and receipt["fillQuantityKnown"] is True

        # The ordinary management cycle first restores WORKING, then retries
        # reconciliation. Both scoped responses fail; the scoped read inside
        # _advance_close recovers and proves the old attempt terminal.
        scoped_failures_remaining = 2
        executor.manage_positions()

        completed = next(item for item in executor.intents.all(kind="CLOSE")
                         if item["intent_id"] == identity)
        assert len(failed_scopes) == 2 and all(item["intent_id"] == identity for item in failed_scopes)
        assert completed["status"] == "CANCELLED"
        assert completed["payload"]["cumulative_filled"] == 30
        assert completed["payload"]["order_ids"] == []
        assert "proposal_id" not in completed["payload"]
        assert len(proposals) == len(approvals) == len(server.placed) == 1
        assert proposals[0]["metadata"]["client_intent_id"] != identity
        assert proposals[0]["quantity"] == 10 and approvals[0][0] is True
        assert float(server.placed[0].order.totalQuantity) == 10
        assert server.inventory == 100
        assert executor.state.open_position(OWNER, CONID) is None
        assert server._emergency_order_journal.get(identity) == old_claim
        assert len(attempts) == 1
        # The old server claim remains a separate duplicate-send guard; the
        # assertions above require the correct local request/identity handoff.
    finally:
        if executor is not None:
            executor.intents.journal.close()
        if server is not None:
            server.order_tracker.close(timeout=1)
            if server.order_tracker._journal is not None:
                server.order_tracker._journal.close()
            journal = getattr(server, "_server_order_journal", None)
            if journal is not None:
                journal.journal.close()
            if server.order_tracker._temporary is not None:
                server.order_tracker._temporary.cleanup()
