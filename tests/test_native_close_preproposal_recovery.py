"""A local pre-proposal crash is distinct from an uncertain broker send.

The real executor persists SUBMITTING before entering propose. This control
stops at that exact boundary, before proposal allocation or any native claim,
then restarts the executor against the unchanged durable request.
"""
import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from review.test_review_order_contract import _coordinated_trader, _stock
from review.test_review_strategy_contract import FakeResult
from trader.messaging.trader_service_api import TraderServiceApi
from trader.strategy.auto_executor import AutoExecutor


class _PreProposalCrash(BaseException):
    """Process-stop seam: bypass ordinary submission exception recovery."""


def test_native_close_resumes_after_crash_before_proposal_allocation(tmp_path, monkeypatch):
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    trader = _coordinated_trader(tmp_path, held=140)
    managers = []
    try:
        owner, conid = "preproposal_recovery", 100
        proposals, approvals = [], []

        def propose(**kwargs):
            proposals.append(kwargs)
            return len(proposals), None, None

        def approve(proposal_id, **kwargs):
            approvals.append(proposal_id)
            proposal = proposals[proposal_id - 1]
            result = asyncio.run(trader.place_expressive_order(
                _stock(), proposal["action"], proposal["quantity"],
                {"order_type": "MARKET"}, algo_name=owner,
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
            resolve=lambda symbol, **kwargs: [_stock()] if symbol in (conid, "AUDIT") else [],
            propose=propose, approve=approve,
            cancel=lambda order_id: api.cancel_order(order_id),
            _proposal_store=lambda: SimpleNamespace(get=lambda _pid: None),
        )
        path = str(tmp_path / "preproposal-executor.duckdb")
        first = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        managers.append(first)
        entry = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
        first.state.record_open(owner, conid, 40, entry, 501, None, None)
        first._load_open_view()

        def crash_before_allocation(**kwargs):
            raise _PreProposalCrash()

        with monkeypatch.context() as crash:
            crash.setattr(sdk, "propose", crash_before_allocation)
            with pytest.raises(_PreProposalCrash):
                first._execute_close(owner, conid, entry + dt.timedelta(minutes=1),
                                     40, "retained explicit close")
        pending, = first.intents.all(kind="CLOSE")
        identity = pending["intent_id"]
        assert pending["status"] == "SUBMITTING" and pending["payload"]["submitted_at"] > 0
        assert not pending["payload"].get("proposal_id") and not pending["payload"].get("order_ids")
        assert proposals == approvals == [] and trader.placed == []
        assert trader.server_order_journal().get(identity) is None
        proof = asyncio.run(trader.execution_snapshot(intent_id=identity))
        assert proof["orders"] == [] and proof["retry_safe"] is False

        first.intents.journal.close()
        restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        managers.append(restarted)
        restored, = restarted.intents.all(kind="CLOSE")
        assert restored["intent_id"] == identity and restored["status"] == "SUBMITTING"
        restarted._advance_close(restored)

        assert len(proposals) == len(approvals) == len(trader.placed) == 1
        assert proposals[0]["quantity"] == 40
        assert proposals[0]["metadata"]["client_intent_id"] == identity
        assert trader.inventory == 100
        assert restarted.state.open_position(owner, conid) is None
        final, = restarted.intents.all(kind="CLOSE")
        assert final["status"] == "FILLED" and final["payload"]["cumulative_filled"] == 40
    finally:
        trader.order_tracker.close(timeout=1)
        tracker_journal = getattr(trader.order_tracker, "_journal", None)
        if tracker_journal is not None:
            tracker_journal.close()
        server_journal = getattr(trader, "_server_order_journal", None)
        if server_journal is not None:
            server_journal.journal.close()
        for manager in managers:
            manager.intents.journal.close()
