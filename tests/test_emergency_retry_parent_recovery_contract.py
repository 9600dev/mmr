"""Parent request recovery through the native intent producer and a fake broker adapter."""
from test_execution_recovery import recovery
import pandas as pd

from review.test_review_strategy_contract import TS
from test_emergency_retry_restoration_contract import (
    CONID, OWNER, REQUEST_BAR, observe, pending_add, restart, retry_owner,
)


def test_rotated_timer_keeps_parent_policy_during_broker_only_recovery(retry_owner, monkeypatch):
    ctx = retry_owner
    addition, _opening = pending_add(ctx)
    ctx.sdk.cancel_fails = True
    ctx.executor._execute_close_durable(
        OWNER, CONID, REQUEST_BAR, 40, "entry A timer", entry_bar_ts=TS)
    parent, = ctx.executor.intents.all(kind="CLOSE")
    assert parent["status"] == "WAITING"
    assert ctx.calls == []

    def unavailable_journal(*args, **kwargs):
        raise OSError("local journal read unavailable for the existing timer")

    with monkeypatch.context() as outage:
        outage.setattr(ctx.executor.intents.journal, "transaction", unavailable_journal)
        ctx.executor._execute_close(
            OWNER, CONID, REQUEST_BAR, 40, "entry A timer", entry_bar_ts=TS, explicit=False)
    initial, = ctx.calls
    assert len(ctx.faults) == 1 and isinstance(ctx.faults[0][1], OSError)
    ctx.faults.clear()

    # A repeated valid timer request for the same currently owned entry is
    # retained by the real merger, then may replace the proven-unsent attempt.
    ctx.executor._remember_emergency_exit(
        OWNER, CONID, REQUEST_BAR + pd.Timedelta(seconds=5), "retained entry A timer",
        entry_bar_ts=TS, capture_explicit=False)
    # Native reduction coordination can now confirm the earlier opening
    # cancellation before the replacement sends. A later cumulative receipt
    # is a correction of this terminal0 observation, not an ignored refusal.
    ctx.sdk.cancel_fails = False
    ctx.sdk.cancel(addition["orderId"])
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)
    ctx.retry_safe_ids.add(initial["client_intent_id"])
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert [call["quantity"] for call in ctx.calls] == [40, 40]
    current = ctx.calls[-1]
    assert current["client_intent_id"] != initial["client_intent_id"]

    addition.update(filled=20, status="Cancelled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60
    observe(ctx, current["client_intent_id"], filled=40, quantity=40, status="Filled")
    # Restart before retry can copy the volatile request payload locally.
    # Discovery must recover the older timer policy through the durable parent,
    # rather than assign the raw child's physical UUID a fresh B-entry policy.
    manager = restart(ctx)
    manager.manage_positions()
    manager.manage_positions()
    assert manager.state.open_position(OWNER, CONID)["quantity"] == 20
    assert ctx.sdk.broker[CONID] == 120
    assert [proposal["action"] for proposal in ctx.sdk.propose_calls] == ["BUY", "BUY"]
    assert len(ctx.calls) == 2
    assert ctx.faults == []
