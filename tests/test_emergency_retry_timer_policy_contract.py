"""Restoring a receipt cannot broaden its timer policy."""
from test_execution_recovery import recovery
from test_emergency_retry_restoration_contract import (
    CONID, OWNER, observe, pending_add, remember, restart, retry_owner,
)


def test_restored_old_timer_leaves_the_later_buy_entry_residual(retry_owner):
    ctx = retry_owner
    addition, _opening = pending_add(ctx)
    remember(ctx, timer=True)
    ctx.executor._retry_emergency_exits()
    attempt, = ctx.calls
    assert attempt["quantity"] == 40

    # This already-submitted BUY completes after the old timer's physical
    # attempt. It keeps the holding UUID but starts the later BUY's policy.
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60
    observe(ctx, attempt["client_intent_id"], filled=40, quantity=40, status="Filled")

    ctx.executor._retry_emergency_exits()

    # The old physical40 still counts; only its stale continuation is refused.
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 20
    assert ctx.sdk.broker[CONID] == 120
    manager = restart(ctx)
    manager.manage_positions()
    assert manager.state.open_position(OWNER, CONID)["quantity"] == 20
    assert ctx.sdk.broker[CONID] == 120
    assert len(ctx.calls) == 1
    assert len([row for row in ctx.sdk.accepted if row["action"] == "SELL"]) == 1
    assert ctx.faults == []
