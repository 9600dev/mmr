"""Emergency retries preserve identity until a proven-unsent holding changes.

The state, journals and recovery consumers are real. Broker observations use
the explicit completeness adapter from the shared recovery fixture.
"""
from test_execution_recovery import recovery
import logging

import pytest

from review.test_review_strategy_contract import FakeResult
from test_emergency_retry_restoration_contract import (
    CONID, OWNER, observe, pending_add, remember, restart, retry_owner,
)


def test_proven_unsent_retry_of_same_holding_preserves_physical_identity(retry_owner):
    ctx = retry_owner
    remember(ctx)
    ctx.executor._retry_emergency_exits()
    first, = ctx.calls
    ctx.retry_safe_ids.add(first["client_intent_id"])

    # An actual endpoint invocation invalidates the cached broker snapshot.
    # Do not repair that cache from the test between the two retry pulses.
    ctx.executor._retry_emergency_exits()

    assert [call["quantity"] for call in ctx.calls] == [40, 40]
    assert ctx.calls[1]["client_intent_id"] == first["client_intent_id"]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert ctx.faults == []


def test_unsent_exit_rotates_identity_for_captured_add_after_external_flat(retry_owner):
    ctx = retry_owner
    addition, _opening = pending_add(ctx)
    original_epoch = ctx.executor.state.open_position(OWNER, CONID)["ownership_epoch"]
    remember(ctx)
    ctx.executor._retry_emergency_exits()
    first, = ctx.calls
    assert first["quantity"] == 40
    ctx.retry_safe_ids.add(first["client_intent_id"])

    # A complete cancellation precedes an external flatten of the entire
    # account position, including the fixture's manual100. This test makes
    # no assertion that those deliberately removed manual shares survive.
    addition.update(status="Cancelled", filled=0)
    ctx.sdk.broker[CONID] = 0
    ctx.executor._snapshot_cache = None
    ctx.executor._reconciled = False
    ctx.executor._reconcile_once()
    assert ctx.executor.state.open_position(OWNER, CONID) is None

    # IB may correct cancelled-zero with a late fill. The already captured
    # OPEN is still authorized by this explicit SELL, but now owns a new UUID.
    addition.update(status="Cancelled", filled=20)
    ctx.sdk.broker[CONID] = 20
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)
    holding = ctx.executor.state.open_position(OWNER, CONID)
    assert holding["quantity"] == 20
    assert holding["ownership_epoch"] != original_epoch

    # Allow either same-pulse scheduling or one more management pulse.
    # Continuing a retained request is required; its precise pulse is not.
    ctx.executor._retry_emergency_exits()
    ctx.executor._retry_emergency_exits()
    assert [call["quantity"] for call in ctx.calls] == [40, 20]
    second = ctx.calls[1]
    assert second["client_intent_id"] != first["client_intent_id"]
    assert ctx.executor._emergency_epoch(second["client_intent_id"]) == holding["ownership_epoch"]

    observe(ctx, second["client_intent_id"], filled=20, quantity=20, status="Filled")
    ctx.executor._retry_emergency_exits()
    assert ctx.executor.state.open_position(OWNER, CONID) is None
    assert ctx.sdk.broker[CONID] == 0
    manager = restart(ctx)
    manager.manage_positions()
    assert manager.state.open_position(OWNER, CONID) is None
    assert len(ctx.calls) == 2
    assert len([row for row in ctx.sdk.accepted if row["action"] == "SELL"]) == 1
    assert ctx.faults == []


@pytest.mark.parametrize("accepted", [False, True])
def test_emergency_endpoint_result_reports_failure_without_claiming_fill(
        retry_owner, monkeypatch, accepted):
    ctx = retry_owner
    reason = "UNKNOWN: broker reply lost for emergency request"
    records = []

    def endpoint(**kwargs):
        ctx.calls.append(dict(kwargs))
        return FakeResult(ok=accepted, obj=[1999] if accepted else None,
                          error=None if accepted else reason)

    def capture_error(message, *args, **kwargs):
        records.append(logging.LogRecord("auto-executor", logging.ERROR,
                                         __file__, 0, message, args, None))

    monkeypatch.setattr(ctx.sdk, "emergency_close_position", endpoint)
    monkeypatch.setattr("trader.strategy.auto_executor.logging.error", capture_error)
    remember(ctx)
    ctx.executor._retry_emergency_exits()

    if accepted:
        assert records == []
    else:
        assert any(reason in record.getMessage() for record in records)
        assert any(reason in record.getMessage()
                   and all(word in record.getMessage().lower()
                           for word in ("emergency", "reduction", "pending"))
                   for record in records)
    # Acceptance is not execution, and uncertainty does not change ownership.
    assert len(ctx.calls) == 1
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert ctx.faults == []


def test_emergency_exception_retains_pending_request_and_durability_diagnostic(
        retry_owner, monkeypatch):
    ctx = retry_owner
    lost_reply = ConnectionError("E41: emergency broker reply interrupted")

    def endpoint(**kwargs):
        ctx.calls.append(dict(kwargs))
        raise lost_reply

    monkeypatch.setattr(ctx.sdk, "emergency_close_position", endpoint)
    remember(ctx)
    ctx.executor._retry_emergency_exits()
    # A second pulse cannot turn the ambiguous first call into another send.
    ctx.executor._retry_emergency_exits()

    assert len(ctx.calls) == 1
    assert ctx.calls[0]["quantity"] == 40
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert len(ctx.faults) == 1
    message, actual_exception = ctx.faults[0]
    assert actual_exception is lost_reply
    assert all(word in str(message).lower()
               for word in ("emergency", "pending", "no", "local", "durability"))
