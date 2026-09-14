"""Remaining close coordination boundaries use exact ownership and broker scope.

These controls use the existing isolated coordinator fixture. They do not
start services or contact a broker; acceptance and cancellation are recorded
at the fixture boundary rather than inferred from order quantities.
"""

import logging

import pytest

from test_advance_close_contract_gaps import (
    CONID, ENTRY, IDENT, LATER, OWNER, _intent, close_context,
)
from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_intents import IntentStore, timestamp_text


@pytest.fixture
def remaining_close(close_context):
    try:
        yield close_context
    finally:
        close_context.executor.intents.journal.close()


def test_unbound_legacy_close_with_lost_ack_stays_reserved_after_restart(
        remaining_close, monkeypatch):
    ctx = remaining_close
    executor = ctx.executor
    # Restore persisted pre-epoch metadata, including a possible submission
    # with no saved numeric acknowledgement. New code must not bind this old
    # physical attempt to whichever holding happens to exist now.
    intent = executor.intents.restore_exit("legacy-close-with-lost-ack", OWNER, CONID, {
        "bar_ts": timestamp_text(LATER), "quantity": 10.0,
        "reason": "persisted close awaiting acknowledgement", "ident": dict(IDENT),
        "submitted_at": ENTRY.timestamp(), "order_ids": [],
    })
    monkeypatch.setattr(executor, "_reconcile_intent",
                        AutoExecutor._reconcile_intent.__get__(executor, AutoExecutor))
    original = executor.state.open_position(OWNER, CONID)

    executor._advance_close(intent)
    assert intent["status"] == "UNKNOWN"
    assert intent["payload"]["attribution_unresolved"] is True
    assert intent["payload"].get("ownership_epoch") is None

    # IntentStore takes the original DuckDB base path and derives its own
    # SQLite journal path. Reopen that same base, not the journal filename.
    base = executor.state.db.db_path
    executor.intents.journal.close()
    executor.intents = IntentStore(base)
    restored, = executor.intents.all(strategy=OWNER, conid=CONID, kind="CLOSE")
    executor._advance_close(restored)

    assert restored["status"] == "UNKNOWN"
    assert restored["payload"]["attribution_unresolved"] is True
    assert restored["payload"].get("ownership_epoch") is None
    assert ctx.submitted == ctx.protective_cancels == ctx.opening_cancels == ctx.retries == []
    assert executor.state.open_position(OWNER, CONID) == original


def test_close_cancels_broker_visible_protection_without_a_local_id_fallback(remaining_close):
    ctx = remaining_close
    # Only the broker view proves this protector. Neither the owned row nor
    # any local protective intent supplies its ID; unrelated rows coexist.
    ctx.snapshot["orders"] = [
        {"orderId": 71, "orderRef": OWNER, "clientIntentId": "observed-stop",
         "conId": CONID, "action": "SELL", "orderType": "STP", "status": "Submitted"},
        {"orderId": 72, "orderRef": "other_owner", "conId": CONID,
         "action": "SELL", "orderType": "STP", "status": "Submitted"},
        {"orderId": 73, "orderRef": OWNER, "conId": 202,
         "action": "SELL", "orderType": "STP", "status": "Submitted"},
    ]
    intent = _intent(ctx)

    ctx.executor._advance_close(intent)

    assert ctx.protective_cancels == [(OWNER, CONID, 71, "operator close")]
    submitted, = ctx.submitted
    assert submitted["payload"]["quantity"] == 10.0


def test_smallest_positive_broker_id_can_release_a_protective_reservation(remaining_close):
    ctx = remaining_close
    protective = _intent(ctx, "PROTECTIVE", status="WORKING", order_ids=[1])
    intent = _intent(ctx)

    ctx.executor._advance_close(intent)

    assert ctx.protective_cancels == [(OWNER, CONID, 1, "operator close")]
    saved, = ctx.executor.intents.all(strategy=OWNER, conid=CONID, kind="PROTECTIVE")
    assert saved["intent_id"] == protective["intent_id"]
    assert saved["status"] == "CANCELLED"
    submitted, = ctx.submitted
    assert submitted["payload"]["quantity"] == 10.0


def test_legacy_ownership_warning_retains_each_owner_instrument_and_missing_origin(
        remaining_close, monkeypatch):
    ctx = remaining_close
    other = "second_owner"
    ctx.executor.state.record_open(other, CONID, 4, ENTRY, 52, None, None)
    ctx.executor.state.db.execute(
        "UPDATE auto_exec_positions SET ownership_epoch=NULL, ownership_started_at=NULL "
        "WHERE conid=?", [CONID])
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "8")
    ctx.snapshot["positions"][0]["avgCost"] = 100.0
    # Protection and close coordination diagnose the same missing holding
    # proof. Switching paths must not produce duplicate alerts for it.
    for owner in (OWNER, other):
        ctx.executor._ensure_protective(owner, CONID)
    intents = [_intent(ctx, strategy=owner) for owner in (OWNER, other)]

    for _ in range(2):
        for intent in intents:
            ctx.executor._advance_close(intent)

    warnings = [record.getMessage() for record in ctx.records if record.levelno == logging.WARNING]
    messages = [message for message in warnings if 'ownership proof unavailable' in message]
    assert len(messages) == 2
    for owner in (OWNER, other):
        own_message, = [message for message in messages if owner in message]
        assert str(CONID) in own_message
        assert "legacy" in own_message.lower() and "holding" in own_message.lower()
        position = ctx.executor.state.open_position(owner, CONID)
        assert position["ownership_epoch"] is None
    # Legacy quantity-only rows separately lack fill-price evidence. That
    # diagnostic is also once per owner, without duplicating ownership alerts.
    cost_messages = [message for message in warnings if 'owned fill price unavailable' in message]
    assert len(cost_messages) == 2
    for owner in (OWNER, other):
        own_cost, = [message for message in cost_messages if owner in message]
        assert str(CONID) in own_cost
    assert all(intent["status"] == "WAITING" for intent in intents)
    assert ctx.submitted == ctx.protective_cancels == ctx.retries == []
