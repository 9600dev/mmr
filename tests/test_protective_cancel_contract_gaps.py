"""Cancellation confirmation preserves scoped ownership and terminal truth.

The broker cancellation/reconciliation boundaries are deterministic stubs;
intent and attribution stores are real temporary databases.
"""

import datetime as dt
import logging
import sys
from types import SimpleNamespace

import pytest

from trader.strategy import auto_executor as execution
from trader.strategy.auto_executor import AutoExecutor, AutoExecState
from trader.strategy.execution_intents import IntentStore, timestamp_text


ENTRY = dt.datetime(2024, 1, 3, 15, tzinfo=dt.timezone.utc)
OWNER, CONID, ORDER = "cancel_owner", 101, 71


@pytest.fixture
def cancel_context(tmp_path, monkeypatch):
    executor = AutoExecutor.__new__(AutoExecutor)
    path = str(tmp_path / "cancellation.duckdb")
    executor.intents = IntentStore(path)
    executor.state = AutoExecState(path)
    executor.state.record_open(OWNER, CONID, 10, ENTRY, 1, None, None)
    executor.state.set_protective(OWNER, CONID, ORDER)
    executor._snapshot_cache = {"earlier_working_observation": True}
    cancelled, reconciled = [], []
    monkeypatch.setattr(executor, "_cancel_order", lambda oid: cancelled.append(oid) or True)
    def reconcile_cancelled(intent):
        # Model a successful scoped replay, including its durable zero-fill checkpoint.
        reconciled.append(intent["intent_id"])
        executor.state.apply_fill(intent, 0.0)
        executor.intents.update(intent, status="CANCELLED", cumulative_filled=0.0)

    monkeypatch.setattr(executor, "_reconcile_intent", reconcile_cancelled)
    records = []

    def record(level, message, args, exc_info=None):
        records.append(logging.LogRecord("mmr.contract.protective_cancel", level,
                                         __file__, 0, message, args, exc_info))

    # Retain real message interpolation and exception facts without rendering
    # a mutation-instrumented traceback through global logging handlers.
    logger = SimpleNamespace(
        warning=lambda message, *args: record(logging.WARNING, message, args),
        exception=lambda message, *args: record(logging.ERROR, message, args, sys.exc_info()),
    )
    monkeypatch.setattr(execution, "logging", logger)
    return SimpleNamespace(executor=executor, cancelled=cancelled, reconciled=reconciled,
                           records=records, path=path)


def _intent(ctx, *, strategy=OWNER, conid=CONID, kind="PROTECTIVE", status="WORKING",
            order_ids=None, **payload):
    position = ctx.executor.state.open_position(strategy, conid)
    proof = ({"ownership_epoch": position["ownership_epoch"],
              "ownership_started_at": position["ownership_started_at"]}
             if position is not None and kind == "PROTECTIVE" else {})
    body = {"bar_ts": timestamp_text(ENTRY), **proof, **payload}
    if order_ids is not None:
        body["order_ids"] = order_ids
    return ctx.executor.intents.create(strategy, conid, kind, body, status=status)


def test_confirmed_cancel_clears_only_its_position_pointer_and_old_snapshot(cancel_context):
    ctx = cancel_context
    other = "other_owner"
    ctx.executor.state.record_open(other, CONID, 20, ENTRY, 2, None, None)
    ctx.executor.state.set_protective(other, CONID, 72)
    target = _intent(ctx, order_ids=[ORDER])

    assert ctx.executor._cancel_protective(OWNER, CONID, ORDER, "resize for new fills") is True

    assert ctx.cancelled == [ORDER]
    assert ctx.reconciled == [target["intent_id"]]
    assert ctx.executor._snapshot_cache is None
    assert ctx.executor.state.open_position(OWNER, CONID)["protective_order_id"] is None
    assert ctx.executor.state.open_position(other, CONID)["protective_order_id"] == 72
    saved, = IntentStore(ctx.path).all(strategy=OWNER, conid=CONID, kind="PROTECTIVE")
    assert saved["status"] == "CANCELLED"


@pytest.mark.parametrize("order_id", [ORDER, None])
def test_unproven_protective_attribution_keeps_working_protection_reserved(cancel_context, order_id):
    ctx = cancel_context
    target = _intent(ctx, order_ids=[ORDER], attribution_unresolved=True)

    assert ctx.executor._cancel_protective(OWNER, CONID, order_id, "close requested") is False

    assert ctx.cancelled == ctx.reconciled == []
    assert ctx.executor.state.open_position(OWNER, CONID)["protective_order_id"] == ORDER
    assert ctx.executor.intents.all()[0]["status"] == target["status"] == "WORKING"


@pytest.mark.parametrize("difference", ["strategy", "conid", "kind", "terminal"])
def test_unrelated_uncertainty_cannot_block_this_protective_cancel(cancel_context, difference):
    ctx = cancel_context
    kwargs = {
        "strategy": {"strategy": "other_owner"},
        "conid": {"conid": 202},
        "kind": {"kind": "OPEN"},
        "terminal": {"status": "RESOLVED"},
    }[difference]
    unrelated = _intent(ctx, order_ids=[72], attribution_unresolved=True, **kwargs)
    target = _intent(ctx, order_ids=[ORDER])

    assert ctx.executor._cancel_protective(OWNER, CONID, ORDER, "close requested") is True

    assert ctx.reconciled == [target["intent_id"]]
    saved = {item["intent_id"]: item for item in ctx.executor.intents.all()}
    assert saved[unrelated["intent_id"]] == unrelated


def test_terminal_history_with_a_reused_numeric_id_is_not_reconciled_again(cancel_context):
    ctx = cancel_context
    historical = _intent(ctx, order_ids=[ORDER], status="FILLED")
    target = _intent(ctx, order_ids=[ORDER])

    assert ctx.executor._cancel_protective(OWNER, CONID, ORDER, "current stop") is True

    assert ctx.reconciled == [target["intent_id"]]
    saved = {item["intent_id"]: item for item in ctx.executor.intents.all()}
    assert saved[historical["intent_id"]] == historical


@pytest.mark.parametrize("difference", ["strategy", "conid", "kind"])
def test_same_numeric_id_cannot_rewrite_another_intents_scoped_history(cancel_context, difference):
    # The journal accepts historical identities from different broker ID
    # namespaces. A numeric alias must not erase the explicit local scope.
    ctx = cancel_context
    kwargs = {"strategy": {"strategy": "other_owner"}, "conid": {"conid": 202},
              "kind": {"kind": "CLOSE"}}[difference]
    unrelated = _intent(ctx, order_ids=[ORDER], **kwargs)
    target = _intent(ctx, order_ids=[ORDER])

    assert ctx.executor._cancel_protective(OWNER, CONID, ORDER, "current stop") is True

    assert ctx.reconciled == [target["intent_id"]]
    saved = {item["intent_id"]: item for item in ctx.executor.intents.all()}
    assert saved[unrelated["intent_id"]] == unrelated


def test_unacknowledged_protective_intent_has_no_order_id_to_reconcile(cancel_context):
    ctx = cancel_context
    pending = _intent(ctx, status="SUBMITTING")

    assert ctx.executor._cancel_protective(OWNER, CONID, ORDER, "tracked old stop") is True

    assert ctx.cancelled == [ORDER]
    assert ctx.reconciled == []
    assert ctx.executor.intents.all()[0] == pending


def test_stop_filled_during_cancel_keeps_filled_terminal_history(cancel_context, monkeypatch):
    ctx = cancel_context
    target = _intent(ctx, order_ids=[ORDER])

    def observe_fill(intent):
        ctx.executor.intents.update(intent, status="FILLED", cumulative_filled=10)

    monkeypatch.setattr(ctx.executor, "_reconcile_intent", observe_fill)

    assert ctx.executor._cancel_protective(OWNER, CONID, ORDER, "close requested") is True

    saved, = ctx.executor.intents.all()
    assert saved["intent_id"] == target["intent_id"]
    assert saved["status"] == "FILLED"
    assert saved["payload"]["cumulative_filled"] == 10


def test_pending_cancel_warning_preserves_order_identity_and_reason(cancel_context, monkeypatch):
    ctx = cancel_context
    monkeypatch.setattr(ctx.executor, "_cancel_order", lambda _oid: False)

    assert ctx.executor._cancel_protective(OWNER, CONID, ORDER, "resize after partial fill") is False

    messages = [record.getMessage() for record in ctx.records]
    assert len(messages) == 1
    assert str(ORDER) in messages[0] and "resize after partial fill" in messages[0]
    assert ctx.executor.state.open_position(OWNER, CONID)["protective_order_id"] == ORDER


def test_uncertain_cancel_reports_the_order_and_underlying_exception(cancel_context, monkeypatch):
    ctx = cancel_context

    def unavailable(_oid):
        raise ConnectionError("broker reply unavailable")

    monkeypatch.setattr(ctx.executor, "_cancel_order", unavailable)

    assert ctx.executor._cancel_protective(OWNER, CONID, ORDER, "close requested") is False

    assert len(ctx.records) == 1
    record = ctx.records[0]
    assert str(ORDER) in record.getMessage()
    assert record.exc_info and str(record.exc_info[1]) == "broker reply unavailable"
