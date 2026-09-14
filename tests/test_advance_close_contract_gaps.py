"""Close coordination keeps exact scope, fresh quantities and recovery demand.

Tests use the native local stores and deterministic broker-boundary controls.
They exercise coordination separately from the existing complete broker replay
scenarios; no configured services or accounts are contacted.
"""

import copy
import datetime as dt
import logging
import threading
from types import SimpleNamespace

import pytest

from trader.strategy import auto_executor as execution
from trader.strategy import execution_intents as intent_storage
from trader.strategy.auto_executor import AutoExecutor, AutoExecState
from trader.strategy.execution_intents import IntentStore, timestamp_text


OWNER, CONID = "close_contract", 101
ENTRY = dt.datetime(2024, 1, 3, 15, tzinfo=dt.timezone.utc)
LATER = ENTRY + dt.timedelta(minutes=1)
IDENT = {"symbol": "SYNTHETIC", "exchange": "NYSE", "currency": "USD", "sec_type": "STK"}


@pytest.fixture
def close_context(tmp_path, monkeypatch):
    executor = AutoExecutor.__new__(AutoExecutor)
    path = str(tmp_path / "close.duckdb")
    executor.state = AutoExecState(path)
    executor.intents = IntentStore(path)
    executor.state.record_open(OWNER, CONID, 10, ENTRY, 51, None, None)
    executor._view_lock = threading.Lock()
    executor._emergency_exits = {}
    executor._managed_view = []
    executor._ownership_warnings = set()
    snapshot = {"complete": True, "positions_complete": True, "retry_safe": False,
                "orders": [], "positions": [{"conId": CONID, "position": 110.0}]}
    sdk = SimpleNamespace(execution_snapshot=lambda **_kwargs: snapshot)
    submitted, protective_cancels, opening_cancels = [], [], []
    reconciled, refreshed, resolves, retries, views = [], [], [], [], []
    monkeypatch.setattr(executor, "_get_sdk", lambda: sdk)
    monkeypatch.setattr(executor, "_execution_snapshot", lambda intent=None: snapshot)
    monkeypatch.setattr(executor, "_reconcile_intent", lambda intent: reconciled.append(intent["intent_id"]))
    monkeypatch.setattr(executor, "_reconcile_intents", lambda strategy=None, conid=None: refreshed.append((strategy, conid)))
    monkeypatch.setattr(executor, "_resolve_exact", lambda conid: resolves.append(conid) or dict(IDENT))
    monkeypatch.setattr(executor, "_cancel_order", lambda oid: opening_cancels.append(oid) or True)
    cancelled = set()

    def cancel_protective(strategy, conid, order_id, reason):
        protective_cancels.append((strategy, conid, order_id, reason))
        cancelled.add((strategy, conid, order_id))
        # A successful cancellation control must expose the corresponding
        # terminal local state to the final reconciliation check.
        for current in executor.intents.all(strategy=strategy, conid=conid, kind="PROTECTIVE", active=True):
            ids = current["payload"].get("order_ids", [])
            if ids and all((strategy, conid, oid) in cancelled for oid in ids):
                executor.intents.update(current, status="CANCELLED")
        return True

    monkeypatch.setattr(executor, "_cancel_protective", cancel_protective)
    monkeypatch.setattr(executor, "_submit_intent", lambda intent: submitted.append(copy.deepcopy(intent)))
    monkeypatch.setattr(executor, "_retry_emergency_exits", lambda: retries.append(True))
    monkeypatch.setattr(executor, "_load_open_view", lambda: views.append(True))
    records = []

    def record(level, message, args):
        records.append(logging.LogRecord("mmr.contract.advance_close", level,
                                         __file__, 0, message, args, None))

    logger = SimpleNamespace(
        warning=lambda message, *args: record(logging.WARNING, message, args),
        critical=lambda message, *args: record(logging.CRITICAL, message, args),
    )
    monkeypatch.setattr(execution, "logging", logger)
    return SimpleNamespace(executor=executor, snapshot=snapshot, submitted=submitted,
                           protective_cancels=protective_cancels, opening_cancels=opening_cancels,
                           reconciled=reconciled, refreshed=refreshed, resolves=resolves,
                           retries=retries, views=views, records=records)


def _intent(ctx, kind="CLOSE", *, strategy=OWNER, conid=CONID, status="WAITING", **payload):
    # A physical reduction already submitted by the native coordinator carries
    # immutable holding proof. New, unsubmitted CLOSE demand is bound later.
    if kind == "PROTECTIVE" or (kind == "CLOSE" and (status != "WAITING" or payload.get("proposal_id") is not None)):
        position = ctx.executor.state.open_position(strategy, conid)
        if position is not None:
            payload.setdefault("ownership_epoch", position["ownership_epoch"])
            payload.setdefault("ownership_started_at", position["ownership_started_at"])
    return ctx.executor.intents.create(strategy, conid, kind,
        {"bar_ts": timestamp_text(LATER), "quantity": 10.0,
         "reason": "operator close", "exit_request_active": True, **payload}, status=status)


@pytest.mark.parametrize("known_identity", [False, True])
def test_close_resolves_only_a_missing_exact_identity_and_binds_owned_quantity(close_context, known_identity):
    ctx = close_context
    intent = _intent(ctx, **({"ident": dict(IDENT)} if known_identity else {}))

    ctx.executor._advance_close(intent)

    submitted, = ctx.submitted
    position = ctx.executor.state.open_position(OWNER, CONID)
    assert ctx.resolves == ([] if known_identity else [CONID])
    assert ctx.refreshed == [(OWNER, CONID)]
    assert submitted["payload"]["ident"] == IDENT
    assert submitted["payload"]["quantity"] == 10.0
    assert submitted["payload"]["ownership_epoch"] == position["ownership_epoch"]
    assert submitted["payload"]["ownership_started_at"] == position["ownership_started_at"]


@pytest.mark.parametrize(("status", "proposal", "retry_safe", "allowed", "resuming"), [
    ("WAITING", 91, False, True, False),
    ("SUBMITTING", 91, False, False, False),
    ("SUBMITTING", 91, True, True, True),
    ("UNKNOWN", 91, True, True, True),
    ("UNKNOWN", None, False, True, False),
])
def test_only_a_submitted_proposal_needs_explicit_retry_safe_evidence(
        close_context, status, proposal, retry_safe, allowed, resuming):
    ctx = close_context
    payload = {"proposal_id": proposal} if proposal is not None else {}
    intent = _intent(ctx, status=status, **payload)
    ctx.snapshot["retry_safe"] = retry_safe

    ctx.executor._advance_close(intent)

    assert bool(ctx.submitted) is allowed
    assert bool(intent["payload"].get("resume_approval")) is resuming


def test_protective_sources_form_one_exact_scoped_cancellation_set(close_context):
    ctx = close_context
    _intent(ctx, "PROTECTIVE", status="FILLED", order_ids=[75])
    _intent(ctx, "PROTECTIVE", status="WORKING", order_ids=[71, 72])
    _intent(ctx, "PROTECTIVE", strategy="other_owner", status="WORKING", order_ids=[76])
    _intent(ctx, "PROTECTIVE", conid=202, status="WORKING", order_ids=[77])
    ctx.executor.state.set_protective(OWNER, CONID, 73)
    ctx.snapshot["orders"] = [
        {"orderId": 71, "orderRef": OWNER, "conId": CONID, "action": "SELL",
         "orderType": "STP", "status": "Submitted"},
        {"orderId": 74, "orderRef": "other_owner", "conId": CONID, "action": "SELL",
         "orderType": "STP", "status": "Submitted"},
    ]
    intent = _intent(ctx)

    ctx.executor._advance_close(intent)

    assert sorted(ctx.protective_cancels) == [
        (OWNER, CONID, 71, "operator close"),
        (OWNER, CONID, 72, "operator close"),
        (OWNER, CONID, 73, "operator close"),
    ]
    assert len(ctx.submitted) == 1


def test_protective_without_an_acknowledged_id_does_not_invent_a_cancel_target(close_context):
    ctx = close_context
    _intent(ctx, "PROTECTIVE", status="SUBMITTING")
    intent = _intent(ctx)

    ctx.executor._advance_close(intent)

    # This isolated coordination check makes no claim that a missing broker
    # acknowledgement authorizes another order; reconciliation is stubbed.
    assert ctx.protective_cancels == []


@pytest.mark.parametrize("order_ids", [[0], [-1], [True], ["71"], [71.5]])
def test_malformed_legacy_protective_ids_retain_the_reservation(close_context, order_ids):
    ctx = close_context
    _intent(ctx, "PROTECTIVE", status="UNKNOWN", order_ids=order_ids)
    intent = _intent(ctx)

    ctx.executor._advance_close(intent)

    assert ctx.submitted == ctx.protective_cancels == []
    assert intent["status"] == "WAITING"


def test_unproven_protective_reservation_defers_close_even_without_a_cancel_id(close_context):
    ctx = close_context
    _intent(ctx, "PROTECTIVE", status="UNKNOWN", attribution_unresolved=True)
    intent = _intent(ctx)

    ctx.executor._advance_close(intent)

    assert ctx.submitted == ctx.protective_cancels == ctx.retries == []
    assert intent["status"] == "WAITING"


@pytest.mark.parametrize("status,order_ids", [("SUBMITTING", []), ("WORKING", [88])])
def test_protection_discovered_by_final_reconciliation_keeps_close_pending(
        close_context, monkeypatch, status, order_ids):
    ctx = close_context
    intent = _intent(ctx)

    def discover_protection(strategy, conid):
        assert (strategy, conid) == (OWNER, CONID)
        # The final refresh may discover a native protective reservation that
        # was absent from the earlier cancellation set. It remains executable
        # or uncertain until the next coordination cycle establishes its fate.
        _intent(ctx, "PROTECTIVE", status=status, order_ids=order_ids)

    monkeypatch.setattr(ctx.executor, "_reconcile_intents", discover_protection)

    ctx.executor._advance_close(intent)

    assert ctx.submitted == []
    assert intent["status"] == "WAITING"
    assert ctx.executor.intents.all(strategy=OWNER, conid=CONID, kind="PROTECTIVE", active=True)


def test_other_instruments_and_terminal_open_history_do_not_block_this_close(close_context):
    ctx = close_context
    _intent(ctx, "OPEN", status="FILLED")
    _intent(ctx, "OPEN", conid=202, status="SUBMITTING")
    intent = _intent(ctx)

    ctx.executor._advance_close(intent)

    assert len(ctx.submitted) == 1
    assert ctx.opening_cancels == []
    assert ctx.reconciled == [intent["intent_id"]]


def test_late_terminal_open_replay_does_not_skip_a_second_live_reservation(close_context, monkeypatch):
    ctx = close_context
    historical = _intent(ctx, "OPEN", status="FILLED", order_ids=[81])
    live = _intent(ctx, "OPEN", status="WORKING", order_ids=[82])
    # A late receipt can return historical terminal metadata to UNKNOWN
    # after a newer active OPEN was created. Native IntentStore permits this.
    updates = iter((live["updated"] + 1.0, live["updated"] + 2.0))
    with monkeypatch.context() as patch:
        patch.setattr(intent_storage, "time", SimpleNamespace(time=lambda: next(updates)))
        ctx.executor.intents.update(historical, status="UNKNOWN")
        ctx.executor.intents.update(live, status="WORKING")
    ctx.snapshot["orders"] = [
        {"orderId": 81, "clientIntentId": historical["intent_id"], "conId": CONID,
         "action": "BUY", "status": "Unknown", "brokerStatus": "Cancelled"},
        {"orderId": 82, "clientIntentId": live["intent_id"], "conId": CONID,
         "action": "BUY", "status": "Submitted"},
    ]
    intent = _intent(ctx)

    ctx.executor._advance_close(intent)

    assert ctx.opening_cancels == [82]
    assert {historical["intent_id"], live["intent_id"]}.issubset(ctx.reconciled)
    assert len(ctx.submitted) == 1


@pytest.mark.parametrize("stage", ["opening_reconciliation", "protective_cancellation"])
def test_stale_timer_resolves_when_a_new_entry_appears_during_coordination(close_context, monkeypatch, stage):
    ctx = close_context
    intent = _intent(ctx, policy_entry_bar_ts=timestamp_text(ENTRY))
    ctx.executor.state.set_protective(OWNER, CONID, 73)

    def newer_entry():
        ctx.executor.state.record_add(OWNER, CONID, 5, LATER, 52, None, None)
        ctx.snapshot["positions"][0]["position"] = 115.0

    if stage == "opening_reconciliation":
        opening = _intent(ctx, "OPEN", status="UNKNOWN", order_ids=[81])
        ctx.snapshot["orders"] = [{"orderId": 81, "clientIntentId": opening["intent_id"],
            "conId": CONID, "action": "BUY", "status": "Filled",
            "filled": 5.0, "totalQuantity": 5.0, "fillQuantityKnown": True}]

        def reconcile(current):
            if current["intent_id"] == opening["intent_id"]:
                newer_entry()

        monkeypatch.setattr(ctx.executor, "_reconcile_intent", reconcile)
    else:
        def cancel(*args):
            ctx.protective_cancels.append(args)
            newer_entry()
            return True

        monkeypatch.setattr(ctx.executor, "_cancel_protective", cancel)

    ctx.executor._advance_close(intent)

    assert ctx.submitted == ctx.retries == []
    assert intent["status"] == "RESOLVED"
    assert len(ctx.protective_cancels) == (0 if stage == "opening_reconciliation" else 1)


def test_final_submission_uses_ownership_and_broker_quantity_refreshed_after_cancel(close_context, monkeypatch):
    ctx = close_context
    ctx.executor.state.set_protective(OWNER, CONID, 73)
    intent = _intent(ctx)

    def cancel(*args):
        ctx.executor.state.record_add(OWNER, CONID, 5, LATER, 52, None, None)
        ctx.snapshot["positions"][0]["position"] = 115.0
        return True

    monkeypatch.setattr(ctx.executor, "_cancel_protective", cancel)

    ctx.executor._advance_close(intent)

    submitted, = ctx.submitted
    assert submitted["payload"]["quantity"] == 15.0


def test_complete_empty_position_snapshot_resolves_the_exit_with_auditable_status(close_context):
    ctx = close_context
    ctx.snapshot["positions"] = []
    intent = _intent(ctx)

    ctx.executor._advance_close(intent)

    assert ctx.submitted == ctx.retries == []
    assert intent["status"] == "RESOLVED"
    assert ctx.executor.state.open_position(OWNER, CONID) is None
    status, reason = ctx.executor.state.db.execute(
        "SELECT status,closed_reason FROM auto_exec_positions WHERE strategy=? AND conid=?",
        [OWNER, CONID], fetch="one")
    assert status == "CLOSED_EXTERNALLY"
    assert isinstance(reason, str) and "broker" in reason.lower() and "holding" in reason.lower()


@pytest.mark.parametrize("proof", ["missing_holding_epoch", "different_physical_epoch"])
def test_a_close_without_the_current_holding_identity_cannot_submit(close_context, proof):
    ctx = close_context
    if proof == "missing_holding_epoch":
        ctx.executor.state.db.execute(
            "UPDATE auto_exec_positions SET ownership_epoch=NULL WHERE strategy=? AND conid=?",
            [OWNER, CONID])
        payload = {}
    else:
        payload = {"ownership_epoch": "0" * 32, "ownership_started_at": 1.0}
    intent = _intent(ctx, **payload)

    ctx.executor._advance_close(intent)

    assert ctx.submitted == ctx.retries == []
    assert intent["status"] == ("WAITING" if proof == "missing_holding_epoch" else "RESOLVED")


@pytest.mark.parametrize("binding", ["timer", "request", "explicit", "legacy_unbound"])
def test_local_failure_transfers_exact_request_authority_to_the_emergency_mailbox(close_context, monkeypatch, binding):
    ctx = close_context
    scope = {"positions": [{"entry_bar_ts": timestamp_text(ENTRY), "proposal_id": 51}],
             "openings": {}}
    payload = {
        "timer": {"policy_entry_bar_ts": timestamp_text(ENTRY)},
        "request": {"request_entry_bar_ts": timestamp_text(ENTRY)},
        "explicit": {"explicit_exit_scope": scope},
        "legacy_unbound": {},
    }[binding]
    intent = _intent(ctx, **payload)

    def unavailable(_conid):
        raise ConnectionError("exact contract lookup unavailable")

    monkeypatch.setattr(ctx.executor, "_resolve_exact", unavailable)

    ctx.executor._advance_close(intent)

    assert ctx.submitted == []
    assert ctx.retries == [True]
    emergency = ctx.executor._emergency_exits[(OWNER, CONID)]
    assert emergency["parent_intent_id"] == intent["intent_id"]
    assert emergency["bar_ts"] == timestamp_text(LATER)
    assert emergency["reason"] == "operator close"
    assert emergency["policy_entry_bar_ts"] == payload.get("policy_entry_bar_ts")
    assert emergency["request_entry_bar_ts"] == payload.get("request_entry_bar_ts")
    assert emergency["explicit_exit_scope"] == payload.get("explicit_exit_scope")
    warning = next(record.getMessage() for record in ctx.records if record.levelno == logging.WARNING)
    assert intent["intent_id"] in warning and "exact contract lookup unavailable" in warning
