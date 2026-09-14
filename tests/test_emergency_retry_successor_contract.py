"""A retained successor preserves request scope across emergency recovery.

The explicit snapshot adapter is inherited from the reviewed retry fixture.
These controls assert request/receipt behavior, not native broker timing.
"""
from test_execution_recovery import recovery
import pandas as pd
import pytest

from review.test_review_strategy_contract import TS, make_work
from test_emergency_retry_restoration_contract import (
    CONID, OWNER, REQUEST_BAR, observe, pending_add, remember, restart, retry_owner,
)
from trader.strategy.execution_intents import timestamp_text


def test_first_restore_keeps_captured_scope_when_followup_write_fails(retry_owner, monkeypatch):
    ctx = retry_owner
    addition, _opening = pending_add(ctx)
    remember(ctx)
    ctx.executor._retry_emergency_exits()
    initial, = ctx.calls
    receipt = observe(ctx, initial["client_intent_id"], filled=0, quantity=40, status="Submitted")

    def unavailable_scope_write(*args, **kwargs):
        raise OSError("followup scope write unavailable after restore insert")

    with monkeypatch.context() as fault:
        fault.setattr(ctx.executor.intents, "retain_exit_scope", unavailable_scope_write)
        ctx.executor._retry_emergency_exits()
    restored, = ctx.executor.intents.all(kind="CLOSE")
    assert restored["intent_id"] == initial["client_intent_id"]

    # Close the original journal before replaying the committed first insert.
    manager = restart(ctx)
    receipt.update(filled=40, status="Filled")
    ctx.sdk.broker[CONID] -= 40
    manager.manage_positions()
    assert manager.state.open_position(OWNER, CONID) is None
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    manager.manage_positions()
    assert manager.state.open_position(OWNER, CONID) is None
    assert ctx.sdk.broker[CONID] == 100
    assert ctx.sdk.propose_calls[-1]["quantity"] == 20
    assert len(ctx.calls) == 1


def test_proven_unsent_successor_keeps_its_scope_and_request_audit(retry_owner):
    ctx = retry_owner
    addition, _opening = pending_add(ctx)
    remember(ctx)
    ctx.executor._retry_emergency_exits()
    initial, = ctx.calls

    # A second valid explicit request for the same owned holding passes the
    # real request merger. It is not a new BUY or a synthetic successor dict.
    successor_bar = REQUEST_BAR + pd.Timedelta(seconds=5)
    ctx.executor._remember_emergency_exit(
        OWNER, CONID, successor_bar, "second explicit close request")
    ctx.retry_safe_ids.add(initial["client_intent_id"])
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert len(ctx.calls) == 2
    current = ctx.calls[-1]
    assert current["client_intent_id"] != initial["client_intent_id"]
    assert current["quantity"] == 40
    observe(ctx, current["client_intent_id"], filled=40, quantity=40, status="Filled")
    ctx.executor._retry_emergency_exits()
    restored, = ctx.executor.intents.all(kind="CLOSE")
    assert restored["payload"]["reason"] == "second explicit close request"
    assert pd.Timestamp(restored["payload"]["bar_ts"]) == successor_bar.tz_localize("UTC")
    assert ctx.executor.state.open_position(OWNER, CONID) is None

    manager = restart(ctx)
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    manager.manage_positions()
    assert manager.state.open_position(OWNER, CONID) is None
    assert ctx.sdk.broker[CONID] == 100
    assert ctx.sdk.propose_calls[-1]["quantity"] == 20
    assert len(ctx.calls) == 2
    assert ctx.faults == []


def test_legacy_entry_bound_request_retains_binding_after_emergency_restore(retry_owner, monkeypatch):
    ctx = retry_owner
    addition, _opening = pending_add(ctx)
    # Explicitly supported legacy WAITING input: exact entry binding exists,
    # but the later explicit-scope schema and timer-policy key do not. The
    # actual coordinator hands it to emergency recovery when its read fails.
    request = ctx.executor.intents.create(OWNER, CONID, "CLOSE", dict(
        bar_ts=timestamp_text(REQUEST_BAR), quantity=40, reason="legacy entry-bound close",
        request_entry_bar_ts=timestamp_text(TS), exit_request_active=True), status="WAITING")

    def unavailable_journal(*args, **kwargs):
        raise OSError("local journal unavailable during legacy close coordination")

    with monkeypatch.context() as outage:
        outage.setattr(ctx.executor.intents.journal, "transaction", unavailable_journal)
        ctx.executor._advance_close(request)
    initial, = ctx.calls
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)
    observe(ctx, initial["client_intent_id"], filled=40, quantity=40, status="Filled")
    ctx.executor._retry_emergency_exits()
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 20
    assert ctx.sdk.broker[CONID] == 120
    manager = restart(ctx)
    manager.manage_positions()
    assert manager.state.open_position(OWNER, CONID)["quantity"] == 20
    assert ctx.sdk.broker[CONID] == 120
    assert [proposal["action"] for proposal in ctx.sdk.propose_calls] == ["BUY", "BUY"]
    assert len(ctx.calls) == 1


@pytest.mark.parametrize("first_state", ["already_flat", "future_view", "stale_attempt"])
def test_unavailable_or_superseded_owner_does_not_block_other_owned_exit(
        retry_owner, monkeypatch, first_state):
    ctx = retry_owner
    other = "z_other_owner"
    ctx.executor._process_signal(make_work(strategy_name=other, quantity=30))
    if first_state == "already_flat":
        remember(ctx, timer=True)
        ctx.executor._execute_close_durable(OWNER, CONID, REQUEST_BAR, 40,
                                            "healthy close completed", entry_bar_ts=TS)
        assert ctx.executor.state.open_position(OWNER, CONID) is None
    elif first_state == "future_view":
        addition, opening = pending_add(ctx)
        addition.update(filled=20, status="Filled")
        ctx.sdk.broker[CONID] += 20
        ctx.executor._snapshot_cache = None

        def unavailable_view():
            raise OSError("owned view publication unavailable")

        def unavailable_claim(*args, **kwargs):
            raise OSError("close claim journal unavailable")

        with monkeypatch.context() as fault:
            fault.setattr(ctx.executor, "_load_open_view", unavailable_view)
            with pytest.raises(OSError, match="view publication"):
                ctx.executor._reconcile_intent(opening)
            fault.setattr(ctx.executor.intents, "create", unavailable_claim)
            ctx.executor._execute_close(
                OWNER, CONID, REQUEST_BAR, 60, "newer validated timer",
                entry_bar_ts=TS + pd.Timedelta(seconds=10), explicit=False)
        ctx.faults.clear()
    else:
        addition, _opening = pending_add(ctx)
        remember(ctx, timer=True)
        ctx.executor._retry_emergency_exits()
        initial, = ctx.calls
        addition.update(filled=20, status="Filled")
        ctx.sdk.broker[CONID] += 20
        ctx.executor._snapshot_cache = None
        ctx.executor._reconcile_intents(OWNER, CONID)
        ctx.retry_safe_ids.add(initial["client_intent_id"])

    before = len(ctx.calls)
    remember(ctx, owner=other)
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert [(call["strategy_name"], call["quantity"]) for call in ctx.calls[before:]] == [(other, 30)]
    assert ctx.executor.state.open_position(other, CONID)["quantity"] == 30
    assert ctx.faults == []


@pytest.mark.parametrize("recovered_receipt", [False, True])
def test_legacy_successor_rotation_preserves_its_concrete_entry(
        retry_owner, monkeypatch, recovered_receipt):
    ctx = retry_owner
    earlier, _earlier_intent = pending_add(ctx)
    ctx.sdk.cancel(earlier["orderId"])
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)
    # The earlier add is terminal0, so a second add can be admitted before any
    # exit exists. Both later cumulative observations stay within their20.
    ctx.sdk.fill_next = 0
    ctx.executor._process_signal(make_work(
        quantity=20, pyramid_max_adds=2, bar_ts=TS + pd.Timedelta(seconds=15)))
    ctx.sdk.fill_next = None
    later = ctx.sdk.accepted[-1]
    assert later["orderId"] != earlier["orderId"]
    request = ctx.executor.intents.create(OWNER, CONID, "CLOSE", dict(
        bar_ts=timestamp_text(REQUEST_BAR), quantity=40, reason="legacy entry-bound close",
        request_entry_bar_ts=timestamp_text(TS), exit_request_active=True), status="WAITING")

    def unavailable_journal(*args, **kwargs):
        raise OSError("legacy close journal read unavailable")

    with monkeypatch.context() as outage:
        outage.setattr(ctx.executor.intents.journal, "transaction", unavailable_journal)
        ctx.executor._advance_close(request)
    initial, = ctx.calls
    earlier.update(filled=20, status="Cancelled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60

    # A valid request for the now-current earlier-add entry is handed to the
    # real merger. The old legacy request had no explicit-scope schema; its
    # successor therefore needs its concrete entry even with policy=None.
    ctx.executor._remember_emergency_exit(
        OWNER, CONID, REQUEST_BAR + pd.Timedelta(seconds=5), "later validated timer",
        entry_bar_ts=TS + pd.Timedelta(seconds=10), capture_explicit=False)
    ctx.retry_safe_ids.add(initial["client_intent_id"])
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert len(ctx.calls) == 2
    current = ctx.calls[-1]
    assert current["quantity"] == 60
    later.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)

    if recovered_receipt:
        observe(ctx, current["client_intent_id"], filled=60, quantity=60, status="Filled")
        ctx.executor._retry_emergency_exits()
        assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 20
        manager = restart(ctx)
        manager.manage_positions()
        assert manager.state.open_position(OWNER, CONID)["quantity"] == 20
        assert ctx.sdk.broker[CONID] == 120
    else:
        # This alternative outcome is proven unsent; it is not later combined
        # with a contradictory fill. The stale timer cannot retry for entry C.
        ctx.retry_safe_ids.add(current["client_intent_id"])
        ctx.executor._snapshot_cache = None
        ctx.executor._retry_emergency_exits()
        assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 80
        assert ctx.sdk.broker[CONID] == 180
    assert len(ctx.calls) == 2
    assert [proposal["action"] for proposal in ctx.sdk.propose_calls] == ["BUY", "BUY", "BUY"]
    assert ctx.faults == []


def test_legacy_concrete_request_waits_for_lagging_view_then_sends(retry_owner, monkeypatch):
    ctx = retry_owner
    addition, opening = pending_add(ctx)
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None

    def unavailable_view():
        raise OSError("owned view publication unavailable")

    with monkeypatch.context() as fault:
        fault.setattr(ctx.executor, "_load_open_view", unavailable_view)
        with pytest.raises(OSError, match="view publication"):
            ctx.executor._reconcile_intent(opening)
    request = ctx.executor.intents.create(OWNER, CONID, "CLOSE", dict(
        bar_ts=timestamp_text(REQUEST_BAR), quantity=60, reason="legacy newer entry request",
        request_entry_bar_ts=timestamp_text(TS + pd.Timedelta(seconds=10)),
        exit_request_active=True), status="WAITING")

    def unavailable_journal(*args, **kwargs):
        raise OSError("legacy request journal read unavailable")

    with monkeypatch.context() as outage:
        outage.setattr(ctx.executor.intents.journal, "transaction", unavailable_journal)
        ctx.executor._advance_close(request)
    assert ctx.calls == []
    ctx.executor._load_open_view()
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert [call["quantity"] for call in ctx.calls] == [60]
    assert ctx.sdk.broker[CONID] == 160
    assert ctx.faults == []


def test_first_restore_retains_scope_for_native_entry_bound_successor(retry_owner, monkeypatch):
    ctx = retry_owner
    addition, _opening = pending_add(ctx)
    remember(ctx, timer=True)
    ctx.executor._retry_emergency_exits()
    first, = ctx.calls

    # The real mailbox merger captures the pending add and gives the explicit
    # successor a concrete current-entry binding. No request dict is invented.
    ctx.executor._remember_emergency_exit(
        OWNER, CONID, REQUEST_BAR + pd.Timedelta(seconds=5), "explicit successor close")
    # The earlier uncertain attempt is proven unsent. Before its replacement
    # could send, native cancellation can confirm terminal zero on the BUY.
    # Its later partial execution below is a corrected cumulative observation.
    ctx.sdk.cancel_fails = False
    ctx.sdk.cancel(addition["orderId"])
    assert addition["status"] == "Cancelled" and addition["filled"] == 0
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)
    ctx.retry_safe_ids.add(first["client_intent_id"])
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert [call["quantity"] for call in ctx.calls] == [40, 40]
    replacement = ctx.calls[-1]
    assert replacement["client_intent_id"] != first["client_intent_id"]
    receipt = observe(ctx, replacement["client_intent_id"],
                      filled=0, quantity=40, status="Submitted")

    def unavailable_scope_write(*args, **kwargs):
        raise OSError("followup scope write unavailable after first successor insert")

    with monkeypatch.context() as fault:
        fault.setattr(ctx.executor.intents, "retain_exit_scope", unavailable_scope_write)
        ctx.executor._retry_emergency_exits()
    restored, = ctx.executor.intents.all(kind="CLOSE")
    assert restored["intent_id"] == replacement["client_intent_id"]
    assert restored["payload"]["request_entry_bar_ts"] == timestamp_text(TS)

    manager = restart(ctx)
    # The corrected BUY changes the holding's latest entry before the first
    # reduction finishes. Its captured authority must survive the crash; an
    # older concrete entry alone cannot close this twenty-share residual.
    addition.update(filled=20, status="Cancelled")
    ctx.sdk.broker[CONID] += 20
    receipt.update(filled=40, status="Filled")
    ctx.sdk.broker[CONID] -= 40
    manager.manage_positions()
    manager.manage_positions()
    assert manager.state.open_position(OWNER, CONID) is None
    assert ctx.sdk.broker[CONID] == 100
    assert ctx.sdk.propose_calls[-1]["action"] == "SELL"
    assert ctx.sdk.propose_calls[-1]["quantity"] == 20
    assert len(ctx.calls) == 2
    assert ctx.faults == []
