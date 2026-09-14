"""One uncertain holding must not hide independent reconciliation work.

The stores and coordinator are real. The existing lifecycle SDK supplies fake
orders and a complete historical DataFrame position snapshot; no broker or
thread is started. Compatibility declarations are used only for a second
instrument that the single-instrument fake resolver cannot open.
"""
import logging
from types import SimpleNamespace

import pandas as pd
import pytest

from review.test_review_strategy_contract import TS, make_work
from test_execution_recovery import recovery


OWNER = ("orb_test", 1111)
SECOND = ("second_position", 2222)


@pytest.fixture
def reconciler(recovery, monkeypatch):
    executor, sdk, _ = recovery
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    original_positions = sdk.positions

    def complete_positions():
        frame = original_positions()
        frame.attrs["complete"] = True
        return frame

    monkeypatch.setattr(sdk, "positions", complete_positions)
    ctx = SimpleNamespace(executor=executor, sdk=sdk)
    try:
        yield ctx
    finally:
        executor.intents.journal.close()


def open_owned(ctx, *, quantity=40):
    ctx.executor._process_signal(make_work(quantity=quantity))
    assert ctx.executor.state.open_position(*OWNER)["quantity"] == quantity


def confirm_flat(ctx, *keys):
    for _strategy, conid in keys:
        ctx.sdk.broker[conid] = 0
    ctx.executor._snapshot_cache = None
    ctx.executor._reconciled = False


def declare_second_position(ctx, monkeypatch):
    ctx.executor.state.record_open(*SECOND, 20, TS, 222, None, None)
    real_rows = ctx.executor.state.all_open
    # SQL result order is unspecified. Fix only traversal order, retaining
    # every real persisted row, so the uncertain first holding is decisive.
    monkeypatch.setattr(ctx.executor.state, "all_open", lambda: sorted(
        real_rows(), key=lambda row: 0 if row[:2] == OWNER else 1))


def pending_open(ctx, key=OWNER):
    # A durable CREATED claim before proposal/approval is already a valid
    # opening reservation. No broker receipt or simultaneous active OPEN is
    # invented; the initial OPEN is fully filled before this claim is made.
    return ctx.executor.intents.create(*key, "OPEN", dict(
        bar_ts=(TS + pd.Timedelta(seconds=15)).isoformat(), quantity=20), status="CREATED")


def assert_external_closure(ctx, key):
    assert ctx.executor.state.open_position(*key) is None
    row = ctx.executor.state.db.execute(
        "SELECT status, closed_reason FROM auto_exec_positions WHERE strategy=? AND conid=?",
        list(key), fetch="one")
    assert row[0] == "CLOSED_EXTERNALLY"
    assert isinstance(row[1], str)
    assert "broker snapshot" in row[1].casefold() and "position absent" in row[1].casefold()


def test_one_confirmed_share_remains_owned(reconciler):
    ctx = reconciler
    open_owned(ctx, quantity=1)
    ctx.executor._reconciled = False
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_once()
    assert ctx.executor.state.open_position(*OWNER)["quantity"] == 1
    assert ctx.sdk.broker[OWNER[1]] == 1
    assert ctx.sdk.cancel_calls == []


@pytest.mark.parametrize("reservation", ["matching", "other_strategy", "other_instrument", "terminal_history"])
def test_only_the_matching_pending_open_reserves_external_flat_attribution(reconciler, reservation):
    ctx = reconciler
    open_owned(ctx)
    if reservation == "matching":
        pending_open(ctx)
    elif reservation == "other_strategy":
        pending_open(ctx, ("other_strategy", OWNER[1]))
    elif reservation == "other_instrument":
        pending_open(ctx, (OWNER[0], SECOND[1]))
    # terminal_history already has the actual completed initial OPEN.
    confirm_flat(ctx, OWNER)
    ctx.executor._reconcile_once()
    if reservation == "matching":
        assert ctx.executor.state.open_position(*OWNER)["quantity"] == 40
        assert ctx.sdk.cancel_calls == []
    else:
        assert_external_closure(ctx, OWNER)
    assert ctx.sdk.broker[OWNER[1]] == 0


def test_pending_open_on_one_holding_does_not_hide_another_flat_holding(reconciler, monkeypatch):
    ctx = reconciler
    open_owned(ctx)
    pending_open(ctx)
    declare_second_position(ctx, monkeypatch)
    confirm_flat(ctx, OWNER, SECOND)
    ctx.executor._reconcile_once()
    assert ctx.executor.state.open_position(*OWNER)["quantity"] == 40
    assert_external_closure(ctx, SECOND)


def test_uncertain_protection_does_not_hide_another_flat_holding(reconciler, monkeypatch, caplog):
    ctx = reconciler
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "8")
    open_owned(ctx)
    stop_id = ctx.executor.state.open_position(*OWNER)["protective_order_id"]
    assert stop_id in ctx.sdk.active_stops
    declare_second_position(ctx, monkeypatch)
    confirm_flat(ctx, OWNER, SECOND)
    ctx.sdk.cancel_fails = True
    with caplog.at_level(logging.WARNING):
        ctx.executor._reconcile_once()
    assert ctx.executor.state.open_position(*OWNER)["quantity"] == 40
    assert stop_id in ctx.sdk.active_stops
    assert_external_closure(ctx, SECOND)
    messages = [record.getMessage() for record in caplog.records]
    assert any(str(stop_id) in message and "externally closed position" in message.casefold()
               for message in messages)


@pytest.mark.parametrize("foreign", ["owner", "instrument"])
def test_an_unresolved_protector_only_reserves_its_own_owner_and_instrument(reconciler, monkeypatch, foreign):
    ctx = reconciler
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "8")
    open_owned(ctx)
    stop_id = ctx.executor.state.open_position(*OWNER)["protective_order_id"]
    key = ("other_strategy", OWNER[1]) if foreign == "owner" else (OWNER[0], SECOND[1])
    legacy = ctx.executor.intents.create(*key, "PROTECTIVE", dict(
        bar_ts=TS.isoformat(), attribution_unresolved=True), status="UNKNOWN")
    # An unresolved migrated intent can outlive its old holding. It must
    # remain reserved without blocking an unrelated holding's cancellation.
    confirm_flat(ctx, OWNER)
    ctx.executor._reconcile_once()
    assert_external_closure(ctx, OWNER)
    assert stop_id not in ctx.sdk.active_stops
    assert stop_id in ctx.sdk.cancel_calls
    saved = next(item for item in ctx.executor.intents.all() if item["intent_id"] == legacy["intent_id"])
    assert saved["status"] == "UNKNOWN" and saved["payload"]["attribution_unresolved"] is True


def test_broker_read_failure_preserves_ownership_and_explains_the_retry(reconciler, monkeypatch, caplog):
    ctx = reconciler
    open_owned(ctx)
    reason = "position feed disconnected during refresh"

    def unavailable():
        raise OSError(reason)

    ctx.executor._reconciled = False
    with monkeypatch.context() as fault, caplog.at_level(logging.WARNING):
        fault.setattr(ctx.sdk, "positions", unavailable)
        ctx.executor._reconcile_once()
    assert ctx.executor.state.open_position(*OWNER)["quantity"] == 40
    messages = [record.getMessage() for record in caplog.records]
    assert any("reconciliation" in message.casefold() and reason in message for message in messages)
    confirm_flat(ctx, OWNER)
    ctx.executor._reconcile_once()
    assert_external_closure(ctx, OWNER)
