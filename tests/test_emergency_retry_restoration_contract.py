"""Emergency receipt restoration preserves attributed inventory and request lifetime.

These are real local state/intent consumers with a small explicit-snapshot
adapter over LifecycleSDK. The adapter is not evidence of native IB timing.
"""
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from review.test_review_strategy_contract import FakeResult, TS, make_work
from test_execution_recovery import recovery
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutor


OWNER, CONID = "orb_test", 1111
REQUEST_BAR = TS + pd.Timedelta(seconds=20)


@pytest.fixture
def retry_owner(request, recovery, monkeypatch):
    executor, sdk, path = recovery
    reopened = []
    try:
        monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
        executor.cooldown_seconds = 0
        sdk.broker[CONID] = 100
        # The one indirect case starts with a real partially filled larger BUY.
        sdk.fill_next = 40
        executor._process_signal(make_work(quantity=getattr(request, "param", 40)))
        sdk.fill_next = None
        calls, faults = [], []
        retry_safe_ids = set()

        def snapshot(intent_id="", order_ids=None):
            rows = sdk.trades().to_dict("records")
            if intent_id:
                rows = [row for row in rows if row.get("clientIntentId") == intent_id]
            elif order_ids:
                rows = [row for row in rows if row["orderId"] in order_ids]
            safe = bool(intent_id in retry_safe_ids and not rows)
            return dict(complete=bool(not intent_id or rows or safe), orders=rows,
                        positions_complete=True, positions=sdk.positions().to_dict("records"),
                        retry_safe=safe)

        def unknown_attempt(**kwargs):
            # Recording an uncertain attempt does not create a fake broker fill.
            calls.append(dict(kwargs))
            return FakeResult(ok=False, error="UNKNOWN: reply unavailable")

        sdk.execution_snapshot = snapshot
        sdk.emergency_close_position = unknown_attempt
        monkeypatch.setattr("trader.strategy.auto_executor.logging.exception",
                            lambda message, *args, **kwargs: faults.append((message, sys.exc_info()[1])))
        executor._snapshot_cache = None
        context = SimpleNamespace(executor=executor, sdk=sdk, path=path, calls=calls,
                                  faults=faults, reopened=reopened, retry_safe_ids=retry_safe_ids)
        yield context
    finally:
        for manager in reopened:
            manager.intents.journal.close()
        executor.intents.journal.close()


def remember(ctx, *, owner=OWNER, timer=False):
    ctx.executor._remember_emergency_exit(
        owner, CONID, REQUEST_BAR, "reviewed exit request",
        entry_bar_ts=TS if timer else None, capture_explicit=not timer)


def observe(ctx, identity, *, filled, quantity, status):
    row = dict(orderId=1999, orderRef=OWNER + "|mmr:" + identity,
               clientIntentId=identity, conId=CONID, action="SELL", orderType="LMT",
               totalQuantity=quantity, filled=filled, avgFillPrice=100, status=status,
               fillQuantityKnown=True)
    ctx.sdk.accepted.append(row)
    ctx.sdk.broker[CONID] -= filled
    ctx.executor._snapshot_cache = None
    return row


def restart(ctx):
    previous = ctx.reopened[-1] if ctx.reopened else ctx.executor
    previous.intents.journal.close()
    manager = AutoExecutor(ctx.path, paper_trading=True, cooldown_seconds=0,
                           sdk_factory=lambda: ctx.sdk)
    ctx.reopened.append(manager)
    return manager


def pending_add(ctx, quantity=20):
    ctx.sdk.fill_next = 0
    ctx.executor._process_signal(make_work(quantity=quantity, pyramid_max_adds=1,
                                           bar_ts=TS + pd.Timedelta(seconds=10)))
    ctx.sdk.fill_next = None
    row = ctx.sdk.accepted[-1]
    intent = next(item for item in ctx.executor.intents.all(kind="OPEN")
                  if item["intent_id"] == row["clientIntentId"])
    return row, intent


def test_direct_retry_restores_partial_fill_then_releases_fulfilled_admission(retry_owner):
    ctx = retry_owner
    remember(ctx, timer=True)
    ctx.executor._retry_emergency_exits()
    initial, = ctx.calls
    observe(ctx, initial["client_intent_id"], filled=10, quantity=40, status="Cancelled")

    # Exercise retry itself before management's broader discovery can repair it.
    ctx.executor._retry_emergency_exits()

    assert ctx.executor.state.open_position(OWNER, CONID) is None
    assert ctx.sdk.broker[CONID] == 100
    assert ctx.sdk.propose_calls[-1]["quantity"] == 30
    restored = next(item for item in ctx.executor.intents.all(kind="CLOSE")
                    if item["intent_id"] == initial["client_intent_id"])
    assert restored["payload"]["reason"] == "reviewed exit request"
    assert pd.Timestamp(restored["payload"]["bar_ts"]) == REQUEST_BAR.tz_localize("UTC")
    assert restored["payload"]["cumulative_filled"] == 10
    assert len(ctx.calls) == 1

    # A completed emergency must not strand later legitimate BUY admission.
    ctx.executor._process_signal(make_work(quantity=40, bar_ts=TS + pd.Timedelta(seconds=40)))
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert len(ctx.calls) == 1
    assert ctx.faults == []


@pytest.mark.parametrize("retry_owner", [60], indirect=True)
def test_filled_timer_attempt_keeps_same_entry_late_open_residual(retry_owner):
    ctx = retry_owner
    remember(ctx, timer=True)
    ctx.executor._retry_emergency_exits()
    initial, = ctx.calls
    assert initial["quantity"] == 40
    opening = ctx.sdk.accepted[0]
    opening.update(filled=60, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60
    observe(ctx, initial["client_intent_id"], filled=40, quantity=40, status="Filled")

    ctx.executor._retry_emergency_exits()

    assert ctx.executor.state.open_position(OWNER, CONID) is None
    assert ctx.sdk.broker[CONID] == 100
    assert ctx.sdk.propose_calls[-1]["quantity"] == 20
    assert len(ctx.calls) == 1
    assert ctx.faults == []


def test_retry_merges_new_explicit_scope_into_an_already_restored_timer(retry_owner, monkeypatch):
    ctx = retry_owner
    addition, _opening = pending_add(ctx)
    remember(ctx, timer=True)
    ctx.executor._retry_emergency_exits()
    initial, = ctx.calls
    receipt = observe(ctx, initial["client_intent_id"], filled=0, quantity=40, status="Submitted")
    ctx.executor._retry_emergency_exits()

    def unavailable_scope_write(*args, **kwargs):
        raise OSError("scope journal write temporarily unavailable")

    # The earlier broker attempt already exists locally. A later real SELL
    # must retain its broader opening authority even if this write fails.
    with monkeypatch.context() as fault:
        fault.setattr(ctx.executor.intents, "retain_exit_scope", unavailable_scope_write)
        ctx.executor._process_signal(make_work(action=Action.SELL, bar_ts=REQUEST_BAR))
    assert len(ctx.faults) == 1 and isinstance(ctx.faults[0][1], OSError)
    ctx.faults.clear()
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert len(ctx.calls) == 1

    receipt.update(filled=40, status="Filled")
    ctx.sdk.broker[CONID] -= 40
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert ctx.executor.state.open_position(OWNER, CONID) is None

    # The previously captured OPEN completes after the first holding is flat.
    manager = restart(ctx)
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    manager.manage_positions()
    assert manager.state.open_position(OWNER, CONID) is None
    assert ctx.sdk.broker[CONID] == 100
    assert ctx.sdk.propose_calls[-1]["quantity"] == 20
    manager.manage_positions()
    assert len(ctx.calls) == 1
    assert len([row for row in ctx.sdk.accepted if row["action"] == "SELL"]) == 2
    assert ctx.faults == []


@pytest.mark.parametrize("first_state", ["unknown_ack", "observed_working",
                                         "restore_unavailable", "legacy_lookup_missing"])
def test_one_unresolved_request_does_not_starve_another_owner(retry_owner, monkeypatch, first_state):
    ctx = retry_owner
    other = "z_other_owner"
    ctx.executor._process_signal(make_work(strategy_name=other, quantity=30))
    # Both rows are actual published ownership. Fix only their traversal order
    # so a broadened owner match cannot hide behind database row ordering.
    with ctx.executor._view_lock:
        ctx.executor._managed_view.sort(key=lambda row: row["strategy_name"])
    remember(ctx)
    ctx.executor._retry_emergency_exits()
    initial, = ctx.calls
    if first_state in ("observed_working", "restore_unavailable"):
        observe(ctx, initial["client_intent_id"], filled=0, quantity=40, status="Submitted")
    if first_state == "restore_unavailable":
        def unavailable_restore(*args, **kwargs):
            raise OSError("local restore journal unavailable")
        monkeypatch.setattr(ctx.executor.intents, "restore_exit", unavailable_restore)
    if first_state == "legacy_lookup_missing":
        # The supported historical DataFrame adapter cannot prove an old
        # attempt unsent, but a new independent owned request can still run.
        monkeypatch.delattr(ctx.sdk, "execution_snapshot")
    remember(ctx, owner=other)
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert [(call["strategy_name"], call["quantity"]) for call in ctx.calls] == [
        (OWNER, 40), (other, 30)]
    assert ctx.sdk.broker[CONID] == 170
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.executor.state.open_position(other, CONID)["quantity"] == 30
    assert ctx.faults == []


def test_newer_validated_timer_waits_for_failed_view_publication_then_progresses(retry_owner, monkeypatch):
    ctx = retry_owner
    addition, opening = pending_add(ctx)
    later_entry = TS + pd.Timedelta(seconds=10)
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
        # The fill transaction committed; only its old cached view remains.
        assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60
        assert ctx.executor.managed_positions()[0]["quantity"] == 40
        fault.setattr(ctx.executor.intents, "create", unavailable_claim)
        ctx.executor._execute_close(OWNER, CONID, REQUEST_BAR, 60,
                                    "validated later timer", entry_bar_ts=later_entry, explicit=False)
    assert ctx.calls == []
    assert len(ctx.faults) == 1 and isinstance(ctx.faults[0][1], OSError)
    ctx.faults.clear()

    ctx.executor._load_open_view()
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    call, = ctx.calls
    assert call["quantity"] == 60
    assert ctx.sdk.broker[CONID] == 160
    assert ctx.faults == []


def test_stale_unattempted_timer_cannot_exit_or_block_a_later_valid_add(retry_owner):
    ctx = retry_owner
    ctx.executor._process_signal(make_work(quantity=20, pyramid_max_adds=1,
                                           bar_ts=TS + pd.Timedelta(seconds=10)))
    # This request carries the actual earlier entry of a delayed timer.
    remember(ctx, timer=True)
    ctx.executor._retry_emergency_exits()
    assert ctx.calls == []

    ctx.executor._process_signal(make_work(quantity=10, pyramid_max_adds=2,
                                           bar_ts=TS + pd.Timedelta(seconds=40)))
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 70
    assert ctx.sdk.broker[CONID] == 170
    assert ctx.calls == []
    assert ctx.faults == []
