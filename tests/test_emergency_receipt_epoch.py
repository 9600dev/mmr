"""Emergency request authority and a physical order's holding UUID are separate."""
import hashlib
import sys
import time
from types import SimpleNamespace

import pandas as pd
import pytest

from review.test_review_strategy_contract import FakeResult, TS, make_work
from test_execution_recovery import recovery
from trader.strategy.auto_executor import AutoExecutor, AutoExecutionError
from trader.strategy.execution_intents import timestamp_text
from trader.trading.order_reference import split_order_reference


OWNER, CONID = "orb_test", 1111


@pytest.fixture
def emergency(recovery, monkeypatch):
    executor, sdk, path = recovery
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    executor.cooldown_seconds = 0
    sdk.broker[CONID] = 100
    executor._process_signal(make_work(quantity=40))
    calls, faults = [], []
    retry_safe = [False]

    def snapshot(intent_id="", order_ids=None):
        rows = sdk.trades().to_dict("records")
        if intent_id:
            rows = [row for row in rows if row.get("clientIntentId") == intent_id]
        elif order_ids:
            rows = [row for row in rows if row["orderId"] in order_ids]
        return dict(complete=True, positions_complete=True, orders=rows,
                    positions=sdk.positions().to_dict("records"),
                    retry_safe=bool(intent_id and not rows and retry_safe[0]))

    def attempt(**kwargs):
        # This fake deliberately records a wire attempt with an unknown
        # outcome; it neither fabricates a broker fill nor proves no send.
        calls.append(dict(kwargs))
        return FakeResult(ok=False, error="UNKNOWN: simulated lost acknowledgment")

    sdk.execution_snapshot = snapshot
    sdk.emergency_close_position = attempt
    monkeypatch.setattr("trader.strategy.auto_executor.logging.exception",
                        lambda message, *args, **kwargs: faults.append((message, sys.exc_info()[1])))
    executor._snapshot_cache = None
    return SimpleNamespace(executor=executor, sdk=sdk, path=path, calls=calls,
                           faults=faults, retry_safe=retry_safe)


def remember(ctx, *, parent=None):
    ctx.executor._remember_emergency_exit(
        OWNER, CONID, TS + pd.Timedelta(seconds=20), "explicit exit awaiting recovery",
        parent_intent_id=parent)


def encoded_child(epoch, *, suffix=None):
    # Build the independent wire fixture directly from the declared grammar.
    base = f"emergency-{int(time.time() * 1000):x}-012345abcdef"
    return base + ("-epoch-" + epoch if suffix is None else suffix)


def broker_receipt(ctx, identity, *, filled=10, quantity=40, status="Cancelled"):
    row = dict(orderId=1999, orderRef=OWNER + "|mmr:" + identity,
               clientIntentId=identity, conId=CONID, action="SELL", orderType="LMT",
               totalQuantity=quantity, filled=filled, avgFillPrice=100, status=status)
    ctx.sdk.accepted.append(row)
    ctx.executor._snapshot_cache = None
    return row


@pytest.mark.parametrize("parent", [None, "auto-" + "f" * 300])
def test_first_emergency_wire_attempt_carries_exact_selected_epoch(emergency, parent):
    ctx = emergency
    position = ctx.executor.state.open_position(OWNER, CONID)
    remember(ctx, parent=parent)

    ctx.executor._retry_emergency_exits()
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()

    call, = ctx.calls
    identity = call["client_intent_id"]
    assert identity.endswith("-epoch-" + position["ownership_epoch"])
    assert len(identity.encode()) <= 160
    assert split_order_reference(OWNER + "|mmr:" + identity) == (OWNER, identity)
    if parent:
        assert "-parent-" + hashlib.sha256(parent.encode()).hexdigest() in identity
    pending, = ctx.executor._emergency_exits.values()
    assert pending["ownership_epoch"] == position["ownership_epoch"]
    assert pending["attempted"] is True
    assert call["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert ctx.faults == []


@pytest.mark.parametrize("new_holding", [False, True])
def test_broker_only_child_replay_uses_encoded_epoch_not_current_holding(emergency, new_holding):
    ctx = emergency
    original = ctx.executor.state.open_position(OWNER, CONID)
    identity = encoded_child(original["ownership_epoch"])
    if new_holding:
        ctx.executor.state.record_close(OWNER, CONID, "CLOSED", "previous holding ended")
        ctx.executor.state.record_open(OWNER, CONID, 60, TS + pd.Timedelta(seconds=10), 502, None, None)
        ctx.sdk.broker[CONID] = 160  # receipt below is learned after the old holding ended
    else:
        ctx.sdk.broker[CONID] = 130
    broker_receipt(ctx, identity)

    for _ in range(2):
        restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
        restarted._reconcile_intents(OWNER, CONID)
        assert restarted.state.open_position(OWNER, CONID)["quantity"] == (60 if new_holding else 30)
        recovered, = restarted.intents.all(kind="CLOSE")
        assert recovered["intent_id"] == identity
        assert recovered["payload"]["ownership_epoch"] == original["ownership_epoch"]
        assert not recovered["payload"].get("attribution_unresolved")
        _positions, checkpoints = restarted.state.ownership_snapshot(restarted.intents.all())
        assert checkpoints[identity] == 10
    if new_holding:
        # Ignoring an old execution delta is insufficient if recovery also
        # resurrects its exit request against the unrelated current holding.
        for _ in range(2):
            restarted.manage_positions()
            current = restarted.state.open_position(OWNER, CONID)
            assert (current["quantity"] if current else 0) == 60
        assert ctx.sdk.broker[CONID] == 160
    assert ctx.calls == []
    assert len([row for row in ctx.sdk.accepted if row["action"] == "SELL"]) == 1
    assert ctx.faults == []


@pytest.mark.parametrize("suffix", ["", "-epoch-" + "a" * 31,
                                    "-epoch-" + "z" * 32, "-epoch-" + "a" * 32 + "-extra"])
def test_missing_or_malformed_broker_epoch_stays_unresolved(emergency, suffix):
    ctx = emergency
    identity = encoded_child("unused", suffix=suffix)
    broker_receipt(ctx, identity)
    ctx.sdk.broker[CONID] = 130

    for _ in range(2):
        restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
        restarted._reconcile_intents(OWNER, CONID)
        assert restarted.state.open_position(OWNER, CONID)["quantity"] == 40
        recovered, = restarted.intents.all(kind="CLOSE")
        assert recovered["payload"]["attribution_unresolved"] is True
        assert recovered["status"] == "UNKNOWN"
        _positions, checkpoints = restarted.state.ownership_snapshot(restarted.intents.all())
        assert checkpoints.get(identity, 0) == 0
    assert ctx.calls == []
    assert ctx.sdk.broker[CONID] == 130
    assert ctx.faults == []


def pending_add(ctx, quantity=60):
    ctx.sdk.fill_next = 0
    ctx.executor._process_signal(make_work(quantity=quantity, pyramid_max_adds=1,
                                          bar_ts=TS + pd.Timedelta(seconds=10)))
    return ctx.sdk.accepted[-1], ctx.executor.intents.all(kind="OPEN")[-1]


@pytest.mark.parametrize("prior_attempt", ["none", "unknown", "proven_unsent"])
def test_rebinding_request_to_later_captured_open_requires_a_new_attempt(emergency, prior_attempt):
    ctx = emergency
    first = ctx.executor.state.open_position(OWNER, CONID)
    addition, _intent = pending_add(ctx)
    remember(ctx)
    if prior_attempt != "none":
        ctx.executor._retry_emergency_exits()
        assert len(ctx.calls) == 1
        assert ctx.calls[0]["client_intent_id"].endswith("-epoch-" + first["ownership_epoch"])

    # This OPEN was captured by the original explicit exit request. It is
    # allowed to receive a new attempt once its later holding is proven.
    ctx.executor.state.record_close(OWNER, CONID, "CLOSED", "first holding ended")
    addition.update(filled=60, status="Filled")
    ctx.sdk.broker[CONID] = 160
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intents(OWNER, CONID)
    ctx.executor._load_open_view()
    current = ctx.executor.state.open_position(OWNER, CONID)
    assert current["ownership_epoch"] != first["ownership_epoch"]
    remember(ctx)  # retain a successor without rewriting an attempted identity
    ctx.retry_safe[0] = prior_attempt == "proven_unsent"
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()

    assert [call["quantity"] for call in ctx.calls] == {
        "none": [60], "unknown": [40], "proven_unsent": [40, 60],
    }[prior_attempt]
    if prior_attempt != "unknown":
        assert ctx.calls[-1]["client_intent_id"].endswith("-epoch-" + current["ownership_epoch"])
    if prior_attempt == "proven_unsent":
        assert ctx.calls[0]["client_intent_id"] != ctx.calls[1]["client_intent_id"]
    assert ctx.sdk.broker[CONID] == 160
    assert ctx.faults == []


def test_unpublished_flat_crossing_defers_until_fresh_ownership_uuid_is_read(emergency):
    ctx = emergency
    first = ctx.executor.state.open_position(OWNER, CONID)
    addition, opening = pending_add(ctx, quantity=20)
    closing = ctx.executor.intents.create(OWNER, CONID, "CLOSE", dict(
        bar_ts=timestamp_text(TS), quantity=40, ownership_epoch=first["ownership_epoch"],
        ownership_started_at=first["ownership_started_at"], order_ids=[1999]), status="WORKING")
    ctx.executor._load_open_view()
    addition.update(filled=20, status="Filled")
    broker_receipt(ctx, closing["intent_id"], filled=40, status="Filled")
    ctx.sdk.broker[CONID] = 120
    remember(ctx)

    ctx.executor._retry_emergency_exits()

    assert ctx.calls == [], "quantity arithmetic cannot establish a new holding UUID"
    assert len(ctx.faults) == 1 and isinstance(ctx.faults[0][1], AutoExecutionError)
    assert ctx.executor.state.apply_fill(closing, 40) == 40
    ctx.executor.intents.update(closing, status="FILLED", cumulative_filled=40)
    assert ctx.executor.state.apply_fill(opening, 20) == 20
    ctx.executor.intents.update(opening, status="FILLED", cumulative_filled=20)
    ctx.executor._load_open_view()
    current = ctx.executor.state.open_position(OWNER, CONID)
    assert current["ownership_epoch"] != first["ownership_epoch"]
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()

    call, = ctx.calls
    assert call["quantity"] == 20
    assert call["client_intent_id"].endswith("-epoch-" + current["ownership_epoch"])
    assert ctx.sdk.broker[CONID] == 120
