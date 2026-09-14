"""Durable close failures preserve the exact request authority for recovery.

These are request-layer tests with real intent storage and real capture,
retention and matching helpers. A fault stops the durable coordinator before
physical submission; the emergency sender is paused to inspect its retained
request. They do not claim broker fills or a complete trading schedule.
"""
import datetime as dt
from types import SimpleNamespace

import pytest

from test_emergency_request_contract_gaps import ENTRY, LATER, OWNER, owner
from trader.strategy import auto_executor as execution
from trader.strategy.auto_executor import AutoExecutor


REQUEST = ENTRY + dt.timedelta(minutes=5)


def unavailable(*args, **kwargs):
    raise OSError("durable request journal unavailable")


@pytest.fixture
def requests(owner, monkeypatch):
    ctx = SimpleNamespace(executor=owner, retries=[], faults=[])
    monkeypatch.setattr(owner, "_execute_close_durable", unavailable)
    monkeypatch.setattr(owner, "_retry_emergency_exits", lambda: ctx.retries.append(True))
    monkeypatch.setattr(execution.logging, "exception", lambda *args, **kwargs: ctx.faults.append(True))
    try:
        yield ctx
    finally:
        owner.intents.journal.close()


def declare_position(ctx, entry, proposal):
    # This fixture declares the published position seen by request consumers;
    # actual OPEN attribution and physical epoch binding have separate tests.
    position = ctx.executor.state.open_position(*OWNER)
    position.update(entry_bar_ts=entry, proposal_id=proposal)
    ctx.executor._managed_view[0].update(position)
    return dict(position)


def parent_request(ctx, *, policy=None, scope=None, owner_key=OWNER, status="WAITING"):
    return ctx.executor.intents.create(
        *owner_key, "CLOSE",
        dict(bar_ts=ENTRY.isoformat(), quantity=40, reason="retained request",
             policy_entry_bar_ts=policy.isoformat() if policy is not None else None,
             explicit_exit_scope=scope, exit_request_active=True), status=status)


def successor(ctx, parent, entry, bar=REQUEST):
    ctx.executor.intents.retain_exit_successor(parent, dict(
        request_id="validated-timer-demand", entry_bar_ts=entry.isoformat(),
        policy_entry_bar_ts=entry.isoformat(), bar_ts=bar.isoformat(), reason="later timer"))


def pending(ctx):
    assert ctx.faults == [True]
    assert ctx.retries == [True]
    return ctx.executor._emergency_exits[OWNER]


def test_explicit_failure_retains_the_parents_broader_captured_inventory(requests):
    ctx = requests
    executor = ctx.executor
    first = dict(executor.state.open_position(*OWNER))
    parent = parent_request(ctx, scope=executor._capture_exit_scope(*OWNER))
    current = declare_position(ctx, LATER, 22)
    executor.intents.retain_exit_scope(parent, executor._capture_exit_scope(*OWNER))
    retained = parent["payload"]["explicit_exit_scope"]
    executor._execute_close(*OWNER, REQUEST, 40, "explicit retry")
    emergency = pending(ctx)
    assert emergency["parent_intent_id"] == parent["intent_id"]
    assert emergency["explicit_exit_scope"] == retained
    assert emergency["reason"] == "explicit retry"
    assert executor._close_entry_matches({"payload": emergency}, first) is True
    assert executor._close_entry_matches({"payload": emergency}, current) is True
    assert executor._close_entry_matches(
        {"payload": emergency}, {"entry_bar_ts": REQUEST, "proposal_id": 33}) is False


def test_explicit_failure_does_not_borrow_a_parent_that_never_captured_this_holding(requests):
    ctx = requests
    executor = ctx.executor
    first = dict(executor.state.open_position(*OWNER))
    parent_request(ctx, scope=executor._capture_exit_scope(*OWNER))
    current = declare_position(ctx, LATER, 22)
    executor._execute_close(*OWNER, REQUEST, 40, "new explicit SELL")
    emergency = pending(ctx)
    assert emergency["parent_intent_id"] is None
    assert executor._close_entry_matches({"payload": emergency}, first) is False
    assert executor._close_entry_matches({"payload": emergency}, current) is True


@pytest.mark.parametrize("use_successor", [False, True])
def test_timer_failure_keeps_its_exact_parent_and_entry_restriction(requests, use_successor):
    ctx = requests
    executor = ctx.executor
    entry = LATER if use_successor else ENTRY
    captured_opening = None
    if use_successor:
        # An explicit parent already owns a still-pending OPEN C. A timer B
        # fallback must retain that identity even though B is its own bound.
        captured_opening = executor.intents.create(*OWNER, "OPEN", dict(
            bar_ts=(LATER + dt.timedelta(minutes=1)).isoformat(), proposal_id=33), status="UNKNOWN")
        parent = parent_request(ctx, scope=executor._capture_exit_scope(*OWNER))
        declare_position(ctx, entry, 22)
        executor.intents.retain_exit_scope(parent, executor._capture_exit_scope(*OWNER))
        successor(ctx, parent, entry)
    else:
        parent = parent_request(ctx, policy=ENTRY)
    executor._execute_close(*OWNER, REQUEST, 40, "timer must remain bound",
                            entry_bar_ts=entry, explicit=False)
    emergency = pending(ctx)
    assert emergency["parent_intent_id"] == parent["intent_id"]
    assert emergency["reason"] == "timer must remain bound"
    assert emergency["policy_entry_bar_ts"] == entry.isoformat()
    if captured_opening is not None:
        assert emergency["explicit_exit_scope"] == parent["payload"]["explicit_exit_scope"]
        assert executor._scope_accepts_opening(emergency["explicit_exit_scope"], captured_opening) is True
        assert executor._close_entry_matches({"payload": emergency}, dict(
            entry_bar_ts=LATER + dt.timedelta(minutes=1), proposal_id=33)) is True
    else:
        assert emergency["explicit_exit_scope"] is None
    assert executor._close_entry_matches(
        {"payload": emergency}, {"entry_bar_ts": entry, "proposal_id": 22 if use_successor else 11}) is True
    assert executor._close_entry_matches({"payload": emergency}, {"entry_bar_ts": REQUEST}) is False


@pytest.mark.parametrize("other_successor", [False, True])
def test_timer_failure_never_links_a_different_entrys_request(requests, other_successor):
    ctx = requests
    executor = ctx.executor
    parent = parent_request(ctx, policy=ENTRY)
    entry = LATER
    if other_successor:
        successor(ctx, parent, LATER, REQUEST)
        entry = LATER + dt.timedelta(minutes=1)
    declare_position(ctx, entry, 33)
    # A same-bar request for another entry must not supply parent authority.
    executor._execute_close(*OWNER, REQUEST, 40, "current timer",
                            entry_bar_ts=entry, explicit=False)
    emergency = pending(ctx)
    assert emergency["parent_intent_id"] is None
    assert emergency["policy_entry_bar_ts"] == entry.isoformat()
    assert executor._close_entry_matches({"payload": emergency}, {"entry_bar_ts": ENTRY}) is False


@pytest.mark.parametrize("excluded", ["owner", "instrument", "terminal"])
def test_timer_recovery_does_not_link_foreign_or_finished_parent_requests(requests, excluded):
    ctx = requests
    owner_key = (("another-strategy", OWNER[1]) if excluded == "owner" else
                 (OWNER[0], 2222) if excluded == "instrument" else OWNER)
    parent_request(ctx, policy=ENTRY, owner_key=owner_key,
                   status="RESOLVED" if excluded == "terminal" else "WAITING")
    ctx.executor._execute_close(*OWNER, REQUEST, 40, "local timer",
                                entry_bar_ts=ENTRY, explicit=False)
    emergency = pending(ctx)
    assert emergency["parent_intent_id"] is None
    assert (emergency["strategy"], emergency["conid"]) == OWNER
    assert emergency["policy_entry_bar_ts"] == ENTRY.isoformat()


def test_legacy_unbound_timer_failure_preserves_the_request_without_fresh_capture(requests):
    ctx = requests
    executor = ctx.executor
    parent = executor.intents.create(*OWNER, "CLOSE",
        dict(bar_ts=ENTRY.isoformat(), reason="legacy unbound time exit"), status="WAITING")
    # The supported four-argument BarWork route has no entry binding. A
    # retry must preserve that existing request rather than silently replace
    # its authority with only the position visible during the journal fault.
    executor._execute_close(*OWNER, REQUEST, 40, "legacy timer", explicit=False)
    emergency = pending(ctx)
    assert emergency["parent_intent_id"] == parent["intent_id"]
    assert emergency["explicit_exit_scope"] is None
    assert emergency["policy_entry_bar_ts"] is None
    assert executor._close_entry_matches({"payload": emergency},
        {"entry_bar_ts": LATER, "proposal_id": 22}) is True
