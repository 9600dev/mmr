"""Timer requests preserve ownership, progress and audit under read failures."""

import logging
from types import SimpleNamespace

import pandas as pd
import pytest

from review.test_review_strategy_contract import FakeResult, TS, make_work
from test_execution_recovery import recovery
from trader.objects import Action
from trader.strategy import auto_executor as execution
from trader.strategy.auto_executor import AutoExecutionError, BarWork
from trader.strategy.execution_intents import timestamp_text


OWNER = ("orb_test", 1111)
DUE = TS + pd.Timedelta(minutes=1)


def unavailable(*args, **kwargs):
    raise OSError("execution state unavailable")


@pytest.fixture
def timed(recovery, monkeypatch):
    executor, sdk, path = recovery
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    sdk.broker[OWNER[1]] = 100
    executor._process_signal(make_work(quantity=40, max_hold_bars=1))
    ctx = SimpleNamespace(executor=executor, sdk=sdk, path=path,
                          complete=True, calls=[], faults=[])

    def snapshot(intent_id="", order_ids=None):
        rows = sdk.trades().to_dict("records")
        if intent_id:
            rows = [row for row in rows if row.get("clientIntentId") == intent_id]
        elif order_ids:
            rows = [row for row in rows if row["orderId"] in order_ids]
        return dict(complete=ctx.complete, positions_complete=ctx.complete,
                    orders=rows, positions=sdk.positions().to_dict("records"),
                    retry_safe=False)

    def attempt(**kwargs):
        # Observe the request boundary only; no broker action or invented
        # fill follows from an unknown acknowledgment.
        ctx.calls.append(kwargs)
        return FakeResult(ok=False, error="UNKNOWN: acknowledgment unavailable")

    sdk.execution_snapshot = snapshot
    sdk.emergency_close_position = attempt
    monkeypatch.setattr(execution.logging, "exception", lambda *args, **kwargs: ctx.faults.append(True))
    executor._snapshot_cache = None
    return ctx


def timer(*, bound=True):
    return BarWork(*OWNER, DUE, 1, entry_bar_ts=TS if bound else None)


def test_due_timer_refreshes_native_observations_before_closing_owned_shares(timed):
    ctx = timed
    ctx.executor._snapshot_cache = {("", ()): dict(complete=False, orders=[])}
    ctx.executor._process_bar(timer())
    assert ctx.sdk.broker[OWNER[1]] == 100
    assert ctx.executor.state.open_position(*OWNER) is None
    assert [call["action"] for call in ctx.sdk.propose_calls] == ["BUY", "SELL"]
    assert ctx.calls == []
    assert ctx.faults == []


def test_already_accepted_same_bar_close_is_not_dispatched_again(timed, monkeypatch):
    ctx = timed
    ctx.sdk.fill_next = 0
    ctx.executor._process_signal(make_work(action=Action.SELL, bar_ts=DUE))
    assert ctx.executor.state.executed_for_bar(*OWNER, DUE)
    calls = []
    monkeypatch.setattr(ctx.executor, "_execute_close", lambda *args, **kwargs: calls.append((args, kwargs)))
    ctx.executor._process_bar(timer())
    assert calls == []
    assert ctx.executor.state.open_position(*OWNER)["quantity"] == 40


@pytest.mark.parametrize("failed_read", ["position", "dedup"])
@pytest.mark.parametrize("bound", [True, False])
def test_timer_outage_retains_exact_owner_reason_and_timer_authority(timed, monkeypatch, failed_read, bound):
    ctx = timed
    method = "open_position" if failed_read == "position" else "executed_for_bar"
    monkeypatch.setattr(ctx.executor.state, method, unavailable)
    ctx.executor._process_bar(timer(bound=bound))
    pending, = ctx.executor._emergency_exits.values()
    assert (pending["strategy"], pending["conid"]) == OWNER
    assert pending["bar_ts"] == timestamp_text(DUE)
    assert "max_hold_bars=1" in pending["reason"]
    assert ("local state unavailable" if failed_read == "position" else
            "dedup storage unavailable") in pending["reason"]
    assert pending["policy_entry_bar_ts"] == (timestamp_text(TS) if bound else None)
    # Compatibility timers without optional entry metadata remain timers;
    # they do not acquire a newly captured explicit-SELL opening scope.
    assert pending["explicit_exit_scope"] is None
    call, = ctx.calls
    assert (call["strategy_name"], call["con_id"], call["quantity"]) == (*OWNER, 40)
    assert ctx.sdk.broker[OWNER[1]] == 140
    assert ctx.faults == []


def test_unavailable_position_does_not_lose_timer_demand_to_an_old_acceptance_log(timed, monkeypatch):
    ctx = timed
    ctx.sdk.fill_next = 0
    ctx.executor._process_signal(make_work(action=Action.SELL, bar_ts=DUE))
    assert ctx.executor.state.executed_for_bar(*OWNER, DUE)
    ctx.executor._load_open_view()
    monkeypatch.setattr(ctx.executor.state, "open_position", unavailable)
    ctx.executor._process_bar(timer())
    pending, = ctx.executor._emergency_exits.values()
    assert pending["policy_entry_bar_ts"] == timestamp_text(TS)
    assert "local state unavailable" in pending["reason"]
    assert ctx.sdk.broker[OWNER[1]] == 140
    assert ctx.faults == []


def test_missing_cached_owner_during_storage_outage_creates_no_timer_request(recovery, monkeypatch):
    executor, sdk, _ = recovery
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    monkeypatch.setattr(executor.state, "open_position", unavailable)
    executor._process_bar(timer())
    assert executor._emergency_exits == {}
    assert sdk.propose_calls == []


def test_old_entry_timer_is_not_retained_while_both_reads_are_unavailable(timed, monkeypatch):
    ctx = timed
    later = TS + pd.Timedelta(seconds=30)
    ctx.executor.cooldown_seconds = 0
    ctx.executor._process_signal(make_work(quantity=20, max_hold_bars=1,
                                          pyramid_max_adds=1, bar_ts=later))
    ctx.executor._load_open_view()
    ctx.complete = False
    monkeypatch.setattr(ctx.executor.state, "open_position", unavailable)
    ctx.executor._process_bar(timer())
    assert ctx.executor._emergency_exits == {}
    assert ctx.calls == []
    assert ctx.sdk.broker[OWNER[1]] == 160


def test_legacy_timer_durable_attempt_does_not_acquire_explicit_sell_scope(timed):
    ctx = timed
    ctx.sdk.fill_next = 0
    ctx.executor._process_bar(timer(bound=False))
    attempt, = ctx.executor.intents.all(kind="CLOSE")
    assert attempt["payload"]["explicit_exit_scope"] is None
    assert attempt["payload"]["policy_entry_bar_ts"] is None
    assert ctx.executor.state.open_position(*OWNER)["quantity"] == 40


def test_refused_timer_preserves_readable_identity_and_durable_decision(timed, monkeypatch, caplog):
    ctx = timed
    reason = "exact contract resolution unavailable"

    def unresolved(conid):
        raise AutoExecutionError(reason)

    monkeypatch.setattr(ctx.executor, "_resolve_exact", unresolved)
    with caplog.at_level(logging.ERROR):
        ctx.executor._process_bar(timer())
    rows = ctx.executor.state.db.execute(
        "SELECT strategy, conid, bar_ts, action, decision, reason FROM auto_exec_bar_log "
        "WHERE bar_ts=?", [DUE.to_pydatetime()], fetch="all")
    assert rows == [(OWNER[0], OWNER[1], DUE.to_pydatetime(), "SELL", "refused", reason)]
    messages = [record.getMessage() for record in caplog.records]
    assert any(OWNER[0] in message and str(OWNER[1]) in message and reason in message
               for message in messages)
    assert ctx.sdk.broker[OWNER[1]] == 140
    assert ctx.calls == []
