"""Close handoffs retain exact ownership and demand across later receipts."""

from types import SimpleNamespace

import pandas as pd
import pytest

from review.test_review_strategy_contract import LifecycleSDK, TS, make_work
from trader.strategy.auto_executor import AutoExecutionError, AutoExecutor
from trader.strategy.execution_intents import timestamp_text


OWNER = ("orb_test", 1111)
ADD = TS + pd.Timedelta(seconds=30)
DUE = TS + pd.Timedelta(minutes=1)


@pytest.fixture
def handoff(tmp_path, monkeypatch):
    monkeypatch.delenv("MMR_AUTO_EXECUTE_DISABLED", raising=False)
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    monkeypatch.setattr(AutoExecutor, "_now_utc", lambda self:
                        (TS.tz_localize("UTC") + pd.Timedelta(seconds=70)).to_pydatetime())
    sdk = LifecycleSDK()
    sdk.broker[OWNER[1]] = 100  # manual inventory is never this request's property
    executor = AutoExecutor(str(tmp_path / "handoff.duckdb"), paper_trading=True,
                            cooldown_seconds=0, sdk_factory=lambda: sdk)
    try:
        executor._process_signal(make_work(quantity=40))
        yield SimpleNamespace(executor=executor, sdk=sdk)
    finally:
        executor.intents.journal.close()


def _unavailable_identity(conid):
    raise AutoExecutionError("exact contract metadata temporarily unavailable")


def _working_timer(ctx):
    ctx.sdk.fill_next = 0
    ctx.executor._execute_close_durable(*OWNER, DUE, 40, "original timer", entry_bar_ts=TS)
    attempt, = ctx.executor.intents.all(kind="CLOSE", active=True)
    assert attempt["status"] == "WORKING"
    assert ctx.sdk.accepted[-1]["filled"] == 0
    return attempt


def test_new_opening_receipt_invalidates_old_timer_before_contract_resolution(handoff, monkeypatch):
    ctx = handoff
    ctx.sdk.fill_next = 0
    ctx.executor._process_signal(make_work(quantity=20, pyramid_max_adds=1, bar_ts=ADD))
    # This fill arrives after the earlier signal work finished. The durable
    # close entry point must observe it before admitting the obsolete timer.
    ctx.sdk.accepted[-1].update(filled=20, status="Filled")
    ctx.sdk.broker[OWNER[1]] += 20
    monkeypatch.setattr(ctx.executor, "_resolve_exact", _unavailable_identity)

    ctx.executor._execute_close_durable(*OWNER, DUE, 40, "old timer", entry_bar_ts=TS)

    position = ctx.executor.state.open_position(*OWNER)
    assert position["entry_bar_ts"] == ADD
    assert position["quantity"] == 60
    assert ctx.executor.intents.all(kind="CLOSE") == []
    assert [call["action"] for call in ctx.sdk.propose_calls] == ["BUY", "BUY"]
    assert ctx.sdk.broker[OWNER[1]] == 160


@pytest.mark.parametrize("foreign_owner", [("other_strategy", 1111), ("orb_test", 2222)])
def test_owned_close_does_not_depend_on_another_positions_failing_checkpoint(
        handoff, monkeypatch, foreign_owner):
    ctx = handoff
    foreign = ctx.executor.intents.create(*foreign_owner, "OPEN", dict(
        bar_ts=timestamp_text(TS), quantity=1, proposal_id=999, order_ids=[8000]), status="WORKING")
    ctx.sdk.accepted.append(dict(orderId=8000, orderRef=foreign_owner[0], conId=foreign_owner[1],
        status="Submitted", action="BUY", totalQuantity=1, filled=0,
        avgFillPrice=100, clientIntentId=foreign["intent_id"]))
    apply_fill = ctx.executor.state.apply_fill

    def checkpoint(intent, cumulative, **evidence):
        if intent["intent_id"] == foreign["intent_id"]:
            raise OSError("unrelated ownership checkpoint unavailable")
        return apply_fill(intent, cumulative, **evidence)

    monkeypatch.setattr(ctx.executor.state, "apply_fill", checkpoint)
    ctx.executor._execute_close_durable(*OWNER, DUE, 40, "owned exit")

    assert ctx.sdk.broker[OWNER[1]] == 100
    assert ctx.executor.state.open_position(*OWNER) is None
    assert ctx.executor.intents.all(strategy=foreign_owner[0], conid=foreign_owner[1], kind="OPEN") == [foreign]
    assert [call["action"] for call in ctx.sdk.propose_calls] == ["BUY", "SELL"]


@pytest.mark.parametrize("foreign_owner", [("other_strategy", 1111), ("orb_test", 2222)])
def test_close_demand_cannot_be_attached_to_another_owners_waiting_request(handoff, foreign_owner):
    ctx = handoff
    # A request awaiting reconciliation may outlive its old position. It must
    # not become the pending-close slot for a different (strategy, conId).
    foreign = ctx.executor.intents.create(*foreign_owner, "CLOSE", dict(
        bar_ts=timestamp_text(TS), quantity=1, reason="foreign request",
        policy_entry_bar_ts=timestamp_text(TS), exit_request_active=True), status="WAITING")

    ctx.executor._execute_close_durable(*OWNER, DUE, 40, "owned exit",
                                       exit_scope=ctx.executor._capture_exit_scope(*OWNER))

    assert ctx.sdk.broker[OWNER[1]] == 100
    assert ctx.executor.state.open_position(*OWNER) is None
    assert ctx.executor.intents.all(strategy=foreign_owner[0], conid=foreign_owner[1], kind="CLOSE") == [foreign]
    assert [call["action"] for call in ctx.sdk.propose_calls] == ["BUY", "SELL"]


def test_explicit_sell_reuses_working_identity_and_persists_reason_during_metadata_outage(handoff, monkeypatch):
    ctx = handoff
    attempt = _working_timer(ctx)
    scope = ctx.executor._capture_exit_scope(*OWNER)
    monkeypatch.setattr(ctx.executor, "_resolve_exact", _unavailable_identity)

    ctx.executor._execute_close_durable(*OWNER, DUE + pd.Timedelta(seconds=1), 40,
                                       "operator requested flatten", exit_scope=scope)

    saved, = ctx.executor.intents.all(kind="CLOSE")
    assert saved["intent_id"] == attempt["intent_id"]
    assert saved["payload"]["order_ids"] == attempt["payload"]["order_ids"]
    assert saved["payload"]["explicit_exit_scope"] == scope
    assert saved["payload"]["reason"] == "operator requested flatten"
    successor = saved["payload"]["successor_exit"]
    assert successor["reason"] == "operator requested flatten"
    assert successor["entry_bar_ts"] == timestamp_text(TS)
    assert [call["action"] for call in ctx.sdk.propose_calls] == ["BUY", "SELL"]
    assert ctx.sdk.broker[OWNER[1]] == 140


def test_repeated_unsent_timer_cannot_close_a_new_entry_discovered_during_cancellation(handoff, monkeypatch):
    ctx = handoff
    ctx.sdk.fill_next = 0
    ctx.executor._process_signal(make_work(quantity=20, pyramid_max_adds=1, bar_ts=ADD))
    opening_receipt = ctx.sdk.accepted[-1]
    ctx.sdk.cancel_fails = True
    ctx.executor._execute_close_durable(*OWNER, DUE, 40, "original timer", entry_bar_ts=TS)
    attempt, = ctx.executor.intents.all(kind="CLOSE", active=True)
    assert attempt["status"] == "WAITING"
    assert not attempt["payload"].get("order_ids")
    ctx.sdk.cancel_fails = False
    ctx.sdk.fill_next = None
    cancel = ctx.sdk.cancel

    def fill_racing_cancel(order_id):
        if order_id == opening_receipt["orderId"]:
            opening_receipt.update(filled=20, status="Cancelled")
            ctx.sdk.broker[OWNER[1]] += 20
        return cancel(order_id)

    monkeypatch.setattr(ctx.sdk, "cancel", fill_racing_cancel)

    ctx.executor._execute_close_durable(*OWNER, DUE + pd.Timedelta(seconds=1), 40,
                                       "timer still due", entry_bar_ts=TS)

    position = ctx.executor.state.open_position(*OWNER)
    assert position["entry_bar_ts"] == ADD
    assert position["quantity"] == 60
    assert ctx.sdk.broker[OWNER[1]] == 160
    assert [call["action"] for call in ctx.sdk.propose_calls] == ["BUY", "BUY"]
    assert len(ctx.sdk.approve_calls) == 2


def test_explicit_handoff_retains_pending_opening_scope_for_late_fills(handoff):
    ctx = handoff
    ctx.sdk.fill_next = 0
    ctx.executor._process_signal(make_work(quantity=20, pyramid_max_adds=1, bar_ts=ADD))
    opening_receipt = ctx.sdk.accepted[-1]
    # Terminal broker evidence excludes a live BUY remainder, while its final
    # execution quantity is still unknown. The already confirmed 40 may exit.
    opening_receipt.update(status="Unknown", brokerStatus="Cancelled", fillQuantityKnown=False)
    attempt = _working_timer(ctx)
    closing_receipt = ctx.sdk.accepted[-1]
    scope = ctx.executor._capture_exit_scope(*OWNER)
    assert scope["openings"]

    ctx.executor._execute_close_durable(*OWNER, DUE + pd.Timedelta(seconds=1), 40,
                                       "explicit flatten", exit_scope=scope)

    # A correction to the old terminal BUY now establishes 20 more owned
    # shares. It does not mean that a terminal order began executing again.
    opening_receipt.update(filled=20, status="Cancelled", fillQuantityKnown=True)
    ctx.sdk.broker[OWNER[1]] += 20
    closing_receipt.update(filled=40, status="Filled")
    ctx.sdk.broker[OWNER[1]] -= 40
    ctx.sdk.fill_next = None
    ctx.executor.manage_positions()

    assert ctx.sdk.broker[OWNER[1]] == 100
    assert ctx.executor.state.open_position(*OWNER) is None
    assert [call["quantity"] for call in ctx.sdk.propose_calls if call["action"] == "SELL"] == [40, 20]
    historical = next(row for row in ctx.executor.intents.all(kind="CLOSE")
                      if row["intent_id"] == attempt["intent_id"])
    assert historical["payload"]["explicit_exit_scope"] == scope
