"""A conclusively unsent close cannot block a later owned exit.

A native stale close can finish before any physical submission. That known
history must remain distinct from a legacy order with missing ownership proof.
"""
from test_execution_recovery import recovery
import pandas as pd
import pytest

from review.test_review_strategy_contract import TS, make_work
from test_emergency_retry_restoration_contract import (
    CONID, OWNER, REQUEST_BAR, pending_add, retry_owner,
)
from trader.objects import Action
from trader.strategy.execution_intents import timestamp_text


@pytest.mark.parametrize("legacy_physical_history", [False, True])
def test_never_sent_resolved_close_does_not_poison_a_later_owned_exit(
        retry_owner, monkeypatch, legacy_physical_history):
    ctx = retry_owner
    addition, _opening = pending_add(ctx)
    cancel = ctx.sdk.cancel

    def fill_add_during_cancel(order_id):
        result = cancel(order_id)
        if order_id == addition["orderId"]:
            addition.update(filled=20, status="Cancelled")
            ctx.sdk.broker[CONID] += 20
        return result

    # A real current WAITING timer is superseded by the already-submitted
    # add's late fill while it cancels the opening, before it can propose SELL.
    with monkeypatch.context() as race:
        race.setattr(ctx.sdk, "cancel", fill_add_during_cancel)
        ctx.executor._execute_close_durable(
            OWNER, CONID, REQUEST_BAR, 40, "old entry timer", entry_bar_ts=TS)
    prior, = ctx.executor.intents.all(kind="CLOSE")
    assert prior["status"] == "RESOLVED"
    assert not any(prior["payload"].get(field) for field in
                   ("proposal_id", "order_ids", "submitted_at", "ownership_epoch"))
    assert [proposal["action"] for proposal in ctx.sdk.propose_calls] == ["BUY", "BUY"]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60
    ctx.executor._finish_close_request(prior)
    assert prior["payload"]["exit_request_active"] is False

    if legacy_physical_history:
        # Explicitly persisted legacy physical history: a broker ID/proposal
        # exists, but no holding UUID proves which ownership its receipt used.
        # Its absence from bounded current history cannot prove no execution.
        ctx.executor.intents.create(OWNER, CONID, "CLOSE", dict(
            bar_ts=timestamp_text(TS), reason="legacy possibly sent close",
            quantity=60, proposal_id=999, order_ids=[9888], submitted_at=1.0),
            status="CANCELLED")
        ctx.executor._load_open_view()

    def unavailable_journal(*args, **kwargs):
        raise OSError("temporary local execution journal outage")

    # Fault the actual journal transaction boundary, retaining the already
    # committed native state/cache. Higher-level scope/retry decisions run.
    with monkeypatch.context() as outage:
        outage.setattr(ctx.executor.intents.journal, "transaction", unavailable_journal)
        ctx.executor._process_signal(make_work(
            action=Action.SELL, bar_ts=REQUEST_BAR + pd.Timedelta(seconds=20)))

    assert [call["quantity"] for call in ctx.calls] == ([] if legacy_physical_history else [60])
    assert ctx.sdk.broker[CONID] == 160
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60


@pytest.mark.parametrize("retirement", ["stale_before_retry", "protective_fill", "broker_flat"])
def test_unsent_retirement_preserves_later_cached_exit_progress(
        retry_owner, monkeypatch, retirement):
    """Real local request retirement must not poison a later valid exit.

    LifecycleSDK supplies explicit broker observations and cancellation
    outcomes; this exercises real stores and reducers, not native IB timing.
    """
    ctx = retry_owner
    if retirement == "stale_before_retry":
        addition, opening = pending_add(ctx)
        # Cancellation uncertainty leaves a real, still-unsent timer WAITING.
        with monkeypatch.context() as pending_cancel:
            pending_cancel.setattr(ctx.sdk, "cancel_fails", True)
            ctx.executor._execute_close_durable(
                OWNER, CONID, REQUEST_BAR, 40, "old entry timer", entry_bar_ts=TS)
        prior, = ctx.executor.intents.all(kind="CLOSE")
        assert prior["status"] == "WAITING"

        # The already-accepted add completes before the next worker attempt.
        addition.update(filled=20, status="Filled")
        ctx.sdk.broker[CONID] += 20
        ctx.executor._snapshot_cache = None
        ctx.executor._reconcile_intents(OWNER, CONID)
        observed_opening = next(item for item in ctx.executor.intents.all(kind="OPEN")
                                if item["intent_id"] == opening["intent_id"])
        assert observed_opening["status"] == "FILLED"
        ctx.executor._advance_close(prior)
        expected_quantity = 60
    elif retirement == "protective_fill":
        monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "8")
        ctx.executor._ensure_protective(OWNER, CONID)
        protective, = ctx.executor.intents.all(kind="PROTECTIVE", active=True)
        stop_id, = protective["payload"]["order_ids"]
        stop_row, = [row for row in ctx.sdk.trades().to_dict("records")
                     if row["orderId"] == stop_id]
        cancel = ctx.sdk.cancel

        def fill_before_cancel_confirmation(order_id):
            result = cancel(order_id)
            if order_id == stop_id:
                # The receipt belongs to the protective actually placed by
                # _ensure_protective; a cancellation reply cannot erase it.
                terminal = dict(stop_row, filled=40, status="Filled",
                                fillQuantityKnown=True, avgFillPrice=92)
                # The later fill replaces the cancelled zero-fill snapshot
                # for this physical order; do not publish both as current.
                ctx.sdk.cancelled_stops.pop(stop_id, None)
                ctx.sdk.accepted.append(terminal)
                ctx.sdk.broker[CONID] -= 40
            return result

        with monkeypatch.context() as cancellation_race:
            cancellation_race.setattr(ctx.sdk, "cancel", fill_before_cancel_confirmation)
            ctx.executor._execute_close_durable(
                OWNER, CONID, REQUEST_BAR, 40, "close protected holding", entry_bar_ts=TS)
        prior, = ctx.executor.intents.all(kind="CLOSE")
        observed_protective = next(item for item in ctx.executor.intents.all(kind="PROTECTIVE")
                                   if item["intent_id"] == protective["intent_id"])
        assert observed_protective["status"] == "FILLED"
        assert ctx.executor.state.open_position(OWNER, CONID) is None
        assert ctx.sdk.broker[CONID] == 100
        expected_quantity = 30
    else:
        # An external account close is reported by a complete position read;
        # no strategy SELL has been submitted for this pending request.
        ctx.sdk.broker[CONID] = 0
        ctx.executor._snapshot_cache = None
        ctx.executor._execute_close_durable(
            OWNER, CONID, REQUEST_BAR, 40, "broker holding is flat", entry_bar_ts=TS)
        prior, = ctx.executor.intents.all(kind="CLOSE")
        assert ctx.executor.state.open_position(OWNER, CONID) is None
        assert ctx.sdk.broker[CONID] == 0
        expected_quantity = 30

    assert prior["status"] == "RESOLVED"
    assert not any(prior["payload"].get(field) for field in
                   ("proposal_id", "order_ids", "submitted_at", "ownership_epoch"))
    ctx.executor._finish_close_request(prior)
    assert prior["payload"]["exit_request_active"] is False
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    if retirement != "stale_before_retry":
        # After the account-flat case, a separate manual holding is restored
        # before a new strategy entry. The old timer has no authority over it.
        ctx.sdk.broker[CONID] = 100
        ctx.executor._process_signal(make_work(
            quantity=expected_quantity, bar_ts=REQUEST_BAR + pd.Timedelta(seconds=20)))
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == expected_quantity
    assert all(proposal["action"] == "BUY" for proposal in ctx.sdk.propose_calls)

    def unavailable_journal(*args, **kwargs):
        raise OSError("temporary local execution journal outage")

    # The observable contract is progress of the later, independently valid
    # request. No assertion depends on the internal never_submitted flag.
    with monkeypatch.context() as outage:
        outage.setattr(ctx.executor.intents.journal, "transaction", unavailable_journal)
        ctx.executor._process_signal(make_work(
            action=Action.SELL, bar_ts=REQUEST_BAR + pd.Timedelta(seconds=40)))

    assert [call["quantity"] for call in ctx.calls] == [expected_quantity]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == expected_quantity
    assert ctx.sdk.broker[CONID] == 100 + expected_quantity
    assert all(proposal["action"] == "BUY" for proposal in ctx.sdk.propose_calls)
