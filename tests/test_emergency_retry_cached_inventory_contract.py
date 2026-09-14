"""Cached retry quantities follow receipts without inventing ownership.

Local state, intent creation and admission are real. The shared explicit
snapshot adapter records broker observations; the endpoint recorder does not
fill an order. Historical DataFrame controls are labeled separately.
"""
import pandas as pd
import pytest

from review.test_review_strategy_contract import FakeResult, TS, make_work
from test_execution_recovery import recovery
from test_emergency_retry_restoration_contract import (
    CONID, OWNER, pending_add, remember, retry_owner,
)
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutionError


@pytest.mark.parametrize("retry_owner", [60], indirect=True)
def test_same_entry_unpublished_open_fill_is_included_in_timer_reduction(retry_owner):
    ctx = retry_owner
    remember(ctx, timer=True)
    ctx.sdk.accepted[0].update(filled=60, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert [call["quantity"] for call in ctx.calls] == [60]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 160
    assert ctx.faults == []


def test_cancelled_zero_later_open_does_not_supersede_current_timer(retry_owner):
    ctx = retry_owner
    addition, opening = pending_add(ctx)
    addition.update(filled=0, status="Cancelled")
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intent(opening)
    remember(ctx, timer=True)

    ctx.executor._retry_emergency_exits()

    assert [call["quantity"] for call in ctx.calls] == [40]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert ctx.faults == []


def test_one_share_later_open_retires_timer_without_blocking_other_owner_or_add(retry_owner):
    ctx = retry_owner
    other = "z_other_owner"
    ctx.executor._process_signal(make_work(strategy_name=other, quantity=30))
    addition, _opening = pending_add(ctx, quantity=1)
    remember(ctx, timer=True)
    remember(ctx, owner=other)
    addition.update(filled=1, status="Filled")
    ctx.sdk.broker[CONID] += 1
    ctx.executor._snapshot_cache = None

    # Permit an extra management pulse: the contract is progress, not an
    # arbitrary requirement that independent owners run in one pulse.
    ctx.executor._retry_emergency_exits()
    ctx.executor._retry_emergency_exits()
    assert [(call["strategy_name"], call["quantity"]) for call in ctx.calls] == [(other, 30)]

    ctx.executor._reconcile_intents(OWNER, CONID)
    ctx.executor._process_signal(make_work(quantity=5, pyramid_max_adds=2,
                                           bar_ts=TS + pd.Timedelta(seconds=40)))
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 46
    assert ctx.sdk.broker[CONID] == 176
    assert len(ctx.calls) == 1 and ctx.faults == []


@pytest.mark.parametrize("close_fill,expected", [(40, [("z_other_owner", 30)]),
                                               (39, [(OWNER, 1), ("z_other_owner", 30)])])
def test_cached_close_fill_leaves_only_real_residual_and_other_owner_progress(
        retry_owner, close_fill, expected):
    ctx = retry_owner
    other = "z_other_owner"
    ctx.executor._process_signal(make_work(strategy_name=other, quantity=30))
    ctx.sdk.fill_next = 0
    ctx.executor._process_signal(make_work(action=Action.SELL,
                                           bar_ts=TS + pd.Timedelta(seconds=10)))
    ctx.sdk.fill_next = None
    closing = ctx.sdk.accepted[-1]
    assert closing["action"] == "SELL" and closing["totalQuantity"] == 40
    closing.update(filled=close_fill, status="Filled" if close_fill == 40 else "Cancelled")
    ctx.sdk.broker[CONID] -= close_fill
    remember(ctx)
    remember(ctx, owner=other)
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()
    ctx.executor._retry_emergency_exits()

    assert [(call["strategy_name"], call["quantity"]) for call in ctx.calls] == expected
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.executor.state.open_position(other, CONID)["quantity"] == 30
    assert ctx.sdk.broker[CONID] == 170 - close_fill
    assert ctx.faults == []


def test_complete_absent_broker_holding_cannot_mint_a_one_share_attempt(retry_owner):
    ctx = retry_owner
    remember(ctx)
    ctx.sdk.broker.pop(CONID)
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert ctx.calls == []
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.faults == []


def test_missing_protective_ack_remains_a_reservation_during_cached_retry(retry_owner, monkeypatch):
    ctx = retry_owner
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "8")
    monkeypatch.setattr(ctx.sdk, "place_protective_order",
                        lambda **kwargs: FakeResult(ok=False, error="UNKNOWN: placement timeout"))
    ctx.executor._ensure_protective(OWNER, CONID)
    protective, = ctx.executor.intents.all(kind="PROTECTIVE", active=True)
    assert protective["status"] == "UNKNOWN" and not protective["payload"].get("order_ids")
    assert protective["payload"]["ownership_epoch"] is not None
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    remember(ctx)
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert ctx.calls == []
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert len(ctx.faults) == 1 and isinstance(ctx.faults[0][1], AutoExecutionError)


def test_adopted_fill_without_claim_origin_cannot_size_an_emergency(retry_owner):
    ctx = retry_owner
    # Optional native claim provenance is absent for a legacy server claim.
    # Live status proves a working remainder, not who owned its earlier fill.
    ctx.sdk.accepted.append(dict(orderId=991, clientId=7, permId=1991,
        clientIntentId="protective:missing-origin", orderRef=OWNER, conId=CONID,
        action="SELL", orderType="STP", totalQuantity=40, filled=10,
        avgFillPrice=95, status="Submitted", fillQuantityKnown=True))
    ctx.sdk.broker[CONID] -= 10
    ctx.executor._snapshot_cache = None
    ctx.executor._adopt_observed_protectives(OWNER, CONID)
    protective, = ctx.executor.intents.all(kind="PROTECTIVE", active=True)
    assert protective["payload"]["ownership_epoch"] is not None
    assert protective["payload"]["attribution_unresolved"] is True
    remember(ctx)

    ctx.executor._retry_emergency_exits()

    assert ctx.calls == []
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 130
    assert len(ctx.faults) == 1 and isinstance(ctx.faults[0][1], AutoExecutionError)

    assert all(word in str(ctx.faults[0][1]).lower() for word in ("ownership", "reconciliation"))


@pytest.mark.parametrize("history", ["foreign_owner", "rejected_open"])
def test_irrelevant_cached_history_does_not_hide_later_owned_open_delta(retry_owner, monkeypatch, history):
    ctx = retry_owner
    if history == "foreign_owner":
        ctx.executor._process_signal(make_work(strategy_name="z_other_owner", quantity=30))
    else:
        def reject_proposal(**kwargs):
            raise ValueError("proposal cannot be created")
        with monkeypatch.context() as fault:
            fault.setattr(ctx.sdk, "propose", reject_proposal)
            ctx.executor._process_signal(make_work(quantity=20, pyramid_max_adds=1,
                                                   bar_ts=TS + pd.Timedelta(seconds=10)))
        assert len(ctx.faults) == 1 and isinstance(ctx.faults[0][1], ValueError)
        ctx.faults.clear()
    ctx.sdk.fill_next = 0
    ctx.executor._process_signal(make_work(quantity=20, pyramid_max_adds=1,
                                           bar_ts=TS + pd.Timedelta(seconds=20)))
    ctx.sdk.fill_next = None
    addition = ctx.sdk.accepted[-1]
    assert addition["action"] == "BUY" and addition["filled"] == 0
    remember(ctx)
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert [call["quantity"] for call in ctx.calls] == [60]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == (190 if history == "foreign_owner" else 160)
    assert ctx.faults == []


def test_legacy_dataframe_numeric_identity_recovers_known_open_delta(retry_owner, monkeypatch):
    ctx = retry_owner
    addition, _opening = pending_add(ctx)
    remember(ctx)
    # This is the explicitly supported historical adapter, with a known
    # numeric ID but no encoded or explicit broker-intent reference.
    addition.pop("clientIntentId")
    addition["orderRef"] = OWNER
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    monkeypatch.delattr(ctx.sdk, "execution_snapshot")
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert [call["quantity"] for call in ctx.calls] == [60]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 160 and ctx.faults == []


@pytest.mark.parametrize("problem", ["ambiguous_identity", "negative_fill"])
def test_legacy_dataframe_bad_receipt_does_not_authorize_cached_exit(retry_owner, monkeypatch, problem):
    ctx = retry_owner
    remember(ctx)
    row = ctx.sdk.accepted[0]
    if problem == "ambiguous_identity":
        row["identityAmbiguous"] = True
    else:
        row["filled"] = -1
    # Native complete snapshots already reject identity ambiguity and
    # normalize quantities. The historical DataFrame adapter has separate
    # row validation despite its whole-frame completeness default.
    monkeypatch.delattr(ctx.sdk, "execution_snapshot")
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert ctx.calls == []
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert len(ctx.faults) == 1 and isinstance(ctx.faults[0][1], AutoExecutionError)

    cause = str(ctx.faults[0][1]).lower()
    assert (("identity" in cause and "reconciliation" in cause) if problem == "ambiguous_identity"
            else ("fill" in cause and "unreadable" in cause))


# These controls use real local journals/ownership with the existing explicit
# snapshot adapter. They do not claim native IB timing or physical wire sends.
import logging

import duckdb

from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_intents import timestamp_text
from test_emergency_retry_restoration_contract import REQUEST_BAR, observe, restart


def _middle_journal_unavailable(*args, **kwargs):
    raise OSError("local close journal unavailable during cached recovery")


def _middle_pending_add(ctx, *, quantity=20, bar=TS + pd.Timedelta(seconds=10)):
    ctx.sdk.fill_next = 0
    try:
        ctx.executor._process_signal(make_work(quantity=quantity, pyramid_max_adds=1, bar_ts=bar))
    finally:
        ctx.sdk.fill_next = None
    row = ctx.sdk.accepted[-1]
    intent = next(item for item in ctx.executor.intents.all(kind="OPEN")
                  if item["intent_id"] == row["clientIntentId"])
    assert row["action"] == "BUY" and row["filled"] == 0
    return row, intent


def _middle_cancelled_stops(ctx, monkeypatch, count):
    # LifecycleSDK normally drops a cancelled stop. Preserve the actual
    # placed row as terminal history, as the explicit native protocol does.
    cancel = ctx.sdk.cancel

    def cancel_with_history(order_id):
        was_stop = order_id in ctx.sdk.active_stops
        before = (next(row for row in ctx.sdk.trades().to_dict("records")
                       if row["orderId"] == order_id) if was_stop else None)
        result = cancel(order_id)
        if before is not None and result.is_success():
            before.update(status="Cancelled", filled=0, fillQuantityKnown=True)
            ctx.sdk.accepted.append(before)
        return result

    monkeypatch.setattr(ctx.sdk, "cancel", cancel_with_history)
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "8")
    receipts = []
    try:
        for _ in range(count):
            ctx.executor._ensure_protective(OWNER, CONID)
            order_id = ctx.sdk.next_protective_id - 1
            assert order_id in ctx.sdk.active_stops
            assert ctx.executor._cancel_protective(OWNER, CONID, order_id, "reviewed replacement")
            receipts.append(next(row for row in ctx.sdk.accepted if row["orderId"] == order_id))
    finally:
        monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    return receipts


@pytest.mark.parametrize("reductions,addition,expected", [
    ((39,), 20, 21), ((40,), 1, None), ((20, 20), 20, None),
])
def test_cached_corrections_distinguish_partial_reduction_from_flat_crossing(
        retry_owner, monkeypatch, reductions, addition, expected):
    ctx = retry_owner
    opening, intent = _middle_pending_add(ctx, quantity=addition)
    stops = _middle_cancelled_stops(ctx, monkeypatch, len(reductions))
    remember(ctx)
    # The opening was captured while pending, then cancellation is confirmed
    # before any reduction. Later observations correct cumulative quantities.
    ctx.sdk.cancel(opening["orderId"])
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intent(intent)
    opening.update(filled=addition, status="Cancelled")
    for row, filled in zip(stops, reductions):
        row.update(filled=filled, status="Cancelled")
    ctx.sdk.broker[CONID] += addition - sum(reductions)
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    if expected is None:
        assert ctx.calls == []
        assert any(isinstance(exc, AutoExecutionError)
                   and all(word in str(exc).lower() for word in ("ownership", "flat", "epoch"))
                   for _message, exc in ctx.faults)
    else:
        assert [call["quantity"] for call in ctx.calls] == [expected]
        assert ctx.faults == []
    # The uncertain endpoint recorder never fills; cached attribution has
    # not consumed these later receipts or invented a new ownership epoch.
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140 + addition - sum(reductions)


@pytest.mark.parametrize("retry_owner", [60], indirect=True)
def test_restart_ordering_keeps_an_earlier_positive_open_delta(retry_owner, monkeypatch):
    ctx = retry_owner
    first = ctx.sdk.accepted[0]
    first_intent = next(item for item in ctx.executor.intents.all(kind="OPEN")
                        if item["intent_id"] == first["clientIntentId"])
    ctx.sdk.cancel(first["orderId"])
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intent(first_intent)
    addition, opening = _middle_pending_add(ctx)
    # Updating the older terminal OPEN after the new pending OPEN gives the
    # durable updated/intent_id loader its actual B-before-A order.
    first.update(filled=41, status="Cancelled")
    ctx.sdk.broker[CONID] += 1
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intent(first_intent)
    stop, = _middle_cancelled_stops(ctx, monkeypatch, 1)
    ctx.executor = restart(ctx)
    ids = [item["intent_id"] for item in ctx.executor.intents.cached()]
    assert ids.index(opening["intent_id"]) < ids.index(first_intent["intent_id"])
    remember(ctx)
    ctx.sdk.cancel(addition["orderId"])
    ctx.executor._snapshot_cache = None
    opening = next(item for item in ctx.executor.intents.all(kind="OPEN")
                   if item["intent_id"] == opening["intent_id"])
    ctx.executor._reconcile_intent(opening)
    stop.update(filled=41, status="Cancelled")
    addition.update(filled=20, status="Cancelled")
    ctx.sdk.broker[CONID] += 20 - 41
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert ctx.calls == []
    assert any(isinstance(exc, AutoExecutionError)
               and all(word in str(exc).lower() for word in ("ownership", "flat", "epoch"))
               for _message, exc in ctx.faults)
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 41
    assert ctx.sdk.broker[CONID] == 120


def test_old_holding_close_does_not_hide_current_open_correction(retry_owner):
    ctx = retry_owner
    old_epoch = ctx.executor.state.open_position(OWNER, CONID)["ownership_epoch"]
    ctx.executor._execute_close_durable(OWNER, CONID, REQUEST_BAR, 40, "old holding close", entry_bar_ts=TS)
    ctx.executor.manage_positions()
    assert ctx.executor.state.open_position(OWNER, CONID) is None
    ctx.executor._process_signal(make_work(quantity=30, bar_ts=TS + pd.Timedelta(seconds=30)))
    addition, _opening = _middle_pending_add(ctx, bar=TS + pd.Timedelta(seconds=40))
    current = ctx.executor.state.open_position(OWNER, CONID)
    assert current["ownership_epoch"] != old_epoch
    remember(ctx)
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert [call["quantity"] for call in ctx.calls] == [50]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 30
    assert ctx.sdk.broker[CONID] == 150 and ctx.faults == []


@pytest.mark.parametrize("retry_owner", [60], indirect=True)
def test_uncaptured_old_open_correction_cannot_expand_current_explicit_exit(retry_owner):
    ctx = retry_owner
    old_open = ctx.sdk.accepted[0]
    old_intent = next(item for item in ctx.executor.intents.all(kind="OPEN")
                      if item["intent_id"] == old_open["clientIntentId"])
    ctx.sdk.cancel(old_open["orderId"])
    ctx.executor._snapshot_cache = None
    ctx.executor._reconcile_intent(old_intent)
    ctx.executor._execute_close_durable(OWNER, CONID, REQUEST_BAR, 40, "old holding close", entry_bar_ts=TS)
    ctx.executor.manage_positions()
    ctx.executor._process_signal(make_work(quantity=30, bar_ts=TS + pd.Timedelta(seconds=30)))
    remember(ctx)
    old_open.update(filled=41, status="Cancelled")
    ctx.sdk.broker[CONID] += 1
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert ctx.calls == []
    assert any(isinstance(exc, AutoExecutionError)
               and all(word in str(exc).lower() for word in ("opening", "authority"))
               for _message, exc in ctx.faults)
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 30
    assert ctx.sdk.broker[CONID] == 131


def test_lost_open_ack_and_unreferenced_manual_history_preserve_quantity(retry_owner, monkeypatch):
    ctx = retry_owner
    snapshot = ctx.sdk.execution_snapshot

    def scoped_transport_failure(intent_id="", order_ids=None):
        if intent_id:
            raise ConnectionError("scoped acknowledgement unavailable")
        return snapshot(intent_id=intent_id, order_ids=order_ids)

    with monkeypatch.context() as fault:
        fault.setattr(ctx.sdk, "execution_snapshot", scoped_transport_failure)
        ctx.sdk.timeout_after_submit = True
        try:
            addition, opening = _middle_pending_add(ctx)
        finally:
            ctx.sdk.timeout_after_submit = False
    assert opening["status"] == "UNKNOWN" and not opening["payload"].get("order_ids")
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    # This is the same native manual terminal projection used by the existing
    # manual-history contract: no invented MMR reference or ownership.
    ctx.sdk.accepted.append(dict(orderId=7777, clientId=99, permId=777700,
        orderRef="order", brokerOrderRef="", clientIntentId="", conId=CONID,
        action="BUY", status="Cancelled", totalQuantity=1, filled=0,
        avgFillPrice=0, fillQuantityKnown=True))
    remember(ctx)
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    assert [call["quantity"] for call in ctx.calls] == [60]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 160 and ctx.faults == []


@pytest.mark.parametrize("prior_kind", ["lost_close_ack", "restored_zero_id"])
def test_unmatched_possible_close_retains_reservation_and_reports_why(
        retry_owner, monkeypatch, prior_kind):
    ctx = retry_owner
    snapshot = ctx.sdk.execution_snapshot

    def scoped_transport_failure(intent_id="", order_ids=None):
        if intent_id:
            raise ConnectionError("scoped recovery temporarily unavailable")
        return snapshot(intent_id=intent_id, order_ids=order_ids)

    if prior_kind == "lost_close_ack":
        with monkeypatch.context() as fault:
            fault.setattr(ctx.sdk, "execution_snapshot", scoped_transport_failure)
            ctx.sdk.fill_next = 0
            ctx.sdk.timeout_after_submit = True
            try:
                ctx.executor._process_signal(make_work(action=Action.SELL, bar_ts=REQUEST_BAR))
            finally:
                ctx.sdk.fill_next = None
                ctx.sdk.timeout_after_submit = False
        receipt = ctx.sdk.accepted[-1]
        prior, = ctx.executor.intents.all(kind="CLOSE")
        receipt.update(filled=40, status="Filled")
        ctx.sdk.broker[CONID] -= 40
        assert prior["status"] == "UNKNOWN" and prior["payload"].get("proposal_id")
    else:
        remember(ctx)
        ctx.executor._retry_emergency_exits()
        first, = ctx.calls
        receipt = observe(ctx, first["client_intent_id"], filled=40, quantity=40, status="Filled")
        # Native completedOrder preserves reference/permanent identity while
        # its cancellation ID can be zero. Scoped failure leaves restore's
        # actual WORKING producer intact before any attribution.
        receipt.update(orderId=0, clientId=0, permId=1701)
        with monkeypatch.context() as fault:
            fault.setattr(ctx.sdk, "execution_snapshot", scoped_transport_failure)
            ctx.executor._retry_emergency_exits()
        prior, = ctx.executor.intents.all(kind="CLOSE")
        assert prior["status"] == "WORKING" and prior["payload"].get("emergency")
        assert prior["payload"]["order_ids"] == [] and not prior["payload"].get("proposal_id")

    # A later bounded replay can omit an old completed order. It cannot
    # certify that the persisted possible send never happened. Other durable
    # OPEN observations can remain in the tracker; global and scoped proof
    # are deliberately distinct, just as in the native RPC contract.
    identity = receipt["clientIntentId"]

    def bounded_history(intent_id="", order_ids=None):
        result = snapshot(intent_id=intent_id, order_ids=order_ids)
        result["orders"] = [row for row in result["orders"] if row.get("clientIntentId") != identity]
        if intent_id == identity:
            result.update(complete=False, retry_safe=False)
        return result

    monkeypatch.setattr(ctx.sdk, "execution_snapshot", bounded_history)
    ctx.executor = restart(ctx)
    before = len(ctx.calls)
    ctx.faults.clear()
    with monkeypatch.context() as outage:
        outage.setattr(ctx.executor.intents.journal, "transaction", _middle_journal_unavailable)
        ctx.executor._execute_close(OWNER, CONID, REQUEST_BAR + pd.Timedelta(seconds=5),
                                    40, "followup owned exit")

    assert len(ctx.calls) == before
    assert any(isinstance(exc, AutoExecutionError)
               and all(word in str(exc).lower() for word in ("unresolved", "broker"))
               for _message, exc in ctx.faults)
    saved = next(item for item in ctx.executor.intents.all(kind="CLOSE")
                 if item["intent_id"] == prior["intent_id"])
    assert saved["status"] == prior["status"]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 100


def test_legacy_request_entry_alone_retires_a_superseded_cached_exit(retry_owner, monkeypatch):
    ctx = retry_owner
    addition, _opening = _middle_pending_add(ctx)
    request = ctx.executor.intents.create(OWNER, CONID, "CLOSE", dict(
        bar_ts=timestamp_text(REQUEST_BAR), quantity=40, reason="legacy entry-bound close",
        request_entry_bar_ts=timestamp_text(TS), exit_request_active=True), status="WAITING")
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None
    with monkeypatch.context() as outage:
        outage.setattr(ctx.executor.intents.journal, "transaction", _middle_journal_unavailable)
        ctx.executor._advance_close(request)

    assert ctx.calls == []
    ctx.executor.manage_positions()
    ctx.executor._process_signal(make_work(quantity=5, pyramid_max_adds=2,
                                           bar_ts=TS + pd.Timedelta(seconds=40)))
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 65
    assert ctx.sdk.broker[CONID] == 165
    assert ctx.calls == [] and ctx.faults == []


def test_retained_unsent_stale_timer_cannot_starve_another_owner(retry_owner):
    ctx = retry_owner
    other = "z_other_owner"
    ctx.executor._process_signal(make_work(strategy_name=other, quantity=30))
    addition, _opening = _middle_pending_add(ctx)
    remember(ctx, timer=True)
    ctx.executor._retry_emergency_exits()
    first, = ctx.calls
    # The explicit adapter models a RETRYABLE claim with no physical orders,
    # not absence alone. Native _run_order_intent can publish this after an
    # UNKNOWN pre-reservation capacity failure.
    ctx.retry_safe_ids.add(first["client_intent_id"])
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    remember(ctx, owner=other)
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()
    ctx.executor._retry_emergency_exits()

    assert [(call["strategy_name"], call["quantity"]) for call in ctx.calls] == [(OWNER, 40), (other, 30)]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.executor.state.open_position(other, CONID)["quantity"] == 30
    assert ctx.sdk.broker[CONID] == 190 and ctx.faults == []


def test_incomplete_positions_do_not_block_another_owners_known_fill_restore(retry_owner, monkeypatch):
    ctx = retry_owner
    other = "z_other_owner"
    ctx.executor._process_signal(make_work(strategy_name=other, quantity=30))
    remember(ctx)
    remember(ctx, owner=other)
    ctx.executor._retry_emergency_exits()
    first, second = ctx.calls
    assert first["strategy_name"] == OWNER and second["strategy_name"] == other
    ctx.retry_safe_ids.add(first["client_intent_id"])
    receipt = observe(ctx, second["client_intent_id"], filled=30, quantity=30, status="Filled")
    receipt["orderRef"] = other + "|mmr:" + second["client_intent_id"]
    snapshot = ctx.sdk.execution_snapshot

    def positions_not_fresh(intent_id="", order_ids=None):
        result = snapshot(intent_id=intent_id, order_ids=order_ids)
        result["positions_complete"] = False
        return result

    # Separate history completeness and position freshness are part of the
    # explicit protocol. No new reduction is allowed from the stale position
    # response, but the second owner's known execution can be attributed.
    monkeypatch.setattr(ctx.sdk, "execution_snapshot", positions_not_fresh)
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    ctx.executor._retry_emergency_exits()

    assert len(ctx.calls) == 2
    assert ctx.executor.state.open_position(other, CONID) is None
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140 and ctx.faults == []
    restored, = ctx.executor.intents.all(kind="CLOSE")
    assert restored["intent_id"] == second["client_intent_id"] and restored["status"] == "FILLED"


def test_migrated_owner_warning_keeps_proof_facts_and_other_owner_progress(
        retry_owner, monkeypatch, tmp_path):
    ctx = retry_owner
    legacy_path = str(tmp_path / "pre_epoch_retry.duckdb")
    # Exact supported pre-column schema. The original fixture's real40 BUY
    # supplies broker inventory; its historical ownership row has no invented
    # UUID or claim timestamp when the new executor migrates this database.
    with duckdb.connect(legacy_path) as connection:
        connection.execute("""CREATE TABLE auto_exec_positions (
            strategy VARCHAR NOT NULL, conid BIGINT NOT NULL, quantity DOUBLE NOT NULL,
            entry_bar_ts TIMESTAMP, entry_time TIMESTAMP NOT NULL, proposal_id BIGINT,
            close_by_time VARCHAR, max_hold_bars BIGINT, status VARCHAR NOT NULL,
            closed_reason VARCHAR, close_proposal_id BIGINT, updated TIMESTAMP NOT NULL)""")
        connection.execute("INSERT INTO auto_exec_positions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                           [OWNER, CONID, 40, TS.to_pydatetime(), TS.to_pydatetime(), 100,
                            None, None, "OPEN", None, None, TS.to_pydatetime()])
    manager = AutoExecutor(legacy_path, paper_trading=True, cooldown_seconds=0, sdk_factory=lambda: ctx.sdk)
    ctx.reopened.append(manager)
    ctx.executor = manager
    assert manager.state.open_position(OWNER, CONID)["ownership_epoch"] is None
    other = "z_other_owner"
    manager._process_signal(make_work(strategy_name=other, quantity=30))
    records = []

    def warning(message, *args, **kwargs):
        records.append(logging.LogRecord("auto-executor", logging.WARNING, __file__, 0, message, args, None))

    monkeypatch.setattr("trader.strategy.auto_executor.logging.warning", warning)
    remember(ctx)
    remember(ctx, owner=other)
    manager._retry_emergency_exits()
    manager._retry_emergency_exits()

    assert [(call["strategy_name"], call["quantity"]) for call in ctx.calls] == [(other, 30)]
    assert any(OWNER in record.getMessage() and str(CONID) in record.getMessage()
               and "legacy" in record.getMessage().lower() and "proof" in record.getMessage().lower()
               for record in records)
    assert manager.state.open_position(OWNER, CONID)["quantity"] == 40
    assert manager.state.open_position(other, CONID)["quantity"] == 30
    assert ctx.sdk.broker[CONID] == 170 and ctx.faults == []
