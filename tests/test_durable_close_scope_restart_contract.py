"""Recovery preserves the authority committed before a partial request write."""

import pandas as pd

from test_durable_close_handoff_contract import ADD, DUE, OWNER, handoff
from review.test_review_strategy_contract import TS, make_work
from trader.strategy.auto_executor import AutoExecutor


def test_later_explicit_failure_keeps_pre_restart_scope_from_partial_upgrade(handoff, monkeypatch):
    ctx = handoff
    executor, sdk = ctx.executor, ctx.sdk
    sdk.fill_next = 0
    executor._process_signal(make_work(quantity=20, pyramid_max_adds=1, bar_ts=ADD))
    old_receipt = sdk.accepted[-1]
    old_opening = executor.intents.all(kind="OPEN", active=True)[0]
    old_receipt.update(status="Cancelled")
    executor._reconcile_intents(*OWNER)
    newer_entry = TS + pd.Timedelta(seconds=45)
    executor._process_signal(make_work(quantity=20, pyramid_max_adds=1, bar_ts=newer_entry))
    newer_receipt = sdk.accepted[-1]
    # A delayed active-status observation conservatively reopens uncertainty
    # after C was admitted. The native tracker allows that status regression;
    # this does not claim the old terminal BUY physically began filling again.
    old_receipt.update(status="Submitted", brokerStatus="Submitted")
    sdk.cancel_fails = True
    executor._execute_close_durable(*OWNER, DUE, 40, "old timer", entry_bar_ts=TS)
    parent, = executor.intents.all(kind="CLOSE", active=True)
    assert parent["status"] == "WAITING"
    update = executor.intents.update

    def fail_first_upgrade(intent, **changes):
        if intent["intent_id"] == parent["intent_id"] and changes.get("reason") == "first explicit SELL":
            raise OSError("policy write failed after scope committed")
        return update(intent, **changes)

    with monkeypatch.context() as fault:
        fault.setattr(executor.intents, "update", fail_first_upgrade)
        fault.setattr(executor, "_retry_emergency_exits", lambda: None)
        executor._execute_close(*OWNER, DUE, 40, "first explicit SELL")
    saved, = executor.intents.all(kind="CLOSE", active=True)
    assert old_opening["intent_id"] in saved["payload"]["explicit_exit_scope"]["openings"]
    # Stop using the first instance. A new manager recovers durable scope but
    # intentionally has none of the first process's emergency-only memory.
    executor.intents.journal.close()
    restarted = AutoExecutor(executor.state.db.db_path, paper_trading=True, sdk_factory=lambda: sdk)
    try:
        assert restarted._emergency_exits == {}
        newer_receipt.update(filled=20, status="Filled")
        sdk.broker[OWNER[1]] += 20
        old_receipt.update(status="Cancelled", brokerStatus="Cancelled", fillQuantityKnown=True)
        restarted._reconcile_intents(*OWNER)
        assert restarted.state.open_position(*OWNER)["entry_bar_ts"] == newer_entry
        fresh_scope = restarted._capture_exit_scope(*OWNER)
        assert old_opening["intent_id"] not in fresh_scope["openings"]

        def fail_successor(*args, **kwargs):
            raise OSError("successor write unavailable after explicit policy upgrade")

        monkeypatch.setattr(restarted.intents, "retain_exit_successor", fail_successor)
        monkeypatch.setattr(restarted, "_retry_emergency_exits", lambda: None)
        restarted._execute_close(*OWNER, DUE + pd.Timedelta(seconds=1), 60, "new explicit SELL")
        emergency, = restarted._emergency_exits.values()
        scope = emergency["explicit_exit_scope"]
        assert restarted._scope_accepts_opening(scope, old_opening) is True
        assert restarted._close_entry_matches({"payload": emergency}, dict(
            entry_bar_ts=ADD, proposal_id=old_opening["payload"]["proposal_id"])) is True
        assert restarted._close_entry_matches({"payload": emergency}, dict(
            entry_bar_ts=DUE, proposal_id=999999)) is False
        assert len(sdk.approve_calls) == 3
        assert sdk.broker[OWNER[1]] == 160
    finally:
        restarted.intents.journal.close()
