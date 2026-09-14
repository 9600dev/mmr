"""A new timer cannot inherit an already fulfilled explicit exit request.

Real intent/ownership stores and the existing lifecycle SDK exercise request
retirement and a later cumulative correction. The pause is an executor work
boundary before physical submission, not a clock or concurrent-writer claim.
"""

import pandas as pd

from review.test_review_strategy_contract import TS, make_work
from test_execution_recovery import recovery


OWNER = ("orb_test", 1111)
OLD_ADD = TS + pd.Timedelta(seconds=15)
OLD_EXIT = TS + pd.Timedelta(seconds=20)
NEW_ENTRY = TS + pd.Timedelta(seconds=30)
NEW_DUE = TS + pd.Timedelta(seconds=40)


def test_new_timer_does_not_revive_retired_scope_after_an_old_open_fill_correction(
        recovery, monkeypatch):
    executor, sdk, _ = recovery
    monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
    executor.cooldown_seconds = 0
    sdk.broker[OWNER[1]] = 100  # manual shares survive every strategy operation
    try:
        executor._process_signal(make_work(quantity=40))
        sdk.fill_next = 0
        executor._process_signal(make_work(quantity=20, pyramid_max_adds=1,
                                           bar_ts=OLD_ADD))
        old_open, = executor.intents.all(kind="OPEN", active=True)
        old_receipt = next(row for row in sdk.accepted
                           if row["clientIntentId"] == old_open["intent_id"])

        # The explicit request captures pending B before the real cancellation
        # coordinator obtains Cancelled0 and closes the original 40 shares.
        sdk.fill_next = None
        executor._execute_close(*OWNER, OLD_EXIT, 40, "explicit flatten")
        executor.manage_positions()
        retired, = executor.intents.all(kind="CLOSE")
        assert old_receipt["status"] == "Cancelled"
        assert old_receipt["filled"] == 0
        assert old_open["intent_id"] in retired["payload"]["explicit_exit_scope"]["openings"]
        assert retired["payload"]["exit_request_active"] is False
        assert executor.state.open_position(*OWNER) is None
        assert sdk.broker[OWNER[1]] == 100

        # C is admitted after the old explicit request was actually fulfilled.
        executor._process_signal(make_work(quantity=30, bar_ts=NEW_ENTRY))
        assert executor.state.open_position(*OWNER)["entry_bar_ts"] == NEW_ENTRY
        assert sdk.broker[OWNER[1]] == 130

        # Keep the newly admitted due-C request durable at a worker boundary.
        # No representation assertion assumes how that request was stored.
        with monkeypatch.context() as pause:
            pause.setattr(executor, "_advance_close", lambda intent: None)
            executor._execute_close_durable(*OWNER, NEW_DUE, 30, "timer C",
                                            entry_bar_ts=NEW_ENTRY)
        pending, = executor.intents.all(kind="CLOSE", active=True)

        # A terminal cancellation may receive corrected cumulative executions.
        # This does not claim the cancelled order began executing again. The
        # real reconciliation/checkpoint path applies the newly reported ten.
        old_receipt.update(filled=10, fillQuantityKnown=True)
        sdk.broker[OWNER[1]] += 10
        executor._snapshot_cache = None
        executor._reconcile_intents(*OWNER)
        position = executor.state.open_position(*OWNER)
        assert position["entry_bar_ts"] == OLD_ADD
        assert position["proposal_id"] == old_open["payload"]["proposal_id"]
        assert position["quantity"] == 40

        # Latest-BUY-wins makes C's timer stale. Its request cannot borrow the
        # retired explicit request's captured B identity to close this holding.
        executor._advance_close(pending)
        assert [call["action"] for call in sdk.propose_calls] == [
            "BUY", "BUY", "SELL", "BUY"]
        assert sdk.broker[OWNER[1]] == 140
        assert executor.state.open_position(*OWNER)["quantity"] == 40
    finally:
        executor.intents.journal.close()
