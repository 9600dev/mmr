"""A failed claim can remain in a captured scope after its cache disappears."""

import json

import pandas as pd
import pytest

from review.test_review_strategy_contract import TS, make_work
from test_execution_recovery import recovery
from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_intents import timestamp_text


OWNER = ("orb_test", 1111)


class CommitFailure:
    """Fail one real transaction after its matching OPEN was cached."""

    def __init__(self, connection, entry):
        self.connection = connection
        self.entry = timestamp_text(entry)
        self.armed = False

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def execute(self, sql, parameters=()):
        result = self.connection.execute(sql, parameters)
        if (sql.startswith("INSERT INTO execution_intents VALUES")
                and parameters[3] == "OPEN"
                and json.loads(parameters[5])["bar_ts"] == self.entry):
            self.armed = True
        return result

    def commit(self):
        if self.armed:
            self.armed = False
            raise OSError("OPEN claim commit failed")
        return self.connection.commit()


def test_restarted_scope_tolerates_a_captured_open_whose_claim_rolled_back(recovery, monkeypatch):
    executor, sdk, path = recovery
    restarted = None
    try:
        monkeypatch.setenv("MMR_PROTECTIVE_STOP_PCT", "0")
        executor.cooldown_seconds = 0
        sdk.broker[OWNER[1]] = 100
        executor._process_signal(make_work(quantity=40))

        # An earlier accepted add has no currently known fill and is terminal.
        # It can later receive a corrected cumulative observation.
        sdk.fill_next = 0
        earlier = TS + pd.Timedelta(seconds=10)
        executor._process_signal(make_work(quantity=20, pyramid_max_adds=1, bar_ts=earlier))
        earlier_order = sdk.accepted[-1]
        earlier_order["status"] = "Cancelled"
        executor._reconcile_intents(*OWNER)

        failed_entry = TS + pd.Timedelta(seconds=20)
        journal = executor.intents.journal
        with monkeypatch.context() as fault:
            fault.setattr(journal, "_connection", CommitFailure(journal._connection, failed_entry))
            with pytest.raises(OSError, match="claim commit failed"):
                executor._process_signal(make_work(quantity=20, pyramid_max_adds=1, bar_ts=failed_entry))
        ghost, = [item for item in executor.intents.cached()
                  if item["kind"] == "OPEN" and item["payload"]["bar_ts"] == timestamp_text(failed_entry)]
        assert ghost["payload"].get("proposal_id") is None
        assert ghost["intent_id"] not in {item["intent_id"] for item in executor.intents.all()}

        # A transient capture read uses the last cached identities; immediately
        # recovered later reads persist that scope through the real close path.
        query = executor.intents.all
        failed_read = []

        def capture_read(**filters):
            if filters.get("kind") == "OPEN" and not filters.get("active") and not failed_read:
                failed_read.append(True)
                raise OSError("scope read temporarily unavailable")
            return query(**filters)

        due = TS + pd.Timedelta(minutes=1)
        with monkeypatch.context() as fault:
            fault.setattr(executor.intents, "all", capture_read)
            executor._execute_close(*OWNER, due, 40, "already-admitted explicit SELL")
        closing, = executor.intents.all(kind="CLOSE")
        assert ghost["intent_id"] in closing["payload"]["explicit_exit_scope"]["openings"]

        # Later history corrects the earlier add's cumulative quantity, giving
        # the current holding its other proposal/entry pair. The close itself
        # was canceled without filling; no additional SELL should be proposed.
        earlier_order["filled"] = 10
        sdk.broker[OWNER[1]] += 10
        sdk.accepted[-1]["status"] = "Cancelled"
        restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        assert ghost["intent_id"] not in {item["intent_id"] for item in restarted.intents.cached()}
        restarted.manage_positions()
        position = restarted.state.open_position(*OWNER)
        assert position["quantity"] == 50
        assert position["entry_bar_ts"] == earlier
        saved, = restarted.intents.all(kind="CLOSE")
        assert restarted._close_entry_matches(saved, position) is False
        assert saved["payload"]["exit_request_active"] is True
        assert [call["action"] for call in sdk.propose_calls] == ["BUY", "BUY", "SELL"]
        assert sdk.broker[OWNER[1]] == 150
    finally:
        if restarted is not None:
            restarted.intents.journal.close()
        executor.intents.journal.close()
