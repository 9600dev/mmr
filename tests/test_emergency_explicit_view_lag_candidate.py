"""A valid exit survives delayed ownership publication and a failed close claim."""
from test_execution_recovery import recovery
from contextlib import contextmanager

import pandas as pd
import pytest

from review.test_review_strategy_contract import TS, make_work
from test_emergency_retry_restoration_contract import (
    CONID, OWNER, REQUEST_BAR, pending_add, retry_owner,
)
from trader.objects import Action


@pytest.mark.parametrize("explicit", [False, True])
def test_valid_exit_waits_for_committed_ownership_to_reach_the_cached_view(
        retry_owner, monkeypatch, explicit):
    ctx = retry_owner
    addition, opening = pending_add(ctx)
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None

    def unavailable_view():
        raise OSError("ownership view publication temporarily unavailable")

    with monkeypatch.context() as publish_fault:
        publish_fault.setattr(ctx.executor, "_load_open_view", unavailable_view)
        with pytest.raises(OSError, match="view publication"):
            ctx.executor._reconcile_intent(opening)
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60
    assert ctx.executor.managed_positions()[0]["quantity"] == 40

    transaction = ctx.executor.intents.journal.transaction

    class RefuseCloseInsert:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, sql, parameters=()):
            if sql.startswith("INSERT INTO execution_intents") and parameters[3] == "CLOSE":
                raise OSError("close claim insert temporarily unavailable")
            return self.connection.execute(sql, parameters)

        def __getattr__(self, name):
            return getattr(self.connection, name)

    @contextmanager
    def readable_journal_with_failed_close_claim():
        with transaction() as connection:
            yield RefuseCloseInsert(connection)

    # The actual SQLite transaction rolls back a CLOSE INSERT; reads and the
    # previously committed OPEN/checkpoint remain available. Neither the
    # scope producer nor the emergency request merger is replaced.
    with monkeypatch.context() as claim_fault:
        claim_fault.setattr(ctx.executor.intents.journal, "transaction",
                            readable_journal_with_failed_close_claim)
        if explicit:
            ctx.executor._process_signal(make_work(action=Action.SELL, bar_ts=REQUEST_BAR))
        else:
            ctx.executor._execute_close(
                OWNER, CONID, REQUEST_BAR, 60, "validated newer timer",
                entry_bar_ts=TS + pd.Timedelta(seconds=10), explicit=False)
    assert ctx.calls == []
    assert ctx.executor.intents.all(kind="CLOSE") == []
    assert len(ctx.faults) == 1 and isinstance(ctx.faults[0][1], OSError)
    ctx.faults.clear()

    ctx.executor._load_open_view()
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert [call["quantity"] for call in ctx.calls] == [60]
    assert ctx.sdk.broker[CONID] == 160
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60
    assert ctx.faults == []


def test_explicit_view_lag_does_not_block_an_independent_owned_exit(retry_owner, monkeypatch):
    ctx = retry_owner
    other = "independent_exit_owner"
    ctx.executor._process_signal(make_work(strategy_name=other, quantity=30))
    addition, opening = pending_add(ctx)
    addition.update(filled=20, status="Filled")
    ctx.sdk.broker[CONID] += 20
    ctx.executor._snapshot_cache = None

    def unavailable_view():
        raise OSError("ownership view publication temporarily unavailable")

    with monkeypatch.context() as publish_fault:
        publish_fault.setattr(ctx.executor, "_load_open_view", unavailable_view)
        with pytest.raises(OSError, match="view publication"):
            ctx.executor._reconcile_intent(opening)
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60
    cached = {row["strategy_name"]: row for row in ctx.executor.managed_positions()}
    assert cached[OWNER]["quantity"] == 40 and cached[other]["quantity"] == 30

    transaction = ctx.executor.intents.journal.transaction

    class RefuseCloseInsert:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, sql, parameters=()):
            if sql.startswith("INSERT INTO execution_intents") and parameters[3] == "CLOSE":
                raise OSError("close claim insert temporarily unavailable")
            return self.connection.execute(sql, parameters)

        def __getattr__(self, name):
            return getattr(self.connection, name)

    @contextmanager
    def readable_journal_with_failed_close_claim():
        with transaction() as connection:
            yield RefuseCloseInsert(connection)

    # Capture the actual new durable entry while its cached view is older.
    # The SQLite CLOSE insert fails under rollback, leaving reads available.
    with monkeypatch.context() as claim_fault:
        claim_fault.setattr(ctx.executor.intents.journal, "transaction",
                            readable_journal_with_failed_close_claim)
        ctx.executor._process_signal(make_work(action=Action.SELL, bar_ts=REQUEST_BAR))
    assert ctx.calls == [] and ctx.executor.intents.all(kind="CLOSE") == []
    assert len(ctx.faults) == 1 and isinstance(ctx.faults[0][1], OSError)
    ctx.faults.clear()

    # The normal merger inserts the independent request after the stale one.
    # Its own cached and durable quantities agree, so it need not await the
    # first owner's publication recovery. Unknown acknowledgement is no fill.
    ctx.executor._remember_emergency_exit(
        other, CONID, REQUEST_BAR, "independent explicit exit")
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert [(call["strategy_name"], call["quantity"]) for call in ctx.calls] == [(other, 30)]
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 60
    assert ctx.executor.state.open_position(other, CONID)["quantity"] == 30
    assert ctx.sdk.broker[CONID] == 190
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert len(ctx.calls) == 1

    ctx.executor._load_open_view()
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert [(call["strategy_name"], call["quantity"]) for call in ctx.calls] == [
        (other, 30), (OWNER, 60)]
    assert ctx.sdk.broker[CONID] == 190
    assert ctx.faults == []
