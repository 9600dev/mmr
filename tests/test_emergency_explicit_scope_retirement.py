"""Deferring a matching durable scope must not retain unrelated old demand."""
import pandas as pd

from review.test_review_strategy_contract import TS, make_work
from test_emergency_retry_restoration_contract import CONID, OWNER, retry_owner
from test_execution_recovery import recovery
from trader.objects import Action


def test_old_captured_scope_cannot_sell_or_block_a_new_unrelated_holding(retry_owner):
    ctx = retry_owner
    old_scope = ctx.executor._capture_exit_scope(OWNER, CONID)
    ctx.executor._process_signal(make_work(action=Action.SELL,
                                           bar_ts=TS + pd.Timedelta(seconds=10)))
    assert ctx.executor.state.open_position(OWNER, CONID) is None
    assert ctx.sdk.broker[CONID] == 100

    ctx.executor._process_signal(make_work(quantity=30,
                                           bar_ts=TS + pd.Timedelta(seconds=20)))
    current = ctx.executor.state.open_position(OWNER, CONID)
    assert current["quantity"] == 30 and ctx.sdk.broker[CONID] == 130
    assert not ctx.executor._scope_matches_position(old_scope, current)

    # Exercise a delayed retained-request retry using its original exact
    # capture. This is the request-retention boundary, not a new admitted SELL
    # against the unrelated holding or an assertion of concurrent writers.
    ctx.executor._remember_emergency_exit(
        OWNER, CONID, TS + pd.Timedelta(seconds=5), "delayed old captured request",
        explicit_exit_scope=old_scope, capture_explicit=False)
    ctx.executor._snapshot_cache = None
    ctx.executor._retry_emergency_exits()
    assert ctx.calls == []
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 30
    assert ctx.sdk.broker[CONID] == 130

    # A blanket retain-on-scope-mismatch would block this otherwise valid add.
    # Its real admission/filled inventory proves the obsolete request retired.
    ctx.executor._process_signal(make_work(quantity=10, pyramid_max_adds=1,
                                           bar_ts=TS + pd.Timedelta(seconds=40)))
    assert ctx.executor.state.open_position(OWNER, CONID)["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert ctx.calls == [] and ctx.faults == []
