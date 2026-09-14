"""New close attempts resolve exact identity before retaining executable work."""

import pytest

from test_durable_close_handoff_contract import DUE, OWNER, handoff
from trader.strategy.auto_executor import AutoExecutionError


def test_new_close_uses_identity_already_resolved_before_metadata_becomes_unavailable(handoff, monkeypatch):
    ctx = handoff
    resolve = ctx.sdk.resolve
    reads = []

    def transient_lookup(symbol, **hints):
        if symbol == OWNER[1]:
            reads.append(symbol)
            if len(reads) > 1:
                return []
        return resolve(symbol, **hints)

    monkeypatch.setattr(ctx.sdk, "resolve", transient_lookup)
    ctx.executor._execute_close(*OWNER, DUE, 40, "exit with captured exact identity")

    assert [call["action"] for call in ctx.sdk.propose_calls] == ["BUY", "SELL"]
    assert ctx.sdk.broker[OWNER[1]] == 100
    assert ctx.executor.state.open_position(*OWNER) is None
    assert ctx.executor._emergency_exits == {}


def test_exact_identity_refusal_cannot_turn_into_a_recoverable_emergency_close(handoff, monkeypatch):
    ctx = handoff
    monkeypatch.setattr(ctx.sdk, "resolve", lambda *args, **kwargs: [])

    with pytest.raises(AutoExecutionError, match="conId 1111 not found"):
        ctx.executor._execute_close(*OWNER, DUE, 40, "unresolvable exact instrument")

    assert ctx.executor.intents.all(kind="CLOSE") == []
    assert ctx.executor._emergency_exits == {}
    assert [call["action"] for call in ctx.sdk.propose_calls] == ["BUY"]
    assert ctx.sdk.broker[OWNER[1]] == 140
