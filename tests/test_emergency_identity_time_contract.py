"""Emergency creation time controls broker-history recovery eligibility."""

import datetime as dt

from test_emergency_receipt_epoch import CONID, OWNER, broker_receipt, emergency
from test_execution_recovery import recovery
from trader.strategy.auto_executor import AutoExecutor


def test_emergency_creation_time_excludes_history_before_the_current_entry(emergency, monkeypatch):
    ctx = emergency
    old_position = ctx.executor.state.open_position(OWNER, CONID)
    origin = old_position["ownership_started_at"]
    clock = [origin + 1]
    monkeypatch.setattr("trader.strategy.auto_executor.time.time", lambda: clock[0])
    old_identity = AutoExecutor._emergency_identity(
        ownership_epoch=old_position["ownership_epoch"])

    # These explicit compatibility declarations establish two separate
    # holdings; this fixture does not claim to simulate the intervening fills.
    ctx.executor.state.record_close(OWNER, CONID, "CLOSED", "prior holding ended")
    entry = dt.datetime.fromtimestamp(origin + 10, dt.timezone.utc).replace(tzinfo=None)
    clock[0] = origin + 11
    ctx.executor.state.record_open(OWNER, CONID, 60, entry, 502, None, None)
    ctx.sdk.broker[CONID] = 160
    position = ctx.executor.state.open_position(OWNER, CONID)
    current_identity = AutoExecutor._emergency_identity(
        ownership_epoch=position["ownership_epoch"])

    # Both are exact, zero-fill terminal observations. Only the current
    # entry's receipt is eligible for first-time local recovery.
    old_receipt = broker_receipt(ctx, old_identity, filled=0, quantity=40)
    old_receipt["orderId"] = 1998
    broker_receipt(ctx, current_identity, filled=0, quantity=60)

    for _ in range(2):
        restarted = AutoExecutor(ctx.path, paper_trading=True, sdk_factory=lambda: ctx.sdk)
        restarted._adopt_observed_protectives(OWNER, CONID)
        recovered = restarted.intents.all(kind="CLOSE")
        assert {intent["intent_id"] for intent in recovered} == {current_identity}
        assert recovered[0]["payload"]["ownership_epoch"] == position["ownership_epoch"]
        assert restarted.state.open_position(OWNER, CONID)["quantity"] == 60
    assert ctx.calls == []
    assert ctx.faults == []
