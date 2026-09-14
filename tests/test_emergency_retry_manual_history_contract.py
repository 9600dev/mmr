"""Unrelated native history may lack an MMR reference."""
from test_execution_recovery import recovery
from test_emergency_retry_restoration_contract import CONID, remember, retry_owner


def test_manual_order_history_does_not_prevent_an_owned_emergency_reduction(retry_owner):
    ctx = retry_owner
    # A known terminal zero-fill manual order neither changes ownership nor
    # reserves exposure. The native projection uses the generic owner label
    # while preserving the empty raw broker ref and absent MMR intent ID.
    ctx.sdk.accepted.append(dict(orderId=7777, clientId=99, permId=777700,
                                 orderRef="order", brokerOrderRef="", clientIntentId="", conId=CONID,
                                 action="BUY", status="Cancelled", totalQuantity=1,
                                 filled=0, avgFillPrice=0, fillQuantityKnown=True))
    remember(ctx)
    ctx.executor._snapshot_cache = None

    ctx.executor._retry_emergency_exits()

    call, = ctx.calls
    assert call["quantity"] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert ctx.faults == []
