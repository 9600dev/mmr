"""An incomplete order book defers a close; it must not escalate (review 2026-09-11).

``_advance_close`` returned quietly on an incomplete POSITIONS snapshot but let
``_own_live_protectives`` raise on an incomplete ORDERS snapshot, and the
generic handler turned that routine deferral into an emergency exit while the
durable CLOSE intent stayed WAITING: two independent reduction submitters for
the same shares. Both incompleteness cases now defer the same way.
"""
from test_advance_close_contract_gaps import CONID, IDENT, OWNER, _intent, close_context  # noqa: F401


def test_incomplete_order_book_defers_the_close_without_emergency_escalation(close_context):
    ctx = close_context
    ctx.snapshot['positions_complete'] = True
    ctx.snapshot['complete'] = False  # the post-reconnect window: positions confirmed, history not
    intent = _intent(ctx, ident=dict(IDENT))

    ctx.executor._advance_close(intent)

    assert ctx.submitted == []
    assert ctx.retries == [], 'no emergency retry was triggered'
    assert ctx.executor._emergency_exits == {}, 'no second submitter was remembered'
    pending, = ctx.executor.intents.all(strategy=OWNER, conid=CONID, kind='CLOSE', active=True)
    assert pending['status'] == 'WAITING'

    ctx.snapshot['complete'] = True
    ctx.executor._advance_close(pending)
    submitted, = ctx.submitted
    assert submitted['payload']['quantity'] == 10.0, 'the same durable close proceeds once the book is complete'


def test_missing_completeness_metadata_is_treated_as_incomplete(close_context):
    """Fail closed: a snapshot that does not say it is complete is not complete."""
    ctx = close_context
    ctx.snapshot['positions_complete'] = True
    del ctx.snapshot['complete']
    intent = _intent(ctx, ident=dict(IDENT))

    ctx.executor._advance_close(intent)

    assert ctx.submitted == [] and ctx.retries == []
    assert ctx.executor._emergency_exits == {}
    pending, = ctx.executor.intents.all(strategy=OWNER, conid=CONID, kind='CLOSE', active=True)
    assert pending['status'] == 'WAITING'
