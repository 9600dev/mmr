"""Protective placement receipts preserve the right retry boundary.

Real local intent/ownership stores use the existing deterministic SDK adapter.
No claim here depends on a broker accepting a simulated uncertain submission.
"""
import datetime as dt
from types import SimpleNamespace

import pytest

from review.test_review_strategy_contract import FakeResult, LifecycleSDK
from trader.strategy.auto_executor import AutoExecutor


OWNER, CONID = 'protective_receipt_outcomes', 1111


@pytest.fixture
def protection(tmp_path, monkeypatch):
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '8')
    faults, attempts, instances = [], [], []
    monkeypatch.setattr('trader.strategy.auto_executor.logging.exception',
                        lambda *args, **kwargs: faults.append(args))
    sdk = LifecycleSDK()
    sdk.broker[CONID] = 140  # one hundred shares belong to someone else
    path = str(tmp_path / 'protective-receipts.duckdb')

    def restart():
        executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        instances.append(executor)
        return executor

    executor = restart()
    # This holding has a retained priced opening checkpoint: forty shares
    # at 100 each. The existing hundred manual shares remain independent.
    opening = executor.intents.create(OWNER, CONID, 'OPEN', {
        'bar_ts': dt.datetime.now(dt.timezone.utc).isoformat(),
        'quantity': 40, 'proposal_id': 501}, status='FILLED')
    assert executor.state.apply_fill(opening, 40, cumulative_quote_notional=4000) == 40
    assert executor.state.open_position(OWNER, CONID)['avg_cost'] == 100
    original_place = sdk.place_protective_order
    ctx = SimpleNamespace(executor=executor, sdk=sdk, restart=restart,
                          attempts=attempts, faults=faults, response=None)

    def place(**kwargs):
        attempts.append(dict(kwargs))
        # The durable ownership-bound attempt exists before the first call
        # that could reach the broker, including a call whose reply is lost.
        saved, = executor.intents.all(kind='PROTECTIVE', active=True)
        current = executor.state.open_position(OWNER, CONID)
        assert saved['status'] == 'SUBMITTING'
        assert saved['intent_id'] == kwargs['client_intent_id']
        assert saved['payload']['ownership_epoch'] == current['ownership_epoch']
        assert saved['payload']['quantity'] == kwargs['quantity'] == 40
        if isinstance(ctx.response, Exception):
            raise ctx.response
        if ctx.response is not None:
            return ctx.response
        return original_place(**kwargs)

    monkeypatch.setattr(sdk, 'place_protective_order', place)
    try:
        yield ctx
    finally:
        for instance in instances:
            instance.intents.journal.close()


@pytest.mark.parametrize('error', ['broker rejected this order', 'protective order refused by venue'])
def test_definitive_refusal_allows_a_new_protection_attempt(protection, error):
    ctx = protection
    ctx.response = FakeResult(ok=False, error=error)
    ctx.executor._ensure_protective(OWNER, CONID)
    rejected, = ctx.executor.intents.all(kind='PROTECTIVE')
    assert rejected['status'] == 'REJECTED'
    assert not ctx.executor.intents.all(kind='PROTECTIVE', active=True)
    assert len(ctx.attempts) == 1 and not ctx.sdk.active_stops

    ctx.response = None
    resumed = ctx.restart()
    resumed._ensure_protective(OWNER, CONID)
    active, = resumed.intents.all(kind='PROTECTIVE', active=True)
    assert active['intent_id'] != rejected['intent_id']
    assert active['status'] == 'WORKING'
    assert len(ctx.attempts) == 2 and len(ctx.sdk.active_stops) == 1
    assert resumed.state.open_position(OWNER, CONID)['quantity'] == 40
    assert ctx.sdk.broker[CONID] == 140
    assert not ctx.faults


@pytest.mark.parametrize('error', [
    'UNKNOWN: reply may be lost',
    'order rejected after timeout',
    'order refused after connection loss',
    'UNKNOWN: broker rejection state is uncertain',
    'unexpected transport state',
])
def test_ambiguous_failure_retains_one_attempt_across_restart(protection, error):
    ctx = protection
    ctx.response = FakeResult(ok=False, error=error)
    ctx.executor._ensure_protective(OWNER, CONID)
    first, = ctx.executor.intents.all(kind='PROTECTIVE', active=True)
    assert first['status'] == 'UNKNOWN'
    assert not first['payload'].get('order_ids')

    # Even if the endpoint becomes responsive, an unobserved previous send
    # cannot be replaced by another executable protective order.
    ctx.response = None
    ctx.executor._ensure_protective(OWNER, CONID)
    resumed = ctx.restart()
    resumed.manage_positions()
    resumed._ensure_protective(OWNER, CONID)
    retained, = resumed.intents.all(kind='PROTECTIVE', active=True)
    assert retained['intent_id'] == first['intent_id'] and retained['status'] == 'UNKNOWN'
    assert len(ctx.attempts) == 1 and not ctx.sdk.active_stops
    assert resumed.state.open_position(OWNER, CONID)['quantity'] == 40
    assert not ctx.faults


@pytest.mark.parametrize('receipt', [None, SimpleNamespace(), SimpleNamespace(order=SimpleNamespace(orderId=0))],
                         ids=['no-object', 'no-order', 'zero-id'])
def test_success_without_a_physical_identity_is_still_reserved(protection, receipt):
    ctx = protection
    ctx.response = FakeResult(obj=receipt)
    ctx.executor._ensure_protective(OWNER, CONID)
    first, = ctx.executor.intents.all(kind='PROTECTIVE', active=True)
    assert first['status'] == 'UNKNOWN' and not first['payload'].get('order_ids')

    ctx.response = None
    resumed = ctx.restart()
    resumed.manage_positions()
    retained, = resumed.intents.all(kind='PROTECTIVE', active=True)
    assert retained['intent_id'] == first['intent_id']
    assert len(ctx.attempts) == 1 and not ctx.sdk.active_stops
    assert not ctx.faults


@pytest.mark.parametrize('error', [ConnectionError('send receipt lost'), TimeoutError('send timed out')])
def test_exception_after_claim_keeps_the_same_unresolved_attempt(protection, error):
    ctx = protection
    ctx.response = error
    ctx.executor._ensure_protective(OWNER, CONID)
    first, = ctx.executor.intents.all(kind='PROTECTIVE', active=True)
    assert len(ctx.faults) == 1

    ctx.response = None
    resumed = ctx.restart()
    resumed.manage_positions()
    retained, = resumed.intents.all(kind='PROTECTIVE', active=True)
    assert retained['intent_id'] == first['intent_id']
    assert len(ctx.attempts) == 1 and not ctx.sdk.active_stops
    assert resumed.state.open_position(OWNER, CONID)['quantity'] == 40
    assert len(ctx.faults) == 1


def test_confirmed_live_coverage_is_reused_without_cancel_or_replacement(protection):
    ctx = protection
    ctx.executor._ensure_protective(OWNER, CONID)
    first, = ctx.executor.intents.all(kind='PROTECTIVE', active=True)
    assert first['status'] == 'WORKING' and len(first['payload']['order_ids']) == 1
    ctx.executor._ensure_protective(OWNER, CONID)
    resumed = ctx.restart()
    resumed.manage_positions()
    resumed._ensure_protective(OWNER, CONID)

    retained, = resumed.intents.all(kind='PROTECTIVE', active=True)
    assert retained['intent_id'] == first['intent_id']
    assert resumed.state.open_position(OWNER, CONID)['protective_order_id'] == first['payload']['order_ids'][0]
    assert len(ctx.attempts) == 1 and not ctx.sdk.cancel_calls
    assert sum(stop['quantity'] for stop in ctx.sdk.active_stops.values()) == 40
    assert ctx.sdk.broker[CONID] == 140
    assert not ctx.faults
