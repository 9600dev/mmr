"""Cancellation outcome and cache invalidation, using complete broker records."""
from copy import deepcopy

import pytest

from review.test_review_strategy_contract import FakeResult
from test_execution_recovery import recovery


def _snapshot_sdk(executor, sdk, monkeypatch, *, acknowledgment):
    row = dict(orderId=711, conId=1111, action='SELL', orderType='STP',
               orderRef='orb_test', clientIntentId='protective-current',
               status='Submitted', brokerStatus='Submitted', filled=0.0,
               fillQuantityKnown=True, remaining=40.0, totalQuantity=40.0)
    calls = []

    def snapshot(*, intent_id='', order_ids=None):
        calls.append((intent_id, tuple(order_ids or ())))
        rows = [deepcopy(row)] if order_ids is None or 711 in order_ids else []
        return dict(complete=True, orders=rows, positions_complete=True,
                    positions=[dict(conId=1111, position=140.0, avgCost=100.0)])

    def cancel(order_id):
        assert order_id == 711
        row.update(status='Cancelled', brokerStatus='Cancelled', remaining=40.0)
        return acknowledgment

    monkeypatch.setattr(sdk, 'execution_snapshot', snapshot, raising=False)
    monkeypatch.setattr(sdk, 'cancel', cancel)
    return row, calls


@pytest.mark.parametrize('legacy_none_ack', [False, True])
def test_cancel_invalidates_every_cached_scope_before_the_next_read(recovery, monkeypatch, legacy_none_ack):
    executor, sdk, _ = recovery
    acknowledgment = None if legacy_none_ack else FakeResult()
    _row, calls = _snapshot_sdk(executor, sdk, monkeypatch, acknowledgment=acknowledgment)
    intent = executor.intents.create('orb_test', 1111, 'PROTECTIVE',
                                    dict(order_ids=[711], broker_intent_id='protective-current'))
    assert executor._execution_snapshot()['orders'][0]['status'] == 'Submitted'
    assert executor._execution_snapshot(intent)['orders'][0]['status'] == 'Submitted'
    assert executor._cancel_order(711) is True
    # Both cached observations must be replaceable after the broker mutation.
    assert executor._execution_snapshot()['orders'][0]['status'] == 'Cancelled'
    assert executor._execution_snapshot(intent)['orders'][0]['status'] == 'Cancelled'
    assert any(intent_id == 'protective-current' for intent_id, _ in calls)


def test_failed_cancel_ack_is_not_relabelled_success_by_a_later_terminal_read(recovery, monkeypatch):
    executor, sdk, _ = recovery
    row, _calls = _snapshot_sdk(executor, sdk, monkeypatch,
                                acknowledgment=FakeResult(ok=False, error='cancel reply unavailable'))
    assert executor._cancel_order(711) is False
    # A later management turn can use the independently observed terminal fact;
    # the failed operation's result itself must remain a failed acknowledgment.
    assert row['status'] == 'Cancelled'
    assert executor._order_is_terminal(711) is True
