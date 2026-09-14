"""Emergency sizing must use the fill progress committed with owned inventory."""
import pandas as pd
import pytest

from test_execution_recovery import recovery
from review.test_review_strategy_contract import FakeResult, TS, make_work
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutor


def _emergency_after_storage_failure(executor, sdk, monkeypatch):
    calls = []

    def unavailable(*args, **kwargs):
        raise OSError('local execution storage is unavailable')

    def emergency_close_position(**kwargs):
        calls.append(kwargs)
        # The server coordinates cancellation before this final reduction.
        for order in sdk.accepted:
            if order['status'] != 'Filled':
                order['status'] = 'Cancelled'
        sdk.active_stops.clear()
        sdk.broker[1111] -= kwargs['quantity']
        return FakeResult(obj=[1999])

    monkeypatch.setattr(executor.intents, 'all', unavailable)
    monkeypatch.setattr(executor.state, 'open_position', unavailable)
    monkeypatch.setattr(sdk, 'emergency_close_position', emergency_close_position, raising=False)
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=3)))
    assert len(calls) == 1
    return calls[0]['quantity']


@pytest.mark.parametrize('kind', ['OPEN', 'CLOSE'])
def test_incomplete_split_snapshot_cannot_reapply_committed_fills_during_emergency(recovery, monkeypatch, kind):
    executor, sdk, _ = recovery
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    sdk.broker[1111] = 100  # unrelated manual inventory
    if kind == 'CLOSE':
        executor._process_signal(make_work(quantity=140))
    sdk.fill_next = 80
    executor._process_signal(make_work(
        quantity=140, action=Action.BUY if kind == 'OPEN' else Action.SELL,
        bar_ts=TS if kind == 'OPEN' else TS + pd.Timedelta(minutes=1)))
    expected_owned = 80 if kind == 'OPEN' else 60
    intent, = executor.intents.all(kind=kind)
    for row in sdk.accepted:
        row['orderType'] = 'MKT'
    # Two physical legs each contributed 40. Bounded replay later omits one.
    first = sdk.accepted[-1]
    first.update(filled=40, totalQuantity=70)
    second = dict(first, orderId=first['orderId'] + 1)
    sdk.accepted.append(second)
    executor.manage_positions()
    assert executor.state.open_position('orb_test', 1111)['quantity'] == expected_owned

    missing_leg = True

    def execution_snapshot(intent_id='', order_ids=None):
        omitted = bool(missing_leg and intent_id == intent['intent_id'])
        rows = [dict(row) for row in sdk.accepted if not (omitted and row is second)]
        return dict(complete=not omitted, orders=rows, positions_complete=True,
                    positions=sdk.positions().to_dict('records'))

    monkeypatch.setattr(sdk, 'execution_snapshot', execution_snapshot, raising=False)
    executor.manage_positions()
    assert executor.intents.all(kind=kind)[0]['status'] == 'UNKNOWN'
    assert executor.state.open_position('orb_test', 1111)['quantity'] == expected_owned
    missing_leg = False
    quantity = _emergency_after_storage_failure(executor, sdk, monkeypatch)
    assert quantity == expected_owned
    assert sdk.broker[1111] == 100


@pytest.mark.parametrize('kind', ['OPEN', 'CLOSE'])
def test_committed_fill_before_intent_ack_is_not_counted_again_after_restart_outage(recovery, monkeypatch, kind):
    executor, sdk, path = recovery
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    sdk.broker[1111] = 100
    if kind == 'CLOSE':
        executor._process_signal(make_work(quantity=140))
    sdk.fill_next = 80
    update = executor.intents.update

    def crash_before_ack(intent, **kwargs):
        if intent['kind'] == kind and 'cumulative_filled' in kwargs:
            raise SystemExit('process ended after owned inventory committed')
        return update(intent, **kwargs)

    monkeypatch.setattr(executor.intents, 'update', crash_before_ack)
    with pytest.raises(SystemExit, match='owned inventory committed'):
        executor._process_signal(make_work(
            quantity=140, action=Action.BUY if kind == 'OPEN' else Action.SELL,
            bar_ts=TS if kind == 'OPEN' else TS + pd.Timedelta(minutes=1)))
    expected_owned = 80 if kind == 'OPEN' else 60
    assert executor.state.open_position('orb_test', 1111)['quantity'] == expected_owned
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    # Storage is lost before restart reconciliation can acknowledge that fill.
    quantity = _emergency_after_storage_failure(restarted, sdk, monkeypatch)
    assert quantity == expected_owned
    assert sdk.broker[1111] == 100


@pytest.mark.parametrize('kind', ['OPEN', 'CLOSE'])
def test_fill_after_cached_inventory_still_adjusts_emergency_quantity(recovery, monkeypatch, kind):
    executor, sdk, _ = recovery
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    sdk.broker[1111] = 100
    if kind == 'CLOSE':
        executor._process_signal(make_work(quantity=140))
    sdk.fill_next = 80
    executor._process_signal(make_work(
        quantity=140, action=Action.BUY if kind == 'OPEN' else Action.SELL,
        bar_ts=TS if kind == 'OPEN' else TS + pd.Timedelta(minutes=1)))
    # A later broker fill was never applied to inventory before storage failed.
    sdk.accepted[-1]['filled'] = 100
    sdk.broker[1111] += 20 if kind == 'OPEN' else -20
    quantity = _emergency_after_storage_failure(executor, sdk, monkeypatch)
    assert quantity == (100 if kind == 'OPEN' else 40)
    assert sdk.broker[1111] == 100
