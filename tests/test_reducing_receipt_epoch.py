"""A reducing order's late receipt remains bound to its original holding."""

import json

import pandas as pd
import pytest

from review.test_review_strategy_contract import TS, make_work
from test_protective_repair_races import CONID, OWNER, repair
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutor


@pytest.mark.parametrize('late_cumulative', [10.0, 40.0])
def test_old_cancelled_close_cannot_debit_new_holding_after_request_finished(
        repair, monkeypatch, late_cumulative):
    executor, sdk, path = repair
    sdk.broker[CONID] = 100.0
    executor._process_signal(make_work(quantity=40.0))
    sdk.fill_next = 0.0
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    old, = executor.intents.all(kind='CLOSE')
    old_order_id, = old['payload']['order_ids']
    original_epoch = executor.state.open_position(OWNER, CONID)['ownership_epoch']
    assert old['payload']['ownership_epoch'] == original_epoch
    assert old['status'] == 'WORKING'
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 40.0

    # Confirm the first physical attempt cancelled without a fill. Its
    # retained exit request obtains a new attempt and finishes the old holding.
    sdk.cancel(old_order_id)
    sdk.fill_next = None
    executor._snapshot_cache = None
    executor.manage_positions()
    executor.manage_positions()
    assert executor.state.open_position(OWNER, CONID) is None
    saved = next(item for item in executor.intents.all(kind='CLOSE') if item['intent_id'] == old['intent_id'])
    assert saved['status'] == 'CANCELLED'
    assert saved['payload']['exit_request_active'] is False
    assert [row['filled'] for row in sdk.accepted if row['action'] == 'SELL'] == [0.0, 0.0, 40.0]

    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
                        (TS.tz_localize('UTC') + pd.Timedelta(minutes=3)).to_pydatetime())
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk,
                             cooldown_seconds=0)
    restarted._process_signal(make_work(quantity=60.0, bar_ts=TS + pd.Timedelta(minutes=2)))
    assert restarted.state.open_position(OWNER, CONID)['quantity'] == 60.0
    row = next(row for row in sdk.accepted if row['orderId'] == old_order_id)
    row.update(filled=late_cumulative,
               status='Filled' if late_cumulative == row['totalQuantity'] else 'Cancelled')
    sdk.broker[CONID] -= late_cumulative

    for _ in range(2):
        recovered = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        recovered._reconcile_intents(OWNER, CONID)
        assert recovered.state.open_position(OWNER, CONID)['quantity'] == 60.0
        assert recovered.state.open_position(OWNER, CONID)['ownership_epoch'] != original_epoch
        observed = next(item for item in recovered.intents.all(kind='CLOSE')
                        if item['intent_id'] == old['intent_id'])
        assert observed['payload']['ownership_epoch'] == original_epoch
        _positions, checkpoints = recovered.state.ownership_snapshot(recovered.intents.all())
        assert checkpoints[old['intent_id']] == late_cumulative
    assert sdk.broker[CONID] == 160.0 - late_cumulative
    assert len(sdk.approve_calls) == 4, 'old receipts cannot create another execution'


def test_current_holding_close_partial_receipts_still_apply_once_after_restart(repair):
    executor, sdk, path = repair
    sdk.broker[CONID] = 100.0
    executor._process_signal(make_work(quantity=40.0))
    sdk.fill_next = 10.0
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    close, = executor.intents.all(kind='CLOSE')
    close_order_id, = close['payload']['order_ids']
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 30.0

    for cumulative in (10.0, 20.0, 20.0):
        row = next(row for row in sdk.accepted if row['orderId'] == close_order_id)
        sdk.broker[CONID] -= cumulative - row['filled']
        row['filled'] = cumulative
        recovered = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        recovered._reconcile_intents(OWNER, CONID)
        assert recovered.state.open_position(OWNER, CONID)['quantity'] == 40.0 - cumulative
        _positions, checkpoints = recovered.state.ownership_snapshot(recovered.intents.all())
        assert checkpoints[close['intent_id']] == cumulative
    assert sdk.broker[CONID] == 120.0
    assert len(sdk.approve_calls) == 2


def test_legacy_close_missing_epoch_preserves_its_applied_fill_checkpoint(repair):
    executor, sdk, path = repair
    sdk.broker[CONID] = 100.0
    executor._process_signal(make_work(quantity=40.0))
    sdk.fill_next = 10.0
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    close, = executor.intents.all(kind='CLOSE')
    close_order_id, = close['payload']['order_ids']
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 30.0
    payload = {key: value for key, value in close['payload'].items()
               if key not in ('ownership_epoch', 'ownership_started_at')}
    with executor.intents.journal.transaction() as conn:
        conn.execute('UPDATE execution_intents SET payload=? WHERE intent_id=?',
                     [json.dumps(payload), close['intent_id']])
    row = next(row for row in sdk.accepted if row['orderId'] == close_order_id)
    row['filled'] = 20.0
    sdk.broker[CONID] = 120.0

    for _ in range(2):
        recovered = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        recovered._reconcile_intents(OWNER, CONID)
        assert recovered.state.open_position(OWNER, CONID)['quantity'] == 30.0
        pending, = recovered.intents.all(kind='CLOSE', active=True)
        assert pending['status'] == 'UNKNOWN' and pending['payload']['attribution_unresolved'] is True
        _positions, checkpoints = recovered.state.ownership_snapshot(recovered.intents.all())
        assert checkpoints[close['intent_id']] == 10.0
    assert sdk.broker[CONID] == 120.0
    assert len(sdk.approve_calls) == 2


@pytest.mark.parametrize('field', ['ownership_epoch', 'ownership_started_at'])
def test_submitted_close_ownership_binding_cannot_be_erased_or_changed(repair, field):
    executor, sdk, path = repair
    sdk.broker[CONID] = 100.0
    executor._process_signal(make_work(quantity=40.0))
    sdk.fill_next = 0.0
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    close, = executor.intents.all(kind='CLOSE')
    binding = {key: close['payload'][key] for key in ('ownership_epoch', 'ownership_started_at')}
    assert all(value is not None for value in binding.values())
    assert close['payload']['order_ids'], 'binding protects an actual submitted physical attempt'
    changed = 'another-ownership-epoch' if field == 'ownership_epoch' else binding[field] + 1.0

    for value in (None, changed):
        with pytest.raises(ValueError, match='immutable'):
            executor.intents.update(close, **{field: value})
    executor.intents.update(close, status='WORKING', reason='same attempt remains pending', **binding)
    recovered = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    saved, = recovered.intents.all(kind='CLOSE')
    assert {key: saved['payload'][key] for key in binding} == binding
    assert saved['payload']['reason'] == 'same attempt remains pending'
    assert recovered.state.open_position(OWNER, CONID)['quantity'] == 40.0
