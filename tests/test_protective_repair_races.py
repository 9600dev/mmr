"""Protection replacement must use observations made after cancellation.

The broker below retains terminal orders and reports explicit snapshot
completeness. Fills change both broker shares and real executor attribution;
no service, market-data feed or broker connection is used.
"""

import json

import pandas as pd
import pytest

from review.test_review_strategy_contract import LifecycleSDK, FakeResult, TS, make_work
from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutor


OWNER = 'orb_test'
CONID = 1111


class RepairSDK(LifecycleSDK):
    def __init__(self):
        super().__init__()
        self.orders_complete = True
        self.positions_complete = True
        self.cancel_fill = 0.0
        self.after_cancel = None
        self.before_snapshot = None
        self.snapshot_positions = None
        self.partial_stop_fills = {}

    def trades(self):
        frame = super().trades()
        for order_id, filled in self.partial_stop_fills.items():
            frame.loc[frame['orderId'] == order_id, 'filled'] = filled
        return frame

    def execution_snapshot(self, intent_id='', order_ids=None):
        if self.before_snapshot is not None:
            self.before_snapshot(intent_id, order_ids)
        rows = self.trades().to_dict('records')
        if intent_id:
            rows = [row for row in rows if row.get('clientIntentId') == intent_id]
        elif order_ids:
            rows = [row for row in rows if row['orderId'] in order_ids]
        positions = (self.positions().to_dict('records') if self.snapshot_positions is None
                     else self.snapshot_positions)
        return dict(complete=self.orders_complete, orders_complete=self.orders_complete,
                    executions_complete=True, positions_complete=self.positions_complete,
                    orders=rows, positions=positions)

    def finish_stop(self, order_id, filled):
        stop = self.active_stops.pop(order_id)
        already_filled = self.partial_stop_fills.pop(order_id, 0.0)
        assert already_filled <= filled <= stop['quantity']
        self.broker[CONID] -= filled - already_filled
        row = dict(orderId=order_id, orderRef=stop['order_ref'],
                   clientIntentId=stop['client_intent_id'], conId=CONID,
                   status='Filled' if filled == stop['quantity'] else 'Cancelled',
                   action='SELL', orderType='STP', totalQuantity=stop['quantity'],
                   filled=filled, avgFillPrice=stop['aux_price'], fillQuantityKnown=True)
        self.accepted.append(row)
        return row

    def reveal_stop_fill(self, order_id, cumulative):
        """A late execution receipt updates a previously acknowledged cancel."""
        row = next(row for row in self.accepted if row['orderId'] == order_id)
        assert row['filled'] <= cumulative <= row['totalQuantity']
        self.broker[CONID] -= cumulative - row['filled']
        row.update(filled=cumulative,
                   status='Filled' if cumulative == row['totalQuantity'] else 'Cancelled')

    def cancel(self, order_id):
        if order_id not in self.active_stops:
            return super().cancel(order_id)
        self.cancel_calls.append(order_id)
        if self.cancel_fails:
            return FakeResult(ok=False, error='cancellation outcome unknown')
        row = self.finish_stop(order_id, self.cancel_fill)
        if self.after_cancel is not None:
            self.after_cancel(row)
        return FakeResult(ok=True)


@pytest.fixture
def repair(tmp_path, monkeypatch):
    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '8')
    monkeypatch.setenv('MMR_STALE_BAR_MULTIPLE', '3')
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
                        (TS.tz_localize('UTC') + pd.Timedelta(seconds=70)).to_pydatetime())
    sdk = RepairSDK()
    path = str(tmp_path / 'execution.duckdb')
    executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    return executor, sdk, path


def start_resize(executor, sdk):
    sdk.broker[CONID] = 100.0  # unrelated manually held shares
    sdk.fill_next = 40.0
    executor._process_signal(make_work(quantity=100.0))
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 40
    assert len(sdk.active_stops) == 1
    old = next(iter(sdk.active_stops))
    assert sdk.active_stops[old]['quantity'] == 40
    sdk.accepted[0].update(filled=100.0, status='Filled')
    sdk.broker[CONID] = 200.0
    executor._snapshot_cache = None
    return old


def coverage(sdk):
    return sum(stop['quantity'] for stop in sdk.active_stops.values())


@pytest.mark.parametrize('filled_during_cancel', [0.0, 10.0, 40.0])
def test_replacement_uses_remaining_owned_shares_and_survives_restart(repair, filled_during_cancel):
    executor, sdk, path = repair
    old = start_resize(executor, sdk)
    sdk.cancel_fill = filled_during_cancel

    executor._ensure_protective(OWNER, CONID)

    remaining = 100.0 - filled_during_cancel
    assert executor.state.open_position(OWNER, CONID)['quantity'] == remaining
    assert sdk.broker[CONID] == remaining + 100.0
    assert sdk.cancel_calls == [old]
    assert coverage(sdk) == remaining, 'replacement must not reserve manually owned shares'
    assert [call['quantity'] for call in sdk.protective_calls] == [40.0, remaining]

    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    restarted.manage_positions()
    restarted.manage_positions()
    assert restarted.state.open_position(OWNER, CONID)['quantity'] == remaining
    assert coverage(sdk) == remaining
    assert len(sdk.protective_calls) == 2, 'replayed stop fills must not trigger duplicate repair'


def test_late_fill_after_cancel_ack_is_reconciled_before_replacement(repair):
    executor, sdk, _ = repair
    old = start_resize(executor, sdk)
    revealed = False

    def reveal_on_next_global_snapshot(intent_id, order_ids):
        nonlocal revealed
        if sdk.cancel_calls and not intent_id and not order_ids and not revealed:
            revealed = True
            sdk.reveal_stop_fill(old, 40.0)

    sdk.before_snapshot = reveal_on_next_global_snapshot
    executor._ensure_protective(OWNER, CONID)

    assert revealed
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 60.0
    assert sdk.broker[CONID] == 160.0
    assert coverage(sdk) == 60.0


def test_fresh_terminal_probe_invalidates_earlier_working_snapshot(repair):
    executor, sdk, _ = repair
    old = start_resize(executor, sdk)
    revealed = False

    def fill_before_terminal_probe(intent_id, order_ids):
        nonlocal revealed
        if not intent_id and order_ids == [old] and not revealed:
            revealed = True
            sdk.finish_stop(old, 40.0)

    sdk.before_snapshot = fill_before_terminal_probe
    executor._ensure_protective(OWNER, CONID)

    assert revealed
    assert sdk.cancel_calls == [], 'the fresh probe already proved the stop terminal'
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 60.0
    assert sdk.broker[CONID] == 160.0
    assert coverage(sdk) == 60.0


@pytest.mark.parametrize('stop_fill, remaining_owned, broker_remaining', [
    (30.0, 10.0, 0.0),
    (40.0, 0.0, -10.0),
])
def test_no_replacement_when_cancel_fill_leaves_no_reducible_holding(
        repair, stop_fill, remaining_owned, broker_remaining):
    executor, sdk, _ = repair
    sdk.broker[CONID] = 100.0
    executor._process_signal(make_work(quantity=40.0))
    old = next(iter(sdk.active_stops))
    # An external sale leaves a net manual short of ten shares. The old stop
    # now exceeds the broker's thirty-share net long and requires resizing.
    sdk.broker[CONID] = 30.0
    sdk.cancel_fill = stop_fill
    executor._snapshot_cache = None

    executor._ensure_protective(OWNER, CONID)

    position = executor.state.open_position(OWNER, CONID)
    assert (position['quantity'] if position else 0.0) == remaining_owned
    assert sdk.broker[CONID] == broker_remaining
    assert sdk.cancel_calls == [old]
    assert len(sdk.protective_calls) == 1
    assert not sdk.active_stops


@pytest.mark.parametrize('unreadable', ['incomplete', 'quantity', 'missing'])
def test_unreadable_refreshed_position_defers_replacement(repair, unreadable):
    executor, sdk, _ = repair
    old = start_resize(executor, sdk)

    def invalidate_position(_row):
        if unreadable == 'incomplete':
            sdk.positions_complete = False
        elif unreadable == 'quantity':
            sdk.snapshot_positions = [dict(conId=CONID, position=float('nan'), avgCost=100.0)]
        else:
            sdk.snapshot_positions = []

    sdk.after_cancel = invalidate_position
    executor._ensure_protective(OWNER, CONID)

    assert sdk.cancel_calls == [old]
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 100.0
    assert len(sdk.protective_calls) == 1
    assert not sdk.active_stops
    # A readable display cache must not substitute for the failed fresh read.
    assert sdk.positions().iloc[0]['position'] == 200.0


@pytest.mark.parametrize('account_cost', [float('nan'), 200.0], ids=['unreadable-account-cost', 'changed-account-cost'])
def test_replacement_preserves_owned_fill_basis_when_account_cost_changes(repair, account_cost):
    executor, sdk, _ = repair
    old = start_resize(executor, sdk)
    # The accepted opening fill's quote-price evidence stays at 100. Only the
    # account aggregate changes during cancellation, independently of this
    # strategy's attributed holding and its committed fill-price checkpoints.
    assert sdk.accepted[0]['avgFillPrice'] == 100.0
    sdk.after_cancel = lambda _row: setattr(sdk, 'avg_cost', account_cost)

    executor._ensure_protective(OWNER, CONID)

    position = executor.state.open_position(OWNER, CONID)
    assert sdk.cancel_calls == [old]
    assert position['quantity'] == 100.0
    assert position['avg_cost'] == 100.0
    assert position['cost_evaluable'] is True
    assert sdk.broker[CONID] == 200.0
    assert coverage(sdk) == 100.0
    assert sdk.protective_calls[-1]['aux_price'] == pytest.approx(92.0)


def test_unknown_final_stop_quantity_keeps_replacement_reserved(repair):
    executor, sdk, _ = repair
    old = start_resize(executor, sdk)
    sdk.cancel_fill = 10.0

    def incomplete_final_quantity(row):
        # This is the native tracker shape for a broker-terminal order whose
        # final cumulative fill remains unknown after bounded history replay.
        row.update(status='Unknown', brokerStatus='Cancelled', fillQuantityKnown=False)

    sdk.after_cancel = incomplete_final_quantity
    executor._ensure_protective(OWNER, CONID)
    executor._snapshot_cache = None
    executor._ensure_protective(OWNER, CONID)

    assert sdk.cancel_calls == [old]
    assert executor.state.open_position(OWNER, CONID)['quantity'] == 90.0
    assert len(sdk.protective_calls) == 1
    assert not sdk.active_stops
    pending = executor.intents.all(strategy=OWNER, conid=CONID, kind='PROTECTIVE', active=True)
    assert len(pending) == 1
    assert pending[0]['status'] == 'UNKNOWN'


@pytest.mark.parametrize('late_cumulative', [10.0, 40.0])
def test_old_executor_stop_receipt_cannot_debit_a_new_holding_after_restart(
        repair, monkeypatch, late_cumulative):
    executor, sdk, path = repair
    sdk.broker[CONID] = 100.0
    executor._process_signal(make_work(quantity=40.0))
    old = next(iter(sdk.active_stops))
    old_intent, = executor.intents.all(kind='PROTECTIVE')
    assert not old_intent['payload'].get('adopted'), 'this stop was created by the executor itself'

    # The original holding closes after a known zero-fill cancel. That
    # terminal observation permits a later fresh entry; its cumulative fill
    # may still be corrected by a delayed broker receipt.
    executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    executor.manage_positions()
    assert executor.state.open_position(OWNER, CONID) is None
    assert next(row for row in sdk.accepted if row['orderId'] == old)['filled'] == 0

    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
                        (TS.tz_localize('UTC') + pd.Timedelta(minutes=3)).to_pydatetime())
    restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk,
                             cooldown_seconds=0)
    restarted._process_signal(make_work(quantity=60.0, bar_ts=TS + pd.Timedelta(minutes=2)))
    assert restarted.state.open_position(OWNER, CONID)['quantity'] == 60.0
    assert len(sdk.protective_calls) == 2
    sdk.reveal_stop_fill(old, late_cumulative)

    for _ in range(2):
        recovered = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        recovered._reconcile_intents(OWNER, CONID)
        assert recovered.state.open_position(OWNER, CONID)['quantity'] == 60.0
        _positions, checkpoints = recovered.state.ownership_snapshot(recovered.intents.all())
        assert checkpoints[old_intent['intent_id']] == late_cumulative
    assert sdk.broker[CONID] == 160.0 - late_cumulative
    assert len(sdk.protective_calls) == 2, 'reconciling the old receipt must place no new stop'


def test_legacy_executor_stop_without_ownership_epoch_keeps_fill_unresolved(repair):
    executor, sdk, path = repair
    sdk.broker[CONID] = 100.0
    executor._process_signal(make_work(quantity=40.0))
    old = next(iter(sdk.active_stops))
    intent, = executor.intents.all(kind='PROTECTIVE')
    # Persist the historical payload shape. A later migration/read cannot
    # manufacture proof that this old broker order belongs to the current
    # owned row, even if its bar timestamp happens to match.
    payload = {key: value for key, value in intent['payload'].items()
               if key not in ('ownership_epoch', 'ownership_started_at')}
    with executor.intents.journal.transaction() as conn:
        conn.execute('UPDATE execution_intents SET payload=? WHERE intent_id=?',
                     [json.dumps(payload), intent['intent_id']])
    sdk.finish_stop(old, 10.0)

    for _ in range(2):
        recovered = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        recovered._reconcile_intents(OWNER, CONID)
        recovered._ensure_protective(OWNER, CONID)
        assert recovered.state.open_position(OWNER, CONID)['quantity'] == 40.0
        pending, = recovered.intents.all(kind='PROTECTIVE', active=True)
        assert pending['status'] == 'UNKNOWN'
        assert pending['payload']['attribution_unresolved'] is True
        _positions, checkpoints = recovered.state.ownership_snapshot(recovered.intents.all())
        assert checkpoints.get(intent['intent_id'], 0.0) == 0.0
    assert sdk.broker[CONID] == 130.0
    assert len(sdk.protective_calls) == 1


def test_legacy_working_stop_keeps_protection_when_fill_ownership_is_unresolved(repair):
    executor, sdk, path = repair
    sdk.broker[CONID] = 100.0
    executor._process_signal(make_work(quantity=40.0))
    old = next(iter(sdk.active_stops))
    intent, = executor.intents.all(kind='PROTECTIVE')
    payload = {key: value for key, value in intent['payload'].items()
               if key not in ('ownership_epoch', 'ownership_started_at')}
    with executor.intents.journal.transaction() as conn:
        conn.execute('UPDATE execution_intents SET payload=? WHERE intent_id=?',
                     [json.dumps(payload), intent['intent_id']])
    sdk.partial_stop_fills[old] = 10.0
    sdk.broker[CONID] = 130.0
    sdk.cancel_fill = 10.0  # cancellation would confirm the already observed partial fill

    for _ in range(2):
        recovered = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        recovered._ensure_protective(OWNER, CONID)
        assert sdk.cancel_calls == [], 'unresolved attribution must preserve the existing broker protection'
        assert set(sdk.active_stops) == {old}
        row, = [row for row in sdk.trades().to_dict('records') if row['orderId'] == old]
        assert row['status'] == 'Submitted' and row['totalQuantity'] - row['filled'] == 30.0
        assert recovered.state.open_position(OWNER, CONID)['quantity'] == 40.0
    assert sdk.broker[CONID] == 130.0
    assert len(sdk.protective_calls) == 1


@pytest.mark.parametrize('saved_ids', [[], [0]])
def test_unresolved_protective_without_cancellable_id_blocks_new_close(repair, saved_ids):
    executor, sdk, path = repair
    sdk.broker[CONID] = 100.0
    executor._process_signal(make_work(quantity=40.0))
    old = next(iter(sdk.active_stops))
    intent, = executor.intents.all(kind='PROTECTIVE')
    payload = {key: value for key, value in intent['payload'].items()
               if key not in ('ownership_epoch', 'ownership_started_at')}
    payload['order_ids'] = saved_ids
    with executor.intents.journal.transaction() as conn:
        conn.execute('UPDATE execution_intents SET payload=? WHERE intent_id=?',
                     [json.dumps(payload), intent['intent_id']])
    row = sdk.finish_stop(old, 10.0)
    row['orderId'] = 0  # bounded completed-order replay may omit the cancel ID
    executor.state.set_protective(OWNER, CONID, None)

    recovered = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    recovered._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))

    assert [call['action'] for call in sdk.propose_calls] == ['BUY']
    assert sdk.cancel_calls == []
    assert recovered.state.open_position(OWNER, CONID)['quantity'] == 40.0
    pending, = recovered.intents.all(kind='PROTECTIVE', active=True)
    assert pending['status'] == 'UNKNOWN' and pending['payload']['attribution_unresolved'] is True
    assert sdk.broker[CONID] == 130.0
