"""Operator-attested adoption of holdings that predate ownership epochs (review 2026-09-11).

The ownership_epoch migration is a bare ADD COLUMN and nothing assigned an
epoch to rows attributed by the previous build, so the executor could never
close, emergency-close or re-protect a position held at deploy time. Adoption
is the explicit act the execution contract calls for: broker-corroborated,
single-shot, and it seeds the cost basis the protective plan needs.

Messages are asserted exactly (anchored) on purpose: they are what the operator
acts on, and the mutation gate otherwise cannot tell a changed message from the
original.
"""
import asyncio
import datetime as dt
import math
import threading
import time
from types import SimpleNamespace

import pytest

from trader.messaging.strategy_service_api import StrategyServiceApi
from trader.strategy.auto_executor import AutoExecutionError, AutoExecutor, AutoExecState
from trader.strategy.execution_intents import IntentStore

OWNER, CONID = 'legacy_owner', 4391
ENTRY = dt.datetime(2026, 9, 1, 15, tzinfo=dt.timezone.utc)
COST_COLUMNS = ('sequence, ownership_epoch, intent_id, start_quantity, end_quantity, '
                'attributed_delta, cumulative_quote_notional, price_conflict')


def _legacy_executor(tmp_path, *, broker_position=10.0, avg_cost=25.5, complete=True):
    executor = AutoExecutor.__new__(AutoExecutor)
    path = str(tmp_path / 'legacy.duckdb')
    executor.state = AutoExecState(path)
    executor.intents = IntentStore(path)
    executor.state.record_open(OWNER, CONID, 10, ENTRY, 51, None, None)
    # The previous build's row: attributed, with no ownership evidence at all.
    executor.state.db.execute(
        "UPDATE auto_exec_positions SET ownership_epoch=NULL, ownership_started_at=NULL "
        "WHERE strategy=? AND conid=?", [OWNER, CONID])
    executor._view_lock = threading.Lock()
    executor._emergency_exits = {}
    executor._managed_view = []
    executor._ownership_warnings = {(OWNER, CONID, 'legacy holding')}
    executor._snapshot_cache = None
    executor._first_empty_broker_read = None
    snapshot = {'complete': True, 'positions_complete': complete, 'retry_safe': False, 'orders': [],
                'positions': [{'conId': CONID, 'position': broker_position, 'avgCost': avg_cost}]}
    sdk = SimpleNamespace(execution_snapshot=lambda **_kwargs: snapshot)
    executor._get_sdk = lambda: sdk
    executor.management_requests = []
    executor.submit_management = lambda: executor.management_requests.append(True)
    return executor


def _cost_events(state):
    return state.db.execute(f'SELECT {COST_COLUMNS} FROM auto_exec_cost_events ORDER BY sequence', [], fetch='all')


# --- executor level -----------------------------------------------------------

def test_legacy_row_is_unusable_until_adopted(tmp_path):
    executor = _legacy_executor(tmp_path)
    before = executor.state.open_position(OWNER, CONID)
    assert before['ownership_epoch'] is None and before['cost_evaluable'] is False
    started_before = time.time()

    adopted = executor.adopt_legacy_holding(OWNER, CONID)

    after = executor.state.open_position(OWNER, CONID)
    assert adopted == {'strategy': OWNER, 'conid': CONID, 'quantity': 10.0, 'avg_cost': 25.5,
                       'ownership_epoch': after['ownership_epoch'],
                       'ownership_started_at': after['ownership_started_at']}
    assert len(after['ownership_epoch']) == 32
    assert started_before <= after['ownership_started_at'] <= time.time()
    assert after['cost_evaluable'] is True and after['avg_cost'] == pytest.approx(25.5)
    assert executor.management_requests == [True], 'the worker re-reads ownership on its next cycle'
    assert (OWNER, CONID, 'legacy holding') not in executor._ownership_warnings
    (sequence, epoch, attestation, start, end, delta, notional, conflict), = _cost_events(executor.state)
    assert (sequence, epoch, start, end, delta, notional, conflict) == (1, after['ownership_epoch'], 0.0, 10.0, 10.0, 255.0, False)
    prefix, stamp = attestation.split(':', 1)
    assert prefix == 'legacy-adoption'
    assert dt.datetime.fromisoformat(stamp).tzinfo is not None, 'the attestation instant is recorded, not observed'


def test_explicit_cost_basis_overrides_the_broker_average(tmp_path):
    executor = _legacy_executor(tmp_path, avg_cost=25.5)
    adopted = executor.adopt_legacy_holding(OWNER, CONID, avg_cost=30)
    assert adopted['avg_cost'] == 30.0 and isinstance(adopted['avg_cost'], float)
    assert executor.state.open_position(OWNER, CONID)['avg_cost'] == pytest.approx(30.0)


@pytest.mark.parametrize('kwargs, call, message', [
    (dict(broker_position=5.0), {}, f'broker holds 5 of conId {CONID}, less than the 10 attributed to {OWNER}; reconcile before adopting'),
    # A non-finite broker quantity makes the whole position read incomplete.
    (dict(broker_position=float('nan')), {}, 'broker position snapshot is incomplete; retry once the trader is connected'),
    (dict(complete=False), {}, 'broker position snapshot is incomplete; retry once the trader is connected'),
    (dict(avg_cost=None), {}, 'broker average cost is unavailable; pass the cost basis explicitly'),
    (dict(avg_cost=0.0), {}, 'broker average cost is unavailable; pass the cost basis explicitly'),
    (dict(avg_cost='n/a'), {}, 'broker average cost is unavailable; pass the cost basis explicitly'),
    (dict(), {'avg_cost': 0.0}, 'average cost must be finite and positive'),
    (dict(), {'avg_cost': -1.0}, 'average cost must be finite and positive'),
    (dict(), {'avg_cost': float('inf')}, 'average cost must be finite and positive'),
])
def test_adoption_requires_broker_corroboration_and_a_cost_basis(tmp_path, kwargs, call, message):
    executor = _legacy_executor(tmp_path, **kwargs)
    with pytest.raises(AutoExecutionError) as refused:
        executor.adopt_legacy_holding(OWNER, CONID, **call)
    assert str(refused.value) == message
    assert executor.state.open_position(OWNER, CONID)['ownership_epoch'] is None
    assert _cost_events(executor.state) == []
    assert executor.management_requests == []
    assert (OWNER, CONID, 'legacy holding') in executor._ownership_warnings


def test_broker_holding_exactly_the_attributed_quantity_is_enough(tmp_path):
    executor = _legacy_executor(tmp_path, broker_position=10.0)
    assert executor.adopt_legacy_holding(OWNER, CONID)['quantity'] == 10.0


def test_adoption_is_single_shot_and_needs_an_attributed_holding(tmp_path):
    executor = _legacy_executor(tmp_path)
    adopted = executor.adopt_legacy_holding(OWNER, CONID)
    with pytest.raises(AutoExecutionError) as repeat:
        executor.adopt_legacy_holding(OWNER, CONID)
    assert str(repeat.value) == f'{OWNER}/{CONID} already has ownership epoch {adopted["ownership_epoch"]}'
    with pytest.raises(AutoExecutionError) as absent:
        executor.adopt_legacy_holding('someone_else', CONID)
    assert str(absent.value) == f'someone_else/{CONID} has no attributed OPEN holding to adopt'
    assert len(_cost_events(executor.state)) == 1


def test_rpc_wraps_adoption_and_reports_a_missing_executor(tmp_path):
    executor = _legacy_executor(tmp_path)
    api = StrategyServiceApi(SimpleNamespace(auto_executor=executor))
    result = asyncio.run(api.adopt_legacy_holding(OWNER, CONID))
    assert result.is_success(), result.error
    assert result.obj['ownership_epoch'] == executor.state.open_position(OWNER, CONID)['ownership_epoch']

    repeat = asyncio.run(api.adopt_legacy_holding(OWNER, CONID))
    assert not repeat.is_success() and 'already has ownership epoch' in repeat.error

    absent = asyncio.run(StrategyServiceApi(SimpleNamespace(auto_executor=None)).adopt_legacy_holding(OWNER, CONID))
    assert not absent.is_success()
    assert absent.error == 'auto-executor is not running in this strategy service'


# --- state level: the guards the executor normally pre-empts -----------------

@pytest.mark.parametrize('quantity, avg_cost, message', [
    (0.0, 25.5, 'legacy adoption requires the attributed positive quantity'),
    (-10.0, 25.5, 'legacy adoption requires the attributed positive quantity'),
    (float('nan'), 25.5, 'legacy adoption requires the attributed positive quantity'),
    (10.0, 0.0, 'legacy adoption requires a finite positive average cost'),
    (10.0, -25.5, 'legacy adoption requires a finite positive average cost'),
    (10.0, float('inf'), 'legacy adoption requires a finite positive average cost'),
])
def test_state_guards_reject_bad_inputs_before_any_write(tmp_path, quantity, avg_cost, message):
    state = _legacy_executor(tmp_path).state
    with pytest.raises(ValueError) as refused:
        state.adopt_legacy_ownership(OWNER, CONID, quantity, avg_cost, 'legacy-adoption:test')
    assert str(refused.value) == message
    assert state.open_position(OWNER, CONID)['ownership_epoch'] is None
    assert _cost_events(state) == []


def test_state_adoption_is_transactional_against_the_current_row(tmp_path):
    state = _legacy_executor(tmp_path).state
    with pytest.raises(ValueError) as absent:
        state.adopt_legacy_ownership('someone_else', CONID, 10.0, 25.5, 'legacy-adoption:test')
    assert str(absent.value) == f'someone_else/{CONID} has no attributed OPEN holding'
    with pytest.raises(ValueError) as stale:
        state.adopt_legacy_ownership(OWNER, CONID, 7.0, 25.5, 'legacy-adoption:test')
    assert str(stale.value) == 'attributed quantity is 10, not 7; re-read the holding before attesting'
    assert state.open_position(OWNER, CONID)['ownership_epoch'] is None
    assert _cost_events(state) == []

    adopted = state.adopt_legacy_ownership(OWNER, CONID, 10.0, 25.5, 'legacy-adoption:first')

    assert set(adopted) == {'strategy', 'conid', 'quantity', 'avg_cost', 'ownership_epoch', 'ownership_started_at'}
    position = state.open_position(OWNER, CONID)
    assert position['ownership_epoch'] == adopted['ownership_epoch']
    assert position['ownership_started_at'] == adopted['ownership_started_at']
    assert math.isfinite(adopted['ownership_started_at'])
    assert _cost_events(state) == [(1, adopted['ownership_epoch'], 'legacy-adoption:first', 0.0, 10.0, 10.0, 255.0, False)]
    with pytest.raises(ValueError) as again:
        state.adopt_legacy_ownership(OWNER, CONID, 10.0, 25.5, 'legacy-adoption:second')
    assert str(again.value) == f'{OWNER}/{CONID} already has ownership epoch {adopted["ownership_epoch"]}'
    assert len(_cost_events(state)) == 1


def test_second_adoption_event_sequences_after_existing_cost_history(tmp_path):
    """The cost-event sequence is global: a second adopted holding continues it."""
    executor = _legacy_executor(tmp_path)
    executor.state.record_open('other_owner', 7777, 3, ENTRY, 52, None, None)
    executor.state.db.execute(
        "UPDATE auto_exec_positions SET ownership_epoch=NULL, ownership_started_at=NULL "
        "WHERE strategy=? AND conid=?", ['other_owner', 7777])
    first = executor.state.adopt_legacy_ownership(OWNER, CONID, 10.0, 25.5, 'legacy-adoption:a')
    second = executor.state.adopt_legacy_ownership('other_owner', 7777, 3.0, 4.0, 'legacy-adoption:b')
    assert first['ownership_epoch'] != second['ownership_epoch']
    assert [(row[0], row[1], row[6]) for row in _cost_events(executor.state)] == [
        (1, first['ownership_epoch'], 255.0), (2, second['ownership_epoch'], 12.0)]
    assert executor.state.open_position('other_owner', 7777)['avg_cost'] == pytest.approx(4.0)


# --- boundaries and adapter paths the mutation gate showed were unpinned ------

def test_fractional_cost_basis_and_quantity_boundaries(tmp_path):
    """Guards are strict-positive, not greater-than-one: a penny-stock basis
    is valid, and a sub-share quantity fails the ROW match, not the guard."""
    state = _legacy_executor(tmp_path).state
    with pytest.raises(ValueError) as stale:
        state.adopt_legacy_ownership(OWNER, CONID, 0.5, 25.5, 'legacy-adoption:test')
    assert str(stale.value) == 'attributed quantity is 10, not 0.5; re-read the holding before attesting'
    adopted = state.adopt_legacy_ownership(OWNER, CONID, 10.0, 0.5, 'legacy-adoption:penny')
    assert adopted['avg_cost'] == 0.5
    assert state.open_position(OWNER, CONID)['avg_cost'] == pytest.approx(0.5)


def test_penny_broker_average_and_explicit_penny_basis_are_accepted(tmp_path):
    executor = _legacy_executor(tmp_path, avg_cost=0.5)
    assert executor.adopt_legacy_holding(OWNER, CONID)['avg_cost'] == 0.5
    executor = _legacy_executor(tmp_path / 'explicit', avg_cost=25.5)
    assert executor.adopt_legacy_holding(OWNER, CONID, avg_cost=0.75)['avg_cost'] == 0.75


def test_instrument_absent_from_the_broker_read_counts_as_zero_held(tmp_path):
    executor = _legacy_executor(tmp_path)
    snapshot = executor._get_sdk().execution_snapshot()
    snapshot['positions'] = [{'conId': 999, 'position': 10.0, 'avgCost': 1.0}]
    with pytest.raises(AutoExecutionError) as refused:
        executor.adopt_legacy_holding(OWNER, CONID)
    assert str(refused.value) == f'broker holds 0 of conId {CONID}, less than the 10 attributed to {OWNER}; reconcile before adopting'


def test_adoption_invalidates_the_snapshot_cache_for_the_next_cycle(tmp_path):
    executor = _legacy_executor(tmp_path)
    executor._snapshot_cache = {('', ()): {'stale': True}}
    executor.adopt_legacy_holding(OWNER, CONID)
    assert executor._snapshot_cache is None
    assert executor._execution_snapshot()['positions_complete'] is True, 'a fresh read follows'


def _frame_sdk(rows, complete=True):
    import pandas as pd
    frame = pd.DataFrame(rows)
    frame.attrs['complete'] = complete
    return SimpleNamespace(positions=lambda: frame)


def test_dataframe_positions_adapter_supplies_the_cost_basis(tmp_path):
    executor = _legacy_executor(tmp_path)
    executor._get_sdk = lambda: _frame_sdk([{'conId': CONID, 'position': 10.0, 'avgCost': 12.25}])
    adopted = executor.adopt_legacy_holding(OWNER, CONID)
    assert adopted['avg_cost'] == 12.25
    assert executor.state.open_position(OWNER, CONID)['avg_cost'] == pytest.approx(12.25)


def test_dataframe_positions_adapter_without_a_cost_refuses(tmp_path):
    executor = _legacy_executor(tmp_path)
    executor._get_sdk = lambda: _frame_sdk([{'conId': CONID, 'position': 10.0}])
    with pytest.raises(AutoExecutionError) as refused:
        executor.adopt_legacy_holding(OWNER, CONID)
    assert str(refused.value) == 'broker average cost is unavailable; pass the cost basis explicitly'
    executor._get_sdk = lambda: _frame_sdk([], complete=True)
    assert executor._broker_average_cost(CONID) is None


def test_broker_average_cost_scans_past_other_instruments_and_frames(tmp_path):
    import pandas as pd
    executor = _legacy_executor(tmp_path)
    snapshot = executor._get_sdk().execution_snapshot()
    snapshot['positions'] = [{'position': 1.0, 'avgCost': 99.0},           # no conId at all
                             {'conId': 555, 'position': 1.0, 'avgCost': 98.0},
                             {'conId': CONID, 'position': 10.0, 'avgCost': 25.5},
                             {'conId': CONID, 'position': 10.0, 'avgCost': 1.0}]  # first match wins
    assert executor._broker_average_cost(CONID) == 25.5
    executor._snapshot_cache = None
    snapshot['positions'] = pd.DataFrame([{'conId': CONID, 'position': 10.0, 'avgCost': 7.5}])
    assert executor._broker_average_cost(CONID) == 7.5
    executor._snapshot_cache = None
    del snapshot['positions']
    assert executor._broker_average_cost(CONID) is None


def test_dataframe_adapter_returning_no_frame_yields_no_cost(tmp_path):
    executor = _legacy_executor(tmp_path)
    executor._get_sdk = lambda: SimpleNamespace(positions=lambda: None)
    assert executor._broker_average_cost(CONID) is None
