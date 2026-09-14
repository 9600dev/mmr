"""Protection uses exact ownership, fresh position inputs and live stop types."""
from types import SimpleNamespace

import pandas as pd
import pytest

from trader.strategy.auto_executor import AutoExecutor, AutoExecutionError


OWNER = 'protective_contract'
CONID = 1111


@pytest.fixture
def owner(monkeypatch):
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '8')
    executor = AutoExecutor.__new__(AutoExecutor)
    executor._snapshot_cache = None
    return executor


@pytest.mark.parametrize(('owned', 'broker', 'expected'), [(30.0, 130.0, 30.0), (130.0, 30.0, 30.0)])
def test_native_plan_caps_both_owned_and_broker_inventory(owner, owned, broker, expected):
    owner._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: None)
    snapshot = {'positions_complete': True, 'positions': [
        {'conId': 2222, 'position': 999.0, 'avgCost': 1.0},
        {'conId': CONID, 'position': broker, 'avgCost': 200.0},
    ]}
    plan = owner._protective_plan(CONID, {'quantity': owned, 'avg_cost': 200.0}, snapshot)
    assert plan is not None
    assert plan.quantity == expected
    assert plan.stop_price == pytest.approx(184.0)


@pytest.mark.parametrize('snapshot', [
    {'positions_complete': False, 'positions': [{'conId': CONID, 'position': 40, 'avgCost': 100}]},
    {'positions': [{'conId': CONID, 'position': 40, 'avgCost': 100}]},
    {'positions_complete': True, 'positions': None},
    {'positions_complete': True, 'positions': []},
    {'positions_complete': True, 'positions': [{'conId': 2222, 'position': 40, 'avgCost': 100}]},
    {'positions_complete': True, 'positions': [{'conId': CONID, 'position': float('nan'), 'avgCost': 100}]},
])
def test_native_unusable_position_never_falls_back_to_display_inventory(owner, snapshot):
    def display_positions():
        pytest.fail('native completeness cannot fall back to cached display positions')

    owner._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: None, positions=display_positions)
    assert owner._protective_plan(CONID, {'quantity': 40.0, 'avg_cost': 100.0}, snapshot) is None


def test_known_owned_cost_does_not_depend_on_account_average_cost(owner):
    owner._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: None)
    snapshot = {'positions_complete': True, 'positions': [
        {'conId': CONID, 'position': 40.0, 'avgCost': float('nan')},
    ]}
    plan = owner._protective_plan(CONID, {'quantity': 40.0, 'avg_cost': 100.0}, snapshot)
    assert plan is not None
    assert (plan.quantity, plan.stop_price) == (40.0, 92.0)


@pytest.mark.parametrize('cost', [None, float('nan'), float('inf'), 0.0, -1.0],
                         ids=['unknown', 'nan', 'infinite', 'zero', 'negative'])
def test_unusable_owned_cost_never_falls_back_to_account_average(owner, cost):
    owner._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: None)
    snapshot = {'positions_complete': True, 'positions': [
        {'conId': CONID, 'position': 40.0, 'avgCost': 100.0},
    ]}
    assert owner._protective_plan(CONID, {'quantity': 40.0, 'avg_cost': cost}, snapshot) is None


@pytest.mark.parametrize('complete', [True, False])
def test_historical_position_frame_honors_its_completeness(owner, complete):
    frame = pd.DataFrame([{'conId': CONID, 'position': 30.0, 'avgCost': 100.0}])
    frame.attrs['complete'] = complete
    owner._sdk = SimpleNamespace(positions=lambda: frame.copy())
    plan = owner._protective_plan(CONID, {'quantity': 130.0, 'avg_cost': 100.0}, {})
    if complete:
        assert plan is not None
        assert (plan.quantity, plan.stop_price) == (30.0, 92.0)
    else:
        assert plan is None


def _stop(**changes):
    return dict({'orderId': 91, 'orderRef': OWNER + '|mmr:protective-native',
                 'conId': CONID, 'action': 'SELL', 'orderType': 'STP',
                 'status': 'Submitted', 'totalQuantity': 40.0, 'filled': 0.0}, **changes)


@pytest.mark.parametrize('order_type', ['STP', 'STP LMT', 'TRAIL', 'TRAIL LIMIT', 'stp'])
def test_owned_protective_recognizes_supported_stop_types(order_type):
    assert AutoExecutor._is_owned_protective(_stop(orderType=order_type), OWNER, CONID) is True


@pytest.mark.parametrize('changes', [
    {'orderRef': OWNER + '_different|mmr:protective-native'},
    {'conId': 2222}, {'action': 'BUY'}, {'orderType': 'LMT'}, {'orderType': ''},
])
def test_foreign_or_generic_sell_does_not_become_owned_protection(changes):
    assert AutoExecutor._is_owned_protective(_stop(**changes), OWNER, CONID) is False


def test_absent_order_type_is_not_inferred_as_a_stop():
    row = _stop()
    del row['orderType']
    assert AutoExecutor._is_owned_protective(row, OWNER, CONID) is False


@pytest.mark.parametrize('status', ['PendingSubmit', 'ApiPending', 'PreSubmitted', 'Submitted', 'PendingCancel'])
def test_every_executable_stop_status_remains_a_reservation(status):
    row = _stop(status=status)
    assert AutoExecutor._protective_rows([row], OWNER, CONID) == [row]


@pytest.mark.parametrize('status', ['Filled', 'Cancelled', 'ApiCancelled', 'Inactive'])
def test_terminal_stop_is_not_a_live_order_reservation(status):
    assert AutoExecutor._protective_rows([_stop(status=status)], OWNER, CONID) == []


def test_incomplete_order_snapshot_cannot_be_read_as_no_protection(owner):
    owner._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: {'complete': False, 'orders': []})
    with pytest.raises(AutoExecutionError):
        owner._own_live_protectives(owner._sdk, OWNER, CONID)


def test_live_stop_scan_returns_only_matching_physical_reservations(owner):
    rows = [_stop(), _stop(orderId=92, orderType='LMT'), _stop(orderId=93, conId=2222),
            _stop(orderId=94, status='Filled')]
    owner._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: {'complete': True, 'orders': rows})
    assert owner._own_live_protectives(owner._sdk, OWNER, CONID) == [91]


@pytest.mark.timeout(2)
@pytest.mark.parametrize(('cost', 'expected'), [
    (1.4999999999999999e23, 1.3799999999999998e23),
    (1.5000000000000001e100, 1.38e100),
    (1e30, 9.2e29),
    (1.95e306, 1.7940000000000002e306),
], ids=['billion-cent-plateau', 'large-finite-plateau', 'affordable-floor', 'finite-cent-ceiling'])
def test_native_protective_rounding_finishes_for_extreme_finite_cost(owner, cost, expected):
    # These proven owned fill costs are finite; planning must return
    # even when a one-cent decrement does not change the represented price.
    owner._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: None)
    snapshot = {'positions_complete': True, 'positions': [
        {'conId': CONID, 'position': 50.0, 'avgCost': cost},
    ]}
    plan = owner._protective_plan(CONID, {'quantity': 30.0, 'avg_cost': cost}, snapshot)
    assert plan is not None
    assert plan.quantity == 30.0
    assert plan.stop_price == expected


def test_native_protective_rounding_steps_below_an_unreachable_cent_target(owner):
    # Converting the integer cents to float skips the .67 target: the next
    # available price is .66. Protection must step down rather than up to .69.
    owner._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: None)
    snapshot = {'positions_complete': True, 'positions': [
        {'conId': CONID, 'position': 50.0, 'avgCost': 200.0},
    ]}
    plan = owner._protective_plan(
        CONID, {'quantity': 30.0, 'avg_cost': 99315492305367.03}, snapshot)
    assert plan is not None
    assert plan.quantity == 30.0
    assert plan.stop_price == 91370252920937.66


def test_native_positive_stop_distance_that_rounds_to_entry_has_no_plan(owner, monkeypatch):
    # A positive configuration value can still be smaller than the available
    # price precision. It must not turn protection into a stop at entry.
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '1e-20')
    owner._sdk = SimpleNamespace(execution_snapshot=lambda **kwargs: None)
    snapshot = {'positions_complete': True, 'positions': [
        {'conId': CONID, 'position': 50.0, 'avgCost': 200.0},
    ]}
    assert owner._protective_plan(
        CONID, {'quantity': 30.0, 'avg_cost': 100.0}, snapshot) is None
