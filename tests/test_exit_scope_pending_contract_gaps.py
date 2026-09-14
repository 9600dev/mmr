"""Pending opening obligations remain scoped to the durable exit request."""
import datetime as dt

import pytest

from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_intents import IntentStore


OWNER = ('scope_contract', 1111)
BAR = dt.datetime(2026, 9, 9, 14, 0, tzinfo=dt.timezone.utc).isoformat()
LATER_BAR = dt.datetime(2026, 9, 9, 14, 1, tzinfo=dt.timezone.utc).isoformat()


@pytest.fixture
def scope_owner(tmp_path):
    executor = AutoExecutor.__new__(AutoExecutor)
    executor.intents = IntentStore(str(tmp_path / 'pending-opening-scope.duckdb'))
    return executor


def _request(executor, scope=None):
    return executor.intents.create(*OWNER, 'CLOSE',
                                   {'bar_ts': BAR, 'explicit_exit_scope': scope}, status='WAITING')


@pytest.mark.parametrize('irrelevant', [
    'foreign_owner', 'foreign_instrument', 'protective',
    'FILLED', 'CANCELLED', 'REJECTED', 'RESOLVED',
])
def test_legacy_timer_waits_only_for_active_same_owner_same_instrument_openings(scope_owner, irrelevant):
    request = _request(scope_owner)
    strategy, conid = OWNER
    kind, status = 'OPEN', 'WORKING'
    if irrelevant == 'foreign_owner':
        strategy = 'other_owner'
    elif irrelevant == 'foreign_instrument':
        conid = 2222
    elif irrelevant == 'protective':
        kind = 'PROTECTIVE'
    else:
        status = irrelevant
    scope_owner.intents.create(strategy, conid, kind, {'bar_ts': BAR}, status=status)
    # The request itself is an active CLOSE; it cannot count as an OPEN
    # obligation and keep its own retirement pending forever.
    assert scope_owner._scope_has_pending_openings(request) is False


@pytest.mark.parametrize('status', ['CREATED', 'WORKING', 'UNKNOWN'])
def test_legacy_timer_retains_the_matching_unresolved_opening(scope_owner, status):
    request = _request(scope_owner)
    scope_owner.intents.create(*OWNER, 'OPEN', {'bar_ts': BAR}, status=status)
    assert scope_owner._scope_has_pending_openings(request) is True


def test_bounded_recovered_scope_does_not_capture_an_unrelated_pending_open(scope_owner):
    scope_owner.intents.create(*OWNER, 'OPEN', {'bar_ts': LATER_BAR, 'proposal_id': 22}, status='UNKNOWN')
    # A broker-only recovery can prove its original holding without proving
    # which pending OPEN intents belonged to the original request. Its sealed
    # empty opening set must not expand merely because an OPEN exists now.
    scope = {'positions': [{'entry_bar_ts': BAR, 'proposal_id': 11}], 'openings': {}}
    request = _request(scope_owner, scope)
    assert scope_owner._scope_has_pending_openings(request) is False


def test_captured_opening_stays_pending_until_its_exact_terminal_receipt(scope_owner):
    opening = scope_owner.intents.create(*OWNER, 'OPEN',
                                         {'bar_ts': BAR, 'proposal_id': 22}, status='UNKNOWN')
    scope = {'positions': [], 'openings': {
        opening['intent_id']: {'entry_bar_ts': BAR, 'proposal_id': 22},
    }}
    request = _request(scope_owner, scope)
    assert scope_owner._scope_has_pending_openings(request) is True
    scope_owner.intents.update(opening, status='FILLED')
    assert scope_owner._scope_has_pending_openings(request) is False
