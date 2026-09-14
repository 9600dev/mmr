"""Explicit exit authority is captured once from exact opening provenance."""
import datetime as dt
import threading
from types import SimpleNamespace

import pytest

from trader.strategy.auto_executor import AutoExecutor
from trader.strategy.execution_intents import IntentStore


OWNER = ('provenance_contract', 1111)
ENTRY = dt.datetime(2026, 9, 9, 14, 0, tzinfo=dt.timezone.utc)
LATER = ENTRY + dt.timedelta(minutes=1)


@pytest.fixture
def owner(tmp_path):
    executor = AutoExecutor.__new__(AutoExecutor)
    executor.intents = IntentStore(str(tmp_path / 'exit-provenance.duckdb'))
    executor._view_lock = threading.Lock()
    executor._unpublished_open_fills = set()
    return executor


def _opening(owner, *, entry=LATER, proposal=22, status='UNKNOWN', strategy=OWNER[0], conid=OWNER[1]):
    return owner.intents.create(strategy, conid, 'OPEN',
                                {'bar_ts': entry.isoformat(), 'proposal_id': proposal}, status=status)


def test_captured_opening_identity_accepts_its_later_epoch_without_position_fallback(owner):
    opening = _opening(owner)
    scope = {'positions': [{'entry_bar_ts': ENTRY.isoformat(), 'proposal_id': 11}],
             'openings': {opening['intent_id']: {'entry_bar_ts': LATER.isoformat(), 'proposal_id': None}}}
    assert owner._scope_accepts_opening(scope, opening) is True


@pytest.mark.parametrize(('opening_proposal', 'captured_proposal', 'entry', 'expected'), [
    (22, 22, ENTRY, True),
    (None, None, ENTRY, False),
    (None, 22, ENTRY, False),
    (23, 22, ENTRY, False),
    (22, 22, LATER, False),
])
def test_position_provenance_requires_nonmissing_proposal_and_matching_entry(
        owner, opening_proposal, captured_proposal, entry, expected):
    opening = _opening(owner, entry=entry, proposal=opening_proposal)
    scope = {'positions': [{'entry_bar_ts': ENTRY.isoformat(), 'proposal_id': captured_proposal}],
             'openings': {}}
    assert owner._scope_accepts_opening(scope, opening) is expected


@pytest.mark.parametrize(('entry', 'proposal', 'expected'), [
    (ENTRY, 11, True), (LATER, 11, False), (ENTRY, 12, False),
])
def test_original_captured_position_requires_both_entry_and_proposal(owner, entry, proposal, expected):
    scope = {'positions': [{'entry_bar_ts': ENTRY.isoformat(), 'proposal_id': 11}], 'openings': {}}
    assert owner._scope_matches_position(scope, {'entry_bar_ts': entry, 'proposal_id': proposal}) is expected
    assert owner._scope_matches_position(scope, None) is False


@pytest.mark.parametrize('assigned', [False, True])
def test_captured_opening_resolves_a_later_proposal_only_from_its_exact_id(owner, assigned):
    opening = _opening(owner, proposal=None)
    scope = {'positions': [], 'openings': {
        opening['intent_id']: {'entry_bar_ts': LATER.isoformat(), 'proposal_id': None},
    }}
    if assigned:
        owner.intents.update(opening, proposal_id=22)
    assert owner._scope_matches_position(scope, {'entry_bar_ts': LATER, 'proposal_id': 22}) is assigned
    assert owner._scope_matches_position(scope, {'entry_bar_ts': LATER, 'proposal_id': 23}) is False
    assert owner._scope_matches_position(scope, {'entry_bar_ts': LATER, 'proposal_id': None}) is False


@pytest.mark.parametrize('unavailable', ['none', 'position', 'position_and_journal'])
def test_scope_capture_excludes_foreign_and_published_terminal_openings(owner, monkeypatch, unavailable):
    terminal = _opening(owner, entry=ENTRY, proposal=11, status='FILLED')
    pending = _opening(owner, entry=LATER, proposal=22)
    _opening(owner, strategy='other_owner', proposal=33)
    _opening(owner, conid=2222, proposal=44)
    position = {'entry_bar_ts': ENTRY, 'proposal_id': 11}
    owner.state = SimpleNamespace(open_position=lambda strategy, conid: position)
    owner._managed_view = [dict(position, strategy_name=OWNER[0], conid=OWNER[1])]
    owner._unpublished_open_fills.add(terminal['intent_id'])

    def unavailable_read(*args, **kwargs):
        raise OSError('storage temporarily unavailable')

    if unavailable != 'none':
        owner.state.open_position = unavailable_read
    if unavailable == 'position_and_journal':
        monkeypatch.setattr(owner.intents, 'all', unavailable_read)
    scope = owner._capture_exit_scope(*OWNER)
    assert scope['positions'] == [{'entry_bar_ts': ENTRY.isoformat(), 'proposal_id': 11}]
    expected = {pending['intent_id']}
    if unavailable != 'none':
        # A committed OPEN whose ownership view could not be published is
        # already related authority, even if its journal status is terminal.
        expected.add(terminal['intent_id'])
    assert set(scope['openings']) == expected
    assert scope['openings'][pending['intent_id']] == {
        'entry_bar_ts': LATER.isoformat(), 'proposal_id': 22,
    }
