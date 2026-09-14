"""Independent management preserves exact request and mailbox lifetimes."""
import datetime as dt
import threading
from types import SimpleNamespace

import pytest

from trader.objects import Action
from trader.strategy.auto_executor import AutoExecutor, BarWork, SignalWork
from trader.strategy.execution_intents import IntentStore


ENTRY = dt.datetime(2026, 9, 9, 14, 0, tzinfo=dt.timezone.utc)
OWNER = ('management_contract', 1111)


@pytest.fixture
def owner(tmp_path, monkeypatch):
    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    executor = AutoExecutor.__new__(AutoExecutor)
    executor.intents = IntentStore(str(tmp_path / 'management-contract.duckdb'))
    executor._view_lock = threading.Lock()
    executor._exit_overflow = {}
    executor._bar_overflow = {}
    executor._emergency_exits = {}
    executor._unpublished_open_fills = set()
    position = {'entry_bar_ts': ENTRY, 'proposal_id': 11}
    executor.state = SimpleNamespace(open_position=lambda *args: position)
    executor._managed_view = [dict(position, strategy_name=OWNER[0], conid=OWNER[1])]
    return executor


def _sell(strategy=OWNER[0], conid=OWNER[1], minutes=0):
    return SignalWork(strategy, conid, Action.SELL, ENTRY + dt.timedelta(minutes=minutes),
                      bar_size_seconds=60)


def test_management_routes_only_pending_closes_to_attempt_advancement(owner, monkeypatch):
    terminal = owner.intents.create(*OWNER, 'CLOSE', {'bar_ts': ENTRY.isoformat()}, status='FILLED')
    active = owner.intents.create(*OWNER, 'CLOSE', {'bar_ts': ENTRY.isoformat()}, status='WAITING')
    owner.intents.create(*OWNER, 'OPEN', {'bar_ts': ENTRY.isoformat()}, status='UNKNOWN')
    owner.intents.create(*OWNER, 'PROTECTIVE', {'bar_ts': ENTRY.isoformat()}, status='WORKING')
    owner.state.all_open = lambda: [(OWNER[0], OWNER[1], 10.0, ENTRY, 91)]
    owner._snapshot_cache = {'old observation': object()}
    owner._reconciled = True
    advanced, finished, protected, published = [], [], [], []
    monkeypatch.setattr(owner, '_claim_overflow_exits', lambda: None)
    monkeypatch.setattr(owner, '_retry_emergency_exits', lambda: None)

    def reconcile():
        assert not owner._reconciled, 'every management pass must refresh reconciliation'

    monkeypatch.setattr(owner, '_reconcile_once', reconcile)
    monkeypatch.setattr(owner, '_advance_close', lambda intent: advanced.append(intent['intent_id']))
    monkeypatch.setattr(owner, '_finish_close_request', lambda intent: finished.append(intent['intent_id']))
    monkeypatch.setattr(owner, '_ensure_protective', lambda *key: protected.append(key))
    monkeypatch.setattr(owner, '_load_open_view', lambda: published.append(True))
    owner.manage_positions()
    # Attempt advancement and durable request retirement have different
    # input domains. An active OPEN/protector is never a close attempt.
    assert advanced == [active['intent_id']]
    assert set(finished) == {terminal['intent_id'], active['intent_id']}
    assert protected == [OWNER]
    assert published == [True]


def test_signal_kill_switch_discards_all_captured_sells_but_keeps_timer_management(owner, monkeypatch):
    owner._exit_overflow = {OWNER: _sell(), ('second_owner', 2222): _sell('second_owner', 2222)}
    timer = BarWork(*OWNER, ENTRY, 1)
    owner._bar_overflow[OWNER] = timer
    closed, bars = [], []
    monkeypatch.setattr(owner, '_execute_close', lambda *args, **kwargs: closed.append(args))
    monkeypatch.setattr(owner, '_process_bar', lambda work: bars.append(work))
    monkeypatch.setenv('MMR_AUTO_EXECUTE_DISABLED', '1')
    owner._claim_overflow_exits()
    assert owner._exit_overflow == {}
    assert owner._bar_overflow == {}
    assert bars == [timer]
    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED')
    owner._claim_overflow_exits()
    assert closed == [], 'clearing the switch must not resurrect discarded old signals'


def test_killed_snapshot_does_not_remove_a_newer_signal_mailbox_item(owner, monkeypatch):
    old, new = _sell(), _sell(minutes=1)
    owner._exit_overflow[OWNER] = old

    def killed(executor):
        with executor._view_lock:
            executor._exit_overflow[OWNER] = new
        return True

    monkeypatch.setattr(AutoExecutor, 'kill_switch', property(killed))
    owner._claim_overflow_exits()
    assert owner._exit_overflow == {OWNER: new}


@pytest.mark.parametrize('replace_while_processing', [False, True])
def test_processed_bar_retires_only_its_own_mailbox_identity(owner, monkeypatch, replace_while_processing):
    old = BarWork(*OWNER, ENTRY, 1)
    new = BarWork(*OWNER, ENTRY + dt.timedelta(minutes=1), 2)
    owner._bar_overflow[OWNER] = old
    processed = []

    def process(work):
        processed.append(work)
        if replace_while_processing and work is old:
            with owner._view_lock:
                owner._bar_overflow[OWNER] = new

    monkeypatch.setattr(owner, '_process_bar', process)
    owner._claim_overflow_exits()
    assert owner._bar_overflow == ({OWNER: new} if replace_while_processing else {})
    owner._claim_overflow_exits()
    assert owner._bar_overflow == {}
    assert processed == ([old, new] if replace_while_processing else [old])


@pytest.mark.parametrize('replace_while_processing', [False, True])
def test_durable_signal_claim_retires_only_its_own_mailbox_identity(owner, monkeypatch, replace_while_processing):
    old, new = _sell(), _sell(minutes=1)
    owner._exit_overflow[OWNER] = old
    processed = []
    reasons = []

    def close(strategy, conid, bar_ts, attributed_quantity, reason):
        processed.append((strategy, conid, bar_ts))
        reasons.append(reason)
        if replace_while_processing and bar_ts == old.bar_ts:
            with owner._view_lock:
                owner._exit_overflow[OWNER] = new

    monkeypatch.setattr(owner, '_execute_close', close)
    owner._claim_overflow_exits()
    assert owner._exit_overflow == ({OWNER: new} if replace_while_processing else {})
    owner._claim_overflow_exits()
    assert owner._exit_overflow == {}
    expected = [(*OWNER, old.bar_ts)]
    if replace_while_processing:
        expected.append((*OWNER, new.bar_ts))
    assert processed == expected
    assert owner._emergency_exits == {}
    assert all(isinstance(reason, str) and 'sell' in reason.casefold()
               and 'overload' in reason.casefold() for reason in reasons)


def test_overflow_failure_retains_emergency_owner_bar_and_original_scope(owner, monkeypatch):
    signal = _sell(minutes=1)
    owner._exit_overflow[OWNER] = signal

    def unavailable(*args, **kwargs):
        raise OSError('durable claim unavailable')

    monkeypatch.setattr(owner, '_execute_close', unavailable)
    owner._claim_overflow_exits()
    assert owner._exit_overflow == {}
    assert set(owner._emergency_exits) == {OWNER}
    retained = owner._emergency_exits[OWNER]
    assert (retained['strategy'], retained['conid']) == OWNER
    assert retained['bar_ts'] == signal.bar_ts.isoformat()
    assert retained['attempted'] is False
    assert isinstance(retained['reason'], str)
    assert 'journal' in retained['reason'].casefold()
    assert retained['explicit_exit_scope'] == {
        'positions': [{'entry_bar_ts': ENTRY.isoformat(), 'proposal_id': 11}],
        'openings': {},
    }
