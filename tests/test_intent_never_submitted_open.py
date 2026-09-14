"""An OPEN intent that never reached the broker no longer reserves its key forever.

The crash window is create -> SUBMITTING -> propose -> proposal id recorded ->
approve. A restart used to leave the intent SUBMITTING with no order ids; every
BUY for the key was skipped as "unresolved execution intent reserves exposure"
and every exit waited for its size. Reconciliation now resolves the intent when
it is PROVABLY unsent — no proposal, a never-approved proposal, or the trader's
RETRYABLE claim with no reserved order — and otherwise explains ONCE which
operator command applies. Only an intent from an earlier process is judged.
"""
import logging
import time
from types import SimpleNamespace

import pandas as pd
import pytest

from test_intent_support import CONID, OWNER, SnapshotSDK, TS, build_executor, make_work
from trader.data.proposal_store import ProposalStore
from trader.strategy.auto_executor import AutoExecutor
from trader.trading.proposal import TradeProposal


class _ProcessDies(BaseException):
    """Process-stop seam: bypasses the executor's ordinary exception recovery."""


def _restart(path, sdk):
    time.sleep(0.01)  # a new process starts strictly after the intent was created
    return AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)


def _store_backed_propose(sdk, store):
    def propose(**kwargs):
        pid = store.add(TradeProposal(symbol=kwargs['symbol'], action=kwargs['action'],
                                      quantity=kwargs.get('quantity'), amount=kwargs.get('amount'),
                                      source=kwargs.get('source', ''), metadata=dict(kwargs.get('metadata') or {})))
        sdk.propose_calls.append(kwargs)
        sdk.proposals[pid] = SimpleNamespace(quantity=kwargs.get('quantity'), metadata=kwargs.get('metadata', {}))
        return pid, None, None
    return propose


def _crash_open(executor, sdk, monkeypatch, *, at):
    """Drive one BUY into the crash window ``at`` ('propose' or a fake approve)."""
    if at == 'propose':
        def dies(**kwargs):
            raise _ProcessDies()
        monkeypatch.setattr(sdk, 'propose', dies)
    else:
        monkeypatch.setattr(sdk, 'approve', at)
    with pytest.raises(_ProcessDies):
        executor._process_signal(make_work(quantity=140))
    stuck, = executor.intents.all(kind='OPEN')
    assert stuck['status'] == 'SUBMITTING' and not stuck['payload'].get('order_ids')
    return stuck


def test_open_that_crashed_before_proposing_is_resolved_after_restart(tmp_path, monkeypatch, caplog):
    sdk = SnapshotSDK()
    executor, path = build_executor(tmp_path, sdk, monkeypatch)
    original_propose = sdk.propose
    stuck = _crash_open(executor, sdk, monkeypatch, at='propose')
    assert not stuck['payload'].get('proposal_id')
    monkeypatch.setattr(sdk, 'propose', original_propose)

    # Same process: the submission could still be in flight, so it stays reserved.
    executor.manage_positions()
    assert executor.intents.get(stuck['intent_id'])['status'] == 'SUBMITTING'

    restarted = _restart(path, sdk)
    with caplog.at_level(logging.WARNING, logger='auto_executor'):
        restarted.manage_positions()
    resolved = restarted.intents.get(stuck['intent_id'])
    assert resolved['status'] == 'RESOLVED' and resolved['payload']['never_submitted'] is True
    assert 'no proposal' in resolved['payload']['never_submitted_proof']
    assert any('never reached the broker' in record.getMessage() for record in caplog.records)

    # The instrument is open for business again and the phantom holds no cooldown.
    restarted._process_signal(make_work(quantity=140, bar_ts=TS + pd.Timedelta(minutes=1)))
    assert restarted.state.open_position(OWNER, CONID)['quantity'] == 140
    assert len(sdk.approve_calls) == 1


@pytest.mark.parametrize('proposal_id_recorded', [True, False])
def test_pending_proposal_is_rejected_and_the_open_resolved(tmp_path, monkeypatch, proposal_id_recorded):
    sdk = SnapshotSDK()
    executor, path = build_executor(tmp_path, sdk, monkeypatch)
    store = ProposalStore(path)
    monkeypatch.setattr(sdk, 'propose', _store_backed_propose(sdk, store))

    def dies(pid, **kwargs):
        raise _ProcessDies()

    stuck = _crash_open(executor, sdk, monkeypatch, at=dies)
    pid = stuck['payload']['proposal_id']
    assert store.get(pid).status == 'PENDING'
    if not proposal_id_recorded:
        executor.intents.update(stuck, proposal_id=None)  # the crash pre-empted the id write

    restarted = _restart(path, sdk)
    restarted.manage_positions()
    resolved = restarted.intents.get(stuck['intent_id'])
    assert resolved['status'] == 'RESOLVED' and resolved['payload']['never_submitted'] is True
    assert str(pid) in resolved['payload']['never_submitted_proof']
    proposal = store.get(pid)
    assert proposal.status == 'REJECTED'
    assert proposal.rejection_reason == 'never approved; resolved on restart'


@pytest.mark.parametrize('retry_safe', [True, False])
def test_approved_proposal_resolves_only_with_the_traders_retryable_proof(tmp_path, monkeypatch, caplog, retry_safe):
    sdk = SnapshotSDK()
    executor, path = build_executor(tmp_path, sdk, monkeypatch)
    store = ProposalStore(path)
    monkeypatch.setattr(sdk, 'propose', _store_backed_propose(sdk, store))

    def approve_reaches_the_trader_then_dies(pid, **kwargs):
        assert store.try_transition(pid, 'PENDING', 'APPROVED')
        raise _ProcessDies()

    stuck = _crash_open(executor, sdk, monkeypatch, at=approve_reaches_the_trader_then_dies)
    sdk.retry_safe = retry_safe

    restarted = _restart(path, sdk)
    with caplog.at_level(logging.WARNING, logger='auto_executor'):
        restarted.manage_positions()
        restarted.manage_positions()
    intent = restarted.intents.get(stuck['intent_id'])
    if retry_safe:
        assert intent['status'] == 'RESOLVED' and intent['payload']['never_submitted'] is True
        assert 'RETRYABLE' in intent['payload']['never_submitted_proof']
        return
    # Approve was called and the trader cannot prove no order was reserved:
    # never resolved on a guess, and explained exactly once with the remedy.
    assert intent['status'] == 'SUBMITTING'
    explained = [record.getMessage() for record in caplog.records
                 if 'cannot be proven unsent' in record.getMessage()]
    assert len(explained) == 1, explained
    assert f'mmr strategies resolve-intent {stuck["intent_id"]}' in explained[0]
    assert f'--strategy {OWNER} --conid {CONID}' in explained[0]


def test_executed_proposal_hands_its_recorded_order_ids_to_the_intent(tmp_path, monkeypatch):
    """The reply was lost after placement; the proposal kept the physical ids."""
    sdk = SnapshotSDK()
    executor, path = build_executor(tmp_path, sdk, monkeypatch)
    store = ProposalStore(path)
    monkeypatch.setattr(sdk, 'propose', _store_backed_propose(sdk, store))

    def approve_places_then_dies(pid, **kwargs):
        assert store.try_transition(pid, 'PENDING', 'APPROVED')
        sdk.broker[CONID] = 140
        # A completed-order row without the client reference: only the
        # numeric id the proposal recorded can associate it.
        sdk.accepted.append(dict(orderId=4242, orderRef=OWNER, conId=CONID, status='Filled', action='BUY',
                                 totalQuantity=140.0, filled=140.0, avgFillPrice=100.0, clientIntentId=''))
        assert store.try_transition(pid, 'APPROVED', 'EXECUTED', order_ids=[4242])
        raise _ProcessDies()

    stuck = _crash_open(executor, sdk, monkeypatch, at=approve_places_then_dies)
    restarted = _restart(path, sdk)
    restarted.manage_positions()
    intent = restarted.intents.get(stuck['intent_id'])
    assert intent['payload']['order_ids'] == [4242]
    assert intent['status'] == 'FILLED' and intent['payload']['cumulative_filled'] == 140.0
    assert restarted.state.open_position(OWNER, CONID)['quantity'] == 140
    assert sdk.approve_calls == [], 'the lost reply is recovered, never re-approved'
