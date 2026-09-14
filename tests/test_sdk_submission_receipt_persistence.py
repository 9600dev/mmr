"""Native SDK receipt bookkeeping over an offline broker transport.

The real proposal store, approval API, order splitter and server journal are
used. The existing coordinated Trader fixture returns actual ib_async Trades
in Submitted state; it never fills an order or contacts a broker. Storage faults
are scoped to the SDK's ProposalStore instance after native placement returns.
"""

import asyncio
from types import SimpleNamespace

import pytest

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.data.event_store import EventStore
from trader.data.proposal_store import ProposalStore
from trader.messaging.trader_service_api import TraderServiceApi
from trader.sdk import MMR
from trader.trading.order_lifecycle import OrderLifecycleTracker
from trader.trading.proposal import ExecutionSpec
from trader.trading.risk_gate import RiskGate, RiskLimits


@pytest.fixture
def receipt_owner(tmp_path):
    server = _coordinated_trader(tmp_path, held=2)
    try:
        # Use the real durable event timestamps and queries before any receipt.
        previous_tracker = server.order_tracker
        previous_tracker.close(timeout=1)
        if previous_tracker._journal is not None:
            previous_tracker._journal.close()
        if previous_tracker._temporary is not None:
            previous_tracker._temporary.cleanup()
        server.event_store = EventStore(server.duckdb_path)
        server.order_tracker = OrderLifecycleTracker(server.event_store)
        server.risk_gate = RiskGate(RiskLimits(), server.event_store)
        server.require_proposal_approval = True
        api = TraderServiceApi(server)
        proposals = ProposalStore(server.duckdb_path)
        contract = _stock()
        ctx = SimpleNamespace(server=server, proposals=proposals,
                              contract=contract, calls=[], receipts=[])

        def place_expressive_order(**kwargs):
            ctx.calls.append(dict(kwargs))
            result = asyncio.run(api.place_expressive_order(**kwargs))
            ctx.receipts.append(result)
            return result

        rpc = SimpleNamespace(
            place_expressive_order=place_expressive_order,
            get_account_values=lambda: {},
        )
        sdk = MMR.__new__(MMR)
        sdk._prop_store = proposals
        sdk._client = SimpleNamespace(is_setup=True, rpc=lambda **kwargs: rpc)
        # Exact universe resolution and the optional proposal-time quote are
        # external seams. The SDK's matching, proposal and approval stay native.
        sdk.resolve = lambda symbol, **kwargs: (
            [contract] if symbol in (contract.conId, contract.symbol) else [])
        sdk.snapshot = lambda *args, **kwargs: None
        ctx.sdk = sdk
        yield ctx
    finally:
        server.order_tracker.close(timeout=1)
        if server.order_tracker._journal is not None:
            server.order_tracker._journal.close()
        journal = getattr(server, '_server_order_journal', None)
        if journal is not None:
            journal.journal.close()
        if server.order_tracker._temporary is not None:
            server.order_tracker._temporary.cleanup()


def _receipt_proposal(ctx, *, action='BUY', quantity=1):
    proposal_id, _, _ = ctx.sdk.propose(
        symbol=ctx.contract.symbol, action=action, quantity=quantity,
        execution=ExecutionSpec(order_type='MARKET', exit_type='NONE'),
        sec_type=ctx.contract.secType, exchange=ctx.contract.exchange,
        currency=ctx.contract.currency, source='sdk_receipt_regression',
        metadata={'con_id': ctx.contract.conId, 'strategy': 'sdk_receipt'},
    )
    assert ctx.proposals.get(proposal_id).status == 'PENDING'
    return proposal_id


def _observed_ids(ctx, count):
    assert len(ctx.server.placed) == count
    assert all(trade.orderStatus.status == 'Submitted' and trade.orderStatus.filled == 0
               for trade in ctx.server.placed)
    assert ctx.server.inventory == 2
    ids = [trade.order.orderId for trade in ctx.server.placed]
    assert all(type(order_id) is int and order_id > 0 for order_id in ids)
    assert len(set(ids)) == count
    return ids


def _assert_bookkeeping_unknown(reply, proposal_id, fault):
    assert not reply.is_success()
    assert 'UNKNOWN' in str(reply.error) and 'reconcil' in str(reply.error).lower()
    assert str(proposal_id) in str(reply.error) and str(fault) in str(reply.error)
    assert reply.exception is fault


def test_native_single_receipt_retains_successful_approval(receipt_owner):
    ctx = receipt_owner
    proposal_id = _receipt_proposal(ctx)
    reply = ctx.sdk.approve(proposal_id)
    ids = _observed_ids(ctx, 1)
    assert reply.is_success() and reply.obj == ids
    stored = ctx.proposals.get(proposal_id)
    assert stored.status == 'EXECUTED' and stored.order_ids == ids
    assert len(ctx.calls) == 1 and ctx.calls[0]['proposal_id'] == proposal_id
    assert ctx.receipts[0].is_success()


def test_native_successful_split_persists_outcome_before_terminal_state(receipt_owner):
    ctx = receipt_owner
    proposal_id = _receipt_proposal(ctx, action='SELL', quantity=3)
    reply = ctx.sdk.approve(proposal_id)
    ids = _observed_ids(ctx, 2)
    assert reply.is_success(), reply.error
    assert reply.obj == ids
    actual = ctx.receipts[0].execution_outcome
    assert reply.execution_outcome == actual
    assert actual['reduction'] == {'status': 'SUBMITTED', 'quantity': 2.0, 'order_ids': ids[:1]}
    assert actual['opening'] == {'status': 'SUBMITTED', 'quantity': 1.0, 'order_ids': ids[1:]}
    stored = ctx.proposals.get(proposal_id)
    assert stored.status == 'EXECUTED' and stored.order_ids == ids
    assert stored.metadata['submission_outcome'] == actual
    assert len(ctx.calls) == 1


@pytest.mark.parametrize('commit_first', [False, True], ids=['before-commit', 'after-commit'])
def test_native_receipt_survives_execution_status_write_error(receipt_owner, monkeypatch, commit_first):
    ctx = receipt_owner
    proposal_id = _receipt_proposal(ctx)
    fault = OSError('execution status acknowledgement unavailable')
    native_transition = ctx.proposals.try_transition
    injected = []

    def transition(id, from_status, to_status, **kwargs):
        if id == proposal_id and to_status == 'EXECUTED' and not injected:
            injected.append(True)
            if commit_first:
                native_transition(id, from_status, to_status, **kwargs)
            raise fault
        return native_transition(id, from_status, to_status, **kwargs)

    monkeypatch.setattr(ctx.proposals, 'try_transition', transition)
    reply = ctx.sdk.approve(proposal_id)
    ids = _observed_ids(ctx, 1)
    _assert_bookkeeping_unknown(reply, proposal_id, fault)
    assert reply.obj == ids
    stored = ctx.proposals.get(proposal_id)
    assert stored.status == ('EXECUTED' if commit_first else 'APPROVED')
    assert stored.order_ids == (ids if commit_first else [])
    assert injected == [True] and len(ctx.calls) == 1
    claim = ctx.server.server_order_journal().get(f'proposal:{proposal_id}')
    assert claim['status'] == 'SUBMITTED'

    if not commit_first:
        # A still-APPROVED retry keeps its native server identity; the retained
        # claim refuses a second send while the receipt is reconciled.
        monkeypatch.setattr(ctx.proposals, 'try_transition', native_transition)
        replay = ctx.sdk.approve(proposal_id, resume=True)
        assert not replay.is_success() and 'UNKNOWN' in str(replay.error)
        assert _observed_ids(ctx, 1) == ids
        assert len(ctx.calls) == 2 and ctx.calls[0]['client_intent_id'] == ctx.calls[1]['client_intent_id']
        assert ctx.proposals.get(proposal_id).status == 'APPROVED'


@pytest.mark.parametrize('refuse_opening', [False, True], ids=['successful-split', 'partial-split'])
def test_native_split_receipt_survives_outcome_write_error(receipt_owner, monkeypatch, refuse_opening):
    ctx = receipt_owner
    if refuse_opening:
        ctx.server.risk_gate = None  # Native opening refusal; the reduction still submits.
    proposal_id = _receipt_proposal(ctx, action='SELL', quantity=3)
    fault = OSError('submission outcome storage unavailable')
    native_update = ctx.proposals.update_metadata
    injected = []

    def update(id, extra):
        if id == proposal_id and 'submission_outcome' in extra and not injected:
            injected.append(True)
            raise fault
        return native_update(id, extra)

    monkeypatch.setattr(ctx.proposals, 'update_metadata', update)
    reply = ctx.sdk.approve(proposal_id)
    ids = _observed_ids(ctx, 1 if refuse_opening else 2)
    _assert_bookkeeping_unknown(reply, proposal_id, fault)
    actual_receipt = ctx.receipts[0]
    if refuse_opening:
        assert reply.obj == actual_receipt.obj
        assert reply.obj['reduction'] == {'status': 'SUBMITTED', 'quantity': 2.0, 'order_ids': ids}
        assert reply.obj['opening']['status'] == 'REJECTED'
        assert reply.obj['opening']['quantity'] == 1.0
    else:
        assert reply.obj == ids
        assert reply.execution_outcome == actual_receipt.execution_outcome
        assert reply.execution_outcome['reduction']['order_ids'] == ids[:1]
        assert reply.execution_outcome['opening']['order_ids'] == ids[1:]
    stored = ctx.proposals.get(proposal_id)
    assert stored.status == 'APPROVED' and stored.order_ids == []
    assert 'submission_outcome' not in stored.metadata
    assert injected == [True] and len(ctx.calls) == 1
    claim = ctx.server.server_order_journal().get(f'proposal:{proposal_id}')
    assert claim['status'] == ('PARTIAL' if refuse_opening else 'SUBMITTED')


def test_native_partial_refusal_retains_actual_reduction_outcome(receipt_owner):
    ctx = receipt_owner
    ctx.server.risk_gate = None
    proposal_id = _receipt_proposal(ctx, action='SELL', quantity=3)
    reply = ctx.sdk.approve(proposal_id)
    ids = _observed_ids(ctx, 1)
    assert not reply.is_success() and 'PARTIAL' in str(reply.error)
    assert reply.obj == ctx.receipts[0].obj
    assert reply.obj['reduction'] == {'status': 'SUBMITTED', 'quantity': 2.0, 'order_ids': ids}
    assert reply.obj['opening']['status'] == 'REJECTED'
    stored = ctx.proposals.get(proposal_id)
    assert stored.status == 'FAILED' and stored.order_ids == ids
    assert stored.metadata['submission_outcome'] == reply.obj
    assert len(ctx.calls) == 1


def test_native_pre_submission_resolution_error_remains_failed(receipt_owner, monkeypatch):
    ctx = receipt_owner
    proposal_id = _receipt_proposal(ctx)

    def unavailable(*args, **kwargs):
        raise ConnectionError('exact contract catalogue unavailable')

    monkeypatch.setattr(ctx.sdk, 'resolve', unavailable)
    reply = ctx.sdk.approve(proposal_id)
    assert not reply.is_success()
    assert 'exact contract catalogue unavailable' in str(reply.error)
    assert ctx.proposals.get(proposal_id).status == 'FAILED'
    assert not ctx.calls and not ctx.receipts and not ctx.server.placed
