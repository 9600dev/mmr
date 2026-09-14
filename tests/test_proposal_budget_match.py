"""Server budget match for amount-sized approved proposals (review 2026-09-11).

``sdk.approve`` sizes an amount proposal as whole shares of ``amount`` at ITS
last-trade snapshot. The server re-checks the sized quantity against the
budget; valuing it at the marketable side (ask for a BUY) with zero tolerance
refused every amount-sized open on any positive spread and transitioned the
proposal APPROVED->FAILED. The server now values at the same anchor and admits
snapshot drift, while an inflated quantity still fails the match.
"""
from types import SimpleNamespace

import pytest

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.data.proposal_store import ProposalStore
from trader.messaging.trader_service_api import TraderServiceApi
from trader.trading.order_math import whole_shares_for_notional
from trader.trading.proposal import ExecutionSpec, TradeProposal
from trader.trading.risk_gate import RiskGate, RiskLimits


def _approving_trader(tmp_path, *, last, ask, bid):
    trader = _coordinated_trader(tmp_path, held=0)
    trader.require_proposal_approval = True
    trader.risk_gate = RiskGate(RiskLimits(), trader.event_store)
    ticker = SimpleNamespace(contract=_stock(), last=last, close=last, ask=ask, bid=bid)
    trader.client.get_snapshot.return_value = ticker
    trader.client.ib.tickers = lambda: [ticker]
    return trader


def _approved_amount_proposal(trader, amount, execution=None):
    store = ProposalStore(trader.duckdb_path)
    proposal = TradeProposal('AUDIT', 'BUY', amount=amount, metadata={'con_id': 100},
                             execution=execution or ExecutionSpec())
    pid = store.add(proposal)
    store.try_transition(pid, 'PENDING', 'APPROVED')
    return pid, proposal


async def _submit(trader, pid, proposal, quantity):
    return await TraderServiceApi(trader).place_expressive_order(
        _stock(), 'BUY', float(quantity), proposal.execution.to_dict(),
        proposal_id=pid, client_intent_id=f'proposal:{pid}')


@pytest.mark.asyncio
async def test_amount_sized_quantity_survives_a_positive_spread(tmp_path):
    trader = _approving_trader(tmp_path, last=100.0, ask=100.02, bid=99.98)
    pid, proposal = _approved_amount_proposal(trader, 2000.0)
    quantity = whole_shares_for_notional(2000.0, 100.0)  # exactly what sdk.approve sends
    assert quantity == 20
    result = await _submit(trader, pid, proposal, quantity)
    assert result.is_success(), result.error
    trade, = trader.placed
    assert trade.order.action == 'BUY' and trade.order.totalQuantity == 20


@pytest.mark.asyncio
async def test_inflated_quantity_still_fails_the_budget_match(tmp_path):
    trader = _approving_trader(tmp_path, last=100.0, ask=100.02, bid=99.98)
    pid, proposal = _approved_amount_proposal(trader, 2000.0)
    result = await _submit(trader, pid, proposal, 21)  # 2100 > 2000 * 1.01
    assert not result.is_success()
    assert 'Opening authorization refused' in result.error
    assert not trader.placed


@pytest.mark.asyncio
async def test_limit_above_last_does_not_inflate_the_budget_valuation(tmp_path):
    """The approver tier lets a client limit push valuation UP (defending a
    forged-low notional); the budget match must not, because the client sized
    at last. A LIMIT 105 proposal sized at last=100 is a legitimate approval."""
    trader = _approving_trader(tmp_path, last=100.0, ask=100.02, bid=99.98)
    pid, proposal = _approved_amount_proposal(
        trader, 2000.0, ExecutionSpec(order_type='LIMIT', limit_price=105.0))
    result = await _submit(trader, pid, proposal, 20)  # 20 * 105 = 2100 at the limit, 2000 at last
    assert result.is_success(), result.error
    trade, = trader.placed
    assert trade.order.orderType == 'LMT' and trade.order.lmtPrice == 105.0 and trade.order.totalQuantity == 20


@pytest.mark.asyncio
async def test_budget_match_still_needs_a_server_price(tmp_path):
    trader = _approving_trader(tmp_path, last=float('nan'), ask=float('nan'), bid=float('nan'))
    pid, proposal = _approved_amount_proposal(trader, 2000.0)
    result = await _submit(trader, pid, proposal, 20)
    assert not result.is_success() and not trader.placed
