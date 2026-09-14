"""Pinned order-contract regressions from the 2026-09 architecture review.

These regressions preserve the exact review counterexamples and exercise the
repaired contract. All broker I/O is replaced at the IB adapter; no services or
real accounts are used. Durable-intent cases use isolated temporary databases.
The original counterexamples and added recovery cases use ordinary assertions.
"""

from types import SimpleNamespace
import asyncio
import threading
import datetime as dt
import time
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, MagicMock

import pytest
import reactivex as rx
from ib_async import Option, OrderStatus, Position, Stock, Trade

from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.common.reactivex import EventSubject, SuccessFail
from trader.listeners.ibreactive import IBAIORx
from trader.messaging.trader_service_api import TraderServiceApi
from trader.sdk import MMR
from trader.trading.executioner import TradeExecutioner
from trader.trading.order_lifecycle import OrderLifecycleTracker
from trader.trading.risk_gate import RiskGate, RiskInputs, RiskLimits
from trader.trading.trading_runtime import Trader


class _MemoryEvents:
    def __init__(self):
        self.events = []

    def append(self, event):
        self.events.append(event)

    def query_since(self, since, event_type=None):
        return [
            event for event in self.events
            if event.timestamp >= since
            and (event_type is None or event.event_type == event_type)
        ]


def _stock():
    return Stock('AUDIT', 'SMART', 'USD', conId=100)


def _trader(held=0, gate_enabled=True, portfolio_value=100_000):
    """Use the real runtime, gate, splitter, lifecycle, and placement sink.

    The fake broker acknowledges orders but never fills them. Thus its held
    quantity remains accurate and unchanged while multiple orders are working.
    """
    trader = object.__new__(Trader)
    contract = _stock()
    trader.ib_account = 'DU_REVIEW_FAKE'
    trader.require_proposal_approval = False
    trader.approver_required_above_usd = 0
    trader.get_positions = lambda: (
        [Position(trader.ib_account, contract, held, 10)] if held else [])
    trader.event_store = _MemoryEvents()
    trader.order_tracker = OrderLifecycleTracker(trader.event_store)
    trader.risk_gate = (
        RiskGate(RiskLimits(), trader.event_store) if gate_enabled else None)
    trader.book = MagicMock()
    trader.gather_risk_inputs = lambda: RiskInputs(
        open_order_count=0, daily_pnl=0, daily_pnl_evaluable=True,
        portfolio_value=portfolio_value, portfolio_value_evaluable=True)

    # ib_async.Ticker.__post_init__ resets supplied quote fields to NaN, so use
    # a quote-shaped object with explicit usable prices for this fake broker.
    ticker = SimpleNamespace(
        contract=contract, last=10, close=10, ask=10, bid=10)
    trader.client = SimpleNamespace()
    trader.client.is_connected = lambda: True
    trader.client.ib = SimpleNamespace(
        tickers=lambda: [ticker],
        accountValues=lambda: [SimpleNamespace(
            tag='NetLiquidation', currency='USD', account=trader.ib_account,
            value=str(portfolio_value))],
        cancelOrder=MagicMock(),
        # working_trades() fails LOUD when the broker's working set cannot be
        # read (an unreadable set is not an empty one); this fake broker has
        # no working orders and says so explicitly.
        openTrades=lambda: [],
    )
    trader.client.get_snapshot = AsyncMock(return_value=ticker)
    trader.check_order_margin = AsyncMock(return_value={
        'initMarginAfter': 100,
        'equityWithLoanAfter': portfolio_value,
    })
    trader.placed = []

    def cancel(order):
        for trade in trader.placed:
            if trade.order.orderId == order.orderId:
                trade.orderStatus.status = 'Cancelled'
                trader.order_tracker.on_trade(trade)
                return trade
        return None

    trader.client.ib.cancelOrder.side_effect = cancel

    async def place(contract, order):
        order.orderId = len(trader.placed) + 1
        trade = Trade(contract, order, OrderStatus(
            orderId=order.orderId, status='Submitted',
            remaining=order.totalQuantity))
        trader.placed.append(trade)
        trader.order_tracker.on_trade(trade)
        return rx.of(trade)

    trader.client.subscribe_place_order = AsyncMock(side_effect=place)
    # This lightweight fake has no durable native claims. Coordinated tests
    # below restore the real reservation reader and use a real sidecar.
    trader.unobserved_reduction_quantity = AsyncMock(return_value=0.0)
    trader.executioner = TradeExecutioner()
    trader.executioner.connect(trader)
    return trader


@pytest.mark.asyncio
async def test_control_exact_contract_exit_survives_unavailable_risk_gate():
    """A genuine reduction remains possible when opens cannot be authorized."""
    trader = _trader(held=100, gate_enabled=False)
    result = await trader.place_expressive_order(
        _stock(), 'SELL', 100, {'order_type': 'MARKET'})
    assert result.is_success()
    assert len(trader.placed) == 1


@pytest.mark.asyncio
async def test_control_explicit_quantity_requires_proposal():
    trader = _trader()
    trader.require_proposal_approval = True
    result = await TraderServiceApi(trader).place_order_simple(
        _stock(), 'BUY', equity_amount=None, quantity=10, limit_price=None,
        market_order=True)
    assert not result.is_success()
    assert not trader.placed


@pytest.mark.asyncio
async def test_stock_position_does_not_authorize_unheld_option_short():
    """100 held shares must not authorize 100 short 100-share call contracts."""
    trader = _trader(held=100, gate_enabled=False)
    option = Option('AUDIT', '20261218', 200, 'C', 'SMART',
                    multiplier='100', conId=200)
    await trader.place_expressive_order(
        option, 'SELL', 100, {'order_type': 'MARKET'})
    assert not trader.placed, 'An unheld option was submitted as a stock exit'


@pytest.mark.asyncio
async def test_amount_sized_open_requires_proposal():
    """The same 10-share open must be gated whether specified by size or cash."""
    trader = _trader()
    trader.require_proposal_approval = True
    await TraderServiceApi(trader).place_order_simple(
        _stock(), 'BUY', equity_amount=100, quantity=None, limit_price=None,
        market_order=True)
    assert not trader.placed, 'Amount sizing placed 10 shares without a proposal'


@pytest.mark.asyncio
async def test_closing_long_does_not_attach_ungated_reopening_buy():
    """A SELL closing a long cannot make its later BUY child reduce exposure."""
    trader = _trader(held=100, gate_enabled=False)
    await trader.place_expressive_order(_stock(), 'SELL', 100, {
        'order_type': 'MARKET', 'exit_type': 'STOP_LOSS', 'stop_loss_price': 11})
    assert not any(trade.order.action == 'BUY' for trade in trader.placed), (
        'The close attached a new BUY 100 despite having no risk gate')


@pytest.mark.asyncio
async def test_working_exit_orders_cannot_exceed_held_quantity_ungated():
    """Two valid SELL-100 closes of the same 100 shares can fill to short 100."""
    trader = _trader(held=100, gate_enabled=False)
    for _ in range(2):
        await trader.place_expressive_order(_stock(), 'SELL', 100, {
            'order_type': 'LIMIT', 'limit_price': 12})
    # Cancelled/replaced orders are not executable. Blocking OCA groups can
    # safely cover the same shares; count only their largest possible fill.
    # PendingCancel still executes until the broker confirms its cancellation.
    terminal = {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'}
    standalone = 0.0
    oca_quantities = {}
    for trade in trader.placed:
        if trade.orderStatus.status in terminal:
            continue
        remaining = trade.orderStatus.remaining
        group = trade.order.ocaGroup
        if group and trade.order.ocaType in (1, 2):
            oca_quantities[group] = max(oca_quantities.get(group, 0), remaining)
        else:
            standalone += remaining
    working_sell_quantity = standalone + sum(oca_quantities.values())
    assert working_sell_quantity <= 100, (
        f'{working_sell_quantity} shares can execute against 100 held')


@pytest.mark.asyncio
async def test_direct_open_fails_closed_when_margin_cannot_be_read():
    """The public direct path must enforce the same margin gate as approve."""
    trader = _trader()
    trader.check_order_margin = AsyncMock(side_effect=ConnectionError('unavailable'))
    await TraderServiceApi(trader).place_order_simple(
        _stock(), 'BUY', equity_amount=None, quantity=1, limit_price=None,
        market_order=True)
    assert not trader.placed, 'The direct open never attempted its margin probe'


@pytest.mark.asyncio
async def test_unknown_exit_type_does_not_submit_naked_entry():
    """A malformed protection request must fail before an entry is sent."""
    trader = _trader()
    await trader.place_expressive_order(_stock(), 'BUY', 1, {
        'order_type': 'MARKET', 'exit_type': 'BRACKETT',
        'take_profit_price': 12, 'stop_loss_price': 8})
    assert not trader.placed, 'Invalid bracket request submitted one transmitting MKT order'


def test_nan_margin_inputs_are_unevaluable():
    gate = RiskGate(RiskLimits(), _MemoryEvents())
    result = gate.check_leverage({
        'initMarginAfter': float('nan'), 'equityWithLoanAfter': float('nan'),
    }, float('nan'))
    assert not result.approved, f'Invalid data produced {result.checks}'


def test_partial_fill_is_recorded_when_remainder_is_cancelled():
    """40 real fills of a 100-share order must remain visible after cancel."""
    events = _MemoryEvents()
    tracker = OrderLifecycleTracker(events)
    trade = SimpleNamespace(
        contract=_stock(),
        order=SimpleNamespace(
            orderId=1, orderRef='review_strategy', action='BUY', totalQuantity=100),
        orderStatus=SimpleNamespace(status='Submitted', filled=40, avgFillPrice=10),
    )
    tracker.on_trade(trade)
    trade.orderStatus.status = 'Cancelled'
    tracker.on_trade(trade)
    filled = sum(event.quantity for event in events.events
                 if event.event_type == EventType.ORDER_FILLED)
    assert filled == 40, f'Broker filled 40; ledger recorded {filled}'


@pytest.mark.asyncio
async def test_adding_to_position_at_concentration_cap_is_refused():
    """Proposed contract: cap the resulting holding, not each individual add.

    The current per-order behavior is explicitly documented in risk_gate.py;
    this is an architectural coverage gap, not a violation of that algorithm.
    A 1-share add increases a 100-share/$1,000 holding past a $1,000 cap.
    """
    trader = _trader(held=100, portfolio_value=10_000)
    await trader.place_expressive_order(
        _stock(), 'BUY', 1, {'order_type': 'MARKET'})
    assert not trader.placed, 'The 10% concentration cap considered only the $10 add'


@pytest.mark.parametrize('path', ['expressive', 'simple'])
@pytest.mark.asyncio
async def test_order_placement_disposes_its_temporary_subscription(path):
    """Use the actual IB Rx adapter; five finished calls leave five listeners."""
    trader = _trader(held=100, gate_enabled=False)
    adapter = object.__new__(IBAIORx)
    adapter.trades_subject = EventSubject()
    adapter.ib = trader.client.ib

    def place(contract, order):
        order.orderId = len(trader.placed) + 1
        trade = Trade(contract, order, OrderStatus(
            orderId=order.orderId, status='Submitted',
            remaining=order.totalQuantity))
        trader.placed.append(trade)
        trader.order_tracker.on_trade(trade)
        return trade

    adapter.ib.placeOrder = place
    trader.client.subscribe_place_order = adapter.subscribe_place_order
    for _ in range(5):
        if path == 'expressive':
            result = await trader.place_expressive_order(
                _stock(), 'SELL', 1, {'order_type': 'MARKET'})
        else:
            result = await TraderServiceApi(trader).place_order_simple(
                _stock(), 'SELL', equity_amount=None, quantity=1,
                limit_price=None, market_order=True)
        assert result.is_success()
    assert len(adapter.trades_subject.observers) == 0, (
        'Each completed call retained a live per-contract observer')


def _resize_sdk(delta_status, cancel_status):
    """A real SDK with synchronous, local RPC responses and one known stop."""
    client = MagicMock()
    client.is_setup = True
    service = client.rpc.return_value
    service.place_order_simple.return_value = SuccessFail.success(obj=Trade(
        orderStatus=OrderStatus(status=delta_status)))
    service.cancel_order.return_value = SuccessFail.success(obj=Trade(
        orderStatus=OrderStatus(status=cancel_status)))
    service.place_standalone_order.return_value = SuccessFail.success(obj=Trade())
    sdk = object.__new__(MMR)
    sdk._client = client
    sdk._contract_map = {'AUDIT': _stock()}
    plan = {'adjustments': [{
        'symbol': 'AUDIT', 'conId': 100, 'current_qty': 100,
        'target_qty': 50, 'delta_qty': -50, 'action': 'SELL',
        'associated_orders': [{
            'orderId': 10, 'orderType': 'STP', 'action': 'SELL',
            'auxPrice': 8, 'lmtPrice': 0, 'trailingPercent': 0, 'tif': 'GTC',
        }],
    }]}
    return sdk, service, plan


def test_resize_waits_for_protective_cancel_before_replacement():
    """PendingCancel is a working old stop, so adding a new stop double-covers."""
    sdk, service, plan = _resize_sdk('Filled', 'PendingCancel')
    sdk.execute_resize_plan(plan)
    service.place_standalone_order.assert_not_called()


def test_resize_keeps_full_protection_until_trim_fills():
    """A Submitted 50-share sell still leaves all 100 original shares held.

    This checks the undercoverage outcome, without prescribing a sequencing
    fix. Waiting for fills alone leaves an oversized-stop race; a complete
    protocol must coordinate the reduction and protection (e.g. OCA/reduction
    semantics) and also prevent overselling, as REVIEW-O04 demonstrates.
    """
    sdk, service, plan = _resize_sdk('Submitted', 'Cancelled')
    sdk.execute_resize_plan(plan)
    original_coverage = 0 if service.cancel_order.called else 100
    replacement_coverage = sum(
        call.kwargs['quantity']
        for call in service.place_standalone_order.call_args_list)
    assert original_coverage + replacement_coverage >= 100, (
        'The broker still holds 100 shares but confirmed protectives cover only '
        f'{original_coverage + replacement_coverage}')


def _coordinated_trader(tmp_path, held=100):
    """Fresh broker snapshots and stable IDs, including modify-in-place ack."""
    trader = _trader(held=held, gate_enabled=False)
    trader.duckdb_path = str(tmp_path / 'orders.duckdb')
    trader.trading_runtime_ib_client_id = 7
    trader.unobserved_reduction_quantity = Trader.unobserved_reduction_quantity.__get__(trader)
    trader._execution_history_ready = True  # fake broker has no omitted historical executions
    trader.inventory = held
    trader.get_positions = lambda: [Position(trader.ib_account, _stock(), trader.inventory, 10)]
    broker = trader.client.ib
    sequence = iter(range(1, 1000))
    broker.client = SimpleNamespace(getReqId=lambda: next(sequence))
    broker.openTrades = lambda: [t for t in trader.placed if t.orderStatus.status not in {
        'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'}]
    broker.reqOpenOrdersAsync = AsyncMock(side_effect=lambda: broker.openTrades())
    broker.reqPositionsAsync = AsyncMock(side_effect=lambda: trader.get_positions())
    broker.isConnected = lambda: True
    broker.managedAccounts = lambda: [trader.ib_account]

    async def place(contract, order):
        if not order.orderId:
            order.orderId = broker.client.getReqId()
        order.clientId = 7
        old = next((t for t in trader.placed if t.order.orderId == order.orderId), None)
        if old is not None:
            old.order = order
            trader.order_tracker.on_trade(old)
            return rx.of(old)
        trade = Trade(contract, order, OrderStatus(orderId=order.orderId, status='Submitted',
                                                  remaining=order.totalQuantity))
        trader.placed.append(trade)
        trader.order_tracker.on_trade(trade)
        return rx.of(trade)

    trader.client.subscribe_place_order.side_effect = place
    return trader


@pytest.mark.asyncio
async def test_resize_defers_single_stop_partial_handoff_before_any_mutation(tmp_path):
    trader = _coordinated_trader(tmp_path)
    stop = await trader.place_standalone_order(_stock(), 'SELL', 100, 'STP', aux_price=8)
    assert stop.is_success()
    result = await trader.resize_position(_stock(), 50, client_intent_id='partial-resize')
    assert not result.is_success() and 'DEFERRED' in result.error
    assert len(trader.placed) == 1
    assert trader.placed[0].order.totalQuantity == 100
    assert not trader.placed[0].order.ocaGroup
    trader.client.ib.cancelOrder.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('first_fill', ['trim', 'stop'])
async def test_matching_resize_tranche_retains_partial_fill_protection_and_capacity(tmp_path, first_fill):
    """The requested broker OCA type 2 contract preserves the residual tranche.

    Fake IB models documented reduce-with-block semantics; this is not a claim
    of testing a real exchange's order support or market-gap execution prices.
    """
    trader = _coordinated_trader(tmp_path)
    for _ in range(2):
        assert (await trader.place_standalone_order(_stock(), 'SELL', 50, 'STP', aux_price=8)).is_success()
    result = await trader.resize_position(_stock(), 50, client_intent_id='paired-resize')
    assert result.is_success(), result.error
    assert result.obj['status'] == 'SUBMITTED'
    stops = [t for t in trader.placed if t.order.orderType == 'STP']
    trim = next(t for t in trader.placed if t.order.orderType == 'MKT')
    paired = next(t for t in stops if t.order.ocaGroup)
    untouched = next(t for t in stops if not t.order.ocaGroup)
    assert paired.order.ocaGroup == trim.order.ocaGroup
    assert paired.order.ocaType == trim.order.ocaType == 2
    assert paired.order.totalQuantity == trim.order.totalQuantity == 50
    assert len(trader.placed) == 3
    # A 20-share execution reduces its peer by the same amount. The still-
    # working stop protects all 80 held shares; executable capacity is also 80.
    filled, sibling = (trim, paired) if first_fill == 'trim' else (paired, trim)
    filled.orderStatus.filled = 20
    filled.orderStatus.remaining = 30
    sibling.order.totalQuantity = 30
    sibling.orderStatus.remaining = 30
    trader.inventory = 80
    assert paired.orderStatus.remaining + untouched.orderStatus.remaining == trader.inventory
    assert max(paired.orderStatus.remaining, trim.orderStatus.remaining) + untouched.orderStatus.remaining == trader.inventory


@pytest.mark.asyncio
async def test_unconfirmed_oca_modification_never_publishes_resize_trim(tmp_path):
    trader = _coordinated_trader(tmp_path)
    for _ in range(2):
        await trader.place_standalone_order(_stock(), 'SELL', 50, 'STP', aux_price=8)
    trader.client.ib.reqOpenOrdersAsync.side_effect = [trader.placed[:], []]
    result = await trader.resize_position(_stock(), 50, client_intent_id='unknown-oca')
    assert not result.is_success() and 'UNKNOWN' in result.error
    assert all(t.order.orderType == 'STP' for t in trader.placed)
    assert trader.server_order_journal().get('unknown-oca')['status'] == 'UNKNOWN'


@pytest.mark.asyncio
async def test_same_durable_intent_never_submits_twice(tmp_path):
    trader = _coordinated_trader(tmp_path)
    first = await trader.place_expressive_order(_stock(), 'SELL', 20, {'order_type': 'MARKET'},
                                                client_intent_id='one-close')
    duplicate = await trader.place_expressive_order(_stock(), 'SELL', 20, {'order_type': 'MARKET'},
                                                    client_intent_id='one-close')
    assert first.is_success()
    assert not duplicate.is_success() and 'UNKNOWN' in duplicate.error
    assert len(trader.placed) == 1
    snapshot = await trader.execution_snapshot(intent_id='one-close')
    assert snapshot['complete'] and snapshot['positions_complete']
    assert len(snapshot['orders']) == 1
    assert snapshot['orders'][0]['clientIntentId'] == 'one-close'
    assert snapshot['positions'][0]['avgCost'] == 10


@pytest.mark.asyncio
async def test_pending_reduction_cancel_retains_capacity_until_terminal(tmp_path):
    trader = _coordinated_trader(tmp_path)
    await trader.place_expressive_order(_stock(), 'SELL', 100, {'order_type': 'MARKET'})
    def pending(order):
        trader.placed[0].orderStatus.status = 'PendingCancel'
    trader.client.ib.cancelOrder.side_effect = pending
    trader.executioner._cancel_wait_timeout = 0.0
    result = await trader.place_expressive_order(_stock(), 'SELL', 100, {'order_type': 'MARKET'},
                                                 client_intent_id='pending-close')
    assert not result.is_success()
    assert len(trader.placed) == 1
    assert trader.placed[0].orderStatus.status == 'PendingCancel'


@pytest.mark.asyncio
async def test_fill_racing_cancel_clamps_replacement_to_remaining_inventory(tmp_path):
    trader = _coordinated_trader(tmp_path)
    await trader.place_expressive_order(_stock(), 'SELL', 100, {'order_type': 'MARKET'})
    def fill_then_cancel(order):
        old = trader.placed[0]
        old.orderStatus.filled = 40
        old.orderStatus.remaining = 60
        old.orderStatus.status = 'Cancelled'
        trader.inventory = 60
        trader.order_tracker.on_trade(old)
    trader.client.ib.cancelOrder.side_effect = fill_then_cancel
    result = await trader.place_expressive_order(_stock(), 'SELL', 100, {'order_type': 'MARKET'})
    assert result.is_success()
    assert trader.placed[-1].order.totalQuantity == 60


@pytest.mark.asyncio
async def test_concurrent_reductions_share_one_account_reservation(tmp_path):
    trader = _coordinated_trader(tmp_path)
    results = await asyncio.gather(*[
        trader.place_expressive_order(_stock(), 'SELL', 100, {'order_type': 'MARKET'})
        for _ in range(2)])
    assert all(r.is_success() for r in results)
    active = trader.client.ib.openTrades()
    assert sum(t.orderStatus.remaining for t in active) <= trader.inventory


@pytest.mark.asyncio
async def test_raw_expressive_rpc_cannot_substitute_for_approved_proposal():
    trader = _trader()
    trader.require_proposal_approval = True
    result = await TraderServiceApi(trader).place_expressive_order(
        _stock(), 'BUY', 1, {'order_type': 'MARKET'})
    assert not result.is_success()
    assert 'approved proposal' in result.error
    assert not trader.placed


@pytest.mark.asyncio
async def test_server_accepts_exact_approved_proposal_and_rejects_changed_quantity(tmp_path):
    from trader.data.proposal_store import ProposalStore
    from trader.trading.proposal import TradeProposal
    trader = _coordinated_trader(tmp_path, held=0)
    trader.require_proposal_approval = True
    trader.risk_gate = RiskGate(RiskLimits(), trader.event_store)
    proposal = TradeProposal('AUDIT', 'BUY', quantity=1, metadata={'con_id': 100})
    store = ProposalStore(trader.duckdb_path)
    pid = store.add(proposal)
    store.try_transition(pid, 'PENDING', 'APPROVED')
    api = TraderServiceApi(trader)
    changed = await api.place_expressive_order(
        _stock(), 'BUY', 2, proposal.execution.to_dict(),
        proposal_id=pid, client_intent_id=f'proposal:{pid}')
    assert not changed.is_success() and not trader.placed
    exact = await api.place_expressive_order(
        _stock(), 'BUY', 1, proposal.execution.to_dict(),
        proposal_id=pid, client_intent_id=f'proposal:{pid}')
    assert exact.is_success(), exact.error
    assert len(trader.placed) == 1


@pytest.mark.asyncio
async def test_foreign_notional_is_converted_and_unknown_fx_refuses_open():
    trader = _trader(portfolio_value=1000)
    foreign = Stock('FOREIGN', 'TSEJ', 'JPY', conId=101)
    trader.client.ib.tickers()[0].contract = foreign
    refused = await trader.place_expressive_order(foreign, 'BUY', 100, {'order_type': 'MARKET'})
    assert not refused.is_success() and not trader.placed
    trader.client.ib.accountValues = lambda: [
        SimpleNamespace(account=trader.ib_account, tag='NetLiquidation', currency='USD', value='1000'),
        SimpleNamespace(account=trader.ib_account, tag='ExchangeRate', currency='JPY', value='0.01'),
        SimpleNamespace(account='OTHER_ACCOUNT', tag='ExchangeRate', currency='JPY', value='99'),
    ]
    accepted = await trader.place_expressive_order(foreign, 'BUY', 100, {'order_type': 'MARKET'})
    assert accepted.is_success(), accepted.error
    submitted = [e for e in trader.event_store.events if e.event_type == EventType.ORDER_SUBMITTED]
    assert submitted[-1].metadata['notional'] == 10
    assert submitted[-1].metadata['notional_currency'] == 'BASE'


@pytest.mark.asyncio
async def test_usd_approver_tier_converts_foreign_price_before_comparison():
    trader = _trader()
    trader.approver_required_above_usd = 100
    trader.approver_key = 'unprovided'
    foreign = Stock('FOREIGN', 'TSEJ', 'JPY', conId=101)
    trader.client.ib.accountValues = lambda: [
        SimpleNamespace(account=trader.ib_account, tag='NetLiquidation', currency='CAD', value='100000'),
        SimpleNamespace(account=trader.ib_account, tag='ExchangeRate', currency='USD', value='1.4'),
        SimpleNamespace(account=trader.ib_account, tag='ExchangeRate', currency='JPY', value='0.014'),
    ]
    # 100 * JPY10 = JPY1000 = USD10, below the USD100 threshold.
    assert await trader.enforce_approver_tier(foreign, 'BUY', 100, 'MARKET', None, '') is None
    assert trader.convert_notional(1000, 'JPY', 'USD') == pytest.approx(10)


def test_sdk_missing_fx_never_silently_uses_rate_one():
    with pytest.raises(ValueError, match='FX rate unavailable'):
        MMR._to_base(100, 'JPY', {'USD': 1.0})


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['simple', 'expressive'])
async def test_split_outcome_retains_reduction_id_and_opening_refusal(path):
    trader = _trader(held=100, gate_enabled=False)
    if path == 'expressive':
        result = await trader.place_expressive_order(_stock(), 'SELL', 110, {'order_type': 'MARKET'})
        outcome = result.obj
    else:
        result = await TraderServiceApi(trader).place_order_simple(
            _stock(), 'SELL', None, 110, None, market_order=True)
        outcome = result.execution_outcome
    assert outcome['reduction']['status'] == 'SUBMITTED'
    assert outcome['reduction']['order_ids'] == [1]
    assert outcome['opening']['status'] == 'REJECTED'
    assert outcome['opening']['quantity'] == 10
    assert len(trader.placed) == 1


@pytest.mark.asyncio
async def test_emergency_exit_survives_journal_outage_without_allowing_new_exposure(tmp_path):
    trader = _coordinated_trader(tmp_path)
    # A running service proved its prior claims before this storage outage;
    # a fresh process with unreadable history must not invent empty capacity.
    await trader.unobserved_reduction_quantity(_stock(), 'SELL')
    trader.server_order_journal = MagicMock(side_effect=OSError('disk unavailable'))
    api = TraderServiceApi(trader)
    close = await api.emergency_close_position(100, 20, 'review_strategy', 'emergency-close')
    assert close.is_success(), close.error
    assert len(trader.placed) == 1
    assert trader.placed[0].order.orderRef == 'review_strategy|mmr:emergency-close'
    duplicate = await api.emergency_close_position(100, 20, 'review_strategy', 'emergency-close')
    assert not duplicate.is_success()
    assert len(trader.placed) == 1
    opened = await trader.place_expressive_order(_stock(), 'BUY', 1, {'order_type': 'MARKET'},
                                                 client_intent_id='forbidden-open')
    assert not opened.is_success() and len(trader.placed) == 1


@pytest.mark.asyncio
async def test_unknown_pending_cancel_can_resume_same_intent_after_terminal_confirmation(tmp_path):
    trader = _coordinated_trader(tmp_path)
    await trader.place_expressive_order(_stock(), 'SELL', 100, {'order_type': 'MARKET'})
    def pending(order):
        trader.placed[0].orderStatus.status = 'PendingCancel'
    trader.client.ib.cancelOrder.side_effect = pending
    trader.executioner._cancel_wait_timeout = 0
    result = await trader.place_expressive_order(_stock(), 'SELL', 100, {'order_type': 'MARKET'},
                                                 client_intent_id='retryable-close')
    assert not result.is_success() and 'UNKNOWN' in result.error
    snapshot = await trader.execution_snapshot(intent_id='retryable-close')
    assert snapshot['retry_safe']
    trader.placed[0].orderStatus.status = 'Cancelled'
    trader.order_tracker.on_trade(trader.placed[0])  # native orderStatus callback
    resumed = await trader.place_expressive_order(_stock(), 'SELL', 100, {'order_type': 'MARKET'},
                                                  client_intent_id='retryable-close')
    assert resumed.is_success(), resumed.error
    assert len(trader.placed) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['simple', 'expressive'])
async def test_restored_database_blocks_opens_and_preserves_reductions(tmp_path, path):
    trader = _coordinated_trader(tmp_path)
    trader.risk_gate = RiskGate(RiskLimits(), trader.event_store)
    (tmp_path / 'BROKER_RECONCILIATION_REQUIRED.json').write_text('{}')
    api = TraderServiceApi(trader)
    if path == 'simple':
        opened = await api.place_order_simple(_stock(), 'BUY', None, 1, None, market_order=True)
        closed = await api.place_order_simple(_stock(), 'SELL', None, 1, None, market_order=True)
    else:
        opened = await api.place_expressive_order(_stock(), 'BUY', 1, {'order_type': 'MARKET'})
        closed = await api.place_expressive_order(_stock(), 'SELL', 1, {'order_type': 'MARKET'})
    assert not opened.is_success() and 'reconciliation' in opened.error
    assert closed.is_success(), closed.error
    assert len(trader.placed) == 1 and trader.placed[0].order.action == 'SELL'
    # A complete broker snapshot is evidence, not operator authorization to
    # erase the unknown interval between the backup and the restored server.
    await trader.execution_snapshot()
    assert trader.opening_restore_error()


@pytest.mark.asyncio
async def test_direct_intent_is_durable_and_semantic_replay_does_not_duplicate(tmp_path):
    trader = _coordinated_trader(tmp_path)
    api = TraderServiceApi(trader)
    first = await api.place_order_simple(_stock(), 'SELL', None, 5, None, market_order=True,
                                         client_intent_id='direct-once')
    replay = await api.place_order_simple(contract=_stock(), action='SELL', equity_amount=None,
                                          quantity=5, limit_price=None, market_order=True,
                                          client_intent_id='direct-once')
    assert first.is_success(), first.error
    assert not replay.is_success() and 'UNKNOWN' in replay.error
    assert first.client_intent_id == 'direct-once'
    assert len(trader.placed) == 1
    assert trader.server_order_journal().get('direct-once')['orders']


@pytest.mark.asyncio
async def test_submission_audit_does_not_block_broker_loop_or_let_next_order_overtake():
    trader = _trader(held=100)
    entered, release = threading.Event(), threading.Event()
    original_append = trader.event_store.append
    def append(event):
        if event.event_type == EventType.ORDER_SUBMITTED:
            entered.set()
            release.wait(2)
        original_append(event)
    trader.event_store.append = append
    first = asyncio.create_task(trader.place_expressive_order(_stock(), 'SELL', 1, {'order_type': 'MARKET'}))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        second = asyncio.create_task(trader.place_expressive_order(_stock(), 'SELL', 1, {'order_type': 'MARKET'}))
        await asyncio.sleep(0.03)
        assert not first.done() and not second.done()
        assert len(trader.placed) == 0
    finally:
        release.set()
    assert (await first).is_success()
    assert (await second).is_success()
    assert len([e for e in trader.event_store.events if e.event_type == EventType.ORDER_SUBMITTED]) == 2


def test_account_filter_applies_to_broker_and_fallback_position_sources():
    trader = _trader()
    own = Position(trader.ib_account, _stock(), 10, 10)
    other = Position('OTHER_ACCOUNT', _stock(), 1000, 10)
    trader.client.ib.positions = MagicMock(return_value=[own, other])
    assert Trader.get_positions(trader) == [own]
    trader.client.ib.positions.return_value = []
    trader.portfolio = SimpleNamespace(get_positions=lambda: [own, other])
    assert Trader.get_positions(trader) == [own]


def test_sdk_does_not_choose_wrong_listing_when_exact_hints_fail():
    mmr = MMR.__new__(MMR)
    wrong = SimpleNamespace(conId=1, symbol='BHP', secType='STK', exchange='NYSE',
                            primaryExchange='NYSE', currency='USD')
    mmr.resolve = MagicMock(return_value=[wrong])
    mmr._client = MagicMock()
    mmr._client.is_setup = True
    mmr._client.rpc.return_value.resolve_contract.return_value = [wrong]
    with pytest.raises(ValueError, match='No exact contract'):
        mmr._resolve_contract('BHP', exchange='ASX', currency='AUD')


def test_sdk_refuses_distinct_conids_instead_of_defaulting_to_usd():
    mmr = MMR.__new__(MMR)
    mmr.resolve = MagicMock(return_value=[
        SimpleNamespace(conId=1, symbol='BHP', secType='STK', exchange='NYSE',
                        primaryExchange='NYSE', currency='USD'),
        SimpleNamespace(conId=2, symbol='BHP', secType='STK', exchange='ASX',
                        primaryExchange='ASX', currency='AUD')])
    with pytest.raises(ValueError, match='Ambiguous contract'):
        mmr._resolve_contract('BHP')


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['simple', 'expressive'])
async def test_split_result_and_every_leg_remain_under_one_durable_intent(tmp_path, path):
    trader = _coordinated_trader(tmp_path)
    intent_id = f'partial-{path}'
    if path == 'simple':
        result = await TraderServiceApi(trader).place_order_simple(
            _stock(), 'SELL', None, 110, None, market_order=True,
            client_intent_id=intent_id)
        outcome = result.execution_outcome
    else:
        result = await trader.place_expressive_order(
            _stock(), 'SELL', 110, {'order_type': 'MARKET'}, client_intent_id=intent_id)
        outcome = result.obj
    prior = trader.server_order_journal().get(intent_id)
    assert prior['status'] == 'PARTIAL'
    assert prior['outcome'] == outcome
    assert prior['orders'] == [{'orderId': 1, 'clientId': 7}]
    assert trader.placed[0].order.orderRef.endswith(f'|mmr:{intent_id}')


def test_hong_kong_listing_without_smart_capability_keeps_exact_venue():
    mmr = MMR.__new__(MMR)
    definition = SimpleNamespace(conId=6244735, symbol='2800', secType='STK', exchange='SEHK',
                                 primaryExchange='SEHK', currency='HKD', validExchanges='SEHK')
    mmr.resolve = MagicMock(return_value=[definition])
    contract = mmr._resolve_contract(6244735, exchange='SEHK', currency='HKD')
    assert contract.exchange == 'SEHK'
    assert contract.conId == 6244735 and contract.currency == 'HKD'
    assert mmr._to_contract(contract).exchange == 'SEHK'


def _submission_event():
    return TradingEvent(EventType.ORDER_SUBMITTED, dt.datetime.now(), strategy_name='review',
                        order_id=1, quantity=10,
                        metadata={'submission_identity': 'submission:paper:7:intent:1',
                                  'notional': 100, 'notional_evaluable': True})


def test_submission_audit_survives_failure_restart_and_lost_delivery_ack(tmp_path):
    store = EventStore(str(tmp_path / 'outbox.duckdb'))
    append_once = store.append_once
    store.append_once = MagicMock(side_effect=OSError('audit store unavailable'))
    first = OrderLifecycleTracker(store)
    event = _submission_event()
    try:
        with pytest.raises(TimeoutError, match='audit remains pending'):
            first.record_submission(event, timeout=0.03)
        assert not first.health['healthy']
    finally:
        first.close(timeout=0.05)

    entered, release = threading.Event(), threading.Event()
    def append(event, identity):
        entered.set()
        assert release.wait(2)
        return append_once(event, identity)
    store.append_once = append
    second = OrderLifecycleTracker(store)
    try:
        assert entered.wait(1)
        assert second.health['pending_submissions'] == 1
        assert not second.health['healthy']
        release.set()
        assert second.flush(timeout=2)
        assert second.health['healthy']
    finally:
        release.set()
        second.close()
    # Simulate crash after DuckDB commit, before SQLite delivery ACK.
    with second._journal.transaction() as conn:
        conn.execute('UPDATE broker_event_outbox SET delivered=0')
    third = OrderLifecycleTracker(store)
    try:
        assert third.flush(timeout=2)
        rows = store.query_since(dt.datetime(2000, 1, 1), EventType.ORDER_SUBMITTED)
        assert len(rows) == 1 and rows[0].quantity == 10
    finally:
        third.close()


@pytest.mark.asyncio
async def test_blocked_submission_journal_does_not_hold_broker_callback_lock(tmp_path):
    store = EventStore(str(tmp_path / 'blocked-outbox.duckdb'))
    tracker = OrderLifecycleTracker(store)
    writer = sqlite3.connect(tracker._journal.path)
    writer.execute('BEGIN IMMEDIATE')
    pool = ThreadPoolExecutor(max_workers=1)
    pending = pool.submit(tracker.record_submission, _submission_event(), 2)
    try:
        deadline = time.monotonic() + 1
        while tracker.health['pending_submissions'] == 0 and time.monotonic() < deadline:
            time.sleep(0.005)
        assert tracker.health['pending_submissions'] == 1 and not pending.done()
        trade = Trade(_stock(), order=SimpleNamespace(orderId=2, clientId=7, permId=2,
                      account='paper', totalQuantity=10, action='BUY', orderRef='review'),
                      orderStatus=OrderStatus(orderId=2, status='Submitted', filled=0, remaining=10))
        started = time.monotonic()
        tracker.on_trade(trade)
        assert time.monotonic() - started < 0.2
    finally:
        writer.rollback()
        writer.close()
        pending.result(timeout=3)
        pool.shutdown()
        tracker.close()


@pytest.mark.asyncio
async def test_unavailable_submission_audit_blocks_open_before_broker_but_not_exit(tmp_path):
    trader = _coordinated_trader(tmp_path)
    trader.risk_gate = RiskGate(RiskLimits(), trader.event_store)
    trader.executioner._audit_timeout = 0.03
    append = trader.event_store.append
    trader.event_store.append = MagicMock(side_effect=OSError('audit store unavailable'))
    try:
        opened = await trader.place_expressive_order(_stock(), 'BUY', 1, {'order_type': 'MARKET'},
                                                     client_intent_id='never-sent')
        assert not opened.is_success() and 'order was not sent' in opened.error
        assert not trader.placed
        row = trader.server_order_journal().get('never-sent')
        assert row['status'] == 'REJECTED' and not row['orders']
        closed = await trader.place_expressive_order(_stock(), 'SELL', 1, {'order_type': 'MARKET'},
                                                     client_intent_id='degraded-exit')
        assert closed.is_success(), closed.error
        assert len(trader.placed) == 1 and trader.placed[0].order.action == 'SELL'
    finally:
        trader.event_store.append = append
        await asyncio.to_thread(trader.order_tracker.close)


def test_proposal_leverage_estimate_uses_contract_multiplier_and_base_currency():
    mmr = MMR.__new__(MMR)
    store = MagicMock()
    store.add.return_value = 1
    mmr._proposal_store = lambda: store
    mmr.snapshot = MagicMock(return_value={'last': 100, 'bid': 100, 'ask': 100})
    mmr._client = MagicMock()
    mmr._client.is_setup = True
    mmr._client.rpc.return_value.get_account_values.return_value = {
        'NetLiquidation': {'value': '1000'}, 'GrossPositionValue': {'value': '200'},
        'BuyingPower': {'value': '1000'}}
    mmr._fx_rates = lambda: {'HKD': 0.1}
    mmr._resolve_contract = MagicMock(return_value=SimpleNamespace(
        conId=123, secType='OPT', currency='HKD', multiplier='10'))
    _, leverage, _ = mmr.propose('TEST', 'BUY', quantity=1, sec_type='OPT', currency='HKD')
    assert leverage['current_leverage'] == 0.2
    assert leverage['estimated_leverage'] == 0.3  # 200 base + HKD100 * 10 * 0.1.


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_extreme_amount_sizing_reaches_proposal_gate_without_placing_order():
    """Synchronous sizing must terminate even before policy rejects the open."""
    from ib_async import Ticker

    trader = _trader(held=0)
    try:
        trader.require_proposal_approval = True
        contract = Option('AUDIT', '20261218', 200, 'C', 'SMART',
                          multiplier='100', currency='USD', conId=200)
        ticker = Ticker(contract=contract)
        ticker.last = ticker.ask = ticker.bid = ticker.close = 0.1
        trader.client.get_snapshot.return_value = ticker
        trader.split_for_order = MagicMock(wraps=trader.split_for_order)

        # The explicit finite amount is accepted by the API. With native
        # prices/multiplier, unit-at-a-time rounding used to stall here before
        # the proposal-required gate could reject this unheld option opening.
        result = await TraderServiceApi(trader).place_order_simple(
            contract, 'BUY', equity_amount=1e30, quantity=None,
            limit_price=None, market_order=True)

        assert not result.is_success()
        assert 'require_proposal_approval' in result.error
        trader.client.get_snapshot.assert_awaited_once_with(contract)
        trader.split_for_order.assert_called_once()
        sized_contract, action, quantity = trader.split_for_order.call_args.args
        assert sized_contract is contract and action == 'BUY'
        assert quantity >= 1
        assert quantity * ticker.ask * 100.0 <= 1e30
        assert trader.placed == []
        trader.client.subscribe_place_order.assert_not_awaited()
        trader.check_order_margin.assert_not_awaited()
    finally:
        tracker = trader.order_tracker
        try:
            await asyncio.to_thread(tracker.close, timeout=1.0)
        finally:
            if tracker._journal is not None:
                tracker._journal.close()
            if tracker._temporary is not None:
                tracker._temporary.cleanup()
