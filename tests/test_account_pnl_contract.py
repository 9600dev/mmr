"""Only a current, exact-account PnL callback can authorize new exposure.

Position PnL is an incomplete display feed: a finite surviving position says
nothing about today's realized loss on positions that are already closed.
All broker events here are local; no IB connection or account is contacted.
"""

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from eventkit import Event
from ib_async import OrderStatus, PnL, PnLSingle, Stock, Trade

from trader.objects import Action
from trader.trading.proposal import ExecutionSpec
from trader.trading.risk_gate import RiskGateResult
from trader.trading.strategy import Signal
from trader.trading import trading_runtime
from trader.trading.trading_runtime import Trader


ACCOUNT = 'DU_ACCOUNT_PNL_TEST'


class AccountIB:
    """The reqPnL object and its event are separate pieces of evidence."""

    def __init__(self):
        self.pnlEvent = Event('pnlEvent')
        self.connectedEvent = Event('connectedEvent')
        self.disconnectedEvent = Event('disconnectedEvent')
        for name in ('orderStatusEvent', 'orderModifyEvent', 'newOrderEvent',
                     'cancelOrderEvent', 'openOrderEvent', 'execDetailsEvent'):
            setattr(self, name, Event(name))
        self.connected = True
        self.requests = []
        self.cancellations = []
        self.active = {}
        self.on_request = None

    def isConnected(self):
        return self.connected

    def managedAccounts(self):
        return [ACCOUNT]

    def openTrades(self):
        # This fake has no working orders. The competing-order read now fails
        # LOUD when it cannot read the broker (2026-09-14), so a fake must
        # answer explicitly rather than rely on the old swallowed exception.
        return []

    def accountValues(self):
        return [SimpleNamespace(account=ACCOUNT, tag='NetLiquidation',
                                currency='USD', value='100000')]

    def reqPnL(self, account, modelCode=''):
        key = account, modelCode
        assert key not in self.active, 'duplicate live account PnL subscription'
        item = PnL(account=account, modelCode=modelCode)
        self.active[key] = item
        self.requests.append(key)
        if self.on_request is not None:
            self.on_request(item)
        return item

    def cancelPnL(self, account, modelCode=''):
        self.cancellations.append((account, modelCode))
        self.active.pop((account, modelCode), None)

    def pnl(self, account='', modelCode=''):
        return [item for (scope, model), item in self.active.items()
                if (not account or account == scope) and model == modelCode]

    def disconnect(self):
        self.connected = False

    def emit(self, item, daily):
        item.dailyPnL = daily
        self.pnlEvent.emit(item)


@pytest.fixture
def account_trader(risk_gate):
    trader = object.__new__(Trader)
    trader.ib_account = ACCOUNT
    trader.client = SimpleNamespace(ib=AccountIB())
    trader.book = SimpleNamespace(get_open_order_count=lambda: 0)
    trader.get_positions = lambda: []
    trader.get_pnl = lambda: []
    trader.event_store = SimpleNamespace(count_since=lambda *args, **kwargs: 0)
    trader.risk_gate = risk_gate
    trader._ib_upstream_connected = True
    trader._ib_upstream_error = ''
    trader._main_loop = None
    yield trader
    # Disconnect only this test's local event handler. No live client exists.
    invalidate = getattr(trader, '_invalidate_account_pnl', None)
    if invalidate is not None:
        invalidate()


def opening_decision(trader):
    inputs = trader.gather_risk_inputs()
    return trader.risk_gate.evaluate(
        Signal(source_name='account-pnl-contract', action=Action.BUY,
               probability=1.0, risk=0.0),
        open_order_count=inputs.open_order_count,
        daily_pnl=inputs.daily_pnl,
        daily_pnl_evaluable=inputs.daily_pnl_evaluable,
        portfolio_value=inputs.portfolio_value,
        portfolio_value_evaluable=inputs.portfolio_value_evaluable,
        position_value=100.0,
    )


def assert_pnl_unknown(trader):
    assert trader.gather_risk_inputs().daily_pnl_evaluable is False
    decision = opening_decision(trader)
    assert decision.approved is False
    assert decision.checks['daily_loss'] == 'unevaluable:daily-pnl'


def test_finite_position_subset_cannot_certify_account_after_closed_loss(account_trader):
    trader = account_trader
    # A remaining position is up $25; a closed position lost $2,000. The
    # position display cache contains only the former, even after it warms.
    trader.get_pnl = lambda: [PnLSingle(account=ACCOUNT, conId=1, dailyPnL=25.0)]
    trader.event_store = SimpleNamespace(count_since=lambda *args, **kwargs: 3)
    assert_pnl_unknown(trader)


def test_flat_account_without_local_fills_does_not_prove_zero_daily_loss(account_trader):
    # The operator can have closed a losing trade through another client.
    # Neither an empty local history nor an empty position book proves zero.
    assert_pnl_unknown(account_trader)


@pytest.mark.parametrize('boundary', ['replacement', 'upstream_lost'])
def test_stale_position_cache_cannot_authorize_after_connection_boundary(account_trader, boundary):
    trader = account_trader
    trader.get_pnl = lambda: [PnLSingle(account=ACCOUNT, conId=1, dailyPnL=0.0)]
    if boundary == 'replacement':
        trader.client.ib = AccountIB()
    else:
        trader._on_ib_error(SimpleNamespace(errorCode=1100, errorString='test upstream loss'))
    assert_pnl_unknown(trader)


def subscribe(trader):
    trader._ensure_account_pnl_subscription()
    ib = trader.client.ib
    assert ib.requests[-1] == (ACCOUNT, '')
    return ib, ib.active[ACCOUNT, '']


def test_position_feed_warms_but_only_account_callback_exposes_closed_loss(account_trader):
    trader = account_trader
    ib, account_pnl = subscribe(trader)
    display = PnLSingle(account=ACCOUNT, conId=1)
    trader.get_pnl = lambda: [display]
    assert_pnl_unknown(trader)
    display.dailyPnL = 25.0  # Position-level feed does warm in the real startup.
    assert_pnl_unknown(trader)

    # The account reports $25 on the surviving position and -$2,000 realized
    # elsewhere. Risk consumes the account total once it actually arrives.
    ib.emit(account_pnl, -1975.0)
    inputs = trader.gather_risk_inputs()
    assert inputs.daily_pnl_evaluable is True
    assert inputs.daily_pnl == -1975.0
    decision = opening_decision(trader)
    assert decision.approved is False
    assert decision.checks['daily_loss'] == 'fail'


def test_position_display_failure_does_not_erase_a_received_account_total(account_trader):
    trader = account_trader
    ib, item = subscribe(trader)
    ib.emit(item, -25.0)

    def broken_display():
        raise RuntimeError('test position display is unavailable')

    trader.get_pnl = broken_display
    inputs = trader.gather_risk_inputs()
    assert inputs.daily_pnl == -25.0
    assert inputs.daily_pnl_evaluable is True
    assert opening_decision(trader).approved is True


@pytest.mark.parametrize('daily', [0.0, 25.0, -75.0])
def test_fresh_finite_account_receipt_is_usable_even_while_flat(account_trader, daily):
    trader = account_trader
    ib, item = subscribe(trader)
    assert_pnl_unknown(trader)
    ib.emit(item, daily)
    inputs = trader.gather_risk_inputs()
    assert inputs.daily_pnl_evaluable is True
    assert inputs.daily_pnl == daily
    assert opening_decision(trader).approved is True


def test_finite_returned_object_requires_actual_callback(account_trader):
    trader = account_trader
    ib = trader.client.ib
    ib.on_request = lambda item: setattr(item, 'dailyPnL', 0.0)
    _, item = subscribe(trader)
    assert_pnl_unknown(trader)
    ib.emit(item, 0.0)
    assert opening_decision(trader).approved is True


def test_callback_during_request_return_counts_for_exact_requested_object(account_trader):
    trader = account_trader
    ib = trader.client.ib
    ib.on_request = lambda item: ib.emit(item, -25.0)
    subscribe(trader)
    inputs = trader.gather_risk_inputs()
    assert inputs.daily_pnl_evaluable is True
    assert inputs.daily_pnl == -25.0


def test_superseded_request_failure_does_not_cancel_new_callback_checkpoint(account_trader):
    """Defensive reentrancy control; production reqPnL does not pump a loop."""
    trader = account_trader
    old_ib = trader.client.ib
    new_ib = AccountIB()
    new_ib.on_request = lambda item: new_ib.emit(item, -25.0)

    def replace_during_request(old_item):
        trader.client.ib = new_ib
        trader._ensure_account_pnl_subscription()
        raise ConnectionError('superseded test request failed after replacement')

    old_ib.on_request = replace_during_request
    trader._ensure_account_pnl_subscription()
    assert old_ib.active == {}
    assert len(old_ib.pnlEvent) == 0
    assert new_ib.requests == [(ACCOUNT, '')]
    assert new_ib.cancellations == []
    assert trader.gather_risk_inputs().daily_pnl == -25.0
    assert opening_decision(trader).approved is True


@pytest.mark.parametrize('wrong_receipt', ['other_account', 'model_subset', 'same_fields_new_object'])
def test_unrelated_callback_cannot_warm_exact_account_subscription(account_trader, wrong_receipt):
    trader = account_trader
    ib, item = subscribe(trader)
    # Also make the actual returned object finite: a receipt for some other
    # object must not bless this cached number by accident.
    item.dailyPnL = 0.0
    receipt = PnL(account=ACCOUNT, modelCode='', dailyPnL=0.0)
    if wrong_receipt == 'other_account':
        receipt.account = 'DU_DIFFERENT_TEST_ACCOUNT'
    elif wrong_receipt == 'model_subset':
        receipt.modelCode = 'subset'
    ib.pnlEvent.emit(receipt)
    assert_pnl_unknown(trader)
    ib.emit(item, -30.0)
    assert trader.gather_risk_inputs().daily_pnl == -30.0
    assert opening_decision(trader).approved is True


@pytest.mark.parametrize('bad', [float('nan'), float('inf'), -float('inf'),
                               sys.float_info.max, -sys.float_info.max, None,
                               'unavailable'])
def test_invalid_account_update_revokes_previously_usable_value(account_trader, bad):
    trader = account_trader
    ib, item = subscribe(trader)
    ib.emit(item, 0.0)
    assert opening_decision(trader).approved is True
    ib.emit(item, bad)
    assert_pnl_unknown(trader)
    ib.emit(item, -25.0)
    assert opening_decision(trader).approved is True


def test_missing_account_value_cannot_be_coerced_to_zero(account_trader):
    trader = account_trader
    ib, item = subscribe(trader)
    ib.emit(item, 0.0)
    del item.dailyPnL
    ib.pnlEvent.emit(item)
    assert_pnl_unknown(trader)


def test_duplicate_subscription_setup_does_not_duplicate_requests_or_lose_receipt(account_trader):
    trader = account_trader
    ib, item = subscribe(trader)
    ib.emit(item, -25.0)
    for _ in range(3):
        trader._ensure_account_pnl_subscription()
    assert ib.requests == [(ACCOUNT, '')]
    assert len(ib.pnlEvent) == 1
    assert trader.gather_risk_inputs().daily_pnl == -25.0
    assert opening_decision(trader).approved is True


def test_invalidation_detaches_old_object_and_replacement_requires_new_receipt(account_trader):
    trader = account_trader
    old_ib, old_item = subscribe(trader)
    old_ib.emit(old_item, 0.0)
    assert opening_decision(trader).approved is True
    trader._invalidate_account_pnl()
    assert len(old_ib.pnlEvent) == 0
    assert_pnl_unknown(trader)
    ib, item = subscribe(trader)
    assert item is not old_item
    assert ib.requests == [(ACCOUNT, ''), (ACCOUNT, '')]
    old_ib.emit(old_item, 0.0)
    assert_pnl_unknown(trader)
    ib.emit(item, 0.0)
    assert opening_decision(trader).approved is True


def test_replaced_ib_instance_cannot_use_old_object_even_before_setup(account_trader):
    trader = account_trader
    old_ib, old_item = subscribe(trader)
    old_ib.emit(old_item, 0.0)
    assert opening_decision(trader).approved is True
    trader.client.ib = AccountIB()
    assert_pnl_unknown(trader)
    new_ib, new_item = subscribe(trader)
    assert len(old_ib.pnlEvent) == 0
    old_ib.emit(old_item, 0.0)
    assert_pnl_unknown(trader)
    new_ib.emit(new_item, -50.0)
    assert trader.gather_risk_inputs().daily_pnl == -50.0
    assert opening_decision(trader).approved is True


@pytest.mark.asyncio
@pytest.mark.parametrize('connection', ['same_instance', 'replacement', 'replacement_lost_upstream'])
async def test_connected_event_binds_upstream_status_to_its_ib_instance(account_trader, connection):
    trader = account_trader
    old_ib, old_item = subscribe(trader)
    old_ib.emit(old_item, 0.0)
    trader._on_ib_error(SimpleNamespace(errorCode=1100, errorString='old session loss'))
    if connection != 'same_instance':
        trader.client.ib = AccountIB()
    if connection == 'replacement_lost_upstream':
        trader._on_ib_error(SimpleNamespace(errorCode=1100, errorString='new session loss'))

    # Account feed binding precedes the existing setup-coalescing guard. A
    # successful new socket need not repeat the old socket's 1101/1102 event.
    trader._in_connected_event = True
    await trader.connected_event()
    ib = trader.client.ib
    assert_pnl_unknown(trader)
    old_ib.emit(old_item, 0.0)
    assert_pnl_unknown(trader)
    if connection == 'replacement':
        assert ib.requests == [(ACCOUNT, '')]
        ib.emit(ib.active[ACCOUNT, ''], -25.0)
        assert opening_decision(trader).approved is True
    else:
        assert ib.requests == ([(ACCOUNT, '')] if connection == 'same_instance' else [])
        assert trader._ib_upstream_connected is False


def test_explicit_reconnect_revokes_readiness_before_disconnect(account_trader):
    trader = account_trader
    ib, item = subscribe(trader)
    ib.emit(item, 0.0)
    observed = []

    def disconnect():
        observed.append(trader.gather_risk_inputs().daily_pnl_evaluable)
        ib.connected = False

    ib.disconnect = disconnect
    trader.reconnect()
    assert observed == [False]
    assert_pnl_unknown(trader)


@pytest.mark.asyncio
async def test_shutdown_revokes_readiness_before_disconnect(account_trader):
    trader = account_trader
    ib, item = subscribe(trader)
    ib.emit(item, 0.0)
    observed = []

    def stop_at_disconnect():
        observed.append(trader.gather_risk_inputs().daily_pnl_evaluable)
        raise asyncio.CancelledError

    ib.disconnect = stop_at_disconnect
    with pytest.raises(asyncio.CancelledError):
        await trader.shutdown()
    assert observed == [False]
    assert_pnl_unknown(trader)


@pytest.mark.asyncio
@pytest.mark.parametrize('already_reconnecting', [False, True])
async def test_disconnect_event_revokes_readiness_before_wait_or_coalescing(
        account_trader, monkeypatch, already_reconnecting):
    trader = account_trader
    ib, item = subscribe(trader)
    ib.emit(item, 0.0)
    trader._reconnecting = already_reconnecting
    at_wait = []

    async def stop_at_first_wait(delay):
        at_wait.append(trader.gather_risk_inputs().daily_pnl_evaluable)
        raise asyncio.CancelledError

    monkeypatch.setattr(trading_runtime.asyncio, 'sleep', stop_at_first_wait)
    if already_reconnecting:
        await trader.disconnected_event()
        assert at_wait == []
    else:
        with pytest.raises(asyncio.CancelledError):
            await trader.disconnected_event()
        assert at_wait == [False]
    assert_pnl_unknown(trader)


@pytest.mark.parametrize('restore_code', [1101, 1102])
def test_upstream_loss_requires_new_receipt_after_restoration(account_trader, restore_code):
    trader = account_trader
    ib, old_item = subscribe(trader)
    ib.emit(old_item, 0.0)
    trader._on_ib_error(SimpleNamespace(errorCode=1100, errorString='test loss'))
    ib.emit(old_item, 0.0)
    assert_pnl_unknown(trader)
    trader._on_ib_error(SimpleNamespace(errorCode=restore_code, errorString='test restored'))
    # Restore means resubscribe, not reuse yesterday's/session's ready flag.
    ib, new_item = subscribe(trader)
    assert new_item is not old_item
    assert_pnl_unknown(trader)
    ib.emit(new_item, -50.0)
    assert trader.gather_risk_inputs().daily_pnl == -50.0
    assert opening_decision(trader).approved is True


def test_per_farm_warning_does_not_invalidate_account_receipt(account_trader):
    trader = account_trader
    ib, item = subscribe(trader)
    ib.emit(item, -50.0)
    trader._on_ib_error(SimpleNamespace(errorCode=2103, errorString='test farm warning'))
    assert ib.requests == [(ACCOUNT, '')]
    assert trader.gather_risk_inputs().daily_pnl == -50.0
    assert opening_decision(trader).approved is True


@pytest.mark.parametrize('registered_before_failure', [False, True])
def test_subscription_failure_is_unknown_and_can_retry(
        account_trader, monkeypatch, registered_before_failure):
    trader = account_trader
    ib = trader.client.ib
    request = ib.reqPnL

    def refused_request(*args, **kwargs):
        raise ConnectionError('test request was not sent')

    if registered_before_failure:
        # ib_async records the account/model mapping before sending the wire
        # request, so a failed send can leave a duplicate-subscription trap.
        ib.on_request = refused_request
    else:
        monkeypatch.setattr(ib, 'reqPnL', refused_request)
    trader._ensure_account_pnl_subscription()
    assert_pnl_unknown(trader)
    assert ib.active == {}
    ib.on_request = None
    monkeypatch.setattr(ib, 'reqPnL', request)
    ib, item = subscribe(trader)
    ib.emit(item, 0.0)
    assert len(ib.pnlEvent) == 1
    assert opening_decision(trader).approved is True


@pytest.mark.parametrize('account', ['', None])
def test_missing_pinned_account_cannot_start_an_aggregate_subscription(account_trader, account):
    trader = account_trader
    trader.ib_account = account
    trader._ensure_account_pnl_subscription()
    assert trader.client.ib.requests == []
    assert_pnl_unknown(trader)


@pytest.mark.parametrize('field,value', [('account', 'DU_OTHER_TEST'), ('modelCode', 'subset')])
def test_current_object_with_wrong_scope_cannot_certify_account_total(account_trader, field, value):
    trader = account_trader
    ib, item = subscribe(trader)
    ib.emit(item, 0.0)
    setattr(item, field, value)
    ib.emit(item, 0.0)
    assert_pnl_unknown(trader)


@pytest.mark.asyncio
async def test_setup_subscribes_to_account_before_waiting_for_position_events(account_trader):
    trader = account_trader
    entered = asyncio.Event()
    never = asyncio.Event()

    async def wait_for_book(events):
        entered.set()
        await never.wait()

    trader.book.subscribe_to_eventkit_event = wait_for_book
    task = asyncio.create_task(trader.setup_subscriptions())
    try:
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        assert trader.client.ib.requests == [(ACCOUNT, '')]
        assert_pnl_unknown(trader)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


class OrderSink:
    def __init__(self):
        self.placed = []

    async def subscribe_place_order_direct(self, approved):
        import reactivex as rx
        self.placed.append(approved)
        approved.order.orderId = len(self.placed)
        return rx.just(Trade(contract=approved.contract, order=approved.order,
                             orderStatus=OrderStatus(status='Submitted')))


def prepare_order_path(trader):
    trader.executioner = OrderSink()
    trader.order_tracker = None
    trader.margin_checks = AsyncMock(return_value=RiskGateResult(True, checks={'leverage': 'pass'}))
    trader.enforce_approver_tier = AsyncMock(return_value=None)
    return Stock('PNLTEST', 'SMART', 'USD', conId=99101)


@pytest.mark.asyncio
async def test_expressive_buy_refuses_before_receipt_then_checks_actual_closed_loss(account_trader):
    trader = account_trader
    contract = prepare_order_path(trader)
    ib, item = subscribe(trader)
    trader.get_pnl = lambda: [PnLSingle(account=ACCOUNT, conId=1, dailyPnL=25.0)]
    spec = ExecutionSpec(order_type='LIMIT', limit_price=100.0, exit_type='NONE').to_dict()

    unknown = await trader.place_expressive_order(contract, 'BUY', 1.0, spec)
    assert not unknown.is_success()
    assert 'daily PnL could not be read' in unknown.error
    assert trader.executioner.placed == []

    ib.emit(item, -1975.0)
    loss = await trader.place_expressive_order(contract, 'BUY', 1.0, spec)
    assert not loss.is_success()
    assert 'daily loss limit exceeded' in loss.error
    assert trader.executioner.placed == []

    ib.emit(item, -25.0)
    warmed = await trader.place_expressive_order(contract, 'BUY', 1.0, spec)
    assert warmed.is_success(), warmed.error
    assert len(trader.executioner.placed) == 1
    assert trader.executioner.placed[0].is_exit is False
    assert trader.executioner.placed[0].checks['daily_loss'] == 'pass'


@pytest.mark.asyncio
async def test_expressive_exit_still_submits_with_unknown_account_pnl(account_trader):
    trader = account_trader
    contract = prepare_order_path(trader)
    trader.get_positions = lambda: [SimpleNamespace(
        account=ACCOUNT, contract=contract, position=1.0, avgCost=100.0)]
    assert_pnl_unknown(trader)
    spec = ExecutionSpec(order_type='LIMIT', limit_price=100.0, exit_type='NONE').to_dict()
    result = await trader.place_expressive_order(contract, 'SELL', 1.0, spec)
    assert result.is_success(), result.error
    assert len(trader.executioner.placed) == 1
    approved = trader.executioner.placed[0]
    assert approved.is_exit is True
    assert approved.order.action == 'SELL'
    assert approved.order.totalQuantity == 1.0
    trader.margin_checks.assert_not_awaited()
