import logging
"""Targeted tests for TradeExecutioner. Exercises the risk-gate and trading-
filter gating paths that sit between a raw order request and the IB place_order
call. A full IB round-trip is out of scope; we stub the ib_async boundary.

The boundary is the server-side EXIT-CLASS predicate: an order that reduces
the live broker position is never refusable by gates; everything else is
gated (fail-closed), and the client-supplied skip_risk_gate flag is ignored.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
import reactivex as rx

from trader.trading.approved_order import mint_approved_order

# A faithful gate record for tests that mint an OPENING token and hand it
# straight to the chokepoint (bypassing place_order, hence the gate). The
# chokepoint refuses an exposure-increasing order carrying no gate record, so a
# fixture standing in for "an approved open" must look like one — the real
# RiskGate always records the tri-state per check on approval.
_APPROVED_CHECKS = {'max_open_orders': 'pass', 'daily_loss': 'pass',
                    'concentration': 'pass', 'order_rate': 'pass'}
from trader.trading.executioner import TradeExecutioner
from trader.trading.risk_gate import RiskGateResult, RiskInputs
from trader.objects import Action, ContractOrderPair, ExecutorCondition


def _inputs(**kw):
    defaults = dict(
        open_order_count=0,
        daily_pnl=0.0,
        daily_pnl_evaluable=True,
        portfolio_value=100_000.0,
        portfolio_value_evaluable=True,
    )
    defaults.update(kw)
    return RiskInputs(**defaults)


def _make_executioner_with_trader(risk_gate=None, book_orders=None, ib_account='DU1',
                                  is_exit=False, inputs=None):
    """Assemble just enough of Trader for the executioner to exercise.

    ``risk_gate`` is set explicitly (None means "gate missing" — the real
    Trader declares risk_gate = None in __init__), and the exit-class
    predicate + risk-inputs reads are stubbed since they hit IB state.
    """
    trader = MagicMock()
    trader.ib_account = ib_account
    trader.book = MagicMock()
    trader.book.get_orders.return_value = book_orders or []
    trader.event_store = MagicMock()
    trader.client = MagicMock()
    trader.client.subscribe_place_order = AsyncMock(return_value=rx.from_iterable([MagicMock()]))
    trader.client.get_snapshot = AsyncMock(return_value=MagicMock(bid=100, ask=101))
    trader.risk_gate = risk_gate
    trader.order_reduces_exposure = MagicMock(return_value=is_exit)
    # The approver notional tier is a separate collaborator on the real Trader;
    # these executioner tests are not about the tier, so it no-ops here (the
    # real method returns None for exits and when the feature is off).
    trader.enforce_approver_tier = AsyncMock(return_value=None)
    trader.gather_risk_inputs = MagicMock(return_value=inputs or _inputs())
    ex = TradeExecutioner()
    ex.connect(trader)
    return ex, trader


class _Order:
    def __init__(self, action='BUY', account='DU1', lmt_price=100):
        self.action = action
        self.totalQuantity = 10
        self.account = account
        self.lmtPrice = lmt_price
        self.orderId = 0

    def __str__(self):
        return f'<Order {self.action} {self.totalQuantity}>'


class _Contract:
    def __init__(self, symbol='AMD', exchange='NASDAQ', sec_type='STK', conId=1):
        self.symbol = symbol
        self.exchange = exchange
        self.secType = sec_type
        self.conId = conId
        self.multiplier = None


class _ApproveAllGate:
    def check_instrument(self, **kw):
        return RiskGateResult(approved=True)

    def check_leverage(self, *a, **kw):
        return RiskGateResult(approved=True)

    def evaluate(self, **kw):
        return RiskGateResult(approved=True, checks={'max_open_orders': 'pass', 'daily_loss': 'pass', 'concentration': 'pass', 'order_rate': 'pass'})


class _RejectEvaluateGate(_ApproveAllGate):
    def evaluate(self, **kw):
        return RiskGateResult(approved=False, reason='daily loss limit')


class _BoomGate:
    """A gate whose limit checks must never run."""
    def check_instrument(self, **kw):
        return RiskGateResult(approved=False, reason='denylisted (observability only)')

    def evaluate(self, **kw):
        raise AssertionError('evaluate must not run for exit-class orders')


@pytest.mark.asyncio
async def test_trading_filter_rejection_blocks_order():
    """A denylist hit from the trading filter → no IB call, event logged."""
    class _Gate(_ApproveAllGate):
        def check_instrument(self, symbol, exchange, sec_type):
            return RiskGateResult(approved=False, reason=f'{symbol} is blocked')

    ex, trader = _make_executioner_with_trader(risk_gate=_Gate())
    pair = ContractOrderPair(contract=_Contract(symbol='AMD'), order=_Order())

    observable = await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)

    # Place should never reach the IB client
    assert trader.client.subscribe_place_order.call_count == 0

    # And it should emit an error observable rather than a happy path
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'trading filter' in str(errors[0]).lower() or 'blocked' in str(errors[0]).lower()


@pytest.mark.asyncio
async def test_risk_gate_rejection_blocks_order():
    """A rate-limit or daily-loss rejection should also block the place."""
    ex, trader = _make_executioner_with_trader(risk_gate=_RejectEvaluateGate())
    pair = ContractOrderPair(contract=_Contract(), order=_Order())

    observable = await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    assert trader.client.subscribe_place_order.call_count == 0
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'risk gate' in str(errors[0]).lower() or 'daily loss' in str(errors[0]).lower()


@pytest.mark.asyncio
async def test_evaluate_receives_full_inputs_and_price_hint():
    """The gate must get daily_pnl / portfolio_value / evaluable flags from
    gather_risk_inputs and the position-value hint — not just open orders
    (the old wiring made daily-loss and concentration dead code here)."""
    captured = {}

    class _Gate(_ApproveAllGate):
        def evaluate(self, **kw):
            captured.update(kw)
            return RiskGateResult(approved=True, checks={'max_open_orders': 'pass', 'daily_loss': 'pass', 'concentration': 'pass', 'order_rate': 'pass'})

    inputs = _inputs(open_order_count=3, daily_pnl=-42.0, portfolio_value=55_000.0)
    ex, trader = _make_executioner_with_trader(risk_gate=_Gate(), inputs=inputs)
    pair = ContractOrderPair(contract=_Contract(), order=_Order())

    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS,
                         position_value_hint=1234.5)
    assert captured['open_order_count'] == 3
    assert captured['daily_pnl'] == -42.0
    assert captured['daily_pnl_evaluable'] is True
    assert captured['portfolio_value'] == 55_000.0
    assert captured['portfolio_value_evaluable'] is True
    assert captured['position_value'] == 1234.5
    assert captured['position_value_evaluable'] is True


@pytest.mark.asyncio
async def test_limit_price_backfills_position_value_hint():
    """No caller hint, but a limit order carries its own price — the
    concentration check stays evaluable."""
    captured = {}

    class _Gate(_ApproveAllGate):
        def evaluate(self, **kw):
            captured.update(kw)
            return RiskGateResult(approved=True, checks={'max_open_orders': 'pass', 'daily_loss': 'pass', 'concentration': 'pass', 'order_rate': 'pass'})

    ex, trader = _make_executioner_with_trader(risk_gate=_Gate())
    pair = ContractOrderPair(contract=_Contract(), order=_Order(lmt_price=100))

    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    assert captured['position_value'] == 10 * 100
    assert captured['position_value_evaluable'] is True


@pytest.mark.asyncio
async def test_account_mismatch_rejected():
    """The executioner must reject orders with a wrong account before IB."""
    ex, trader = _make_executioner_with_trader(ib_account='DU1')

    bad_order = _Order(account='DU_OTHER')
    observable = await ex.subscribe_place_order_direct(
        mint_approved_order(_Contract(), bad_order, is_exit=False, checks=_APPROVED_CHECKS))
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'account' in str(errors[0]).lower()
    # Must not have reached IB
    assert trader.client.subscribe_place_order.call_count == 0


@pytest.mark.asyncio
async def test_blank_order_account_rejected():
    """A blank order.account routes to IB's default account (wrong on a
    multi-account login). It must be rejected even when ib_account is set."""
    ex, trader = _make_executioner_with_trader(ib_account='U26774889')

    blank = _Order(account='')
    observable = await ex.subscribe_place_order_direct(
        mint_approved_order(_Contract(), blank, is_exit=False, checks=_APPROVED_CHECKS))
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'blank' in str(errors[0]).lower()
    assert trader.client.subscribe_place_order.call_count == 0


@pytest.mark.asyncio
async def test_blank_configured_account_rejected():
    """If ib_account itself is blank, the old equality guard ('' == '') would
    pass a blank order straight to IB. Must be rejected."""
    ex, trader = _make_executioner_with_trader(ib_account='')

    order = _Order(account='')
    observable = await ex.subscribe_place_order_direct(
        mint_approved_order(_Contract(), order, is_exit=False, checks=_APPROVED_CHECKS))
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'no ib_account' in str(errors[0]).lower()
    assert trader.client.subscribe_place_order.call_count == 0


@pytest.mark.asyncio
async def test_exit_class_account_pinning_still_unconditional():
    """Exit-class exemption covers GATES only — the account-pinning guard in
    subscribe_place_order_direct still applies to closes."""
    ex, trader = _make_executioner_with_trader(ib_account='DU1', is_exit=True)
    pair = ContractOrderPair(contract=_Contract(), order=_Order(action='SELL', account='DU_OTHER'))

    observable = await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'account' in str(errors[0]).lower()
    assert trader.client.subscribe_place_order.call_count == 0


@pytest.mark.asyncio
async def test_happy_path_reaches_ib():
    """When the gates approve, place_order should reach the IB client."""
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    pair = ContractOrderPair(contract=_Contract(), order=_Order())

    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    assert trader.client.subscribe_place_order.call_count == 1


@pytest.mark.asyncio
async def test_skip_risk_gate_flag_no_longer_bypasses():
    """skip_risk_gate=True is IGNORED: a non-exit-class order still runs the
    gates and a rejection still blocks. (Close-all keeps working because its
    orders are exit-class, not because of the flag.)"""
    ex, trader = _make_executioner_with_trader(risk_gate=_RejectEvaluateGate())
    pair = ContractOrderPair(contract=_Contract(), order=_Order())

    observable = await ex.place_order(
        pair, condition=ExecutorCondition.NO_CHECKS, skip_risk_gate=True)
    assert trader.client.subscribe_place_order.call_count == 0
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'risk gate' in str(errors[0]).lower()


@pytest.mark.asyncio
async def test_exit_class_order_skips_gate_limits():
    """An exit-class order (reduces the live position) is never refusable by
    gates: evaluate must not run, and a filter hit is observability-only."""
    ex, trader = _make_executioner_with_trader(risk_gate=_BoomGate(), is_exit=True)
    pair = ContractOrderPair(contract=_Contract(), order=_Order(action='SELL'))

    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    assert trader.client.subscribe_place_order.call_count == 1


@pytest.mark.asyncio
async def test_gate_missing_refuses_open_fail_closed():
    """risk_gate is None (declared so on Trader until connect() builds it):
    a non-exit-class order must be refused loudly, not fail open."""
    ex, trader = _make_executioner_with_trader(risk_gate=None)
    pair = ContractOrderPair(contract=_Contract(), order=_Order())

    observable = await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    assert trader.client.subscribe_place_order.call_count == 0
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'risk gate unavailable' in str(errors[0]).lower()


@pytest.mark.asyncio
async def test_gate_missing_still_places_exit_class():
    """risk_gate None + exit-class close → placed. Disarming the gate can
    never strand a position."""
    ex, trader = _make_executioner_with_trader(risk_gate=None, is_exit=True)
    pair = ContractOrderPair(contract=_Contract(), order=_Order(action='SELL'))

    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    assert trader.client.subscribe_place_order.call_count == 1


@pytest.mark.asyncio
async def test_skip_risk_gate_true_logs_deprecation(caplog):
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    pair = ContractOrderPair(contract=_Contract(), order=_Order())

    with caplog.at_level('WARNING'):
        await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS, skip_risk_gate=True)
    assert any('deprecated' in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# ORDER_SUBMITTED stamping — the open-rate limit only works if the event's
# strategy_name matches the gate's pseudo-signal source (the order's orderRef)
# and exit-class submissions are marked so they don't count as opens.
# ---------------------------------------------------------------------------

class _OrderRef:
    """An order carrying an orderRef — the real originator the event must be
    stamped with (not a dead 'manual'/'proposal' constant)."""
    def __init__(self, action='BUY', account='DU1', order_ref='my_strat'):
        self.action = action
        self.totalQuantity = 10
        self.account = account
        self.lmtPrice = 100
        self.orderId = 0
        self.orderRef = order_ref

    def __str__(self):
        return f'<Order {self.action} {self.totalQuantity} {self.orderRef}>'


@pytest.mark.asyncio
async def test_order_submitted_stamped_with_orderref_not_manual():
    """ORDER_SUBMITTED must carry the order's orderRef as strategy_name so the
    rate check (which queries by the pseudo-signal source == orderRef) counts
    the right bucket instead of a dead 'manual' constant."""
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    appended = []
    trader.event_store.append = lambda ev: appended.append(ev)

    order = _OrderRef(order_ref='keltner_breakout')
    await ex.subscribe_place_order_direct(
        mint_approved_order(_Contract(), order, is_exit=False, checks=_APPROVED_CHECKS))

    from trader.data.event_store import EventType
    submitted = [e for e in appended if e.event_type == EventType.ORDER_SUBMITTED]
    assert len(submitted) == 1
    assert submitted[0].strategy_name == 'keltner_breakout'
    assert not submitted[0].metadata.get('exit_class')


@pytest.mark.asyncio
async def test_exit_class_order_submitted_is_marked():
    """An exit-class placement stamps exit_class=True so the open-rate limit
    excludes it."""
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    appended = []
    trader.event_store.append = lambda ev: appended.append(ev)

    order = _OrderRef(action='SELL', order_ref='keltner_breakout')
    await ex.subscribe_place_order_direct(
        mint_approved_order(_Contract(), order, is_exit=True))

    from trader.data.event_store import EventType
    submitted = [e for e in appended if e.event_type == EventType.ORDER_SUBMITTED]
    assert len(submitted) == 1
    assert submitted[0].metadata.get('exit_class') is True


@pytest.mark.asyncio
async def test_blank_orderref_falls_back_to_manual():
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    appended = []
    trader.event_store.append = lambda ev: appended.append(ev)

    order = _OrderRef(order_ref='')
    await ex.subscribe_place_order_direct(
        mint_approved_order(_Contract(), order, is_exit=False, checks=_APPROVED_CHECKS))

    from trader.data.event_store import EventType
    submitted = [e for e in appended if e.event_type == EventType.ORDER_SUBMITTED]
    assert submitted[0].strategy_name == 'manual'


@pytest.mark.asyncio
async def test_pseudo_signal_source_matches_orderref_and_sec_type_passed():
    """place_order's pseudo-signal source_name must be the order's orderRef
    (so rate counting lines up) and the order's sec_type must reach evaluate
    (so the forex CASH exemption can fire)."""
    captured = {}

    class _Gate(_ApproveAllGate):
        def evaluate(self, **kw):
            captured.update(kw)
            return RiskGateResult(approved=True, checks={'max_open_orders': 'pass', 'daily_loss': 'pass', 'concentration': 'pass', 'order_rate': 'pass'})

    ex, trader = _make_executioner_with_trader(risk_gate=_Gate())
    order = _OrderRef(order_ref='orb_strategy')
    pair = ContractOrderPair(contract=_Contract(sec_type='CASH'), order=order)

    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    assert captured['signal'].source_name == 'orb_strategy'
    assert captured['sec_type'] == 'CASH'


# ---------------------------------------------------------------------------
# ApprovedOrder capability token — place_order mints exactly one token on the
# APPROVE branch and hands it to the sink; a refusal mints none.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_place_order_mints_token_carrying_gate_checks():
    """On approve, place_order mints a token whose is_exit reflects the
    server-side classification and whose checks carry the gate's tri-state
    record — then hands THAT to the sink (nothing else reaches IB)."""
    class _Gate(_ApproveAllGate):
        def evaluate(self, **kw):
            return RiskGateResult(approved=True,
                                  checks={'daily_loss': 'pass', 'concentration': 'skipped:no-price'})

    ex, trader = _make_executioner_with_trader(risk_gate=_Gate())
    captured = {}

    from trader.trading.approved_order import ApprovedOrder

    async def _spy(approved):
        captured['token'] = approved
        return rx.from_iterable([MagicMock()])
    ex.subscribe_place_order_direct = _spy  # type: ignore[method-assign]

    pair = ContractOrderPair(contract=_Contract(), order=_Order())
    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)

    tok = captured['token']
    assert isinstance(tok, ApprovedOrder)
    assert tok.is_exit is False
    assert tok.checks == {'daily_loss': 'pass', 'concentration': 'skipped:no-price'}


@pytest.mark.asyncio
async def test_place_order_exit_class_mints_exit_token():
    """An exit-class order mints a token with is_exit=True (and no gate ran, so
    checks are empty)."""
    ex, trader = _make_executioner_with_trader(risk_gate=_BoomGate(), is_exit=True)
    captured = {}

    async def _spy(approved):
        captured['token'] = approved
        return rx.from_iterable([MagicMock()])
    ex.subscribe_place_order_direct = _spy  # type: ignore[method-assign]

    pair = ContractOrderPair(contract=_Contract(), order=_Order(action='SELL'))
    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)

    assert captured['token'].is_exit is True
    assert captured['token'].checks == {}


@pytest.mark.asyncio
async def test_refused_order_mints_no_token():
    """A gate refusal mints no token — the sink is never reached, so no
    placement is possible for an order that failed the gate."""
    ex, trader = _make_executioner_with_trader(risk_gate=_RejectEvaluateGate())
    minted = []

    async def _spy(approved):
        minted.append(approved)
        return rx.from_iterable([MagicMock()])
    ex.subscribe_place_order_direct = _spy  # type: ignore[method-assign]

    pair = ContractOrderPair(contract=_Contract(), order=_Order())
    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    assert minted == []


# ---------------------------------------------------------------------------
# helper_create_order sizing — floor-and-refuse via whole_shares_for_notional
# ---------------------------------------------------------------------------

def _make_helper_executioner():
    trader = MagicMock()
    trader.ib_account = 'DU1'
    ex = TradeExecutioner()
    ex.connect(trader)
    return ex


def _tick(bid, ask):
    t = MagicMock()
    t.bid = bid
    t.ask = ask
    return t


class TestHelperCreateOrderSizing:
    def test_buy_sized_by_ask_and_floored(self):
        ex = _make_helper_executioner()
        pair = ex.helper_create_order(
            contract=_Contract(), action=Action.BUY, latest_tick=_tick(139.0, 140.0),
            equity_amount=5000.0, quantity=None, limit_price=None, market_order=True,
            stop_loss_percentage=0.0, algo_name='test',
        )
        # 5000 / 140 = 35.7 → floor to 35, never round to 36
        assert pair.order.totalQuantity == 35

    def test_sell_sized_by_bid(self):
        ex = _make_helper_executioner()
        pair = ex.helper_create_order(
            contract=_Contract(), action=Action.SELL, latest_tick=_tick(100.0, 101.0),
            equity_amount=1000.0, quantity=None, limit_price=None, market_order=True,
            stop_loss_percentage=0.0, algo_name='test',
        )
        # 1000 / 100 (bid) = 10
        assert pair.order.totalQuantity == 10

    def test_unaffordable_share_refuses_not_bumps(self):
        """BRK.A regression: $5000 at $700k must refuse (ValueError naming
        amount and price), never bump to 1 share."""
        ex = _make_helper_executioner()
        with pytest.raises(ValueError, match='5000'):
            ex.helper_create_order(
                contract=_Contract(symbol='BRK A'), action=Action.BUY,
                latest_tick=_tick(699000.0, 700000.0),
                equity_amount=5000.0, quantity=None, limit_price=None, market_order=True,
                stop_loss_percentage=0.0, algo_name='test',
            )

    def test_explicit_quantity_untouched_including_fractional(self):
        """Fractional closes depend on the explicit-quantity path staying
        byte-identical — no rounding."""
        ex = _make_helper_executioner()
        pair = ex.helper_create_order(
            contract=_Contract(), action=Action.SELL, latest_tick=_tick(100.0, 101.0),
            equity_amount=None, quantity=2.5, limit_price=None, market_order=True,
            stop_loss_percentage=0.0, algo_name='test',
        )
        assert pair.order.totalQuantity == 2.5

    def test_multiplier_respected(self):
        ex = _make_helper_executioner()
        contract = _Contract()
        contract.multiplier = '100'
        pair = ex.helper_create_order(
            contract=contract, action=Action.BUY, latest_tick=_tick(3.0, 3.5),
            equity_amount=3000.0, quantity=None, limit_price=None, market_order=True,
            stop_loss_percentage=0.0, algo_name='test',
        )
        # 3000 / (3.5 * 100) = 8.57 → 8 contracts
        assert pair.order.totalQuantity == 8
        # postcondition: notional within the sized amount
        assert 8 * 3.5 * 100 <= 3000.0


@pytest.mark.asyncio
async def test_chokepoint_refuses_ungated_opening_token():
    """An exposure-increasing token carrying NO gate record never reaches IB.

    The capability token's type only proves someone called mint(); mint itself
    deliberately does not validate (a human-owned invariant pins that a token may
    be constructed with an empty record). So the evidence is demanded where the
    token is SPENT. Without this, any present or future mint site could place an
    opening order that no gate ever approved, and both `ty` and the type system
    would be perfectly happy.
    """
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    order = _Order()
    order.account = 'DU1'

    observable = await ex.subscribe_place_order_direct(
        mint_approved_order(_Contract(), order, is_exit=False))  # no checks

    errors = []
    observable.subscribe(on_error=errors.append)
    assert trader.client.subscribe_place_order.call_count == 0, 'ungated open reached IB'
    assert errors and 'placement refused' in str(errors[0])


@pytest.mark.asyncio
async def test_chokepoint_refuses_opening_token_with_a_failed_check():
    """A token minted with a FAILED check is refused too — a record that exists
    is not the same as a record that passed."""
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    order = _Order()
    order.account = 'DU1'

    observable = await ex.subscribe_place_order_direct(
        mint_approved_order(_Contract(), order, is_exit=False,
                            checks={'daily_loss': 'fail', 'concentration': 'pass'}))

    errors = []
    observable.subscribe(on_error=errors.append)
    assert trader.client.subscribe_place_order.call_count == 0
    assert errors and 'daily_loss' in str(errors[0])


@pytest.mark.asyncio
async def test_chokepoint_allows_exit_token_with_empty_record():
    """Exits are exempt: they are never gate-refusable, so an empty record is
    legitimate and must still place. Refusing an exit is the worse failure."""
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    order = _Order()
    order.account = 'DU1'

    await ex.subscribe_place_order_direct(
        mint_approved_order(_Contract(), order, is_exit=True))

    assert trader.client.subscribe_place_order.call_count == 1


@pytest.mark.asyncio
async def test_stale_position_classified_exit_is_logged_but_still_placed(caplog):
    """A POSITION_CLASSIFIED exit whose position has since moved is corroborated,
    logged loudly — and PLACED anyway. Refusing an exit is worse than acting on a
    stale classification, and refusing here would re-introduce the read-race that
    enforce_approver_tier documents as its reason not to re-check."""
    from trader.trading.approved_order import ExitReason
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    trader.order_reduces_exposure = MagicMock(return_value=False)  # position moved
    order = _Order()
    order.account = 'DU1'

    with caplog.at_level(logging.ERROR):
        await ex.subscribe_place_order_direct(
            mint_approved_order(_Contract(), order, is_exit=True,
                                exit_reason=ExitReason.POSITION_CLASSIFIED))

    assert trader.client.subscribe_place_order.call_count == 1, 'an exit was refused'
    assert 'STALE exit claim' in caplog.text


@pytest.mark.asyncio
async def test_protective_child_is_not_corroborated(caplog):
    """A bracket leg is exit-class BY CONSTRUCTION — its entry is staged and has
    not filled, so no position exists. Corroborating it would alarm on every
    single bracket, so the predicate must not even be consulted."""
    from trader.trading.approved_order import ExitReason
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    trader.order_reduces_exposure = MagicMock(return_value=False)  # no position yet
    order = _Order()
    order.account = 'DU1'

    with caplog.at_level(logging.ERROR):
        await ex.subscribe_place_order_direct(
            mint_approved_order(_Contract(), order, is_exit=True,
                                exit_reason=ExitReason.PROTECTIVE_CHILD))

    assert trader.client.subscribe_place_order.call_count == 1
    assert 'STALE exit claim' not in caplog.text
    trader.order_reduces_exposure.assert_not_called()


@pytest.mark.asyncio
async def test_unattributed_exit_exemption_is_logged(caplog):
    """An exit with no ExitReason skipped the gate-record check without stating
    why. Placed (never refuse an exit) but recorded as unattributed."""
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    order = _Order()
    order.account = 'DU1'

    with caplog.at_level(logging.ERROR):
        await ex.subscribe_place_order_direct(
            mint_approved_order(_Contract(), order, is_exit=True))

    assert trader.client.subscribe_place_order.call_count == 1
    assert 'UNATTRIBUTED exit exemption' in caplog.text


@pytest.mark.asyncio
async def test_corroboration_failure_does_not_block_the_exit(caplog):
    """If the position re-read itself raises, the exit still places."""
    from trader.trading.approved_order import ExitReason
    ex, trader = _make_executioner_with_trader(risk_gate=_ApproveAllGate())
    trader.order_reduces_exposure = MagicMock(side_effect=RuntimeError('portfolio unreadable'))
    order = _Order()
    order.account = 'DU1'

    with caplog.at_level(logging.WARNING):
        await ex.subscribe_place_order_direct(
            mint_approved_order(_Contract(), order, is_exit=True,
                                exit_reason=ExitReason.POSITION_CLASSIFIED))

    assert trader.client.subscribe_place_order.call_count == 1
    assert 'could not corroborate' in caplog.text
