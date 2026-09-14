"""Physical reduction coordination at the placement chokepoint.

``test_gate_properties.py`` says no POLICY gate refuses an exit. This file pins
what happens when an exit meets other WORKING reductions on the same
instrument, which is coordination, not policy:

  1. With no competing reservation an exit is always placed, at the requested
     quantity (bounding it to the position is the splitter's job, pinned in
     ``test_order_split.py``).
  2. Owner classes: a strategy name is its own class; every operator path
     (``mmr buy/sell`` stamps 'global', ``approve`` stamps 'proposal', plus ''
     and 'manual') is one class.
  3. An exit may displace only working reductions of its OWN class: those are
     cancelled, confirmed terminal, and the exit is then placed.
  4. When another class's working reduction claims the shares, the exit is
     refused ``DEFERRED`` BEFORE any cancel is sent and that order is left
     working. A partial claim clamps the exit to the unreserved remainder,
     again without touching the foreign order.

Field origin (review, 2026-09-11): a manual close cancelled a strategy's
disaster stop and could then be refused (cancel unconfirmed, inventory
unreadable, an earlier unconfirmed send still reserving capacity), leaving the
position naked with nothing working against it.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import reactivex as rx
from hypothesis import HealthCheck, given, settings, strategies as st
from ib_async import Contract, Order, OrderStatus, Trade

from trader.objects import ContractOrderPair, ExecutorCondition
from trader.trading.executioner import TradeExecutioner

ACCOUNT = 'DU1'
TERMINAL = {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'}
OPERATOR_ALIASES = ('', 'global', 'proposal', 'manual')
STRATEGIES = ('orb_googl', 'orb_pltr')

_SETTINGS = settings(max_examples=60, deadline=None,
                     suppress_health_check=[HealthCheck.too_slow])


def _contract():
    return Contract(secType='STK', conId=4391, symbol='AMD', exchange='SMART', currency='USD')


def _working(order_id, quantity, owner):
    """A working protective reduction as the broker reports it."""
    order = Order(orderId=order_id, action='SELL', totalQuantity=float(quantity), orderType='STP',
                  auxPrice=1.0, account=ACCOUNT, orderRef=f'{owner}|mmr:reserved-{order_id}')
    return Trade(_contract(), order, OrderStatus(orderId=order_id, status='Submitted', remaining=float(quantity)))


def _executioner(held, working):
    """Stub-trader idiom from test_gate_properties: exit-class predicate answers
    True, no unobserved claims, the given working reductions on the book."""
    trader = MagicMock()
    trader.ib_account = ACCOUNT
    trader.event_store = MagicMock()
    trader.order_reduces_exposure = MagicMock(return_value=True)
    trader.enforce_approver_tier = AsyncMock(return_value=None)
    trader.unobserved_reduction_quantity = AsyncMock(return_value=0.0)
    trader._signed_position = MagicMock(return_value=float(held))
    trader.working_trades = lambda contract: [t for t in working if t.orderStatus.status not in TERMINAL]
    placed = []

    async def place(contract, order):
        placed.append(order)
        return rx.from_iterable([MagicMock()])

    def cancel(order):
        for trade in working:
            if trade.order.orderId == order.orderId:
                trade.orderStatus.status = 'Cancelled'

    trader.client = MagicMock()
    trader.client.subscribe_place_order = AsyncMock(side_effect=place)
    trader.client.ib.cancelOrder = MagicMock(side_effect=cancel)
    ex = TradeExecutioner()
    ex.connect(trader)
    return ex, trader, placed


def _place_exit(ex, quantity, owner):
    order = Order(action='SELL', totalQuantity=float(quantity), orderType='MKT', account=ACCOUNT, orderRef=owner)
    pair = ContractOrderPair(contract=_contract(), order=order)
    errors = []

    async def _run():
        observable = await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
        observable.subscribe(on_next=lambda _: None, on_error=errors.append)

    asyncio.run(_run())
    return errors


@st.composite
def _book(draw, foreign):
    """(held, reserved, exit_owner, competing_owner) with reserved <= held and
    the competing owner in a different class (foreign) or the same class."""
    held = draw(st.integers(min_value=1, max_value=10_000))
    reserved = draw(st.integers(min_value=1, max_value=held))
    exit_owner = draw(st.sampled_from(OPERATOR_ALIASES + STRATEGIES))
    exit_is_operator = exit_owner in OPERATOR_ALIASES
    if foreign:
        pool = STRATEGIES if exit_is_operator else OPERATOR_ALIASES + tuple(s for s in STRATEGIES if s != exit_owner)
    else:
        pool = OPERATOR_ALIASES if exit_is_operator else (exit_owner,)
    return held, reserved, exit_owner, draw(st.sampled_from(pool))


# ---------------------------------------------------------------------------
# 1. Nothing competing: always placed, as requested
# ---------------------------------------------------------------------------

@_SETTINGS
@given(held=st.integers(min_value=1, max_value=10_000),
       quantity=st.integers(min_value=1, max_value=20_000),
       owner=st.sampled_from(OPERATOR_ALIASES + STRATEGIES))
def test_exit_with_no_competing_reservation_is_always_placed(held, quantity, owner):
    ex, trader, placed = _executioner(held, [])
    errors = _place_exit(ex, quantity, owner)
    assert errors == [], f'exit refused with nothing competing: {errors}'
    (order,) = placed
    assert order.totalQuantity == float(quantity)
    trader.client.ib.cancelOrder.assert_not_called()


# ---------------------------------------------------------------------------
# 2. Owner classes
# ---------------------------------------------------------------------------

@_SETTINGS
@given(name=st.text(min_size=1).filter(lambda s: s not in OPERATOR_ALIASES and '|mmr:' not in s),
       suffix=st.text().filter(lambda s: '|mmr:' not in s))
def test_owner_class_is_the_strategy_or_the_operator(name, suffix):
    assert TradeExecutioner._owner_class(name) == name
    assert TradeExecutioner._owner_class(f'{name}|mmr:{suffix}') == name
    for alias in OPERATOR_ALIASES:
        assert TradeExecutioner._owner_class(alias) == 'operator'
        assert TradeExecutioner._owner_class(f'{alias}|mmr:{suffix}') == 'operator'
    assert TradeExecutioner._owner_class(None) == 'operator'


# ---------------------------------------------------------------------------
# 3. Own class: displaced, then placed
# ---------------------------------------------------------------------------

@_SETTINGS
@given(book=_book(foreign=False), quantity=st.integers(min_value=1, max_value=20_000))
def test_exit_displaces_only_its_own_owner_class(book, quantity):
    held, reserved, exit_owner, own_owner = book
    own = _working(11, reserved, own_owner)
    ex, trader, placed = _executioner(held, [own])
    errors = _place_exit(ex, quantity, exit_owner)
    assert errors == [], f'own-class displacement must not refuse: {errors}'
    (order,) = placed
    if reserved + quantity <= held:
        # It fits beside the working order: nothing is displaced.
        trader.client.ib.cancelOrder.assert_not_called()
        assert own.orderStatus.status == 'Submitted'
        assert order.totalQuantity == float(quantity)
    else:
        trader.client.ib.cancelOrder.assert_called_once()
        assert trader.client.ib.cancelOrder.call_args.args[0].orderId == 11
        assert own.orderStatus.status == 'Cancelled', 'displacement waits for a terminal cancel'
        assert order.totalQuantity == float(min(quantity, held))


# ---------------------------------------------------------------------------
# 4. Another class: never cancelled; refused DEFERRED or clamped around it
# ---------------------------------------------------------------------------

@_SETTINGS
@given(book=_book(foreign=True), quantity=st.integers(min_value=1, max_value=20_000))
def test_exit_never_displaces_another_owners_working_reduction(book, quantity):
    held, reserved, exit_owner, foreign_owner = book
    foreign = _working(12, reserved, foreign_owner)
    ex, trader, placed = _executioner(held, [foreign])
    errors = _place_exit(ex, quantity, exit_owner)
    trader.client.ib.cancelOrder.assert_not_called()
    assert foreign.orderStatus.status == 'Submitted', "the other owner's protection is still working"
    available = held - reserved
    if available == 0:
        assert placed == [], 'nothing may be sent against fully reserved inventory'
        (error,) = errors
        assert 'DEFERRED' in str(error) and 'UNKNOWN' not in str(error), 'refused cleanly, before any side effect'
        assert '12' in str(error), 'the refusal names the competing order'
    else:
        assert errors == [], f'a partial foreign claim clamps, it does not refuse: {errors}'
        (order,) = placed
        assert order.totalQuantity == float(min(quantity, available))
