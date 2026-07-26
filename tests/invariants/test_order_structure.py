"""Invariants of record: nothing structurally malformed reaches the broker.

The decision (``order_structure.structural_rejection``) is deliberately narrow —
it rejects only orders that are DEFINITELY broken and never second-guesses a
price LEVEL. That narrowness is what makes it safe to apply to exits too, and it
means this spec has to pin BOTH directions:

  * ACCEPTANCE implies soundness — a sane action, a finite positive quantity,
    and a usable price for the order types that require one;
  * REJECTION is not over-eager — a MARKET order is never rejected over its
    price fields, because ib_async leaves them at UNSET_DOUBLE and a validator
    that read them would refuse every market order ever placed.

A one-directional spec here would be worse than none: a validator that refuses
everything satisfies "nothing malformed reaches the broker" perfectly, and would
silently stop the book from trading.

The last two properties are about WIRING, not the decision. This check existed
for years and was correct; it was reachable only from ``mmr buy`` / ``mmr sell``,
so every automated order — approve, the AutoExecutor, bracket legs, the
protective stop — went to IB unchecked. Properties that hold on a function
nobody calls are the failure mode being pinned here, so they are asserted at the
placement chokepoint rather than on the pure function.
"""

import asyncio
import math
from unittest.mock import AsyncMock, MagicMock

import reactivex as rx
from hypothesis import given, settings, strategies as st

from trader.trading.approved_order import ExitReason, mint_approved_order
from trader.trading.executioner import TradeExecutioner
from trader.trading.order_structure import (
    _NEEDS_LIMIT, _NEEDS_STOP, rejection_for_order, structural_rejection,
)

_SETTINGS = settings(max_examples=300, deadline=None)

# Enough to reach every branch: valid and invalid actions, priced and unpriced
# order types, and quantities/prices spanning the degenerate region (0, negative,
# NaN, inf) rather than only the healthy one — the mistake that left 70
# boundary mutants alive in position_sizing.
_ACTIONS = st.sampled_from(['BUY', 'SELL', 'buy', ' sell ', 'HODL', '', 'BUY '])
_ORDER_TYPES = st.sampled_from(['MKT', 'LMT', 'STP', 'STP LMT', 'TRAIL', 'lmt', '', 'MOC'])
_NUMBERS = st.one_of(
    st.floats(min_value=-1e6, max_value=1e6),
    st.floats(allow_nan=True, allow_infinity=True),
    st.sampled_from([0, 0.0, -0.0, 1, 1e-300, None, 'not-a-number']),
)


def _finite_positive(x) -> bool:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return False
    return math.isfinite(v) and v > 0


# ---------------------------------------------------------------------------
# Acceptance implies soundness
# ---------------------------------------------------------------------------

@_SETTINGS
@given(action=_ACTIONS, quantity=_NUMBERS, order_type=_ORDER_TYPES,
       limit_price=_NUMBERS, stop_price=_NUMBERS)
def test_accepting_an_order_implies_it_is_structurally_sound(
        action, quantity, order_type, limit_price, stop_price):
    """The whole contract of the module, as an independent oracle.

    Stated over the same input space the ``@deal`` postconditions cover, on
    purpose: the contracts are enforcement, this is the spec, and neither alone
    would survive the other being deleted.
    """
    reason = structural_rejection(action, quantity, order_type, limit_price, stop_price)
    if reason is not None:
        return
    assert str(action).strip().upper() in ('BUY', 'SELL')
    assert _finite_positive(quantity)
    otype = str(order_type or '').strip().upper()
    if otype in _NEEDS_LIMIT:
        assert _finite_positive(limit_price)
    if otype in _NEEDS_STOP:
        assert _finite_positive(stop_price)


@_SETTINGS
@given(action=_ACTIONS, quantity=_NUMBERS, order_type=_ORDER_TYPES,
       limit_price=_NUMBERS, stop_price=_NUMBERS)
def test_a_refusal_always_says_why(action, quantity, order_type, limit_price, stop_price):
    """An empty reason is worse than no reason: call sites branch on `is not
    None`, but the operator reads the string, and a blank one reads as 'fine'."""
    reason = structural_rejection(action, quantity, order_type, limit_price, stop_price)
    assert reason is None or (isinstance(reason, str) and reason.strip())


# ---------------------------------------------------------------------------
# Rejection is not over-eager
# ---------------------------------------------------------------------------

@_SETTINGS
@given(action=st.sampled_from(['BUY', 'SELL']),
       quantity=st.floats(min_value=1e-6, max_value=1e6, allow_nan=False,
                          allow_infinity=False),
       order_type=st.sampled_from(['MKT', 'TRAIL', 'MOC', '']),
       limit_price=_NUMBERS, stop_price=_NUMBERS)
def test_an_unpriced_order_type_is_never_rejected_over_its_price_fields(
        action, quantity, order_type, limit_price, stop_price):
    """ib_async leaves lmtPrice/auxPrice at UNSET_DOUBLE (sys.float_info.max) on
    a MarketOrder. A validator that consulted them regardless would reject every
    market order in the system — including every close."""
    assert structural_rejection(
        action, quantity, order_type, limit_price, stop_price) is None


@_SETTINGS
@given(action=st.sampled_from(['BUY', 'SELL', 'buy', 'Sell', ' BUY ', 'sell ']),
       quantity=st.floats(min_value=1.0, max_value=1e6, allow_nan=False,
                          allow_infinity=False))
def test_a_well_formed_market_order_is_always_accepted(action, quantity):
    """The floor: the shape every close in the system uses must pass."""
    assert structural_rejection(action, quantity, 'MKT', 0.0, 0.0) is None


@_SETTINGS
@given(otype=st.sampled_from(['LMT', 'lmt', ' Limit '.strip().upper(), 'STP', 'stp']),
       price=st.floats(min_value=0.01, max_value=1e6, allow_nan=False,
                       allow_infinity=False))
def test_a_priced_order_with_its_price_is_accepted(otype, price):
    """Guards the converse of the price checks: supplying the price is enough."""
    assert structural_rejection('BUY', 10, otype, price, price) is None


@given(action=st.sampled_from(['BUY', 'SELL']),
       order_type=st.sampled_from(['MKT', 'LMT', 'STP', 'STP LMT']))
@settings(max_examples=50, deadline=None)
def test_case_and_surrounding_whitespace_never_change_the_verdict(action, order_type):
    """IB fields arrive from YAML, RPC and hand-typed CLI args. A verdict that
    depended on casing would be a validator that works until someone types
    'buy'."""
    plain = structural_rejection(action, 10, order_type, 5.0, 5.0)
    noisy = structural_rejection(
        f'  {action.lower()} ', 10, f' {order_type.lower()}  ', 5.0, 5.0)
    assert plain == noisy


# ---------------------------------------------------------------------------
# Reading a partial order: fail closed, and do not crash
# ---------------------------------------------------------------------------

class _PartialOrder:
    """An order object missing one structural attribute."""

    def __init__(self, omit):
        full = {'action': 'BUY', 'totalQuantity': 10, 'orderType': 'STP LMT',
                'lmtPrice': 5.0, 'auxPrice': 4.5}
        for key, value in full.items():
            if key != omit:
                setattr(self, key, value)


@given(omit_and_assumed=st.sampled_from([
    ('action', repr('')),
    ('totalQuantity', repr(0)),
    ('lmtPrice', repr(0)),
    ('auxPrice', repr(0)),
]))
@settings(max_examples=20, deadline=None)
def test_an_order_missing_a_structural_field_is_refused(omit_and_assumed):
    """The adapter reads the chokepoint's inputs with ``getattr`` defaults, and
    those defaults are policy: they decide what happens to an object that is not
    the Order we assumed.

    Fail CLOSED — a field we could not read is refused, exactly as an unreadable
    NetLiquidation refuses an open in the risk gate. A permissive default (say
    ``lmtPrice`` defaulting to 1) would place a limit order at a price nobody
    chose, and it would look like a well-formed order all the way to IB.

    The assumed value is pinned as well as the refusal, for the reason
    ``test_the_DEFAULT_is_disarmed`` exists in the auto-execute spec: a default
    nothing asserts is a decision nobody is holding. Each is the falsy value
    that routes a missing field down the SAME refusal path as an explicitly bad
    one, and the refusal reports it so the operator can see what was assumed.
    """
    omit, assumed = omit_and_assumed
    reason = rejection_for_order(_PartialOrder(omit))
    assert reason is not None
    assert assumed in reason, (
        f'a missing {omit} was assumed to be something other than {assumed}')


def test_an_order_missing_its_type_degrades_to_unpriced_rather_than_refusing():
    """The one deliberate exception, pinned so it stays deliberate.

    An absent ``orderType`` means the price checks do not apply, not that the
    order is broken — that is the conservative direction (fewer checks, no false
    refusal), and several internal callers construct order-like objects without
    one. Quantity and action are still enforced.
    """
    assert rejection_for_order(_PartialOrder('orderType')) is None


def test_reading_a_partial_order_never_raises():
    """This runs at the chokepoint. An AttributeError here converts a refusal
    into a crash inside the placement path, which is a strictly worse failure:
    the caller gets a traceback instead of a reason, and nothing records why."""
    class _Empty:
        pass

    assert rejection_for_order(_Empty()) is not None


# ---------------------------------------------------------------------------
# Wiring: the chokepoint, not the function
# ---------------------------------------------------------------------------

class _Contract:
    symbol = 'AMD'
    exchange = 'NASDAQ'
    secType = 'STK'
    conId = 4391
    multiplier = None


class _Order:
    def __init__(self, action='SELL', quantity=10, order_type='MKT',
                 lmt=0.0, aux=0.0):
        self.action = action
        self.totalQuantity = quantity
        self.orderType = order_type
        self.lmtPrice = lmt
        self.auxPrice = aux
        self.account = 'DU1'
        self.orderId = 0
        self.orderRef = 'invariant'

    def __str__(self):
        return f'<Order {self.action} {self.totalQuantity} {self.orderType}>'


def _executioner():
    trader = MagicMock()
    trader.ib_account = 'DU1'
    trader.event_store = MagicMock()
    trader.client = MagicMock()
    trader.client.subscribe_place_order = AsyncMock(
        return_value=rx.from_iterable([MagicMock()]))
    trader.order_reduces_exposure = MagicMock(return_value=True)
    ex = TradeExecutioner()
    ex.connect(trader)
    return ex, trader


def _place(ex, order, *, is_exit):
    """Drive the chokepoint; return the list of errors it emitted."""
    approved = mint_approved_order(
        _Contract(), order, is_exit=is_exit,
        checks={} if is_exit else {'concentration': 'pass'},
        exit_reason=ExitReason.POSITION_CLASSIFIED if is_exit else None)

    async def _run():
        observable = await ex.subscribe_place_order_direct(approved)
        errors = []
        observable.subscribe(on_next=lambda _: None, on_error=errors.append)
        return errors

    return asyncio.run(_run())


@settings(max_examples=100, deadline=None)
@given(
    action=st.sampled_from(['BUY', 'SELL', 'HODL', '']),
    quantity=st.sampled_from([0, -1, -0.0, float('nan'), float('inf'), 10]),
    order_type=st.sampled_from(['MKT', 'LMT', 'STP', 'STP LMT']),
    price=st.sampled_from([0.0, -1.0, float('nan'), 5.0]),
    is_exit=st.booleans(),
)
def test_no_malformed_order_reaches_the_broker(
        action, quantity, order_type, price, is_exit):
    """The property that the old wiring did not have.

    Every path — the AutoExecutor's opens and closes, approve, bracket legs,
    the protective stop — spends its token here. If the pure decision refuses an
    order, IB must never see it, and being exit-class must not buy an exemption:
    a SELL of NaN shares closes nothing.
    """
    order = _Order(action, quantity, order_type, lmt=price, aux=price)
    malformed = rejection_for_order(order) is not None
    ex, trader = _executioner()
    errors = _place(ex, order, is_exit=is_exit)
    if malformed:
        assert trader.client.subscribe_place_order.call_count == 0, (
            'a structurally malformed order reached IB')
        assert errors, 'the chokepoint refused silently — the caller saw success'
    else:
        assert trader.client.subscribe_place_order.call_count == 1, (
            f'a well-formed order was refused: {errors}')


@settings(max_examples=50, deadline=None)
@given(order_type=st.sampled_from(['MKT', 'LMT', 'STP']),
       quantity=st.floats(min_value=1.0, max_value=1e5, allow_nan=False,
                          allow_infinity=False))
def test_a_well_formed_exit_still_places(order_type, quantity):
    """Pinned separately, because 'nothing malformed gets through' is trivially
    satisfied by a validator that refuses everything — and the failure that
    would cause (a close that cannot be placed) is the one this codebase treats
    as worse than any limit breach."""
    order = _Order('SELL', quantity, order_type, lmt=12.5, aux=12.5)
    ex, trader = _executioner()
    errors = _place(ex, order, is_exit=True)
    assert errors == [], f'a well-formed exit was refused: {errors}'
    assert trader.client.subscribe_place_order.call_count == 1
