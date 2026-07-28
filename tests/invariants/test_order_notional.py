"""Invariant of record: the amount→shares conversion never overspends.

``whole_shares_for_notional`` is the single conversion every dollar-notional
order path uses (sdk.approve, executioner.helper_create_order). The invariant:
for any sized amount, price, and contract multiplier, the returned quantity's
notional never exceeds the amount — and when the amount doesn't cover one
whole share the function REFUSES (ValueError), never returns 0 and never bumps
to 1 share. The bump-to-1 behaviour is what once turned a ~$340 auto-sized
amount on a >$510 stock into a full share at inflated notional.

Deeper example-based cases live in tests/test_order_math.py; this file is the
human-owned property plus the pinned counterexamples.
"""

import math

import pytest
from hypothesis import given, settings, strategies as st

from trader.trading.order_math import reducible_quantity, whole_shares_for_notional


@settings(max_examples=500, deadline=None)
@given(
    amount=st.floats(min_value=1.0, max_value=1e7, allow_nan=False, allow_infinity=False),
    price=st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
    multiplier=st.sampled_from([1.0, 100.0]),
)
def test_notional_never_exceeds_amount_and_refusal_over_zero_or_bump(amount, price, multiplier):
    """THE flagship property.

    Either the function returns a whole-share quantity whose notional fits
    inside ``amount``, or it raises ValueError. It never returns 0 (a silent
    no-trade masquerading as a quantity) and never returns a quantity that
    overspends (the bump-to-1 bug). Refusal only happens when one share is
    genuinely unaffordable.
    """
    try:
        shares = whole_shares_for_notional(amount, price, multiplier)
    except ValueError:
        # Refusal is only legitimate when the amount cannot cover a single
        # share (up to float wiggle in the division/comparison).
        assert amount < price * multiplier * (1 + 1e-9), (
            f'refused although one share is affordable: '
            f'amount={amount} price={price} multiplier={multiplier}'
        )
        return

    assert isinstance(shares, int)
    assert shares >= 1, 'a returned quantity of 0 must be a refusal, not a result'
    assert shares * price * multiplier <= amount, (
        f'overspend: {shares} x {price} x {multiplier} '
        f'= {shares * price * multiplier} > amount {amount}'
    )


@settings(max_examples=200, deadline=None)
@given(
    amount=st.floats(min_value=1.0, max_value=1e7, allow_nan=False, allow_infinity=False),
    price=st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
    multiplier=st.sampled_from([1.0, 100.0]),
)
def test_never_undersizes_by_more_than_one_share(amount, price, multiplier):
    """Maximality (tolerant): when a quantity is returned, one more share
    would overspend — floor-and-refuse must not also chronically undersize.
    Tolerant of one float-rounding ulp on the comparison."""
    try:
        shares = whole_shares_for_notional(amount, price, multiplier)
    except ValueError:
        return
    assert (shares + 1) * price * multiplier > amount * (1 - 1e-9)


class TestPinnedCounterexamples:
    """Named regressions per the invariants policy: every counterexample
    becomes a deterministic pinned case before the fix lands."""

    def test_brk_a_refusal(self):
        """BRK.A: $5,000 sized amount at a $700,000 share price must refuse —
        the old bump-to-1 would have bought a $700k share on a $5k budget."""
        with pytest.raises(ValueError, match='5000'):
            whole_shares_for_notional(5000.0, 700000.0)

    def test_one_cent_short_of_a_share_refuses(self):
        with pytest.raises(ValueError):
            whole_shares_for_notional(99.99, 100.0)

    def test_exactly_one_share_affordable(self):
        assert whole_shares_for_notional(100.0, 100.0) == 1

    def test_float_boundary_rounds_down_not_up(self):
        """8.28 / 2.76 floats to 3.0000000000000004 — the conversion must
        step down until the postcondition holds, never ride the float up."""
        shares = whole_shares_for_notional(8.28, 2.76)
        assert shares * 2.76 <= 8.28

    def test_multiplier_refusal_not_bump(self):
        """$3,000 on a $35 option with 100x multiplier: one contract costs
        $3,500 — refuse, don't bump."""
        with pytest.raises(ValueError):
            whole_shares_for_notional(3000.0, 35.0, multiplier=100.0)

    def test_denormal_product_underflow_refuses_not_crashes(self):
        """CrossHair (symbolic execution over the deal contracts) found that
        ``price * multiplier`` underflows to 0.0 for denormal inputs, so the
        pre-contract code did ``amount / 0.0`` → ZeroDivisionError — an
        undeclared crash on the single conversion every order path uses. The
        conversion must REFUSE loudly (ValueError), never crash. These are the
        exact symbolic inputs CrossHair reported."""
        with pytest.raises(ValueError):
            whole_shares_for_notional(2.0, 3.0765742648370966e-154, 3.034084836703205e-308)


# ---------------------------------------------------------------------------
# The never-oversell clamp: how much of an attributed position may be reduced
# ---------------------------------------------------------------------------

@settings(max_examples=500, deadline=None)
@given(
    attributed=st.one_of(
        st.floats(min_value=-1e4, max_value=1e6),
        st.sampled_from([0, 0.0, -0.0, 0.5, float('nan'), float('inf'), None, 'x']),
    ),
    broker=st.one_of(
        st.floats(min_value=-1e4, max_value=1e6),
        st.sampled_from([0, 0.0, -0.0, 0.5, float('nan'), float('inf'), None, 'x']),
    ),
)
def test_a_reduction_never_exceeds_the_position_or_the_attribution(attributed, broker):
    """Both bounds, over the whole input space including the unreadable parts.

    Exceeding the BROKER quantity opens a short out of a closing order —
    and reducing orders are exit-class, hence exempt from the trading filter,
    the leverage check, the risk gate and the approver tier, so nothing
    downstream would argue. Exceeding the ATTRIBUTED quantity sells shares this
    strategy has no claim on, i.e. a manual position in the same instrument.
    """
    result = reducible_quantity(attributed, broker)
    assert result >= 0.0
    if result > 0.0:
        assert result <= float(broker)
        assert result <= float(attributed)


@settings(max_examples=200, deadline=None)
@given(
    attributed=st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
    broker=st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
)
def test_a_readable_position_is_always_reducible(attributed, broker):
    """The converse. "Never oversell" is satisfied by a function that always
    returns zero, and a close that silently refuses to close is how a position
    outlives the strategy that opened it."""
    assert reducible_quantity(attributed, broker) == min(attributed, broker)


class TestPinnedOversellCounterexamples:
    def test_a_nan_broker_quantity_reduces_nothing(self):
        """Pinned counterexample (2026-07-26).

        Both call sites wrote ``min(attributed, broker)`` and then guarded with
        ``if qty <= 0``. Neither holds for NaN: ``min(140.0, nan)`` is 140.0
        (the comparison is False, so min keeps the first argument) and
        ``nan <= 0`` is False. A NaN broker read therefore passed the clamp AND
        the guard, and would have sold the full attributed size against a
        position of unknown size.
        """
        assert reducible_quantity(140.0, float('nan')) == 0.0
        assert reducible_quantity(float('nan'), 140.0) == 0.0

    def test_an_infinite_broker_quantity_reduces_nothing(self):
        """``min(140.0, inf)`` is 140.0, which happens to be safe — but an
        infinite position is an unreadable one, and acting on it means acting
        on a number the broker did not give us."""
        assert reducible_quantity(140.0, float('inf')) == 0.0

    def test_a_non_numeric_broker_quantity_reduces_nothing(self):
        """``min(140.0, '')`` raises TypeError inside the placement path."""
        assert reducible_quantity(140.0, None) == 0.0
        assert reducible_quantity(140.0, 'unknown') == 0.0


class TestAValuationNeverSilentlyReadsZero:
    """SPEC: an order that cannot be valued says so. It never reports $0.

    This is the property that makes a cumulative notional limit possible at
    all, and its absence is why one could not be built on the existing audit
    trail. `ORDER_SUBMITTED` recorded `price=order.lmtPrice`, and for a MARKET
    or STOP order ib_async leaves that field at its unset sentinel,
    `sys.float_info.max`. Confirmed in the live event store: market submissions
    are recorded at 1.7976931348623157e+308.

    Nothing looked broken, because the only consumer was the order-RATE limit,
    which counts rows and never reads value.

    The sentinel is the dangerous part, and not for the reason first assumed.
    It is FINITE and POSITIVE, so the natural guard — `isfinite(price) and
    price > 0` — passes it, and at a quantity of 1.0 the product is still
    finite. A first cut of this valuation therefore reported a $1.8e308
    notional as VALID. One of those in a cumulative total exceeds every
    conceivable cap forever, so the cap would refuse all opens: not the
    fail-open originally predicted, but a system-bricking fail-closed.

    So the invariant is about the failure mode, not the arithmetic. Unevaluable
    must be DISTINGUISHABLE from zero, and a sentinel must never be mistaken
    for a price.
    """

    def test_a_market_order_with_no_price_anywhere_is_not_evaluable(self):
        from trader.trading.order_math import order_notional
        # A MARKET order carries no limit and no stop; with no cached price
        # either, there is nothing to value it with.
        notional, evaluable = order_notional((0, 0), 100.0)
        assert evaluable is False
        assert notional == 0.0

    def test_the_unset_sentinel_is_never_mistaken_for_a_price(self):
        """THE regression. sys.float_info.max is what ib_async puts in an unset
        lmtPrice, and it is finite and positive, so it defeats the obvious
        guard. Quantity 1.0 is the case that matters: the product stays finite,
        so nothing downstream catches it either."""
        import sys as _sys
        from trader.trading.order_math import order_notional
        unset = _sys.float_info.max
        for qty in (1.0, 0.5, 2.0, 100.0):
            assert order_notional((unset,), qty) == (0.0, False), qty
        # A STOP order: lmtPrice unset, auxPrice real. The stop price is the
        # valuation, and the sentinel ahead of it must not win.
        assert order_notional((unset, 90.0), 3.0) == (270.0, True)

    def test_zero_notional_is_never_reported_as_evaluable(self):
        """The precise confusion to prevent: a real valuation and a failed one
        must not both look like 0.0-with-confidence."""
        from trader.trading.order_math import order_notional
        for candidates in ((), (0,), (None,), (float('nan'),), (-5.0,),
                           (float('inf'),)):
            notional, evaluable = order_notional(candidates, 10.0)
            assert (notional, evaluable) == (0.0, False), candidates

    @settings(max_examples=300, deadline=None)
    @given(
        price=st.floats(min_value=1e-6, max_value=1e6,
                        allow_nan=False, allow_infinity=False),
        qty=st.floats(min_value=1e-6, max_value=1e6,
                      allow_nan=False, allow_infinity=False),
        multiplier=st.sampled_from([1.0, 100.0, 50.0]),
    )
    def test_a_usable_price_always_yields_an_evaluable_positive_notional(
            self, price, qty, multiplier):
        from trader.trading.order_math import order_notional
        notional, evaluable = order_notional((price,), qty, multiplier)
        if evaluable:
            assert notional > 0.0
        else:
            # The only permitted failure with a usable price is an overflow to
            # infinity, which is not a valuation either.
            assert notional == 0.0
            assert not math.isfinite(price * qty * multiplier)

    def test_the_first_usable_candidate_wins_in_the_order_given(self):
        """Callers own the price POLICY by ordering the tuple. If this function
        reordered or picked a 'best' price, the approver tier's anti-forgery
        rule (never value below the live market) would silently change."""
        from trader.trading.order_math import order_notional
        assert order_notional((0, None, 7.0, 9999.0), 1.0) == (7.0, True)

    def test_direction_does_not_change_the_size(self):
        from trader.trading.order_math import order_notional
        assert order_notional((10.0,), 5.0) == order_notional((10.0,), -5.0)

    def test_a_zero_quantity_is_not_a_valuation(self):
        """0 shares at a real price multiplies out to $0, which would be
        reported as an EVALUABLE zero — indistinguishable from a real $0
        valuation and, in a cumulative total, free."""
        from trader.trading.order_math import order_notional
        assert order_notional((19.5,), 0.0) == (0.0, False)
        assert order_notional((19.5,), -0.0) == (0.0, False)

    def test_a_degenerate_multiplier_is_not_a_valuation(self):
        """Same trap on the other factor: a multiplier of 0 makes every order
        worth $0, and a cumulative cap would never fill."""
        from trader.trading.order_math import order_notional
        for mult in (0.0, -1.0, float('nan'), float('inf')):
            assert order_notional((19.5,), 10.0, mult) == (0.0, False), mult

    def test_non_numeric_inputs_are_not_evaluable(self):
        from trader.trading.order_math import order_notional
        assert order_notional((19.5,), 'ten') == (0.0, False)
        assert order_notional((19.5,), 10.0, 'x') == (0.0, False)
        assert order_notional((19.5,), None) == (0.0, False)

    def test_the_multiplier_multiplies(self):
        """An options contract on 100 shares is worth 100x, not 1/100th. Every
        other test here uses multiplier 1.0, where multiply and divide agree —
        so this is the only test that can tell them apart."""
        from trader.trading.order_math import order_notional
        assert order_notional((2.5,), 4.0, 100.0) == (1000.0, True)
        assert order_notional((2.5,), 4.0, 50.0) == (500.0, True)

    def test_an_overflowing_valuation_is_not_a_valuation(self):
        """A product that leaves the float range is not a number we can compare
        against a cap, so it must be reported as unevaluable rather than as inf
        (which exceeds every cap) or as a wrapped value."""
        from trader.trading.order_math import order_notional
        assert order_notional((1e200,), 1e200) == (0.0, False)
        assert order_notional((1e300,), 1e10, 1e10) == (0.0, False)
