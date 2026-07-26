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
