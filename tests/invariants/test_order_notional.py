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

from trader.trading.order_math import whole_shares_for_notional


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
