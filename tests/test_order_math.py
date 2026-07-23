"""Tests for trader.trading.order_math.whole_shares_for_notional.

The invariant under test: result >= 1 and result * price * multiplier <= amount,
or ValueError. Never a bump-to-1 — that's the bug class this function kills.
"""

import math
import os
import sys

import pytest
from hypothesis import given, strategies as st

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from trader.trading.order_math import whole_shares_for_notional


class TestExactFloors:
    def test_exact_multiple(self):
        assert whole_shares_for_notional(1000.0, 10.0) == 100

    def test_floors_partial_share(self):
        assert whole_shares_for_notional(1050.0, 100.0) == 10

    def test_floors_never_rounds_up(self):
        # round() would give 36 (36 * 140 = 5040 > 5000); floor gives 35
        assert whole_shares_for_notional(5000.0, 140.0) == 35

    def test_just_below_two_shares(self):
        assert whole_shares_for_notional(199.99, 100.0) == 1

    def test_exactly_one_share(self):
        assert whole_shares_for_notional(100.0, 100.0) == 1

    def test_multiplier(self):
        # options-style contract: 100x multiplier
        assert whole_shares_for_notional(10_000.0, 3.0, multiplier=100.0) == 33

    def test_multiplier_refusal(self):
        with pytest.raises(ValueError, match='fractional shares'):
            whole_shares_for_notional(250.0, 3.0, multiplier=100.0)

    def test_fp_boundary_never_exceeds_notional(self):
        # 8.28 / 2.76 == 3.0000000000000004 in floats, but 3 * 2.76 > 8.28
        shares = whole_shares_for_notional(8.28, 2.76)
        assert shares * 2.76 <= 8.28
        assert shares >= 1


class TestRefusal:
    def test_brka_class_regression(self):
        # amount=$5000 on a $700k stock must REFUSE, never return 1
        with pytest.raises(ValueError) as excinfo:
            whole_shares_for_notional(5000.0, 700_000.0)
        msg = str(excinfo.value)
        assert '5000' in msg
        assert '700000' in msg
        assert 'fractional shares' in msg

    def test_auto_sized_amount_below_price(self):
        # the live bug: ~$340 auto-sized amount on a >$510 stock became 1 share
        with pytest.raises(ValueError, match='fractional shares'):
            whole_shares_for_notional(340.0, 510.0)

    def test_refusal_names_amount_and_price(self):
        with pytest.raises(ValueError, match=r'340\.0.*510\.0'):
            whole_shares_for_notional(340.0, 510.0)


class TestBadInputs:
    @pytest.mark.parametrize('amount', [0.0, -1.0, -5000.0, float('nan'), float('inf'), float('-inf')])
    def test_bad_amount(self, amount):
        with pytest.raises(ValueError, match='amount'):
            whole_shares_for_notional(amount, 100.0)

    @pytest.mark.parametrize('price', [0.0, -0.01, float('nan'), float('inf')])
    def test_bad_price(self, price):
        with pytest.raises(ValueError, match='price'):
            whole_shares_for_notional(1000.0, price)

    @pytest.mark.parametrize('multiplier', [0.0, -100.0, float('nan'), float('inf')])
    def test_bad_multiplier(self, multiplier):
        with pytest.raises(ValueError, match='multiplier'):
            whole_shares_for_notional(1000.0, 10.0, multiplier=multiplier)

    def test_non_numeric_price(self):
        with pytest.raises(ValueError, match='price'):
            whole_shares_for_notional(1000.0, '10')

    def test_non_numeric_amount(self):
        with pytest.raises(ValueError, match='amount'):
            whole_shares_for_notional(None, 10.0)

    def test_error_names_offending_value(self):
        with pytest.raises(ValueError, match=r'-1\.0'):
            whole_shares_for_notional(-1.0, 100.0)


class TestProperties:
    @given(
        amount=st.floats(min_value=1.0, max_value=1e7, allow_nan=False, allow_infinity=False),
        price=st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
    )
    def test_result_within_notional_or_refuses(self, amount, price):
        try:
            shares = whole_shares_for_notional(amount, price)
        except ValueError:
            # refusal happens exactly when one share is unaffordable
            assert price > amount
            return
        assert isinstance(shares, int)
        assert shares >= 1
        assert shares * price <= amount

    @given(
        amount=st.floats(min_value=1.0, max_value=1e7, allow_nan=False, allow_infinity=False),
        price=st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
        multiplier=st.floats(min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
    )
    def test_multiplier_property(self, amount, price, multiplier):
        try:
            shares = whole_shares_for_notional(amount, price, multiplier)
        except ValueError:
            assert price * multiplier > amount
            return
        assert shares >= 1
        assert shares * price * multiplier <= amount

    @given(
        amount=st.floats(min_value=1.0, max_value=1e7, allow_nan=False, allow_infinity=False),
        price=st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
    )
    def test_one_more_share_would_exceed(self, amount, price):
        """The floor is tight: buying one extra share would cost more than amount."""
        try:
            shares = whole_shares_for_notional(amount, price)
        except ValueError:
            return
        assert (shares + 1) * price > amount or math.isclose((shares + 1) * price, amount)
