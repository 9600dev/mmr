"""Invariants of record: a flip decomposes without losing or inventing shares.

The flip residual was the last documented hole in the exit-class rule: an
oversized reduction is labelled an exit, so its net-new opening remainder
passed every gate. Confirmed live 2026-07-27 (SELL 5 against 3 held, accepted
with no refusal from anything).

Splitting closes it without breaking the rule that created it. Both directions
are pinned here because each alone is satisfiable by a broken implementation:

  * a splitter that puts everything in `open_qty` closes the hole and makes
    exits refusable, which is the worse failure;
  * a splitter that puts everything in `reduce_qty` preserves exits and leaves
    the hole exactly as it was.

Conservation is the property that catches both, and any arithmetic slip:
the halves must sum to the request, always.
"""

import math

from hypothesis import given, settings, strategies as st

from trader.trading.exit_class import reduces_exposure
from trader.trading.order_split import SplitPlan, split_order

_SETTINGS = settings(max_examples=500, deadline=None)

_HELD = st.one_of(
    st.floats(min_value=-1e5, max_value=1e5, allow_nan=False, allow_infinity=False),
    st.sampled_from([0.0, -0.0, 1.0, -1.0, 0.5, -0.5]))
_QTY = st.one_of(
    st.floats(min_value=0.0, max_value=1e5, allow_nan=False, allow_infinity=False),
    st.sampled_from([0.0, 1.0, 0.5, 1e-9]))
_ACTION = st.sampled_from(['BUY', 'SELL', 'buy', ' sell ', 'HODL', ''])


class TestConservation:
    @_SETTINGS
    @given(action=_ACTION, held=_HELD, qty=_QTY)
    def test_the_halves_sum_to_the_request(self, action, held, qty):
        """No share is lost or invented. This is the property that catches an
        arithmetic slip in either direction."""
        plan = split_order(action, held, qty)
        if qty > 0:
            assert plan.reduce_qty + plan.open_qty == qty
        else:
            assert plan == SplitPlan(0.0, 0.0)

    @_SETTINGS
    @given(action=_ACTION, held=_HELD, qty=_QTY)
    def test_neither_half_is_negative(self, action, held, qty):
        plan = split_order(action, held, qty)
        assert plan.reduce_qty >= 0.0 and plan.open_qty >= 0.0

    @_SETTINGS
    @given(action=_ACTION, held=_HELD, qty=_QTY)
    def test_the_reduction_never_exceeds_the_position(self, action, held, qty):
        """The reduction half must be a real reduction. A larger value would
        be an oversell wearing the exit label, which is the original bug."""
        plan = split_order(action, held, qty)
        assert plan.reduce_qty <= abs(held)


class TestTheHoleIsClosed:
    def test_the_live_case_splits(self):
        """The exact 2026-07-27 probe: SELL 5 against 3 held."""
        plan = split_order('SELL', 3.0, 5.0)
        assert plan == SplitPlan(3.0, 2.0)
        assert plan.is_flip is True

    def test_a_short_flip_splits_the_same_way(self):
        plan = split_order('BUY', -3.0, 5.0)
        assert plan == SplitPlan(3.0, 2.0)

    @_SETTINGS
    @given(held=st.floats(min_value=0.01, max_value=1e4, allow_nan=False,
                          allow_infinity=False),
           excess=st.floats(min_value=0.01, max_value=1e4, allow_nan=False,
                            allow_infinity=False))
    def test_every_oversized_reduction_yields_a_gated_remainder(self, held, excess):
        """The hole, stated as a property: whenever a reduction exceeds the
        position, the excess must land in open_qty where the gates can see it,
        and never in reduce_qty where they cannot."""
        plan = split_order('SELL', held, held + excess)
        assert plan.open_qty == (held + excess) - held
        assert plan.reduce_qty == held


class TestTheRuleThatCreatedTheHoleIsPreserved:
    @_SETTINGS
    @given(held=st.floats(min_value=0.01, max_value=1e4, allow_nan=False,
                          allow_infinity=False),
           frac=st.floats(min_value=0.01, max_value=1.0, allow_nan=False,
                          allow_infinity=False))
    def test_a_reduction_that_fits_is_never_split(self, held, frac):
        """An ordinary close must stay wholly exit-class. If any part of it
        leaked into open_qty it would become gate-refusable, which is the
        failure this whole design exists to prevent."""
        qty = held * frac
        plan = split_order('SELL', held, qty)
        assert plan.open_qty == 0.0
        assert plan.reduce_qty == qty

    def test_an_exact_close_is_not_a_flip(self):
        plan = split_order('SELL', 3.0, 3.0)
        assert plan == SplitPlan(3.0, 0.0)
        assert plan.is_flip is False


class TestNonReductionsAreUnchanged:
    @_SETTINGS
    @given(qty=st.floats(min_value=0.01, max_value=1e4, allow_nan=False,
                         allow_infinity=False))
    def test_opening_from_flat_is_all_opening(self, qty):
        assert split_order('SELL', 0.0, qty) == SplitPlan(0.0, qty)
        assert split_order('BUY', 0.0, qty) == SplitPlan(0.0, qty)

    @_SETTINGS
    @given(held=st.floats(min_value=0.01, max_value=1e4, allow_nan=False,
                          allow_infinity=False),
           qty=st.floats(min_value=0.01, max_value=1e4, allow_nan=False,
                         allow_infinity=False))
    def test_adding_to_a_long_is_all_opening(self, held, qty):
        assert split_order('BUY', held, qty) == SplitPlan(0.0, qty)

    @_SETTINGS
    @given(held=_HELD, qty=st.floats(min_value=0.01, max_value=1e3,
                                     allow_nan=False, allow_infinity=False))
    def test_an_unknown_action_is_all_opening(self, held, qty):
        """Fail closed: an action we cannot classify is treated as new
        exposure, so it is gated rather than waved through as an exit."""
        assert split_order('HODL', held, qty) == SplitPlan(0.0, qty)


class TestDegenerateInputs:
    @given(qty=st.sampled_from([0.0, -1.0, float('nan'), float('inf'), None, 'x']))
    @settings(max_examples=20, deadline=None)
    def test_a_non_order_splits_into_nothing(self, qty):
        """Structural refusal owns these upstream; this only has to be total."""
        assert split_order('SELL', 3.0, qty) == SplitPlan(0.0, 0.0)

    @given(held=st.sampled_from([float('nan'), float('inf'), None, 'x']))
    @settings(max_examples=20, deadline=None)
    def test_an_unreadable_position_yields_all_opening(self, held):
        """A position we cannot read is not evidence of anything to reduce, so
        the shares become gated new exposure. Fail closed."""
        assert split_order('SELL', held, 5.0) == SplitPlan(0.0, 5.0)


class TestAgreementWithTheExitClassifier:
    @_SETTINGS
    @given(action=_ACTION, held=_HELD, qty=st.floats(min_value=0.01, max_value=1e4,
                                                     allow_nan=False, allow_infinity=False))
    def test_a_reduction_half_exists_exactly_when_the_order_reduces(self, action, held, qty):
        """The split and the exit classifier must never disagree about whether
        an order reduces anything. Two sources of truth on that question is how
        the original hole would grow back."""
        plan = split_order(action, held, qty)
        assert (plan.reduce_qty > 0) == reduces_exposure(action, held, qty)
