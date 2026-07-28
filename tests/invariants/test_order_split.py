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

# The tiny-magnitude branches are not decoration. The step-down that keeps the
# halves inside the request is only REACHABLE down there: for ordinary sizes,
# `qty - available` falls in the range where Sterbenz's lemma makes float
# subtraction exact, so nothing to correct. Mutation testing found this — two
# mutants of the step-down survived because no test ever entered the regime
# where it does any work.
_HELD = st.one_of(
    st.floats(min_value=-1e5, max_value=1e5, allow_nan=False, allow_infinity=False),
    st.floats(min_value=-1e-300, max_value=1e-300, allow_nan=False, allow_infinity=False),
    st.sampled_from([0.0, -0.0, 1.0, -1.0, 0.5, -0.5, 5e-324, -5e-324,
                     2.656539007259914e-180]))
_QTY = st.one_of(
    st.floats(min_value=0.0, max_value=1e5, allow_nan=False, allow_infinity=False),
    st.floats(min_value=0.0, max_value=1e-300, allow_nan=False, allow_infinity=False),
    st.sampled_from([0.0, 1.0, 0.5, 1e-9, 5e-324, 3.79631089884772e-178]))
_ACTION = st.sampled_from(['BUY', 'SELL', 'buy', ' sell ', 'HODL', ''])


class TestConservation:
    """SPEC REVISED 2026-07-27, and the revision is deliberate.

    This property was first written as exact equality: `reduce + open == qty`.
    That form is not satisfiable by ANY implementation. One half is pinned to
    `|held|` and the other is a float subtraction, so the two can miss the
    request by an ULP. The counterexample is pinned below.

    So this is a spec bug, not the usual "red invariant means the code is
    wrong". The rule about never weakening a property still applies, and the
    revision honours it by being STRICTLY STRONGER where it counts: the two
    directions of error are no longer treated alike.

      * summing to MORE than the request is now refused EXACTLY, with no
        tolerance at all. That direction invents exposure out of rounding,
        which is precisely the class of thing this module exists to stop.
        The old exact-equality form refused it too, but so did it refuse the
        harmless direction, which is why it could not hold.
      * summing to LESS is tolerated to one ULP of the request, and only
        that.

    Net effect: nothing that the old property would have caught is now
    allowed, and the unachievable half is gone.
    """

    @_SETTINGS
    @given(action=_ACTION, held=_HELD, qty=_QTY)
    def test_the_halves_never_exceed_the_request(self, action, held, qty):
        """EXACT. Shares may not be invented by arithmetic."""
        plan = split_order(action, held, qty)
        assert plan.reduce_qty + plan.open_qty <= qty if qty > 0 else True

    @_SETTINGS
    @given(action=_ACTION, held=_HELD, qty=_QTY)
    def test_the_halves_account_for_the_request(self, action, held, qty):
        """No meaningful share is lost. This is the property that catches an
        arithmetic slip: dropping a half, halving a value, swapping operands."""
        plan = split_order(action, held, qty)
        if qty > 0:
            assert qty - (plan.reduce_qty + plan.open_qty) <= math.ulp(qty)
        else:
            assert plan == SplitPlan(0.0, 0.0)

    def test_the_float_counterexample_that_revised_this_spec(self):
        """held = -1.2890519581908961, BUY 513.7350762451457.

        `1.2890519581908961 + (513.7350762451457 - 1.2890519581908961)`
        does not return 513.7350762451457. Found by the `deal` postcondition
        under Hypothesis while property-testing the proposal gate, after 500
        examples of THIS file had passed over the same float range — the wider
        strategy here samples the flip band too thinly to land on it.

        Pinned so the step-down that fixes it cannot be removed, and so the
        exact-equality form cannot come back.
        """
        plan = split_order('BUY', -1.2890519581908961, 513.7350762451457)
        total = plan.reduce_qty + plan.open_qty
        assert total <= 513.7350762451457, 'invented shares'
        assert 513.7350762451457 - total <= math.ulp(513.7350762451457)
        assert plan.reduce_qty == 1.2890519581908961, 'the reduction is still exact'

    def test_the_step_down_is_exercised_where_it_actually_does_work(self):
        """The step-down only has work to do at tiny magnitudes.

        For ordinary sizes, `qty - available` lands in the range where
        Sterbenz's lemma makes float subtraction EXACT, so the halves already
        sum to the request and the loop never runs. It runs only when
        `qty > 2 * available`, which at ordinary sizes forces a remainder far
        above 1.0 — and down here forces one far below it.

        Mutation testing found the gap: `while open_qty > 0.0` mutated to
        `> 1.0`, and `nextafter(open_qty, 0.0)` mutated to `nextafter(open_qty,
        1.0)` (stepping the wrong way for a sub-1.0 remainder), both survived
        the entire suite. Neither is equivalent; the tests simply never went
        where the code does its job. Same denormal corner that CrossHair found
        in `_floor_shares_for_notional`.
        """
        held, qty = -2.656539007259914e-180, 3.79631089884772e-178
        assert 0 < (qty - abs(held)) < 1.0, 'precondition: a sub-1.0 remainder'
        assert abs(held) + (qty - abs(held)) > qty, 'precondition: the naive sum overshoots'

        plan = split_order('BUY', held, qty)
        assert plan.reduce_qty + plan.open_qty <= qty, (
            'the step-down did not run: the halves sum to MORE than the request, '
            'inventing exposure out of rounding')
        assert plan.reduce_qty == abs(held)


class TestTheConservationOracleItself:
    """`_conserves` is the postcondition, so a weakened version fails silently:
    the contract simply stops catching things and every test still passes.
    Mutation testing surfaced that (`and` -> `or` survived), which is the
    reason to test the oracle directly rather than only through the function
    it guards."""

    def test_it_rejects_a_plan_that_invents_shares(self):
        from trader.trading.order_split import _conserves
        assert not _conserves(SplitPlan(3.0, 3.0), 5.0)

    def test_it_rejects_a_plan_that_loses_shares(self):
        from trader.trading.order_split import _conserves
        assert not _conserves(SplitPlan(1.0, 1.0), 5.0)

    def test_it_accepts_an_exact_split(self):
        from trader.trading.order_split import _conserves
        assert _conserves(SplitPlan(3.0, 2.0), 5.0)

    def test_it_tolerates_exactly_one_ulp_short_and_no_more(self):
        from trader.trading.order_split import _conserves
        qty = 513.7350762451457
        assert _conserves(SplitPlan(qty - math.ulp(qty), 0.0), qty)
        assert not _conserves(SplitPlan(qty - 3 * math.ulp(qty), 0.0), qty)

    def test_a_non_positive_request_permits_only_an_empty_plan(self):
        from trader.trading.order_split import _conserves
        assert _conserves(SplitPlan(0.0, 0.0), 0.0)
        assert not _conserves(SplitPlan(1.0, 0.0), 0.0)
        assert not _conserves(SplitPlan(0.0, 1.0), 0.0)

    def test_a_zero_request_is_checked_per_half_not_by_their_sum(self):
        """Halves that CANCEL are not an empty plan.

        A sum-based check reads `(-1, +1)` against a request of 0 as
        conserving, because the halves total zero. Per-half is what the
        oracle promises and what the zero branch exists to enforce.

        Written to kill a surviving mutant that moved the zero branch's
        boundary (`qty <= 0` to `qty < 0`), which drops a zero request into
        the sum-based path. Nothing else in the suite could tell the two
        apart.
        """
        from trader.trading.order_split import _conserves
        assert not _conserves(SplitPlan(-1.0, 1.0), 0.0)

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
