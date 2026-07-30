"""SPEC: weight-to-share translation must never invent exposure.

The panel kernel converts target weights into share counts and orders. Every
failure mode here is silent — it changes leverage or liquidates a position
without raising anything, and the backtest still produces a plausible equity
curve. So the properties are about what must NOT happen:

  * a weight must never be exceeded. Rounding to nearest breaches it in
    whichever direction the fraction falls, and over 500 names that compounds
    into leverage nobody chose.
  * an unpriceable instrument must not be confused with a zero target. Those
    are different instructions, and collapsing them liquidates a real position
    because one price was missing.
  * scaling must only ever reduce. A book that is 40% invested is a choice;
    filling it to 100% invents conviction the strategy never expressed.
"""

from __future__ import annotations

import math

import pytest

from hypothesis import assume, given, settings, strategies as st

from trader.simulation.panel import (
    gross_exposure, normalise_weights, rebalance_orders, target_positions)


_SETTINGS = settings(max_examples=400, deadline=None)

_W = st.floats(min_value=-2.0, max_value=2.0,
               allow_nan=False, allow_infinity=False)
_PX = st.floats(min_value=0.01, max_value=1e5,
                allow_nan=False, allow_infinity=False)
_EQ = st.floats(min_value=1.0, max_value=1e9,
                allow_nan=False, allow_infinity=False)


class TestAWeightIsACeilingNotATarget:

    @_SETTINGS
    @given(w=_W, eq=_EQ, px=_PX)
    def test_notional_never_exceeds_the_requested_weight(self, w, eq, px):
        """Truncation toward zero, so |shares * price| <= |weight| * equity.
        Rounding to nearest would breach this half the time."""
        pos = target_positions({1: w}, eq, {1: px})
        assume(1 in pos)
        notional = abs(pos[1] * px)
        assert notional <= abs(w) * eq + 1e-6, (
            f'weight {w} on {eq} at {px} produced {notional} of exposure, '
            f'over the {abs(w) * eq} requested')

    @_SETTINGS
    @given(w=_W, eq=_EQ, px=_PX)
    def test_the_sign_of_the_weight_is_preserved(self, w, eq, px):
        pos = target_positions({1: w}, eq, {1: px})
        assume(1 in pos and pos[1] != 0)
        assert (pos[1] > 0) == (w > 0)

    @_SETTINGS
    @given(w=_W, eq=_EQ, px=_PX)
    def test_whole_shares_unless_fractional_is_asked_for(self, w, eq, px):
        pos = target_positions({1: w}, eq, {1: px})
        assume(1 in pos)
        assert pos[1] == math.trunc(pos[1])


class TestMissingIsNotZero:
    """The distinction that stops one bad price liquidating a position."""

    def test_an_unpriceable_conid_is_omitted_not_zeroed(self):
        pos = target_positions({1: 0.5, 2: 0.5}, 100_000.0, {1: 10.0})
        assert 1 in pos
        assert 2 not in pos, ('a conid with no price must carry NO '
                              'instruction, not an instruction to hold zero')

    @pytest.mark.parametrize('bad', [0.0, -5.0, float('nan'), float('inf')])
    def test_a_nonsense_price_is_omitted(self, bad):
        assert target_positions({1: 0.5}, 100_000.0, {1: bad}) == {}

    def test_an_explicit_zero_weight_IS_an_instruction(self):
        """Zero means 'hold nothing' and must survive, or a strategy could
        never express an exit."""
        assert target_positions({1: 0.0}, 100_000.0, {1: 10.0}) == {1: 0.0}

    def test_a_position_the_strategy_did_not_mention_is_left_alone(self):
        """A rebalancer that liquidates everything absent from the target turns
        one unpriceable instrument into a portfolio-wide sell."""
        orders = rebalance_orders({1: 100.0, 2: 50.0}, {1: 120.0})
        assert orders == {1: 20.0}
        assert 2 not in orders

    def test_zero_equity_produces_no_positions(self):
        assert target_positions({1: 0.5}, 0.0, {1: 10.0}) == {}


class TestRebalancing:

    @_SETTINGS
    @given(cur=st.floats(min_value=-1e6, max_value=1e6,
                         allow_nan=False, allow_infinity=False),
           tgt=st.floats(min_value=-1e6, max_value=1e6,
                         allow_nan=False, allow_infinity=False))
    def test_applying_the_delta_reaches_the_target(self, cur, tgt):
        orders = rebalance_orders({1: cur}, {1: tgt})
        reached = cur + orders.get(1, 0.0)
        assert reached == pytest.approx(tgt, abs=1e-6) or 1 not in orders

    def test_dust_is_dropped_not_rounded_up(self):
        """A churn floor that rounds up would itself generate turnover."""
        assert rebalance_orders({1: 100.0}, {1: 100.4}, min_shares=1.0) == {}

    def test_an_unchanged_position_produces_no_order(self):
        assert rebalance_orders({1: 100.0}, {1: 100.0}) == {}


class TestExposureScalingOnlyEverReduces:

    @_SETTINGS
    @given(ws=st.lists(_W, min_size=1, max_size=30),
           cap=st.floats(min_value=0.1, max_value=3.0,
                         allow_nan=False, allow_infinity=False))
    def test_gross_never_exceeds_the_cap(self, ws, cap):
        weights = {i: w for i, w in enumerate(ws)}
        g = gross_exposure(normalise_weights(weights, max_gross=cap))
        assert g is not None
        assert g <= cap + 1e-9

    @_SETTINGS
    @given(ws=st.lists(_W, min_size=1, max_size=30))
    def test_an_underinvested_book_is_left_underinvested(self, ws):
        """40% invested is a choice. Scaling it up to fill the budget would
        invent conviction the strategy never expressed."""
        weights = {i: w for i, w in enumerate(ws)}
        before = gross_exposure(weights)
        assume(before is not None and before < 1.0)
        after = gross_exposure(normalise_weights(weights, max_gross=1.0))
        assert after == pytest.approx(before)

    @_SETTINGS
    @given(ws=st.lists(
        st.floats(min_value=-2.0, max_value=2.0, allow_nan=False,
                  allow_infinity=False).filter(lambda x: abs(x) >= 1e-6),
        min_size=2, max_size=20))
    def test_scaling_preserves_relative_sizes(self, ws):
        """Scaling must not change WHICH names the book prefers, only how much
        of the book it is.

        Stated over weights of at least 1e-6 (one ten-thousandth of a percent
        of the book). Below that, scaling underflows toward the denormal floor
        and the ratio genuinely cannot be preserved in binary floating point —
        see `test_denormal_weights_collapse_rather_than_scale`. That is a
        property of float, not a defect here, and asserting it over the full
        range would be asserting something no implementation can satisfy."""
        weights = {i: w for i, w in enumerate(ws) if w != 0}
        assume(len(weights) >= 2)
        g = gross_exposure(weights)
        assume(g is not None and g > 1.0)
        scaled = normalise_weights(weights, max_gross=1.0)
        keys = sorted(weights)
        a, b = keys[0], keys[1]
        assert (weights[a] / weights[b]) == pytest.approx(
            scaled[a] / scaled[b], rel=1e-9)

    def test_denormal_weights_collapse_rather_than_scale(self):
        """Pinned so the limitation is recorded rather than rediscovered.

        A weight at the denormal floor multiplied by any scale < 1 underflows
        to exactly zero, so a book containing one loses that name entirely
        instead of shrinking it. This is harmless in practice — 5e-324 of a
        portfolio is not a position, and `target_positions` would floor it to
        zero shares anyway — but it means the ratio-preservation property
        above is stated over realistic weights only.
        """
        scaled = normalise_weights({1: 5e-324, 2: 2.0}, max_gross=1.0)
        assert scaled[1] == 0.0
        assert scaled[2] > 0.0

    def test_a_non_finite_weight_makes_gross_unknown(self):
        """Not 0.0 — an unknown exposure must not read as no exposure."""
        assert gross_exposure({1: float('nan')}) is None
        assert gross_exposure({1: float('inf')}) is None


class TestTheNoTradeBandCanOnlyReduceTurnover:
    """A band exists to stop the book paying for adjustments too small to
    matter. It must never do the opposite, and it must never suppress the two
    decisions that are not adjustments at all: opening and closing."""

    def test_a_small_move_is_held(self):
        from trader.simulation.panel import apply_no_trade_band
        out = apply_no_trade_band({1: 100.0}, {1: 103.0}, band=0.10)
        assert out[1] == 100.0

    def test_a_large_move_is_taken(self):
        from trader.simulation.panel import apply_no_trade_band
        out = apply_no_trade_band({1: 100.0}, {1: 150.0}, band=0.10)
        assert out[1] == 150.0

    def test_an_entry_is_never_banded(self):
        """Going from nothing to a position is a decision, not drift. Banding
        it would leave the book unable to act on its own signal."""
        from trader.simulation.panel import apply_no_trade_band
        out = apply_no_trade_band({}, {1: 5.0}, band=0.99)
        assert out[1] == 5.0

    def test_an_exit_is_never_banded(self):
        from trader.simulation.panel import apply_no_trade_band
        out = apply_no_trade_band({1: 1000.0}, {1: 0.0}, band=0.99)
        assert out[1] == 0.0

    def test_a_sign_flip_is_never_banded(self):
        """Long to short is not an adjustment however small the numbers."""
        from trader.simulation.panel import apply_no_trade_band
        out = apply_no_trade_band({1: 1.0}, {1: -1.0}, band=0.99)
        assert out[1] == -1.0

    @_SETTINGS
    @given(cur=st.floats(min_value=1.0, max_value=1e5, allow_nan=False,
                         allow_infinity=False),
           tgt=st.floats(min_value=1.0, max_value=1e5, allow_nan=False,
                         allow_infinity=False),
           band=st.floats(min_value=0.0, max_value=1.0, allow_nan=False,
                          allow_infinity=False))
    def test_banding_never_increases_the_trade(self, cur, tgt, band):
        """The whole point. Whatever the band does, the resulting order must
        be no larger than the unbanded one."""
        from trader.simulation.panel import apply_no_trade_band
        banded = apply_no_trade_band({1: cur}, {1: tgt}, band=band)
        assert abs(banded[1] - cur) <= abs(tgt - cur) + 1e-9

    @_SETTINGS
    @given(cur=st.floats(min_value=1.0, max_value=1e4, allow_nan=False,
                         allow_infinity=False),
           tgt=st.floats(min_value=1.0, max_value=1e4, allow_nan=False,
                         allow_infinity=False))
    def test_a_zero_band_changes_nothing(self, cur, tgt):
        from trader.simulation.panel import apply_no_trade_band
        assert apply_no_trade_band({1: cur}, {1: tgt}, band=0.0)[1] == tgt

    def test_the_band_is_measured_in_notional_when_prices_are_given(self):
        """Cost scales with dollars traded, not share counts. A 3-share move
        in a $5,000 stock is not the same trade as a 3-share move in a $2 one."""
        from trader.simulation.panel import apply_no_trade_band
        # Same share delta, same fraction of target -> both banded or neither,
        # regardless of price. What price changes is nothing here; the point is
        # that supplying prices must not corrupt the ratio.
        cheap = apply_no_trade_band({1: 100.0}, {1: 103.0}, band=0.10,
                                    prices={1: 2.0})
        dear = apply_no_trade_band({1: 100.0}, {1: 103.0}, band=0.10,
                                   prices={1: 5000.0})
        assert cheap[1] == 100.0 and dear[1] == 100.0

    def test_a_negative_band_is_refused(self):
        import deal
        from trader.simulation.panel import apply_no_trade_band
        with pytest.raises((deal.PreContractError, ValueError)):
            apply_no_trade_band({1: 1.0}, {1: 2.0}, band=-0.1)


class TestMembershipHysteresis:
    """The weight-band failed because the turnover was never in the weights -
    it was in decile membership, where almost every trade is a whole position
    opening or closing. These properties pin the buffer that attacks that."""

    def _ranks(self, **kw):
        return kw

    def test_a_name_above_the_entry_bar_is_taken(self):
        from trader.simulation.panel import buffered_membership
        out = buffered_membership({1: 0.95, 2: 0.5}, frozenset(), 0.10, 0.25)
        assert out == frozenset({1})

    def test_a_held_name_inside_the_buffer_is_kept(self):
        """Rank 0.80 is below the 0.90 entry bar but above the 0.75 exit bar.
        A name already held stays; that is the entire mechanism."""
        from trader.simulation.panel import buffered_membership
        assert buffered_membership({1: 0.80}, frozenset({1}), 0.10, 0.25) \
            == frozenset({1})

    def test_the_same_name_is_not_BOUGHT_inside_the_buffer(self):
        """Asymmetry both ways: the buffer holds what you have, it does not
        admit what you do not. Otherwise the entry bar is just exit_pct."""
        from trader.simulation.panel import buffered_membership
        assert buffered_membership({1: 0.80}, frozenset(), 0.10, 0.25) \
            == frozenset()

    def test_a_name_below_the_exit_bar_is_dropped_even_if_held(self):
        from trader.simulation.panel import buffered_membership
        assert buffered_membership({1: 0.40}, frozenset({1}), 0.10, 0.25) \
            == frozenset()

    def test_equal_bars_reduce_to_no_buffer(self):
        """enter == exit is the un-buffered book, and must behave exactly as
        the plain decile rule - the degenerate case the buffer generalises."""
        from trader.simulation.panel import buffered_membership
        ranks = {1: 0.95, 2: 0.85, 3: 0.5}
        assert buffered_membership(ranks, frozenset({2}), 0.10, 0.10) \
            == frozenset({1})

    @_SETTINGS
    @given(rs=st.lists(st.floats(min_value=0.0, max_value=1.0,
                                 allow_nan=False, allow_infinity=False),
                       min_size=1, max_size=40),
           enter=st.floats(min_value=0.05, max_value=0.5, allow_nan=False,
                           allow_infinity=False),
           extra=st.floats(min_value=0.0, max_value=0.5, allow_nan=False,
                           allow_infinity=False))
    def test_the_buffer_can_only_widen_what_is_held(self, rs, enter, extra):
        """Buffering must never hold FEWER names than the plain rule, or it
        would be creating churn rather than damping it."""
        from trader.simulation.panel import buffered_membership
        ranks = {i: r for i, r in enumerate(rs)}
        held = frozenset(ranks)
        plain = buffered_membership(ranks, held, enter, enter)
        buffered = buffered_membership(ranks, held, enter, min(1.0, enter + extra))
        assert plain <= buffered

    def test_an_exit_bar_tighter_than_the_entry_bar_is_refused(self):
        """It would eject names faster than it admitted them and churn MORE -
        the opposite of the purpose."""
        import deal
        from trader.simulation.panel import buffered_membership
        with pytest.raises((deal.PreContractError, ValueError)):
            buffered_membership({1: 0.9}, frozenset(), 0.25, 0.10)

    def test_a_nonfinite_rank_is_skipped_not_held(self):
        from trader.simulation.panel import buffered_membership
        assert buffered_membership({1: float('nan')}, frozenset({1}),
                                   0.10, 0.25) == frozenset()


class TestTheBandsInteractionWithRebalancing:
    """`apply_no_trade_band` carried 23 surviving mutants - the second-largest
    concentration in the panel kernel. The existing tests cover single-name
    cases; these cover the arithmetic boundaries and the multi-name behaviour
    that a rebalanced book actually exercises."""

    def test_the_band_is_a_strict_threshold_not_inclusive(self):
        """A move of exactly `band` TRADES. Making it inclusive would suppress
        the marginal trade, and over a 500-name book the boundary case is not
        rare - it is where most of the distribution sits."""
        from trader.simulation.panel import apply_no_trade_band
        # target 100 -> 110 is exactly 10/110 = 0.0909... of target
        out = apply_no_trade_band({1: 100.0}, {1: 110.0}, band=10.0 / 110.0)
        assert out[1] == 110.0

    def test_each_name_is_banded_independently(self):
        """A small move on one name must not suppress a large move on another.
        A mutant that bands the whole dict on one name's ratio would pass every
        single-name test."""
        from trader.simulation.panel import apply_no_trade_band
        out = apply_no_trade_band({1: 100.0, 2: 100.0},
                                  {1: 101.0, 2: 200.0}, band=0.10)
        assert out[1] == 100.0        # 1% move: held
        assert out[2] == 200.0        # 100% move: taken

    def test_shorts_are_banded_on_magnitude_not_sign(self):
        """A short growing from -100 to -103 is a 3% adjustment, same as a long.
        A mutant comparing signed values would band the wrong side."""
        from trader.simulation.panel import apply_no_trade_band
        assert apply_no_trade_band({1: -100.0}, {1: -103.0}, band=0.10)[1] == -100.0
        assert apply_no_trade_band({1: -100.0}, {1: -150.0}, band=0.10)[1] == -150.0

    def test_a_name_absent_from_the_target_is_not_invented(self):
        """The band decides whether to ACT on an instruction, never to create
        one. A held name the strategy did not mention must not appear."""
        from trader.simulation.panel import apply_no_trade_band
        out = apply_no_trade_band({1: 100.0, 2: 50.0}, {1: 101.0}, band=0.10)
        assert set(out) == {1}

    def test_a_nonfinite_target_is_dropped_not_held(self):
        from trader.simulation.panel import apply_no_trade_band
        for bad in (float('nan'), float('inf')):
            assert apply_no_trade_band({1: 10.0}, {1: bad}, band=0.1) == {}

    def test_prices_only_scale_the_comparison_not_the_result(self):
        """Supplying prices changes WHETHER a trade happens, never the quantity
        traded. A mutant that returned a price-scaled quantity would corrupt
        every order."""
        from trader.simulation.panel import apply_no_trade_band
        out = apply_no_trade_band({1: 100.0}, {1: 200.0}, band=0.10,
                                  prices={1: 37.5})
        assert out[1] == 200.0

    def test_an_unpriced_name_still_bands_on_share_count(self):
        """Prices are optional per name. A name missing from the price map must
        fall back to the share comparison rather than being dropped."""
        from trader.simulation.panel import apply_no_trade_band
        out = apply_no_trade_band({1: 100.0, 2: 100.0}, {1: 101.0, 2: 300.0},
                                  band=0.10, prices={1: 5.0})
        assert out[1] == 100.0 and out[2] == 300.0


class TestNormalisationBoundaries:
    """`normalise_weights` had 8 survivors; the existing tests cover the
    scale-down path but not its edges."""

    def test_gross_exactly_at_the_cap_is_untouched(self):
        from trader.simulation.panel import gross_exposure, normalise_weights
        w = {1: 0.5, 2: -0.5}
        assert gross_exposure(normalise_weights(w, 1.0)) == pytest.approx(1.0)
        assert normalise_weights(w, 1.0) == w

    def test_a_non_finite_cap_leaves_the_book_alone(self):
        """An unusable cap must not silently scale to zero or raise; the book is
        the caller's expressed intent and a bad parameter is not a reason to
        discard it."""
        from trader.simulation.panel import normalise_weights
        w = {1: 2.0, 2: -3.0}
        for bad in (float('nan'), float('inf'), 0.0, -1.0):
            assert normalise_weights(w, bad) == w

    def test_a_non_finite_weight_is_dropped_from_the_result(self):
        from trader.simulation.panel import normalise_weights
        out = normalise_weights({1: 0.5, 2: float('nan')}, 1.0)
        assert set(out) == {1}

    def test_an_all_zero_book_survives_untouched(self):
        from trader.simulation.panel import normalise_weights
        assert normalise_weights({1: 0.0, 2: 0.0}, 1.0) == {1: 0.0, 2: 0.0}
