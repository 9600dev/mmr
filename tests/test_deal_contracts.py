"""The deal contracts on the pure kernel double as executable test oracles.

The ``deal`` ``@pre`` / ``@ensure`` / ``@raises`` contracts CrossHair verifies
symbolically are also enforced at runtime, so exercising the functions here (in
the ordinary pytest run) catches a contract regression even without invoking
CrossHair. For the string-domain transition predicates we use ``deal.cases`` —
deal's own Hypothesis integration, which generates inputs and asserts the
contracts hold. For the numeric helpers we drive bounded Hypothesis strategies
directly: their preconditions carve out narrow numeric bands ([0, 1], small
positive ranges) that ``deal.cases``'s unbounded float generation would reject
almost entirely, and the order-math float-correction loop has a pathological
slow path on absurd (denormal / ~1e18-share) inputs that never occur in trading
and that unbounded fuzzing would hang on. Realistic bounds keep the oracles
fast and on-domain while still exercising every contract.

Contracted functions:
  * order_math.whole_shares_for_notional / _floor_shares_for_notional
  * position_sizing._confidence_scale / _volatility_multiplier / compute_atr
  * proposal_transitions.is_known_status / is_valid_transition
"""

import deal
import pytest
from hypothesis import given, settings, strategies as st

from trader.data.proposal_transitions import (
    _ALLOWED_TRANSITIONS,
    _TERMINAL,
    is_known_status,
    is_valid_transition,
)
from trader.trading.order_math import (
    _floor_shares_for_notional,
    whole_shares_for_notional,
)
from trader.trading.position_sizing import (
    _confidence_scale,
    _volatility_multiplier,
    compute_atr,
)


# ---------------------------------------------------------------------------
# deal.cases oracles — safe where the input domain is strings (no narrow
# numeric precondition to filter against). deal generates cases and asserts
# the @ensure invariants hold.
# ---------------------------------------------------------------------------

def test_cases_is_known_status():
    deal.cases(is_known_status, count=100)()


def test_cases_is_valid_transition():
    deal.cases(is_valid_transition, count=100)()


# ---------------------------------------------------------------------------
# The contracts are LIVE at runtime (deal not disabled / stripped).
# ---------------------------------------------------------------------------

class TestContractsAreLive:
    def test_floor_shares_precondition_fires_on_non_positive(self):
        """The @deal.pre(finite & > 0) on the pure core is enforced — a
        precondition violation raises a deal PreContractError, proving the
        contract runs (the PUBLIC function keeps its own ValueError path)."""
        with pytest.raises(deal.PreContractError):
            _floor_shares_for_notional(0.0, 10.0)
        with pytest.raises(deal.PreContractError):
            _floor_shares_for_notional(100.0, -1.0)

    def test_public_function_keeps_valueerror_not_precontract(self):
        """The defensively-total public function raises a plain ValueError
        (naming the param), NOT a deal ContractError — behaviour unchanged."""
        with pytest.raises(ValueError, match='amount') as ei:
            whole_shares_for_notional(0.0, 10.0)
        assert not isinstance(ei.value, deal.ContractError)

    def test_confidence_scale_precondition(self):
        with pytest.raises(deal.PreContractError):
            _confidence_scale(1.5, 0.5)   # min_scale out of [0, 1]

    def test_volatility_multiplier_precondition(self):
        with pytest.raises(deal.PreContractError):
            _volatility_multiplier(0.0, 0.02, 0.25, 2.0)   # reference_atr_pct <= 0
        with pytest.raises(deal.PreContractError):
            _volatility_multiplier(0.02, 0.02, 2.0, 0.25)  # vol_scale_min > vol_scale_max


# ---------------------------------------------------------------------------
# order_math: flagship postcondition + the CrossHair-found underflow regression.
# ---------------------------------------------------------------------------

class TestOrderMathContract:
    @settings(max_examples=300, deadline=None)
    @given(
        amount=st.floats(min_value=1.0, max_value=1e7, allow_nan=False, allow_infinity=False),
        price=st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
        multiplier=st.sampled_from([1.0, 100.0]),
    )
    def test_public_ensure_holds_or_valueerror(self, amount, price, multiplier):
        try:
            shares = whole_shares_for_notional(amount, price, multiplier)
        except ValueError:
            return
        # This is exactly the @deal.ensure, asserted independently.
        assert shares >= 1
        assert shares * price * multiplier <= amount

    @settings(max_examples=300, deadline=None)
    @given(
        amount=st.floats(min_value=1.0, max_value=1e7, allow_nan=False, allow_infinity=False),
        price=st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
        multiplier=st.sampled_from([1.0, 100.0]),
    )
    def test_pure_core_ensure_holds_or_valueerror(self, amount, price, multiplier):
        """Same oracle on the contracted pure core (its @deal.pre is satisfied
        by construction: every input is finite & > 0)."""
        try:
            shares = _floor_shares_for_notional(amount, price, multiplier)
        except ValueError:
            return
        assert shares >= 1
        assert shares * price * multiplier <= amount

    def test_crosshair_underflow_counterexample_refuses(self):
        """Pinned regression: CrossHair found that price * multiplier can
        underflow to 0.0 for denormal inputs, which made the original code raise
        ZeroDivisionError (undeclared, violating @deal.raises(ValueError)). The
        fix refuses loudly with ValueError. These are the EXACT symbolic inputs
        CrossHair reported."""
        with pytest.raises(ValueError):
            whole_shares_for_notional(2.0, 3.0765742648370966e-154, 3.034084836703205e-308)
        with pytest.raises(ValueError):
            _floor_shares_for_notional(2.0, 3.0765742648370966e-154, 3.034084836703205e-308)

    def test_overflow_ratio_refuses(self):
        """Companion to the underflow case: a huge amount over a denormal
        per-share cost overflows the ratio to inf; math.floor(inf) would raise
        OverflowError. The guard refuses with ValueError instead."""
        with pytest.raises(ValueError):
            _floor_shares_for_notional(1e308, 1e-308, 1.0)


# ---------------------------------------------------------------------------
# position_sizing: pure helpers stay inside their documented bands.
# ---------------------------------------------------------------------------

class TestSizingHelperContracts:
    @settings(max_examples=300, deadline=None)
    @given(
        min_scale=st.floats(min_value=0.0, max_value=1.0),
        confidence=st.floats(min_value=-2.0, max_value=3.0, allow_nan=False),
    )
    def test_confidence_scale_in_band(self, min_scale, confidence):
        scale = _confidence_scale(min_scale, confidence)
        assert min_scale - 1e-9 <= scale <= 1.0 + 1e-9

    @settings(max_examples=300, deadline=None)
    @given(
        ref=st.floats(min_value=0.001, max_value=0.1),
        atr=st.floats(min_value=0.0001, max_value=1.0),
        vmin=st.floats(min_value=0.05, max_value=1.0),
        vspan=st.floats(min_value=0.0, max_value=3.0),
    )
    def test_volatility_multiplier_in_band(self, ref, atr, vmin, vspan):
        vmax = vmin + vspan
        mult = _volatility_multiplier(ref, atr, vmin, vmax)
        assert vmin - 1e-9 <= mult <= vmax + 1e-9

    @settings(max_examples=200, deadline=None)
    @given(
        highs=st.lists(st.floats(min_value=-1e6, max_value=1e6), max_size=40),
        lows=st.lists(st.floats(min_value=-1e6, max_value=1e6), max_size=40),
        closes=st.lists(st.floats(min_value=-1e6, max_value=1e6), max_size=40),
    )
    def test_compute_atr_none_or_nonnegative(self, highs, lows, closes):
        """The @deal.ensure: None, or a non-negative ATR (never NaN)."""
        result = compute_atr(highs, lows, closes, period=5)
        assert result is None or result >= 0.0

    def test_compute_atr_drops_nonfinite_bars(self):
        """The _bad filter now drops NaN AND ±inf, keeping the postcondition
        (non-negative ATR) true even when a poison bar is present."""
        highs = [10.0, 11.0, float('inf'), 12.0, 13.0, 14.0, 15.0]
        lows = [9.0, 10.0, 5.0, 11.0, 12.0, 13.0, 14.0]
        closes = [9.5, 10.5, float('nan'), 11.5, 12.5, 13.5, 14.5]
        result = compute_atr(highs, lows, closes, period=3)
        assert result is None or result >= 0.0

    def test_compute_atr_period_precondition(self):
        with pytest.raises(deal.PreContractError):
            compute_atr([1.0, 2.0, 3.0], [0.5, 1.5, 2.5], [0.9, 1.9, 2.9], period=0)


# ---------------------------------------------------------------------------
# proposal_transitions: the pure predicate matches the documented table and its
# contract invariants (no outgoing edge from a terminal state).
# ---------------------------------------------------------------------------

class TestTransitionPredicateContracts:
    ALL = sorted(set(_ALLOWED_TRANSITIONS) | _TERMINAL)

    def test_terminal_states_have_no_outgoing_edges(self):
        for term in _TERMINAL:
            for to in self.ALL:
                assert is_valid_transition(term, to) is False, (
                    f'{term} is terminal but {term}->{to} reported legal'
                )

    def test_legal_transition_implies_source_not_terminal_and_target_known(self):
        for frm in self.ALL:
            for to in self.ALL:
                if is_valid_transition(frm, to):
                    assert frm not in _TERMINAL           # contract-invariant 1
                    assert is_known_status(to)            # contract-invariant 2

    @settings(max_examples=200, deadline=None)
    @given(frm=st.text(max_size=12), to=st.text(max_size=12))
    def test_arbitrary_strings_never_break_the_contract(self, frm, to):
        result = is_valid_transition(frm, to)
        # the @deal.ensure invariants, asserted independently over junk input:
        assert (not result) or (frm not in _TERMINAL)
        assert (not result) or (to in _ALLOWED_TRANSITIONS)
