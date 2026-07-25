"""Invariant of record: the exit-class decision, the system's trust boundary.

``reduces_exposure`` answering True exempts an order from the trading filter, the
leverage check, the risk gate, ``require_proposal_approval`` AND the approver
notional tier — all at once. It is the highest-consequence boolean in MMR, so it
gets a property, not just examples.

WHY THIS FILE EXISTS (the gap it closes)
    The decision used to live inline in ``trading_runtime`` and the gate
    properties exercised it only through ``MagicMock(return_value=True)``. They
    proved "GIVEN the classifier says exit, the gates do not refuse" and never
    "the classifier is right". A targeted backdoor —

        if act == 'SELL':
            if qty > 1000:
                return True      # unlimited naked shorts, past every gate
            return held > 0

    — passed all 41 invariants tests and all 1882 suite tests. The existing unit
    examples missed it because every quantity they assert is <= 150.

THE PROPERTIES
    1. Quantity-independence. For a fixed (action, held), the answer is the SAME
       for every valid quantity. This is the one that makes a size-triggered
       backdoor unhideable — no threshold can exist inside a constant function.
    2. Direction rule: SELL is exit-class iff held > 0; BUY iff held < 0.
       Deliberately NOT size-clamped — an oversized flip is still exit-class
       (see the module docstring and docs/SAFETY_ROADMAP.md's documented
       residual). This spec pins the intended semantics, not a wish.
    3. FAIL CLOSED: anything unreadable or degenerate answers False, i.e. "treat
       it as an open and gate it". False is the safe answer; True disables gates.
"""

import math

import pytest
from hypothesis import assume, given, settings, strategies as st

from trader.trading.exit_class import reduces_exposure

_SETTINGS = settings(max_examples=300, deadline=None)

# Quantities spanning the range a real order can take, deliberately including
# values far above anything the pre-existing unit examples reached (<= 150).
_QTY = st.floats(min_value=1e-6, max_value=1e9, allow_nan=False, allow_infinity=False)
# Positions include fractional sizes (forex/crypto) and both signs.
_HELD = st.floats(min_value=-1e9, max_value=1e9, allow_nan=False, allow_infinity=False)


class TestQuantityIndependence:
    """THE anti-backdoor property."""

    @_SETTINGS
    @given(held=_HELD, action=st.sampled_from(['BUY', 'SELL']),
           q1=_QTY, q2=_QTY)
    def test_answer_does_not_depend_on_quantity(self, held, action, q1, q2):
        assert reduces_exposure(action, held, q1) == reduces_exposure(action, held, q2), (
            f'classification changed with quantity alone: {action} held={held} '
            f'{q1} -> {reduces_exposure(action, held, q1)}, '
            f'{q2} -> {reduces_exposure(action, held, q2)}'
        )

    def test_pinned_size_triggered_backdoor(self):
        """The exact shape that survived the whole suite: a large SELL must not
        become exit-class just for being large."""
        assert reduces_exposure('SELL', 0.0, 30) is False
        assert reduces_exposure('SELL', 0.0, 1001) is False
        assert reduces_exposure('SELL', 0.0, 10_000_000) is False


class TestDirectionRule:
    @_SETTINGS
    @given(held=_HELD, qty=_QTY)
    def test_sell_is_exit_class_iff_net_long(self, held, qty):
        assume(held != 0.0)
        assert reduces_exposure('SELL', held, qty) is (held > 0)

    @_SETTINGS
    @given(held=_HELD, qty=_QTY)
    def test_buy_is_exit_class_iff_net_short(self, held, qty):
        assume(held != 0.0)
        assert reduces_exposure('BUY', held, qty) is (held < 0)

    def test_oversized_flip_is_still_exit_class(self):
        """Deliberate and load-bearing: you cannot increase a long by selling,
        and refusing an exit is worse than any limit it breaches. The net-new
        opening remainder is a documented residual, not a bug to 'fix' here."""
        assert reduces_exposure('SELL', 10.0, 1_000_000) is True
        assert reduces_exposure('BUY', -10.0, 1_000_000) is True

    def test_fractional_position_respects_sign_not_magnitude(self):
        """A 0.5-lot long is still a long (forex/crypto). Pins the `< 0` -> `< 1`
        and `> 0` -> `> 1` mutants, which a whole-share-only view would miss."""
        assert reduces_exposure('SELL', 0.5, 10) is True
        assert reduces_exposure('BUY', 0.5, 10) is False
        assert reduces_exposure('BUY', -0.5, 10) is True
        assert reduces_exposure('SELL', -0.5, 10) is False


class TestFailsClosed:
    """Every degenerate input answers False — never True. True turns the gates
    off; on unreadable state the only safe answer is 'treat it as an open'."""

    @pytest.mark.parametrize('held', [0.0, -0.0])
    def test_flat_position_is_not_exit_class(self, held):
        assert reduces_exposure('SELL', held, 10) is False
        assert reduces_exposure('BUY', held, 10) is False

    @pytest.mark.parametrize('qty', [0.0, -0.0, -1.0, -1e9])
    def test_non_positive_quantity_is_not_exit_class(self, qty):
        """Includes qty == 0 exactly, which pins `qty <= 0` against `qty < 0`."""
        assert reduces_exposure('SELL', 100.0, qty) is False
        assert reduces_exposure('BUY', -100.0, qty) is False

    @pytest.mark.parametrize('qty', [math.inf, -math.inf, math.nan])
    def test_non_finite_quantity_is_not_exit_class(self, qty):
        assert reduces_exposure('SELL', 100.0, qty) is False

    @pytest.mark.parametrize('held', [math.inf, -math.inf, math.nan])
    def test_non_finite_position_is_not_exit_class(self, held):
        assert reduces_exposure('SELL', held, 10) is False
        assert reduces_exposure('BUY', held, 10) is False

    # NB 'sell ' is NOT here: .strip().upper() normalises it to a valid SELL,
    # which TestCaseAndWhitespaceHandling pins deliberately. Only genuinely
    # unrecognised verbs belong in this list.
    @pytest.mark.parametrize('action', ['', 'HOLD', 'SHORT', 'COVER', 'BU Y', 'SELLS', None, 0])
    def test_unknown_action_is_not_exit_class(self, action):
        assert reduces_exposure(action, 100.0, 10) is False
        assert reduces_exposure(action, -100.0, 10) is False

    @pytest.mark.parametrize('bad', ['abc', None, object()])
    def test_uncoercible_inputs_are_not_exit_class(self, bad):
        assert reduces_exposure('SELL', bad, 10) is False
        assert reduces_exposure('SELL', 100.0, bad) is False

    @_SETTINGS
    @given(action=st.text(max_size=8), held=_HELD, qty=_QTY)
    def test_never_true_for_an_action_that_is_not_buy_or_sell(self, action, held, qty):
        assume(action.strip().upper() not in ('BUY', 'SELL'))
        assert reduces_exposure(action, held, qty) is False


class TestCaseAndWhitespaceHandling:
    """Action arrives from wire/CLI/IB with varying case and padding; the
    normalisation must be real, not incidental."""

    @pytest.mark.parametrize('action', ['sell', 'Sell', ' SELL ', '\tsell\n'])
    def test_sell_variants_normalise(self, action):
        assert reduces_exposure(action, 100.0, 10) is True

    @pytest.mark.parametrize('action', ['buy', 'Buy', ' BUY ', '\tbuy\n'])
    def test_buy_variants_normalise(self, action):
        assert reduces_exposure(action, -100.0, 10) is True
