"""Regression tests derived from mutation testing (mutmut) of the pure kernel.

Each test below pins a behavior that a surviving mutant proved was NOT covered
by the existing suite — a real test gap surfaced by mutating the code and
watching which mutations no test noticed. The mutant id each test kills is named
in its docstring so the mapping stays auditable. See scripts/run_mutation.py and
docs on the mutation pass for how these were found.

These are ordinary behavioral assertions on the same public/pure functions the
rest of the kernel tests exercise — they do not touch production code, they only
close the observation gaps.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from trader.trading.order_math import (  # noqa: E402
    _all_finite_positive,
    _floor_shares_for_notional,
    whole_shares_for_notional,
)
from trader.trading.position_sizing import (  # noqa: E402
    PortfolioState,
    PositionSizer,
    PositionSizingConfig,
    compute_atr,
)
from trader.data.event_store import EventStore  # noqa: E402
from trader.objects import Action  # noqa: E402
from trader.trading.risk_gate import RiskGate, RiskLimits  # noqa: E402
from trader.trading.strategy import Signal  # noqa: E402


class TestOrderMathMutationKills:
    """Kills for surviving mutants in trader/trading/order_math.py."""

    def test_all_finite_positive_rejects_non_numeric(self):
        """Kills x__all_finite_positive__mutmut_7 (except TypeError: return False -> True).

        The guard must treat non-numeric inputs (str, None) as invalid — a
        mutation that returned True on the TypeError path went unnoticed because
        nothing exercised _all_finite_positive with a non-numeric value.
        """
        assert _all_finite_positive('x') is False
        assert _all_finite_positive(None) is False
        assert _all_finite_positive(10.0, '5') is False
        # sanity: genuinely finite-positive inputs still pass
        assert _all_finite_positive(1.0, 2.0, 3.0) is True

    def test_floor_core_default_multiplier_is_one(self):
        """Kills x__floor_shares_for_notional__mutmut_1 (default multiplier 1.0 -> 2.0).

        The private pure core's documented default multiplier is 1.0. Nothing
        called it with a *defaulted* multiplier, so flipping the default to 2.0
        (which would halve the share count) survived.
        """
        assert _floor_shares_for_notional(1000.0, 10.0) == 100

    def test_fp_overshoot_loop_returns_tight_floor(self):
        """Kills the loop-step mutants 23 (shares=1) and 25 (shares-=2).

        1.7 / 0.1 == 17.000000000000004 in float, and 17*0.1 == 1.7000000000000002
        > 1.7, so the fp-overshoot correction loop actually FIRES and must step
        17 -> 16 (one at a time). A mutant that jumps to 1, or steps by 2, still
        satisfies the "never exceeds notional" invariant but returns a wrong,
        under-bought count — so only an exact-value assertion catches it.
        """
        assert whole_shares_for_notional(1.7, 0.1) == 16
        # invariant still holds and the floor is tight (one more would exceed):
        assert 16 * 0.1 <= 1.7 < 17 * 0.1

    def test_fp_overshoot_loop_guard_uses_multiplier_product(self):
        """Kills x__floor_shares_for_notional__mutmut_20 (guard *multiplier -> /multiplier).

        With multiplier != 1 the loop guard `shares*price*multiplier > amount`
        differs from `shares*price/multiplier`. 0.018 / (0.001*2) overshoots at
        9 shares (9*0.001*2 == 0.018000000000000002 > 0.018); the correct answer
        is 8. The `/multiplier` mutant skips the correction (or the @deal.ensure
        postcondition rejects the over-notional 9) — either way != 8.
        """
        assert whole_shares_for_notional(0.018, 0.001, 2.0) == 8
        assert 8 * 0.001 * 2 <= 0.018 < 9 * 0.001 * 2

    def test_degenerate_denominator_message(self):
        """Kills x__floor_shares_for_notional__mutmut_9 (degenerate-denom msg -> None).

        price*multiplier overflowing to inf is a degenerate per-share cost. The
        branch was never exercised, so blanking its message survived. Assert the
        message names the degeneracy.
        """
        with pytest.raises(ValueError, match='degenerate'):
            whole_shares_for_notional(1.0, 1e300, 1e300)

    def test_ratio_overflow_message(self):
        """Kills x__floor_shares_for_notional__mutmut_14 (overflow msg -> None).

        A finite denominator with amount/denom overflowing to inf is degenerate.
        Reach it with a huge amount over a tiny per-share cost and assert the
        message names the overflow.
        """
        with pytest.raises(ValueError, match='overflow'):
            whole_shares_for_notional(1e308, 1e-5, 1e-5)

    def test_refusal_message_includes_multiplier_when_not_one(self):
        """Kills x__floor_shares_for_notional__mutmut_29 (`!= 1.0` -> `== 1.0`).

        When the amount can't cover one whole contract and multiplier != 1, the
        refusal message must name the multiplier. The condition-flip mutant would
        omit it for multiplier != 1 (and add it for 1.0).
        """
        with pytest.raises(ValueError) as ei:
            whole_shares_for_notional(250.0, 3.0, 100.0)
        assert 'multiplier 100' in str(ei.value)

    def test_refusal_message_omits_multiplier_when_one(self):
        """Kills mutants 30 (`!= 2.0`) and 31 (`else ""` -> `else "XXXX"`).

        With the default multiplier (1.0), the refusal message must NOT carry a
        multiplier suffix or a placeholder. Both mutants inject spurious text.
        """
        with pytest.raises(ValueError) as ei:
            whole_shares_for_notional(340.0, 510.0)
        msg = str(ei.value)
        assert 'multiplier' not in msg
        assert 'XXXX' not in msg
        assert msg.endswith('fractional shares are not enabled on this path')

    @pytest.mark.parametrize('param,args', [
        ('amount', (0.0, 10.0, 1.0)),
        ('price', (1000.0, 0.0, 1.0)),
        ('multiplier', (1000.0, 10.0, 0.0)),
    ])
    def test_bad_input_message_starts_with_param_name(self, param, args):
        """Kills mutants 2/4/6 (name literals 'amount'/'price'/'multiplier' -> 'XX..XX').

        The existing bad-input tests only assert the param name is a *substring*,
        which 'XXamountXX' also satisfies. The "fail loudly, name the offending
        parameter" contract means the message must START with the exact param
        name.
        """
        with pytest.raises(ValueError) as ei:
            whole_shares_for_notional(*args)
        assert str(ei.value).startswith(f'{param} must be a finite number > 0')


# Deterministic OHLC where the 14-period and 15-period ATR genuinely differ, so
# a test on the DEFAULT period distinguishes period=14 from period=15.
_ATR_H = [100, 102, 101, 105, 103, 107, 104, 109, 106, 111, 108, 113, 110, 115, 112, 118, 116, 120]
_ATR_L = [98, 99, 97, 101, 99, 102, 100, 104, 101, 106, 103, 108, 105, 110, 107, 112, 111, 115]
_ATR_C = [99, 101, 99, 103, 101, 105, 102, 107, 104, 109, 106, 111, 108, 113, 110, 116, 114, 118]


class TestComputeAtrMutationKills:
    """Kills for surviving mutants in trader/trading/position_sizing.compute_atr.

    compute_atr is a @deal-contracted pure helper feeding volatility-aware
    sizing; a wrong ATR silently mis-sizes every volatile position.
    """

    def test_default_period_is_14(self):
        """Kills x_compute_atr__mutmut_1 (default period 14 -> 15).

        Nothing pinned the default period, so bumping it to 15 (a different SMA
        window) went unnoticed. This asserts the default equals an explicit 14
        and differs from an explicit 15 on data chosen so the windows diverge.
        """
        assert compute_atr(_ATR_H, _ATR_L, _ATR_C) == compute_atr(_ATR_H, _ATR_L, _ATR_C, 14)
        assert compute_atr(_ATR_H, _ATR_L, _ATR_C) != compute_atr(_ATR_H, _ATR_L, _ATR_C, 15)

    def test_true_range_uses_all_three_terms_and_prev_close(self):
        """Kills mutants 36 (prev_c index i-1 -> i-2), 51 (drop |H-prevC|), 52 (drop |L-prevC|).

        TR = max(H-L, |H-prevC|, |L-prevC|). This series has bars where the
        gap-vs-prev-close terms dominate, so dropping a term or shifting prev_c
        changes the ATR. The exact value pins all three.
        """
        highs = [10, 12, 9, 15, 8, 11]
        lows = [9, 8, 7, 11, 6, 9]
        closes = [9.5, 11, 8, 14, 7, 10]
        assert compute_atr(highs, lows, closes, 3) == pytest.approx(6.333333333333333)

    def test_non_finite_bar_is_dropped_not_kept(self):
        """Kills x_compute_atr__mutmut_19 (`_bad(l) or _bad(c)` -> `_bad(l) and _bad(c)`).

        A bar with a single NaN component must be DROPPED (else the NaN
        propagates and ATR becomes NaN, violating the non-negative postcondition
        and silently disabling volatility sizing). The `and` mutant keeps a bar
        whose low alone is NaN. Assert the NaN-low series equals the same series
        with that bar removed.
        """
        highs = [10, 12, 9, 15, 8, 11]
        lows_nan = [9, 8, float('nan'), 11, 6, 9]
        closes = [9.5, 11, 8, 14, 7, 10]
        # same bars with the NaN bar (index 2) removed
        h_drop = [10, 12, 15, 8, 11]
        l_drop = [9, 8, 11, 6, 9]
        c_drop = [9.5, 11, 14, 7, 10]
        result = compute_atr(highs, lows_nan, closes, 3)
        assert result is not None
        assert result == pytest.approx(compute_atr(h_drop, l_drop, c_drop, 3))


class TestComputeMutationKills:
    """Kills for safety-relevant survivors in PositionSizer.compute."""

    def test_default_confidence_is_conservative(self):
        """Kills xǁPositionSizerǁcompute__mutmut_1 (default confidence 0.0 -> 1.0).

        The fail-safe default is confidence 0.0 (smallest size). Flipping the
        default to 1.0 would silently size every un-scored call at FULL size.
        Nothing pinned the default, so assert compute() with no confidence sizes
        exactly like confidence=0.0 and strictly below confidence=1.0.
        """
        sizer = PositionSizer(PositionSizingConfig())
        state = PortfolioState(net_liquidation=100_000.0)
        default = sizer.compute(portfolio_state=state)
        conf0 = sizer.compute(confidence=0.0, portfolio_state=state)
        conf1 = sizer.compute(confidence=1.0, portfolio_state=state)
        assert default.amount_usd == conf0.amount_usd
        assert default.amount_usd < conf1.amount_usd


@pytest.fixture
def _gate(tmp_path):
    es = EventStore(str(tmp_path / 'mutation_kills_events.duckdb'))
    return RiskGate(limits=RiskLimits(), event_store=es)


def _signal():
    return Signal(source_name='mk', action=Action.BUY, probability=0.8, risk=0.2, conid=4391)


class TestRiskGateBoundaryMutationKills:
    """Kills for gate-decision BOUNDARY survivors in RiskGate.

    Unlike a value-clamp boundary (same number either way), these flip the gate's
    verdict (approved vs refused) at the exact limit. The gate's rule is "refuse
    only when STRICTLY beyond the limit; AT the limit is allowed" — each `<`->`<=`
    or `>`->`>=` mutation silently reverses that at the boundary, and nothing
    exercised the exact-limit case.
    """

    def test_daily_loss_exactly_at_limit_is_allowed(self, _gate):
        """Kills xǁRiskGateǁevaluate__mutmut_46 (`daily_pnl < -limit` -> `<=`).

        A daily PnL of exactly -max_daily_loss is allowed; only a loss strictly
        beyond it is refused.
        """
        limit = _gate.limits.max_daily_loss
        assert _gate.evaluate(_signal(), daily_pnl=-limit).approved is True
        # strictly beyond is still refused (guards against over-loosening)
        assert _gate.evaluate(_signal(), daily_pnl=-limit - 0.01).approved is False

    def test_concentration_exactly_at_limit_is_allowed(self, _gate):
        """Kills xǁRiskGateǁevaluate__mutmut_128 (`concentration > max` -> `>=`).

        A position whose value is exactly max_position_size_pct of the portfolio
        is allowed; only strictly-over is refused.
        """
        pct = _gate.limits.max_position_size_pct
        pv = 100_000.0
        at_limit = _gate.evaluate(_signal(), portfolio_value=pv, position_value=pv * pct)
        assert at_limit.approved is True
        over = _gate.evaluate(_signal(), portfolio_value=pv, position_value=pv * pct + 1.0)
        assert over.approved is False

    def test_leverage_exactly_at_limit_is_allowed(self, _gate):
        """Kills xǁRiskGateǁcheck_leverage__mutmut_24 (`post_leverage > max` -> `>=`).

        Post-trade leverage exactly at the cap is allowed; only strictly-over is
        refused. (equity chosen so the cushion check comfortably passes.)
        """
        nl = 100_000.0
        at = _gate.check_leverage(
            {'initMarginAfter': nl * _gate.limits.max_leverage, 'equityWithLoanAfter': 2 * nl}, nl)
        assert at.approved is True
        over = _gate.check_leverage(
            {'initMarginAfter': nl * _gate.limits.max_leverage + 1.0, 'equityWithLoanAfter': 2 * nl}, nl)
        assert over.approved is False

    def test_cushion_exactly_at_minimum_is_allowed(self, _gate):
        """Kills xǁRiskGateǁcheck_leverage__mutmut_36 (`cushion < min` -> `<=`).

        A margin cushion exactly at the minimum is allowed; only strictly-below
        is refused. init chosen so leverage passes and cushion == min exactly.
        """
        nl = 100_000.0
        min_c = _gate.limits.min_margin_cushion
        init = nl * 0.9  # leverage 0.9 < max, passes
        equity = init + min_c * nl  # cushion == min exactly
        assert _gate.check_leverage({'initMarginAfter': init, 'equityWithLoanAfter': equity}, nl).approved is True
        equity_low = init + (min_c * nl) - 1.0  # just below min
        assert _gate.check_leverage({'initMarginAfter': init, 'equityWithLoanAfter': equity_low}, nl).approved is False

    def test_missing_init_margin_defaults_to_zero_not_none(self, _gate):
        """Post-flip, the default-None mutants are TRUE EQUIVALENTS — this
        test no longer kills them and no test can: None and 0 are both falsy,
        the falsy guard routes both to the no-margin-data refusal, and the
        arithmetic that could have TypeError'd on None sits behind that guard.
        Kept because the BEHAVIOUR it pins (refusal naming the missing datum,
        never a crash, never a leverage-limit refusal) is still the contract.
        """
        result = _gate.check_leverage({'equityWithLoanAfter': 50_000.0}, 100_000.0)
        assert result.approved is False
        assert 'initMarginAfter' in result.reason
