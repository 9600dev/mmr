"""Invariants of record: PositionSizer.

Properties over the documented sizing pipeline
(base × risk × confidence × volatility, then hard caps, then liquidity):

  (a) the computed amount never exceeds any ACTIVE cap — max_position_usd,
      max_position_pct of net liq, and the ADV liquidity cap — whenever
      net_liquidation > 0 and a price is known;
  (b) higher ATR%  ⇒  amount non-increasing (all else fixed);
  (c) higher confidence  ⇒  amount non-decreasing (all else fixed);
  (d) the amount is never negative;
  (e) degenerate spread configs (threshold <= 0) and extreme spreads never
      crash the sizer.

Comparisons are tolerant, not strict: amounts are rounded to cents and the
documented floors/clips (min_position_usd bump, vol_scale clamps, zeroing
below minimum) make strict monotonicity false by design. The tolerances cover
only the documented cent-rounding, nothing more.
"""

from hypothesis import given, settings, strategies as st

from trader.trading.position_sizing import (
    LiquidityInfo,
    PortfolioState,
    PositionSizer,
    PositionSizingConfig,
    VolatilityInfo,
)


# One cent of rounding per compute() call.
ROUND_TOL = 0.011
# Two compute() calls compared against each other.
PAIR_TOL = 0.021


@st.composite
def sizing_configs(draw, max_spread_factor=1.0):
    """Configs drawn from the documented parameter space (min <= max bounds,
    min_confidence_scale in [0, 1], vol_scale_min <= vol_scale_max)."""
    min_pos = draw(st.floats(min_value=50.0, max_value=2_000.0))
    max_pos = draw(st.floats(min_value=min_pos, max_value=60_000.0))
    vol_min = draw(st.floats(min_value=0.05, max_value=1.0))
    vol_max = draw(st.floats(min_value=1.0, max_value=4.0))
    return PositionSizingConfig(
        min_position_usd=min_pos,
        max_position_usd=max_pos,
        max_position_pct=draw(st.floats(min_value=0.01, max_value=0.5)),
        max_total_exposure_pct=draw(st.floats(min_value=0.1, max_value=1.0)),
        max_positions=draw(st.integers(min_value=1, max_value=50)),
        base_position_usd=draw(st.floats(min_value=100.0, max_value=50_000.0)),
        base_position_pct=draw(st.sampled_from([0.0, 0.0, 0.01, 0.05])),
        risk_level=draw(st.sampled_from(['conservative', 'moderate', 'aggressive'])),
        daily_loss_limit_usd=draw(st.floats(min_value=100.0, max_value=10_000.0)),
        min_confidence_scale=draw(st.floats(min_value=0.0, max_value=1.0)),
        volatility_adjustment=draw(st.booleans()),
        reference_atr_pct=draw(st.floats(min_value=0.005, max_value=0.05)),
        vol_scale_min=vol_min,
        vol_scale_max=vol_max,
        max_adv_pct=draw(st.floats(min_value=0.001, max_value=0.1)),
        spread_penalty_threshold=draw(st.floats(min_value=0.0005, max_value=0.02)),
        spread_penalty_factor=draw(st.floats(min_value=0.0, max_value=max_spread_factor)),
    )


@st.composite
def portfolio_states(draw):
    # WIDENED 2026-07-25 to include the degenerate region this generator used to
    # exclude. With net_liq >= 10_000 a guard written `> 0` and one written `> 1`
    # can never disagree, so mutating one into the other was undetectable — 70
    # surviving mutants in position_sizing traced to exactly that. Every state
    # produced here is MEASURED (net_liquidation_evaluable=True), which is what
    # makes the percentage-cap assertions meaningful: an unreadable account has
    # no meaningful cap and is covered separately below.
    # A MIXTURE, not a uniform range. Widening the range alone was not enough:
    # a uniform draw over [0, 5M] lands in the (0, 1] band about once in five
    # million tries, so the boundary mutants (`> 0` vs `> 1`) stayed invisible.
    # Sampling the degenerate band explicitly is what makes them reachable.
    net_liq = draw(st.one_of(
        st.just(0.0),                                          # measured-empty
        st.floats(min_value=1e-6, max_value=1.0),              # sub-unit accounts
        st.floats(min_value=1.0, max_value=1_000.0),           # tiny
        st.floats(min_value=10_000.0, max_value=5_000_000.0),  # realistic
    ))
    return PortfolioState(
        net_liquidation=net_liq,
        net_liquidation_evaluable=True,
        gross_position_value=draw(st.floats(min_value=0.0, max_value=net_liq * 1.2)),
        available_funds=net_liq,
        daily_pnl=draw(st.floats(min_value=-15_000.0, max_value=15_000.0)),
        position_count=draw(st.integers(min_value=0, max_value=60)),
        pending_proposal_value=draw(st.floats(min_value=0.0, max_value=200_000.0)),
    )


# 800 examples, not 200: the account-value strategy is a MIXTURE of four bands
# (measured-empty, sub-unit, tiny, realistic). At 200 draws each band gets ~50,
# which measurably weakened coverage of the realistic band — mutants that had
# been killed started surviving. Widening an input space costs examples; budget
# for it rather than diluting what already worked.
@settings(max_examples=800, deadline=None)
@given(
    cfg=sizing_configs(),
    state=portfolio_states(),
    confidence=st.floats(min_value=0.0, max_value=1.0),
    price=st.one_of(st.floats(min_value=1e-4, max_value=1.0),      # sub-unit prices
                    st.floats(min_value=1.0, max_value=5_000.0)),
    adv=st.floats(min_value=0.0, max_value=5e7),
)
def test_amount_never_exceeds_any_active_cap(cfg, state, confidence, price, adv):
    """(a) For a MEASURED account, the amount respects every active cap
    simultaneously — including when the measurement is zero, where every cap is
    zero and the only conforming answer is to refuse."""
    liquidity = LiquidityInfo(
        avg_daily_volume=adv, bid=price * 0.999, ask=price * 1.001)
    result = PositionSizer(cfg).compute(
        confidence=confidence,
        portfolio_state=state,
        price=price,
        liquidity=liquidity,
    )
    amount = result.amount_usd
    assert amount <= cfg.max_position_usd + ROUND_TOL
    assert amount <= state.net_liquidation * cfg.max_position_pct + ROUND_TOL
    if adv > 0:
        assert amount <= adv * cfg.max_adv_pct * price + ROUND_TOL, (
            f'amount {amount} exceeds ADV cap '
            f'{adv * cfg.max_adv_pct * price} (capped_by={result.capped_by!r})'
        )


@settings(max_examples=800, deadline=None)
@given(
    cfg=sizing_configs(),
    state=portfolio_states(),
    confidence=st.floats(min_value=0.0, max_value=1.0),
    price=st.floats(min_value=1.0, max_value=5_000.0),
    atr_a=st.floats(min_value=0.001, max_value=0.25),
    atr_b=st.floats(min_value=0.001, max_value=0.25),
)
def test_higher_atr_never_increases_amount(cfg, state, confidence, price, atr_a, atr_b):
    """(b) Volatility sizing is ATR-inverse: more volatile never means a
    bigger position (tolerant of cent rounding; clamps make many pairs
    equal, which is fine — the property is non-increase, not decrease)."""
    lo, hi = sorted([atr_a, atr_b])
    sizer = PositionSizer(cfg)
    amount_lo = sizer.compute(
        confidence=confidence, portfolio_state=state, price=price,
        volatility=VolatilityInfo(atr=lo * price, price=price),
    ).amount_usd
    amount_hi = sizer.compute(
        confidence=confidence, portfolio_state=state, price=price,
        volatility=VolatilityInfo(atr=hi * price, price=price),
    ).amount_usd
    assert amount_hi <= amount_lo + PAIR_TOL, (
        f'ATR {hi:.4f} sized ${amount_hi} > ATR {lo:.4f} sized ${amount_lo}'
    )


@settings(max_examples=800, deadline=None)
@given(
    cfg=sizing_configs(),
    state=portfolio_states(),
    conf_a=st.floats(min_value=0.0, max_value=1.0),
    conf_b=st.floats(min_value=0.0, max_value=1.0),
    price=st.floats(min_value=1.0, max_value=5_000.0),
)
def test_higher_confidence_never_decreases_amount(cfg, state, conf_a, conf_b, price):
    """(c) Confidence scaling is monotone upward (given the documented
    min_confidence_scale in [0, 1])."""
    lo, hi = sorted([conf_a, conf_b])
    sizer = PositionSizer(cfg)
    amount_lo = sizer.compute(confidence=lo, portfolio_state=state, price=price).amount_usd
    amount_hi = sizer.compute(confidence=hi, portfolio_state=state, price=price).amount_usd
    assert amount_lo <= amount_hi + PAIR_TOL, (
        f'confidence {lo:.3f} sized ${amount_lo} > confidence {hi:.3f} sized ${amount_hi}'
    )


@settings(max_examples=800, deadline=None)
@given(
    cfg=sizing_configs(max_spread_factor=3.0),  # beyond the documented [0, 1] on purpose
    state=portfolio_states(),
    confidence=st.floats(min_value=0.0, max_value=1.0),
    price=st.floats(min_value=0.01, max_value=1e6),
    bid=st.floats(min_value=0.0, max_value=1_000.0),
    ask_mult=st.floats(min_value=1.0, max_value=1_000.0),
    adv=st.floats(min_value=0.0, max_value=5e7),
    atr_pct=st.floats(min_value=0.0, max_value=1.0),
)
def test_amount_is_never_negative(cfg, state, confidence, price, bid, ask_mult, adv, atr_pct):
    """(d) Post-fix: no input combination — including a spread_penalty_factor
    above 1, which once produced a negative reduction — yields a negative
    amount."""
    liquidity = LiquidityInfo(avg_daily_volume=adv, bid=bid, ask=bid * ask_mult, last=price)
    volatility = VolatilityInfo(atr=atr_pct * price, price=price)
    result = PositionSizer(cfg).compute(
        confidence=confidence,
        portfolio_state=state,
        price=price,
        liquidity=liquidity,
        volatility=volatility,
    )
    assert result.amount_usd >= 0.0, (
        f'negative sized amount {result.amount_usd} (capped_by={result.capped_by!r})'
    )
    assert result.quantity >= 0


@settings(max_examples=150, deadline=None)
@given(
    threshold=st.one_of(
        st.sampled_from([0.0, -0.005, -1.0]),
        st.floats(min_value=0.0001, max_value=0.05),
    ),
    bid=st.floats(min_value=0.01, max_value=100.0),
    ask_mult=st.floats(min_value=1.0, max_value=10_000.0),
    confidence=st.floats(min_value=0.0, max_value=1.0),
)
def test_degenerate_spread_config_and_extreme_spreads_never_crash(
        threshold, bid, ask_mult, confidence):
    """(e) Post-fix: a non-positive spread_penalty_threshold (which once
    divided by zero) and absurd spreads must degrade gracefully — the sizer
    returns a non-negative amount, it does not raise."""
    cfg = PositionSizingConfig(spread_penalty_threshold=threshold)
    liquidity = LiquidityInfo(avg_daily_volume=1e6, bid=bid, ask=bid * ask_mult)
    result = PositionSizer(cfg).compute(
        confidence=confidence,
        portfolio_state=PortfolioState(net_liquidation=100_000.0),
        price=bid,
        liquidity=liquidity,
    )
    assert result.amount_usd >= 0.0


@settings(max_examples=800, deadline=None)
@given(cfg=sizing_configs(),
       net_liq=st.floats(min_value=1e-6, max_value=5_000_000.0,
                         allow_nan=False, allow_infinity=False),
       confidence=st.floats(min_value=0.0, max_value=1.0),
       price=st.floats(min_value=0.01, max_value=5_000.0))
def test_a_measured_positive_account_is_never_refused_as_worthless(
        cfg, net_liq, confidence, price):
    """A MEASURED account with any positive value must never be refused on the
    grounds of having no value.

    The `net-liq-not-positive` refusal exists for a measured zero or negative
    account. Its guard is `<= 0`, and mutating that to `<= 1` would silently
    refuse every sub-unit account — a refusal the cap properties cannot catch,
    because refusing always satisfies an upper bound. This is the property that
    pins the guard from the other side.
    """
    state = PortfolioState(net_liquidation=net_liq, net_liquidation_evaluable=True,
                           available_funds=net_liq)
    result = PositionSizer(cfg).compute(
        confidence=confidence, portfolio_state=state, price=price)
    assert result.capped_by != 'net-liq-not-positive', (
        f'a measured account worth {net_liq} was refused as worthless')
