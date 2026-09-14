"""Ordinary sizing contract examples, staged outside the active mutation oracle.

Expected dollar/share values are independent examples. These tests deliberately
avoid exact human-readable diagnostic wording and do not execute broker calls.
"""

import pytest

from trader.trading.position_sizing import (
    LiquidityInfo,
    PortfolioState,
    PositionSizer,
    PositionSizingConfig,
)


def _sizer(**changes):
    """Keep unrelated limits out of a focused sizing example."""
    config = {
        "base_position_usd": 100.0,
        "min_position_usd": 0.01,
        "max_position_usd": 100_000.0,
        "max_position_pct": 1.0,
        "max_total_exposure_pct": 1.0,
        "min_confidence_scale": 1.0,
        "volatility_adjustment": False,
    }
    config.update(changes)
    return PositionSizer(PositionSizingConfig(**config))


def _assert_warnings(warnings, count):
    assert isinstance(warnings, list)
    assert len(warnings) == count
    assert all(isinstance(warning, str) and warning.strip() for warning in warnings)


@pytest.mark.parametrize(
    "changes,state_values,liquidity_values,price",
    [
        ({"min_position_usd": 100.0}, {}, None, 10.0),
        ({"max_position_usd": 100.0}, {}, None, 10.0),
        ({"max_position_pct": 0.1}, {"net_liquidation": 1000.0}, None, 10.0),
        (
            {},
            {"net_liquidation": 1000.0, "gross_position_value": 800.0,
             "pending_proposal_value": 100.0},
            None,
            10.0,
        ),
        ({"max_adv_pct": 0.1}, {}, {"avg_daily_volume": 100.0}, 10.0),
    ],
    ids=["minimum", "maximum", "position-percent", "exposure", "adv"],
)
def test_reaching_a_limit_without_reduction_does_not_report_a_cap(
    changes, state_values, liquidity_values, price,
):
    result = _sizer(**changes).compute(
        portfolio_state=PortfolioState(**state_values),
        liquidity=LiquidityInfo(**liquidity_values) if liquidity_values else None,
        price=price,
    )
    assert result.amount_usd == 100.0
    assert result.quantity == 10
    assert result.capped_by == ""


@pytest.mark.parametrize(
    "already_deployed,expected_amount,expected_quantity",
    [(100.0, 0.0, 0), (100.5, 0.0, 0), (99.5, 0.5, 2)],
)
def test_exposure_capacity_has_no_one_dollar_floor(
    already_deployed, expected_amount, expected_quantity,
):
    result = _sizer(base_position_usd=2.0).compute(
        portfolio_state=PortfolioState(
            net_liquidation=100.0, gross_position_value=already_deployed,
        ),
        price=0.25,
    )
    assert result.amount_usd == expected_amount
    assert result.quantity == expected_quantity
    assert result.capped_by == "max_total_exposure_pct"


def test_percentage_base_applies_to_a_positive_sub_dollar_account():
    result = _sizer(base_position_pct=0.5).compute(
        portfolio_state=PortfolioState(
            net_liquidation=0.5, net_liquidation_evaluable=True,
        ),
        price=0.25,
    )
    assert result.amount_usd == 0.25
    assert result.quantity == 1
    assert result.capped_by == ""


@pytest.mark.parametrize("gross,warnings", [(89.99, 0), (90.0, 1)])
def test_near_exposure_warning_includes_the_ninety_percent_boundary(gross, warnings):
    result = _sizer(base_position_usd=5.0).compute(
        portfolio_state=PortfolioState(net_liquidation=100.0, gross_position_value=gross),
        price=1.0,
    )
    assert result.amount_usd == 5.0
    assert result.quantity == 5
    _assert_warnings(result.warnings, warnings)


@pytest.mark.parametrize("net_liquidation", [0.0, -1.0])
def test_measured_nonpositive_account_refusal_explains_its_zero_size(net_liquidation):
    result = _sizer().compute(
        portfolio_state=PortfolioState(
            net_liquidation=net_liquidation, net_liquidation_evaluable=True,
        ),
    )
    assert result.amount_usd == 0.0
    assert result.quantity == 0
    assert result.capped_by == "net-liq-not-positive"
    assert isinstance(result.reasoning, str) and result.reasoning.strip()
    _assert_warnings(result.warnings, 1)


@pytest.mark.parametrize(
    "pnl,limit,amount,warnings",
    [
        (0.0, 0.0, 100.0, 0),
        (0.5, 0.25, 100.0, 0),
        (0.45, 0.5, 100.0, 0),
        (100.0, 100.0, 100.0, 0),
        (-79.99, 100.0, 100.0, 0),
        (-80.0, 100.0, 100.0, 1),
        (-99.99, 100.0, 100.0, 1),
        (-100.0, 100.0, 0.0, 1),
    ],
)
def test_loss_gate_and_warning_require_an_actual_loss(pnl, limit, amount, warnings):
    result = _sizer(daily_loss_limit_usd=limit).compute(
        portfolio_state=PortfolioState(daily_pnl=pnl), price=10.0,
    )
    assert result.amount_usd == amount
    assert result.quantity == (10 if amount else 0)
    _assert_warnings(result.warnings, warnings)


def test_explicit_penny_price_is_not_replaced_by_a_different_market_mid():
    result = _sizer(base_position_usd=10.0).compute(
        price=0.5, liquidity=LiquidityInfo(bid=2.0, ask=2.0),
    )
    assert result.amount_usd == 10.0
    assert result.quantity == 20
    _assert_warnings(result.warnings, 0)


def test_missing_explicit_price_uses_a_positive_penny_mid_for_whole_shares():
    result = _sizer(base_position_usd=0.75).compute(
        liquidity=LiquidityInfo(bid=0.25, ask=0.25),
    )
    assert result.amount_usd == 0.75
    assert result.quantity == 3
    _assert_warnings(result.warnings, 0)


def test_positive_sub_dollar_size_still_obeys_adv_capacity():
    result = _sizer(base_position_usd=0.75, max_adv_pct=0.5).compute(
        price=1.0, liquidity=LiquidityInfo(avg_daily_volume=1.0),
    )
    assert result.amount_usd == 0.5
    assert result.quantity == 0
    assert result.capped_by == "adv_liquidity"


def test_known_volume_without_any_price_keeps_the_offline_dollar_size():
    result = _sizer().compute(liquidity=LiquidityInfo(avg_daily_volume=1000.0))
    assert result.amount_usd == 100.0
    assert result.quantity == 0
    assert result.capped_by == ""
    _assert_warnings(result.warnings, 0)


@pytest.mark.parametrize("amount,warnings", [(100.0, 0), (150.0, 1), (200.0, 1)])
def test_adv_warning_starts_above_half_of_the_configured_participation(amount, warnings):
    # 2% of 1,000 shares is 20 shares; at $10 the cap is $200.
    result = _sizer(base_position_usd=amount).compute(
        price=10.0, liquidity=LiquidityInfo(avg_daily_volume=1000.0),
    )
    assert result.amount_usd == amount
    assert result.capped_by == ""
    _assert_warnings(result.warnings, warnings)


def test_adv_warning_accepts_an_average_volume_of_one_share():
    # 0.015 shares is below the 0.02-share ADV cap, but above its halfway warning.
    # No whole share is affordable; both independent warnings must be present.
    result = _sizer(base_position_usd=150.0).compute(
        price=10_000.0, liquidity=LiquidityInfo(avg_daily_volume=1.0),
    )
    assert result.amount_usd == 150.0
    assert result.quantity == 0
    assert result.capped_by == ""
    _assert_warnings(result.warnings, 2)


def test_adv_warning_accepts_a_penny_price():
    # $75 at $0.25 is 300 shares: 1.5% of a 20,000-share ADV.
    result = _sizer(base_position_usd=75.0).compute(
        price=0.25, liquidity=LiquidityInfo(avg_daily_volume=20_000.0),
    )
    assert result.amount_usd == 75.0
    assert result.quantity == 300
    _assert_warnings(result.warnings, 1)


def test_exact_spread_threshold_has_no_penalty_or_warning():
    result = _sizer(spread_penalty_threshold=0.25).compute(
        price=4.0, liquidity=LiquidityInfo(bid=3.5, ask=4.5),
    )
    assert result.amount_usd == 100.0
    assert result.quantity == 25
    assert result.capped_by == ""
    _assert_warnings(result.warnings, 0)


def test_spread_penalty_never_inflates_a_positive_sub_dollar_size():
    result = _sizer(base_position_usd=0.75, spread_penalty_threshold=0.25).compute(
        price=0.125, liquidity=LiquidityInfo(bid=0.0625, ask=0.1875),
    )
    # A 50% penalty leaves $0.375; money is reported to cents, shares floor to 3.
    assert result.amount_usd == 0.38
    assert result.quantity == 3
    assert result.capped_by == "spread_penalty"
    _assert_warnings(result.warnings, 1)


def test_an_already_refused_size_does_not_add_liquidity_warnings():
    result = _sizer(spread_penalty_threshold=0.25).compute(
        portfolio_state=PortfolioState(position_count=20),
        price=0.125, liquidity=LiquidityInfo(bid=0.0625, ask=0.1875),
    )
    assert result.amount_usd == 0.0
    assert result.quantity == 0
    assert result.capped_by == "max_positions"
    _assert_warnings(result.warnings, 1)


def test_zero_minimum_does_not_add_an_unaffordable_warning_after_another_refusal():
    result = _sizer(min_position_usd=0.0).compute(
        portfolio_state=PortfolioState(position_count=20), price=10.0,
    )
    assert result.amount_usd == 0.0
    assert result.quantity == 0
    assert result.capped_by == "max_positions"
    _assert_warnings(result.warnings, 1)


@pytest.mark.parametrize(
    "price,amount,bid_size,ask_size,warnings",
    [
        (10.0, 30.0, 0.0, 2.0, 1),
        (10.0, 20.0, 1.0, 10.0, 1),
        (10.0, 20.0, 2.0, 1.0, 0),
        (0.25, 2.0, 4.0, 100.0, 1),
        (0.25, 0.75, 2.0, 100.0, 1),
        (0.25, 0.25, 1.0, 100.0, 0),
    ],
    ids=["ask-fallback", "one-share-bid", "exact-depth", "penny-price",
         "sub-dollar-size", "one-share-order"],
)
def test_book_depth_warning_uses_positive_bid_else_ask_and_strict_excess(
    price, amount, bid_size, ask_size, warnings,
):
    result = _sizer(base_position_usd=amount).compute(
        price=price,
        liquidity=LiquidityInfo(bid=price, ask=price, bid_size=bid_size, ask_size=ask_size),
    )
    assert result.amount_usd == amount
    _assert_warnings(result.warnings, warnings)


def test_sub_dollar_liquidity_remainder_below_minimum_is_refused():
    result = _sizer(base_position_usd=2.0, min_position_usd=1.0).compute(
        price=0.25, liquidity=LiquidityInfo(avg_daily_volume=100.0),
    )
    # ADV permits two shares ($0.50), below the operator's $1 minimum.
    assert result.amount_usd == 0.0
    assert result.quantity == 0
    assert result.capped_by == "below_minimum_after_constraints"
    _assert_warnings(result.warnings, 1)


@pytest.mark.parametrize("price,quantity,warnings", [(2.0, 0, 1), (1.0, 1, 0), (0.5, 2, 0)])
def test_unaffordable_warning_includes_minimum_and_excludes_affordable_shares(
    price, quantity, warnings,
):
    result = _sizer(base_position_usd=1.0, min_position_usd=1.0).compute(price=price)
    assert result.amount_usd == 1.0
    assert result.quantity == quantity
    _assert_warnings(result.warnings, warnings)


def test_numeric_yaml_values_feed_cent_precision_and_whole_share_quantity(tmp_path):
    config_path = tmp_path / "position_sizing.yaml"
    config_path.write_text(
        'base_position_usd: "123.456"\n'
        'min_position_usd: "0.01"\n'
        'min_confidence_scale: "1.0"\n'
        'max_positions: "7"\n'
        'volatility_adjustment: false\n'
    )
    config = PositionSizingConfig.load(str(config_path))
    result = PositionSizer(config).compute(price=10.0)
    assert config.max_positions == 7
    assert isinstance(config.max_positions, int)
    assert result.amount_usd == 123.46
    assert isinstance(result.amount_usd, float)
    assert result.quantity == 12


def test_default_session_summary_exposes_the_public_fields_and_confidence_examples():
    summary = PositionSizer(PositionSizingConfig()).session_summary()
    assert summary["config"] == {
        "min_position_usd": 500.0,
        "max_position_usd": 25_000.0,
        "max_position_pct": 0.1,
        "max_total_exposure_pct": 0.8,
        "max_positions": 20,
        "base_position_usd": 5000.0,
        "base_position_pct": 0.0,
        "effective_base_usd": 5000.0,
        "risk_level": "moderate",
        "daily_loss_limit_usd": 2000.0,
        "min_confidence_scale": 0.3,
        "volatility_adjustment": True,
        "reference_atr_pct": 0.02,
        "vol_scale_min": 0.25,
        "vol_scale_max": 2.0,
        "max_adv_pct": 0.02,
        "spread_penalty_threshold": 0.005,
        "spread_penalty_factor": 0.5,
    }
    assert summary["capacity"] == {"remaining_usd": 0.0, "remaining_positions": 20}
    assert set(summary["recommended_sizes"]) == {
        "high_confidence", "medium_confidence", "low_confidence",
    }
    for label, amount in [
        ("high_confidence", 4650.0), ("medium_confidence", 3250.0), ("low_confidence", 2200.0),
    ]:
        row = summary["recommended_sizes"][label]
        assert set(row) == {"amount_usd", "reasoning", "capped_by"}
        assert row["amount_usd"] == amount
        assert row["capped_by"] == ""
        assert isinstance(row["reasoning"], str) and row["reasoning"].strip()
    _assert_warnings(summary["warnings"], 0)


@pytest.mark.parametrize(
    "state_values,amount,cap",
    [
        ({"position_count": 20}, 0.0, "max_positions"),
        ({"daily_pnl": -2000.0}, 0.0, "daily_loss_limit"),
        (
            {"net_liquidation": 10_000.0, "gross_position_value": 7000.0,
             "pending_proposal_value": 1000.0},
            0.0,
            "max_total_exposure_pct",
        ),
        ({"net_liquidation": 10_000.0}, 1000.0, "max_position_pct"),
    ],
    ids=["positions", "daily-loss", "pending-capacity", "position-cap"],
)
def test_every_confidence_recommendation_uses_the_supplied_account_state(state_values, amount, cap):
    summary = PositionSizer(PositionSizingConfig()).session_summary(PortfolioState(**state_values))
    for row in summary["recommended_sizes"].values():
        assert row["amount_usd"] == amount
        assert row["capped_by"] == cap
        assert isinstance(row["reasoning"], str) and row["reasoning"].strip()


def test_session_preserves_portfolio_values_pending_capacity_and_display_precision():
    state = PortfolioState(
        net_liquidation=10_000.0,
        gross_position_value=1234.567,
        available_funds=321.123,
        daily_pnl=34.56,
        position_count=2,
        pending_proposal_value=234.567,
    )
    summary = PositionSizer(PositionSizingConfig()).session_summary(state)
    assert summary["portfolio"] == {
        "net_liquidation": 10_000.0,
        "gross_position_value": 1234.567,
        "available_funds": 321.123,
        "daily_pnl": 34.56,
        "position_count": 2,
        "pending_proposal_value": 234.567,
        "exposure_pct": 0.1235,
        "rpc_errors": [],
    }
    # $8,000 exposure allowance less $1,469.134 already held or proposed.
    assert summary["capacity"] == {"remaining_usd": 6530.87, "remaining_positions": 18}


@pytest.mark.parametrize("gross,position_count", [(800.0, 20), (900.0, 21)])
def test_exhausted_session_capacity_and_slots_remain_zero(gross, position_count):
    summary = PositionSizer(PositionSizingConfig()).session_summary(
        PortfolioState(net_liquidation=1000.0, gross_position_value=gross, position_count=position_count),
    )
    assert summary["capacity"] == {"remaining_usd": 0.0, "remaining_positions": 0}


def test_session_reports_capacity_of_a_positive_sub_dollar_account():
    summary = PositionSizer(PositionSizingConfig()).session_summary(
        PortfolioState(net_liquidation=0.5, gross_position_value=0.125, pending_proposal_value=0.0625),
    )
    assert summary["capacity"] == {"remaining_usd": 0.21, "remaining_positions": 20}


@pytest.mark.parametrize(
    "base_pct,net_liquidation,expected_base",
    [(0.0, 10_000.0, 5000.0), (0.05, 10_000.0, 500.0),
     (0.5, 0.5, 0.25), (0.05, 0.0, 5000.0), (0.0, 0.0, 5000.0)],
)
def test_session_effective_base_uses_percentage_only_with_positive_account_and_setting(
    base_pct, net_liquidation, expected_base,
):
    summary = PositionSizer(PositionSizingConfig(base_position_pct=base_pct)).session_summary(
        PortfolioState(net_liquidation=net_liquidation),
    )
    assert summary["config"]["effective_base_usd"] == expected_base


@pytest.mark.parametrize(
    "net_liquidation,gross,position_count,warnings",
    [
        (10_000.0, 1000.0, 0, 0),
        (10_000.0, 1000.5, 0, 1),
        (10_000.0, 1001.0, 1, 0),
        (10_000.0, 1001.0, 2, 0),
        (10_000.0, 0.0, 0, 0),
        (0.0, 1001.0, 0, 0),
        # The positive tiny account has both over-exposure and missing-positions warnings.
        (0.5, 1001.0, 0, 2),
    ],
)
def test_session_feed_warning_requires_positive_account_material_value_and_no_positions(
    net_liquidation, gross, position_count, warnings,
):
    summary = _sizer().session_summary(
        PortfolioState(
            net_liquidation=net_liquidation, gross_position_value=gross, position_count=position_count,
        ),
    )
    _assert_warnings(summary["warnings"], warnings)
