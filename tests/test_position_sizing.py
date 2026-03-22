"""Unit tests for trader.trading.position_sizing."""

import os
import tempfile

import pytest
import yaml

from trader.trading.position_sizing import (
    LiquidityInfo,
    PositionSizer,
    PositionSizingConfig,
    PortfolioState,
    RISK_MULTIPLIERS,
    SizingResult,
    VolatilityInfo,
    compute_atr,
)


class TestPositionSizingConfig:
    def test_defaults(self):
        cfg = PositionSizingConfig()
        assert cfg.min_position_usd == 500.0
        assert cfg.max_position_usd == 25_000.0
        assert cfg.max_position_pct == 0.10
        assert cfg.max_total_exposure_pct == 0.80
        assert cfg.max_positions == 20
        assert cfg.base_position_usd == 5000.0
        assert cfg.risk_level == 'moderate'
        assert cfg.daily_loss_limit_usd == 2000.0
        assert cfg.min_confidence_scale == 0.3
        assert cfg.volatility_adjustment is True
        assert cfg.reference_atr_pct == 0.02
        assert cfg.vol_scale_min == 0.25
        assert cfg.vol_scale_max == 2.0
        assert cfg.max_adv_pct == 0.02
        assert cfg.spread_penalty_threshold == 0.005
        assert cfg.spread_penalty_factor == 0.5

    def test_to_dict(self):
        cfg = PositionSizingConfig()
        d = cfg.to_dict()
        assert d['min_position_usd'] == 500.0
        assert d['risk_level'] == 'moderate'
        assert d['max_positions'] == 20

    def test_yaml_round_trip(self):
        cfg = PositionSizingConfig(
            base_position_usd=3000.0,
            risk_level='aggressive',
            max_positions=15,
        )
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            tmp_path = f.name
        try:
            cfg.save(path=tmp_path)
            loaded = PositionSizingConfig.load(path=tmp_path)
            assert loaded.base_position_usd == 3000.0
            assert loaded.risk_level == 'aggressive'
            assert loaded.max_positions == 15
            # Other fields should keep defaults
            assert loaded.min_position_usd == 500.0
            assert loaded.max_position_usd == 25_000.0
        finally:
            os.unlink(tmp_path)

    def test_load_missing_file_returns_defaults(self):
        cfg = PositionSizingConfig.load(path='/tmp/nonexistent_sizing_config.yaml')
        assert cfg.base_position_usd == 5000.0
        assert cfg.risk_level == 'moderate'

    def test_load_partial_yaml(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'base_position_usd': 8000.0}, f)
            tmp_path = f.name
        try:
            cfg = PositionSizingConfig.load(path=tmp_path)
            assert cfg.base_position_usd == 8000.0
            assert cfg.risk_level == 'moderate'  # default
        finally:
            os.unlink(tmp_path)


class TestPortfolioState:
    def test_defaults(self):
        state = PortfolioState()
        assert state.net_liquidation == 0.0
        assert state.position_count == 0

    def test_current_exposure_pct(self):
        state = PortfolioState(net_liquidation=100_000, gross_position_value=50_000)
        assert state.current_exposure_pct == 0.5

    def test_current_exposure_pct_zero_net_liq(self):
        state = PortfolioState(net_liquidation=0, gross_position_value=50_000)
        assert state.current_exposure_pct == 0.0

    def test_remaining_capacity(self):
        state = PortfolioState(net_liquidation=100_000, gross_position_value=60_000)
        # Default max_exposure = 80% => $80k, deployed $60k => $20k remaining
        assert state.remaining_capacity_usd == 20_000.0


class TestConfidenceScaling:
    def test_zero_confidence(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0, min_confidence_scale=0.3)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.0)
        # base * risk_mult(1.0) * scale(0.3) = 5000 * 0.3 = 1500
        assert result.amount_usd == 1500.0

    def test_full_confidence(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0, min_confidence_scale=0.3)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0)
        # base * risk_mult(1.0) * scale(1.0) = 5000
        assert result.amount_usd == 5000.0

    def test_half_confidence(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0, min_confidence_scale=0.3)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.5)
        # scale = 0.3 + 0.7 * 0.5 = 0.65
        # 5000 * 0.65 = 3250
        assert result.amount_usd == 3250.0

    def test_confidence_clamped_above_one(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.5)
        # Clamped to 1.0
        assert result.amount_usd == 5000.0

    def test_confidence_clamped_below_zero(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0, min_confidence_scale=0.3)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=-0.5)
        # Clamped to 0.0 => scale = 0.3
        assert result.amount_usd == 1500.0


class TestRiskMultiplier:
    def test_conservative(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0, risk_level='conservative')
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0)
        # 5000 * 0.5 = 2500
        assert result.amount_usd == 2500.0

    def test_moderate(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0, risk_level='moderate')
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0)
        assert result.amount_usd == 5000.0

    def test_aggressive(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0, risk_level='aggressive')
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0)
        # 5000 * 1.5 = 7500
        assert result.amount_usd == 7500.0

    def test_unknown_risk_level_defaults_to_1x(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0, risk_level='yolo')
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0)
        assert result.amount_usd == 5000.0


class TestHardLimits:
    def test_clamp_to_min(self):
        cfg = PositionSizingConfig(
            base_position_usd=200.0,
            min_position_usd=500.0,
            min_confidence_scale=1.0,
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0)
        assert result.amount_usd == 500.0
        assert result.capped_by == 'min_position_usd'

    def test_clamp_to_max(self):
        cfg = PositionSizingConfig(
            base_position_usd=50_000.0,
            max_position_usd=25_000.0,
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0)
        assert result.amount_usd == 25_000.0
        assert result.capped_by == 'max_position_usd'


class TestPortfolioConstraints:
    def test_max_position_pct(self):
        cfg = PositionSizingConfig(
            base_position_usd=20_000.0,
            max_position_pct=0.10,
            max_position_usd=25_000.0,
        )
        state = PortfolioState(net_liquidation=100_000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, portfolio_state=state)
        # 10% of $100k = $10k, less than $20k base
        assert result.amount_usd == 10_000.0
        assert result.capped_by == 'max_position_pct'

    def test_max_total_exposure(self):
        cfg = PositionSizingConfig(
            base_position_usd=10_000.0,
            max_total_exposure_pct=0.80,
            max_position_pct=0.50,  # high so it doesn't interfere
            max_position_usd=25_000.0,
        )
        state = PortfolioState(
            net_liquidation=100_000.0,
            gross_position_value=75_000.0,  # 75% deployed
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, portfolio_state=state)
        # Max total = 80k, deployed 75k, remaining 5k
        assert result.amount_usd == 5_000.0
        assert result.capped_by == 'max_total_exposure_pct'

    def test_exposure_includes_pending_proposals(self):
        cfg = PositionSizingConfig(
            base_position_usd=10_000.0,
            max_total_exposure_pct=0.80,
            max_position_pct=0.50,
            max_position_usd=25_000.0,
        )
        state = PortfolioState(
            net_liquidation=100_000.0,
            gross_position_value=70_000.0,
            pending_proposal_value=8_000.0,  # 70k + 8k = 78k
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, portfolio_state=state)
        # Max total = 80k, deployed+pending = 78k, remaining 2k
        assert result.amount_usd == 2_000.0

    def test_max_positions(self):
        cfg = PositionSizingConfig(max_positions=20)
        state = PortfolioState(position_count=20)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.9, portfolio_state=state)
        assert result.amount_usd == 0.0
        assert result.capped_by == 'max_positions'
        assert any('position limit' in w for w in result.warnings)

    def test_no_portfolio_state_uses_defaults(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.8)
        # Without portfolio state, no portfolio constraints apply
        # scale = 0.3 + 0.7 * 0.8 = 0.86
        # 5000 * 0.86 = 4300
        assert result.amount_usd == 4300.0
        assert result.capped_by == ''


class TestDailyLossLimit:
    def test_daily_loss_exceeded(self):
        cfg = PositionSizingConfig(daily_loss_limit_usd=2000.0)
        state = PortfolioState(daily_pnl=-2500.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.9, portfolio_state=state)
        assert result.amount_usd == 0.0
        assert result.capped_by == 'daily_loss_limit'
        assert any('Daily loss' in w for w in result.warnings)

    def test_daily_loss_at_exact_limit(self):
        cfg = PositionSizingConfig(daily_loss_limit_usd=2000.0)
        state = PortfolioState(daily_pnl=-2000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.9, portfolio_state=state)
        assert result.amount_usd == 0.0

    def test_daily_loss_approaching_warns(self):
        cfg = PositionSizingConfig(daily_loss_limit_usd=2000.0)
        state = PortfolioState(daily_pnl=-1700.0)  # 85% of limit
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.9, portfolio_state=state)
        assert result.amount_usd > 0  # still allows trading
        assert any('approaching' in w for w in result.warnings)

    def test_positive_pnl_no_warning(self):
        cfg = PositionSizingConfig(daily_loss_limit_usd=2000.0)
        state = PortfolioState(daily_pnl=500.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.9, portfolio_state=state)
        assert result.amount_usd > 0
        assert not any('Daily loss' in w for w in result.warnings)


class TestQuantityComputation:
    def test_quantity_from_price(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, price=100.0)
        assert result.quantity == 50  # $5000 / $100 = 50 shares

    def test_quantity_truncated(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, price=33.0)
        assert result.quantity == 151  # int(5000/33) = 151

    def test_quantity_zero_when_no_price(self):
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, price=0.0)
        assert result.quantity == 0

    def test_quantity_warning_when_price_too_high(self):
        cfg = PositionSizingConfig(base_position_usd=500.0, min_position_usd=500.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.0, price=5000.0)
        # At confidence 0: 500 * 0.3 = 150, below min, so 500
        assert result.quantity == 0


class TestSessionSummary:
    def test_structure(self):
        cfg = PositionSizingConfig()
        sizer = PositionSizer(cfg)
        state = PortfolioState(
            net_liquidation=100_000.0,
            gross_position_value=40_000.0,
            available_funds=60_000.0,
            daily_pnl=-500.0,
            position_count=5,
        )
        summary = sizer.session_summary(state)

        # Top-level keys
        assert 'config' in summary
        assert 'portfolio' in summary
        assert 'capacity' in summary
        assert 'recommended_sizes' in summary
        assert 'warnings' in summary

        # Config section
        assert summary['config']['base_position_usd'] == 5000.0
        assert summary['config']['risk_level'] == 'moderate'
        assert summary['config']['max_positions'] == 20
        assert summary['config']['volatility_adjustment'] is True
        assert summary['config']['reference_atr_pct'] == 0.02
        assert summary['config']['vol_scale_min'] == 0.25
        assert summary['config']['vol_scale_max'] == 2.0

        # Portfolio section
        assert summary['portfolio']['net_liquidation'] == 100_000.0
        assert summary['portfolio']['position_count'] == 5
        assert summary['portfolio']['exposure_pct'] == 0.4

        # Capacity
        assert summary['capacity']['remaining_positions'] == 15
        assert summary['capacity']['remaining_usd'] == 40_000.0  # 80k - 40k

        # Recommended sizes
        assert summary['recommended_sizes']['high_confidence']['amount_usd'] > 0
        assert summary['recommended_sizes']['medium_confidence']['amount_usd'] > 0
        assert summary['recommended_sizes']['low_confidence']['amount_usd'] > 0

        # High > medium > low
        assert (summary['recommended_sizes']['high_confidence']['amount_usd']
                >= summary['recommended_sizes']['medium_confidence']['amount_usd']
                >= summary['recommended_sizes']['low_confidence']['amount_usd'])

    def test_summary_without_portfolio(self):
        cfg = PositionSizingConfig()
        sizer = PositionSizer(cfg)
        summary = sizer.session_summary()
        assert summary['portfolio']['net_liquidation'] == 0.0
        assert summary['capacity']['remaining_usd'] == 0.0
        assert summary['capacity']['remaining_positions'] == 20


class TestSizingResult:
    def test_defaults(self):
        r = SizingResult(amount_usd=5000.0)
        assert r.amount_usd == 5000.0
        assert r.quantity == 0
        assert r.reasoning == ''
        assert r.warnings == []
        assert r.capped_by == ''

    def test_reasoning_populated(self):
        cfg = PositionSizingConfig()
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.7)
        assert 'base' in result.reasoning
        assert 'confidence' in result.reasoning


class TestLiquidityInfo:
    def test_spread(self):
        liq = LiquidityInfo(bid=100.0, ask=100.50)
        assert liq.spread == 0.50

    def test_spread_pct(self):
        liq = LiquidityInfo(bid=100.0, ask=101.0)
        # mid = 100.50, spread = 1.0, pct = 1.0/100.50 ≈ 0.00995
        assert abs(liq.spread_pct - 1.0 / 100.5) < 1e-6

    def test_spread_zero_when_no_bid_ask(self):
        liq = LiquidityInfo(last=100.0)
        assert liq.spread == 0.0
        assert liq.spread_pct == 0.0

    def test_mid_price(self):
        liq = LiquidityInfo(bid=99.0, ask=101.0)
        assert liq.mid_price == 100.0

    def test_mid_price_falls_back_to_last(self):
        liq = LiquidityInfo(last=100.0)
        assert liq.mid_price == 100.0

    def test_has_data(self):
        assert LiquidityInfo().has_data is False
        assert LiquidityInfo(bid=99.0, ask=101.0).has_data is True
        assert LiquidityInfo(avg_daily_volume=1_000_000).has_data is True


class TestADVCap:
    def test_adv_caps_position(self):
        """Position capped to max_adv_pct of average daily volume."""
        cfg = PositionSizingConfig(
            base_position_usd=10_000.0,
            max_adv_pct=0.02,  # 2% of ADV
            max_position_usd=25_000.0,
        )
        liq = LiquidityInfo(
            avg_daily_volume=100_000,  # 100k shares/day
            bid=50.0, ask=50.10, last=50.05,
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, liquidity=liq)
        # 2% of 100k = 2000 shares, at ~$50 mid = $100k max
        # base $10k < $100k, so ADV doesn't cap here
        assert result.amount_usd == 10_000.0

    def test_adv_caps_large_position(self):
        """Large position capped by thin ADV."""
        cfg = PositionSizingConfig(
            base_position_usd=20_000.0,
            max_adv_pct=0.01,  # 1% of ADV
            max_position_usd=25_000.0,
        )
        liq = LiquidityInfo(
            avg_daily_volume=10_000,  # only 10k shares/day — thin stock
            bid=100.0, ask=100.10, last=100.05,
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, liquidity=liq)
        # 1% of 10k = 100 shares, at ~$100 mid = $10,005 max
        # base $20k > $10,005, so ADV caps it
        assert result.capped_by == 'adv_liquidity'
        assert result.amount_usd < 20_000.0
        # Should be roughly $10k (100 shares * $100.05)
        assert abs(result.amount_usd - 100 * 100.05) < 1.0

    def test_adv_warning_when_approaching_limit(self):
        """Warn when position is >50% of ADV limit."""
        cfg = PositionSizingConfig(
            base_position_usd=5000.0,
            max_adv_pct=0.02,
        )
        liq = LiquidityInfo(
            avg_daily_volume=50_000,  # 50k shares/day
            bid=50.0, ask=50.10, last=50.05,
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, liquidity=liq)
        # 2% of 50k = 1000 shares => $50k max. $5k = 100 shares = 0.2% of ADV
        # 0.2% < 1% (50% of 2% limit), so no warning
        assert not any('ADV' in w for w in result.warnings)

    def test_adv_no_cap_without_volume_data(self):
        """Without ADV data, no ADV constraint applied."""
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        liq = LiquidityInfo(bid=50.0, ask=50.10)  # has spread but no ADV
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, liquidity=liq)
        assert result.amount_usd == 5000.0


class TestSpreadPenalty:
    def test_no_penalty_below_threshold(self):
        """No penalty when spread is below threshold."""
        cfg = PositionSizingConfig(
            base_position_usd=5000.0,
            spread_penalty_threshold=0.005,  # 0.5%
        )
        liq = LiquidityInfo(bid=100.0, ask=100.20)  # 0.2% spread, well below 0.5%
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, liquidity=liq)
        assert result.amount_usd == 5000.0

    def test_penalty_above_threshold(self):
        """Size reduced when spread exceeds threshold."""
        cfg = PositionSizingConfig(
            base_position_usd=5000.0,
            spread_penalty_threshold=0.005,  # 0.5%
            spread_penalty_factor=0.5,       # max 50% reduction
        )
        # 2% spread — 4x the threshold
        liq = LiquidityInfo(bid=100.0, ask=102.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, liquidity=liq)
        assert result.amount_usd < 5000.0
        assert any('spread' in w.lower() for w in result.warnings)

    def test_penalty_capped_at_factor(self):
        """Spread penalty doesn't exceed spread_penalty_factor."""
        cfg = PositionSizingConfig(
            base_position_usd=5000.0,
            spread_penalty_threshold=0.005,
            spread_penalty_factor=0.5,       # max 50% reduction
        )
        # 10% spread — extremely illiquid
        liq = LiquidityInfo(bid=100.0, ask=110.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, liquidity=liq)
        # Should not go below 50% of $5000 = $2500
        assert result.amount_usd >= 2500.0

    def test_at_exact_threshold_no_penalty(self):
        """At exactly the threshold, penalty is zero (ratio = 1.0, penalty = factor * 0)."""
        cfg = PositionSizingConfig(
            base_position_usd=5000.0,
            spread_penalty_threshold=0.01,  # 1%
            spread_penalty_factor=0.5,
        )
        # Exactly 1% spread
        liq = LiquidityInfo(bid=100.0, ask=101.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, liquidity=liq)
        # spread_pct ≈ 0.00995, which is just below threshold of 0.01
        # So no penalty should apply
        assert result.amount_usd == 5000.0


class TestBookDepth:
    def test_warns_when_order_exceeds_book(self):
        """Warn when sized shares exceed top-of-book depth."""
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        liq = LiquidityInfo(
            bid=50.0, ask=50.10, last=50.05,
            bid_size=50,  # only 50 shares on the bid
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, price=50.0, liquidity=liq)
        # $5000 / $50 = 100 shares > 50 on book
        assert any('slippage' in w for w in result.warnings)

    def test_no_warning_when_within_book(self):
        """No warning when order fits within book depth."""
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        liq = LiquidityInfo(
            bid=50.0, ask=50.10, last=50.05,
            bid_size=500,
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, price=50.0, liquidity=liq)
        # 100 shares < 500 on book
        assert not any('slippage' in w for w in result.warnings)

    def test_no_warning_without_book_data(self):
        """No warning when book depth is unknown."""
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        liq = LiquidityInfo(bid=50.0, ask=50.10)  # no bid_size/ask_size
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, price=50.0, liquidity=liq)
        assert not any('slippage' in w for w in result.warnings)


class TestLiquidityWithPortfolio:
    def test_adv_and_portfolio_constraints_combined(self):
        """Both ADV and portfolio constraints apply — strictest wins."""
        cfg = PositionSizingConfig(
            base_position_usd=20_000.0,
            max_position_pct=0.10,
            max_adv_pct=0.01,
            max_position_usd=25_000.0,
        )
        state = PortfolioState(net_liquidation=100_000.0)
        liq = LiquidityInfo(
            avg_daily_volume=5_000,  # very thin
            bid=100.0, ask=100.10, last=100.05,
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, portfolio_state=state, liquidity=liq)
        # Portfolio cap: 10% of $100k = $10k
        # ADV cap: 1% of 5k shares * $100 = $5k
        # ADV is stricter
        assert result.capped_by == 'adv_liquidity'
        assert result.amount_usd < 10_000.0

    def test_no_liquidity_info_skips_checks(self):
        """Without LiquidityInfo, sizing works exactly as before."""
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0)
        assert result.amount_usd == 5000.0
        assert not any('ADV' in w or 'spread' in w or 'slippage' in w
                        for w in result.warnings)

    def test_uses_mid_price_when_no_explicit_price(self):
        """When price=0, uses liquidity mid price for quantity calc."""
        cfg = PositionSizingConfig(base_position_usd=5000.0)
        liq = LiquidityInfo(bid=99.95, ask=100.05)  # tight spread, mid ≈ $100
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, price=0.0, liquidity=liq)
        # mid = 100, $5000/100 = 50 shares
        assert result.quantity == 50


class TestBasePositionPct:
    def test_base_position_pct_derives_from_net_liq(self):
        """pct=0.03, net_liq=$1M -> base $30k, confidence=1.0 -> $30k (capped to max)."""
        cfg = PositionSizingConfig(
            base_position_pct=0.03,
            base_position_usd=5000.0,
            max_position_usd=50_000.0,  # raise max so it doesn't interfere
        )
        state = PortfolioState(net_liquidation=1_000_000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, portfolio_state=state)
        # 3% of $1M = $30,000
        assert result.amount_usd == 30_000.0
        assert '3.0%' in result.reasoning
        assert '$1,000,000' in result.reasoning

    def test_base_position_pct_zero_uses_fixed(self):
        """pct=0.0 falls back to base_position_usd."""
        cfg = PositionSizingConfig(
            base_position_pct=0.0,
            base_position_usd=5000.0,
        )
        state = PortfolioState(net_liquidation=1_000_000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, portfolio_state=state)
        assert result.amount_usd == 5000.0

    def test_base_position_pct_no_portfolio_uses_fixed(self):
        """pct=0.03 but no portfolio data -> uses base_position_usd."""
        cfg = PositionSizingConfig(
            base_position_pct=0.03,
            base_position_usd=5000.0,
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0)
        # No portfolio state -> net_liq=0 -> falls back to fixed base
        assert result.amount_usd == 5000.0

    def test_base_position_pct_with_confidence_scaling(self):
        """pct=0.03, net_liq=$1M, confidence=0.5 -> scaled down."""
        cfg = PositionSizingConfig(
            base_position_pct=0.03,
            base_position_usd=5000.0,
            min_confidence_scale=0.3,
            max_position_usd=50_000.0,
        )
        state = PortfolioState(net_liquidation=1_000_000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=0.5, portfolio_state=state)
        # base = 30000, scale = 0.3 + 0.7*0.5 = 0.65, sized = 30000 * 0.65 = 19500
        assert result.amount_usd == 19_500.0

    def test_base_position_pct_clamped_by_hard_limits(self):
        """Derived $30k capped to max_position_usd $25k."""
        cfg = PositionSizingConfig(
            base_position_pct=0.03,
            base_position_usd=5000.0,
            max_position_usd=25_000.0,
        )
        state = PortfolioState(net_liquidation=1_000_000.0)
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0, portfolio_state=state)
        # 3% of $1M = $30k, capped to max $25k
        assert result.amount_usd == 25_000.0
        assert result.capped_by == 'max_position_usd'

    def test_base_position_pct_yaml_round_trip(self):
        """Save/load preserves base_position_pct field."""
        cfg = PositionSizingConfig(
            base_position_pct=0.03,
            base_position_usd=5000.0,
        )
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            tmp_path = f.name
        try:
            cfg.save(path=tmp_path)
            loaded = PositionSizingConfig.load(path=tmp_path)
            assert loaded.base_position_pct == 0.03
            assert loaded.base_position_usd == 5000.0
        finally:
            os.unlink(tmp_path)


class TestComputeATR:
    def test_basic_atr(self):
        """ATR computed correctly for simple data."""
        # 16 bars: need period+1 = 15 bars minimum
        highs = [10.0 + i * 0.1 for i in range(16)]
        lows = [9.0 + i * 0.1 for i in range(16)]
        closes = [9.5 + i * 0.1 for i in range(16)]
        result = compute_atr(highs, lows, closes, period=14)
        assert result is not None
        assert result > 0

    def test_insufficient_data_returns_none(self):
        """Less than period+1 bars returns None."""
        highs = [10.0, 11.0, 12.0]
        lows = [9.0, 10.0, 11.0]
        closes = [9.5, 10.5, 11.5]
        result = compute_atr(highs, lows, closes, period=14)
        assert result is None

    def test_mismatched_lengths(self):
        highs = [10.0] * 20
        lows = [9.0] * 19
        closes = [9.5] * 20
        assert compute_atr(highs, lows, closes) is None

    def test_constant_price_zero_atr(self):
        """Flat price → ATR = H-L only (no gaps)."""
        n = 20
        highs = [100.5] * n
        lows = [99.5] * n
        closes = [100.0] * n
        result = compute_atr(highs, lows, closes)
        assert result is not None
        assert abs(result - 1.0) < 0.01  # TR = 100.5 - 99.5 = 1.0

    def test_gap_up_increases_atr(self):
        """Gap-up creates larger true range."""
        n = 20
        highs = [100.0] * n
        lows = [99.0] * n
        closes = [99.5] * n
        # Force a gap: previous close at 95, high at 100 → |H - prevC| = 5
        closes[5] = 95.0
        result = compute_atr(highs, lows, closes)
        assert result is not None
        assert result > 1.0  # bigger than the simple H-L range


class TestVolatilityInfo:
    def test_atr_pct(self):
        v = VolatilityInfo(atr=2.0, price=100.0)
        assert abs(v.atr_pct - 0.02) < 1e-6

    def test_atr_pct_zero_price(self):
        v = VolatilityInfo(atr=2.0, price=0.0)
        assert v.atr_pct == 0.0

    def test_atr_pct_zero_atr(self):
        v = VolatilityInfo(atr=0.0, price=100.0)
        assert v.atr_pct == 0.0


class TestVolatilityAdjustment:
    def test_normal_volatility_no_adjustment(self):
        """ATR% matching reference → multiplier 1.0."""
        cfg = PositionSizingConfig(
            base_position_usd=5000.0,
            volatility_adjustment=True,
            reference_atr_pct=0.02,
        )
        sizer = PositionSizer(cfg)
        vol = VolatilityInfo(atr=2.0, price=100.0)  # atr_pct = 0.02
        result = sizer.compute(confidence=1.0, volatility=vol)
        assert result.amount_usd == 5000.0

    def test_high_volatility_reduces_size(self):
        """4% ATR (2x reference) → multiplier 0.5."""
        cfg = PositionSizingConfig(
            base_position_usd=10000.0,
            volatility_adjustment=True,
            reference_atr_pct=0.02,
            vol_scale_min=0.25,
            vol_scale_max=2.0,
        )
        sizer = PositionSizer(cfg)
        vol = VolatilityInfo(atr=4.0, price=100.0)  # atr_pct = 0.04 = 2x reference
        result = sizer.compute(confidence=1.0, volatility=vol)
        # multiplier = 0.02 / 0.04 = 0.5
        assert result.amount_usd == 5000.0
        assert 'vol ATR' in result.reasoning

    def test_low_volatility_increases_size(self):
        """1% ATR (0.5x reference) → multiplier 2.0 (capped by vol_scale_max)."""
        cfg = PositionSizingConfig(
            base_position_usd=5000.0,
            volatility_adjustment=True,
            reference_atr_pct=0.02,
            vol_scale_min=0.25,
            vol_scale_max=2.0,
        )
        sizer = PositionSizer(cfg)
        vol = VolatilityInfo(atr=1.0, price=100.0)  # atr_pct = 0.01 = 0.5x reference
        result = sizer.compute(confidence=1.0, volatility=vol)
        # multiplier = 0.02 / 0.01 = 2.0 (at max)
        assert result.amount_usd == 10000.0

    def test_very_volatile_capped_at_min(self):
        """Extremely high ATR → multiplier floored at vol_scale_min."""
        cfg = PositionSizingConfig(
            base_position_usd=10000.0,
            volatility_adjustment=True,
            reference_atr_pct=0.02,
            vol_scale_min=0.25,
            vol_scale_max=2.0,
        )
        sizer = PositionSizer(cfg)
        vol = VolatilityInfo(atr=20.0, price=100.0)  # atr_pct = 0.20 = 10x reference
        result = sizer.compute(confidence=1.0, volatility=vol)
        # multiplier = 0.02 / 0.20 = 0.1, clamped to 0.25
        assert result.amount_usd == 2500.0

    def test_very_stable_capped_at_max(self):
        """Very low ATR → multiplier capped at vol_scale_max."""
        cfg = PositionSizingConfig(
            base_position_usd=5000.0,
            volatility_adjustment=True,
            reference_atr_pct=0.02,
            vol_scale_min=0.25,
            vol_scale_max=2.0,
        )
        sizer = PositionSizer(cfg)
        vol = VolatilityInfo(atr=0.1, price=100.0)  # atr_pct = 0.001
        result = sizer.compute(confidence=1.0, volatility=vol)
        # multiplier = 0.02 / 0.001 = 20.0, clamped to 2.0
        assert result.amount_usd == 10000.0

    def test_volatility_disabled(self):
        """volatility_adjustment=False → no adjustment."""
        cfg = PositionSizingConfig(
            base_position_usd=5000.0,
            volatility_adjustment=False,
        )
        sizer = PositionSizer(cfg)
        vol = VolatilityInfo(atr=10.0, price=100.0)
        result = sizer.compute(confidence=1.0, volatility=vol)
        assert result.amount_usd == 5000.0

    def test_no_volatility_data_no_adjustment(self):
        """No VolatilityInfo → multiplier 1.0."""
        cfg = PositionSizingConfig(
            base_position_usd=5000.0,
            volatility_adjustment=True,
        )
        sizer = PositionSizer(cfg)
        result = sizer.compute(confidence=1.0)
        assert result.amount_usd == 5000.0
        assert 'no volatility data' in result.reasoning

    def test_volatility_with_confidence_scaling(self):
        """Confidence and volatility both apply multiplicatively."""
        cfg = PositionSizingConfig(
            base_position_usd=10000.0,
            min_confidence_scale=0.3,
            volatility_adjustment=True,
            reference_atr_pct=0.02,
        )
        sizer = PositionSizer(cfg)
        vol = VolatilityInfo(atr=4.0, price=100.0)  # atr_pct = 0.04, mult = 0.5
        result = sizer.compute(confidence=0.5, volatility=vol)
        # base=10000, confidence_scale = 0.3 + 0.7*0.5 = 0.65
        # after confidence: 10000 * 0.65 = 6500
        # after vol: 6500 * 0.5 = 3250
        assert result.amount_usd == 3250.0

    def test_volatility_yaml_round_trip(self):
        """Volatility config fields survive YAML save/load."""
        cfg = PositionSizingConfig(
            volatility_adjustment=True,
            reference_atr_pct=0.03,
            vol_scale_min=0.3,
            vol_scale_max=1.5,
        )
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            tmp_path = f.name
        try:
            cfg.save(path=tmp_path)
            loaded = PositionSizingConfig.load(path=tmp_path)
            assert loaded.volatility_adjustment is True
            assert loaded.reference_atr_pct == 0.03
            assert loaded.vol_scale_min == 0.3
            assert loaded.vol_scale_max == 1.5
        finally:
            os.unlink(tmp_path)

    def test_volatility_then_portfolio_cap(self):
        """Volatility adjustment increases size, but max_position_pct caps it."""
        cfg = PositionSizingConfig(
            base_position_usd=10_000.0,
            volatility_adjustment=True,
            reference_atr_pct=0.02,
            vol_scale_max=2.0,
            max_position_pct=0.10,
            max_position_usd=50_000.0,
        )
        sizer = PositionSizer(cfg)
        # Very stable stock → vol_mult 2.0x → $20k, but portfolio cap = 10% of $100k = $10k
        vol = VolatilityInfo(atr=0.5, price=100.0)  # atr_pct = 0.005 → mult capped at 2.0
        state = PortfolioState(net_liquidation=100_000.0)
        result = sizer.compute(confidence=1.0, portfolio_state=state, volatility=vol)
        # After vol: 10000 * 2.0 = 20000, after portfolio cap: 10000
        assert result.amount_usd == 10_000.0
        assert result.capped_by == 'max_position_pct'

    def test_volatility_then_adv_cap(self):
        """Volatility adjustment + ADV cap — strictest wins."""
        cfg = PositionSizingConfig(
            base_position_usd=10_000.0,
            volatility_adjustment=True,
            reference_atr_pct=0.02,
            max_adv_pct=0.01,
            max_position_usd=50_000.0,
        )
        sizer = PositionSizer(cfg)
        # Normal volatility → no vol adjustment. But thin ADV caps it.
        vol = VolatilityInfo(atr=2.0, price=100.0)  # atr_pct = 0.02, mult = 1.0
        liq = LiquidityInfo(
            avg_daily_volume=5_000,  # thin stock
            bid=100.0, ask=100.10, last=100.05,
        )
        result = sizer.compute(confidence=1.0, volatility=vol, liquidity=liq)
        # After vol: $10k * 1.0 = $10k. ADV cap: 1% of 5k * $100 = $5k
        assert result.capped_by == 'adv_liquidity'
        assert result.amount_usd < 10_000.0

    def test_high_vol_reduces_before_spread_penalty(self):
        """Volatility reduces size first, then spread penalty applies on top."""
        cfg = PositionSizingConfig(
            base_position_usd=10_000.0,
            volatility_adjustment=True,
            reference_atr_pct=0.02,
            spread_penalty_threshold=0.005,
            spread_penalty_factor=0.5,
        )
        sizer = PositionSizer(cfg)
        vol = VolatilityInfo(atr=4.0, price=100.0)  # atr_pct=0.04, mult=0.5
        liq = LiquidityInfo(bid=100.0, ask=102.0)   # 2% spread
        result = sizer.compute(confidence=1.0, volatility=vol, liquidity=liq)
        # After vol: $10k * 0.5 = $5k, then spread penalty reduces further
        assert result.amount_usd < 5_000.0
        assert any('spread' in w.lower() for w in result.warnings)


class TestComputeATREdgeCases:
    def test_custom_period(self):
        """ATR with a shorter period."""
        n = 10
        highs = [100.5 + i * 0.1 for i in range(n)]
        lows = [99.5 + i * 0.1 for i in range(n)]
        closes = [100.0 + i * 0.1 for i in range(n)]
        result = compute_atr(highs, lows, closes, period=5)
        assert result is not None
        assert result > 0

    def test_period_equals_data_minus_one(self):
        """Exactly period+1 bars → valid ATR from one window."""
        n = 15
        highs = [100.5] * n
        lows = [99.5] * n
        closes = [100.0] * n
        result = compute_atr(highs, lows, closes, period=14)
        assert result is not None
        assert abs(result - 1.0) < 0.01

    def test_empty_lists(self):
        assert compute_atr([], [], []) is None

    def test_single_bar(self):
        assert compute_atr([100.0], [99.0], [99.5]) is None


# ---------------------------------------------------------------------------
# Tests for compute_resize_deltas (portfolio resizing pure function)
# ---------------------------------------------------------------------------

from trader.sdk import compute_resize_deltas


class TestComputeResizeDeltas:
    def _make_positions(self, items):
        """Build position dicts from (symbol, conId, position, mktPrice, marketValue) tuples."""
        return [
            {
                'symbol': s, 'conId': cid, 'position': pos,
                'mktPrice': price, 'marketValue': val,
            }
            for s, cid, pos, price, val in items
        ]

    def test_scale_down_proportional(self):
        """Total $1M, max_bound $500k → factor 0.5, all deltas are ~-50%."""
        positions = self._make_positions([
            ('AAPL', 265598, 100, 250.0, 25_000),
            ('AMD', 4391, 200, 150.0, 30_000),
            ('MSFT', 272093, 50, 400.0, 20_000),
        ])
        # Total = 75_000; set max_bound = 37_500 → factor = 0.5
        factor, adjs = compute_resize_deltas(positions, max_bound=37_500, min_bound=None)
        assert factor == 0.5
        assert len(adjs) == 3

        aapl = next(a for a in adjs if a['symbol'] == 'AAPL')
        assert aapl['current_qty'] == 100
        assert aapl['target_qty'] == 50
        assert aapl['delta_qty'] == -50
        assert aapl['action'] == 'SELL'

        amd = next(a for a in adjs if a['symbol'] == 'AMD')
        assert amd['current_qty'] == 200
        assert amd['target_qty'] == 100
        assert amd['delta_qty'] == -100
        assert amd['action'] == 'SELL'

    def test_scale_up_proportional(self):
        """Total $300k, min_bound $500k → factor ~1.667, all deltas positive."""
        positions = self._make_positions([
            ('AAPL', 265598, 60, 250.0, 15_000),
            ('AMD', 4391, 100, 150.0, 15_000),
        ])
        # Total = 30_000; min_bound = 50_000 → factor = 50000/30000 ≈ 1.667
        factor, adjs = compute_resize_deltas(positions, max_bound=None, min_bound=50_000)
        assert abs(factor - 50_000 / 30_000) < 0.001
        assert len(adjs) == 2

        aapl = next(a for a in adjs if a['symbol'] == 'AAPL')
        assert aapl['target_qty'] == int(60 * factor)
        assert aapl['delta_qty'] > 0
        assert aapl['action'] == 'BUY'

        amd = next(a for a in adjs if a['symbol'] == 'AMD')
        assert amd['target_qty'] == int(100 * factor)
        assert amd['delta_qty'] > 0
        assert amd['action'] == 'BUY'

    def test_within_bounds_no_op(self):
        """Total $500k, max_bound $800k → factor 1.0, empty adjustments."""
        positions = self._make_positions([
            ('AAPL', 265598, 100, 250.0, 25_000),
        ])
        factor, adjs = compute_resize_deltas(positions, max_bound=800_000, min_bound=None)
        assert factor == 1.0
        assert adjs == []

    def test_zero_delta_skipped(self):
        """Position too small to change after rounding → skipped."""
        positions = self._make_positions([
            ('AAPL', 265598, 1, 250.0, 250),
            ('AMD', 4391, 200, 150.0, 30_000),
        ])
        # Total = 30_250; max_bound = 28_000 → factor ≈ 0.926
        # AAPL: int(1 * 0.926) = 0... delta = -1
        # Actually int(1 * 0.926) = 0, delta = -1, not skipped
        # Let's pick a factor where 1 share rounds to 1: factor > 0.5
        # Total = 30_250; max_bound = 29_000 → factor ≈ 0.959
        # AAPL: int(1 * 0.959) = 0, delta = -1
        # To get delta=0: we need int(1 * factor) == 1, so factor >= 1.0
        # Better test: 2 shares, factor=0.8 → int(2*0.8)=1, delta=-1 (not skipped)
        # 3 shares, factor=0.99 → int(3*0.99)=2, delta=-1 (not skipped)
        # Actually: to get delta=0, we need int(qty * factor) == qty
        # e.g. qty=10, factor=0.95 → int(9.5)=9, delta=-1 → not zero
        # qty=10, factor=1.04 → int(10.4)=10, delta=0 → skipped!
        positions2 = self._make_positions([
            ('AAPL', 265598, 10, 250.0, 2_500),
            ('AMD', 4391, 200, 150.0, 30_000),
        ])
        # Total = 32_500; min_bound = 33_000 → factor = 33000/32500 ≈ 1.0154
        # AAPL: int(10 * 1.0154) = 10, delta = 0 → skipped
        # AMD: int(200 * 1.0154) = 203, delta = 3 → included
        factor, adjs = compute_resize_deltas(positions2, max_bound=None, min_bound=33_000)
        assert factor > 1.0
        symbols = [a['symbol'] for a in adjs]
        assert 'AAPL' not in symbols  # delta rounds to 0
        assert 'AMD' in symbols

    def test_short_positions(self):
        """Short positions scale correctly (BUY to cover when scaling down absolute size)."""
        positions = self._make_positions([
            ('AAPL', 265598, -100, 250.0, -25_000),
        ])
        # Total abs = 25_000; max_bound = 12_500 → factor = 0.5
        factor, adjs = compute_resize_deltas(positions, max_bound=12_500, min_bound=None)
        assert factor == 0.5
        assert len(adjs) == 1
        aapl = adjs[0]
        assert aapl['current_qty'] == -100
        assert aapl['target_qty'] == -50
        assert aapl['delta_qty'] == 50  # covering 50 shares
        assert aapl['action'] == 'BUY'

    def test_both_bounds_total_exceeds_max(self):
        """max_bound and min_bound both specified, total exceeds max."""
        positions = self._make_positions([
            ('AAPL', 265598, 100, 250.0, 25_000),
            ('AMD', 4391, 100, 150.0, 15_000),
        ])
        # Total = 40_000; max_bound = 30_000, min_bound = 20_000
        factor, adjs = compute_resize_deltas(positions, max_bound=30_000, min_bound=20_000)
        assert factor == 30_000 / 40_000  # 0.75
        assert len(adjs) == 2
        for a in adjs:
            assert a['action'] == 'SELL'

    def test_both_bounds_total_below_min(self):
        """max_bound and min_bound both specified, total below min."""
        positions = self._make_positions([
            ('AAPL', 265598, 10, 250.0, 2_500),
        ])
        # Total = 2_500; max_bound = 30_000, min_bound = 5_000
        factor, adjs = compute_resize_deltas(positions, max_bound=30_000, min_bound=5_000)
        assert factor == 5_000 / 2_500  # 2.0
        assert len(adjs) == 1
        assert adjs[0]['action'] == 'BUY'

    def test_empty_positions(self):
        """No positions → factor 1.0, no adjustments."""
        factor, adjs = compute_resize_deltas([], max_bound=100_000, min_bound=None)
        assert factor == 1.0
        assert adjs == []

    def test_mixed_long_short(self):
        """Portfolio with both long and short positions scales correctly."""
        positions = self._make_positions([
            ('AAPL', 265598, 100, 250.0, 25_000),
            ('TSLA', 76792991, -50, 200.0, -10_000),
        ])
        # Total abs = 35_000; max_bound = 17_500 → factor = 0.5
        factor, adjs = compute_resize_deltas(positions, max_bound=17_500, min_bound=None)
        assert factor == 0.5
        assert len(adjs) == 2

        aapl = next(a for a in adjs if a['symbol'] == 'AAPL')
        assert aapl['action'] == 'SELL'
        assert aapl['delta_qty'] == -50

        tsla = next(a for a in adjs if a['symbol'] == 'TSLA')
        assert tsla['action'] == 'BUY'  # covering shorts
        assert tsla['delta_qty'] == 25  # -50 → -25, delta = +25
