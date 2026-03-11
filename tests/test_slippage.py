import numpy as np
import pandas as pd
import pytest

from trader.objects import Action
from trader.simulation.slippage import (
    FixedBPS,
    SquareRootImpact,
    VolatilitySlippage,
    ZeroSlippage,
    get_slippage_model,
)


def _make_bar(high=101.0, low=99.0, close=100.0, volume=10000.0):
    return pd.Series({'open': 99.5, 'high': high, 'low': low, 'close': close, 'volume': volume})


class TestZeroSlippage:
    def test_buy_returns_exact_price(self):
        model = ZeroSlippage()
        assert model.calculate(100.0, 10, Action.BUY, _make_bar()) == 100.0

    def test_sell_returns_exact_price(self):
        model = ZeroSlippage()
        assert model.calculate(100.0, 10, Action.SELL, _make_bar()) == 100.0


class TestFixedBPS:
    def test_buy_increases_price(self):
        model = FixedBPS(bps=10.0)
        fill = model.calculate(100.0, 10, Action.BUY, _make_bar())
        assert fill == pytest.approx(100.0 * 1.001)

    def test_sell_decreases_price(self):
        model = FixedBPS(bps=10.0)
        fill = model.calculate(100.0, 10, Action.SELL, _make_bar())
        assert fill == pytest.approx(100.0 / 1.001)

    def test_zero_bps_no_change(self):
        model = FixedBPS(bps=0.0)
        assert model.calculate(100.0, 10, Action.BUY, _make_bar()) == 100.0
        assert model.calculate(100.0, 10, Action.SELL, _make_bar()) == 100.0

    def test_default_bps_is_1(self):
        model = FixedBPS()
        fill = model.calculate(100.0, 10, Action.BUY, _make_bar())
        assert fill == pytest.approx(100.0 * 1.0001)


class TestSquareRootImpact:
    def test_buy_increases_price(self):
        model = SquareRootImpact(k=0.1)
        fill = model.calculate(100.0, 100, Action.BUY, _make_bar())
        assert fill > 100.0

    def test_sell_decreases_price(self):
        model = SquareRootImpact(k=0.1)
        fill = model.calculate(100.0, 100, Action.SELL, _make_bar())
        assert fill < 100.0

    def test_scales_with_quantity(self):
        model = SquareRootImpact(k=0.1)
        bar = _make_bar()
        fill_small = model.calculate(100.0, 10, Action.BUY, bar)
        fill_large = model.calculate(100.0, 1000, Action.BUY, bar)
        # Larger quantity -> more slippage
        assert fill_large > fill_small

    def test_zero_volume_returns_raw_price(self):
        model = SquareRootImpact(k=0.1)
        bar = _make_bar(volume=0)
        assert model.calculate(100.0, 100, Action.BUY, bar) == 100.0

    def test_flat_bar_returns_raw_price(self):
        model = SquareRootImpact(k=0.1)
        bar = _make_bar(high=100.0, low=100.0)
        assert model.calculate(100.0, 100, Action.BUY, bar) == 100.0

    def test_impact_formula(self):
        model = SquareRootImpact(k=0.1)
        bar = _make_bar(high=102.0, low=98.0, volume=10000.0)
        price = 100.0
        quantity = 100.0
        sigma = (102.0 - 98.0) / 100.0
        expected_impact = 0.1 * sigma * np.sqrt(100.0 / 10000.0)
        expected_fill = price * (1 + expected_impact)
        assert model.calculate(price, quantity, Action.BUY, bar) == pytest.approx(expected_fill)


class TestVolatilitySlippage:
    def test_buy_increases_price(self):
        model = VolatilitySlippage(k=0.5)
        fill = model.calculate(100.0, 10, Action.BUY, _make_bar())
        assert fill > 100.0

    def test_sell_decreases_price(self):
        model = VolatilitySlippage(k=0.5)
        fill = model.calculate(100.0, 10, Action.SELL, _make_bar())
        assert fill < 100.0

    def test_flat_bar_returns_raw_price(self):
        model = VolatilitySlippage(k=0.5)
        bar = _make_bar(high=100.0, low=100.0)
        assert model.calculate(100.0, 10, Action.BUY, bar) == 100.0

    def test_scales_with_range(self):
        model = VolatilitySlippage(k=0.5)
        bar_narrow = _make_bar(high=100.5, low=99.5)
        bar_wide = _make_bar(high=105.0, low=95.0)
        fill_narrow = model.calculate(100.0, 10, Action.BUY, bar_narrow)
        fill_wide = model.calculate(100.0, 10, Action.BUY, bar_wide)
        assert fill_wide > fill_narrow


class TestGetSlippageModel:
    def test_fixed_with_bps(self):
        model = get_slippage_model('fixed', bps=5.0)
        assert isinstance(model, FixedBPS)
        assert model.bps == 5.0

    def test_zero(self):
        model = get_slippage_model('zero')
        assert isinstance(model, ZeroSlippage)

    def test_sqrt(self):
        model = get_slippage_model('sqrt', k=0.2)
        assert isinstance(model, SquareRootImpact)
        assert model.k == 0.2

    def test_volatility(self):
        model = get_slippage_model('volatility', k=0.3)
        assert isinstance(model, VolatilitySlippage)
        assert model.k == 0.3

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match='Unknown slippage model'):
            get_slippage_model('unknown')
