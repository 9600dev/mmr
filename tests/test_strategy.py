import pytest
import pandas as pd
from trader.objects import Action, BarSize
from trader.trading.strategy import (
    Signal, Strategy, StrategyConfig, StrategyContext, StrategyState,
)
from conftest import BuyAbove100Strategy


class TestStrategy:
    def test_install_sets_context(self, installed_strategy):
        assert installed_strategy._context is not None
        assert installed_strategy.state == StrategyState.INSTALLED

    def test_ctx_raises_before_install(self):
        strategy = BuyAbove100Strategy()
        with pytest.raises(RuntimeError, match="not installed"):
            _ = strategy.ctx

    def test_enable_disable_state(self, installed_strategy):
        assert installed_strategy.enable() == StrategyState.RUNNING
        assert installed_strategy.state == StrategyState.RUNNING
        assert installed_strategy.disable() == StrategyState.DISABLED
        assert installed_strategy.state == StrategyState.DISABLED

    def test_backward_compat_properties(self, installed_strategy):
        assert installed_strategy.name == "test_strategy"
        assert installed_strategy.bar_size == BarSize.Mins1
        assert installed_strategy.storage is not None
        assert installed_strategy.conids == [4391]
        assert installed_strategy.paper_only is False

    def test_bare_strategy_is_a_silent_noop(self):
        """Since the precompute + on_bar hook was introduced, ``on_prices``
        is no longer ``@abstractmethod``: strategies can implement either
        API (or both). A bare ``Strategy()`` can therefore be instantiated
        and simply produces no signals — it's harmless.

        (If this ever regresses to "bare Strategy() raises TypeError",
        the abstractmethod decorator was probably restored on on_prices
        by accident — remove it and fix this test.)"""
        s = Strategy()
        assert s.on_prices(pd.DataFrame()) is None
        assert s.on_bar(pd.DataFrame({'close': [1.0]}), {}, 0) is None
        assert s.precompute(pd.DataFrame()) == {}

    def test_signal_dataclass(self):
        sig = Signal(
            source_name="test",
            action=Action.BUY,
            probability=0.8,
            risk=0.2,
        )
        assert sig.source_name == "test"
        assert sig.action == Action.BUY
        assert sig.quantity == 0.0
        assert sig.conid == 0
        assert isinstance(sig.metadata, dict)

    def test_strategy_config_from_strategy(self, installed_strategy):
        installed_strategy.enable()
        config = StrategyConfig.from_strategy(installed_strategy)
        assert config.name == "test_strategy"
        assert config.bar_size == BarSize.Mins1
        assert config.state == StrategyState.RUNNING
        assert config.conids == [4391]
        assert config.paper_only is False
