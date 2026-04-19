"""Tests for strategy loading idempotency, reconciliation (§2), and params support.

Tests load_strategy duplicate detection, config_loader behavior, and
per-strategy params passed from YAML config.
"""

import os
import tempfile
import time

import pytest
import yaml

from trader.data.data_access import TickStorage
from trader.data.universe import UniverseAccessor
from trader.objects import BarSize
from trader.trading.strategy import Strategy, StrategyContext, StrategyState


class TestLoadStrategyIdempotency:
    """Test that load_strategy skips already-loaded strategies."""

    def _make_runtime(self, tmp_path, tmp_duckdb_path):
        """Create a minimal StrategyRuntime without connecting to services."""
        from trader.strategy.strategy_runtime import StrategyRuntime
        rt = object.__new__(StrategyRuntime)
        rt.strategy_implementations = []
        rt.strategies = {}
        rt.streams = {}
        rt.strategies_directory = str(tmp_path / 'strategies')
        rt.strategy_config_file = str(tmp_path / 'strategy_runtime.yaml')
        rt.duckdb_path = tmp_duckdb_path
        rt.universe_library = 'Universes'
        rt.storage = TickStorage(duckdb_path=tmp_duckdb_path)
        rt.universe_accessor = UniverseAccessor.__new__(UniverseAccessor)
        rt.universe_accessor.duckdb_path = tmp_duckdb_path
        rt.universe_accessor.universe_library = 'Universes'
        os.makedirs(rt.strategies_directory, exist_ok=True)
        return rt

    def _write_strategy_file(self, directory, name='test_strat'):
        """Write a minimal strategy file and return its path."""
        path = os.path.join(directory, f'{name}.py')
        with open(path, 'w') as f:
            f.write(f"""
from trader.trading.strategy import Strategy

class TestStrat(Strategy):
    def on_prices(self, prices):
        return None
""")
        return path

    def test_load_strategy_succeeds(self, tmp_path, tmp_duckdb_path):
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)
        path = self._write_strategy_file(rt.strategies_directory)
        rt.load_strategy(
            name='test_strat',
            bar_size_str='1 min',
            conids=[265598],
            universe=None,
            historical_days_prior=5,
            module=path,
            class_name='TestStrat',
            description='test',
        )
        assert len(rt.strategy_implementations) == 1
        assert rt.strategy_implementations[0].name == 'test_strat'

    def test_load_strategy_skips_duplicate(self, tmp_path, tmp_duckdb_path):
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)
        path = self._write_strategy_file(rt.strategies_directory)
        rt.load_strategy(
            name='test_strat', bar_size_str='1 min', conids=[265598],
            universe=None, historical_days_prior=5, module=path,
            class_name='TestStrat', description='test',
        )
        # Load same name again
        rt.load_strategy(
            name='test_strat', bar_size_str='1 min', conids=[265598],
            universe=None, historical_days_prior=5, module=path,
            class_name='TestStrat', description='test',
        )
        # Should still be only 1
        assert len(rt.strategy_implementations) == 1

    def test_load_different_strategies(self, tmp_path, tmp_duckdb_path):
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)
        path1 = self._write_strategy_file(rt.strategies_directory, 'strat_a')
        path2 = self._write_strategy_file(rt.strategies_directory, 'strat_b')

        # Write distinct class names
        with open(path1, 'w') as f:
            f.write("from trader.trading.strategy import Strategy\nclass StratA(Strategy):\n    def on_prices(self, prices): return None\n")
        with open(path2, 'w') as f:
            f.write("from trader.trading.strategy import Strategy\nclass StratB(Strategy):\n    def on_prices(self, prices): return None\n")

        rt.load_strategy(
            name='strat_a', bar_size_str='1 min', conids=[265598],
            universe=None, historical_days_prior=5, module=path1,
            class_name='StratA', description='a',
        )
        rt.load_strategy(
            name='strat_b', bar_size_str='1 min', conids=[265598],
            universe=None, historical_days_prior=5, module=path2,
            class_name='StratB', description='b',
        )
        assert len(rt.strategy_implementations) == 2
        names = {s.name for s in rt.strategy_implementations}
        assert names == {'strat_a', 'strat_b'}

    def test_load_strategy_validates_required_fields(self, tmp_path, tmp_duckdb_path):
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)
        with pytest.raises(ValueError, match='invalid config'):
            rt.load_strategy(
                name='', bar_size_str='1 min', conids=[265598],
                universe=None, historical_days_prior=5, module='x.py',
                class_name='X', description='',
            )

    def test_load_strategy_invalid_module_does_not_crash(self, tmp_path, tmp_duckdb_path):
        """Loading a nonexistent module file should not crash — just skip."""
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)
        rt.load_strategy(
            name='ghost', bar_size_str='1 min', conids=[265598],
            universe=None, historical_days_prior=5,
            module='/nonexistent/path.py', class_name='Ghost', description='',
        )
        assert len(rt.strategy_implementations) == 0


class TestConfigLoader:
    """Test config_loader parses YAML and loads strategies."""

    def _make_runtime(self, tmp_path, tmp_duckdb_path):
        from trader.strategy.strategy_runtime import StrategyRuntime
        rt = object.__new__(StrategyRuntime)
        rt.strategy_implementations = []
        rt.strategies = {}
        rt.streams = {}
        rt.strategies_directory = str(tmp_path / 'strategies')
        rt.strategy_config_file = str(tmp_path / 'strategy_runtime.yaml')
        rt.duckdb_path = tmp_duckdb_path
        rt.universe_library = 'Universes'
        rt.storage = TickStorage(duckdb_path=tmp_duckdb_path)
        rt.universe_accessor = UniverseAccessor.__new__(UniverseAccessor)
        rt.universe_accessor.duckdb_path = tmp_duckdb_path
        rt.universe_accessor.universe_library = 'Universes'
        os.makedirs(rt.strategies_directory, exist_ok=True)
        return rt

    def test_config_loader_loads_strategies(self, tmp_path, tmp_duckdb_path):
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)

        # Write strategy file
        strat_path = os.path.join(rt.strategies_directory, 'my_strat.py')
        with open(strat_path, 'w') as f:
            f.write("from trader.trading.strategy import Strategy\nclass MyStrat(Strategy):\n    def on_prices(self, prices): return None\n")

        # Write config
        config = {
            'strategies': [{
                'name': 'my_strat',
                'bar_size': '1 min',
                'conids': [265598],
                'module': strat_path,
                'class_name': 'MyStrat',
            }]
        }
        config_path = os.path.join(str(tmp_path), 'strategy_runtime.yaml')
        with open(config_path, 'w') as f:
            yaml.dump(config, f)

        rt.config_loader(config_path)
        assert len(rt.strategy_implementations) == 1
        assert rt.strategy_implementations[0].name == 'my_strat'

    def test_config_loader_idempotent_on_reload(self, tmp_path, tmp_duckdb_path):
        """Calling config_loader twice should not duplicate strategies."""
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)

        strat_path = os.path.join(rt.strategies_directory, 'dup_strat.py')
        with open(strat_path, 'w') as f:
            f.write("from trader.trading.strategy import Strategy\nclass DupStrat(Strategy):\n    def on_prices(self, prices): return None\n")

        config = {
            'strategies': [{
                'name': 'dup_strat',
                'bar_size': '1 min',
                'conids': [265598],
                'module': strat_path,
                'class_name': 'DupStrat',
            }]
        }
        config_path = os.path.join(str(tmp_path), 'strategy_runtime.yaml')
        with open(config_path, 'w') as f:
            yaml.dump(config, f)

        rt.config_loader(config_path)
        rt.config_loader(config_path)
        assert len(rt.strategy_implementations) == 1


class TestStrategyParams:
    """Test per-strategy params from YAML config."""

    def _make_runtime(self, tmp_path, tmp_duckdb_path):
        from trader.strategy.strategy_runtime import StrategyRuntime
        rt = object.__new__(StrategyRuntime)
        rt.strategy_implementations = []
        rt.strategies = {}
        rt.streams = {}
        rt.strategies_directory = str(tmp_path / 'strategies')
        rt.strategy_config_file = str(tmp_path / 'strategy_runtime.yaml')
        rt.duckdb_path = tmp_duckdb_path
        rt.universe_library = 'Universes'
        rt.storage = TickStorage(duckdb_path=tmp_duckdb_path)
        rt.universe_accessor = UniverseAccessor.__new__(UniverseAccessor)
        rt.universe_accessor.duckdb_path = tmp_duckdb_path
        rt.universe_accessor.universe_library = 'Universes'
        os.makedirs(rt.strategies_directory, exist_ok=True)
        return rt

    def test_params_passed_to_strategy(self, tmp_path, tmp_duckdb_path):
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)
        strat_path = os.path.join(rt.strategies_directory, 'param_strat.py')
        with open(strat_path, 'w') as f:
            f.write("from trader.trading.strategy import Strategy\nclass ParamStrat(Strategy):\n    def on_prices(self, prices): return None\n")

        rt.load_strategy(
            name='param_strat', bar_size_str='1 min', conids=[265598],
            universe=None, historical_days_prior=5, module=strat_path,
            class_name='ParamStrat', description='',
            params={'fast_period': 5, 'slow_period': 20},
        )
        assert len(rt.strategy_implementations) == 1
        strat = rt.strategy_implementations[0]
        assert strat.params == {'fast_period': 5, 'slow_period': 20}
        assert strat.params.get('fast_period') == 5
        assert strat.params.get('slow_period') == 20

    def test_params_default_empty_dict(self, tmp_path, tmp_duckdb_path):
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)
        strat_path = os.path.join(rt.strategies_directory, 'no_param.py')
        with open(strat_path, 'w') as f:
            f.write("from trader.trading.strategy import Strategy\nclass NoParam(Strategy):\n    def on_prices(self, prices): return None\n")

        rt.load_strategy(
            name='no_param', bar_size_str='1 min', conids=[265598],
            universe=None, historical_days_prior=5, module=strat_path,
            class_name='NoParam', description='',
        )
        strat = rt.strategy_implementations[0]
        assert strat.params == {}
        assert strat.params.get('missing', 42) == 42

    def test_params_from_yaml(self, tmp_path, tmp_duckdb_path):
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)
        strat_path = os.path.join(rt.strategies_directory, 'yaml_param.py')
        with open(strat_path, 'w') as f:
            f.write("from trader.trading.strategy import Strategy\nclass YamlParam(Strategy):\n    def on_prices(self, prices): return None\n")

        config = {
            'strategies': [{
                'name': 'yaml_param',
                'bar_size': '1 day',
                'conids': [265598],
                'module': strat_path,
                'class_name': 'YamlParam',
                'params': {
                    'rsi_period': 21,
                    'threshold': 0.05,
                    'use_volume': True,
                },
            }]
        }
        config_path = os.path.join(str(tmp_path), 'strategy_runtime.yaml')
        with open(config_path, 'w') as f:
            yaml.dump(config, f)

        rt.config_loader(config_path)
        strat = rt.strategy_implementations[0]
        assert strat.params['rsi_period'] == 21
        assert strat.params['threshold'] == 0.05
        assert strat.params['use_volume'] is True

    def test_params_missing_from_yaml_defaults_empty(self, tmp_path, tmp_duckdb_path):
        """YAML entry without params key should give empty dict."""
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)
        strat_path = os.path.join(rt.strategies_directory, 'no_yaml_param.py')
        with open(strat_path, 'w') as f:
            f.write("from trader.trading.strategy import Strategy\nclass NoYamlParam(Strategy):\n    def on_prices(self, prices): return None\n")

        config = {
            'strategies': [{
                'name': 'no_yaml_param',
                'bar_size': '1 min',
                'conids': [265598],
                'module': strat_path,
                'class_name': 'NoYamlParam',
            }]
        }
        config_path = os.path.join(str(tmp_path), 'strategy_runtime.yaml')
        with open(config_path, 'w') as f:
            yaml.dump(config, f)

        rt.config_loader(config_path)
        strat = rt.strategy_implementations[0]
        assert strat.params == {}

    def test_strategy_config_includes_params(self, tmp_path, tmp_duckdb_path):
        """StrategyConfig.from_strategy should preserve params."""
        from trader.trading.strategy import StrategyConfig
        rt = self._make_runtime(tmp_path, tmp_duckdb_path)
        strat_path = os.path.join(rt.strategies_directory, 'cfg_param.py')
        with open(strat_path, 'w') as f:
            f.write("from trader.trading.strategy import Strategy\nclass CfgParam(Strategy):\n    def on_prices(self, prices): return None\n")

        rt.load_strategy(
            name='cfg_param', bar_size_str='1 min', conids=[265598],
            universe=None, historical_days_prior=5, module=strat_path,
            class_name='CfgParam', description='',
            params={'window': 30},
        )
        strat = rt.strategy_implementations[0]
        config = StrategyConfig.from_strategy(strat)
        assert config.params == {'window': 30}
