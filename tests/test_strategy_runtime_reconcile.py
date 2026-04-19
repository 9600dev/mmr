"""Targeted tests for StrategyRuntime's config loading, reconciliation loop,
and strategy-module sandbox. Exercise behaviour that a reviewer could not
infer from reading the code alone — the load_strategy path, the sys.modules
collision fix, safe_load hardening, path traversal rejection, and mtime
corruption recovery.

These tests bypass the IB/ZMQ setup in ``connect()`` — they build a
minimally-wired StrategyRuntime and drive the pieces under test directly.
"""

import asyncio
import os
from pathlib import Path

import pytest
import yaml

from trader.strategy.strategy_runtime import StrategyRuntime
from trader.trading.strategy import Strategy


def _make_runtime(tmp_path, strategies_dir, config_file=None) -> StrategyRuntime:
    """Build a StrategyRuntime with just enough wiring for load_strategy /
    config_loader / _reconcile to work. No ZMQ, no IB, no storage."""
    config_path = str(config_file or tmp_path / 'strategy_runtime.yaml')
    rt = StrategyRuntime.__new__(StrategyRuntime)  # skip __init__
    rt.strategies_directory = str(strategies_dir)
    rt.strategy_config_file = config_path
    rt.strategy_implementations = []
    rt.strategies = {}
    rt.streams = {}
    rt.storage = None  # type: ignore
    rt.universe_accessor = None  # type: ignore
    rt._config_mtime = 0.0
    rt.trader_client = None  # type: ignore
    return rt


def _write_strategy(strategies_dir: Path, name: str, body: str) -> Path:
    strategies_dir.mkdir(parents=True, exist_ok=True)
    path = strategies_dir / f'{name}.py'
    path.write_text(body)
    return path


_VALID_STRATEGY_BODY = """
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class V1(Strategy):
    def on_prices(self, prices):
        return None
"""

_ALT_STRATEGY_BODY = """
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class V2(Strategy):
    marker = 'alt-version'
    def on_prices(self, prices):
        return None
"""


class TestLoadStrategySandbox:
    def test_loads_valid_module_from_strategies_dir(self, tmp_path):
        strategies = tmp_path / 'strategies'
        _write_strategy(strategies, 'mystrat', _VALID_STRATEGY_BODY)
        rt = _make_runtime(tmp_path, strategies)

        rt.load_strategy(
            name='test_v1', bar_size_str='1 min', conids=[1],
            universe=None, historical_days_prior=0,
            module=str(strategies / 'mystrat.py'), class_name='V1',
            description='', paper=True,
        )
        assert any(s.name == 'test_v1' for s in rt.strategy_implementations), (
            'strategy should have been loaded and appended'
        )

    def test_rejects_absolute_path_outside_strategies_dir(self, tmp_path):
        """An attacker-controlled YAML must not load /tmp/evil.py."""
        strategies = tmp_path / 'strategies'
        strategies.mkdir()
        evil = tmp_path / 'evil.py'
        _write_strategy(evil.parent, 'evil', _VALID_STRATEGY_BODY)
        rt = _make_runtime(tmp_path, strategies)

        # Load failure is logged at ERROR; strategy list stays empty.
        rt.load_strategy(
            name='evil_strat', bar_size_str='1 min', conids=[1],
            universe=None, historical_days_prior=0,
            module=str(evil), class_name='V1',
            description='', paper=True,
        )
        assert rt.strategy_implementations == []

    def test_rejects_path_traversal(self, tmp_path):
        strategies = tmp_path / 'strategies'
        strategies.mkdir()
        # Create a file *outside* strategies_dir we should not be able to reach
        outside = tmp_path / 'outside.py'
        _write_strategy(outside.parent, 'outside', _VALID_STRATEGY_BODY)
        rt = _make_runtime(tmp_path, strategies)

        rt.load_strategy(
            name='traversal', bar_size_str='1 min', conids=[1],
            universe=None, historical_days_prior=0,
            module='../outside.py', class_name='V1',
            description='', paper=True,
        )
        assert rt.strategy_implementations == []

    def test_sys_modules_not_clobbered_by_duplicate_basename(self, tmp_path):
        """Two strategies whose source files share a basename must not
        clobber each other in sys.modules."""
        strategies = tmp_path / 'strategies'
        # Put both files under the strategies dir so they resolve correctly.
        a = strategies / 'a'
        b = strategies / 'b'
        a.mkdir(parents=True)
        b.mkdir(parents=True)
        (a / 'shared.py').write_text(_VALID_STRATEGY_BODY)
        (b / 'shared.py').write_text(_ALT_STRATEGY_BODY)

        rt = _make_runtime(tmp_path, strategies)
        rt.load_strategy(
            name='first', bar_size_str='1 min', conids=[1], universe=None,
            historical_days_prior=0, module=str(a / 'shared.py'),
            class_name='V1', description='', paper=True,
        )
        rt.load_strategy(
            name='second', bar_size_str='1 min', conids=[1], universe=None,
            historical_days_prior=0, module=str(b / 'shared.py'),
            class_name='V2', description='', paper=True,
        )
        names = {s.name for s in rt.strategy_implementations}
        assert names == {'first', 'second'}, names
        # And both classes should have been instantiated from the correct files
        classes = {type(s).__name__ for s in rt.strategy_implementations}
        assert classes == {'V1', 'V2'}


class TestConfigLoader:
    def test_safe_load_rejects_python_object_tag(self, tmp_path):
        strategies = tmp_path / 'strategies'
        strategies.mkdir()
        config_file = tmp_path / 'strategy_runtime.yaml'
        config_file.write_text(
            'strategies:\n'
            '  - name: evil\n'
            '    bar_size: "1 min"\n'
            '    module: !!python/object/apply:os.system ["echo pwn"]\n'
            '    class_name: V1\n'
        )
        rt = _make_runtime(tmp_path, strategies, config_file)
        with pytest.raises(yaml.constructor.ConstructorError):
            rt.config_loader(str(config_file))

    def test_empty_strategies_section_no_op(self, tmp_path):
        strategies = tmp_path / 'strategies'
        strategies.mkdir()
        config_file = tmp_path / 'strategy_runtime.yaml'
        config_file.write_text('other_key: true\n')
        rt = _make_runtime(tmp_path, strategies, config_file)
        # Should not raise even though 'strategies' key is missing.
        rt.config_loader(str(config_file))
        assert rt.strategy_implementations == []


class TestReconcileResilience:
    @pytest.mark.asyncio
    async def test_corrupt_yaml_does_not_advance_mtime(self, tmp_path):
        """When the YAML is mid-write and parse fails, _config_mtime must not
        advance — otherwise we'd never retry the reload."""
        strategies = tmp_path / 'strategies'
        strategies.mkdir()
        config_file = tmp_path / 'strategy_runtime.yaml'
        # Write a bad YAML
        config_file.write_text('not: valid: yaml: [: :\n')
        rt = _make_runtime(tmp_path, strategies, config_file)
        rt._config_mtime = 0.0

        # Stub trader_client to avoid the RPC call in step 2.
        class _StubClient:
            def rpc(self):
                raise ConnectionError('not connected — test stub')

        rt.trader_client = _StubClient()  # type: ignore

        await rt._reconcile()
        # After a failed reload, _config_mtime must still be the sentinel 0.0
        # so the next tick tries again.
        assert rt._config_mtime == 0.0

    @pytest.mark.asyncio
    async def test_rpc_connection_error_swallowed(self, tmp_path):
        """Transient trader_service hiccups shouldn't abort reconciliation."""
        strategies = tmp_path / 'strategies'
        strategies.mkdir()
        config_file = tmp_path / 'strategy_runtime.yaml'
        config_file.write_text('strategies: []\n')
        rt = _make_runtime(tmp_path, strategies, config_file)

        class _Strategy:
            name = 'x'
            conids = [1]
            universe = None

        rt.strategy_implementations = [_Strategy()]  # type: ignore

        class _StubClient:
            def rpc(self, return_type=None):
                raise ConnectionError('trader_service restarting')

        rt.trader_client = _StubClient()  # type: ignore
        rt._config_mtime = os.path.getmtime(str(config_file))
        # Should not raise
        await rt._reconcile()

    @pytest.mark.asyncio
    async def test_unexpected_exception_propagates(self, tmp_path):
        """Non-connectivity exceptions in the reconcile body should propagate
        (they indicate real bugs, not transient failures) — previously they
        were logged at DEBUG and masked."""
        strategies = tmp_path / 'strategies'
        strategies.mkdir()
        config_file = tmp_path / 'strategy_runtime.yaml'
        config_file.write_text('strategies: []\n')
        rt = _make_runtime(tmp_path, strategies, config_file)

        class _Strategy:
            name = 'x'
            conids = [1]
            universe = None

        rt.strategy_implementations = [_Strategy()]  # type: ignore

        class _StubClient:
            def rpc(self, return_type=None):
                raise KeyError('programmer bug — dict lookup')

        rt.trader_client = _StubClient()  # type: ignore
        rt._config_mtime = os.path.getmtime(str(config_file))
        with pytest.raises(KeyError):
            await rt._reconcile()

    @pytest.mark.asyncio
    async def test_reconcile_does_not_block_event_loop(self, tmp_path):
        """Regression: the async ``_reconcile`` body used to run sync RPC
        calls on the loop thread, causing asyncio "slow callback" warnings
        and stalling live ticker dispatch for ~1s every 30s on a
        portfolio universe with 10+ conIds. The fix offloads the body
        to a thread via ``asyncio.to_thread`` — verify the loop stays
        responsive during a deliberately-slow reconcile."""
        import time
        strategies = tmp_path / 'strategies'
        strategies.mkdir()
        config_file = tmp_path / 'strategy_runtime.yaml'
        config_file.write_text('strategies: []\n')
        rt = _make_runtime(tmp_path, strategies, config_file)

        class _Strategy:
            name = 'x'
            conids = [1, 2, 3]
            universe = None

        rt.strategy_implementations = [_Strategy()]  # type: ignore

        # Simulate a slow trader_service — each RPC call sleeps 200ms.
        # On the old code this would block the event loop for 600ms+.
        class _SlowClient:
            def rpc(self, return_type=None):
                return self
            def resolve_symbol(self, conId):
                time.sleep(0.2)
                return []

        rt.trader_client = _SlowClient()  # type: ignore
        rt._config_mtime = os.path.getmtime(str(config_file))

        # Run reconcile concurrently with a ticker task that ticks every 10ms.
        # If the loop is blocked we'd see long gaps between ticks.
        tick_gaps = []

        async def ticker():
            prev = time.monotonic()
            while True:
                await asyncio.sleep(0.01)
                now = time.monotonic()
                tick_gaps.append(now - prev)
                prev = now

        ticker_task = asyncio.create_task(ticker())
        try:
            await rt._reconcile()
        finally:
            ticker_task.cancel()
            try:
                await ticker_task
            except asyncio.CancelledError:
                pass

        # If the loop stayed responsive, every tick gap should be near 10ms.
        # Allow a generous ceiling — we just need to prove the reconcile body
        # didn't hold the loop for ~600ms (what the old sync-on-loop code did).
        max_gap = max(tick_gaps) if tick_gaps else 0.0
        assert max_gap < 0.1, (
            f'event loop was blocked for {max_gap*1000:.0f}ms during '
            f'reconcile; sync RPC must run in a thread'
        )
