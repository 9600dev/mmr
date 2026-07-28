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

from unittest.mock import MagicMock
from pathlib import Path

import pytest
import yaml

from trader.strategy.strategy_runtime import StrategyRuntime
from trader.trading.strategy import Strategy


def _make_runtime(tmp_path, strategies_dir, config_file=None, paper_trading=True) -> StrategyRuntime:
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
    rt.paper_trading = paper_trading
    rt._published_conids = set()
    rt._trader_boot_id = None
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
            description='',
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
            description='',
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
            description='',
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
            class_name='V1', description='',
        )
        rt.load_strategy(
            name='second', bar_size_str='1 min', conids=[1], universe=None,
            historical_days_prior=0, module=str(b / 'shared.py'),
            class_name='V2', description='',
        )
        names = {s.name for s in rt.strategy_implementations}
        assert names == {'first', 'second'}, names
        # And both classes should have been instantiated from the correct files
        classes = {type(s).__name__ for s in rt.strategy_implementations}
        assert classes == {'V1', 'V2'}


class TestPaperOnlyGate:
    def test_paper_only_loads_on_paper_service(self, tmp_path):
        strategies = tmp_path / 'strategies'
        _write_strategy(strategies, 'mystrat', _VALID_STRATEGY_BODY)
        rt = _make_runtime(tmp_path, strategies, paper_trading=True)

        rt.load_strategy(
            name='cautious', bar_size_str='1 min', conids=[1],
            universe=None, historical_days_prior=0,
            module=str(strategies / 'mystrat.py'), class_name='V1',
            description='', paper_only=True,
        )
        assert any(s.name == 'cautious' for s in rt.strategy_implementations)

    def test_paper_only_refuses_live_service(self, tmp_path, caplog):
        """A strategy marked paper_only must not load on a live-mode runtime."""
        import logging as stdlib_logging
        strategies = tmp_path / 'strategies'
        _write_strategy(strategies, 'mystrat', _VALID_STRATEGY_BODY)
        rt = _make_runtime(tmp_path, strategies, paper_trading=False)

        with caplog.at_level(stdlib_logging.ERROR):
            rt.load_strategy(
                name='cautious', bar_size_str='1 min', conids=[1],
                universe=None, historical_days_prior=0,
                module=str(strategies / 'mystrat.py'), class_name='V1',
                description='', paper_only=True,
            )
        assert rt.strategy_implementations == []
        assert any('paper_only' in rec.message for rec in caplog.records)

    def test_default_loads_on_live_service(self, tmp_path):
        """paper_only defaults to False — an un-flagged strategy loads on live."""
        strategies = tmp_path / 'strategies'
        _write_strategy(strategies, 'mystrat', _VALID_STRATEGY_BODY)
        rt = _make_runtime(tmp_path, strategies, paper_trading=False)

        rt.load_strategy(
            name='normal', bar_size_str='1 min', conids=[1],
            universe=None, historical_days_prior=0,
            module=str(strategies / 'mystrat.py'), class_name='V1',
            description='',
        )
        assert any(s.name == 'normal' for s in rt.strategy_implementations)


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


class TestManifestLoad:
    """Load-time validation of the optional `manifest:` block. A valid manifest
    lands its primitives on the StrategyContext; a bad one loads the strategy
    DISARMED (auto_execute stripped) with an ERROR, never silently unchecked;
    an absent manifest leaves every field None and arms exactly as today."""

    def _load(self, tmp_path, *, manifest, conids=(1, 2), auto_execute=True):
        strategies = tmp_path / 'strategies'
        _write_strategy(strategies, 'mystrat', _VALID_STRATEGY_BODY)
        rt = _make_runtime(tmp_path, strategies, paper_trading=True)
        rt.load_strategy(
            name='m_strat', bar_size_str='1 min', conids=list(conids),
            universe=None, historical_days_prior=0,
            module=str(strategies / 'mystrat.py'), class_name='V1',
            description='', auto_execute=auto_execute, manifest=manifest,
        )
        loaded = [s for s in rt.strategy_implementations if s.name == 'm_strat']
        return loaded[0] if loaded else None

    def test_absent_manifest_all_fields_none_and_arms(self, tmp_path):
        s = self._load(tmp_path, manifest=None)
        assert s is not None
        ctx = s._context
        assert ctx.manifest_allowed_conids is None
        assert ctx.manifest_direction is None
        assert ctx.manifest_max_opens_per_day is None
        assert ctx.manifest_max_opens_per_hour is None
        assert ctx.auto_execute is True  # armed exactly as before

    def test_valid_manifest_lands_on_context(self, tmp_path):
        s = self._load(tmp_path, conids=(1, 2), manifest={
            'allowed_conids': [1],            # subset of subscription {1,2}
            'direction': 'long',
            'max_opening_orders_per_day': 6,
            'max_opening_orders_per_hour': 2,
        })
        ctx = s._context
        assert ctx.manifest_allowed_conids == [1]
        assert ctx.manifest_direction == 'long'
        assert ctx.manifest_max_opens_per_day == 6
        assert ctx.manifest_max_opens_per_hour == 2
        assert ctx.auto_execute is True

    def test_allowed_conids_not_subset_disarms(self, tmp_path, caplog):
        import logging as stdlib_logging
        with caplog.at_level(stdlib_logging.ERROR):
            s = self._load(tmp_path, conids=(1,), manifest={'allowed_conids': [1, 999]})
        assert s is not None                          # still loaded
        assert s._context.auto_execute is False       # but DISARMED
        assert s._context.manifest_allowed_conids is None  # manifest ignored
        assert any('manifest' in r.message and '999' in r.message for r in caplog.records)

    def test_allowed_conids_wrong_type_disarms(self, tmp_path, caplog):
        import logging as stdlib_logging
        with caplog.at_level(stdlib_logging.ERROR):
            s = self._load(tmp_path, manifest={'allowed_conids': 'AAPL'})
        assert s._context.auto_execute is False
        assert any('allowed_conids' in r.message for r in caplog.records)

    def test_turnover_zero_disarms(self, tmp_path, caplog):
        import logging as stdlib_logging
        with caplog.at_level(stdlib_logging.ERROR):
            s = self._load(tmp_path, manifest={'max_opening_orders_per_day': 0})
        assert s._context.auto_execute is False
        assert s._context.manifest_max_opens_per_day is None

    def test_turnover_negative_disarms(self, tmp_path):
        s = self._load(tmp_path, manifest={'max_opening_orders_per_hour': -3})
        assert s._context.auto_execute is False

    def test_turnover_non_int_disarms(self, tmp_path):
        s = self._load(tmp_path, manifest={'max_opening_orders_per_day': 'lots'})
        assert s._context.auto_execute is False

    def test_direction_short_disarms(self, tmp_path, caplog):
        import logging as stdlib_logging
        with caplog.at_level(stdlib_logging.ERROR):
            s = self._load(tmp_path, manifest={'direction': 'short'})
        assert s._context.auto_execute is False
        assert s._context.manifest_direction is None
        assert any('long-only' in r.message for r in caplog.records)

    def test_direction_both_disarms(self, tmp_path):
        s = self._load(tmp_path, manifest={'direction': 'both'})
        assert s._context.auto_execute is False

    def test_direction_garbage_disarms(self, tmp_path):
        s = self._load(tmp_path, manifest={'direction': 'sideways'})
        assert s._context.auto_execute is False

    def test_allowed_conids_null_is_valid_and_unchecked(self, tmp_path):
        s = self._load(tmp_path, manifest={'allowed_conids': None, 'direction': 'long'})
        assert s._context.auto_execute is True
        assert s._context.manifest_allowed_conids is None
        assert s._context.manifest_direction == 'long'


class TestTraderServiceRestartResubscribes:
    """A trader_service restart drops every market-data subscription, and
    nothing downstream could tell.

    The live outage (2026-07-27): trader_service was restarted to deploy a fix,
    strategy_service kept running, and ticks stopped for 30 minutes. Both
    services reported healthy the whole time. The RPC socket reconnects
    transparently, so every call kept working; only the data stopped. Meanwhile
    `subscribe()` keyed its publish request off the strategies dict, which still
    listed every conId, so each 30-second reconcile re-ran and did nothing.

    Strategies were blind. Only the broker-side disaster stops still protected
    the book. This matters beyond deploys: `supervise()` restarts
    trader_service automatically after a crash, so a routine recovery produced
    a silent data outage.
    """

    def _runtime_with(self, tmp_path, boot_ids):
        strategies = tmp_path / 'strategies'
        strategies.mkdir()
        config_file = tmp_path / 'strategy_runtime.yaml'
        config_file.write_text('strategies: []\n')
        rt = _make_runtime(tmp_path, strategies, config_file)
        published = []

        class _RPC:
            def get_status(self_inner):
                return {'boot_id': boot_ids[0]}

            def publish_contract(self_inner, contract, delayed=False):
                published.append(contract.conId)

            def resolve_symbol(self_inner, conId):
                return []

        class _Client:
            def rpc(self_inner, return_type=None):
                return _RPC()

        rt.trader_client = _Client()  # type: ignore
        rt._published = published
        return rt

    def _contract(self, conid):
        from ib_async.contract import Contract
        return Contract(secType='STK', conId=conid, symbol='X', exchange='SMART')

    def test_a_restart_causes_resubscription(self, tmp_path):
        boot = ['boot-A']
        rt = self._runtime_with(tmp_path, boot)

        class _S:
            name = 's'
        strat = _S()

        rt._note_trader_boot()
        rt.subscribe(strat, self._contract(101))
        assert rt._published == [101]

        # Same process: reconciling again must NOT re-ask.
        rt._note_trader_boot()
        rt.subscribe(strat, self._contract(101))
        assert rt._published == [101], 'a steady-state reconcile must not re-subscribe'

        # trader_service restarts. Its subscriptions are gone.
        boot[0] = 'boot-B'
        assert rt._note_trader_boot() is True
        rt.subscribe(strat, self._contract(101))
        assert rt._published == [101, 101], (
            'after a trader_service restart the conId must be published again')

    def test_the_routing_table_survives_the_restart(self, tmp_path):
        """Only the mirror of the OTHER process's state is discarded. Which
        strategies want a conId is ours and must not be lost, or ticks would
        arrive with nobody to deliver them to."""
        boot = ['boot-A']
        rt = self._runtime_with(tmp_path, boot)

        class _S:
            name = 's'
        strat = _S()
        rt._note_trader_boot()
        rt.subscribe(strat, self._contract(101))

        boot[0] = 'boot-B'
        rt._note_trader_boot()
        assert rt.strategies[101] == [strat]
        rt.subscribe(strat, self._contract(101))
        assert rt.strategies[101] == [strat], 'the strategy was duplicated or dropped'

    def test_publishes_we_cannot_attribute_to_the_live_process_are_resent(self, tmp_path):
        """The hole in the first version of this fix, found on its first live
        test rather than by any test written for it.

        The two facts are learned INDEPENDENTLY, so they can disagree.
        strategy_service published its subscriptions to trader_service instance
        A, then trader_service restarted inside the startup window, so the
        FIRST status read that succeeded came from instance B. With `previous
        is None` treated as "first read, nothing to compare", the publishes
        made to A were kept, never re-sent, and the feed stayed dead — the
        exact outage this fix exists to prevent, surviving the fix.

        A publish that cannot be attributed to the live process must be
        re-sent. The cost of being wrong is a duplicate subscription; the cost
        of the other choice is silence.
        """
        rt = self._runtime_with(tmp_path, ['boot-B'])

        class _S:
            name = 's'
        strat = _S()

        # Published to an instance we never got a status read from.
        rt.subscribe(strat, self._contract(101))
        assert rt._published == [101]
        assert rt._trader_boot_id is None, 'precondition: no boot id observed yet'

        assert rt._note_trader_boot() is True
        rt.subscribe(strat, self._contract(101))
        assert rt._published == [101, 101]

    def test_a_clean_start_does_not_log_a_phantom_restart(self, tmp_path):
        """The converse. With nothing published yet there is nothing to
        re-send, so the first status read is not a restart."""
        rt = self._runtime_with(tmp_path, ['boot-A'])
        assert rt._note_trader_boot() is False
        assert rt._published == []

    def test_an_unreadable_status_is_not_treated_as_a_restart(self, tmp_path):
        """Re-publishing every conId on a transient RPC failure would hammer
        trader_service with duplicate subscriptions on exactly the cycles it is
        least able to serve them."""
        rt = self._runtime_with(tmp_path, ['boot-A'])

        class _S:
            name = 's'
        strat = _S()
        rt._note_trader_boot()
        rt.subscribe(strat, self._contract(101))

        class _Client:
            def rpc(self_inner, return_type=None):
                raise ConnectionError('trader_service restarting')

        rt.trader_client = _Client()  # type: ignore
        assert rt._note_trader_boot() is False
        rt.subscribe(strat, self._contract(101))
        assert rt._published == [101], 'a failed status read must not force a re-subscribe'

    def test_an_older_trader_service_without_a_boot_id_is_tolerated(self, tmp_path):
        rt = self._runtime_with(tmp_path, ['boot-A'])

        class _RPC:
            def get_status(self_inner):
                return {'ib_connected': True}

        class _Client:
            def rpc(self_inner, return_type=None):
                return _RPC()

        rt.trader_client = _Client()  # type: ignore
        assert rt._note_trader_boot() is False

    def test_trader_status_reports_a_stable_boot_id(self):
        """The detector is only as good as the signal. The id must be constant
        within a process — a value that changed per call would re-subscribe
        every cycle forever."""
        from trader.trading import trading_runtime

        t = object.__new__(trading_runtime.Trader)
        t.client = MagicMock()
        t.client.ib.isConnected = MagicMock(return_value=True)
        t._ib_upstream_connected = True
        t.data = object()
        first = t.status()['boot_id']
        t._status_cache_ts = 0.0          # defeat the 1s cache
        assert t.status()['boot_id'] == first
        assert first == trading_runtime._BOOT_ID


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
            def get_status(self):
                time.sleep(0.2)
                return {'boot_id': 'boot-A'}
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
