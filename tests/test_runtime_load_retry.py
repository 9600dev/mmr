"""Silent paths in the runtime now speak (2026-09-14).

(c) A strategy whose load failed for a possibly-transient reason (the isolated
    worker's 60s init deadline on a loaded host) was never retried:
    ``_reconcile_sync`` only re-ran the loader when the YAML mtime or a LOADED
    strategy's source hash changed. Now: retried on the next reconcile ticks,
    at most three attempts, then a clear "giving up" ERROR until the entry or
    source changes.
(b) ``_callback_result`` dropped a BUY silently when the strategy was not
    RUNNING (WAITING_HISTORICAL_DATA) or its generation was superseded; it now
    logs once per (strategy, reason). And the worker's ``_NoRuntime`` says
    plainly that runtime mutation is unsupported live and how to fix the
    strategy.
"""
import logging
import os
import textwrap
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest
import yaml

from trader.objects import Action, BarSize
from trader.strategy.callback_worker import CallbackResult, UnsupportedStrategyAPI, _NoRuntime
from trader.strategy.strategy_runtime import StrategyRuntime, _MAX_LOAD_ATTEMPTS
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState


def _runtime(tmp_path):
    rt = StrategyRuntime.__new__(StrategyRuntime)
    rt.strategies_directory = str(tmp_path / 'strategies')
    rt.strategy_config_file = str(tmp_path / 'strategy_runtime.yaml')
    rt.strategy_implementations, rt.strategies, rt.streams = [], {}, {}
    rt.storage = rt.universe_accessor = None
    rt._config_mtime = 0.0
    rt.paper_trading = True
    rt._published_conids, rt._trader_boot_id = set(), None
    rt.trader_client = MagicMock()
    rt.trader_client.rpc().get_status.return_value = {'boot_id': 'boot'}
    rt.duckdb_path = str(tmp_path / 'runtime.duckdb')
    return rt


def _write(tmp_path, marker):
    strategies = tmp_path / 'strategies'
    strategies.mkdir(exist_ok=True)
    module = strategies / 'flaky.py'
    module.write_text(textwrap.dedent(f'''
        import os
        if not os.path.exists({str(marker)!r}):
            raise RuntimeError('transient: dependency not ready')
        from trader.trading.strategy import Strategy
        class Flaky(Strategy):
            def on_prices(self, prices):
                return None
    '''))
    (tmp_path / 'strategy_runtime.yaml').write_text(yaml.safe_dump({'strategies': [
        dict(name='flaky', module=str(module), class_name='Flaky', bar_size='1 min', conids=[])]}))
    return module


def test_transient_load_failure_is_retried_on_the_next_reconcile_ticks(tmp_path, caplog):
    marker = tmp_path / 'dependency_ready'
    _write(tmp_path, marker)
    rt = _runtime(tmp_path)
    with caplog.at_level(logging.ERROR):
        rt._reconcile_sync()                                    # mtime changed: first load fails
    assert rt.get_strategy('flaky') is None
    assert rt._load_failures['flaky']['attempts'] == 1
    assert any('will retry on the next reconcile (attempt 1/3)' in r.getMessage() for r in caplog.records)

    rt._reconcile_sync()                                        # nothing changed: retried anyway
    assert rt._load_failures['flaky']['attempts'] == 2

    marker.write_text('ready')                                  # the transient condition clears
    rt._reconcile_sync()
    assert rt.get_strategy('flaky') is not None
    assert 'flaky' not in rt._load_failures


def test_retries_are_bounded_then_the_operator_is_told(tmp_path, caplog):
    _write(tmp_path, tmp_path / 'never')
    rt = _runtime(tmp_path)
    loader = Mock(wraps=rt.config_loader)
    rt.config_loader = loader
    with caplog.at_level(logging.ERROR):
        for _ in range(_MAX_LOAD_ATTEMPTS + 2):
            rt._reconcile_sync()
    assert rt._load_failures['flaky']['attempts'] == _MAX_LOAD_ATTEMPTS
    assert loader.call_count == _MAX_LOAD_ATTEMPTS, 'no reload once the budget is spent'
    giving_up = [r for r in caplog.records if 'giving up until its YAML entry or source changes' in r.getMessage()]
    assert len(giving_up) == 1

    # An edit to the YAML entry re-arms the loader as before.
    path = tmp_path / 'strategy_runtime.yaml'
    path.write_text(path.read_text() + '\n# touched\n')
    os.utime(path, (os.path.getmtime(path) + 5, os.path.getmtime(path) + 5))
    rt._reconcile_sync()
    assert loader.call_count == _MAX_LOAD_ATTEMPTS + 1


def test_missing_class_is_a_logged_failure_not_a_silent_return(tmp_path, caplog):
    marker = tmp_path / 'ready'
    marker.write_text('ready')
    module = _write(tmp_path, marker)
    rt = _runtime(tmp_path)
    with caplog.at_level(logging.ERROR):
        rt.load_strategy(name='ghost', bar_size_str='1 min', conids=[], universe=None,
                         historical_days_prior=0, module=str(module), class_name='NoSuchClass',
                         description='')
    assert rt.get_strategy('ghost') is None
    assert any("class 'NoSuchClass' not found" in r.getMessage() for r in caplog.records)
    assert rt._load_failures['ghost']['attempts'] == 1


class TestDroppedBuyIsLogged:
    def _runtime(self):
        rt = StrategyRuntime.__new__(StrategyRuntime)
        rt._handle_signal = Mock()
        strategy = Strategy()
        strategy.install(StrategyContext(
            name='warming', bar_size=BarSize.Mins1, conids=[1], universe=None, historical_days_prior=5,
            paper_only=False, storage=None, universe_accessor=None, logger=logging, params={}))
        strategy.state = StrategyState.WAITING_HISTORICAL_DATA
        rt._deployment_generations = {'warming': 'gen-1'}
        return rt, strategy

    def _result(self, action, generation='gen-1'):
        return CallbackResult(1, pd.Timestamp('2026-09-14 14:30', tz='UTC'), generation,
                              Signal('warming', action, .6, .4))

    def test_buy_while_waiting_for_history_is_dropped_and_said_once(self, caplog):
        rt, strategy = self._runtime()
        with caplog.at_level(logging.WARNING):
            rt._callback_result(strategy, self._result(Action.BUY))
            rt._callback_result(strategy, self._result(Action.BUY))
        rt._handle_signal.assert_not_called()
        dropped = [r for r in caplog.records if 'dropping BUY from warming' in r.getMessage()]
        assert len(dropped) == 1
        assert 'WAITING_HISTORICAL_DATA' in dropped[0].getMessage()

    def test_sell_is_never_dropped(self):
        rt, strategy = self._runtime()
        rt._callback_result(strategy, self._result(Action.SELL))
        rt._handle_signal.assert_called_once()

    def test_superseded_generation_is_its_own_reason_and_a_passing_buy_rearms_the_log(self, caplog):
        rt, strategy = self._runtime()
        with caplog.at_level(logging.WARNING):
            rt._callback_result(strategy, self._result(Action.BUY, generation='stale'))
            rt._callback_result(strategy, self._result(Action.BUY, generation='stale'))
            strategy.state = StrategyState.RUNNING
            rt._callback_result(strategy, self._result(Action.BUY))            # passes
            strategy.state = StrategyState.WAITING_HISTORICAL_DATA
            rt._callback_result(strategy, self._result(Action.BUY))            # dropped again: news
        messages = [r.getMessage() for r in caplog.records if 'dropping BUY' in r.getMessage()]
        assert sum('generation was superseded' in m for m in messages) == 1
        assert sum('WAITING_HISTORICAL_DATA' in m for m in messages) == 1
        rt._handle_signal.assert_called_once()


def test_no_runtime_message_names_the_fix():
    with pytest.raises(UnsupportedStrategyAPI) as info:
        _NoRuntime().subscribe
    message = str(info.value)
    assert 'strategy_runtime.subscribe' in message
    assert 'unsupported live' in message and 'isolated child process' in message
    assert 'conids' in message and 'strategy_runtime.yaml' in message and 'on_prices' in message
