"""Callback-worker budgets are reachable from trader.yaml / env (2026-09-14).

``memory_limit_bytes``, ``callback_timeout_s``, ``startup_timeout_s``,
``max_frame_bytes`` and ``_isolate_callbacks`` were constructor knobs on the
worker that nothing wired: ``_start_callback_worker`` never passed them and
the runtime constructor never accepted them. They now follow the Container
pattern (constructor parameter == config key == upper-cased env var) with the
contract defaults, so behaviour is unchanged unless configured.
"""
import inspect
import sys
from types import SimpleNamespace

import pytest
import yaml

from trader.container import Container
from trader.strategy.callback_worker import StrategyCallbackWorker
from trader.strategy.strategy_runtime import (
    DEFAULT_CALLBACK_TIMEOUT_S, DEFAULT_MAX_FRAME_BYTES, DEFAULT_STARTUP_TIMEOUT_S,
    StrategyRuntime, worker_limits)


_REQUIRED = dict(
    ib_server_address='127.0.0.1', ib_paper_port=7497, ib_live_port=7496, trading_mode='paper',
    strategy_runtime_ib_client_id=7, universe_library='Universes',
    zmq_pubsub_server_address='tcp://127.0.0.1', zmq_pubsub_server_port=42002,
    zmq_rpc_server_address='tcp://127.0.0.1', zmq_rpc_server_port=42001,
    zmq_strategy_rpc_server_address='tcp://127.0.0.1', zmq_strategy_rpc_server_port=42005,
    zmq_messagebus_server_address='tcp://127.0.0.1', zmq_messagebus_server_port=42006,
    strategies_directory='strategies', strategy_config_file='strategy_runtime.yaml',
)


def _container(tmp_path, monkeypatch, **knobs):
    monkeypatch.delenv('TRADER_CONFIG', raising=False)
    for name in ('STRATEGY_CALLBACK_TIMEOUT_S', 'STRATEGY_STARTUP_TIMEOUT_S', 'STRATEGY_MAX_FRAME_BYTES',
                 'STRATEGY_MEMORY_LIMIT_BYTES', 'STRATEGY_ISOLATE_CALLBACKS'):
        monkeypatch.delenv(name, raising=False)
    config = dict(_REQUIRED, duckdb_path=str(tmp_path / 'mmr.duckdb'),
                  history_duckdb_path=str(tmp_path / 'history.duckdb'), **knobs)
    path = tmp_path / 'trader.yaml'
    path.write_text(yaml.safe_dump(config))
    return Container(str(path))


def test_defaults_match_the_contract_and_the_worker_signature():
    assert worker_limits() == {
        'callback_timeout_s': 5.0, 'startup_timeout_s': 60.0,
        'max_frame_bytes': 32 * 1024 * 1024, 'memory_limit_bytes': None}
    params = inspect.signature(StrategyCallbackWorker.__init__).parameters
    assert params['callback_timeout_s'].default == DEFAULT_CALLBACK_TIMEOUT_S
    assert params['startup_timeout_s'].default == DEFAULT_STARTUP_TIMEOUT_S
    assert params['max_frame_bytes'].default == DEFAULT_MAX_FRAME_BYTES
    assert params['memory_limit_bytes'].default is None


def test_unconfigured_runtime_keeps_isolation_on_and_contract_budgets(tmp_path, monkeypatch):
    rt = _container(tmp_path, monkeypatch).resolve(StrategyRuntime)
    assert rt._isolate_callbacks is True
    assert rt._worker_limits == worker_limits()


def test_yaml_values_resolve_through_the_container(tmp_path, monkeypatch):
    rt = _container(tmp_path, monkeypatch, strategy_callback_timeout_s=2.5,
                    strategy_startup_timeout_s=30, strategy_max_frame_bytes=1048576,
                    strategy_memory_limit_bytes=0).resolve(StrategyRuntime)
    assert rt._worker_limits == {'callback_timeout_s': 2.5, 'startup_timeout_s': 30.0,
                                 'max_frame_bytes': 1048576, 'memory_limit_bytes': None}


def test_env_var_wins_over_yaml_and_is_coerced(tmp_path, monkeypatch):
    container = _container(tmp_path, monkeypatch, strategy_callback_timeout_s=2.5)
    monkeypatch.setenv('STRATEGY_CALLBACK_TIMEOUT_S', '1.25')
    monkeypatch.setenv('STRATEGY_MAX_FRAME_BYTES', '4096')
    rt = container.resolve(StrategyRuntime)
    assert rt._worker_limits['callback_timeout_s'] == 1.25
    assert rt._worker_limits['max_frame_bytes'] == 4096


def test_configured_values_reach_the_worker_constructor(tmp_path, monkeypatch):
    rt = _container(tmp_path, monkeypatch, strategy_callback_timeout_s=2.5,
                    strategy_startup_timeout_s=30, strategy_max_frame_bytes=1048576).resolve(StrategyRuntime)
    captured = {}

    class FakeWorker:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def start(self):
            pass

        def wait_ready(self):
            return {'effective_params': {}, 'effective_config_hash': 'h', 'state': 3}

    monkeypatch.setattr('trader.strategy.callback_worker.StrategyCallbackWorker', FakeWorker)
    strategy = SimpleNamespace(
        name='s', conids=[1, 2], universe=None, state=3, _source_path='/x.py', _source_hash='abc',
        ctx=SimpleNamespace(class_name='C', params={}, effective_config_hash='', deployment_generation='g'))
    rt._start_callback_worker(strategy)
    assert captured['callback_timeout_s'] == 2.5
    assert captured['startup_timeout_s'] == 30.0
    assert captured['max_frame_bytes'] == 1048576
    assert captured['memory_limit_bytes'] is None
    assert captured['max_pending'] == 2
    assert callable(captured['on_fatal']), 'idle worker deaths are reported (test_worker_idle_death)'
    assert rt._callback_workers['s'].__class__ is FakeWorker


@pytest.mark.parametrize('bad', [
    dict(callback_timeout_s=0), dict(callback_timeout_s=float('inf')), dict(callback_timeout_s='5'),
    dict(startup_timeout_s=-1), dict(max_frame_bytes=0), dict(max_frame_bytes=1.5),
    dict(memory_limit_bytes=-1), dict(memory_limit_bytes=True),
])
def test_malformed_budgets_are_refused_with_the_config_key_named(bad):
    with pytest.raises(ValueError, match='strategy_'):
        worker_limits(**bad)


def test_memory_limit_is_refused_off_linux_not_ignored(monkeypatch):
    monkeypatch.setattr(sys, 'platform', 'darwin')
    with pytest.raises(ValueError, match='Linux'):
        worker_limits(memory_limit_bytes=256 * 1024 * 1024)
    monkeypatch.setattr(sys, 'platform', 'linux')
    assert worker_limits(memory_limit_bytes=256 * 1024 * 1024)['memory_limit_bytes'] == 256 * 1024 * 1024


def test_a_bad_budget_refuses_to_construct_the_runtime(tmp_path, monkeypatch):
    container = _container(tmp_path, monkeypatch, strategy_callback_timeout_s=0)
    with pytest.raises(ValueError, match='strategy_callback_timeout_s'):
        container.resolve(StrategyRuntime)


def test_isolation_can_only_be_switched_off_explicitly(tmp_path, monkeypatch):
    rt = _container(tmp_path, monkeypatch, strategy_isolate_callbacks=False).resolve(StrategyRuntime)
    assert rt._isolate_callbacks is False
