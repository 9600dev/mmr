"""Real nested RPC: trader control -> strategy reconcile -> trader positions."""
import asyncio
import contextlib
import socket
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trader.messaging.clientserver import RPCClient, RPCServer
from trader.messaging.strategy_service_api import StrategyServiceApi
from trader.messaging.trader_service_api import TraderServiceApi
from trader.trading.strategy import StrategyState
from trader.trading.trading_runtime import Trader


def _port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


@contextlib.contextmanager
def _server(api, port):
    ready = threading.Event()
    state = {}

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        server = RPCServer(api, zmq_rpc_server_port=port)
        state.update(loop=loop, server=server)
        loop.run_until_complete(server.serve())
        ready.set()
        try:
            loop.run_forever()
        finally:
            loop.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert ready.wait(3), 'RPC test server failed to start'
    try:
        yield
    finally:
        loop, server = state['loop'], state['server']
        asyncio.run_coroutine_threadsafe(server.aclose(), loop).result(3)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(3)
        assert not thread.is_alive()


class _Runtime:
    strategy_config_file = 'test-config.yaml'

    def __init__(self, callback):
        self.callback = callback
        self.callbacks = 0

    def config_loader(self, filename):
        assert filename == self.strategy_config_file

    def _read_positions(self):
        assert self.callback.rpc().get_positions() == []
        self.callbacks += 1

    async def _reconcile(self):
        await asyncio.to_thread(self._read_positions)

    def get_strategies(self):
        return []

    def get_strategy(self, name):
        return SimpleNamespace(name=name)

    def enable_strategy(self, name):
        self._read_positions()
        return StrategyState.RUNNING

    def disable_strategy(self, name):
        self._read_positions()
        return StrategyState.DISABLED


@pytest.mark.parametrize('method,args', [('reload_strategies', ()),
                                         ('enable_strategy', ('example',)),
                                         ('disable_strategy', ('example',))])
def test_strategy_control_allows_nested_callback_into_trader(method, args):
    trader_port, strategy_port = _port(), _port()
    bridge = RPCClient(zmq_server_port=strategy_port, timeout=2)
    callback = RPCClient(zmq_server_port=trader_port, timeout=1)
    caller = RPCClient(zmq_server_port=trader_port, timeout=3)
    trader = Trader.__new__(Trader)
    trader.zmq_strategy_client = bridge
    trader.get_positions = lambda: []
    runtime = _Runtime(callback)
    with _server(TraderServiceApi(trader), trader_port), _server(StrategyServiceApi(runtime), strategy_port):
        for client in (bridge, callback, caller):
            asyncio.run(client.connect())
        try:
            result = getattr(caller.rpc(), method)(*args)
            assert result.is_success(), str(result.exception or result.error)
            assert runtime.callbacks == 1
        finally:
            for client in (bridge, callback, caller):
                client.close()


@pytest.mark.asyncio
async def test_strategy_control_timeout_preserves_a_useful_error_message():
    trader = Trader.__new__(Trader)
    trader.zmq_strategy_client = MagicMock()
    trader.zmq_strategy_client.rpc.return_value.reload_strategies.side_effect = TimeoutError('application outcome UNKNOWN')
    result = await trader.reload_strategies()
    assert not result.is_success()
    assert 'UNKNOWN' in result.error
    assert isinstance(result.exception, TimeoutError)
