"""A down trader_service fails fast at connect, not one full timeout per RPC.

Field finding 2026-11-05: with the mmr container gone, `mmr status` blocked
>30s in zmq poll (each sequential RPC waits its whole 30s budget) because ZMQ
connects lazily and connect() "succeeded" with no server.
"""
import socket
import time

import pytest

from trader.sdk import MMR


def _free_port():
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_connect_to_absent_service_fails_fast(monkeypatch):
    monkeypatch.setenv('MMR_CONNECT_PROBE_S', '0.5')
    mmr = MMR.__new__(MMR)
    mmr._rpc_address = 'tcp://127.0.0.1'
    mmr._rpc_port = _free_port()
    mmr._timeout = 30
    mmr._client = None
    t0 = time.monotonic()
    with pytest.raises(ConnectionError, match='not reachable'):
        mmr.connect(probe=True)
    assert time.monotonic() - t0 < 5
    assert mmr._client is None
    # later RPC use reports the same actionable reason, immediately
    with pytest.raises(ConnectionError, match='not reachable'):
        mmr._rpc


def test_wait_ready_true_with_live_server():
    import asyncio, threading, zmq
    from trader.messaging.clientserver import RPCClient
    port = _free_port()
    ctx = zmq.Context.instance()
    router = ctx.socket(zmq.ROUTER)
    router.bind(f'tcp://127.0.0.1:{port}')
    try:
        client = RPCClient(zmq_server_address='tcp://127.0.0.1', zmq_server_port=port, timeout=5)
        asyncio.run(client.connect())
        assert client.wait_ready(2.0)
        client.close()
    finally:
        router.close(0)


def test_lazy_connect_default_unchanged_for_long_lived_callers():
    mmr = MMR.__new__(MMR)
    mmr._rpc_address = 'tcp://127.0.0.1'
    mmr._rpc_port = _free_port()
    mmr._timeout = 30
    mmr._client = None
    mmr.connect()                    # no probe: succeeds lazily as before
    assert mmr._client is not None
    mmr._client.close()
