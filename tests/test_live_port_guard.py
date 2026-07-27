"""AUDIT_ROADMAP G7: pytest must not be able to dial the live services.

Observed 2026-07-16: with the deployed stack up, a full test run produced IB
error-200 contract lookups for the fixture conId 12345 in the LIVE
trader_service log — some test constructed a real RPCClient against the default
tcp://127.0.0.1:42001 and its requests landed on the real service. Read-only
that day; a test that PLACED anything would use the same door.

The guard lives at RPCClient.connect (the one place every RPC dial passes)
and is armed only by MMR_PYTEST=1, which tests/conftest.py sets before any
trader import — production behaviour is byte-identical.
"""
import asyncio

import pytest

from trader.messaging.clientserver import RPCClient


def _connect(port, address='tcp://127.0.0.1'):
    client = RPCClient(zmq_server_address=address, zmq_server_port=port)
    try:
        asyncio.new_event_loop().run_until_complete(client.connect())
    finally:
        try:
            client.ctx.destroy(linger=0)
        except Exception:
            pass


class TestLivePortsAreRefusedUnderPytest:
    @pytest.mark.parametrize('port', [42001, 42003, 42005])
    def test_each_live_service_port_is_refused(self, port):
        with pytest.raises(ConnectionError, match='G7'):
            _connect(port)

    def test_an_ephemeral_port_still_connects(self):
        """The guard must not break the in-process round-trip tests — ZMQ
        connect is lazy, so dialing an unbound high port succeeds locally."""
        _connect(49152)

    def test_a_remote_address_is_not_guarded(self):
        """The hazard is the LOCAL live stack; a test deliberately pointed at a
        remote host is an integration test and its author's business."""
        _connect(42001, address='tcp://10.1.2.3')

    def test_the_escape_hatch_works(self, monkeypatch):
        monkeypatch.setenv('MMR_ALLOW_LIVE_PORTS', '1')
        _connect(42001)

    def test_unarmed_environments_are_unaffected(self, monkeypatch):
        """Production never sets MMR_PYTEST; the guard must read as absent."""
        monkeypatch.delenv('MMR_PYTEST', raising=False)
        _connect(42001)


class TestTheMessageBusIsGuardedToo:
    def test_the_live_signal_bus_port_is_refused(self):
        """42006 carries strategy signals — a test publishing there could hand
        the live auto-executor a signal, which is worse than a stray resolve."""
        from trader.messaging.clientserver import MessageBusClient
        client = MessageBusClient(zmq_address='tcp://127.0.0.1', zmq_port=42006)
        with pytest.raises(ConnectionError, match='G7'):
            asyncio.new_event_loop().run_until_complete(client.connect())
