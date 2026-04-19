"""Tests for trader.messaging.clientserver — error-preservation, dill policy,
and a lightweight end-to-end RPC round-trip.

Network-bound tests set up a server and client on the same event loop in the
main thread to avoid the cross-thread-loop flakiness the previous revision had.
"""

import asyncio
import socket

import pytest

from trader.messaging.clientserver import (
    DillDeserializationError,
    RPCClient,
    RPCError,
    RPCHandler,
    RPCServer,
    _reconstruct_rpc_exception,
    rpcmethod,
    set_dill_whitelist,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class CustomBusinessError(Exception):
    pass


class _Service(RPCHandler):
    @rpcmethod
    def add(self, a: int, b: int) -> int:
        return a + b

    @rpcmethod
    def echo(self, payload):
        return payload

    @rpcmethod
    def raise_value(self):
        raise ValueError('bad value')

    @rpcmethod
    def raise_connection(self):
        raise ConnectionError('peer hangup')

    @rpcmethod
    def raise_custom(self):
        raise CustomBusinessError('not allowed')


# ---------------------------------------------------------------------------
# Pure unit tests on the error reconstructor (no sockets, no threads)
# ---------------------------------------------------------------------------

class TestErrorReconstruction:
    def test_stdlib_exception_preserved(self):
        exc = _reconstruct_rpc_exception('ValueError', ('bad value',), error_table=None)
        assert isinstance(exc, ValueError)
        assert 'bad value' in str(exc)

    def test_connection_error_preserved(self):
        exc = _reconstruct_rpc_exception('ConnectionError', ('peer hangup',), error_table=None)
        assert isinstance(exc, ConnectionError)

    def test_timeout_error_preserved(self):
        exc = _reconstruct_rpc_exception('TimeoutError', ('slow',), error_table=None)
        assert isinstance(exc, TimeoutError)

    def test_custom_error_via_short_name(self):
        exc = _reconstruct_rpc_exception(
            'CustomBusinessError', ('denied',),
            error_table={'CustomBusinessError': CustomBusinessError},
        )
        assert isinstance(exc, CustomBusinessError)
        assert 'denied' in str(exc)

    def test_fq_error_table_key_matches_short_server_name(self):
        """error_table keyed by fully-qualified name (as strategy_runtime does)
        still matches the server's short name."""
        exc = _reconstruct_rpc_exception(
            'CustomBusinessError', ('boom',),
            error_table={'mod.pkg.CustomBusinessError': CustomBusinessError},
        )
        assert isinstance(exc, CustomBusinessError)

    def test_unknown_type_falls_back_to_rpc_error(self):
        exc = _reconstruct_rpc_exception('SomeUnknownError', ('details',), error_table=None)
        assert isinstance(exc, RPCError)
        assert exc.exc_type == 'SomeUnknownError'
        assert exc.exc_args == ('details',)
        assert 'details' in str(exc)

    def test_ctor_mismatch_falls_through(self):
        """If the registered class has a different signature, don't crash — fall
        through to stdlib or RPCError."""
        class Strict(Exception):
            def __init__(self, code: int, kind: str):
                super().__init__(f'{kind}:{code}')
                self.code, self.kind = code, kind

        exc = _reconstruct_rpc_exception(
            'Strict', ('just a string',),  # wrong sig
            error_table={'Strict': Strict},
        )
        assert isinstance(exc, RPCError)  # not Strict, not ValueError
        assert exc.exc_type == 'Strict'


# ---------------------------------------------------------------------------
# Dill policy tests (no sockets)
# ---------------------------------------------------------------------------

class TestDillPolicy:
    def setup_method(self):
        set_dill_whitelist(None)

    def teardown_method(self):
        set_dill_whitelist(None)

    def test_empty_whitelist_rejects_all_ext_objects(self):
        import trader.messaging.clientserver as cs
        import dill
        import msgpack

        set_dill_whitelist([])
        blob = dill.dumps({'x': 1})
        ext = msgpack.ExtType(cs.EXT_OBJECT, blob)
        packed = msgpack.packb(ext, use_bin_type=True)
        with pytest.raises(DillDeserializationError):
            cs.unpack(packed)

    def test_whitelist_accepts_registered_type(self):
        import trader.messaging.clientserver as cs
        import dill
        import msgpack

        set_dill_whitelist([tuple])
        value = (1, 2, 3)
        blob = dill.dumps(value)
        ext = msgpack.ExtType(cs.EXT_OBJECT, blob)
        packed = msgpack.packb(ext, use_bin_type=True)
        assert cs.unpack(packed) == value

    def test_whitelist_rejects_unregistered_type(self):
        import trader.messaging.clientserver as cs
        import dill
        import msgpack

        set_dill_whitelist([list])
        blob = dill.dumps({'x': 1})  # dict not registered
        ext = msgpack.ExtType(cs.EXT_OBJECT, blob)
        packed = msgpack.packb(ext, use_bin_type=True)
        with pytest.raises(DillDeserializationError):
            cs.unpack(packed)

    def test_strict_mode_rejects_unknown_ext_object(self, monkeypatch):
        import trader.messaging.clientserver as cs
        import dill
        import msgpack

        monkeypatch.setattr(cs, 'DILL_STRICT_MODE', True)
        blob = dill.dumps({'x': 1})
        ext = msgpack.ExtType(cs.EXT_OBJECT, blob)
        packed = msgpack.packb(ext, use_bin_type=True)
        with pytest.raises(DillDeserializationError):
            cs.unpack(packed)


# ---------------------------------------------------------------------------
# In-process RPC round-trip (single event loop in this thread, no server loop
# in a separate thread — keeps the test deterministic and killable).
# ---------------------------------------------------------------------------

def test_rpc_round_trip_preserves_error_types():
    """End-to-end: server on a thread, sync client in this thread. Verifies
    the full pack → wire → server → unpack → error-reconstruct pipeline."""
    import threading
    import time as _t

    port = _free_port()

    ready = threading.Event()
    done = threading.Event()
    server_obj = {}

    def _run_server():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        server = RPCServer[_Service](
            instance=_Service(),
            zmq_rpc_server_address='tcp://127.0.0.1',
            zmq_rpc_server_port=port,
        )
        server_obj['server'] = server
        server_obj['loop'] = loop
        loop.run_until_complete(server.serve())
        ready.set()
        try:
            loop.run_forever()
        finally:
            try:
                loop.close()
            except Exception:
                pass
        done.set()

    t = threading.Thread(target=_run_server, daemon=True)
    t.start()
    assert ready.wait(timeout=3.0), 'server thread did not signal ready'
    # Let the ROUTER socket finish binding before we DEALER-connect
    _t.sleep(0.1)

    client = RPCClient[_Service](
        zmq_server_address='tcp://127.0.0.1',
        zmq_server_port=port,
        timeout=3,
        error_table={'CustomBusinessError': CustomBusinessError},
    )
    asyncio.new_event_loop().run_until_complete(client.connect())

    try:
        assert client.rpc().add(2, 3) == 5
        assert client.rpc().echo({'k': 'v'}) == {'k': 'v'}

        with pytest.raises(ValueError) as exc:
            client.rpc().raise_value()
        assert 'bad value' in str(exc.value)

        with pytest.raises(ConnectionError):
            client.rpc().raise_connection()

        with pytest.raises(CustomBusinessError):
            client.rpc().raise_custom()
    finally:
        client.close()
        loop = server_obj.get('loop')
        if loop:
            loop.call_soon_threadsafe(loop.stop)
        t.join(timeout=3.0)
