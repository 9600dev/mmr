"""Tests for trader.messaging.clientserver — error-preservation, dill policy,
and a lightweight end-to-end RPC round-trip.

Network-bound tests set up a server and client on the same event loop in the
main thread to avoid the cross-thread-loop flakiness the previous revision had.
"""

import asyncio
import socket
import typing

import pytest

import dill
import msgpack

from trader.messaging.clientserver import (
    DillDeserializationError,
    EXT_OBJECT,
    RPCClient,
    RPCError,
    RPCHandler,
    RPCServer,
    _convert_return_type,
    _reconstruct_rpc_exception,
    ext_pack,
    pack,
    rpcmethod,
    set_dill_whitelist,
    unpack,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class CustomBusinessError(Exception):
    pass


class _Row(typing.NamedTuple):
    """Stand-in for ib_async NamedTuples (PortfolioItem, Position, ...) —
    tuple subclasses that msgpack flattens to plain arrays on the wire."""
    conid: int
    price: float


class _Service(RPCHandler):
    @rpcmethod
    def add(self, a: int, b: int) -> int:
        return a + b

    @rpcmethod
    def echo(self, payload):
        return payload

    @rpcmethod
    def rows(self):
        return [_Row(conid=101, price=1.5), _Row(conid=202, price=2.5)]

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
# Pure unit tests on return-type conversion (no sockets, no threads)
# ---------------------------------------------------------------------------

class TestConvertReturnType:
    """NamedTuples cross the wire as plain lists (msgpack flattens tuple
    subclasses to arrays; unpackb(use_list=True) yields lists). The
    strategies-pnl "unreachable" bug was this conversion only firing on
    tuples, so list payloads passed through unreconstructed."""

    def test_list_of_lists_reconstructs_namedtuples(self):
        wire = [[101, 1.5], [202, 2.5]]
        out = _convert_return_type(wire, list[_Row])
        assert out == [_Row(101, 1.5), _Row(202, 2.5)]
        assert all(isinstance(r, _Row) for r in out)

    def test_top_level_list_reconstructs_namedtuple(self):
        assert _convert_return_type([101, 1.5], _Row) == _Row(101, 1.5)

    def test_top_level_tuple_still_reconstructs(self):
        assert _convert_return_type((101, 1.5), _Row) == _Row(101, 1.5)

    def test_real_objects_pass_through_unchanged(self):
        # Dataclasses/objects arrive intact via the dill ExtType path and
        # must not be touched.
        obj = CustomBusinessError('already deserialized')
        assert _convert_return_type([obj], list[CustomBusinessError])[0] is obj

    def test_primitive_lists_pass_through(self):
        assert _convert_return_type([1, 2, 3], list[int]) == [1, 2, 3]
        assert _convert_return_type([{'a': 1}], list[dict]) == [{'a': 1}]

    def test_dict_passes_through(self):
        d = {'k': 'v'}
        assert _convert_return_type(d, dict) is d


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

    def test_dill_deserialization_error_reconstructs(self):
        """A server that refuses an undeserializable payload replies with
        exc_type='DillDeserializationError'; the client must reconstruct the
        real type, not a generic RPCError."""
        exc = _reconstruct_rpc_exception(
            'DillDeserializationError', ('refusing global os.system',),
            error_table=None,
        )
        assert isinstance(exc, DillDeserializationError)
        assert 'os.system' in str(exc)


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

        # NamedTuples flatten to plain lists on the wire...
        raw = client.rpc().rows()
        assert raw == [[101, 1.5], [202, 2.5]]
        # ...and return_type reconstructs them (the strategies-pnl regression).
        typed = client.rpc(return_type=list[_Row]).rows()
        assert typed == [_Row(101, 1.5), _Row(202, 2.5)]
        assert all(isinstance(r, _Row) for r in typed)

        with pytest.raises(ValueError) as exc:
            client.rpc().raise_value()
        assert 'bad value' in str(exc.value)

        with pytest.raises(ConnectionError):
            client.rpc().raise_connection()

        with pytest.raises(CustomBusinessError):
            client.rpc().raise_custom()
    finally:
        client.close()
        client.ctx.term()
        loop = server_obj.get('loop')
        if loop:
            # Close the server's ROUTER socket on its own loop BEFORE stopping.
            # A leaked open socket makes any later zmq Context GC finalizer
            # (ctx.term) block forever — the interpreter hangs at exit.
            server = server_obj.get('server')
            if server:
                loop.call_soon_threadsafe(server.close)
            loop.call_soon_threadsafe(loop.stop)
        t.join(timeout=3.0)


# ---------------------------------------------------------------------------
# Fail-silent fix: a rejected/undeserializable request must produce an error
# REPLY (client sees an exception) instead of a dropped request (client hangs
# to TimeoutError with no clue why).
# ---------------------------------------------------------------------------

def test_server_replies_error_on_undeserializable_request():
    import threading
    import time as _t

    import zmq

    port = _free_port()
    ready = threading.Event()
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

    t = threading.Thread(target=_run_server, daemon=True)
    t.start()
    assert ready.wait(timeout=3.0), 'server thread did not signal ready'
    _t.sleep(0.1)

    # Craft a request whose framing is valid msgpack (so req_id is recoverable)
    # but whose args carry a poisoned EXT_OBJECT dill blob that the restricted
    # unpickler refuses. os.system would run if the boundary let it.
    class _Evil:
        def __reduce__(self):
            import os
            return (os.system, ('echo pwned',))

    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f'tcp://127.0.0.1:{port}')
    try:
        req_id = 'poison-req-1'
        poisoned = {
            'method': 'echo',
            'args': [msgpack.ExtType(EXT_OBJECT, dill.dumps(_Evil()))],
            'kwargs': {},
            'req_id': req_id,
        }
        wire = msgpack.packb(poisoned, default=ext_pack, use_bin_type=True)
        sock.send_multipart([b'', wire])

        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        assert poller.poll(3000), 'server dropped the request silently (no reply)'
        frames = sock.recv_multipart()
        reply = unpack(frames[-1])
        assert reply['req_id'] == req_id
        assert reply['error'] is True
        assert reply['exc_type'] == 'DillDeserializationError'
        # The client-side reconstructor turns that into the real exception.
        exc = _reconstruct_rpc_exception(reply['exc_type'], reply['exc_args'], None)
        assert isinstance(exc, DillDeserializationError)
    finally:
        sock.close()
        ctx.term()
        loop = server_obj.get('loop')
        if loop:
            server = server_obj.get('server')
            if server:
                loop.call_soon_threadsafe(server.close)
            loop.call_soon_threadsafe(loop.stop)
        t.join(timeout=3.0)


# ---------------------------------------------------------------------------
# Fail-silent fix: one poisoned PubSub message must be dropped (logged at
# ERROR) and the subscription must SURVIVE — good messages before and after
# it are still delivered. Previously a single bad payload called on_error and
# terminated every downstream ticker subscription.
# ---------------------------------------------------------------------------

def test_pubsub_subscription_survives_poisoned_message():
    from trader.messaging.clientserver import TopicPubSub

    port = _free_port()

    async def _run():
        received = []
        errors = []
        completed = []

        pubsub = TopicPubSub(
            zmq_pubsub_server_address='tcp://127.0.0.1',
            zmq_pubsub_server_port=port,
        )
        subject = await pubsub.subscriber('t')
        subject.subscribe(
            on_next=received.append,
            on_error=errors.append,
            on_completed=lambda: completed.append(True),
        )

        import zmq
        import zmq.asyncio
        pub_ctx = zmq.asyncio.Context()
        pub = pub_ctx.socket(zmq.PUB)
        pub.bind(f'tcp://127.0.0.1:{port}')
        # slow-joiner: let the SUB connection establish before publishing
        await asyncio.sleep(0.3)

        topic = b't'

        class _Evil:
            def __reduce__(self):
                import os
                return (os.system, ('echo pwned',))

        # good, poison, good
        await pub.send_multipart([topic, pack({'seq': 1})])
        await asyncio.sleep(0.05)
        await pub.send_multipart(
            [topic, msgpack.packb(msgpack.ExtType(EXT_OBJECT, dill.dumps(_Evil())),
                                  use_bin_type=True)])
        await asyncio.sleep(0.05)
        await pub.send_multipart([topic, pack({'seq': 2})])
        await asyncio.sleep(0.3)

        pubsub.subscriber_close()
        pub.close()
        pub_ctx.term()
        if pubsub._sub_ctx:
            pubsub._sub_ctx.term()
        return received, errors, completed

    received, errors, completed = asyncio.run(_run())

    seqs = [m['seq'] for m in received if isinstance(m, dict) and 'seq' in m]
    assert seqs == [1, 2], f'poisoned message broke delivery, got {seqs}'
    assert not errors, 'subscription terminated with on_error on a poisoned message'
    assert not completed, 'subscription completed early'
