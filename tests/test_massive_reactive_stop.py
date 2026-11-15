"""`MassiveReactive.stop()` closes the stream it started.

docs/AUDIT_2026-11-14.md B2.4. `WebSocketClient.close` is a coroutine
function. `stop()` called it and dropped the coroutine, so nothing was ever
closed: the socket stayed open and the stream thread kept running behind
completed subjects (in the REPL, a second `stream` then holds two connections
against the vendor's connection limit).

Two tests of the same thing: a fake client whose `close` is `async`, and the
real `massive.WebSocketClient` against a websocket server on 127.0.0.1 (no
vendor is contacted). The second exists because a socket belongs to the event
loop that opened it — awaiting `close()` on some other loop would satisfy a
fake and still not close anything.
"""
import asyncio
import gc
import json
import threading
import time
import warnings

import pytest
from massive import WebSocketClient
from websockets.asyncio.server import serve

from trader.listeners import massive_reactive
from trader.listeners.massive_reactive import MassiveReactive

WAIT = 5.0


def _reactive():
    return MassiveReactive(massive_api_key='test_key', massive_feed='stocks', massive_delayed=False,
                           duckdb_path='', universe_library='Universes')


def _completions(reactive):
    done = []
    for name in ('agg', 'trade', 'quote'):
        getattr(reactive, f'{name}_subject').subscribe(on_completed=lambda name=name: done.append(name))
    return done


# --------------------------------------------------------------------------
# a fake client whose close() is async
# --------------------------------------------------------------------------

class _FakeClient:
    """Shaped like massive.WebSocketClient: `run` blocks its thread on
    `asyncio.run(connect(...))`, and `close` is a coroutine function that only
    works on the loop the connection lives on."""

    instances = []
    to_deliver = []      # messages handed to the processor once connected

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.connected = threading.Event()
        self.finished = threading.Event()
        self.close_awaited = False
        self.closed_on_stream_loop = None
        self._loop = None
        self._closed = None
        _FakeClient.instances.append(self)

    def run(self, handle_msg, close_timeout=1, **kwargs):
        async def wrapper(messages):
            handle_msg(messages)
        asyncio.run(self.connect(wrapper, close_timeout, **kwargs))

    async def connect(self, processor, close_timeout=1, **kwargs):
        self._loop = asyncio.get_running_loop()
        self._closed = asyncio.Event()
        try:
            if self.to_deliver:
                await processor(list(self.to_deliver))
            self.connected.set()
            await self._closed.wait()
        finally:
            self.finished.set()

    async def close(self):
        self.close_awaited = True
        self.closed_on_stream_loop = asyncio.get_running_loop() is self._loop
        if self.closed_on_stream_loop:
            self._closed.set()


@pytest.fixture
def fake_client(monkeypatch):
    _FakeClient.instances.clear()
    _FakeClient.to_deliver = []
    monkeypatch.setattr(massive_reactive, 'WebSocketClient', _FakeClient)
    return _FakeClient


def test_stop_awaits_the_clients_async_close(fake_client):
    reactive = _reactive()
    done = _completions(reactive)
    reactive.start(['AAPL', 'MSFT'], data_type='A')
    client, = fake_client.instances
    assert client.kwargs['subscriptions'] == ['A.AAPL', 'A.MSFT']
    assert client.connected.wait(WAIT)
    thread = reactive._thread

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        reactive.stop()
        gc.collect()        # a dropped coroutine warns when it is collected

    never_awaited = [str(w.message) for w in caught if 'never awaited' in str(w.message)]
    assert not never_awaited, never_awaited
    assert client.close_awaited, 'stop() returned without awaiting WebSocketClient.close()'
    assert client.closed_on_stream_loop, 'close() was awaited on a loop that does not own the socket'
    assert client.finished.wait(WAIT), 'the stream was still running after stop()'
    thread.join(WAIT)
    assert not thread.is_alive()
    assert sorted(done) == ['agg', 'quote', 'trade']


def test_stop_before_start_only_completes_the_subjects(fake_client):
    reactive = _reactive()
    done = _completions(reactive)
    reactive.stop()
    assert sorted(done) == ['agg', 'quote', 'trade'] and fake_client.instances == []


def test_stop_twice_is_harmless(fake_client):
    reactive = _reactive()
    reactive.start(['AAPL'])
    client, = fake_client.instances
    assert client.connected.wait(WAIT)
    reactive.stop()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        reactive.stop()
        gc.collect()
    assert not [w for w in caught if 'never awaited' in str(w.message)]
    assert client.finished.is_set()


def test_stop_immediately_after_start_does_not_leave_a_stream_behind(fake_client):
    """No waiting for the connection: whichever of the two gets there first,
    the thread must not be left connected."""
    for _ in range(20):
        fake_client.instances.clear()
        reactive = _reactive()
        reactive.start(['AAPL'])
        thread = reactive._thread
        reactive.stop()
        thread.join(WAIT)
        assert not thread.is_alive()
        client, = fake_client.instances
        assert client.finished.is_set() or not client.connected.is_set()


def test_stop_called_from_a_message_handler_does_not_wait_on_itself(fake_client):
    """A subscriber that stops the stream from inside on_next runs on the
    stream's own thread: waiting there for the stream's loop would hang."""
    reactive = _reactive()
    reactive.start(['AAPL'])
    client, = fake_client.instances
    assert client.connected.wait(WAIT)
    took = []

    def from_the_stream_thread():
        began = time.monotonic()
        reactive.stop()
        took.append(time.monotonic() - began)

    client._loop.call_soon_threadsafe(from_the_stream_thread)
    assert client.finished.wait(WAIT)
    assert client.close_awaited and took and took[0] < 1.0
    reactive._thread.join(WAIT)
    assert not reactive._thread.is_alive()


def test_messages_reach_the_subjects_on_the_stream_thread(fake_client):
    from massive.websocket.models.models import EquityTrade
    fake_client.to_deliver = [EquityTrade(symbol='AAPL', price=270.5, size=10,
                                          timestamp=1793980800000000000)]
    reactive = _reactive()
    seen = []
    reactive.trade_subject.subscribe(on_next=lambda t: seen.append((t, threading.current_thread())))
    reactive.start(['AAPL'], data_type='T')
    client, = fake_client.instances
    assert client.connected.wait(WAIT)
    (ticker, on_thread), = seen
    assert ticker.contract.symbol == 'AAPL' and ticker.last == 270.5 and ticker.lastSize == 10.0
    assert on_thread is reactive._thread
    reactive.stop()
    assert client.finished.wait(WAIT)


# --------------------------------------------------------------------------
# the real client against a local server
# --------------------------------------------------------------------------

class _LocalFeed:
    """Speaks just enough of the vendor's protocol on 127.0.0.1: status,
    auth, then one aggregate per subscription request."""

    def __init__(self):
        self.port = None
        self.authed = threading.Event()
        self.subscribed = threading.Event()
        self.closed = threading.Event()
        self.close_code = None
        self._ready = threading.Event()
        self._stop = None
        self._loop = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self):
        self._thread.start()
        assert self._ready.wait(WAIT)
        return self

    def __exit__(self, *exc):
        self._loop.call_soon_threadsafe(self._stop.set)
        self._thread.join(WAIT)

    def _run(self):
        asyncio.run(self._serve())

    async def _serve(self):
        self._loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        async with serve(self._handler, '127.0.0.1', 0) as server:
            self.port = server.sockets[0].getsockname()[1]
            self._ready.set()
            await self._stop.wait()

    async def _handler(self, connection):
        try:
            await connection.send(json.dumps([{'ev': 'status', 'status': 'connected', 'message': 'ok'}]))
            auth = json.loads(await connection.recv())
            assert auth['action'] == 'auth'
            await connection.send(json.dumps([{'ev': 'status', 'status': 'auth_success', 'message': 'ok'}]))
            self.authed.set()
            async for raw in connection:
                request = json.loads(raw)
                if request.get('action') == 'subscribe':
                    self.subscribed.set()
                    await connection.send(json.dumps([{
                        'ev': 'A', 'sym': 'AAPL', 'v': 100, 'av': 5000, 'op': 270.0, 'vw': 270.4,
                        'o': 270.1, 'c': 270.5, 'h': 270.9, 'l': 270.0, 'a': 270.3, 'z': 50,
                        's': 1793980800000, 'e': 1793980860000}]))
        finally:
            await connection.wait_closed()
            self.close_code = connection.close_code
            self.closed.set()


@pytest.fixture
def local_feed(monkeypatch):
    with _LocalFeed() as feed:
        class _LocalClient(WebSocketClient):
            def __init__(self, **kwargs):
                kwargs.update(feed=f'127.0.0.1:{feed.port}', secure=False, max_reconnects=0)
                super().__init__(**kwargs)

            def _ssl_context(self):
                return None        # plain ws:// to the local server

        monkeypatch.setattr(massive_reactive, 'WebSocketClient', _LocalClient)
        yield feed


def test_stop_closes_the_real_clients_socket_and_ends_its_thread(local_feed):
    reactive = _reactive()
    ticks, done = [], _completions(reactive)
    got_tick = threading.Event()
    reactive.agg_subject.subscribe(on_next=lambda t: (ticks.append(t), got_tick.set()))

    reactive.start(['AAPL'], data_type='A')
    thread = reactive._thread
    assert local_feed.authed.wait(WAIT) and local_feed.subscribed.wait(WAIT)
    assert got_tick.wait(WAIT), 'no aggregate reached agg_subject'
    assert ticks[0].contract.symbol == 'AAPL' and ticks[0].close == 270.5
    assert not local_feed.closed.is_set()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        reactive.stop()
        gc.collect()

    assert not [str(w.message) for w in caught if 'never awaited' in str(w.message)]
    assert local_feed.closed.wait(WAIT), 'the server still has the connection open after stop()'
    assert local_feed.close_code == 1000, 'a normal closure, not a dropped connection'
    thread.join(WAIT)
    assert not thread.is_alive(), 'the stream thread outlived stop()'
    assert sorted(done) == ['agg', 'quote', 'trade']
