"""Bounded transport lifecycle regressions; every socket uses a random port."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import queue
import socket
import threading
import time

import pytest
import zmq

from trader.messaging.clientserver import (
    MessageBusClient, MessageBusServer, MultithreadedTopicPubSub, RPCClient, TopicPubSub,
    RPCServer,
)


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def test_rpc_lock_wait_uses_call_deadline():
    client = RPCClient(zmq_server_port=free_port(), timeout=.1)
    asyncio.run(client.connect())
    client._lock.acquire()
    try:
        start = time.monotonic()
        with ThreadPoolExecutor(max_workers=1) as worker:
            call = worker.submit(client.rpc().ping)
            with pytest.raises(ConnectionError, match='before sending'):
                call.result(timeout=1)
        assert time.monotonic() - start < 1
    finally:
        client._lock.release()
        client.close()


def test_message_bus_bind_failure_is_propagated():
    ctx = zmq.Context()
    holder = ctx.socket(zmq.ROUTER)
    port = holder.bind_to_random_port('tcp://127.0.0.1')
    bus = MessageBusServer('tcp://127.0.0.1', port)
    try:
        with pytest.raises(RuntimeError, match='could not start'):
            asyncio.run(bus.start(timeout=1))
        assert not bus.read_thread.is_alive()
    finally:
        bus.stop()
        holder.close(linger=0)
        ctx.term()


def test_message_bus_routes_server_writes_and_releases_port():
    port = free_port()
    bus = MessageBusServer('tcp://127.0.0.1', port)
    asyncio.run(bus.start())
    ctx = zmq.Context()
    client = ctx.socket(zmq.DEALER)
    client.connect(f'tcp://127.0.0.1:{port}')
    try:
        client.send_multipart([b'test', b'subscribe'])
        deadline = time.monotonic() + 2
        while not bus.clients and time.monotonic() < deadline:
            time.sleep(.01)
        assert bus.clients
        bus.write('test', {'value': 5})
        assert client.poll(1000)
        topic, payload = client.recv_multipart()
        from trader.messaging.clientserver import unpack
        assert topic == b'test' and unpack(payload) == {'value': 5}
        bus.stop()
        assert not bus.read_thread.is_alive()
        replacement = ctx.socket(zmq.ROUTER)
        replacement.bind(f'tcp://127.0.0.1:{port}')
        replacement.close(linger=0)
    finally:
        bus.stop()
        client.close(linger=0)
        ctx.term()


def test_publisher_queue_is_bounded_and_worker_closes_socket(monkeypatch):
    import trader.messaging.clientserver as cs
    publisher = MultithreadedTopicPubSub('tcp://127.0.0.1', free_port(), queue_size=1)
    entered, release = threading.Event(), threading.Event()
    real_pack = cs.pack

    def paused_pack(value):
        entered.set()
        assert release.wait(3)
        return real_pack(value)

    monkeypatch.setattr(cs, 'pack', paused_pack)
    publisher.start()
    try:
        publisher.put(('test', 1))
        assert entered.wait(1)
        publisher.put(('test', 2))
        with pytest.raises(queue.Full):
            publisher.put(('test', 3))
        assert publisher.rejected_messages == 1
    finally:
        release.set()
        publisher.stop()
    assert not publisher._thread.is_alive()
    assert publisher.zmq_publisher is None


def test_pubsub_close_releases_contexts_and_allows_same_instance_restart():
    async def exercise():
        transport = TopicPubSub('tcp://127.0.0.1', free_port())
        first_subject = await transport.subscriber('test')
        await transport.publisher(1, 'test')
        first_sub_context, first_pub_context = transport._sub_ctx, transport._pub_ctx
        transport.subscriber_close()
        await transport.publisher_close()
        await asyncio.sleep(0)
        assert first_sub_context.closed and first_pub_context.closed
        second_subject = await transport.subscriber('test')
        try:
            await transport.publisher(2, 'test')
            assert second_subject is not first_subject
            assert transport.zmq_subscriber is not None
            assert transport.zmq_publisher is not None
        finally:
            transport.subscriber_close()
            await transport.publisher_close()
    asyncio.run(exercise())


def test_message_bus_disconnect_closes_missing_peer_and_can_reconnect():
    async def exercise():
        client = MessageBusClient('tcp://127.0.0.1', free_port())
        for _ in range(2):
            await client.connect()
            context = client.ctx
            started = time.monotonic()
            await client.disconnect()
            assert time.monotonic() - started < 1
            assert client.client is None and context.closed
        await client.disconnect()  # shutdown is idempotent
    asyncio.run(exercise())


def test_rpc_server_async_close_drains_tasks_and_can_restart():
    async def exercise():
        server = RPCServer(object(), zmq_rpc_server_port=free_port())
        for _ in range(2):
            await server.serve()
            task, context = server._serve_task, server.ctx
            await asyncio.sleep(0)
            await server.aclose()
            assert task.done() and context.closed
            assert not server._requests and server.socket is None
    asyncio.run(exercise())
