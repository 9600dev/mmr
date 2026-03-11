from contextlib import contextmanager
from datetime import date, time, timedelta, tzinfo
from functools import partial
from reactivex.disposable import Disposable
from reactivex.observable import Observable
from reactivex.observer import Observer
from reactivex.subject import Subject
from trader.common.logging_helper import setup_logging
from trader.common.reactivex import ObservableIterHelper
from typing import Any, Callable, cast, Coroutine, Dict, Generic, Optional, Tuple, Type, TypeVar

import asyncio
import datetime as dt
import dill
import functools
import inspect
import msgpack
import pandas as pd
import pyarrow as pa
import reactivex as rx
import reactivex.abc as abc
import struct
import threading
import time as time_
import types
import typing
import uuid
import zmq
import zmq.asyncio


logging = setup_logging(module_name='trader.messaging.clientserver')


T = TypeVar('T')
TSub = TypeVar('TSub')


# ---------------------------------------------------------------------------
# Serialization: msgpack + pyarrow IPC for DataFrames
# ---------------------------------------------------------------------------

def df_dumps(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


def df_loads(df_bytes: bytes) -> pd.DataFrame:
    reader = pa.ipc.open_stream(df_bytes)
    return reader.read_pandas()


# Custom msgpack ExtType codes
EXT_DATETIME = 1
EXT_DATE = 2
EXT_TIME = 3
EXT_TIMEDELTA = 4
EXT_DATAFRAME = 5
EXT_OBJECT = 6  # fallback using dill


def ext_pack(obj):
    """Custom packer for msgpack."""
    if isinstance(obj, dt.datetime):
        data = struct.pack('!d', obj.timestamp())
        tz = str(obj.tzinfo) if obj.tzinfo else ''
        data += tz.encode()
        return msgpack.ExtType(EXT_DATETIME, data)
    elif isinstance(obj, dt.date):
        return msgpack.ExtType(EXT_DATE, struct.pack('!HBB', obj.year, obj.month, obj.day))
    elif isinstance(obj, dt.time):
        return msgpack.ExtType(EXT_TIME, struct.pack('!BBBL', obj.hour, obj.minute, obj.second, obj.microsecond))
    elif isinstance(obj, dt.timedelta):
        return msgpack.ExtType(EXT_TIMEDELTA, struct.pack('!id', obj.days, obj.seconds + obj.microseconds / 1e6))
    elif isinstance(obj, pd.DataFrame):
        return msgpack.ExtType(EXT_DATAFRAME, df_dumps(obj))
    else:
        return msgpack.ExtType(EXT_OBJECT, dill.dumps(obj))


def ext_unpack(code, data):
    """Custom unpacker for msgpack."""
    if code == EXT_DATETIME:
        ts = struct.unpack('!d', data[:8])[0]
        tz_str = data[8:].decode()
        result = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        if tz_str:
            from dateutil.tz import gettz
            result = result.astimezone(gettz(tz_str))
        return result
    elif code == EXT_DATE:
        y, m, d = struct.unpack('!HBB', data)
        return dt.date(y, m, d)
    elif code == EXT_TIME:
        h, m, s, us = struct.unpack('!BBBL', data)
        return dt.time(h, m, s, us)
    elif code == EXT_TIMEDELTA:
        days, secs = struct.unpack('!id', data)
        return dt.timedelta(days=days, seconds=secs)
    elif code == EXT_DATAFRAME:
        return df_loads(data)
    elif code == EXT_OBJECT:
        return dill.loads(data)
    return msgpack.ExtType(code, data)


def pack(obj) -> bytes:
    return msgpack.packb(obj, default=ext_pack, use_bin_type=True)


def unpack(data: bytes):
    return msgpack.unpackb(data, ext_hook=ext_unpack, raw=False)


# ---------------------------------------------------------------------------
# RPC decorator and handler base class
# ---------------------------------------------------------------------------

def rpcmethod(func):
    """Decorator marking a method as an RPC endpoint."""
    func._is_rpc_method = True
    return func


class RPCHandler:
    """Base class for RPC service handlers.

    Provides backward-compatible ``rpcmethod`` class-method decorator so that
    existing ``@RPCHandler.rpcmethod`` usage keeps working.
    """

    @classmethod
    def rpcmethod(cls, func):
        return rpcmethod(func)


# ---------------------------------------------------------------------------
# Helper: return-type conversion (msgpack turns lists to tuples etc.)
# ---------------------------------------------------------------------------

def _convert_return_type(obj, return_type):
    if isinstance(obj, tuple) and hasattr(return_type, '__origin__') and return_type.__origin__ == list:
        inner = typing.get_args(return_type)
        if inner:
            return [_convert_return_type(o, inner[0]) for o in obj]
        return list(obj)
    elif return_type == type(list):
        return list(obj)
    elif isinstance(obj, tuple) and hasattr(return_type, '__origin__') and return_type.__origin__ is tuple:
        return return_type(obj)
    # for whatever reason, things like PortfolioSummary are returned as tuples,
    # so we have to new them up with *args
    elif isinstance(obj, tuple):
        return return_type(*obj)
    else:
        return obj


# ---------------------------------------------------------------------------
# consume() helper
# ---------------------------------------------------------------------------

C = TypeVar('C')


def consume(coro: Coroutine[Any, Any, C]) -> C:
    """Helper to consume an RPC coroutine that is actually just a remote call.

    In theory *coro* should never actually be a coroutine when called via the
    synchronous ``RPCClient.rpc()`` path, but this is useful for the type-checker.
    """
    if inspect.iscoroutine(coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)
    else:
        return cast(C, coro)


# ---------------------------------------------------------------------------
# PubSubSubject / _Handler (kept as-is for TopicPubSub)
# ---------------------------------------------------------------------------

class PubSubSubject(Subject[T]):
    def on_error(self, error: Exception) -> None:
        self.check_disposed()

        if self.is_stopped:
            return

        for obv in list(self.observers):
            obv.on_error(error)


class _Handler(Generic[T]):
    def __init__(self):
        self.subject = PubSubSubject[T]()

    def on_message(self, obj):
        self.subject.on_next(obj)

    def on_throw(self, ex):
        self.subject.on_error(ex)

    def on_close(self):
        self.subject.on_completed()

    def get_subject(self):
        return self.subject


# ---------------------------------------------------------------------------
# _SyncMethodCall  (replaces _AwaitedMethodCall)
# ---------------------------------------------------------------------------

class _SyncMethodCall:
    __slots__ = ('_socket', '_lock', '_timeout', '_names', '_return_type')

    def __init__(self, socket, lock, timeout=None, names=(), return_type: Optional[Type] = None):
        self._socket = socket
        self._lock = lock
        self._timeout = timeout
        self._names = names
        self._return_type = return_type

    def __getattr__(self, name):
        return _SyncMethodCall(self._socket, self._lock, self._timeout,
                               self._names + (name,), self._return_type)

    def __call__(self, *args, **kwargs):
        if not self._names:
            raise ValueError('RPC method name is empty')

        req_id = str(uuid.uuid4())
        request = {
            'method': '.'.join(self._names),
            'args': args,
            'kwargs': kwargs,
            'req_id': req_id,
        }

        with self._lock:
            self._socket.send(pack(request))
            # Server sends multipart [empty_delimiter, payload] via ROUTER→DEALER.
            # Poll in short intervals so Ctrl+C can interrupt instead of
            # blocking for the full RCVTIMEO duration inside C code.
            poller = zmq.Poller()
            poller.register(self._socket, zmq.POLLIN)
            timeout_ms = (self._timeout or 10) * 1000
            elapsed = 0
            poll_interval = 250  # ms
            while elapsed < timeout_ms:
                ready = poller.poll(poll_interval)
                if ready:
                    break
                elapsed += poll_interval
            else:
                raise TimeoutError(f'RPC call timed out after {timeout_ms}ms')
            frames = self._socket.recv_multipart(zmq.NOBLOCK)
            response_data = frames[-1]

        response = unpack(response_data)

        if response.get('error'):
            exc_type = response.get('exc_type', 'Exception')
            exc_args = response.get('exc_args', ())
            raise Exception(f"RPC error ({exc_type}): {exc_args}")

        result = response.get('result')

        # Handle return type conversion (msgpack converts lists to tuples etc.)
        if self._return_type and result is not None:
            result = _convert_return_type(result, self._return_type)

        return result


# ---------------------------------------------------------------------------
# RPCServer
# ---------------------------------------------------------------------------

class RPCServer(Generic[T]):
    def __init__(
        self,
        instance: T,
        zmq_rpc_server_address: str = 'tcp://127.0.0.1',
        zmq_rpc_server_port: int = 42000,
        **kwargs,
    ):
        self.instance = instance
        self.address = f"{zmq_rpc_server_address}:{zmq_rpc_server_port}"
        self.ctx = zmq.asyncio.Context()
        self.socket: Optional[zmq.asyncio.Socket] = None
        self._serve_task: Optional[asyncio.Task] = None

    async def serve(self):
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.bind(self.address)
        self._serve_task = asyncio.create_task(self._serve_loop())

    async def _serve_loop(self):
        while True:
            try:
                frames = await self.socket.recv_multipart()
                # frames: [client_id, empty, request_data]
                client_id = frames[0]
                request = unpack(frames[-1])
                # request = {'method': 'dotted.name', 'args': [...], 'kwargs': {...}, 'req_id': uuid}
                asyncio.create_task(self._handle_request(client_id, request))
            except zmq.ZMQError as e:
                logging.debug(f"RPCServer ZMQ error in serve loop: {e}")
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.exception(f"RPCServer unexpected error: {e}")

    async def _handle_request(self, client_id: bytes, request: dict):
        method_name = request['method']
        args = request.get('args', ())
        kwargs = request.get('kwargs', {})
        req_id = request.get('req_id', '')

        # Convert args from list to tuple if needed (msgpack gives us lists)
        if isinstance(args, list):
            args = tuple(args)

        try:
            # Navigate dotted method names
            obj = self.instance
            for part in method_name.split('.'):
                obj = getattr(obj, part)

            result = obj(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result

            response = {'req_id': req_id, 'result': result, 'error': False}
        except Exception as exc:
            logging.debug(f"RPCServer error handling {method_name}: {exc}")
            response = {
                'req_id': req_id,
                'error': True,
                'exc_type': type(exc).__name__,
                'exc_args': exc.args,
            }

        try:
            await self.socket.send_multipart([client_id, b'', pack(response)])
        except Exception as exc:
            logging.exception(f"RPCServer failed to send response: {exc}")

    def close(self):
        if self._serve_task:
            self._serve_task.cancel()
        if self.socket:
            self.socket.close()


# ---------------------------------------------------------------------------
# RPCClient
# ---------------------------------------------------------------------------

class RPCClient(Generic[T]):
    def __init__(
        self,
        zmq_server_address: str = 'tcp://127.0.0.1',
        zmq_server_port: int = 42000,
        timeout: Optional[int] = None,
        error_table: Optional[Dict[str, Exception]] = None,
        **kwargs,
    ):
        self.zmq_server_address = zmq_server_address
        self.zmq_server_port = zmq_server_port
        self.address = f"{zmq_server_address}:{zmq_server_port}"
        self.ctx = zmq.Context()  # sync context for sync calls
        self.socket: Optional[zmq.Socket] = None
        self.timeout: Optional[int] = timeout
        self.is_setup: bool = False
        self._lock = threading.Lock()
        self.error_table = error_table

    async def connect(self, loop=None):
        logging.debug('trying RPCClient.connect()')
        self.socket = self.ctx.socket(zmq.DEALER)
        self.socket.connect(self.address)
        if self.timeout:
            self.socket.setsockopt(zmq.RCVTIMEO, self.timeout * 1000)
            self.socket.setsockopt(zmq.SNDTIMEO, self.timeout * 1000)
        self.is_setup = True

    def rpc(self, return_type: Optional[Type] = None) -> T:
        if self.socket and self.is_setup:
            return _SyncMethodCall(self.socket, self._lock, self.timeout, return_type=return_type)  # type: ignore
        else:
            raise ConnectionError('not connected')

    def arpc(self, return_type: Optional[Type] = None) -> T:
        if self.socket and self.is_setup:
            return _SyncMethodCall(self.socket, self._lock, self.timeout, return_type=return_type)  # type: ignore
        else:
            raise ConnectionError('not connected')

    def consume(self, future):
        if future is asyncio.Future:
            return future.result()
        else:
            return future

    def close(self):
        if self.socket:
            self.socket.close()


# ---------------------------------------------------------------------------
# MessageBusServer  (raw zmq ROUTER, replacing aiozmq.create_zmq_stream)
# ---------------------------------------------------------------------------

class MessageBusServer:
    def __init__(
        self,
        zmq_address: str,
        zmq_port: int,
        **kwargs,
    ):
        self.zmq_address = zmq_address
        self.zmq_port = zmq_port
        self.lock = threading.Lock()

        self._sentinel = ('stop', 'stop', 'stop')
        self.sentinel_flag: bool = True
        self.clients: Dict[Tuple[str, str], bool] = {}
        self.server: Optional[zmq.asyncio.Socket] = None
        self.read_task: Optional[asyncio.Task] = None
        self.read_loop = None

        self.read_thread: Optional[threading.Thread] = None
        self.message_thread: Optional[threading.Thread] = None
        self.ctx = zmq.asyncio.Context()

    @staticmethod
    def message_subscribe(topic):
        return [topic.encode() if type(topic) is str else topic, b'subscribe']

    @staticmethod
    def message_disconnect():
        return [b'disconnect', b'disconnect']

    @staticmethod
    def message(topic, val: Any):
        return [topic.encode() if type(topic) is str else topic, pack(val)]

    def put(self, topic_item):
        self.wait_handle.loop.call_soon_threadsafe(self.wait_handle.queue.put_nowait, topic_item)  # type: ignore

    def write(self, topic: str, val: Any):
        self.put((topic, val))

    async def __message(self, client_id: bytes, topic: bytes, val: bytes):
        if not self.server:
            raise ValueError('server is not initialized')

        subscribe_marker = b'subscribe'
        disconnect_marker = b'disconnect'

        if val == subscribe_marker and (client_id, topic) not in self.clients:
            self.clients[(client_id, topic)] = True
            return

        if val == disconnect_marker:
            # remove all subscriptions for this client
            for (subscribed_client_id, subscribed_topic) in list(self.clients.keys()):
                if client_id == subscribed_client_id:
                    self.clients.pop((subscribed_client_id, subscribed_topic))
            return

        client_ids = [cid for (cid, t), _ in self.clients.items() if cid != client_id and t == topic]
        for cid in client_ids:
            await self.server.send_multipart([cid, topic, val])

    def __combined_loop(self, wait_handle: threading.Event):
        async def main():
            ctx = zmq.asyncio.Context()
            self.server = ctx.socket(zmq.ROUTER)
            self.server.bind(f'{self.zmq_address}:{self.zmq_port}')

            wait_handle.loop = asyncio.get_running_loop()  # type: ignore
            wait_handle.queue = asyncio.Queue()  # type: ignore
            wait_handle.set()

            async def read_messages():
                while not self.sentinel_flag:
                    try:
                        frames = await self.server.recv_multipart()
                        if len(frames) >= 3:
                            await self.__message(frames[0], frames[1], frames[2])
                        elif len(frames) == 2:
                            await self.__message(frames[0], frames[1], b'')
                    except zmq.ZMQError:
                        break
                    except Exception as ex:
                        logging.exception(f"MessageBusServer read error: {ex}")
                        break

            async def process_writes():
                queue = wait_handle.queue
                while True:
                    item = await queue.get()
                    if item == self._sentinel:
                        queue.task_done()
                        self.sentinel_flag = True
                        break
                    client_id = item[0]
                    topic = item[1]
                    val = item[2]
                    await self.__message(client_id, topic, val)
                    queue.task_done()

            await asyncio.gather(read_messages(), process_writes())

        self.read_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.read_loop)
        self.read_loop.run_until_complete(main())

    async def start(self):
        logging.debug('starting MessageBus server work queue')

        if self.server and self.sentinel_flag is False:
            raise ValueError('server already started')

        self.wait_handle = threading.Event()

        self.sentinel_flag = False
        self.read_thread = threading.Thread(target=self.__combined_loop, args=(self.wait_handle,))
        self.read_thread.start()

        self.wait_handle.wait()

    def stop(self):
        if self.server is None or self.read_loop is None:
            raise ValueError('server not initialized')

        self.sentinel_flag = True
        self.put(self._sentinel)  # type: ignore

        if self.server:
            self.server.close()

        loop = self.read_loop
        if loop.is_running():
            loop.call_soon_threadsafe(loop.stop)

        asyncio.run(asyncio.sleep(0.1))

    async def wait(self):
        while not self.sentinel_flag:
            await asyncio.sleep(1)


# ---------------------------------------------------------------------------
# MessageBusClient  (raw zmq DEALER, replacing aiozmq.create_zmq_stream)
# ---------------------------------------------------------------------------

class MessageBusClient(Generic[T]):
    def __init__(
        self,
        zmq_address: str,
        zmq_port: int,
    ):
        self.zmq_address = zmq_address
        self.zmq_port = zmq_port
        self.client: Optional[zmq.asyncio.Socket] = None
        self.ctx = zmq.asyncio.Context()

    async def connect(self) -> None:
        self.client = self.ctx.socket(zmq.DEALER)
        self.client.connect(f'{self.zmq_address}:{self.zmq_port}')

    async def __iterable_read(self):
        while True:
            try:
                yield await self.read()
            except zmq.ZMQError:
                return
            except Exception as ex:
                raise ex

    def subscribe(
        self,
        topic: str,
        observer: abc.ObserverBase[T],
    ) -> abc.DisposableBase:
        if not self.client:
            raise ValueError('client is not initialized')

        self.client.send_multipart(MessageBusServer.message_subscribe(topic))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
        return ObservableIterHelper(loop).from_aiter(self.__iterable_read()).subscribe(observer)

    def subscribe_topic(self, topic: str) -> None:
        if not self.client:
            raise ValueError('client is not initialized')

        self.client.send_multipart(MessageBusServer.message_subscribe(topic))

    def write(self, topic: str, val: T) -> None:
        if not self.client:
            raise ValueError('client is not initialized')

        self.client.send_multipart(MessageBusServer.message(topic, val))

    async def disconnect(self) -> None:
        if not self.client:
            raise ValueError('client is not initialized')

        self.client.send_multipart(MessageBusServer.message_disconnect())
        # pump the loop
        await asyncio.sleep(1.0)
        self.client.close()

    async def read(self) -> T:
        if not self.client:
            raise ValueError('client is not initialized')

        frames = await self.client.recv_multipart()
        # frames: [topic, val]
        val = frames[-1]
        try:
            return unpack(val)
        except Exception:
            # Fall back to dill for backward compatibility with in-flight messages
            return dill.loads(val)


# ---------------------------------------------------------------------------
# TopicPubSub  (raw zmq PUB/SUB replacing aiozmq.rpc pubsub)
# ---------------------------------------------------------------------------

class TopicPubSub(Generic[T]):
    def __init__(
        self,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        **kwargs,
    ):
        self.zmq_server_address = zmq_pubsub_server_address
        self.zmq_server_port = zmq_pubsub_server_port
        self.handler: Optional[_Handler[T]] = None
        self.zmq_subscriber: Optional[zmq.asyncio.Socket] = None
        self.zmq_publisher: Optional[zmq.Socket] = None
        self.lock = threading.Lock()
        self._sub_task: Optional[asyncio.Task] = None
        self._sub_ctx: Optional[zmq.asyncio.Context] = None
        self._pub_ctx: Optional[zmq.Context] = None

    @contextmanager
    def aquire_timeout(self, lock: threading.Lock, timeout):
        result = lock.acquire(timeout=timeout)
        try:
            yield result
        finally:
            if result:
                lock.release()

    async def subscriber(
        self,
        topic: str = 'default',
    ) -> rx.Observable[T]:
        if not self.handler:
            self.handler = _Handler[T]()
            self._sub_ctx = zmq.asyncio.Context()
            self.zmq_subscriber = self._sub_ctx.socket(zmq.SUB)
            self.zmq_subscriber.connect(f'{self.zmq_server_address}:{self.zmq_server_port}')
            self.zmq_subscriber.setsockopt_string(zmq.SUBSCRIBE, topic)

            self._sub_task = asyncio.create_task(self._subscriber_loop())

        return self.handler.get_subject()

    async def _subscriber_loop(self):
        while True:
            try:
                frames = await self.zmq_subscriber.recv_multipart()
                # frames: [topic, payload]
                if len(frames) >= 2:
                    payload = frames[1]
                    try:
                        obj = unpack(payload)
                    except Exception:
                        obj = dill.loads(payload)
                    self.handler.on_message(obj)
            except zmq.ZMQError:
                break
            except asyncio.CancelledError:
                break
            except Exception as ex:
                logging.exception(f"TopicPubSub subscriber error: {ex}")
                self.handler.on_throw(ex)

    def subscriber_close(self):
        if self._sub_task:
            self._sub_task.cancel()
        if self.zmq_subscriber:
            logging.debug('subscriber_close()')
            self.zmq_subscriber.close()

    async def publisher(
        self,
        obj: T,
        topic: str = 'default',
    ):
        try:
            if not self.zmq_publisher:
                self._pub_ctx = zmq.Context()
                self.zmq_publisher = self._pub_ctx.socket(zmq.PUB)
                self.zmq_publisher.bind(f'{self.zmq_server_address}:{self.zmq_server_port}')
                # Give subscribers time to connect
                await asyncio.sleep(0.1)

            topic_bytes = topic.encode() if isinstance(topic, str) else topic
            payload = pack(obj)
            self.zmq_publisher.send_multipart([topic_bytes, payload])
        except Exception as e:
            logging.exception(e)
            logging.debug(
                f'self.zmq_server_address: {self.zmq_server_address}, '
                f'self.zmq_server_port: {self.zmq_server_port}'
            )
            raise e

    async def publisher_close(self):
        if self.zmq_publisher:
            logging.debug('publisher_close()')
            self.zmq_publisher.close()


# ---------------------------------------------------------------------------
# MultithreadedTopicPubSub
# ---------------------------------------------------------------------------

class MultithreadedTopicPubSub(Generic[T], TopicPubSub[T]):
    def __init__(
        self,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        **kwargs,
    ):
        super().__init__(
            zmq_pubsub_server_address,
            zmq_pubsub_server_port,
        )
        self.wait_handle = threading.Event()
        self._sentinel = ('stop', 'stop')

    def put(self, topic_item: Tuple[str, T]):
        self.wait_handle.loop.call_soon_threadsafe(self.wait_handle.queue.put_nowait, topic_item)  # type: ignore

    def _publisher_loop(self, wait_handle: threading.Event):
        try:
            if not self.zmq_publisher:
                with self.aquire_timeout(self.lock, 5) as acquired:
                    # double check lock
                    if acquired and not self.zmq_publisher:
                        logging.debug(
                            f'clientserver.publisher() self.zmq_server_address: {self.zmq_server_address}, '
                            f'self.zmq_server_port: {self.zmq_server_port}'
                        )
                        self._pub_ctx = zmq.Context()
                        self.zmq_publisher = self._pub_ctx.socket(zmq.PUB)
                        self.zmq_publisher.bind(f'{self.zmq_server_address}:{self.zmq_server_port}')
        except Exception as e:
            logging.exception(e)
            raise e

        async def main():
            wait_handle.loop = asyncio.get_running_loop()  # type: ignore
            wait_handle.queue = task_queue = asyncio.Queue()  # type: ignore
            wait_handle.set()

            while True:
                item = await task_queue.get()
                if item == self._sentinel:
                    task_queue.task_done()
                    break
                topic = item[0]
                val = item[1]

                task = asyncio.create_task(self.publisher(val, topic))
                task.add_done_callback(lambda _: task_queue.task_done())
            await task_queue.join()

        asyncio.run(main())

    def start(self):
        logging.debug('starting _publisher_loop')
        self.wait_handle = threading.Event()

        th = threading.Thread(target=self._publisher_loop, args=(self.wait_handle,))
        th.start()
        self.wait_handle.wait()
        logging.debug('started _publisher_loop')

    def stop(self):
        self.put(self._sentinel)  # type: ignore
