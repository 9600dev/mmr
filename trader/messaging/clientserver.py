from aiozmq.rpc.base import ParametersError, Service
from aiozmq.rpc.packer import _Packer
from aiozmq.rpc.pubsub import PubSubClient, PubSubService
from aiozmq.rpc.rpc import _BaseServerProtocol, _ServerProtocol
from asyncio import CancelledError
from contextlib import contextmanager, suppress
from datetime import date, time, timedelta, tzinfo
from functools import partial
from msgpack import unpackb
from pickle import dumps, HIGHEST_PROTOCOL, loads
from reactivex.subject import Subject
from trader.common.logging_helper import setup_logging
from typing import Any, Callable, Dict, Generic, Optional, Tuple, Type, TypeVar

import aiozmq
import aiozmq.rpc
import asyncio
import ctypes
import dill
import inspect
import nest_asyncio
import pandas as pd
import pyarrow as pa
import reactivex as rx
import threading
import time as time_
import types
import typing
import uuid
import zmq


logging = setup_logging(module_name='trader.messaging.clientserver')


T = TypeVar('T')
TSub = TypeVar('TSub')


def df_dumps(df: pd.DataFrame):
    return pa.serialize_pandas(df).to_pybytes()


def df_loads(df_bytes: pd.DataFrame):
    return pa.deserialize_pandas(df_bytes)


def dill_dumps(obj):
    return dill.dumps(obj)


def dill_loads(obj):
    return dill.loads(obj)


translation_table = {
    127: (object, partial(dill_dumps), dill_loads),
    126: (date, partial(dumps, protocol=HIGHEST_PROTOCOL), loads),
    125: (time, partial(dumps, protocol=HIGHEST_PROTOCOL), loads),
    124: (timedelta, partial(dumps, protocol=HIGHEST_PROTOCOL), loads),
    123: (tzinfo, partial(dumps, protocol=HIGHEST_PROTOCOL), loads),
    122: (pd.DataFrame, partial(df_dumps), df_loads),
}


def exception_monkeypatch(self, fut, name, args, kwargs):
    try:
        fut.result()
    except Exception as exc:
        asyncio.get_event_loop().run_until_complete(self.subject.athrow(exc))


def check_args_monkeypatch(self, func, args, kwargs):
    try:
        sig = inspect.signature(func)
        bargs = sig.bind(*args, **kwargs)
    except TypeError as exc:
        raise ParametersError(repr(exc)) from exc
    else:
        arguments = bargs.arguments
        marker = object()
        for name, param in sig.parameters.items():
            if param.annotation is param.empty:
                continue
            val = arguments.get(name, marker)
            if val is marker:
                continue  # Skip default value
            try:
                # I don't think we need to do this
                arguments[name] = val  # param.annotation(val)
            except (TypeError, ValueError) as exc:
                raise ParametersError(
                    'Invalid value for argument {!r}: {!r}'
                    .format(name, exc)) from exc
        if sig.return_annotation is not sig.empty:
            return bargs.args, bargs.kwargs, sig.return_annotation
        return bargs.args, bargs.kwargs, None


def process_call_result_monkeypatch(self, fut, *, req_id, pre, name,
                                    args, kwargs,
                                    return_annotation=None):
    self.discard_pending(fut)
    self.try_log(fut, name, args, kwargs)
    if self.transport is None:
        return
    try:
        ret = fut.result()
        # if return_annotation is not None:
        #    ret = return_annotation(ret)
        prefix = self.prefix + self.RESP_SUFFIX.pack(req_id,
                                                     time_.time(), False)
        self.transport.write(pre + [prefix, self.packer.packb(ret)])
    except asyncio.CancelledError:
        return
    except Exception as exc:
        prefix = self.prefix + self.RESP_SUFFIX.pack(req_id,
                                                     time_.time(), True)
        exc_type = exc.__class__
        exc_info = (exc_type.__module__ + '.' + exc_type.__qualname__,
                    exc.args, repr(exc))
        self.transport.write(pre + [prefix, self.packer.packb(exc_info)])


def unpackb_monkeypatch(self, packed):
    return unpackb(packed, use_list=False, strict_map_key=False, raw=False, ext_hook=self.ext_type_unpack_hook)


class _AwaitedMethodCall():
    __slots__ = ('_proto', '_timeout', '_names', '_return_type')

    def __init__(self, proto, timeout=None, names=(), return_type: Optional[Type] = None):
        self._proto = proto
        self._timeout = timeout
        self._names = names
        self._return_type = return_type

    def __getattr__(self, name):
        return self.__class__(self._proto, self._timeout,
                              self._names + (name,), self._return_type)

    def __call__(self, *args, **kwargs):
        if not self._names:
            raise ValueError('RPC method name is empty')
        fut = self._proto.call('.'.join(self._names), args, kwargs)
        loop = self._proto.loop

        # return asyncio.get_event_l
        rpc_result = asyncio.get_event_loop().run_until_complete(asyncio.Task(
            asyncio.wait_for(
                fut,
                timeout=self._timeout,
                loop=loop
            ),
            loop=loop
        ))

        def return_type_converter(obj, return_type):
            if isinstance(obj, tuple) and hasattr(return_type, '__origin__') and return_type.__origin__ == list:
                list_result = [return_type_converter(o, typing.get_args(return_type)[0]) for o in obj]
                return list_result
            elif self._return_type == type(list):
                return list(obj)
            elif isinstance(obj, tuple) and hasattr(return_type, '__origin__') and return_type.__origin__ is tuple:
                return return_type(obj)
            # for whatever reason, things like PortfolioSummary are returned as tuples, so we have to new them up with *args
            elif isinstance(obj, tuple):
                return return_type(*obj)
            else:
                return obj

        # msgpack is pretty wonky when it comes to lists, converting them to tuples
        # so we have the 'return_type' argument to help with the casting etc.
        if self._return_type:
            return return_type_converter(rpc_result, self._return_type)
        return rpc_result


class RPCHandler(aiozmq.rpc.AttrHandler):
    pass

    @classmethod
    def rpcmethod(cls, func):
        return aiozmq.rpc.method(func)


class PubSubSubject(Subject[T]):
    def on_error(self, error: Exception) -> None:
        self.check_disposed()

        if self.is_stopped:
            return

        for obv in list(self.observers):
            obv.on_error(error)


class _Handler(RPCHandler, Generic[T]):
    def __init__(self):
        self.subject = PubSubSubject[T]()

    @RPCHandler.rpcmethod
    def on_message(self, obj):
        self.subject.on_next(obj)

    @RPCHandler.rpcmethod
    def on_throw(self, ex):
        self.subject.on_error(ex)

    @RPCHandler.rpcmethod
    def on_close(self):
        self.subject.on_completed()

    def get_subject(self):
        return self.subject


class MessageBusServer(Generic[T]):
    def __init__(
        self,
        zmq_address: str,
        zmq_port: int,
        translation_table: Dict[int, Tuple[Any, partial[Any], Callable]] = translation_table,  # type: ignore
    ):
        nest_asyncio.apply()
        self.zmq_address = zmq_address
        self.zmq_port = zmq_port
        self.translation_table = translation_table
        self.lock = threading.Lock()

        self._sentinel = ('stop', 'stop', 'stop')
        self.sentinel_flag: bool = True
        self.clients: Dict[Tuple[str, str], bool] = {}
        self.server: Optional[aiozmq.ZmqStream] = None
        self.read_task: Optional[asyncio.Task] = None
        self.read_loop = None

        self.read_thread: Optional[threading.Thread] = None
        self.message_thread: Optional[threading.Thread] = None

    def put(self, topic_item: Tuple[str, T]):
        self.wait_handle.loop.call_soon_threadsafe(self.wait_handle.queue.put_nowait, topic_item)  # type: ignore
        # this is faster:
        # self.wait_handle.queue.put_nowait(topic_item)  # type: ignore

    async def _message(self, client_id: str, topic: str, val: T):
        if not self.server:
            raise ValueError('server is not initialized')

        if val == b'subscribe' and (client_id, topic) not in self.clients:
            self.clients[(client_id, topic)] = True
            return

        client_ids = [id for (id, topic), _ in self.clients.items() if id != client_id and topic == topic]
        for id in client_ids:
            self.server.write((id, topic, val))

    def _message_loop(self, wait_handle: threading.Event):
        import time
        while not self.server:
            time.sleep(0.1)

        async def main():
            wait_handle.loop = asyncio.get_running_loop()  # type: ignore
            wait_handle.queue = task_queue = asyncio.Queue()  # type: ignore
            wait_handle.set()

            while True:
                item = await task_queue.get()
                if item == self._sentinel:
                    task_queue.task_done()
                    self.sentinel_flag = True
                    break

                id = item[0]
                topic = item[1]
                val = item[2]

                task = asyncio.create_task(self._message(id, topic, val))
                task.add_done_callback(lambda _: task_queue.task_done())
            await task_queue.join()

        asyncio.run(main())

    def _read_loop(self, loop):
        self.server = asyncio.run(aiozmq.create_zmq_stream(
            zmq.ROUTER,
            bind='{}:{}'.format(self.zmq_address, self.zmq_port),
        ))  # type: ignore

        async def main():
            if not self.server:
                raise ValueError('server is not initialized')

            while not self.sentinel_flag:
                task = asyncio.create_task(self.server.read())
                await task
                result = task.result()
                self.put(result)

        # asyncio.run(main())
        self.read_loop = asyncio.get_event_loop()
        self.read_task = self.read_loop.create_task(main())
        try:
            self.read_loop.run_until_complete(self.read_task)
        except Exception as ex:
            pass

    async def start(self):
        logging.debug('starting MessageBus server work queue')

        if self.server and self.sentinel_flag is False:
            raise ValueError('server already started')

        self.wait_handle = threading.Event()

        self.sentinel_flag = False
        # self.read_thread = threading.Thread(target=self._read_loop, args=(asyncio.get_event_loop(),))
        self.read_thread = threading.Thread(target=self._read_loop, args=(asyncio.get_event_loop(),))
        self.read_thread.start()

        # self.message_thread = threading.Thread(target=self._message_loop, args=(self.wait_handle,))
        self.message_thread = threading.Thread(target=self._message_loop, args=(self.wait_handle,))
        self.message_thread.start()

        self.wait_handle.wait()

    def stop(self):
        if self.server is None or self.read_task is None or self.read_loop is None:
            raise ValueError('server not initialized')

        self.sentinel_flag = True
        self.put(self._sentinel)  # type: ignore
        # self.read_task.cancel()
        self.server.close()

        loop = self.read_loop

        if loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
            with suppress(CancelledError):
                with suppress(RuntimeError):
                    loop.run_until_complete(self.read_task)

        asyncio.run(asyncio.sleep(0.1))

    async def wait(self):
        while not self.sentinel_flag:
            await asyncio.sleep(1)


class MessageBusClient(Generic[T]):
    def __init__(
        self,
        zmq_address: str,
        zmq_port: int,
    ):
        self.zmq_address = zmq_address
        self.zmq_port = zmq_port
        self.client: Optional[aiozmq.ZmqStream] = None

    async def connect(self) -> None:
        self.client = await aiozmq.create_zmq_stream(
            zmq.DEALER,
            connect='{}:{}'.format(self.zmq_address, self.zmq_port),
        )  # type: ignore

    def subscribe(self, topic: str) -> None:
        if not self.client:
            raise ValueError('client is not initialized')

        self.client.write([topic.encode(), 'subscribe'.encode()])

    def write(self, topic: str, val: T) -> None:
        if not self.client:
            raise ValueError('client is not initialized')

        self.client.write([topic.encode(), dill_dumps(val)])

    def disconnect(self) -> None:
        if not self.client:
            raise ValueError('client is not initialized')

        self.client.close()

    async def read(self) -> T:
        if not self.client:
            raise ValueError('client is not initialized')

        _, val = await self.client.read()  # type: ignore
        result = dill_loads(val)
        return result


class TopicPubSub(Generic[T]):
    def __init__(
        self,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        translation_table: Dict[int, Tuple[Any, partial[Any], Callable]] = translation_table,  # type: ignore
    ):
        nest_asyncio.apply()
        self.zmq_server_address = zmq_pubsub_server_address
        self.zmq_server_port = zmq_pubsub_server_port
        self.translation_table = translation_table
        self.handler: Optional[_Handler[T]] = None
        self.zmq_subscriber: Optional[PubSubService] = None
        self.zmq_publisher: Optional[PubSubClient] = None
        self.lock = threading.Lock()

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
            self.zmq_subscriber = await aiozmq.rpc.serve_pubsub(
                self.handler,
                translation_table=self.translation_table,
                subscribe=topic,
                connect='{}:{}'.format(self.zmq_server_address, self.zmq_server_port),
                log_exceptions=True,
            )  # type: ignore

            # we're going to hijack the exception handling mechanism
            proto = self.zmq_subscriber._proto
            proto.subject = self.handler.subject
            proto.try_log = types.MethodType(exception_monkeypatch, proto)

        return self.handler.get_subject()

    def subscriber_close(self):
        if self.zmq_subscriber:
            logging.debug('subscriber_close()')
            self.zmq_subscriber.close()
            self.zmq_subscriber.wait_closed()

    async def publisher(
        self,
        obj: T,
        topic: str = 'default'
    ):
        try:
            if not self.zmq_publisher:
                self.zmq_publisher = asyncio.run(aiozmq.rpc.connect_pubsub(
                    # connect='{}:{}'.format(self.zmq_server_address, self.zmq_server_port),
                    bind='{}:{}'.format(self.zmq_server_address, self.zmq_server_port),
                    translation_table=self.translation_table
                ))  # type: ignore
            else:
                await self.zmq_publisher.publish(topic).on_message(obj)
        except Exception as e:
            logging.exception(e)
            logging.debug(f'self.zmq_server_address: {self.zmq_server_address}, self.zmq_server_port: {self.zmq_server_port}')
            raise e

    async def publisher_close(self):
        if self.zmq_publisher:
            logging.debug('publisher_close()')
            self.zmq_publisher.close()
            self.zmq_publisher.wait_closed()


class MultithreadedTopicPubSub(Generic[T], TopicPubSub[T]):
    def __init__(
        self,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        translation_table: Dict[int, Tuple[Any, partial[Any], Callable]] = translation_table,  # type: ignore
    ):
        super().__init__(
            zmq_pubsub_server_address,
            zmq_pubsub_server_port,
            translation_table
        )
        self.wait_handle = threading.Event()
        self._sentinel = ('stop', 'stop')

    def put(self, topic_item: Tuple[str, T]):
        self.wait_handle.loop.call_soon_threadsafe(self.wait_handle.queue.put_nowait, topic_item)  # type: ignore
        # this is faster:
        # self.wait_handle.queue.put_nowait(topic_item)  # type: ignore

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
                        self.zmq_publisher = asyncio.run(aiozmq.rpc.connect_pubsub(
                            # connect='{}:{}'.format(self.zmq_server_address, self.zmq_server_port),
                            bind='{}:{}'.format(self.zmq_server_address, self.zmq_server_port),
                            translation_table=self.translation_table
                        ))  # type: ignore
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


class RPCServer(Generic[T]):
    def __init__(
        self,
        instance: T,
        zmq_rpc_server_address: str,
        zmq_rpc_server_port: int,
        translation_table: Dict[int, Tuple[Any, partial[Any], Callable]] = translation_table  # type: ignore
    ):

        nest_asyncio.apply()
        _BaseServerProtocol.check_args = check_args_monkeypatch
        _ServerProtocol.process_call_result = process_call_result_monkeypatch
        _Packer.unpackb = unpackb_monkeypatch
        self.zmq_server_address = zmq_rpc_server_address
        self.zmq_server_port = zmq_rpc_server_port
        self.translation_table = translation_table
        self.service: Optional[Service] = None
        self.instance: T = instance

    async def serve(self):
        if not self.service:
            bind = '{}:{}'.format(self.zmq_server_address, self.zmq_server_port)
            self.service = await aiozmq.rpc.serve_rpc(
                self.instance,
                bind=bind,
                translation_table=self.translation_table
            )  # type: ignore


class RemotedClient(Generic[T]):
    def __init__(
        self,
        zmq_server_address: str,
        zmq_server_port: int,
        translation_table: Dict[int, Tuple[Any, partial[Any], Callable]] = translation_table,  # type: ignore
        timeout: Optional[int] = None,
        error_table: Optional[Dict[str, Exception]] = None,
    ):
        nest_asyncio.apply()
        _Packer.unpackb = unpackb_monkeypatch
        self.zmq_server_address = zmq_server_address
        self.zmq_server_port = zmq_server_port
        self.translation_table = translation_table
        self.client: Optional[aiozmq.rpc.rpc.RPCClient] = None
        self.timeout: Optional[int] = timeout
        self.connected: bool = False
        self.error_table: Optional[Dict[str, Exception]] = error_table

    async def connect(self):
        if not self.client:
            logging.debug('trying RemotedClient.connect()')
            bind = '{}:{}'.format(self.zmq_server_address, self.zmq_server_port)
            self.client = await aiozmq.rpc.connect_rpc(
                connect=bind,
                timeout=self.timeout,
                translation_table=self.translation_table,
                error_table=self.error_table
            )  # type: ignore
            self.connected = True

    async def awaitable_rpc(self) -> T:
        return self.client.call  # type: ignore

    def rpc(self, return_type: Optional[Type] = None) -> T:
        if self.client and self.connected:
            return _AwaitedMethodCall(self.client._proto, timeout=self.client._timeout, return_type=return_type)  # type: ignore
        else:
            raise ConnectionError('not connected')
