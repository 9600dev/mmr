import asyncio
from re import I
from struct import unpack
import aiozmq.rpc
import inspect
import pandas as pd
import numpy as np
import datetime
import pyarrow as pa
import aioreactive as rx
import random
import types
import json
import nest_asyncio
import dataclasses
import dill
import typing
import time as time_
from inspect import Parameter
from datetime import timedelta, time, date, tzinfo
from functools import partial
from pickle import dumps, loads, HIGHEST_PROTOCOL
from itertools import count
from functools import wraps
from typing import TypeVar, Generic, Tuple, Callable, Dict, Optional, List, Any, Type
from aioreactive.types import AsyncObservable, AsyncObserver
from aioreactive.subject import AsyncMultiSubject
from expression.system import AsyncDisposable, ObjectDisposedException
from aiozmq.rpc.pubsub import PubSubClient, PubSubService
from aiozmq.rpc.base import Service, ParametersError, NotFoundError
from aiozmq.rpc.rpc import RPCClient, _BaseServerProtocol, _ServerProtocol
from aiozmq.rpc.packer import _Packer
from dataclasses_serialization.json import JSONSerializer
from msgpack import unpackb

from trader.common.logging_helper import setup_logging, get_callstack
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


class PubSubAsyncSubject(AsyncMultiSubject[T]):
    async def athrow(self, error: Exception) -> None:
        self.check_disposed()

        if self._is_stopped:
            return

        for obv in list(self._observers):
            await obv.athrow(error)


class _Handler(RPCHandler, Generic[T]):
    def __init__(self):
        self.subject = PubSubAsyncSubject[T]()

    @RPCHandler.rpcmethod
    async def on_message(self, obj):
        await self.subject.asend(obj)

    @RPCHandler.rpcmethod
    async def on_throw(self, ex):
        await self.subject.athrow(ex)

    @RPCHandler.rpcmethod
    async def on_close(self):
        await self.subject.aclose()

    def get_subject(self):
        return self.subject


class TopicPubSub(Generic[T]):
    def __init__(
        self,
        zmq_pubsub_server_address: str = 'tcp://127.0.0.1',
        zmq_pubsub_server_port: int = 42002,
        topic: str = 'default',
        translation_table: Dict[int, Tuple[Any, partial[Any], Callable]] = translation_table,  # type: ignore
    ):
        nest_asyncio.apply()
        self.zmq_server_address = zmq_pubsub_server_address
        self.zmq_server_port = zmq_pubsub_server_port
        self.topic = topic
        self.translation_table = translation_table
        self.handler: Optional[_Handler[T]] = None
        self.zmq_subscriber: Optional[PubSubService] = None
        self.zmq_publisher: Optional[PubSubClient] = None

    async def subscriber(
        self,
    ) -> rx.AsyncObservable[T]:
        if not self.handler:
            self.handler = _Handler[T]()
            self.zmq_subscriber = await aiozmq.rpc.serve_pubsub(
                self.handler,
                translation_table=self.translation_table,
                subscribe=self.topic,
                connect='{}:{}'.format(self.zmq_server_address, self.zmq_server_port),
                log_exceptions=True,
            )  # type: ignore

            # we're going to hijack the exception handling mechanism
            proto = self.zmq_subscriber._proto
            proto.subject = self.handler.subject
            proto.try_log = types.MethodType(exception_monkeypatch, proto)

        return self.handler.get_subject()

    async def subscriber_close(self):
        if self.zmq_subscriber:
            logging.debug('subscriber_close()')
            self.zmq_subscriber.close()
            self.zmq_subscriber.wait_closed()

    async def publisher(
        self,
        obj: T,
    ):
        if not self.zmq_publisher:
            self.zmq_publisher = await aiozmq.rpc.connect_pubsub(
                # connect='{}:{}'.format(self.zmq_server_address, self.zmq_server_port),
                bind='{}:{}'.format(self.zmq_server_address, self.zmq_server_port),
                translation_table=self.translation_table
            )  # type: ignore

        logging.debug('publisher()')
        await self.zmq_publisher.publish(self.topic).on_message(obj)

    async def publisher_close(self):
        if self.zmq_publisher:
            logging.debug('publisher_close()')
            self.zmq_publisher.close()
            self.zmq_publisher.wait_closed()


class RPCServer(Generic[T]):
    def __init__(
        self,
        instance: T,
        zmq_rpc_server_address: str = 'tcp://127.0.0.1',
        zmq_rpc_server_port: int = 42001,
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
        zmq_server_address: str = 'tcp://127.0.0.1',
        zmq_server_port: int = 42001,
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
            logging.debug('connect()')
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
