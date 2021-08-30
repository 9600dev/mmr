import sys
import os
import asyncio
import aioreactive as rx
import logging

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from asyncio import iscoroutinefunction
from aioreactive import AsyncObserver
from aioreactive.subject import AsyncMultiSubject
from expression.core import pipe
from trader.listeners.ibaiorx import IBAIORx
from trader.common.reactive import AsyncCachedObserver, AsyncCachedSubject, awaitify
from ib_insync import Stock, Contract
from ib_insync.contract import ContractDetails
from ib_insync.ticker import Ticker
from ib_insync.objects import RealTimeBar, Position
from typing import Coroutine, List, Tuple, TypeVar, Generator, Iterable, Optional, Callable, Awaitable, Generic, Set
from functools import wraps


TKey = TypeVar('TKey')
TValue = TypeVar('TValue')
class CachedSubscriberHelper(Generic[TKey, TValue]):
    def __init__(self, key: TKey, value: TValue):
        self.key = key
        self.value = value

c = CachedSubscriberHelper(1, 'asdf')
print(c.key)

T = TypeVar('T')

def list_flattener(input_list: List[T]) -> rx.AsyncObservable[T]:
    return rx.from_iterable(input_list)

async def some_set():
    xs = rx.single({"apple", "banana", "cherry"})

    def mapper(value: Set[str]) -> rx.AsyncObservable[str]:
        return rx.from_iterable(value)

    async def sink(value):
        print(value)

    xs = pipe(
        xs,
        rx.flat_map(mapper)
    )

    subject = AsyncCachedSubject[str]()
    await xs.subscribe_async(subject)

    await subject.subscribe_async(AsyncCachedObserver(sink))
    await subject.subscribe_async(AsyncCachedObserver(sink))
    await rx.run(xs)
    print(subject.get_value())


async def subject():
    xs = rx.from_iterable([[1, 2], [3, 4], [5, 6]])
    xs = pipe(
        xs,
        rx.flat_map(list_flattener)  # type: ignore
    )

    subject = AsyncCachedSubject[int]()

    # subscribe the subject to the source
    await xs.subscribe_async(subject)

    # now subscribe to the subject a few times
    await subject.subscribe_async(AsyncCachedObserver(awaitify(lambda x: print('first: {}'.format(x)))))
    await subject.subscribe_async(AsyncCachedObserver(awaitify(lambda x: print('second: {}'.format(x)))))
    await rx.run(xs)
    print(subject.value())


async def main():
    xs = rx.from_iterable([[1, 2], [3, 4], [5, 6]])
    xs = pipe(
        xs,
        rx.flat_map(list_flattener)  # type: ignore
    )

    cvo = AsyncCachedObserver(asend=awaitify(lambda value: print(value)))
    await xs.subscribe_async(cvo)
    await rx.run(xs)
    print(cvo.value())




loop = asyncio.get_event_loop()
loop.run_until_complete(some_set())
try:
    loop.run_forever()
except KeyboardInterrupt:
    loop.close()
