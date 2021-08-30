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
from aioreactive.observers import AsyncAnonymousObserver
from aioreactive.subject import AsyncMultiSubject, AsyncSubject
from expression.core import pipe
from trader.listeners.ibaiorx import IBAIORx
from trader.common.reactive import AsyncCachedObserver, AsyncCachedSubject, awaitify
from ib_insync import Stock, Contract
from ib_insync.contract import ContractDetails
from ib_insync.ticker import Ticker
from ib_insync.objects import RealTimeBar, Position
from typing import Coroutine, List, Tuple, TypeVar, Generator, Iterable, Optional, Callable, Awaitable, Generic
from functools import wraps
from eventkit import Event


class Data():
    def __init__(self, value):
        self.value = value


def initial_call():
    return Data(42)


async def on_event_update(event_update):
    print('on_event_update called')


pendingTickersEvent = Event('pendingTickersEvent')
# pendingTickersEvent += on_event_update


async def generate_ticks(time: int):
    counter = 0
    pendingTickersEvent('testing {}'.format(counter))
    while True:
        await asyncio.sleep(time)
        pendingTickersEvent('testing {}'.format(counter))
        counter = counter + 1


async def sink(e):
    print(e)


contracts = CachedSubscribeEventHelper[int, Data](event_to_subscribe=pendingTickersEvent)


async def sub():
    source = AsyncSubject()   # (initial_call())
    return await contracts.subscribe(
        key=0,
        source=source,
        filter_function=rx.filter(lambda data: True)
    )


async def main():
    print('setting up subscription')
    result = await sub()
    print('subscribing and sinking')
    await result.subscribe_async(AsyncAnonymousObserver(sink))
    print('done with main()')


loop = asyncio.get_event_loop()
a = loop.run_until_complete(main())
b = loop.run_until_complete(main())
asyncio.run(generate_ticks(1))
try:
    print('run_forever')
    loop.run_forever()
except KeyboardInterrupt:
    loop.close()



