import os
import sys

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import click
import datetime as dt
import signal
import asyncio
import random
import aioreactive as rx
from aioreactive.subject import AsyncSubject, AsyncMultiSubject
from aioreactive.observables import AsyncAnonymousObservable
from aioreactive.observers import AsyncAnonymousObserver, auto_detach_observer, safe_observer
from expression.system import AsyncAnonymousDisposable
from expression.core import pipe

from trader.common.logging_helper import setup_logging, log_callstack_debug
logging = setup_logging(module_name='trading_runtime')

from asyncio import iscoroutinefunction, AbstractEventLoop
from typing import TypeVar, Callable, Awaitable, Optional, Any
from ib_insync import Contract


global holder
loops = 1000000

class PerfTimer():
    def __init__(self):
        self.start_time: dt.datetime
        self.result: float

    def start(self):
        self.start_time = dt.datetime.now()
        self.result = 0.0

    def stop(self) -> str:
        diff = dt.datetime.now() - self.start_time
        self.result = diff.total_seconds() * 1000.0
        self.start_time = dt.datetime.min
        return 'total (ms): {}'.format(self.result)


class TestTrader():
    def __init__(self):
        self.subject: AsyncSubject[int] = AsyncSubject[int]()

    async def run(self):
        await self.perf_test()
        await self.perf_test2()

    async def perf_test(self):
        timer = PerfTimer()
        timer.start()

        async def asend(val):
            global holder
            holder = val

        # xs ends up being an AnonymousObserveable
        xs = pipe(
            self.subject,
            rx.filter(lambda x: x >= 5),
            rx.filter(lambda x: x <= 100),
            rx.filter(lambda x: x <= 50),
        )

        observer = AsyncAnonymousObserver(asend=asend)
        disposable = await xs.subscribe_async(observer)

        for i in range(loops):
            await self.subject.asend(random.randint(0, 100))

        print(timer.stop())
        print('done')

    async def perf_test2(self):
        timer = PerfTimer()
        timer.start()

        async def asend(val):
            global holder
            holder = val

        for i in range(loops):
            r = random.randint(0, 100)
            if r >= 5 and r <= 100 and r <= 50:
                await asend(random.randint(0, 100))

        print(timer.stop())
        print('done')


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def shutdown(signal, loop: AbstractEventLoop):
        """Cleanup tasks tied to the service's shutdown."""
        logging.info(f"Received exit signal {signal.name}...")

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

        [logging.debug(task) for task in tasks]
        [task.cancel() for task in tasks]

        logging.info(f"Cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

    try:
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

        test_trader = TestTrader()
        asyncio.ensure_future(test_trader.run())

        logging.debug('calling IB.run(), which will loop.run_forever()')
        loop.run_forever()
    finally:
        loop.close()
        logging.debug('calling exit()')
        exit()


if __name__ == '__main__':
    main()
