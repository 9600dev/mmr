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
from trader.container import Container
from trader.trading.trading_runtime import Trader
from trader.common.helpers import get_network_ip
from trader.listeners.ibaiorx import IBAIORx
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
    def __init__(self, client: IBAIORx):
        self.client = client
        self.subject: AsyncSubject[int] = AsyncSubject[int]()

    async def get_snapshot(self):
        amd = Contract(symbol='AMD', conId=4391, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
        snapshot = await self.client.get_snapshot(amd, True)
        print(snapshot)

    async def run(self):
        # await self.dis()
        # await self.dis_detach()
        # await self.close_me()
        await self.get_snapshot()
        await self.client.shutdown()

    async def generate(self):
        for i in range(0, 10):
            await self.subject.asend(i)

    async def close_me(self):
        await self.subject.aclose()

    async def dis_detach(self):
        async def asend(val):
            print(val)
        async def athrow(ex):
            print(ex)
        async def aclose():
            print('close')

        # xs ends up being an AnonymousObserveable
        xs = pipe(
            self.subject,
            # rx.filter(lambda x: x >= 5),
            rx.take(2),
            # rx.take(5),
        )

        # using auto_detach_observer or not, doesn't matter for the case
        # where nothing is either asend or aclosed -- it looks like in either case
        # you need at least /something/ sent for those tasks to actually be closed out.
        observer = AsyncAnonymousObserver(asend=asend, athrow=athrow, aclose=aclose)
        safe_obv, auto_detach = auto_detach_observer(obv=observer)
        subscription = await pipe(safe_obv, xs.subscribe_async, auto_detach)

        # wait until ticker is set
        await self.subject.asend(5)
        await asyncio.sleep(1)
        await self.subject.aclose()
        await asyncio.sleep(1)
        await asyncio.sleep(1)
        # await self.subject.asend(5)
        # await self.subject.asend(5)

        # await subscription.dispose_async()

        # if we don't actually generate any values, the tasks (via Mailbox Processor) end up hanging around
        # waiting for things to do
        # await self.generate()

        # calling aclose() or asend() before calling dispose_async, does however, clean up all these tasks
        # await self.subject.aclose()

        # even calling dispose_async doesn't seem to cancel these tasks either -- now we're getting somewhere.
        # if you call dispose_async() before sending a 'close' then tasks seem to hang around.
        # print('calling disposable.dispose_async()')
        print('done')

    async def dis(self):
        async def asend(val):
            print(val)
        # xs ends up being an AnonymousObserveable
        xs = pipe(
            self.subject,
            # rx.filter(lambda x: x >= 5),
            rx.take(2),
        )

        observer = AsyncAnonymousObserver(asend=asend)
        disposable = await xs.subscribe_async(observer)

        # if we don't actually generate any values, the tasks (via Mailbox Processor) end up hanging around
        # waiting for things to do
        # await self.generate()

        # calling aclose() before calling dispose_async, does however, clean up all these tasks
        await self.subject.asend(5)
        await self.subject.asend(5)

        await asyncio.sleep(1)

        # even calling dispose_async doesn't seem to cancel these tasks either -- now we're getting somewhere.
        # if you call dispose_async() before sending a 'close' then tasks seem to hang around.
        # print('calling disposable.dispose_async()')
        await disposable.dispose_async()

        print('done')


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    config = '/home/trader/mmr/configs/trader.yaml'
    container = Container(config)
    # trader = container.resolve(Trader, simulation=False)
    client = container.resolve(IBAIORx)

    async def shutdown(signal, loop: AbstractEventLoop):
        """Cleanup tasks tied to the service's shutdown."""
        logging.info(f"Received exit signal {signal.name}...")

        await client.shutdown()

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

        loop.set_debug(enabled=True)

        client.connect()

        test_trader = TestTrader(client)
        asyncio.ensure_future(test_trader.run())

        logging.debug('calling IB.run(), which will loop.run_forever()')
        loop.run_forever()

        # client.run()

    finally:
        loop.close()
        logging.debug('calling exit()')
        exit()


if __name__ == '__main__':
    main()
