import sys
import datetime as dt
import os
import signal

os.environ['PYTHONASYNCIODEBUG'] = '1'

import asyncio
import aioreactive as rx
from expression.system import AsyncDisposable
from typing import Optional

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from trader.listeners.ibaiorx import IBAIORx
from ib_insync import Contract

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


class MyObserver(rx.AsyncObserver):
    def __init__(self):
        self.sub: Optional[AsyncDisposable] = None

    async def asend(self, val):
        print(val)

    async def athrow(self, ex):
        print(ex)

    async def aclose(self):
        print('aclose')
        if self.sub:
            await self.sub.dispose_async()

    def subscription(self, subscription):
        self.sub = subscription


async def test_contract_history(client: IBAIORx):
    amd = Contract(symbol='AMD', conId=4391, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
    nvda = Contract(symbol='NVDA', conId=4815747, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
    a2m = Contract(symbol='A2M', conId=189114468, exchange='SMART', primaryExchange='ASX', currency='AUD')
    start_date = dt.datetime.now() - dt.timedelta(days=2)
    end_date = dt.datetime.now() - dt.timedelta(days=1)

    timer = PerfTimer()

    timer.start()
    amd_observable = rx.from_async(client.get_contract_history(amd, start_date, end_date))
    observer = MyObserver()
    disposable = await amd_observable.subscribe_async(observer)
    timer.stop()

    # timer.start()
    # nvda_observable = rx.from_async(client.get_contract_history(nvda, start_date, end_date))
    # disposable = await nvda_observable.subscribe_async(anon_observer)
    # await disposable.dispose_async()
    # timer.stop()

    # timer.start()
    # a2m_observable = rx.from_async(client.get_contract_history(a2m, start_date, end_date))
    # disposable = await a2m_observable.subscribe_async(anon_observer)
    # await disposable.dispose_async()
    # timer.stop()

    # snapshot = await client.get_snapshot(amd, True)
    # print(snapshot)

    # obs = rx.single(await client.get_snapshot(amd, True))
    # disposable = await obs.subscribe_async(rx.AsyncAnonymousObserver(my_asend))
    # await disposable.dispose_async()

    print('finished.')


async def start():
    client = IBAIORx('127.0.0.1', 7496)
    client.connect()
    await test_contract_history(client)


def main():
    def stop_loop(loop):
        if loop.is_running():
            pending_tasks = [
                task for task in asyncio.all_tasks() if not task.done()
            ]
            for task in pending_tasks:
                print(task.print_stack())

            loop.run_until_complete(asyncio.gather(*pending_tasks))
            loop.stop()
            # loop.close()

    loop = asyncio.get_event_loop()
    try:
        loop.set_debug(enabled=True)
        loop.add_signal_handler(signal.SIGINT, stop_loop, loop)
        asyncio.ensure_future(start())
        loop.run_forever()
    except KeyboardInterrupt:
        stop_loop(loop)
        exit()

if __name__ == "__main__":
    main()
