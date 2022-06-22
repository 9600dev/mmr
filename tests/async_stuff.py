import os
import sys
import datetime as dt
os.environ['PYTHONASYNCIODEBUG'] = '1'

import aioreactive as rx
import asyncio
import logging
import coloredlogs
from ib_insync import Contract


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))


from trader.common.logging_helper import verbose
logging.basicConfig(level=logging.DEBUG)
coloredlogs.install(level=logging.DEBUG)
verbose()


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


class Client():
    def __init__(self):
        self._futures = {}
        self._results = {}
        self._reqId2Contract = {}

    def startReq(self, key, contract=None, container=None):
        """
        Start a new request and return the future that is associated
        with with the key and container. The container is a list by default.
        """
        future = asyncio.Future()
        self._futures[key] = future
        self._results[key] = container if container is not None else []
        if contract:
            self._reqId2Contract[key] = contract
        return future


async def do_stuff():
    client = Client()
    contract = Contract()
    bars = []
    future = client.startReq('asdf', contract, bars)
    timeout = 3

    task = asyncio.wait_for(future, timeout) if timeout else future
    try:
        await task
    except asyncio.TimeoutError:
        print(f'reqHistoricalData: Timeout for {contract}')


async def start():
    await do_stuff()
    print('here')
    loop = asyncio.get_event_loop()
    loop.create_task(do_stuff())
    await do_stuff()
    print('hello!')


def main():
    loop = asyncio.get_event_loop()
    loop.create_task(start())
    # loop.create_task(start())
    loop.run_forever()
    print("DONE")


async def add(x: int, y: int):
    await asyncio.sleep(5)
    return x + y

async def worker(loop):
    timer = PerfTimer()
    timer.start()
    result1 = await add(3, 4)
    print('here')
    result2 = await add(5, 5)

    print(result1, result2, timer.stop())
    loop.stop()

def real_main():
    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)
    loop.create_task(worker(loop))

    # Blocking call interrupted by loop.stop()
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    real_main()
