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


async def add(x: int, y: int):
    await asyncio.sleep(5)
    return x + y


async def worker_concurrent(loop):
    logging.debug('worker_concurrent')
    timer = PerfTimer()
    timer.start()
    result1 = asyncio.create_task(add(3, 4))
    result2 = asyncio.create_task(add(5, 5))

    print(await result1, await result2, timer.stop())


async def worker_sync(loop):
    logging.debug('worker_sync')
    timer = PerfTimer()
    timer.start()
    result1 = await add(3, 4)
    result2 = await add(5, 5)

    print(result1, result2, timer.stop())


def real_main():
    loop = asyncio.get_event_loop()
    loop.set_debug(enabled=True)
    loop.create_task(worker_sync(loop))
    loop.create_task(worker_concurrent(loop))

    # Blocking call interrupted by loop.stop()
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    real_main()

