import os
import sys
import datetime as dt
from traceback import print_stack
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


async def boil_kettle(water_volume: float) -> str:
    await asyncio.sleep(3 + water_volume * 3)
    return "Kettle boiled."

async def clean_cups(number_of_cups: int) -> str:
    await asyncio.sleep(number_of_cups * 1)
    return "Cups cleaned."

async def main():
    boiling_task = asyncio.create_task(boil_kettle(1.5), name="boiling kettle.")
    clean_cups_task = asyncio.create_task(clean_cups(4), name="cleaning cups.")
    done, pending = await asyncio.wait(
        [boiling_task, clean_cups_task],
        return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        print(f"Task {task.get_name()} did not complete.")
        print(task.print_stack())

        task.cancel()
    for task in done:
        print(task.result())

asyncio.run(main())
