import os
import sys
import asyncio
import aioreactive as rx
import logging
import pandas as pd

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from expression.core import pipe
from trader.listeners.ibaiorx import IBAIORx
from trader.listeners.ibaiorx2 import IBAIORx2
from trader.common.logging_helper import set_all_log_level, suppress_external
from ib_insync import Stock, Contract
from ib_insync.contract import ContractDetails
from ib_insync.ticker import Ticker
from ib_insync.objects import RealTimeBar, RealTimeBarList, Position
from typing import List, Tuple, TypeVar
from trader.common.helpers import parse_fundamentals, parse_fundamentals_pandas

set_all_log_level(logging.INFO)
suppress_external()

client = IBAIORx2('192.168.0.250', 7496)
client.connect()


async def subscribe_contract(conId: int = 4391):
    async def contract_sink(ticker: Ticker):
        print(ticker)

    contract = Contract(conId=conId, exchange='SMART', currency='USD')
    observable = await client.subscribe_contract(contract)
    return await observable.subscribe_async(rx.AsyncAnonymousObserver(contract_sink))


async def subscribe_bars(conId: int = 4391):
    async def bars_sink(bars: RealTimeBarList):
        print('{}: {}'.format(bars.contract, bars[-1]))

    contract = Contract(conId=conId, exchange='SMART', currency='USD')
    observable = await client.subscribe_barlist(contract)
    return await observable.subscribe_async(rx.AsyncAnonymousObserver(bars_sink))


async def subscribe_positions():
    async def positions_sink(positions: List[Position]):
        print(positions)

    observable = await client.subscribe_positions()
    return await observable.subscribe_async(rx.AsyncAnonymousObserver(positions_sink))


async def get_contract_details(conId: int = 4391):
    contract = Contract(conId=conId, exchange='SMART', currency='USD')
    return await(client.get_contract_details(contract))


async def get_position_contract_details():
    async def positions_contract_sink(position_contract: Tuple[Position, ContractDetails]):
        print('position {}  contract {}'.format(position_contract[0], position_contract[1]))

    def contract_cleaner(contract: Contract):
        return Contract(conId=contract.conId, currency=contract.currency, exchange='SMART')

    async def mapper(position: Position) -> Tuple[Position, ContractDetails]:
        details = await client.get_contract_details(contract_cleaner(position.contract))
        if len(details) > 0:
            return (position, details[0])
        else:
            return (position, ContractDetails())

    positions_obs = rx.from_iterable(client.ib.positions())
    xs = pipe(
        positions_obs,
        rx.map_async(mapper)
    )

    return await xs.subscribe_async(rx.AsyncAnonymousObserver(positions_contract_sink))

async def get_fundamentals(contract: Contract) -> pd.DataFrame:
    report = await client.get_fundamental_data_sync(contract)
    return parse_fundamentals_pandas(report)


async def main():
    print('subscribe_positions()')
    await subscribe_positions()
    await subscribe_contract()
    await subscribe_bars(conId=4391)
    await subscribe_bars(conId=4815747)
    print('here')
    print()
    # await subscribe_positions()
    # print(await get_contract_details())
    await get_position_contract_details()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
try:
    loop.run_forever()
except KeyboardInterrupt:
    loop.close()
