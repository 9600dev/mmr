import nest_asyncio
nest_asyncio.apply()

import asyncio
import datetime as dt

from trader.data.market_data import MarketData, SecurityDataStream
from trader.listeners.ibaiorx import IBAIORx
from trader.container import Container
from ib_insync.contract import Contract
from aioreactive.observers import AsyncAnonymousObserver
from cli import *

async def p(df):
    print(df)

async def test():
    container = Container()
    client = container.resolve(IBAIORx)
    client.connect()
    amd = Contract(symbol='AMD', conId=4391, exchange='SMART', primaryExchange='NASDAQ', currency='USD')

    m = MarketData(client, '192.168.0.250', 'Universes')
    stream = await m.subscribe_security(amd, '1 min', start_date=dt.datetime(2021, 10, 28), back_fill=True)
    await stream.subscribe_async(AsyncAnonymousObserver(p))


loop = asyncio.get_event_loop()
loop.create_task(test())
loop.run_forever()


