import os
os.environ['MSGPACK_PUREPYTHON'] = 'true'

from re import U
import sys

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import asyncio
import pandas as pd
import numpy as np
import aiozmq
import aiozmq.rpc
from trader.messaging.clientserver import RPCServer, RPCHandler
from typing import List
from aiozmq.rpc import method
from ib_insync.objects import Contract, PortfolioItem, Position
from ib_insync import Stock

class MyService(RPCHandler):
    @classmethod
    @RPCHandler.rpcmethod
    def df(cls) -> pd.DataFrame:
        df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
        return df

    @classmethod
    @aiozmq.rpc.method
    def get_portfolio(cls):
        return PortfolioItem(
            account='asdf',
            averageCost=2.3,
            contract=Contract(),
            marketPrice=2.3,
            marketValue=3.2,
            position=200.0,
            realizedPNL=0.0,
            unrealizedPNL=0.0,
        )

    @classmethod
    @aiozmq.rpc.method
    def get_stock(cls):
        return [Stock()]

async def start():
    service = MyService()
    serve = RPCServer[MyService](service, 'tcp://127.0.0.1', 42050)
    print('starting service')
    await serve.serve()


def main():
    loop = asyncio.get_event_loop()
    loop.create_task(start())
    loop.run_forever()
    print("DONE")

if __name__ == "__main__":
    main()