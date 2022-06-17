import os
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

class Foo():
    def __init__(self):
        self.a = 1
        self.b = 2

class MyService(RPCHandler):
    @classmethod
    @RPCHandler.rpcmethod
    def add(cls, a: int, b: int):
        return a + b

    @classmethod
    @RPCHandler.rpcmethod
    def df(cls) -> pd.DataFrame:
        df = pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list('ABCD'))
        return df

    @classmethod
    @aiozmq.rpc.method
    def get_list(cls):
        # return [Foo(), Foo()]
        return Foo()

async def gogo():
    service = MyService()
    serve = RPCServer[MyService](service)
    print('starting service')
    await serve.serve()


def main():
    loop = asyncio.get_event_loop()
    loop.create_task(gogo())
    loop.run_forever()
    print("DONE")

if __name__ == "__main__":
    main()

