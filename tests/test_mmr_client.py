import os
import sys


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))


from ib_insync import PortfolioItem, Stock
from test_mmr_server import MyService
from trader.messaging.clientserver import RPCClient
from typing import cast, List

import asyncio


async def gogo():
    client = RPCClient[MyService](zmq_server_address='tcp://127.0.0.1', zmq_server_port=42050)
    await client.connect()

    for i in range(0, 10):
        stock = list(client.rpc(return_type=list[Stock]).get_stock())
        print(stock)
        print()

        df = client.rpc().df()
        print(df)
        print()

        portfolio = client.rpc(return_type=PortfolioItem).get_portfolio()
        print(portfolio)

        portfolio_list = client.rpc(return_type=list[PortfolioItem]).get_portfolio_list()
        print(portfolio_list)
        print('type: {}, inner: {}'.format(type(portfolio_list), type(portfolio_list[0])))


def main():
    loop = asyncio.get_event_loop()
    loop.create_task(gogo())
    loop.run_forever()
    print("DONE")

if __name__ == "__main__":
    main()

