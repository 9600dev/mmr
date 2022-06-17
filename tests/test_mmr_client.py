import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))


import asyncio
from trader.messaging.clientserver import RemotedClient
from test_mmr_server import MyService
from typing import cast, List
from ib_insync import Stock, PortfolioItem

async def gogo():
    client = RemotedClient[MyService](zmq_server_address='tcp://127.0.0.1', zmq_server_port=42050)
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
        print('done')


def main():
    loop = asyncio.get_event_loop()
    loop.create_task(gogo())
    loop.run_forever()
    print("DONE")

if __name__ == "__main__":
    main()

