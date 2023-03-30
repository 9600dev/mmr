import os
import sys


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))


from rpcserver import MyService
from trader.messaging.clientserver import RPCClient

import asyncio


async def gogo():
    client = RPCClient[MyService]()
    await client.connect()

    for i in range(0, 10):
        result = client.rpc().add(1, 2)
        print(result)

        df = client.rpc().df()
        print(df)

        test = client.rpc().get_list()
        print(test)

        print('done')


def main():
    loop = asyncio.get_event_loop()
    loop.create_task(gogo())
    loop.run_forever()
    print("DONE")

if __name__ == "__main__":
    main()

