import os
import sys


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))


from ib_insync import Contract
from reactivex import Observer
from trader.messaging.clientserver import MessageBusClient, MessageBusServer

import asyncio


class ClientServerTest:

    def __init__(self):
        self.server = None

    async def start_server(self):
        print('starting server')
        self.server = MessageBusServer('tcp://127.0.0.1', 42050)
        await self.server.start()
        print('started server')

    async def get_client(self):
        print('starting client')
        client = MessageBusClient('tcp://127.0.0.1', 42050)
        await client.connect()
        return client

loop = asyncio.get_event_loop()

c = ClientServerTest()
asyncio.run(c.start_server())
c1 = asyncio.run(c.get_client())
c2 = asyncio.run(c.get_client())

assert c.server is not None

class MyObserver(Observer[str]):
    def on_next(self, msg):
        print('on_next: {}'.format(msg))

    def on_completed(self):
        print('on_completed')

    def on_error(self, err):
        print('on_error: {}'.format(err))

d1 = c1.subscribe('test', MyObserver())
d2 = c2.subscribe('test', MyObserver())

contract = Contract(secType='STK', symbol='AAPL', exchange='SMART', currency='USD')

c1.write('test', contract)
# result = asyncio.run(c2.read())
# print('result: {}'.format(result))

c2.write('test', 'hello from c2')
# result = asyncio.run(c1.read())
# print('result: {}'.format(result))


import datetime as dt


start = dt.datetime.now()

for i in range(100):
    c1.write('test', str(i))
    # result = asyncio.run(c2.read())

stop = dt.datetime.now()
print('time: {}'.format(stop - start))

import time


asyncio.get_event_loop().run_until_complete(asyncio.sleep(2))

print('disconnecting clients')
c1.disconnect()
c2.disconnect()

print('stopping server')

c.server.stop()

asyncio.get_event_loop().run_until_complete(c.server.wait())

print('finished')

asyncio.get_event_loop().run_until_complete(asyncio.sleep(1))
