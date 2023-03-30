from ib_insync import Contract
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


c = ClientServerTest()
asyncio.run(c.start_server())
c1 = asyncio.run(c.get_client())
c2 = asyncio.run(c.get_client())

assert c.server is not None

c1.subscribe('test')
c2.subscribe('test')

contract = Contract(secType='STK', symbol='AAPL', exchange='SMART', currency='USD')

c1.write('test', contract)
result = asyncio.run(c2.read())
print('result: {}'.format(result))

c2.write('test', 'hello from c2')
result = asyncio.run(c1.read())
print('result: {}'.format(result))

c1.disconnect()
c2.disconnect()

print('waiting')

c.server.stop()

asyncio.get_event_loop().run_until_complete(c.server.wait())

print('finished')

asyncio.get_event_loop().run_until_complete(asyncio.sleep(1))
