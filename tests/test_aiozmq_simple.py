import asyncio
import aiozmq.rpc
import msgpack
from typing import List, cast


class Foo():
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __str__(self):
        return '{} {}'.format(self.a, self.b)

translation_table = {
    0: (
        Foo,
        lambda value: msgpack.packb((value.a, value.b)),
        lambda binary: Foo(*msgpack.unpackb(binary)),
    ),
    1: (
        List,
        lambda value: msgpack.packb(value),
        lambda binary: cast(List, msgpack.unpackb(binary)),
    ),
}

class ServerHandler(aiozmq.rpc.AttrHandler):
    @aiozmq.rpc.method
    def remote_func(self, a: int, b: int) -> int:
        return a + b

    @aiozmq.rpc.method
    def remote_2(self, a: int, b: int) -> list:
        return [Foo(a, b)]


async def go():
    server = await aiozmq.rpc.serve_rpc(ServerHandler(), bind="tcp://*:*", translation_table=translation_table)
    server_addr = list(server.transport.bindings())[0]

    client = await aiozmq.rpc.connect_rpc(connect=server_addr, translation_table=translation_table)

    try:
        ret = await client.call.remote_2(1, 2)
        print(ret)
        print(type(ret))
    except Exception as ex:
        print(ex)


    server.close()
    await server.wait_closed()
    client.close()
    await client.wait_closed()


def main():
    asyncio.run(go())
    print("DONE")


if __name__ == "__main__":
    main()
