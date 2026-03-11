from trader.messaging.clientserver import RPCHandler, rpcmethod
from typing import Optional


class DataServiceApi(RPCHandler):
    def __init__(self, service):
        self.service = service

    @rpcmethod
    async def pull_massive(
        self,
        symbols: Optional[list[str]] = None,
        universe: Optional[str] = None,
        bar_size: str = '1 day',
        prev_days: int = 30,
        max_concurrent: int = 5,
    ) -> dict:
        return await self.service.pull_massive(symbols, universe, bar_size, prev_days, max_concurrent)

    @rpcmethod
    async def pull_ib(
        self,
        symbols: Optional[list[str]] = None,
        universe: Optional[str] = None,
        bar_size: str = '1 min',
        prev_days: int = 5,
        ib_client_id: int = 10,
        max_concurrent: int = 1,
    ) -> dict:
        return await self.service.pull_ib(symbols, universe, bar_size, prev_days, ib_client_id, max_concurrent)

    @rpcmethod
    async def status(self) -> dict:
        return await self.service.status()
