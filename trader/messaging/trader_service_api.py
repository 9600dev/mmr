from ib_async import TradeLogEntry
from ib_async.contract import Contract
from ib_async.objects import PnLSingle, PortfolioItem, Position
from ib_async.order import Order, Trade
from ib_async.ticker import Ticker
from reactivex.abc import DisposableBase
from reactivex.disposable import Disposable
from reactivex.observer import Observer
from trader.common.logging_helper import setup_logging
from trader.common.reactivex import SuccessFail, SuccessFailEnum
from trader.data.data_access import PortfolioSummary, SecurityDefinition
from trader.data.universe import Universe
from trader.messaging.clientserver import RPCHandler, rpcmethod
from trader.objects import TickList, TradeLogSimple
from trader.trading.strategy import StrategyConfig, StrategyState
from typing import List, Optional, Tuple, Union

import asyncio
import time
import trader.trading.trading_runtime as runtime


logging = setup_logging(module_name='trader_service_api')


class TraderServiceApi(RPCHandler):
    def __init__(self, trader):
        self.trader: runtime.Trader = trader

    # json can't encode Dict's with keys that aren't primitive and hashable
    # so we often have to convert to weird containers like List[Tuple]
    @rpcmethod
    def get_positions(self) -> list[Position]:
        return self.trader.get_positions()

    @rpcmethod
    def get_portfolio(self) -> list[PortfolioItem]:
        return self.trader.client.ib.portfolio()

    @rpcmethod
    def get_portfolio_summary(self) -> list[PortfolioSummary]:
        return self.trader.get_portfolio_summary()

    @rpcmethod
    def get_universes(self) -> dict[str, int]:
        return self.trader.universe_accessor.list_universes_count()

    @rpcmethod
    def get_trades(self) -> dict[int, list[Trade]]:
        return self.trader.book.get_trades()

    @rpcmethod
    def get_trade_log(self) -> list[TradeLogSimple]:
        return self.trader.book.get_trade_log()

    @rpcmethod
    def get_orders(self) -> dict[int, list[Order]]:
        return self.trader.book.get_orders()

    @rpcmethod
    def get_pnl(self) -> list[PnLSingle]:
        return self.trader.get_pnl()

    @rpcmethod
    async def place_order_simple(
        self, contract: Contract,
        action: str,
        equity_amount: Optional[float],
        quantity: Optional[float],
        limit_price: Optional[float],
        market_order: bool = False,
        stop_loss_percentage: float = 0.0,
        algo_name: str = 'global',
        debug: bool = False,
    ) -> SuccessFail[Trade]:
        # todo: we'll have to make the cli async so we can subscribe to the trade
        # changes as orders get hit etc
        logging.warn('place_order_simple() is not complete, your mileage may vary')
        from trader.trading.trading_runtime import Action
        act = Action.BUY if 'BUY' in action else Action.SELL

        task = asyncio.Event()
        disposable: DisposableBase = Disposable()
        result: Optional[SuccessFail] = None

        def on_next(trade: Trade):
            nonlocal result
            result = SuccessFail.success(obj=trade)
            task.set()

        def on_error(ex):
            nonlocal result
            result = SuccessFail.fail(exception=ex)

        def on_completed():
            pass

        observer = Observer(on_next=on_next, on_completed=on_completed, on_error=on_error)

        observable = await self.trader.place_order_simple(
            contract=contract,
            action=act,
            equity_amount=equity_amount,
            quantity=quantity,
            limit_price=limit_price,
            market_order=market_order,
            stop_loss_percentage=stop_loss_percentage,
            algo_name=algo_name,
            debug=debug,
        )
        observable.subscribe(observer)

        await task.wait()
        disposable.dispose()
        return result if result else SuccessFail.fail()

    @rpcmethod
    def cancel_order(self, order_id: int) -> SuccessFail[Trade]:
        order = self.trader.cancel_order(order_id)
        if order:
            return SuccessFail.success(order)
        else:
            return SuccessFail.fail()

    @rpcmethod
    def cancel_all(self) -> SuccessFail[list[int]]:
        return self.trader.cancel_all()

    @rpcmethod
    async def get_snapshot(self, contract: Contract, delayed: bool) -> Ticker:
        return await self.trader.client.get_snapshot(contract=contract, delayed=delayed)

    @rpcmethod
    def publish_contract(self, contract: Contract, delayed: bool) -> bool:
        self.trader.publish_contract(contract, delayed)
        return True

    @rpcmethod
    def get_published_contracts(self) -> list[int]:
        return list(self.trader.zmq_pubsub_contracts.keys())

    @rpcmethod
    def get_unique_client_id(self) -> int:
        return self.trader.get_unique_client_id()

    @rpcmethod
    async def resolve_contract(self, contract: Contract) -> list[SecurityDefinition]:
        return await self.trader.resolve_contract(contract)

    @rpcmethod
    async def resolve_symbol(
        self,
        symbol: Union[str, int],
        exchange: str = '',
        universe: str = '',
        sec_type: str = ''
    ) -> list[SecurityDefinition]:
        return await self.trader.resolve_symbol(symbol, exchange, universe, sec_type)

    @rpcmethod
    async def resolve_universe(
        self,
        symbol: Union[str, int],
        exchange: str,
        universe: str,
        sec_type: str,
    ) -> list[tuple[str, SecurityDefinition]]:
        return await self.trader.resolve_universe(symbol, exchange, universe, sec_type)

    @rpcmethod
    def release_client_id(self, client_id: int) -> bool:
        self.trader.release_client_id(client_id)
        return True

    @rpcmethod
    async def get_shortable_shares(self, contract: Contract) -> float:
        return await self.trader.get_shortable_shares(contract)

    @rpcmethod
    async def get_strategies(self) -> SuccessFail[list[StrategyConfig]]:
        return await self.trader.get_strategies()

    @rpcmethod
    async def enable_strategy(self, name: str, paper: bool) -> SuccessFail[StrategyState]:
        return await self.trader.enable_strategy(name, paper)

    @rpcmethod
    async def disable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        return await self.trader.disable_strategy(name)

    @rpcmethod
    async def scanner_data(
        self,
        scan_code: str = 'TOP_PERC_GAIN',
        instrument: str = 'STK',
        location_code: str = 'STK.US.MAJOR',
        num_rows: int = 20,
        above_price: float = 0.0,
        above_volume: int = 0,
        market_cap_above: float = 0.0,
    ) -> list[dict]:
        return await self.trader.scanner_data(
            scan_code=scan_code,
            instrument=instrument,
            location_code=location_code,
            num_rows=num_rows,
            above_price=above_price,
            above_volume=above_volume,
            market_cap_above=market_cap_above,
        )

    @rpcmethod
    async def get_ib_account(self) -> str:
        return self.trader.ib_account
