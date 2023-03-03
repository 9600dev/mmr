from ib_insync.contract import Contract
from ib_insync.objects import PnLSingle, PortfolioItem, Position
from ib_insync.order import Order, Trade
from ib_insync.ticker import Ticker
from reactivex.abc import DisposableBase
from reactivex.disposable import Disposable
from reactivex.observer import Observer
from trader.common.logging_helper import setup_logging
from trader.common.reactivex import SuccessFail, SuccessFailEnum
from trader.data.data_access import PortfolioSummary
from trader.messaging.clientserver import RPCHandler
from typing import Optional

import asyncio
import trader.trading.trading_runtime as runtime


logging = setup_logging(module_name='trader_service_api')


class TraderServiceApi(RPCHandler):
    def __init__(self, trader):
        self.trader: runtime.Trader = trader

    # json can't encode Dict's with keys that aren't primitive and hashable
    # so we often have to convert to weird containers like List[Tuple]
    @RPCHandler.rpcmethod
    def get_positions(self) -> list[Position]:
        return self.trader.portfolio.get_positions()

    @RPCHandler.rpcmethod
    def get_portfolio(self) -> list[PortfolioItem]:
        return self.trader.client.ib.portfolio()

    @RPCHandler.rpcmethod
    def get_portfolio_summary(self) -> list[PortfolioSummary]:
        return self.trader.get_portfolio_summary()

    @RPCHandler.rpcmethod
    def get_universes(self) -> dict[str, int]:
        return self.trader.universe_accessor.list_universes_count()

    @RPCHandler.rpcmethod
    def get_trades(self) -> dict[int, list[Trade]]:
        return self.trader.book.get_trades()

    @RPCHandler.rpcmethod
    def get_orders(self) -> dict[int, list[Order]]:
        return self.trader.book.get_orders()

    @RPCHandler.rpcmethod
    def get_pnl(self) -> list[PnLSingle]:
        return self.trader.get_pnl()

    @RPCHandler.rpcmethod
    def place_order(
        self, contract: Contract,
        action: str,
        equity_amount: Optional[float],
        quantity: Optional[float],
        limit_price: Optional[float],
        market_order: bool = False,
        stop_loss_percentage: float = 0.0,
        debug: bool = False,
    ) -> SuccessFail:
        # todo: we'll have to make the cli async so we can subscribe to the trade
        # changes as orders get hit etc
        logging.warn('place_order() is not complete, your mileage may vary')
        from trader.trading.trading_runtime import Action
        act = Action.BUY if 'BUY' in action else Action.SELL

        task = asyncio.Event()
        disposable: DisposableBase = Disposable()
        result: Optional[SuccessFail] = None

        loop = asyncio.get_event_loop()

        def on_next(trade: Trade):
            nonlocal result
            result = SuccessFail(success_fail=SuccessFailEnum.SUCCESS, obj=trade)
            task.set()

        def on_error(ex):
            nonlocal result
            result = SuccessFail(success_fail=SuccessFailEnum.FAIL, exception=ex)

        def on_completed():
            pass

        observer = Observer(on_next=on_next, on_completed=on_completed, on_error=on_error)

        disposable = loop.run_until_complete(
            self.trader.handle_order(
                contract=contract,
                action=act,
                equity_amount=equity_amount,
                quantity=quantity,
                limit_price=limit_price,
                market_order=market_order,
                stop_loss_percentage=stop_loss_percentage,
                observer=observer,
                debug=debug,
            )
        )

        loop.run_until_complete(task.wait())
        disposable.dispose()
        return result if result else SuccessFail(SuccessFailEnum.FAIL)

    @RPCHandler.rpcmethod
    def cancel_order(self, order_id: int) -> Optional[Trade]:
        return self.trader.cancel_order(order_id)

    @RPCHandler.rpcmethod
    def get_snapshot(self, contract: Contract, delayed: bool) -> Ticker:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.trader.client.get_snapshot(contract=contract, delayed=delayed))

    @RPCHandler.rpcmethod
    def publish_contract(self, contract: Contract, delayed: bool) -> bool:
        self.trader.publish_contract(contract, delayed)
        return True

    @RPCHandler.rpcmethod
    def get_published_contracts(self) -> list[int]:
        return list(self.trader.zmq_pubsub_contracts.keys())

    @RPCHandler.rpcmethod
    def get_unique_client_id(self) -> int:
        return self.trader.get_unique_client_id()

    @RPCHandler.rpcmethod
    def release_client_id(self, client_id: int) -> bool:
        self.trader.release_client_id(client_id)
        return True

    @RPCHandler.rpcmethod
    def start_load_test(self) -> int:
        logging.debug('start_load_test()')
        self.trader.start_load_test()
        return 0

    @RPCHandler.rpcmethod
    def stop_load_test(self) -> int:
        logging.debug('stop_load_test()')
        self.trader.load_test = False
        return 0
