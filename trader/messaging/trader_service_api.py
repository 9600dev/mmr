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
        skip_risk_gate: bool = False,
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
            task.set()

        def on_completed():
            task.set()

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
            skip_risk_gate=skip_risk_gate,
        )
        observable.subscribe(observer)

        try:
            await asyncio.wait_for(task.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            if result is None:
                result = SuccessFail.fail(error='order placement timed out waiting for confirmation')
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
    async def get_market_depth(self, contract: Contract, num_rows: int = 5, is_smart_depth: bool = False) -> dict:
        return await self.trader.get_market_depth(contract, num_rows, is_smart_depth)

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
    async def reload_strategies(self) -> SuccessFail[list[StrategyConfig]]:
        return await self.trader.reload_strategies()

    @rpcmethod
    def get_status(self) -> dict:
        return self.trader.status()

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
    async def scanner_locations(self) -> list[dict]:
        """Diagnostic: list every location code this account is
        authorised to scan. If your target (e.g. ``STK.AU.ASX``) isn't
        in the result, the string is wrong or the subscription is
        missing — error 162 would follow any scan against it."""
        return await self.trader.scanner_locations()

    @rpcmethod
    async def get_snapshots_batch(self, contracts: list, delayed: bool = False) -> list[dict]:
        return await self.trader.get_snapshots_batch(contracts, delayed)

    @rpcmethod
    async def get_history_bars(self, contract: Contract, duration: str = '60 D', bar_size: str = '1 day') -> list[dict]:
        return await self.trader.get_history_bars(contract, duration, bar_size)

    @rpcmethod
    async def get_fundamental_data(self, contract: Contract, report_type: str = 'ReportSnapshot') -> str:
        return await self.trader.get_fundamental_data(contract, report_type)

    @rpcmethod
    async def get_news_headlines(self, conId: int, provider_codes: str = '',
                                  total_results: int = 5) -> list[dict]:
        return await self.trader.get_news_headlines(conId, provider_codes, total_results)

    @rpcmethod
    async def place_expressive_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        execution_spec: dict,
        algo_name: str = 'proposal',
    ) -> SuccessFail[list[Trade]]:
        """Place an order with full execution specification (brackets, trailing stops, etc.)."""
        result = await self.trader.place_expressive_order(
            contract=contract,
            action=action,
            quantity=quantity,
            execution_spec=execution_spec,
            algo_name=algo_name,
        )
        return result

    @rpcmethod
    async def place_standalone_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        order_type: str,
        aux_price: float = 0,
        limit_price: float = 0,
        trailing_percent: float = 0,
        tif: str = 'GTC',
        outside_rth: bool = True,
    ) -> SuccessFail[Trade]:
        """Place a standalone protective order (stop, trailing stop, limit)."""
        return await self.trader.place_standalone_order(
            contract=contract,
            action=action,
            quantity=quantity,
            order_type=order_type,
            aux_price=aux_price,
            limit_price=limit_price,
            trailing_percent=trailing_percent,
            tif=tif,
            outside_rth=outside_rth,
        )

    @rpcmethod
    async def check_order_margin(
        self,
        contract: Contract,
        order: Order,
    ) -> dict:
        """Simulate order to get margin impact without placing it."""
        return await self.trader.check_order_margin(contract, order)

    @rpcmethod
    def get_risk_limits(self) -> dict:
        """Return current risk gate limits."""
        from dataclasses import asdict
        return asdict(self.trader.risk_gate.limits)

    @rpcmethod
    def set_risk_limits(self, **kwargs) -> dict:
        """Update risk gate limits. Only provided fields are changed."""
        from dataclasses import asdict
        limits = self.trader.risk_gate.limits
        for key, value in kwargs.items():
            if hasattr(limits, key):
                setattr(limits, key, type(getattr(limits, key))(value))
        return asdict(limits)

    @rpcmethod
    async def get_ib_account(self) -> str:
        return self.trader.ib_account

    @rpcmethod
    def get_account_values(self) -> dict:
        """Return key account values (cash, net liquidation, buying power, etc.)."""
        vals = self.trader.client.ib.accountValues()
        keys = {
            'TotalCashValue', 'NetLiquidation', 'AvailableFunds',
            'BuyingPower', 'GrossPositionValue', 'MaintMarginReq',
            'InitMarginReq', 'ExcessLiquidity', 'Cushion',
            'FullInitMarginReq', 'FullMaintMarginReq',
            'DayTradesRemaining',
        }
        result = {}
        for v in vals:
            if v.tag in keys and v.currency != 'BASE':
                result[v.tag] = {'value': v.value, 'currency': v.currency}
        return result
