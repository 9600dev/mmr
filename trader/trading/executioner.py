from enum import Enum
from ib_async import (
    Contract,
    ExecutionCondition,
    LimitOrder,
    MarketOrder,
    Order,
    StopLimitOrder,
    StopOrder,
    Ticker,
    Trade
)
from reactivex import Observable, Observer
from reactivex.abc import DisposableBase
from reactivex.disposable import Disposable
from reactivex.subject import Subject
from trader.common.exceptions import trader_exception, TraderException
from trader.common.logging_helper import get_callstack, log_method, setup_logging
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.data.universe import Universe, UniverseAccessor
from trader.objects import Action, Basket, ContractOrderPair, ExecutorCondition
from trader.trading.order_validator import OrderValidator
from trader.trading.risk_gate import RiskGate, RiskGateResult
from typing import cast, List, Optional, TYPE_CHECKING

import datetime as dt
import reactivex as rx
import reactivex.operators as ops


logging = setup_logging(module_name='trading_runtime')

if TYPE_CHECKING:
    from trader.trading.trading_runtime import Trader


class TradeExecutioner():
    def __init__(
        self,
    ):
        self.trader: 'Trader'
        self.connected: bool = False
        self.validator: OrderValidator = OrderValidator()

    def connect(self, trader: 'Trader'):
        self.trader = trader
        self.connected = True

    def _log_event(self, event_type: EventType, contract: Contract, order: Order,
                   strategy_name: str = 'manual') -> None:
        if hasattr(self.trader, 'event_store'):
            event = TradingEvent(
                event_type=event_type,
                timestamp=dt.datetime.now(),
                strategy_name=strategy_name,
                conid=contract.conId or 0,
                symbol=contract.symbol or '',
                action=str(order.action),
                quantity=float(order.totalQuantity or 0),
                price=float(order.lmtPrice or 0),
                order_id=order.orderId or 0,
            )
            self.trader.event_store.append(event)

    async def subscribe_place_order_direct(
        self,
        contract: Contract,
        order: Order,
    ) -> Observable[Trade]:
        def trader_exception_helper(ex):
            return rx.throw(
                exception=trader_exception(self.trader, exception_type=TraderException, message='place_order()', inner=ex)
            )

        # validate that the order is being placed with the right ib_account number
        if order.account != self.trader.ib_account:
            return trader_exception_helper(ValueError('Order.account is not equal to the ib_account configured'))

        try:
            observable = await self.trader.client.subscribe_place_order(contract, order)
            self._log_event(EventType.ORDER_SUBMITTED, contract, order)
            return observable.pipe(
                ops.catch(lambda ex, src: trader_exception_helper(ex))
            )
        except Exception as ex:
            return trader_exception_helper(ex)

    async def place_order(
        self,
        contract_order: ContractOrderPair,
        condition: ExecutorCondition,
        skip_risk_gate: bool = False,
    ) -> Observable[Trade]:
        # Check trading filter (denylist/allowlist) if available
        if not skip_risk_gate and getattr(self.trader, 'risk_gate', None) is not None:
            instrument_result = self.trader.risk_gate.check_instrument(
                symbol=contract_order.contract.symbol,
                exchange=contract_order.contract.exchange or '',
                sec_type=contract_order.contract.secType or '',
            )
            if not instrument_result.approved:
                self._log_event(EventType.RISK_GATE_REJECTED, contract_order.contract, contract_order.order)
                logging.warning(f'trading filter rejected order: {instrument_result.reason}')
                return rx.throw(
                    trader_exception(
                        trader=self.trader,
                        exception_type=TraderException,
                        message=f'trading filter rejected: {instrument_result.reason}'
                    )
                )

        # Run through risk gate if available
        if not skip_risk_gate and getattr(self.trader, 'risk_gate', None) is not None:
            from trader.trading.strategy import Signal
            # Create a pseudo-signal for risk evaluation
            signal = Signal(
                source_name='manual',
                action=Action.BUY if str(contract_order.order.action) == 'BUY' else Action.SELL,
                probability=1.0,
                risk=0.0,
            )
            result = self.trader.risk_gate.evaluate(
                signal=signal,
                open_order_count=len(self.trader.book.get_orders()) if hasattr(self.trader, 'book') else 0,
            )
            if not result.approved:
                self._log_event(EventType.RISK_GATE_REJECTED, contract_order.contract, contract_order.order)
                logging.warning(f'risk gate rejected order: {result.reason}')
                return rx.throw(
                    trader_exception(
                        trader=self.trader,
                        exception_type=TraderException,
                        message=f'risk gate rejected: {result.reason}'
                    )
                )

        if condition == condition.SANITY_CHECK:
            logging.debug('sanity_check_order for {}'.format(contract_order))
            snapshot: Ticker = await self.trader.client.get_snapshot(contract_order.contract, delayed=False)
            if not self.validator.sanity_check_order(contract_order, self.trader.book, snapshot):
                return rx.throw(
                    trader_exception(
                        trader=self.trader,
                        exception_type=TraderException,
                        message='sanity_check_order failed for {}'.format(contract_order)
                    )
                )

        logging.debug('placing order {}'.format(contract_order.order))
        return await self.subscribe_place_order_direct(contract=contract_order.contract, order=contract_order.order)

    def place_basket(
        self,
        basket: Basket
    ):
        pass

    def cancel_order_id(self, order_id: int) -> Optional[Trade]:
        # get the Order
        order = self.trader.book.get_order(order_id)
        if order and order.clientId == self.trader.trading_runtime_ib_client_id:
            logging.info('cancelling order {}'.format(order))
            trade = self.trader.client.ib.cancelOrder(order)
            return trade
        else:
            logging.error('either order does not exist, or originating client_id is different: {} {}'
                          .format(order, self.trader.trading_runtime_ib_client_id))
            return None

    def cancel_basket(
        self,
        basket: Basket
    ):
        pass

    def helper_create_order(
        self,
        contract: Contract,
        action: Action,
        latest_tick: Ticker,
        equity_amount: Optional[float],
        quantity: Optional[float],
        limit_price: Optional[float],
        market_order: bool,
        stop_loss_percentage: float,
        algo_name: str,
        debug: bool = False,
    ) -> ContractOrderPair:
        if limit_price and limit_price <= 0.0:
            raise ValueError('limit_price specified but invalid: {}'.format(limit_price))
        if stop_loss_percentage >= 1.0 or stop_loss_percentage < 0.0:
            raise ValueError('stop_loss_percentage invalid: {}'.format(stop_loss_percentage))
        if not equity_amount and not quantity:
            raise ValueError('equity_amount or quantity need to be specified')

        order_price = 0.0

        if not quantity and equity_amount:
            # assess if we should trade
            multiplier = float(contract.multiplier) if contract.multiplier else 1.0
            quantity = equity_amount / (latest_tick.bid * multiplier)

            if quantity < 1 and quantity > 0:
                quantity = 1.0

            # toddo round the quantity, but probably shouldn't do this given IB supports fractional shares.
            quantity = round(quantity)

        logging.debug('handle_order assessed quantity: {} on bid: {}'.format(
            quantity, latest_tick.bid
        ))

        if limit_price:
            order_price = float(limit_price)
        elif market_order:
            order_price = latest_tick.ask

        # if debug, move the buy/sell by 10%
        if debug and action == Action.BUY:
            order_price = order_price * 0.9
            order_price = round(order_price * 0.9, ndigits=2)
        if debug and action == Action.SELL:
            order_price = round(order_price * 1.1, ndigits=2)

        stop_loss_price = 0.0

        # calculate stop_loss
        if stop_loss_percentage > 0.0:
            stop_loss_price = round(order_price - order_price * stop_loss_percentage, ndigits=2)

        order: Order = Order()

        if market_order and stop_loss_price > 0:
            order = StopOrder(
                action=str(action),
                totalQuantity=cast(float, quantity),
                stopPrice=stop_loss_price,
                orderRef=algo_name,
                account=self.trader.ib_account,
            )
        elif market_order and stop_loss_price == 0.0:
            order = MarketOrder(
                action=str(action),
                totalQuantity=cast(float, quantity),
                orderRef=algo_name,
                account=self.trader.ib_account,
            )
        if not market_order and stop_loss_price > 0:
            order = StopLimitOrder(
                action=str(action),
                totalQuantity=cast(float, quantity),
                lmtPrice=order_price,
                stopPrice=stop_loss_price,
                orderRef=algo_name,
                account=self.trader.ib_account,
            )
        elif not market_order and stop_loss_price == 0.0:
            order = LimitOrder(
                action=str(action),
                totalQuantity=cast(float, quantity),
                lmtPrice=order_price,
                orderRef=algo_name,
                account=self.trader.ib_account,
            )
        return ContractOrderPair(contract=contract, order=order)
