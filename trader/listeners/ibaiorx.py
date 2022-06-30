# the fundamentals of asyncio: https://www.integralist.co.uk/posts/python-asyncio/

import os
import asyncio
import datetime
import datetime as dt
import aioreactive as rx
import pandas as pd
import ib_insync as ibapi
import backoff
import random
import aiohttp
from aioreactive.subject import AsyncMultiSubject
from aioreactive.types import AsyncObservable, AsyncObserver
from aioreactive.observers import AsyncAnonymousObserver, auto_detach_observer, safe_observer
from aioreactive.observables import AsyncAnonymousObservable
from expression.system.disposable import Disposable, AsyncDisposable

from re import I
from eventkit import event
from ib_insync.ib import IB
from ib_insync.util import schedule
from ib_insync.client import Client
from ib_insync.contract import ContractDescription, ContractDetails, Stock, Contract, Forex, Future, Option
from ib_insync.objects import PortfolioItem, Position, RealTimeBarList, BarData, BarDataList, RealTimeBar, Fill
from ib_insync.order import Order, BracketOrder, LimitOrder, StopOrder, OrderStatus, MarketOrder, ExecutionCondition, Trade
from ib_insync.order import StopLimitOrder
from ib_insync.util import df
from ib_insync.ticker import Ticker
from eventkit.event import Event

from asyncio import BaseEventLoop
from asyncio.events import AbstractEventLoop
from expression.core import pipe

from enum import Enum
from typing import (
    List,
    Dict,
    Tuple,
    Callable,
    Coroutine,
    Awaitable,
    Optional,
    Set,
    Generic,
    TypeVar,
    Union,
    AsyncGenerator,
    cast,
)

from trader.common.listener_helpers import Helpers
from trader.common.helpers import Pipe
from trader.common.logging_helper import setup_logging
from trader.common.reactive import AsyncCachedSubject, AsyncCachedPandasSubject, AsyncCachedObserver
from trader.common.reactive import AsyncCachedObservable, awaitify, AsyncEventSubject
from trader.listeners.ib_history_worker import IBHistoryWorker
from trader.objects import WhatToShow, ReportType

logging = setup_logging(module_name="ibaiorx")

TAny = TypeVar('TAny')
TKey = TypeVar('TKey')
TValue = TypeVar('TValue')

# With aioreactive you subscribe observers to observables,


class IBAIORxError():
    def __init__(self, reqId: int, errorCode: int, errorString: str, contract: Contract):
        self.reqId = reqId
        self.errorCode = errorCode
        self.errorString = errorString
        self.contract = contract

    def __str__(self):
        return 'reqId: {}, errorCode: {}, errorString: {}, contract: {}'.format(
            self.reqId,
            self.errorCode,
            self.errorString,
            self.contract
        )


class IBAIORx():
    # master client id should be set to 5 (incremented on the call to connect())
    # todo moving this to random for now, will find a global sync solution later
    client_id_counter = random.randint(4, 98)

    def __init__(
        self,
        ib_server_address: str,
        ib_server_port: int,
        read_only: bool = False
    ):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.read_only = read_only

        self.ib = IB()

        # def mapper(tickers: Set[Ticker]) -> rx.AsyncObservable[Ticker]:
        #     return rx.from_iterable(tickers)

        def mapper(tickers: Set[Ticker]) -> rx.AsyncObservable[Ticker]:
            # warning: for whatever reason, we can get either a Set of tickers or a single ticker.
            # if this raises an exception, all hell brakes loose - we get multiple Tasks
            # that sit around pending, and all sorts...
            if isinstance(tickers, Set):
                return rx.from_iterable(tickers)
            elif isinstance(tickers, Ticker):
                return rx.single(tickers)
            else:
                raise ValueError(tickers)

        self.market_data_subject: rx.AsyncSubject[Ticker] = rx.AsyncSubject[Ticker]()
        self.historical_data_subject: rx.AsyncSubject[pd.DataFrame] = rx.AsyncSubject[pd.DataFrame]()
        self.bars_data_subject = AsyncEventSubject[RealTimeBarList](eventkit_event=self.ib.barUpdateEvent)
        self.positions_subject = AsyncEventSubject[List[Position]](eventkit_event=self.ib.positionEvent)
        self.portfolio_subject = AsyncEventSubject[PortfolioItem](eventkit_event=self.ib.updatePortfolioEvent)
        self.trades_subject = AsyncEventSubject[Trade](eventkit_event=self.ib.orderStatusEvent)
        self._contracts_source = AsyncEventSubject[Set[Ticker]](eventkit_event=self.ib.pendingTickersEvent)
        self.error_subject = AsyncMultiSubject[IBAIORxError]()
        self.error_disposables: Dict[int, AsyncDisposable] = {}

        mapped: AsyncObservable[Ticker] = pipe(
            self._contracts_source,
            rx.flat_map(mapper)
        )

        self.contracts_subject = AsyncAnonymousObservable(mapped.subscribe_async)

        self.contracts_cache: Dict[Contract, rx.AsyncObservable] = {}
        self.bars_cache: Dict[Contract, rx.AsyncObservable[RealTimeBarList]] = {}
        self.historical_subscribers: Dict[Contract, int] = {}
        self.history_worker: Optional[IBHistoryWorker] = None
        self._shutdown: bool = True

        # try binding helper methods to things we care about
        Contract.to_df = Helpers.to_df  # type: ignore

    async def __handle_error(self, reqId, errorCode, errorString, contract):
        global error_code

        if errorCode == 2104 or errorCode == 2158 or errorCode == 2106:
            return

        logging.error(
            "ibrx reqId: {} errorCode {} errorString {} contract {}".format(
                reqId, errorCode, errorString, contract
            )
        )
        await self.error_subject.asend(IBAIORxError(reqId, errorCode, errorString, contract))

    @backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=30)
    def connect(self):
        def __handle_client_id_error(msg):
            logging.error('clientId already in use, randomizing and trying again')
            IBAIORx.client_id_counter = random.randint(10, 99)
            raise ValueError('clientId')

        if self.ib.isConnected():
            return self

        # todo set in the readme that the master client ID has to be set to 5
        IBAIORx.client_id_counter += 1
        self._shutdown = False

        if self.__handle_error not in self.ib.errorEvent:
            self.ib.errorEvent += self.__handle_error

        net_client = cast(Client, self.ib.client)
        net_client.conn.disconnected += __handle_client_id_error

        self.ib.connect(
            self.ib_server_address,
            self.ib_server_port,
            clientId=IBAIORx.client_id_counter,
            timeout=10,
            readonly=self.read_only
        )

        net_client.conn.disconnected -= __handle_client_id_error
        self.history_worker = IBHistoryWorker(self.ib)

        return self

    async def shutdown(self):
        if self._shutdown:
            logging.debug('ibaiorx is already shutdown')
            return

        logging.debug('ibaiorx.shutdown(), disconnecting clients and disposing aioreactive subscriptions')

        if self.history_worker:
            await self.history_worker.disconnect()

        await self.market_data_subject.dispose_async()
        await self.historical_data_subject.dispose_async()
        await self.bars_data_subject.dispose_async()
        await self.positions_subject.dispose_async()
        await self.portfolio_subject.dispose_async()
        await self.trades_subject.dispose_async()
        await self._contracts_source.dispose_async()
        await self.error_subject.dispose_async()

        for reqId, disposable in self.error_disposables.items():
            await disposable.dispose_async()

        self.ib.disconnect()
        self._shutdown = True

    def is_connected(self):
        return self.ib.isConnected()

    def _filter_contract(self, contract: Contract, data) -> bool:
        if data.contract:
            return data.contract.conId == contract.conId
        else:
            logging.debug('_filter_contract failed, as there is no contract object')
            return False

    # one shot, rather than hot observable like the other subscribe_ functions
    # hence, we return disposable, and we hook it up before calling the placeOrder sync method
    async def subscribe_place_order(
        self,
        contract: Contract,
        order: Order,
        observer: rx.AsyncObserver[Trade]
    ) -> AsyncDisposable:
        # the order object gets filled with the order details (clientId, orderId etc)
        # the trade object returned from 'placeOrder' gets filled later, but we don't return it
        # as we want the subscription stream to contain all relevant trade details

        xs = pipe(
            self.trades_subject,
            rx.filter(lambda trade: self._filter_contract(contract, trade)),  # type: ignore
        )

        disposable = await xs.subscribe_async(observer)
        await self.trades_subject.call_event_subscriber_sync(lambda: self.ib.placeOrder(contract, order))
        # todo, figure out what to do here with the disposable
        # should it cancel the order, or just stop listening?
        return disposable

    async def subscribe_cancel_order(
        self,
        contract: Contract,
        order: Order,
        observer: rx.AsyncObserver[Trade]
    ) -> AsyncDisposable:
        xs = pipe(
            self.trades_subject,
            rx.filter(lambda trade: self._filter_contract(contract, trade)),  # type: ignore
        )

        disposable = await xs.subscribe_async(observer)
        await self.trades_subject.call_event_subscriber_sync(lambda: self.ib.cancelOrder(order))

        return disposable

    async def __subscribe_contract(
        self,
        contract: Contract,
        one_time_snapshot: bool = False,
        delayed: bool = False,
    ) -> rx.AsyncAnonymousObservable[Ticker]:
        if delayed:
            # 1 = Live
            # 2 = Frozen
            # 3 = Delayed
            # 4 = Delayed frozen
            logging.debug('reqMarketDataType(3)')
            self.ib.reqMarketDataType(3)

        # reqMktData immediately returns with an empty ticker
        # and starts the subscription

        reqId = self.ib.client._reqIdSeq
        result = await self._contracts_source.call_event_subscriber_sync(
            lambda: self.ib.reqMktData(
                contract=contract,
                genericTickList='',
                snapshot=one_time_snapshot,
                regulatorySnapshot=False,
            ),
            asend_result=False
        )

        if delayed:
            self.ib.reqMarketDataType(1)
            logging.debug('reqMarketDataType(1)')

        contract_filter: AsyncAnonymousObservable[Ticker] = pipe(
            self.contracts_subject,
            rx.filter(lambda ticker: self._filter_contract(contract, ticker)),  # type: ignore
        )

        xs: AsyncMultiSubject = AsyncMultiSubject()
        await contract_filter.subscribe_async(xs)

        # error handling, which will listen to the error source, and pipe
        # any errors through to the subscriber
        async def handle_error(error: IBAIORxError):
            logging.error('__subscribe_snapshot() had error: {}'.format(error))
            await xs.athrow(Exception(error))

        async def handle_exception(exception: Exception):
            logging.error('__subscribe_snapshot() threw {}'.format(exception))
            await xs.athrow(exception)

        error_observer = AsyncAnonymousObserver(asend=handle_error, athrow=handle_exception)
        err = pipe(
            self.error_subject,
            rx.filter(lambda error: error.reqId == reqId)  # type: ignore
        )
        safe_obv, auto_detach = auto_detach_observer(obv=error_observer)
        self.error_disposables[reqId] = await pipe(safe_obv, err.subscribe_async, auto_detach)

        return AsyncAnonymousObservable(xs.subscribe_async)

    async def subscribe_contract(
        self,
        contract: Contract,
        one_time_snapshot: bool = False,
        delayed: bool = False,
    ) -> rx.AsyncObservable[Ticker]:
        if contract not in self.contracts_cache:
            self.contracts_cache[contract] = await self.__subscribe_contract(contract, one_time_snapshot, delayed)
        return self.contracts_cache[contract]

    def unsubscribe_contract(self, contract: Contract):
        raise ValueError('not implemented')

    async def subscribe_barlist(
        self,
        contract: Contract,
        wts: WhatToShow = WhatToShow.MIDPOINT
    ) -> rx.AsyncObservable[RealTimeBarList]:
        # todo this method subscribes and populates a RealTimeBarsList object,
        # which I'm sure will end up being a memory leak
        bar_size = 5

        if contract in self.bars_cache:
            return self.bars_cache[contract]

        await self.bars_data_subject.call_event_subscriber_sync(
            lambda: self.ib.reqRealTimeBars(contract, bar_size, str(wts), False)
        )

        xs = pipe(
            self.bars_data_subject,
            rx.filter(lambda bar_data_list: self._filter_contract(contract, bar_data_list)),  # type: ignore
        )

        self.bars_cache[contract] = xs
        return xs

    def unsubscribe_barlist(self, contract: Contract):
        if contract in self.bars_cache and self.bars_data_subject.value():
            self.ib.cancelRealTimeBars(cast(RealTimeBarList, self.bars_data_subject.value()))
            del self.bars_cache[contract]
        else:
            logging.debug('unsubscribe_barlist failed for {}'.format(contract))

    async def subscribe_positions(self) -> rx.AsyncObservable[List[Position]]:
        await self.positions_subject.call_event_subscriber(self.ib.reqPositionsAsync())
        return self.positions_subject

    async def subscribe_portfolio(self) -> rx.AsyncObservable[PortfolioItem]:
        portfolio_items = self.ib.portfolio()
        for item in portfolio_items:
            await self.portfolio_subject.asend(item)
        return self.portfolio_subject

    async def get_open_orders(self) -> List[Order]:
        return await self.ib.reqAllOpenOrdersAsync()

    async def get_completed_orders(self) -> List[Trade]:
        return await self.ib.reqCompletedOrdersAsync(apiOnly=True)

    async def get_executions(self) -> List[Fill]:
        return await self.ib.reqExecutionsAsync()

    async def get_snapshot(self, contract: Contract, delayed: bool = False) -> Ticker:
        # trying to do this stuff synchronously in rx is a nightmare
        _task: asyncio.Event = asyncio.Event()
        populated_ticker: Optional[Ticker] = None
        thrown_exception: Optional[Exception] = None

        async def update_ticker(ticker: Ticker):
            nonlocal populated_ticker
            populated_ticker = ticker
            _task.set()

        async def athrow(ex: Exception):
            nonlocal thrown_exception
            thrown_exception = ex
            _task.set()

        async def aclose():
            logging.debug('get_snapshot() aclose')

        # we've got to subscribe, then wait for the first Ticker to arrive in the events
        # cachedeventsource, subscribers get the last value after calling subscribe
        observable: AsyncAnonymousObservable[Ticker] = await self.__subscribe_contract(
            contract=contract,
            one_time_snapshot=True,
            delayed=delayed,
        )

        xs: AsyncAnonymousObservable[Ticker] = pipe(
            observable,
            rx.take(1)  # type: ignore
        )

        observer = AsyncAnonymousObserver(asend=update_ticker, athrow=athrow, aclose=aclose)
        safe_obv, auto_detach = auto_detach_observer(obv=observer)
        subscription = await pipe(safe_obv, xs.subscribe_async, auto_detach)

        await _task.wait()
        await safe_obv.aclose()
        await subscription.dispose_async()
        await asyncio.sleep(0.1)
        if thrown_exception is not None:
            ex = cast(Exception, thrown_exception)
            raise ex
        return cast(Ticker, populated_ticker)

    async def get_contract_details(self, contract: Contract) -> List[ContractDetails]:
        result = await self.ib.reqContractDetailsAsync(contract)
        if not result:
            return []
        else:
            return cast(List[ContractDetails], result)

    async def get_fundamental_data_sync(
        self,
        contract: Contract,
        report_type: ReportType = ReportType.ReportSnapshot
    ) -> rx.AsyncObservable[str]:
        return rx.from_async(self.ib.reqFundamentalDataAsync(contract, reportType=str(report_type)))

    async def get_matching_symbols(self, symbol: str) -> List[ContractDescription]:
        result = await self.ib.reqMatchingSymbolsAsync(symbol)
        if not result: return []
        else: return result

    async def __get_contract_description_helper(
        self, symbol: str, secType: str, primaryExchange: str, currency: str
    ) -> Optional[ContractDescription]:
        contract_desc: List[ContractDescription] = await self.get_matching_symbols(symbol)
        f: List[ContractDescription] = []
        if len(contract_desc) == 1 and contract_desc[0].contract:
            return contract_desc[0]
        elif len(contract_desc) > 0:
            if secType:
                f = f + [
                    desc
                    for desc in contract_desc
                    if desc.contract and desc.contract.secType == secType
                ]
            if currency:
                f = f + [
                    desc
                    for desc in contract_desc
                    if desc.contract and desc.contract.currency == currency
                ]
            if len(f) > 0:
                return f[0]
            else:
                return None
        else:
            logging.info("get_contract_description_helper {} returned nothing".format(symbol))
            return None

    async def get_contract_description(
        self,
        symbols: Union[str, List[str]],
        secType: str = 'STK',
        primaryExchange: str = 'SMART',
        currency: str = 'USD'
    ) -> Union[Optional[ContractDescription], List[ContractDescription]]:
        """
        Args:
            secType (str): the security type
            * 'STK' = Stock (or ETF)
            * 'OPT' = Option
            * 'FUT' = Future
            * 'IND' = Index
            * 'FOP' = Futures option
            * 'CASH' = Forex pair
            * 'CFD' = CFD
            * 'BAG' = Combo
            * 'WAR' = Warrant
            * 'BOND'= Bond
            * 'CMDTY'= Commodity
            * 'NEWS' = News
            * 'FUND'= Mutual fund
        """
        if type(symbols) is list:
            result = [
                await self.__get_contract_description_helper(symbol, secType, primaryExchange, currency)
                for symbol in symbols
            ]
            return [r for r in result if r]
        else:
            return await self.__get_contract_description_helper(str(symbols), secType, primaryExchange, currency)

    async def get_conid(
        self,
        symbols: Union[str, List[str]],
        secType: str = 'STK',
        primaryExchange: str = 'SMART',
        currency: str = 'USD',
    ) -> Union[Optional[Contract], List[Contract]]:
        """
        Args:
            secType (str): the security type
            * 'STK' = Stock (or ETF)
            * 'OPT' = Option
            * 'FUT' = Future
            * 'IND' = Index
            * 'FOP' = Futures option
            * 'CASH' = Forex pair
            * 'CFD' = CFD
            * 'BAG' = Combo
            * 'WAR' = Warrant
            * 'BOND'= Bond
            * 'CMDTY'= Commodity
            * 'NEWS' = News
            * 'FUND'= Mutual fund
        """
        if type(symbols) is list:
            result = [
                await self.__get_contract_description_helper(symbol, secType, primaryExchange, currency)
                for symbol in symbols
            ]
            if result: return [r.contract for r in result if r and r.contract]
            else: return None
        else:
            single_result = await self.__get_contract_description_helper(str(symbols), secType, primaryExchange, currency)
            if single_result and single_result.contract: return single_result.contract
            else: return None

    async def get_contract_history(
        self,
        contract: Contract,
        start_date: dt.datetime,
        end_date: dt.datetime = dt.datetime.now(),
        bar_size: str = '1 min',
        what_to_show: WhatToShow = WhatToShow.MIDPOINT,
    ) -> pd.DataFrame:
        if self.history_worker and not self.history_worker.connected:
            await asyncio.wait_for(self.history_worker.connect(), timeout=20.0)

        return await self.history_worker.get_contract_history(  # type: ignore
            security=contract,
            what_to_show=what_to_show,
            bar_size=bar_size,
            start_date=start_date,
            end_date=end_date,
            filter_between_dates=True
        )

    async def subscribe_contract_history(
        self,
        contract: Contract,
        start_date: dt.datetime,
        what_to_show: WhatToShow,
        observer: AsyncObserver[pd.DataFrame],
        refresh_interval: int = 60,
    ) -> AsyncDisposable:
        async def __update(
            subject: AsyncCachedPandasSubject,
            contract: Contract,
            start_date: dt.datetime,
            end_date: dt.datetime
        ):
            if subject._is_disposed:
                return

            data = await self.get_contract_history(
                contract=contract,
                start_date=start_date,
                end_date=end_date,
                what_to_show=what_to_show,
            )

            print(data)
            await subject.asend(data)

            start_date = end_date
            end_date = end_date + dt.timedelta(minutes=1)
            loop = asyncio.get_event_loop()
            loop.call_later(
                refresh_interval,
                asyncio.create_task,
                __update(subject, contract, start_date, end_date)
            )

        subject = AsyncCachedPandasSubject()

        end_date = dt.datetime.now(dt.timezone.utc).astimezone(start_date.tzinfo)

        loop = asyncio.get_event_loop()
        loop.call_later(1, asyncio.create_task, __update(subject, contract, start_date, end_date))
        return await subject.subscribe_async(observer)

    def sleep(self, seconds: float):
        self.ib.sleep(seconds)

    def run(self, *args):
        self.ib.run(*args)

    def client(self):
        return self.ib
