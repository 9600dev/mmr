# the fundamentals of asyncio: https://www.integralist.co.uk/posts/python-asyncio/

from ib_insync.client import Client
from ib_insync.contract import Contract, ContractDescription, ContractDetails
from ib_insync.ib import IB
from ib_insync.objects import Fill, PortfolioItem, Position, RealTimeBarList
from ib_insync.order import Order, Trade
from ib_insync.ticker import Ticker
from reactivex import operators as ops
from reactivex.abc import DisposableBase, ObserverBase
from reactivex.observable import Observable
from reactivex.observer import AutoDetachObserver, Observer
from reactivex.subject import Subject
from trader.common.listener_helpers import Helpers
from trader.common.logging_helper import setup_logging
from trader.common.reactivex import EventSubject
from trader.listeners.ib_history_worker import IBHistoryWorker
from trader.objects import BarSize, WhatToShow
from typing import cast, Dict, List, Optional, Set, TypeVar, Union

import asyncio
import backoff
import datetime
import datetime as dt
import pandas as pd
import reactivex as rx


logging = setup_logging(module_name="ibreactivex")

TAny = TypeVar('TAny')
TKey = TypeVar('TKey')
TValue = TypeVar('TValue')
Any = TypeVar('Any')


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
    def __init__(
        self,
        ib_server_address: str,
        ib_server_port: int,
        ib_client_id: int,
        read_only: bool = False
    ):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.ib_client_id = ib_client_id
        self.read_only = read_only

        self.ib = IB()

        def mapper(tickers: Set[Ticker]) -> Observable[Ticker]:
            # warning: for whatever reason, we can get either a Set of tickers or a single ticker.
            # if this raises an exception, all hell brakes loose - we get multiple Tasks
            # that sit around pending, and all sorts...
            if isinstance(tickers, Set):
                return rx.from_iterable(tickers)
            elif isinstance(tickers, Ticker):
                return rx.of(tickers)
            else:
                raise ValueError(tickers)

        self.market_data_subject: Subject[Ticker] = Subject[Ticker]()
        self.historical_data_subject: Subject[pd.DataFrame] = Subject[pd.DataFrame]()
        self.bars_data_subject = EventSubject[RealTimeBarList](eventkit_event=self.ib.barUpdateEvent)
        self.positions_subject = EventSubject[List[Position]](eventkit_event=self.ib.positionEvent)
        self.portfolio_subject = EventSubject[PortfolioItem](eventkit_event=self.ib.updatePortfolioEvent)
        self.trades_subject = EventSubject[Trade](eventkit_event=self.ib.orderStatusEvent)
        self._contracts_source = EventSubject[Set[Ticker]](eventkit_event=self.ib.pendingTickersEvent)
        self.error_subject = Subject[IBAIORxError]()
        self.error_disposables: Dict[int, DisposableBase] = {}

        self.contracts_subject: Observable[Ticker] = self._contracts_source.pipe(
            ops.flat_map(mapper)
        )

        self.contracts_cache: Dict[Contract, Observable] = {}
        self.bars_cache: Dict[Contract, Observable[RealTimeBarList]] = {}
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
        self.error_subject.on_next(IBAIORxError(reqId, errorCode, errorString, contract))

    @backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=30)
    def connect(self):
        def __handle_client_id_error(msg):
            logging.error('clientId already in use: {}'.format(msg))
            raise ValueError('clientId')

        if self.ib.isConnected():
            return self

        self._shutdown = False

        if self.__handle_error not in self.ib.errorEvent:
            self.ib.errorEvent += self.__handle_error

        net_client = cast(Client, self.ib.client)
        net_client.conn.disconnected += __handle_client_id_error

        self.ib.connect(
            host=self.ib_server_address,
            port=self.ib_server_port,
            clientId=self.ib_client_id,
            timeout=10,
            readonly=self.read_only
        )

        net_client.conn.disconnected -= __handle_client_id_error
        self.history_worker = IBHistoryWorker(
            self.ib_server_address,
            self.ib_server_port,
            self.ib_client_id + 1,
        )

        return self

    async def shutdown(self):
        if self._shutdown:
            logging.debug('ibaiorx is already shutdown')
            return

        logging.debug('ibaiorx.shutdown(), disconnecting clients and disposing reactivex subscriptions')

        if self.history_worker:
            await self.history_worker.disconnect()

        self.market_data_subject.dispose()
        self.historical_data_subject.dispose()
        self.bars_data_subject.dispose()
        self.positions_subject.dispose()
        self.portfolio_subject.dispose()
        self.trades_subject.dispose()
        self._contracts_source.dispose()
        self.error_subject.dispose()

        for reqId, disposable in self.error_disposables.items():
            disposable.dispose()

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
        observer: ObserverBase[Trade]
    ) -> DisposableBase:
        # the order object gets filled with the order details (clientId, orderId etc)
        # the trade object returned from 'placeOrder' gets filled later, but we don't return it
        # as we want the subscription stream to contain all relevant trade details
        def filter_trade(trade: Trade):
            return self._filter_contract(contract, trade)

        xs = self.trades_subject.pipe(
            ops.filter(filter_trade)
        )

        disposable = xs.subscribe(observer)
        self.trades_subject.call_event_subscriber_sync(lambda: self.ib.placeOrder(contract, order))
        # todo, figure out what to do here with the disposable
        # should it cancel the order, or just stop listening?
        return disposable

    def subscribe_contract_direct(
        self,
        contract: Contract,
        one_time_snapshot: bool = False,
        delayed: bool = False,
    ) -> Observable[IBAIORxError]:
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
        self._contracts_source.call_event_subscriber_sync(
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

        def filter_reqid(error: IBAIORxError):
            return error.reqId == reqId

        err = self.error_subject.pipe(
            ops.filter(filter_reqid)
        )

        return err

    def __subscribe_contract(
        self,
        contract: Contract,
        one_time_snapshot: bool = False,
        delayed: bool = False,
    ) -> Observable[Ticker]:
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
        result = self._contracts_source.call_event_subscriber_sync(
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

        def filter_contract(ticker):
            return self._filter_contract(contract, ticker)

        contract_filter: Observable[Ticker] = self.contracts_subject.pipe(
            ops.filter(filter_contract)
        )

        xs: Subject = Subject()
        contract_filter.subscribe(xs)

        # error handling, which will listen to the error source, and pipe
        # any errors through to the subscriber
        def handle_error(error: IBAIORxError):
            logging.error('__subscribe_snapshot() had error: {}'.format(error))
            xs.on_error(Exception(error))

        def handle_exception(exception: Exception):
            logging.error('__subscribe_snapshot() threw {}'.format(exception))
            xs.on_error(exception)

        error_observer = AutoDetachObserver(on_next=handle_error, on_error=handle_exception)

        def filter_reqid(error: IBAIORxError):
            return error.reqId == reqId

        err = self.error_subject.pipe(
            ops.filter(filter_reqid)
        )

        self.error_disposables[reqId] = err.subscribe(error_observer)
        return xs

    def subscribe_contract(
        self,
        contract: Contract,
        one_time_snapshot: bool = False,
        delayed: bool = False,
    ) -> Observable[Ticker]:
        if contract not in self.contracts_cache:
            self.contracts_cache[contract] = self.__subscribe_contract(contract, one_time_snapshot, delayed)
        return self.contracts_cache[contract]

    def unsubscribe_contract(self, contract: Contract):
        raise ValueError('not implemented')

    # async def subscribe_barlist(
    #     self,
    #     contract: Contract,
    #     wts: WhatToShow = WhatToShow.MIDPOINT
    # ) -> Obser[RealTimeBarList]:
    #     # todo this method subscribes and populates a RealTimeBarsList object,
    #     # which I'm sure will end up being a memory leak
    #     bar_size = 5

    #     if contract in self.bars_cache:
    #         return self.bars_cache[contract]

    #     await self.bars_data_subject.call_event_subscriber_sync(
    #         lambda: self.ib.reqRealTimeBars(contract, bar_size, str(wts), False)
    #     )

    #     xs = pipe(
    #         self.bars_data_subject,
    #         rx.filter(lambda bar_data_list: self._filter_contract(contract, bar_data_list)),  # type: ignore
    #     )

    #     self.bars_cache[contract] = xs
    #     return xs

    # def unsubscribe_barlist(self, contract: Contract):
    #     if contract in self.bars_cache and self.bars_data_subject.value():
    #         self.ib.cancelRealTimeBars(cast(RealTimeBarList, self.bars_data_subject.value()))
    #         del self.bars_cache[contract]
    #     else:
    #         logging.debug('unsubscribe_barlist failed for {}'.format(contract))

    async def subscribe_positions(self, observer: Observer[List[Position]]) -> DisposableBase:
        disposable = self.positions_subject.subscribe(observer)
        await self.positions_subject.call_event_subscriber(self.ib.reqPositionsAsync())
        return disposable

    async def subscribe_portfolio(self, observer: Observer[PortfolioItem]) -> DisposableBase:
        disposable = self.portfolio_subject.subscribe(observer)

        portfolio_items = self.ib.portfolio()
        for item in portfolio_items:
            self.portfolio_subject.on_next(item)

        return disposable

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

        def on_next(ticker: Ticker):
            nonlocal populated_ticker
            populated_ticker = ticker
            _task.set()

        def on_error(ex: Exception):
            nonlocal thrown_exception
            thrown_exception = ex
            _task.set()

        def on_completed():
            logging.debug('get_snapshot() aclose')

        # we've got to subscribe, then wait for the first Ticker to arrive in the events
        # cachedeventsource, subscribers get the last value after calling subscribe
        observable: Observable[Ticker] = self.__subscribe_contract(
            contract=contract,
            one_time_snapshot=True,
            delayed=delayed,
        )

        xs: Observable[Ticker] = observable.pipe(
            ops.take(1)
        )

        observer = AutoDetachObserver[Ticker](on_next=on_next, on_error=on_error, on_completed=on_completed)
        subscription = xs.subscribe(observer)

        await _task.wait()
        observer.on_completed()
        subscription.dispose()
        await asyncio.sleep(0.1)
        if thrown_exception is not None:
            ex = cast(Exception, thrown_exception)
            raise ex
        return cast(Ticker, populated_ticker)

    def get_contract_details(self, contract: Contract) -> List[ContractDetails]:
        result = self.ib.reqContractDetails(contract)
        if not result:
            return []
        else:
            return cast(List[ContractDetails], result)

    async def get_matching_symbols(self, symbol: str) -> List[ContractDescription]:
        result = await self.ib.reqMatchingSymbolsAsync(symbol)
        if not result: return []
        else: return result

    async def __get_contract_description_helper(
        self,
        symbol: str,
        secType: str,
        primaryExchange: str,
        currency: str,
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

    # todo this sucks
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
        bar_size: BarSize = BarSize.Mins1,
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
        observer: Observer[pd.DataFrame],
        refresh_interval: int = 60,
    ) -> DisposableBase:
        async def __update(
            subject: Subject,
            contract: Contract,
            start_date: dt.datetime,
            end_date: dt.datetime
        ):
            if subject.is_disposed:
                return

            data = await self.get_contract_history(
                contract=contract,
                start_date=start_date,
                end_date=end_date,
                what_to_show=what_to_show,
            )

            print(data)
            subject.on_next(data)

            start_date = end_date
            end_date = end_date + dt.timedelta(minutes=1)
            loop = asyncio.get_event_loop()
            loop.call_later(
                refresh_interval,
                asyncio.create_task,
                __update(subject, contract, start_date, end_date)
            )

        subject = Subject[pd.DataFrame]()

        end_date = dt.datetime.now(dt.timezone.utc).astimezone(start_date.tzinfo)

        loop = asyncio.get_event_loop()
        loop.call_later(1, asyncio.create_task, __update(subject, contract, start_date, end_date))
        return subject.subscribe(observer)

    def sleep(self, seconds: float):
        self.ib.sleep(seconds)

    def run(self, *args):
        self.ib.run(*args)

    def client(self):
        return self.ib
