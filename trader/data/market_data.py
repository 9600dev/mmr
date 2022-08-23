import os
import numpy as np
import pandas as pd
import datetime as dt
import exchange_calendars
from dataclasses import dataclass, fields
from dateutil.tz import tzlocal, gettz
from dateutil.tz.tz import tzfile

from trader.common.logging_helper import setup_logging

logging = setup_logging(module_name='data')

from pandas.core.base import PandasObject
from arctic import Arctic, TICK_STORE, VERSION_STORE
from arctic.date import DateRange, string_to_daterange
from arctic.tickstore.tickstore import TickStore
from arctic.store.version_store import VersionStore
from arctic.exceptions import NoDataFoundException
from aioreactive.subject import AsyncMultiSubject
from aioreactive import AsyncObserver
from expression.system.disposable import Disposable, AsyncDisposable
from typing import Tuple, List, Optional, Dict, TypeVar, Generic, Type, Union, cast, Set
from durations import Duration
from exchange_calendars import ExchangeCalendar
from pandas import DatetimeIndex
from ib_insync.contract import Contract, ContractDetails
from ib_insync.objects import BarData, RealTimeBar
from trader.common.helpers import dateify, daily_close, daily_open, market_hours, get_contract_from_csv, symbol_to_contract
from trader.data.data_access import Data, SecurityDefinition, TickData, DictData
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ibaiorx import IBAIORx
from trader.listeners.ib_history_worker import IBHistoryWorker, WhatToShow
from ib_insync.ib import IB


class SecurityDataStream(AsyncMultiSubject[pd.DataFrame]):
    def __init__(
            self,
            security: SecurityDefinition,
            bar_size: str,
            date_range: DateRange,
            existing_data: Optional[pd.DataFrame] = None):
        super().__init__()
        self.security = security
        self.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'average', 'bar_count', 'bar_size']
        self.date_range: DateRange
        self.df: pd.DataFrame = pd.DataFrame([], columns=self.columns)
        self.is_being_backfilled: bool = True
        if existing_data is not None:
            self.df = existing_data
        self.bar_size = bar_size

    async def asend(self, value: pd.DataFrame) -> None:
        self.check_disposed()

        if self._is_stopped:
            return

        # todo: gotta be a faster way here
        self.df = self.df.append(value)

        for obv in list(self._observers):
            await obv.asend(self.df)

    async def subscribe_async(self, observer: AsyncObserver[pd.DataFrame]) -> AsyncDisposable:
        self.check_disposed()

        self._observers.append(observer)

        async def dispose() -> None:
            if observer in self._observers:
                self._observers.remove(observer)

        result = AsyncDisposable.create(dispose)

        # send the last cached result
        await observer.asend(self.df)
        return result

    def data(self) -> pd.DataFrame:
        return self.df


class MarketData():
    def __init__(
        self,
        client: IBAIORx,
        arctic_server_address: str,
        arctic_universe_library: str,
    ):
        self.client: IBAIORx = client
        self.arctic_server_address = arctic_server_address
        self.arctic_universe_library = arctic_universe_library
        self.universe = UniverseAccessor(self.arctic_server_address, self.arctic_universe_library)
        self.data = TickData(self.arctic_server_address, 'bardata')

    async def subscribe_security(
        self,
        security: SecurityDefinition,
        bar_size: str,
        start_date: dt.datetime,
        back_fill: bool,
    ) -> SecurityDataStream:
        # if we backfill, this essentially awaits until back_fill is complete
        if not start_date.tzinfo:
            raise ValueError('start_date must specify a timezone (start_date.tzinfo)')

        end_date = dt.datetime.now().astimezone(start_date.tzinfo)
        date_range = DateRange(start=start_date, end=end_date)

        if back_fill:
            # grab the existing data we have
            history_worker = IBHistoryWorker(self.client.ib)
            # todo we need to use the ib_history batch infrastructure here and have it
            # notify via events, but for now we'll just grab the data ourselves.
            calendar = exchange_calendars.get_calendar(security.primaryExchange)
            date_ranges = self.data.missing(
                contract=security,
                exchange_calendar=calendar,
                date_range=date_range
            )
            for date_dr in date_ranges:
                result = await history_worker.get_contract_history(
                    security=security,
                    what_to_show=WhatToShow.TRADES,
                    bar_size=bar_size,
                    start_date=start_date,
                    end_date=end_date,
                    filter_between_dates=True,
                    tz_info='America/New_York'
                )
                logging.info('writing backfill data for {} {}'.format(security, date_dr))
                self.data.write_resolve_overlap(security, data_frame=result)

        df = self.data.get_data(security, date_range=date_range)
        stream = SecurityDataStream(security=security, bar_size=bar_size, date_range=date_range, existing_data=df)
        disposable = await self.client.subscribe_contract_history(
            contract=Universe.to_contract(security),
            start_date=df.index[-1].to_pydatetime(),  # type: ignore
            what_to_show=WhatToShow.TRADES,
            observer=stream
        )

        return stream
