from arctic.date import DateRange
from reactivex import Observer
from reactivex.disposable import Disposable
from reactivex.subject import Subject
from trader.common.logging_helper import setup_logging
from trader.data.data_access import SecurityDefinition, TickData
from trader.data.universe import Universe, UniverseAccessor
from trader.listeners.ib_history_worker import IBHistoryWorker, WhatToShow
from trader.listeners.ibreactive import IBAIORx
from typing import Optional

import datetime as dt
import exchange_calendars
import pandas as pd


logging = setup_logging(module_name='data')

class SecurityDataStream(Subject[pd.DataFrame]):
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

    def on_next(self, value: pd.DataFrame) -> None:
        self.check_disposed()

        if self.is_stopped:
            return

        # todo: gotta be a faster way here
        self.df = self.df.append(value)

        for obv in list(self.observers):
            obv.on_next(self.df)

    async def subscribe_async(self, observer: Observer[pd.DataFrame]) -> Disposable:
        self.check_disposed()

        self.observers.append(observer)

        def dispose() -> None:
            if observer in self.observers:
                self.observers.remove(observer)

        result = Disposable(dispose)

        # send the last cached result
        observer.on_next(self.df)
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
