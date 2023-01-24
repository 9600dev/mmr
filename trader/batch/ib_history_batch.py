import os
import sys
import warnings


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

warnings.simplefilter(action='ignore', category=FutureWarning)
from arctic.date import DateRange
from trader.batch.queuer import Queuer
from trader.common.helpers import dateify, pdt, timezoneify
from trader.common.logging_helper import setup_logging
from trader.data.data_access import SecurityDefinition, TickData, TickStorage
from trader.data.universe import Universe
from trader.listeners.ib_history_worker import IBHistoryWorker
from trader.objects import BarSize, WhatToShow
from typing import List, Optional

import asyncio
import datetime as dt
import exchange_calendars


logging = setup_logging(module_name='ib_history')

class IBHistoryQueuer(Queuer):
    def __init__(
        self,
        ib_server_address: str,
        ib_server_port: int,
        ib_client_id: int,
        arctic_server_address: str,
        bar_size: BarSize,
        redis_server_address: str,
        redis_server_port: int
    ):
        super().__init__(redis_queue='history',
                         redis_server_address=redis_server_address,
                         redis_server_port=redis_server_port)
        self.data = TickStorage(arctic_server_address).get_tickdata(bar_size=bar_size)
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.ib_client_id = ib_client_id
        self.arctic_server_address = arctic_server_address
        self.bar_size = bar_size

    def __try_get_exchange_calendar(self, security: SecurityDefinition):
        exchange_calendar = None
        try:
            exchange_calendar = exchange_calendars.get_calendar(security.primaryExchange)
        except exchange_calendars.exchange_calendar.errors.InvalidCalendarName:
            try:
                exchange_calendar = exchange_calendars.get_calendar(security.exchange)
            except Exception as ex:
                return None
        return exchange_calendar

    def queue_history(
        self,
        security_definitions: List[SecurityDefinition],
        start_date: dt.datetime = dateify(dt.datetime.now() - dt.timedelta(days=5), make_sod=True),
        end_date: dt.datetime = dateify(
            dt.datetime.now() - dt.timedelta(days=1),
            make_eod=True
        ),
        exchange_calendar: Optional[exchange_calendars.ExchangeCalendar] = None,
    ) -> int:
        jobs = 0
        for security in security_definitions:
            # timezonify the start date based on the securities trading timezone
            start_date = timezoneify(start_date, timezone=security.timeZoneId)
            end_date = timezoneify(end_date, timezone=security.timeZoneId)

            # find the missing dates between start_date and end_date, and queue them up
            exchange_calendar = self.__try_get_exchange_calendar(security)
            if not exchange_calendar:
                logging.error('could not find correct exchange calendar for security {}, try passing one'.format(security))
                continue

            date_ranges = self.data.missing(security,
                                            exchange_calendar,
                                            date_range=DateRange(start=start_date, end=end_date))

            if date_ranges:
                logging.debug('missing historical data for {} in date_ranges {}:'.format(security.symbol, date_ranges))

            for date_dr in date_ranges:
                if (
                    not self.is_job_queued(self.args_id([security, date_dr.start, date_dr.end, self.bar_size]))
                ):
                    logging.info('enqueing {} from {} to {}'.format(Universe.to_contract(security),
                                                                    pdt(date_dr.start), pdt(date_dr.end)))

                    history_worker = BatchIBHistoryWorker(
                        ib_server_address=self.ib_server_address,
                        ib_server_port=self.ib_server_port,
                        ib_client_id=self.ib_client_id,
                        arctic_server_address=self.arctic_server_address,
                        redis_server_address=self.redis_server_address,
                        redis_server_port=self.redis_server_port
                    )

                    job = self.enqueue(
                        history_worker.do_work,
                        [
                            security,
                            dateify(date_dr.start, timezone=security.timeZoneId, make_sod=True),
                            dateify(date_dr.end, timezone=security.timeZoneId, make_eod=True),
                            self.bar_size
                        ]
                    )
                    jobs += 1
                    logging.debug('Job history_worker.do_work enqueued, is_queued: {} using cliend_id {}'
                                  .format(job.is_queued, self.ib_client_id))
        return jobs


class BatchIBHistoryWorker():
    def __init__(self,
                 ib_server_address: str,
                 ib_server_port: int,
                 ib_client_id: int,
                 arctic_server_address: str,
                 redis_server_address: str,
                 redis_server_port: int):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.ib_client_id = ib_client_id
        self.arctic_server_address = arctic_server_address
        self.redis_server_address = redis_server_address
        self.redis_server_port = redis_server_port
        self.storage: TickStorage
        self.data: TickData

    def do_work(self, security: SecurityDefinition, start_date: dt.datetime, end_date: dt.datetime, bar_size: BarSize) -> bool:
        setup_logging(module_name='ib_history', suppress_external_info=True)

        with (IBHistoryWorker(
            self.ib_server_address,
            self.ib_server_port,
            self.ib_client_id
        )) as ib_history_worker:
            self.data = TickStorage(self.arctic_server_address).get_tickdata(bar_size)

            logging.info('do_work: {} {} {} {}'.format(security.symbol, pdt(start_date), pdt(end_date), bar_size))
            # result = self.ib_history.get_and_populate_stock_history(cast(Stock, contract), bar_size, start_date, end_date)
            result = asyncio.run(ib_history_worker.get_contract_history(
                security=Universe.to_contract(security),
                what_to_show=WhatToShow.TRADES,
                start_date=start_date,
                end_date=end_date,
                bar_size=bar_size,
                filter_between_dates=True
            ))
            logging.debug('ib_history.get_contract_history for {} returned {} rows, writing to library "{}"'.format(
                security.symbol,
                len(result),
                str(bar_size)
            ))

            if len(result) > 0:
                self.data.write(security, result)

            return True
