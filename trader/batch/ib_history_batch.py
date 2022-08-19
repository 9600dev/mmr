import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import datetime as dt
import ib_insync as ibapi
import pandas as pd
import random
import warnings
import asyncio

from redis import Redis
from rq import Queue
from rq.job import Job
from ib_insync.ib import IB
from dateutil.tz import tzlocal
from typing import Tuple, List, Optional, cast
import exchange_calendars
from exchange_calendars import ExchangeCalendar

warnings.simplefilter(action='ignore', category=FutureWarning)
from arctic.exceptions import OverlappingDataException
from arctic.date import DateRange

from trader.data.data_access import TickData, DictData, SecurityDefinition
from trader.data.contract_metadata import ContractMetadata
from trader.data.universe import Universe
from trader.common.logging_helper import setup_logging
from trader.common.helpers import date_range, dateify, day_iter, get_exchange_calendar, pdt
from trader.common.listener_helpers import Helpers
from trader.listeners.ib_history_worker import IBHistoryWorker
from trader.batch.queuer import Queuer
from trader.container import Container
from trader.objects import WhatToShow

logging = setup_logging(module_name='ib_history_batch')

class IBHistoryQueuer(Queuer):
    def __init__(
        self,
        ib_server_address: str,
        ib_server_port: int,
        arctic_server_address: str,
        arctic_library: str,
        redis_server_address: str,
        redis_server_port: int
    ):
        super().__init__(redis_queue='history',
                         redis_server_address=redis_server_address,
                         redis_server_port=redis_server_port)
        self.data = TickData(arctic_server_address, arctic_library)
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_library

    def queue_history(self,
                      security_definitions: List[SecurityDefinition],
                      bar_size: str = '1 min',
                      start_date: dt.datetime = dateify(dt.datetime.now() - dt.timedelta(days=5), timezone='America/New_York'),
                      end_date: dt.datetime = dateify(
                          dt.datetime.now() - dt.timedelta(days=1),
                          timezone='America/New_York',
                          make_eod=True
                      )):
        for security in security_definitions:
            # find the missing dates between start_date and end_date, and queue them up
            exchange_calendar = exchange_calendars.get_calendar(security.primaryExchange)
            date_ranges = self.data.missing(security,
                                            exchange_calendar,
                                            date_range=DateRange(start=start_date, end=end_date))

            logging.debug('missing date_ranges for {}: {}'.format(security.symbol, date_ranges))

            for date_dr in date_ranges:
                if (
                    not self.is_job_queued(self.args_id([security, date_dr.start, date_dr.end, bar_size]))
                ):
                    logging.info('enqueing {} from {} to {}'.format(Universe.to_contract(security),
                                                                    pdt(date_dr.start), pdt(date_dr.end)))

                    client_id = random.randint(50, 100)
                    history_worker = BatchIBHistoryWorker(
                        ib_server_address=self.ib_server_address,
                        ib_server_port=self.ib_server_port,
                        ib_client_id=client_id,
                        arctic_server_address=self.arctic_server_address,
                        arctic_library=self.arctic_library,
                        redis_server_address=self.redis_server_address,
                        redis_server_port=self.redis_server_port)

                    job = self.enqueue(history_worker.do_work, [security, dateify(date_dr.start), dateify(date_dr.end), bar_size])
                    logging.debug('Job history_worker.do_work enqueued, is_queued: {} using cliend_id {}'
                                  .format(job.is_queued, client_id))


class BatchIBHistoryWorker():
    def __init__(self,
                 ib_server_address: str,
                 ib_server_port: int,
                 ib_client_id: int,
                 arctic_server_address: str,
                 arctic_library: str,
                 redis_server_address: str,
                 redis_server_port: int):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.ib_client_id = ib_client_id
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_library
        self.redis_server_address = redis_server_address
        self.redis_server_port = redis_server_port
        self.data: TickData

    def do_work(self, security: SecurityDefinition, start_date: dt.datetime, end_date: dt.datetime, bar_size: str) -> bool:
        setup_logging(module_name='batch_ib_history_worker', suppress_external_info=True)

        ib = cast(IB, Container().resolve(IB))

        if not ib.isConnected():
            ib.connect(host=self.ib_server_address, port=self.ib_server_port, clientId=self.ib_client_id)

        self.ib_history = IBHistoryWorker(ib)
        self.data = TickData(self.arctic_server_address, self.arctic_library)

        logging.info('do_work: {} {} {} {}'.format(security.symbol, pdt(start_date), pdt(end_date), bar_size))
        # result = self.ib_history.get_and_populate_stock_history(cast(Stock, contract), bar_size, start_date, end_date)
        result = asyncio.run(self.ib_history.get_contract_history(
            security=Universe.to_contract(security),
            what_to_show=WhatToShow.TRADES,
            start_date=start_date,
            end_date=end_date,
            bar_size=bar_size,
            filter_between_dates=True
        ))

        self.data.write(security, result)
        return True
