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

from redis import Redis
from rq import Queue
from rq.job import Job
from ib_insync import Stock, IB, Index, Contract, Ticker, BarDataList
from dateutil.tz import tzlocal
from typing import Tuple, List, Optional, cast
import exchange_calendars
from exchange_calendars import ExchangeCalendar

warnings.simplefilter(action='ignore', category=FutureWarning)
from arctic.exceptions import OverlappingDataException
from arctic.date import DateRange

from trader.common.data import TickData, DictData, ContractMetadata
from trader.common.logging_helper import setup_logging
from trader.common.helpers import date_range, dateify, day_iter, get_exchange_calendar, pdt
from trader.common.listener_helpers import Helpers
from trader.listeners.ib_history import IBHistory
from trader.batch.queuer import Queuer

logging = setup_logging(module_name='ib_history_batch')

class IBHistoryQueuer(Queuer):
    def __init__(self,
                 ib_server_address: str = '127.0.0.1',
                 ib_server_port: int = 7496,
                 ib_client_id: int = 5,
                 arctic_server_address: str = '127.0.0.1',
                 arctic_library: str = 'Historical',
                 redis_server_address: str = '127.0.0.1',
                 redis_server_port: int = 6379):
        super().__init__(redis_queue='history',
                         redis_server_address=redis_server_address,
                         redis_server_port=redis_server_port)
        self.data = TickData(arctic_server_address, arctic_library)
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.ib_client_id = ib_client_id
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_library

    def queue_history(self,
                      contracts: List[Contract],
                      bar_size: str = '1 min',
                      start_date: dt.datetime = dateify(dt.datetime(2021, 1, 1), timezone='America/New_York'),
                      end_date: dt.datetime = dateify(dt.datetime.now() - dt.timedelta(days=1), timezone='America/New_York')):
        for contract in contracts:
            # find the missing dates between start_date and end_date, and queue them up
            exchange_calendar = exchange_calendars.get_calendar(contract.exchange)
            date_ranges = self.data.missing(contract,
                                            exchange_calendar,
                                            date_range=DateRange(start=start_date, end=end_date))

            for date_dr in date_ranges:
                for date_time in date_range(date_dr.start, date_dr.end):
                    if (
                        not self.is_job_queued(self.args_id([contract, date_time, date_time, bar_size]))
                    ):
                        logging.info('enqueing {} at {}'.format(contract, pdt(date_time)))
                        history_worker = IBHistoryWorker(
                            ib_server_address=self.ib_server_address,
                            ib_server_port=self.ib_server_port,
                            ib_client_id=self.ib_client_id + random.randint(0, 50),
                            arctic_server_address=self.arctic_server_address,
                            arctic_library=self.arctic_library,
                            redis_server_address=self.redis_server_address,
                            redis_server_port=self.redis_server_port)

                        self.enqueue(history_worker.do_work, [contract, date_time, date_time, bar_size])

class IBHistoryWorker():
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

    def do_work(self, contract: Contract, start_date: dt.datetime, end_date: dt.datetime, bar_size: str):
        setup_logging(module_name='batch_ib_history_worker', suppress_external_info=True)

        self.ib_history = IBHistory(self.ib_server_address,
                                    self.ib_server_port,
                                    self.ib_client_id,
                                    self.arctic_server_address,
                                    self.arctic_library,
                                    self.redis_server_address,
                                    self.redis_server_port)
        logging.info('do_work: {} {} {} {}'.format(contract.symbol, pdt(start_date), pdt(end_date), bar_size))
        result = self.ib_history.get_and_populate_stock_history(cast(Stock, contract), bar_size, start_date, end_date)
        return result
