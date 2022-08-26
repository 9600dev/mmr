import exchange_calendars
import os
import sys


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from trader.common.logging_helper import setup_logging

import datetime as dt
import pandas as pd


logging = setup_logging(module_name='polygonqueuer')

from arctic.date import DateRange
from arctic.exceptions import NoDataFoundException
from ib_insync import Contract
from trader.batch.queuer import Queuer
from trader.common.helpers import dateify, day_iter, pdt
from trader.common.listener_helpers import Helpers
from trader.data.data_access import DictData, TickData
from trader.listeners.polygon_listener import PolygonListener
from typing import List


class PolygonQueuer(Queuer):
    def __init__(self,
                 arctic_server_address: str = '127.0.0.1',
                 arctic_library: str = 'HistoricalPolygon',
                 redis_server_address: str = '127.0.0.1',
                 redis_server_port: int = 6379):
        super().__init__(redis_queue='historypolygon',
                         redis_server_address=redis_server_address,
                         redis_server_port=redis_server_port)
        self.data = TickData(arctic_server_address, arctic_library)
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_library

    def queue_financials(self,
                         contracts: List[Contract]):
        for contract in contracts:
            logging.info('enqueing get_financials for {}'.format(contract))
            polygon_worker = PolygonWorker(
                arctic_server_address=self.arctic_server_address,
                arctic_library=self.arctic_library + 'Financials',
                redis_server_address=self.redis_server_address,
                redis_server_port=self.redis_server_port)

            self.enqueue(polygon_worker.do_work_get_financials, [contract])

    def queue_history(self,
                      contracts: List[Contract],
                      multiplier: int = 1,
                      timespan: str = 'minute',
                      start_date: dt.datetime = dateify(dt.datetime(2005, 1, 1), timezone='America/New_York'),
                      end_date: dt.datetime = dateify(dt.datetime.now() - dt.timedelta(days=1), timezone='America/New_York')):
        for contract in contracts:
            # find the missing dates between start_date and end_date, and queue them up
            exchange_calendar = exchange_calendars.get_calendar(contract.exchange)
            date_ranges: List[DateRange] = []

            # grabbing 'single day' from polygon is too slow, so we'll just grab
            # the last date that the symbol has data, and go from there, until
            # end_date.
            # if there is a NoDataFoundException it means that the conId doesn't exist
            try:
                _, max_date = self.data.date_summary(contract)
                # date_summary returns the min/max date of data inclusive
                # i.e. if the last chunk of data was 01/04/2020, max_date is 01/04/2020
                # we add an extra day here
                day_after_max_date = dateify(max_date) + dt.timedelta(days=1)

                if day_after_max_date < end_date:
                    adjusted_start_date = max(start_date, day_after_max_date)
                    date_ranges.append(DateRange(start=adjusted_start_date, end=end_date))
            except NoDataFoundException as ex:
                date_ranges.append(DateRange(start=start_date, end=end_date))

            for dr in date_ranges:
                if (
                    not self.is_job_queued(self.args_id([contract, dr.start, dr.end, multiplier, timespan]))
                ):
                    logging.info('enqueing between {} and {} for {}'.format(dr.start, dr.end, contract))
                    polygon_worker = PolygonWorker(
                        arctic_server_address=self.arctic_server_address,
                        arctic_library=self.arctic_library,
                        redis_server_address=self.redis_server_address,
                        redis_server_port=self.redis_server_port)

                    self.enqueue(polygon_worker.do_work, [contract, dr.start, dr.end, multiplier, timespan])

    def queue_history_csv(self,
                          contract_csv_file: str,
                          exclude_csv_file: str,
                          multiplier: int,
                          timespan: str,
                          start_date: dt.datetime):

        if not os.path.exists(contract_csv_file):
            raise ValueError('contract_csv_file does not exist')

        # here's where we look at our symbol data
        included = Helpers.contracts_from_df(pd.read_csv(contract_csv_file))
        excluded = []
        if exclude_csv_file and os.path.exists(exclude_csv_file):
            excluded = Helpers.contracts_from_df(pd.read_csv(exclude_csv_file))

        contracts = [s for s in included if s not in excluded]
        self.queue_history(contracts, multiplier, timespan, start_date)


class PolygonWorker():
    def __init__(self,
                 arctic_server_address: str,
                 arctic_library: str,
                 redis_server_address: str,
                 redis_server_port: int):
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_library
        self.redis_server_address = redis_server_address
        self.redis_server_port = redis_server_port
        self.data: TickData

    def do_work_get_financials(self, contract: Contract):
        if not contract.symbol:
            raise ValueError('contract must have symbol for polygon')

        setup_logging(module_name='polygon_worker', suppress_external_info=True)
        financials_data: DictData = DictData(self.arctic_server_address, self.arctic_library)
        logging.info('do_work_get_financials: {}'.format(contract))
        poly = PolygonListener()
        try:
            result = poly.get_financials(contract.symbol)
            if result and len(result.financials) > 0:
                # delete any data in there already
                financials_data.delete(contract)
                # write the data
                financials_data.write(contract, result)
                logging.info('writing polygon financials for {}'.format(contract))
            else:
                logging.info('no data for polygon financials {}'.format(contract))
            return True
        except Exception as ex:
            logging.error('do_work_get_financials error {}'.format(ex))
            return False

    def do_work(self,
                contract: Contract,
                start_date: dt.datetime,
                end_date: dt.datetime,
                multiplier: int,
                timespan: str):
        def update_metadata(contract, start_date: dt.datetime, end_date: dt.datetime):
            metadata = self.data.read_metadata(contract)
            for date_time in day_iter(start_date, end_date):
                metadata.add_no_data(date_time)
            self.data.write_metadata(contract, metadata)
        if not contract.symbol:
            raise ValueError('contract must have symbol for polygon')

        setup_logging(module_name='polygon_worker', suppress_external_info=True)
        self.data = TickData(self.arctic_server_address, self.arctic_library)

        logging.info('do_work: {} {} {} {} {}'.format(contract,
                                                      pdt(start_date),
                                                      pdt(end_date),
                                                      str(multiplier),
                                                      timespan))
        poly = PolygonListener()
        result = poly.get_aggregates_as_ib(contract.symbol, multiplier, timespan, start_date, end_date)
        if len(result) > 0:
            logging.info('writing polygon history   {} on date {} to {} {} {}'.format(contract.symbol,
                                                                                      pdt(start_date),
                                                                                      pdt(end_date),
                                                                                      multiplier,
                                                                                      timespan))
            self.data.write(contract, result)  # type: ignore
            return True
        else:
            logging.warning('zero results for polygon do_work {} {} {} {} {}'.format(contract.symbol,
                                                                                     pdt(start_date),
                                                                                     pdt(end_date),
                                                                                     multiplier,
                                                                                     timespan))
            update_metadata(contract, start_date, end_date)
            return True
