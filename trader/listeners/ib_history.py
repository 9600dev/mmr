from re import I
import datetime as dt
import ib_insync as ibapi
import pandas as pd
import backoff

from redis import Redis
from rq import Queue
from arctic.exceptions import OverlappingDataException
from ib_insync import Stock, IB, Index, Contract, Ticker, BarDataList
from dateutil.tz import tzlocal, gettz
from typing import Tuple, List, Optional, cast
from functools import reduce

from trader.common.data import TickData, ContractMetadata
from trader.common.logging_helper import setup_logging
from trader.common.helpers import dateify, day_iter, pdt
from trader.common.listener_helpers import Helpers

logging = setup_logging(module_name='ib_history')

class IBHistoryWorker():
    def __init__(self,
                 ib_server_address: str = '127.0.0.1',
                 ib_server_port: int = 7496,
                 ib_client_id: int = 5):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.ib_client_id = ib_client_id
        self.ib_client: IB = IB()

    error_code = 0

    def __rename_columns(self, df: pd.DataFrame, append: str) -> pd.DataFrame:
        return df.rename(columns={
            'open': 'open_' + append,
            'high': 'high_' + append,
            'low': 'low_' + append,
            'close': 'close_' + append,
            'volume': 'volume_' + append,
            'average': 'average_' + append,
            'barCount': 'barCount_' + append
        })

    def __handle_error(self, reqId, errorCode, errorString, contract):
        global error_code

        # ignore the following:
        # ib error reqId: -1 errorCode 2104 errorString Market data farm connection is OK:usfarm.nj contract None
        if errorCode == 2104 or errorCode == 2158 or errorCode == 2106:
            return
        logging.warning('ib error reqId: {} errorCode {} errorString {} contract {}'.format(reqId,
                                                                                            errorCode,
                                                                                            errorString,
                                                                                            contract))

    @backoff.on_exception(backoff.expo, Exception, max_tries=8, max_time=240)
    def connect_try(self):
        # we might have several history clients connecting, try and avoid
        # collisions
        self.ib_client.connect(self.ib_server_address, self.ib_server_port, clientId=self.ib_client_id)

    @backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=240)
    def get_contract_history(self,
                             contract: Contract,
                             what_to_show: List[str],
                             bar_size: str,
                             start_date: dt.datetime,
                             end_date: dt.datetime,
                             filter_between_dates: bool = True,
                             tz_info: str = 'America/New_York') -> pd.DataFrame:

        global has_error
        error_code = 0

        if self.__handle_error not in self.ib_client.errorEvent:
            self.ib_client.errorEvent += self.__handle_error

        if not self.ib_client.isConnected():
            self.connect_try()

        # 16 hours, 4am to 8pm
        duration_step_size = '57600 S'

        if bar_size == '1 day':
            duration_step_size = '10 Y'
        if bar_size == '1 hour':
            duration_step_size = '4 Y'
        if bar_size == '2 hours':
            duration_step_size = '1 Y'

        # we say that the 'end date' is the start of the day after
        start_date = dateify(start_date, timezone=tz_info)
        end_date_offset = dateify(end_date, timezone=tz_info) + dt.timedelta(days=1)
        current_date = end_date_offset

        logging.info('get_contract_history {} {} {} {}'.format(contract.conId, what_to_show, pdt(start_date), pdt(end_date)))
        if not self.ib_client.isConnected():
            raise ConnectionError()

        bars: List[pd.DataFrame] = []

        while current_date >= start_date:
            current_date_bars: List[pd.DataFrame] = []
            for show in what_to_show:
                result = self.ib_client.reqHistoricalData(
                    contract,
                    endDateTime=current_date,
                    durationStr=duration_step_size,
                    barSizeSetting=bar_size,
                    whatToShow=show,
                    useRTH=False,
                    formatDate=1)

                # skip if 'no data' returned
                if error_code > 0 and error_code != 162:
                    raise Exception('error_code: {}'.format(error_code))

                current_date_bars.append(self.__rename_columns(ibapi.util.df(result).set_index('date'), show.lower()))

            joined: pd.DataFrame = reduce(lambda left, right: left.join(right), current_date_bars)

            # arctic requires timezone to be set
            joined.index = pd.to_datetime(joined.index)
            joined.index = joined.index.tz_localize(tz_info)  # type: ignore

            # get rid of any columns that has -1 or -1.0
            joined = joined.loc[:, (joined >= 0).any()]  # type: ignore
            # add to the bars list
            bars.append(joined)
            current_date = dateify(joined.index[-1])

        all_data: pd.DataFrame = pd.concat(bars)
        if filter_between_dates:
            all_data = all_data[(all_data.index >= start_date.replace(tzinfo=gettz(tz_info)))  # type: ignore
                                & (all_data.index <= end_date_offset.replace(tzinfo=gettz(tz_info)))]  # type: ignore
        return all_data.sort_index(ascending=True)


class IBHistory():
    def __init__(self,
                 ib_server_address: str = '127.0.0.1',
                 ib_server_port: int = 7496,
                 ib_client_id: int = 5,
                 arctic_server_address: str = '127.0.0.1',
                 arctic_library: str = 'Historical',
                 redis_server_address: str = '127.0.0.1',
                 redis_server_port: int = 6379):
        self.ib_history_worker = IBHistoryWorker(ib_server_address, ib_server_port, ib_client_id)
        self.data: TickData = TickData(arctic_server_address, arctic_library)
        # self.redis_conn = Redis(host=redis_server_address, port=redis_server_port)
        # self.queue = Queue(connection=self.redis_conn)
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_library

    def get_stock_history(self, contract: Stock, bar_size: str, start_date: dt.datetime, end_date: dt.datetime):
        cleaned_contract = Helpers.clean_contract_object(contract)
        return self.ib_history_worker.get_contract_history(cleaned_contract,
                                                           ['TRADES', 'BID', 'ASK'],
                                                           bar_size,
                                                           start_date,
                                                           end_date)

    def get_and_populate_stock_history(self,
                                       contract: Stock,
                                       bar_size: str,
                                       start_date: dt.datetime,
                                       end_date: dt.datetime):
        def update_metadata(contract, start_date: dt.datetime, end_date: dt.datetime):
            metadata = self.data.read_metadata(contract)
            for date_time in day_iter(start_date, end_date):
                metadata.add_no_data(date_time)
            self.data.write_metadata(contract, metadata)

        data_frame = self.get_stock_history(contract, bar_size, start_date, end_date)
        if len(data_frame) > 0:  # type: ignore
            try:
                logging.info('writing stock history for {} on date {} to {} bar_size {}'.format(contract.conId,
                                                                                                pdt(start_date),
                                                                                                pdt(end_date),
                                                                                                bar_size))
                self.data.write(contract, data_frame)  # type: ignore
                return True
            except OverlappingDataException as ex:
                logging.warning('OverlappingDataException populate_and_get_symbol_history {} {} {} {} {}'.format(contract.conId,
                                                                                                                 pdt(start_date),
                                                                                                                 pdt(end_date),
                                                                                                                 bar_size,
                                                                                                                 ex))
                update_metadata(contract, start_date, end_date)
                return True

        else:
            logging.warning('zero results for get_and_populate_stock_history {} {} {}'.format(contract.conId,
                                                                                              pdt(start_date),
                                                                                              pdt(end_date)))
            update_metadata(contract, start_date, end_date)
            return True
