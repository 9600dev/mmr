from re import I
import datetime as dt
import ib_insync
import pandas as pd
import backoff

from redis import Redis
from rq import Queue
from arctic.exceptions import OverlappingDataException
from ib_insync import Stock, IB, Index, Contract, Ticker, BarDataList
from dateutil.tz import tzlocal, gettz
from typing import Tuple, List, Optional, cast
from functools import reduce

from trader.data.data_access import TickData
from trader.data.contract_metadata import ContractMetadata
from trader.common.logging_helper import setup_logging
from trader.common.helpers import dateify, day_iter, pdt
from trader.common.listener_helpers import Helpers
from trader.listeners.ibaiorx import IBAIORx, WhatToShow

logging = setup_logging(module_name='ibhistoryworker')

class IBHistoryWorker():
    def __init__(self, ib_client: IB):
        self.ib_client = ib_client

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

    # @backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=240)
    async def get_contract_history(
        self,
        contract: Contract,
        what_to_show: WhatToShow,
        bar_size: str,
        start_date: dt.datetime,
        end_date: dt.datetime,
        filter_between_dates: bool = True,
        tz_info: str = 'America/New_York'
    ) -> pd.DataFrame:

        global has_error
        error_code = 0

        if self.__handle_error not in self.ib_client.errorEvent:
            self.ib_client.errorEvent += self.__handle_error

        if not self.ib_client.isConnected():
            raise ConnectionError()

        # 16 hours, 4am to 8pm
        # duration_step_size = '57600 S'
        # 24 hours
        duration_step_size = '86400 S'

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
        local_tz = dt.datetime.now(dt.timezone.utc).astimezone().tzinfo

        logging.info('get_contract_history {} {} {} {}'.format(contract.conId, str(what_to_show), pdt(start_date), pdt(end_date)))

        bars: List[pd.DataFrame] = []

        while current_date >= start_date:
            result = await self.ib_client.reqHistoricalDataAsync(
                contract,
                endDateTime=current_date,
                durationStr=duration_step_size,
                barSizeSetting=bar_size,
                whatToShow=str(what_to_show),
                useRTH=False,
                formatDate=1,
                keepUpToDate=False,
            )

            # skip if 'no data' returned
            if error_code > 0 and error_code != 162:
                raise Exception('error_code: {}'.format(error_code))

            df_result = ib_insync.util.df(result).set_index('date')
            df_result['bar_size'] = bar_size
            df_result.rename({'barCount': 'bar_count'}, inplace=True)

            # arctic requires timezone to be set
            df_result.index = pd.to_datetime(df_result.index)  # type: ignore
            df_result.index = df_result.index.tz_localize(local_tz)  # type: ignore
            df_result.index = df_result.index.tz_convert(tz_info)
            df_result.sort_index(ascending=True, inplace=True)

            # add to the bars list
            bars.append(df_result)
            pd_date = pd.to_datetime(df_result.index[0])
            current_date = dt.datetime(pd_date.year, pd_date.month, pd_date.day, pd_date.hour, pd_date.minute, pd_date.second, tzinfo=gettz(tz_info))

        all_data: pd.DataFrame = pd.concat(bars)
        if filter_between_dates:
            all_data = all_data[(all_data.index >= start_date.replace(tzinfo=gettz(tz_info)))  # type: ignore
                                & (all_data.index <= end_date_offset.replace(tzinfo=gettz(tz_info)))]  # type: ignore
        return all_data.sort_index(ascending=True)
