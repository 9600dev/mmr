from dateutil.tz import gettz
from ib_insync import IB
from ib_insync.contract import Contract
from trader.common.helpers import dateify, timezoneify, utcify_str
from trader.common.logging_helper import setup_logging
from trader.data.data_access import SecurityDefinition
from trader.data.universe import Universe
from trader.objects import BarSize, WhatToShow
from typing import List, Optional, Union

import asyncio
import backoff
import datetime as dt
import ib_insync
import numpy as np
import pandas as pd
import pytz


logging = setup_logging(module_name='ib_history')

class IBHistoryWorker():
    def __init__(
        self,
        ib_server_address: str,
        ib_server_port: int,
        ib_client_id: int,
    ):
        # self.ib_client = ib_client
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.ib_client_id = ib_client_id
        self.ib_client: IB = IB()

        self.error_code: int = 0
        self.error_string: str = ''
        self.error_contract: Optional[Contract] = None
        self.connected: bool = False

    def __enter__(self):
        return self.connect()

    def __exit__(self, *args):
        self.shutdown()

    def __clear_error(self):
        self.error_code = 0
        self.error_string = ''
        self.error_contract = None

    def __handle_error(self, reqId, errorCode, errorString, contract):
        self.error_code = errorCode
        self.error_string = errorString
        self.error_contract = contract

        # ignore the following:
        # ib error reqId: -1 errorCode 2104 errorString Market data farm connection is OK:usfarm.nj contract None
        if errorCode == 2104 or errorCode == 2158 or errorCode == 2106:
            self.error_code = 0
            self.error_string = ''
            self.error_contract = None
            return
        logging.error('ib error reqId: {} errorCode {} errorString {} contract {}'.format(reqId,
                                                                                          errorCode,
                                                                                          errorString,
                                                                                          contract))

    @backoff.on_exception(backoff.expo, exception=ValueError, max_tries=3)
    def connect(self):
        if self.connected:
            return self

        def __handle_client_id_error(msg):
            logging.error('ib_client_id already in use, randomizing and trying again')
            self.ib_client_id = np.random.randint(self.ib_client_id, self.ib_client_id + 20)
            raise ValueError('ib_client_id in use')

        # todo set in the readme that the master client ID has to be set to 5
        if self.__handle_error not in self.ib_client.errorEvent:
            self.ib_client.errorEvent += self.__handle_error

        self.ib_client.connect(
            host=self.ib_server_address,
            port=self.ib_server_port,
            clientId=self.ib_client_id,
            timeout=15,
            readonly=True
        )

        self.error_code = 0
        self.error_string = ''
        self.error_contract = None
        self.ib_client.client.conn.disconnected -= __handle_client_id_error
        self.connected = True
        return self

    def shutdown(self):
        self.ib_client.disconnect()
        self.error_code = 0
        self.error_string = ''
        self.error_contract = None
        self.connected = False

    # @backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=240)
    async def get_contract_history(
        self,
        security: Union[Contract, SecurityDefinition],
        what_to_show: WhatToShow,
        bar_size: BarSize,
        start_date: dt.datetime,
        end_date: dt.datetime,
        filter_between_dates: bool = True,
        tz_info: str = 'US/Eastern'
    ) -> pd.DataFrame:

        if not start_date.tzinfo:
            logging.debug('get_contract_history() start_date had no timezone, applying tz_info')
            start_date.astimezone(pytz.timezone(tz_info))

        # todo doing this with 'asx' based stocks gives us a dataframe with the incorrect timezone
        # figure this out
        self.__clear_error()
        contract = Universe.to_contract(security)

        if not self.connected:
            self.connect()

        # solves for errorCode 321 "please enter exchange"
        if not contract.exchange:
            contract.exchange = 'SMART'

        if self.__handle_error not in self.ib_client.errorEvent:
            self.ib_client.errorEvent += self.__handle_error

        if not self.ib_client.isConnected():
            raise ConnectionError()

        # 16 hours, 4am to 8pm
        # duration_step_size = '57600 S'

        # 24 hours
        duration_step_size = '86400 S'

        if str(bar_size) == '1 day':
            duration_step_size = '10 Y'
        if str(bar_size) == '1 hour':
            duration_step_size = '4 Y'
        if str(bar_size) == '2 hours':
            duration_step_size = '1 Y'

        # we say that the 'end date' is the start of the day after
        # start_date = dateify(start_date, timezone=tz_info)
        # end_date_offset = dateify(end_date, timezone=tz_info) + dt.timedelta(days=1)
        start_date = timezoneify(start_date, timezone=tz_info)
        end_date_offset = timezoneify(end_date, timezone=tz_info)

        current_date = end_date_offset
        local_tz = dt.datetime.now(dt.timezone.utc).astimezone().tzinfo

        logging.info('get_contract_history {} {} {} {}'.format(
            contract.conId,
            str(what_to_show),
            start_date,
            end_date
        ))

        bars: List[pd.DataFrame] = []

        while current_date >= start_date:
            logging.debug('self.ib_client.reqHistoricalDataAsync {} {} {} {} {}'.format(
                security,
                utcify_str(current_date),
                duration_step_size,
                str(bar_size),
                what_to_show,
            ))
            result = await self.ib_client.reqHistoricalDataAsync(
                contract,
                endDateTime=utcify_str(current_date),
                durationStr=duration_step_size,
                barSizeSetting=str(bar_size),
                whatToShow=str(what_to_show),
                useRTH=False,
                formatDate=2,
                keepUpToDate=False,
            )

            # skip if 'no data' returned
            # 162 is 'Historical Market data error' so we gotta parse the message
            if self.error_code > 0:
                raise Exception('error_code: {}, error_string: {}'.format(self.error_code, self.error_string))

            if result:
                df_result = ib_insync.util.df(result).set_index('date')
                df_result['bar_size'] = str(bar_size)
                df_result['what_to_show'] = int(what_to_show)
                df_result.rename({'barCount': 'bar_count'}, inplace=True, axis=1)

                # arctic requires timezone to be set
                df_result.index = pd.to_datetime(df_result.index)  # type: ignore
                # next line no leonger required as we pass in =2 to formatDate
                # df_result.index = df_result.index.tz_localize(local_tz)  # type: ignore
                df_result.index = df_result.index.tz_convert(tz_info)
                df_result.sort_index(ascending=True, inplace=True)

                pd_date = pd.to_datetime(df_result.index[0])
                earliest_date = dt.datetime(
                    pd_date.year,
                    pd_date.month,
                    pd_date.day,
                    pd_date.hour,
                    pd_date.minute,
                    pd_date.second,
                    tzinfo=gettz(tz_info)
                )
            else:
                df_result = pd.DataFrame()
                # we didn't get any data for this particular date, so subtract a day
                earliest_date = dateify(current_date, timezone=tz_info) - dt.timedelta(days=1)

            # ib doesn't have a way of differentiating between a weekend where there is no data,
            # and a trading day, where there were no trades.
            # inject null rows to cover these cases
            missing_dates = pd.date_range(start=earliest_date, end=current_date).difference(df_result.index)
            for d in missing_dates:
                local_date = d.to_pydatetime().replace(tzinfo=None)
                null_row = {
                    'date': [local_date],
                    'open': [np.nan],
                    'high': [np.nan],
                    'low': [np.nan],
                    'close': [np.nan],
                    'volume': [np.nan],
                    'average': [np.nan],
                    'bar_count': [np.nan],
                    'bar_size': [str(bar_size)],
                    'what_to_show': int(what_to_show),
                }
                temp_row = pd.DataFrame.from_dict(null_row)
                temp_row = temp_row.set_index('date')
                temp_row.index = temp_row.index.tz_localize(local_tz)  # type: ignore
                temp_row.index = temp_row.index.tz_convert(tz_info)
                df_result = pd.concat([df_result, temp_row])

            # add to the bars list
            bars.append(df_result)
            pd_date = pd.to_datetime(df_result.index[0])

            current_date = dt.datetime(
                pd_date.year,
                pd_date.month,
                pd_date.day,
                pd_date.hour,
                pd_date.minute,
                pd_date.second,
                tzinfo=gettz(tz_info)
            )

        all_data: pd.DataFrame = pd.concat(bars)

        if filter_between_dates:
            all_data = all_data[(all_data.index >= start_date)  # .replace(tzinfo=gettz(tz_info)))  # type: ignore
                                & (all_data.index <= end_date_offset)]  # .replace(tzinfo=gettz(tz_info)))]  # type: ignore
        return all_data.sort_index(ascending=True)
