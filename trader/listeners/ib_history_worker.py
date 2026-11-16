from datetime import date
from dateutil.tz import gettz
from ib_async import IB
from ib_async.contract import Contract
from trader.common.helpers import dateify, timezoneify, utcify_str
from trader.common.logging_helper import setup_logging
from trader.data.data_access import SecurityDefinition
from trader.data.universe import Universe
from trader.listeners import ib_informational
from trader.objects import BarSize, WhatToShow
from typing import List, Optional, Union

import asyncio
import backoff
import calendar
import datetime as dt
import ib_async
import logging as _logging
import numpy as np
import pandas as pd
import pytz


logging = setup_logging(module_name='ib_history')


# Error codes that indicate informational messages, not real errors. The set
# is owned by ib_informational (the one IB-severity table); aliased for callers.
IB_INFO_CODES = ib_informational.FARM_OK_CODES

# Error codes that indicate connectivity/session issues (retryable after reconnect)
IB_CONNECTIVITY_CODES = {1100, 1101, 1102, 504}

# Error 162 substrings that indicate "no data" (skippable) vs fatal issues
_162_NO_DATA_SUBSTRINGS = [
    'HMDS query returned no data',
    'No market data permissions',
    'No data of type',
    'no items retrieved',
]

_162_FATAL_SUBSTRINGS = [
    'connected from a different IP address',
    'API client has been unsubscribed',
]


class IBConnectivityError(Exception):
    """IB connection/session lost — caller should reconnect and retry."""
    pass


class IBNoDataError(Exception):
    """IB returned no data for this contract/period — skip and continue."""
    pass



def missing_session_days(index, start: dt.datetime, end: dt.datetime, tz_info: str,
                         now: Optional[dt.datetime] = None) -> pd.DatetimeIndex:
    """Calendar days in [start, end] (exchange tz) with NO bar at all, as midnight stamps.

    These get an all-NULL marker row so the gap finder doesn't re-request a
    weekend / no-trade day forever. The old code built the candidate range
    with ``pd.date_range(start, end)``, which steps a day at a time at
    ``start``'s TIME OF DAY: right for daily bars (midnight), wrong for
    intraday — a 1-min window starting 04:00 ET produced a 04:00 candidate
    every day, and any day without a bar at exactly 04:00:00 got a NULL bar
    injected beside its real bars.

    Only a day that is OVER can be said to have had no data. A request made
    before the exchange's day has traded - a service restarted at 06:17 in
    Sydney, four hours before the ASX opens - found no bar for "today" and
    recorded that the source had nothing for it; the day's 372 real bars
    arrived that evening and the marker stayed beside them (six instruments on
    2026-11-16, which is what turned the nightly data audit red). So the
    exchange's current day, and anything after it, never gets a marker;
    ``now`` (default: the clock) is what "current" is measured against.
    """
    def _day(ts):
        t = pd.Timestamp(ts)
        t = t.tz_localize(tz_info) if t.tzinfo is None else t.tz_convert(tz_info)
        return t.normalize()

    today = _day(now if now is not None else pd.Timestamp.now(tz='UTC'))
    present = {_day(ts) for ts in index} if len(index) else set()
    days = pd.date_range(start=_day(start), end=_day(end), freq='D')
    return pd.DatetimeIndex([d for d in days if d not in present and d < today])


_DAILY_OR_LONGER = {'1 day', '1 week', '1 month'}
_RTH_SEC_TYPES = {'STK', 'IND'}


def use_rth_for(bar_size, sec_type: Optional[str]) -> bool:
    """``useRTH`` for an IB history request.

    Daily-and-longer bars for exchange-listed stocks/ETFs/indices use the
    REGULAR session, so the daily close is the official close (matching
    TwelveData / Massive). With ``useRTH=False`` IB folds pre/post-market
    trades into the daily bar: AAPL 2026-11-05 closed 343.25 (post-market
    last) vs the official 344.17. Intraday bars keep extended hours (the
    ORB / VWAP strategies and session_guard rely on pre/post bars), and
    futures / FX / crypto keep the full session — RTH is not their natural
    day there.
    """
    return str(bar_size) in _DAILY_OR_LONGER and (sec_type or '').upper() in _RTH_SEC_TYPES


# What IB puts in a trade field that does not exist for the bar it is sending.
IB_NOT_APPLICABLE = -1

# The trade fields of a bar, and what each holds when the bar was not built
# from trades. Volume is zero — no trade is aggregated in a quote bar, and a
# zero-volume bar is an ordinary, legal bar — while a WAP and a trade count
# simply do not exist (stored NULL). A NULL volume is not available as the
# answer: beside real prices it is a corrupt row (bar_quality: a bar with
# prices and no volume). The count is NULL and not 0 for a second reason:
# `bar_count = 0 AND average IS NULL` is how a TwelveData row is recognised
# (scripts/repair_download_listings.py moves and deletes rows on it).
_QUOTE_BAR_TRADE_FIELDS = (('volume', 0.0), ('average', np.nan), ('bar_count', np.nan))


def state_quote_bar_trade_fields(frame: pd.DataFrame, what_to_show: WhatToShow) -> pd.DataFrame:
    """Say what IB's -1 means on a bar that was not built from trades.

    For a MIDPOINT / BID / ASK request IB fills volume, WAP (``average``) and
    the trade count with -1: there is no such number for a bar built from
    quotes. Passed on as delivered, that is a bar with a negative volume —
    which is, correctly, a row that cannot describe a market
    (``bar_quality.check_bar``: ``negative_volume``) — so ``TickData.write``
    refused every bar of every forex series (strategy priming asks for
    MIDPOINT on CASH contracts), and what it would have stored otherwise is a
    VWAP of minus one.

    The rule was right and stays as it is: -1 was never a volume. This is the
    one place that knows both IB's convention and what was asked for, so the
    translation happens here and the stored row's ``what_to_show`` goes on
    saying what the bar was built from.

    Exact, in both directions: only the value -1, and only when the request
    was not for TRADES. A -1 on a trades bar, or any other negative number on
    a quote bar, is not IB saying "not applicable" and is left for the
    bar-quality check to refuse.
    """
    if what_to_show == WhatToShow.TRADES or len(frame) == 0:
        return frame
    out = frame.copy()
    for column, stated in _QUOTE_BAR_TRADE_FIELDS:
        if column in out.columns:
            out[column] = out[column].mask(out[column] == IB_NOT_APPLICABLE, stated)
    return out


_STEP_DAYS = {'S': 1 / 86400, 'D': 1, 'W': 7, 'M': 31, 'Y': 365}


def window_duration(step: str, start: dt.datetime, end: dt.datetime, margin_days: int = 2) -> str:
    """IB ``durationStr`` for one request: the chunk ``step``, or the window if smaller.

    ``step`` is the per-bar-size chunk (e.g. '10 Y', '1 W', '86400 S'). When
    the requested window (plus ``margin_days`` for weekends / exchange-tz
    edges) is shorter, return it in days. IB caps 'D' durations at 365, so
    longer windows keep the chunk. Unparseable steps are returned unchanged.
    """
    import math
    try:
        n_s, unit = step.split()
        step_days = float(n_s) * _STEP_DAYS[unit.upper()]
    except (ValueError, KeyError):
        return step
    window_days = math.ceil(max((end - start).total_seconds(), 0) / 86400) + margin_days
    if window_days < step_days and window_days <= 365 and window_days >= 1:
        return f'{window_days} D'
    return step


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
        # __handle_error logs every errorEvent message itself (classified), so
        # ib_async's raw "Error <code>, reqId ..." line would be a duplicate.
        ib_informational.quiet_handled_errors(self.ib_client)

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

        if errorCode in IB_INFO_CODES:
            self.__clear_error()
            return

        # Status chatter and warnings (farm 2103/2105/2108/2119/2157, the rest
        # of the 2100 range) are not failures of THIS request. Recording them
        # as one made a download that returned its bars raise and drop them —
        # and aborted strategy priming for every instrument after the first.
        level = ib_informational.ib_log_level(errorCode, errorString)
        if level < _logging.ERROR and errorCode not in IB_CONNECTIVITY_CODES:
            logging.log(level, 'ib status reqId: %s code %s: %s', reqId, errorCode, errorString)
            self.__clear_error()
            return

        if errorCode in IB_CONNECTIVITY_CODES:
            self.connected = False

        logging.log(ib_informational.ib_log_level(errorCode, errorString),
                    'ib error reqId: %s errorCode %s errorString %s contract %s',
                    reqId, errorCode, errorString, contract)

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

    async def connect_async(self):
        if self.connected:
            return self

        if self.__handle_error not in self.ib_client.errorEvent:
            self.ib_client.errorEvent += self.__handle_error

        await self.ib_client.connectAsync(
            host=self.ib_server_address,
            port=self.ib_server_port,
            clientId=self.ib_client_id,
            timeout=15,
            readonly=True
        )

        self.error_code = 0
        self.error_string = ''
        self.error_contract = None
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

        self.__clear_error()
        contract = Universe.to_contract(security)

        # Derive the exchange timezone from the security so daily bars key at the
        # correct exchange midnight (US → ET, ASX → Sydney, etc.) — matching the
        # `data migrate-timezone` migration and keeping old + new bars for the
        # same symbol on one consistent key. Falls back to the tz_info default if
        # the security has no / an unparseable timeZoneId. (This resolves the
        # long-standing "asx stocks get the incorrect timezone" TODO.)
        _sec_tz = getattr(security, 'timeZoneId', '') or ''
        if _sec_tz:
            try:
                pytz.timezone(_sec_tz)
                tz_info = _sec_tz
            except Exception:
                logging.debug('unparseable timeZoneId %r; keeping tz_info=%s', _sec_tz, tz_info)

        if not self.connected:
            await self.connect_async()

        # solves for errorCode 321 "please enter exchange"
        if not contract.exchange:
            contract.exchange = 'SMART'

        if self.__handle_error not in self.ib_client.errorEvent:
            self.ib_client.errorEvent += self.__handle_error

        if not self.ib_client.isConnected():
            raise ConnectionError()

        # 16 hours, 4am to 8pm
        # duration_step_size = '57600 S'

        # IB historical-data chunk sizes per bar size. Bigger chunks =
        # fewer round-trips before hitting IB's pacing limit.
        # Limits per IB docs:
        #   1 sec  -> 1800 S, 5 sec -> 7200 S
        #   1 min  -> 1 W, 5 min -> 1 M
        #   1 hour -> 1 Y, 1 day -> no practical cap
        duration_step_size = '86400 S'

        if str(bar_size) == '1 day':
            duration_step_size = '10 Y'
        if str(bar_size) == '1 hour':
            duration_step_size = '4 Y'
        if str(bar_size) == '2 hours':
            duration_step_size = '1 Y'
        if str(bar_size) == '1 min':
            duration_step_size = '1 W'
        if str(bar_size) == '5 mins' or str(bar_size) == '5 min':
            duration_step_size = '1 M'
        if str(bar_size) == '15 mins' or str(bar_size) == '15 min':
            duration_step_size = '1 M'

        # we say that the 'end date' is the start of the day after
        # start_date = dateify(start_date, timezone=tz_info)
        # end_date_offset = dateify(end_date, timezone=tz_info) + dt.timedelta(days=1)
        start_date = timezoneify(start_date, timezone=tz_info)
        end_date_offset = timezoneify(end_date, timezone=tz_info)

        current_date = end_date_offset
        local_tz = dt.datetime.now(dt.timezone.utc).astimezone().tzinfo

        # Don't ask IB for 10 years of daily bars to fill a 15-day gap: size
        # the request to the window when it is smaller than the chunk (the
        # loop below steps back from the earliest bar, so later chunks are
        # only requested if the window really is larger).
        duration_step_size = window_duration(duration_step_size, start_date, end_date_offset)

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
                useRTH=use_rth_for(bar_size, getattr(contract, 'secType', None)),
                formatDate=2,
                keepUpToDate=False,
            )

            if self.error_code > 0:
                code = self.error_code
                msg = self.error_string
                self.__clear_error()

                if code in IB_CONNECTIVITY_CODES:
                    raise IBConnectivityError(
                        'error_code: {}, error_string: {}'.format(code, msg))

                if code == 162:
                    if any(s in msg for s in _162_FATAL_SUBSTRINGS):
                        raise IBConnectivityError(
                            'error_code: {}, error_string: {}'.format(code, msg))
                    if any(s in msg for s in _162_NO_DATA_SUBSTRINGS):
                        if any(len(frame) > 0 for frame in bars):
                            # The walk went back past the instrument's first
                            # bar. That is the END of its history, not a failed
                            # download: raising here threw away every chunk
                            # already fetched, so anything listed more recently
                            # than the window (an IPO, a spin-off, a new conId)
                            # could never be downloaded or primed at all.
                            logging.info(
                                'no earlier history for %s before %s; keeping the %d chunk(s) fetched',
                                contract.conId, current_date, len(bars))
                            break
                        raise IBNoDataError(
                            'error_code: {}, error_string: {}'.format(code, msg))
                    # unknown 162 variant — treat as no-data to avoid crashing
                    logging.warning('unrecognized 162 error, treating as no-data: {}'.format(msg))
                    raise IBNoDataError(
                        'error_code: {}, error_string: {}'.format(code, msg))

                raise Exception('error_code: {}, error_string: {}'.format(code, msg))

            if result:
                df_result = ib_async.util.df(result).set_index('date')
                df_result['bar_size'] = str(bar_size)
                df_result['what_to_show'] = int(what_to_show)
                df_result.rename({'barCount': 'bar_count'}, inplace=True, axis=1)

                is_daily_dates = type(df_result.index[0]) is date

                if is_daily_dates:
                    # Daily bars: IB returns date objects. Key them at
                    # EXCHANGE-tz midnight (tz_localize), matching the
                    # Massive/TwelveData daily convention. The old
                    # to_datetime(utc=True).tz_convert(tz_info) put the bar at
                    # UTC midnight — ~5h off ET midnight — so the same trading
                    # day had two different timestamps across sources and daily
                    # merges mis-aligned by a day.
                    df_result.index = pd.to_datetime(df_result.index).tz_localize(tz_info)  # type: ignore
                else:
                    # Intraday: IB timestamps are absolute (formatDate=2);
                    # convert to the exchange tz for display/keying.
                    df_result.index = pd.to_datetime(df_result.index, utc=False)  # type: ignore
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
            missing_dates = missing_session_days(df_result.index, earliest_date, current_date, tz_info)
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
                # Localise to the EXCHANGE tz (tz_info), not the host machine's
                # local tz — so injected null-day markers key consistently with
                # the real daily bars regardless of where the process runs.
                temp_row.index = temp_row.index.tz_localize(tz_info)  # type: ignore
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
        # Every frame leaving this worker has passed through here, markers
        # included (their fields are NaN, which is not the sentinel).
        all_data = state_quote_bar_trade_fields(all_data, what_to_show)

        # A no-data marker says "nothing traded that day". Markers are computed
        # per CHUNK, and a day that straddles a chunk boundary has its bars in
        # the neighbouring chunk — so it got a marker AND real bars. Stored via
        # priming / `history ib`, those markers pushed the nightly audit's
        # `1 min|empty_bar` count past its baseline every night (534 vs 278 on
        # 2026-11-14), which is why the audit job had been red for weeks. Keep
        # a marker only when no chunk has a real bar for that venue day.
        if len(all_data) > 0 and 'close' in all_data.columns:
            try:
                venue_days = all_data.index.tz_convert(tz_info).normalize()  # type: ignore
                real = all_data['close'].notna()
                day_has_real = real.groupby(venue_days).transform('any')
                all_data = all_data[real | ~day_has_real]
            except (TypeError, AttributeError) as ex:
                logging.debug('placeholder de-duplication skipped: %s', ex)

        if filter_between_dates:
            all_data = all_data[(all_data.index >= start_date)  # .replace(tzinfo=gettz(tz_info)))  # type: ignore
                                & (all_data.index <= end_date_offset)]  # .replace(tzinfo=gettz(tz_info)))]  # type: ignore
        return all_data.sort_index(ascending=True)
