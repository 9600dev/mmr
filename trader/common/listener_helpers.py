from bson import json_util
from ib_insync.contract import Contract, Forex, Stock
from ib_insync.ticker import Ticker
from ib_insync.util import df
from typing import Any, Awaitable, Dict, List, Optional, Tuple, TypeVar

import asyncio
import functools
import json
import logging
import numpy as np
import pandas as pd


class Helpers():
    @staticmethod
    def equity(symbol: str) -> Stock:
        return Stock(symbol=symbol, exchange='SMART', currency='USD')

    @staticmethod
    def forex(symbol: str) -> Forex:
        return Forex(pair=symbol, exchange='IDEALPRO')

    @staticmethod
    def to_df(contract: Contract) -> pd.DataFrame:
        return pd.DataFrame.from_dict([contract.__dict__])  # type: ignore

    @staticmethod
    def clean_contract_object(contract: Contract) -> Contract:
        if '.' in contract.primaryExchange:
            contract.exchange = 'SMART'  # contract.primaryExchange[:contract.primaryExchange.index('.')]
            contract.primaryExchange = ''  # contract.primaryExchange[:contract.primaryExchange.index('.')]
        if contract.conId and contract.conId > 0 and contract.symbol:
            contract.symbol = ''
        return contract

    @staticmethod
    def contracts_from_df(data_frame: pd.DataFrame) -> List[Contract]:
        temp_contract = Contract()
        contract_columns = list(temp_contract.__dict__.keys())
        columns = [c for c in contract_columns if c in data_frame.columns]
        framed = data_frame[columns]
        return [Contract(**kwargs) for kwargs in framed.to_dict(orient='records') if kwargs['conId'] > 0]

    @staticmethod
    def symbol(contract: Contract) -> int:
        return contract.conId
        # if type(contract) is Forex:
        #     return contract.symbol + contract.currency
        # if type(contract) is Stock:
        #     return contract.symbol + contract.currency
        # if type(contract) is Future:
        #     return contract.symbol
        # else:
        #     raise ValueError('not implemented')

    @staticmethod
    def df_simple(t: Ticker) -> pd.DataFrame:
        symbol = 0
        if t.contract:
            symbol = Helpers.symbol(t.contract)
        return pd.DataFrame([[symbol,
                              # t.time,
                              t.bid,
                              t.bidSize,
                              t.ask,
                              t.askSize,
                              t.last,
                              t.lastSize,
                              t.volume]],
                            columns=['contract', 'bid', 'bidSize', 'ask', 'askSize', 'last', 'lastSize', 'volume'])

    @staticmethod
    def df_complex(t: Ticker) -> pd.DataFrame:
        symbol = 0
        if t.contract:
            symbol = Helpers.symbol(t.contract)

        return pd.DataFrame([[symbol,
                              t.time,
                              t.marketDataType,
                              t.bid,
                              t.bidSize,
                              t.ask,
                              t.askSize,
                              t.last,
                              t.lastSize,
                              t.prevBid,
                              t.prevBidSize,
                              t.prevAsk,
                              t.prevAskSize,
                              t.prevLast,
                              t.prevLastSize,
                              t.volume,
                              t.vwap,
                              t.halted]],
                            columns=['contract', 'time', 'marketDataType', 'bid', 'bidSize', 'ask', 'askSize', 'last',
                                     'lastSize', 'prevBid', 'prevBidSize', 'prevAsk', 'prevAskSize', 'prevLast',
                                     'prevLastSize', 'volume', 'vwap', 'halted'])

    @staticmethod
    def dict_complex(t: Ticker) -> Dict:
        symbol = 0
        if t.contract:
            symbol = Helpers.symbol(t.contract)
        return {
            'contract': symbol,
            'time': t.time,
            'marketDataType': t.marketDataType,
            'bid': t.bid,
            'bidSize': t.bidSize,
            'ask': t.ask,
            'askSize': t.askSize,
            'last': t.last,
            'lastSize': t.lastSize,
            'prevBid': t.prevBid,
            'prevBidSize': t.prevBidSize,
            'prevAsk': t.prevAsk,
            'prevAskSize': t.prevAskSize,
            'prevLast': t.prevLast,
            'prevLastSize': t.prevLast,
            'volume': t.volume,
            'vwap': t.vwap,
            'halted': t.halted
        }

    @staticmethod
    def json_complex(dict: Dict) -> str:
        # https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
        return json.dumps(dict, default=json_util.default)

    @staticmethod
    def df(t: Ticker) -> pd.DataFrame:
        return df([t])  # type: ignore

    @staticmethod
    def rolling_linreg(df, window=90):
        '''
        Does linear regression on columns in df and returns two frames.
        The first is a slope; the second is the Pearson correlation coefficient.
        '''
        y = df
        x = y.copy()
        x.values.fill(1.0)
        x = x.cumsum()

        sum_y = y.rolling(window).sum()
        sum_y2 = (y ** 2).rolling(window).sum()
        sum_x = x.rolling(window).sum()
        sum_x2 = (x ** 2).rolling(window).sum()
        sum_xy = x.mul(y).rolling(window).sum()
        a_numerator = sum_y.mul(sum_x2) - sum_x.mul(sum_xy)
        denominator = window * sum_x2 - sum_x.mul(sum_x)
        a = a_numerator / denominator

        b_numerator = window * sum_xy - sum_x.mul(sum_y)
        b = b_numerator / denominator

        r_numerator = b_numerator
        r_denominator = ((window * sum_x2 - sum_x.mul(sum_x)).mul(window * sum_y2 - sum_y.mul(sum_y))) ** 0.5
        r = r_numerator / r_denominator

        return b, r

    @staticmethod
    def jump(ln_series, window=90):
        ln_delta = ln_series - ln_series.shift()
        no_jump = ln_delta.abs() < np.log(1.15)
        return no_jump.rolling(window).min().fillna(0.0).astype(bool)

    @staticmethod
    def window(df, days: int = 0, hours: int = 0, minutes: int = 0, seconds: int = 0, time_delta: Optional[pd.Timedelta] = None):
        '''
        index must be datetime based and last row is most recent time
        '''
        last = df.tail(1)
        time_index = time_delta
        if not time_index:
            time_index = last.index - pd.Timedelta(
                days=days,
                hours=hours,
                minutes=minutes,
                seconds=seconds)  # type: ignore
        return df[df.index >= time_index.item()]  # type: ignore

    @staticmethod
    def eod(df):
        return df.iloc[df.reset_index().groupby(df.index.to_period('D'))['index'].idxmax()]

    @staticmethod
    def sod(df):
        return df.iloc[df.reset_index().groupby(df.index.to_period('D'))['index'].idxmin()]

    T = TypeVar('T')

    @staticmethod
    def _handle_task_result(
        task: asyncio.Task,
        *,
        logger: logging.Logger,
        message: str,
        message_args: Tuple[Any, ...] = (),
    ) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass  # Task cancellation should not be logged as an error.
        # Ad the pylint ignore: we want to handle all exceptions here so that the result of the task
        # is properly logged. There is no point re-raising the exception in this callback.
        except Exception:  # pylint: disable=broad-except
            logger.exception(message, *message_args)

    @staticmethod
    def create_task(
        coroutine: Awaitable[T],
        *,
        logger: logging.Logger,
        message: str,
        message_args: Tuple[Any, ...] = (),
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> asyncio.Task[T]:
        '''
        This helper function wraps a ``loop.create_task(coroutine())`` call and ensures there is
        an exception handler added to the resulting task. If the task raises an exception it is logged
        using the provided ``logger``, with additional context provided by ``message`` and optionally
        ``message_args``.
        '''
        if loop is None:
            loop = asyncio.get_running_loop()
        task = loop.create_task(coroutine)  # type: ignore
        task.add_done_callback(
            functools.partial(
                Helpers._handle_task_result,
                logger=logger,
                message=message,
                message_args=message_args
            )
        )
        return task
