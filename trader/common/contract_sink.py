from ib_insync import Contract
from ib_insync.ticker import Ticker
from reactivex import abc, Observable, Observer
from reactivex.subject import Subject
from trader.common.logging_helper import setup_logging
from typing import Callable, Optional, Union

import pandas as pd


logging = setup_logging(module_name='contract_sink')


class ContractSink(Observable[Ticker], Observer[Ticker]):
    def __init__(self, contract: Contract):
        super(Observable, self).__init__()
        super(Observer, self).__init__()
        self.contract: Contract = contract
        self.latest_tick: Ticker = Ticker()
        self.latest_df: pd.DataFrame = pd.DataFrame()
        self.last_tick: Ticker = Ticker()
        self.last_df: pd.DataFrame = pd.DataFrame()
        self.data_frame: pd.DataFrame = pd.DataFrame()
        self.subject = Subject[Ticker]()

    def subscribe(
        self,
        on_next: Optional[
            Union[abc.ObserverBase[Ticker], abc.OnNext[Ticker], None]
        ] = None,
        on_error: Optional[abc.OnError] = None,
        on_completed: Optional[abc.OnCompleted] = None,
        *,
        scheduler: Optional[abc.SchedulerBase] = None,
    ) -> abc.DisposableBase:
        return self.subject.subscribe(on_next=on_next, on_error=on_error, on_completed=on_completed, scheduler=scheduler)

    def dispose(self):
        self.subject.dispose()

    # todo this is a duplicate from the Helpers class because we can't have circular imports
    def symbol_from_contract(self, contract: Contract) -> int:
        return contract.conId

    def df(self):
        return self.data_frame

    def latest_tick_df(self):
        return self.latest_df

    # todo this is a duplicate from the Helpers class because we can't have circular imports
    def df_from_ticker(self, t: Ticker) -> pd.DataFrame:
        symbol = 0
        if t.contract:
            symbol = self.symbol_from_contract(t.contract)

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

    def last(self):
        return self.data_frame.tail(1)

    def on_next(self, tick: Ticker):
        self.last_tick = self.latest_tick
        self.last_df = self.latest_df

        self.latest_tick = tick
        self.latest_df = self.df_from_ticker(tick)
        if len(self.data_frame) == 0:
            self.data_frame = self.latest_df
        else:
            self.data_frame = self.data_frame.append(self.latest_df, ignore_index=True)
        self.subject.on_next(tick)

    def on_completed(self):
        logging.debug('on_completed')
        self.subject.on_completed()

    def on_error(self, error):
        logging.error(error)
        self.subject.on_error(error)

    def __str__(self):
        return str(self.last())

    def pipe(self, *operators: Callable[[Observable], Observable]) -> Observable:  # type: ignore
        return self.subject.pipe(*operators)
