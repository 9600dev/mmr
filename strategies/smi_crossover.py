from arctic.date import DateRange
from ib_insync import Contract
from logging import Logger
from trader.data.data_access import TickStorage
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.trading.strategy import Signal, Strategy, StrategyConfig, StrategyState
from typing import Optional, Tuple

import datetime as dt
import exchange_calendars
import pandas as pd
import trader.strategy.strategy_runtime as runtime
import vectorbt as vbt


@StrategyConfig(
    name='SMICrossOver',
    conids=[4391, 34805876],
    bar_size=BarSize.Mins1,
    historical_days_prior=180
)
class SMICrossOver(Strategy):
    def __init__(
        self,
        storage: TickStorage,
        accessor: UniverseAccessor,
        logging: Logger,
    ):
        super().__init__(
            storage,
            accessor,
            logging
        )

    def install(self, strategy_runtime: runtime.StrategyRuntime) -> bool:
        self.strategy_runtime = strategy_runtime
        return True

    def uninstall(self) -> bool:
        return True

    def enable(self) -> StrategyState:
        if not self.strategy_runtime:
            raise ValueError('install() has not been called')

        date_range: Optional[DateRange] = None
        if self.historical_days_prior:
            date_range = DateRange(start=dt.datetime.now() - dt.timedelta(days=self.historical_days_prior), end=dt.datetime.now())

        # check to see if we have all the available data we need
        # todo fix this
        for conid in self.conids:
            missing_data = self.storage.get_tickdata(self.bar_size).missing(
                conid,
                exchange_calendar=exchange_calendars.get_calendar('NASDAQ'),
                pd_offset=None,
                date_range=date_range
            )

            if missing_data:
                self.state = StrategyState.WAITING_HISTORICAL_DATA
                return self.state

        # start subscriptions
        for conid in self.conids:
            self.strategy_runtime.subscribe(self, Contract(conId=conid))

        self.state = StrategyState.RUNNING
        return self.state

    def disable(self) -> StrategyState:
        self.state = StrategyState.DISABLED
        return self.state

    def signals(self, open_price: pd.Series) -> Optional[Tuple[pd.Series, pd.Series]]:
        if len(open_price) <= 50:
            return None

        fast_ma = vbt.MA.run(open_price, 10)
        slow_ma = vbt.MA.run(open_price, 50)
        entries = fast_ma.ma_crossed_above(slow_ma)  # type: ignore
        exits = fast_ma.ma_crossed_below(slow_ma)  # type: ignore
        return (entries, exits)

    def on_next(self, prices: pd.DataFrame) -> Optional[Signal]:
        result = self.signals(prices.ask)
        if result and result[0].iloc[-1] is True:
            return Signal(Action.BUY, 0.0, 0.0)
        elif result and result[1].iloc[-1] is True:
            return Signal(Action.SELL, 0.0, 0.0)
        else:
            return None
