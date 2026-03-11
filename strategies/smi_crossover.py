from trader.data.store import DateRange
from ib_async import Contract
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
from trader.objects import Action, BarSize
from typing import Optional, Tuple

import datetime as dt
import exchange_calendars
import pandas as pd
import trader.strategy.strategy_runtime as runtime
import vectorbt as vbt


class SMICrossOver(Strategy):
    def __init__(self):
        super().__init__()
        self.strategy_runtime: Optional[runtime.StrategyRuntime] = None

    def install(self, context: StrategyContext) -> bool:
        super().install(context)
        return True

    def enable(self) -> StrategyState:
        if not self.strategy_runtime:
            raise ValueError('strategy_runtime not set')

        if not self.conids:
            raise ValueError('conids not set')

        date_range: Optional[DateRange] = None
        if self.historical_days_prior:
            date_range = DateRange(start=dt.datetime.now() - dt.timedelta(days=self.historical_days_prior), end=dt.datetime.now())

        # check to see if we have all the available data we need
        for conid in self.conids:
            missing_data = self.ctx.storage.get_tickdata(self.bar_size).missing(
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

    def __signals(self, open_price: pd.Series) -> Optional[Tuple[pd.Series, pd.Series]]:
        if len(open_price) <= 50:
            return None

        fast_ma = vbt.MA.run(open_price, 10)
        slow_ma = vbt.MA.run(open_price, 50)
        entries = fast_ma.ma_crossed_above(slow_ma)  # type: ignore
        exits = fast_ma.ma_crossed_below(slow_ma)  # type: ignore
        return (entries, exits)

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        if self.state != StrategyState.RUNNING:
            return None

        result = self.__signals(prices['close'])
        if result and result[0].iloc[-1] is True:
            return Signal('smi_crossover', Action.BUY, 0.0, 0.0)
        elif result and result[1].iloc[-1] is True:
            return Signal('smi_crossover', Action.SELL, 0.0, 0.0)
        else:
            return None

    def on_error(self, error):
        self.state = StrategyState.ERROR
        return super().on_error(error)
