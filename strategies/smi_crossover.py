from datetime import timedelta
from ib_insync import Contract
from logging import Logger
from trader.data.data_access import TickData
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.trading.strategy import Signal, Strategy, StrategyConfig, StrategyState
from typing import Optional, Tuple

import pandas as pd
import trader.strategy.strategy_runtime as runtime
import vectorbt as vbt


@StrategyConfig(
    universe_symbol_pairs=[('NASDAQ', '4391'), ('ASX', '34805876')],
    bar_size=BarSize.Mins1,
    historical_data_timedelta=timedelta(days=180),
)
class SMICrossOver(Strategy):
    def __init__(
        self,
        data: TickData,
        accessor: UniverseAccessor,
        logging: Logger,

    ):
        super().__init__(
            data,
            accessor,
            logging
        )

        self.contracts = [
            Contract(conId=76792991),  # TSLA
            Contract(conId=34805876),  # FMG.ASX
        ]

    def install(self, strategy_runtime: runtime.StrategyRuntime) -> bool:
        self.strategy_runtime = strategy_runtime
        return True

    def uninstall(self) -> bool:
        return True

    def enable(self) -> StrategyState:
        if not self.strategy_runtime:
            raise ValueError('install() has not been called')

        # start subscriptions
        for contract in self.contracts:
            self.strategy_runtime.subscribe(self, contract)

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
