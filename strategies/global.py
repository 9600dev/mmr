from logging import Logger
from trader.data.data_access import TickStorage
from trader.data.universe import UniverseAccessor
from trader.trading.strategy import Signal, Strategy, StrategyState
from typing import Optional, Tuple

import pandas as pd
import trader.strategy.strategy_runtime as runtime


class Global(Strategy):
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
        self.state = StrategyState.INSTALLED
        return True

    def uninstall(self) -> bool:
        self.state = StrategyState.NOT_INSTALLED
        return True

    def enable(self) -> StrategyState:
        self.state = StrategyState.RUNNING
        return self.state

    def disable(self) -> StrategyState:
        self.state = StrategyState.DISABLED
        return self.state

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        return None

    def on_signal(self, signal: Signal) -> Optional[Signal]:
        return None

    def on_error(self, error):
        return super().on_error(error)
