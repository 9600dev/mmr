from abc import ABC, abstractmethod
from enum import IntEnum
from logging import Logger
from trader.data.data_access import TickData
from trader.data.universe import UniverseAccessor
from trader.objects import Action
from typing import Optional

import datetime as dt
import pandas as pd
# to avoid circular import


class Signal():
    def __init__(
        self,
        action: Action,
        probability: float,
        risk: float,
        date_time: dt.datetime = dt.datetime.now(),
        generating_frame: object = None
    ):
        self.action: Action = action
        self.probability: float = probability
        self.risk: float = risk
        self.date_time: dt.datetime = date_time
        self.generating_frame: object = generating_frame


class StrategyState(IntEnum):
    NOT_INSTALLED = 0,
    RUNNING = 1,
    DISABLED = 2,
    ERROR = 3,


class Strategy(ABC):
    def __init__(
        self,
        data: TickData,
        accessor: UniverseAccessor,
        logging: Logger,
    ):
        self.data = data
        self.accessor = accessor
        self.strategy_runtime = None
        self.logging = logging
        self.state = StrategyState.NOT_INSTALLED

    @abstractmethod
    def install(self, strategy_runtime) -> bool:
        pass

    @abstractmethod
    def uninstall(self) -> bool:
        pass

    @abstractmethod
    def enable(self) -> StrategyState:
        pass

    @abstractmethod
    def disable(self) -> StrategyState:
        pass

    @abstractmethod
    def on_next(self, prices: pd.DataFrame) -> Optional[Signal]:
        pass
