from abc import ABC, abstractmethod
from datetime import timedelta
from enum import IntEnum
from logging import Logger
from trader.data.data_access import TickData
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from typing import List, Optional, Tuple

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
    GETTING_HISTORICAL_DATA = 1,
    RUNNING = 2,
    DISABLED = 3,
    ERROR = 4,


def StrategyConfig(
    universe_symbol_pairs: List[Tuple[str, str]] = [],
    bar_size: BarSize = BarSize.Mins1,
    historical_data_timedelta: Optional[timedelta] = None,
    runs_when_crontab: Optional[str] = None,
):
    class StrategyWrapper:
        def __init__(self, cls):
            self.strategy_class = cls

        def __call__(self, *cls_args):
            strategy = self.strategy_class(*cls_args)
            strategy.universe_symbol_pairs = universe_symbol_pairs
            strategy.historical_data_timedelta = historical_data_timedelta
            strategy.bar_size = bar_size
            strategy.crontab = runs_when_crontab
            return strategy
    return StrategyWrapper


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

        # set by the StrategyConfig decorator
        self.universe_symbol_pairs: List[Tuple[str, str]] = []
        self.bar_size: BarSize = BarSize.Mins1
        self.historical_data_timedelta: Optional[timedelta] = None
        self.runs_when_crontab: Optional[str] = None

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
