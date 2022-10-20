from abc import ABC, abstractmethod
from enum import IntEnum
from logging import Logger
from trader.data.data_access import TickStorage
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from typing import List, Optional

import datetime as dt
import pandas as pd


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
    WAITING_HISTORICAL_DATA = 1,
    RUNNING = 2,
    DISABLED = 3,
    ERROR = 4,


def StrategyConfig(
    name: str,
    bar_size: BarSize = BarSize.Mins1,
    conids: Optional[List[int]] = None,
    universe: Optional[str] = None,
    historical_days_prior: Optional[int] = None,
    runs_when_crontab: Optional[str] = None,
    description: Optional[str] = None,
):
    class StrategyWrapper:
        def __init__(self, cls):
            self.strategy_class = cls

        def __call__(self, *cls_args):
            strategy = self.strategy_class(*cls_args)
            strategy.name = name
            strategy.bar_size = bar_size
            strategy.conids = conids
            strategy.universe = universe
            strategy.historical_days_prior = historical_days_prior
            strategy.runs_when_crontab = runs_when_crontab
            strategy.description = description
            return strategy
    return StrategyWrapper


class Strategy(ABC):
    def __init__(
        self,
        storage: TickStorage,
        accessor: UniverseAccessor,
        logging: Logger,
    ):
        self.storage = storage
        self.accessor = accessor
        self.strategy_runtime = None
        self.logging = logging
        self.state = StrategyState.NOT_INSTALLED

        # set by the StrategyConfig decorator, or by the strategy_runtime.yaml file
        self.name: str = ''
        self.bar_size: BarSize = BarSize.Mins1
        self.conids: Optional[List[int]] = None
        self.universe: Optional[str] = None
        self.historical_days_prior: Optional[int] = None
        self.runs_when_crontab: Optional[str] = None
        self.description: Optional[str] = None

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
