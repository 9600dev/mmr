from abc import ABC, abstractmethod
from enum import IntEnum
from logging import Logger
from trader.common.helpers import DictHelper
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
    INSTALLED = 1,
    WAITING_HISTORICAL_DATA = 2,
    RUNNING = 3,
    DISABLED = 4,
    ERROR = 5,


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

        self.name: Optional[str] = None
        self.bar_size: BarSize = BarSize.Mins1
        self.conids: Optional[List[int]] = None
        self.universe: Optional[str] = None
        self.module: Optional[str] = None
        self.class_name: Optional[str] = None
        self.historical_days_prior: Optional[int] = None
        self.runs_when_crontab: Optional[str] = None
        self.description: Optional[str] = None
        self.paper: bool = True

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
    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        pass

    @abstractmethod
    def on_signal(self, signal: Signal) -> Optional[Signal]:
        pass

    @abstractmethod
    def on_error(self, error):
        pass


class StrategyConfig():
    def __init__(
        self,
        name: str,
        state: StrategyState,
        bar_size: Optional[BarSize] = None,
        conids: Optional[List[int]] = None,
        universe: Optional[str] = None,
        module: Optional[str] = None,
        class_name: Optional[str] = None,
        historical_days_prior: Optional[int] = None,
        runs_when_crontab: Optional[str] = None,
        description: Optional[str] = None,
        paper: bool = True,
    ):
        self.name = name
        self.bar_size = bar_size
        self.conids = conids
        self.universe = universe
        self.module = module
        self.class_name = class_name
        self.historical_days_prior = historical_days_prior
        self.runs_when_crontab = runs_when_crontab
        self.description = description
        self.state = state
        self.paper = paper

    @staticmethod
    def from_strategy(strategy: Strategy) -> 'StrategyConfig':
        return StrategyConfig(
            name=strategy.name if strategy.name is not None else 'not_set',
            bar_size=strategy.bar_size,
            conids=strategy.conids,
            universe=strategy.universe,
            module=strategy.module,
            class_name=strategy.class_name,
            historical_days_prior=strategy.historical_days_prior,
            runs_when_crontab=strategy.runs_when_crontab,
            description=strategy.description,
            state=strategy.state,
            paper=strategy.paper
        )
