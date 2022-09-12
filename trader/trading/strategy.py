from abc import ABC, abstractmethod
from trader.objects import Action
from typing import Optional

import datetime as dt
import pandas as pd


class Signal():
    def __init__(
        self,
        action: Action,
        probability: float,
        date_time: dt.datetime = dt.datetime.now(),
        generating_frame: object = None
    ):
        self.action: Action = action
        self.probability: float = probability
        self.date_time: dt.datetime = date_time
        self.generating_frame: object = generating_frame


class Strategy(ABC):
    def __init__(
        self,
    ):
        pass

    @abstractmethod
    def install(self) -> bool:
        pass

    @abstractmethod
    def pre_condition(self) -> bool:
        pass

    @abstractmethod
    def post_condition(self) -> bool:
        pass

    @abstractmethod
    def on_next(self, prices: pd.DataFrame) -> Optional[Signal]:
        pass
