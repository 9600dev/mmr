from dataclasses import dataclass
import os
from types import resolve_bases
import numpy as np
import pandas as pd
import vectorbt as vbt
import datetime as dt
from dateutil.tz import tzlocal, gettz
from dateutil.tz.tz import tzfile

from trader.common.logging_helper import setup_logging

logging = setup_logging(module_name='data.universe')

from pandas.core.base import PandasObject
from arctic import Arctic, TICK_STORE, VERSION_STORE
from arctic.date import DateRange, string_to_daterange
from arctic.tickstore.tickstore import VERSION, TickStore
from arctic.store.version_store import VersionStore
from arctic.exceptions import NoDataFoundException
from typing import Tuple, List, Optional, Dict, TypeVar, Generic, Type, Union, cast, Set
from durations import Duration
from exchange_calendars import ExchangeCalendar
from pandas import DatetimeIndex
from ib_insync.contract import Contract
from trader.common.helpers import dateify, daily_close, daily_open, market_hours, get_contract_from_csv, symbol_to_contract
from trader.data.contract_metadata import ContractMetadata
from trader.data.data_access import DictData


# follows quantrocket definition: https://www.quantrocket.com/codeload/moonshot-intro/intro_moonshot/Part2-Universe-Selection.ipynb.html
@dataclass
class SecurityDefinition():
    conId: int
    symbol: str
    exchange: str
    primaryExchange: str
    currency: str
    country: str
    secType: str
    etf: bool
    timezone: str
    name: str
    delisted: bool
    dateDelisted: Optional[dt.datetime]
    rolloverDate: Optional[dt.datetime]
    tradeable: bool


class Universe():
    def __init__(self, name: str, security_definitions: List[SecurityDefinition] = []):
        self.name: str = name
        self.security_definitions: List[SecurityDefinition] = security_definitions

    @staticmethod
    def to_contract(definition: SecurityDefinition) -> Contract:
        contract = Contract(secType=definition.secType, conId=definition.conId, symbol=definition.symbol,
                            currency=definition.currency, exchange=definition.exchange,
                            primaryExchange=definition.primaryExchange)
        return contract


class UniverseAccessor():
    def __init__(self, arctic_server_address: str, arctic_universe_library: str):
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_universe_library
        self.store = Arctic(self.arctic_server_address)
        self.store.initialize_library(self.arctic_library, lib_type=VERSION_STORE)
        self.library: VersionStore = self.store[self.arctic_library]

    def list_universes(self) -> List[str]:
        return self.library.list_symbols()

    def get_all(self) -> List[Universe]:
        result: List[Universe] = []
        for name in self.list_universes():
            u = self.get(name)
            if u:
                result.append(u)
        return result

    def get(self, name: str) -> Optional[Universe]:
        try:
            return self.library.read(name).data
        except NoDataFoundException:
            return Universe(name)

    def update(self, universe: Universe) -> None:
        self.library.write(universe.name, universe)

    def delete(self, universe: Universe) -> None:
        self.library.delete(universe.name)
