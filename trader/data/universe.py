from dataclasses import dataclass, fields
import os
import csv
from types import resolve_bases
import numpy as np
import pandas as pd
import vectorbt as vbt
import datetime as dt
from dateutil.tz import tzlocal, gettz
from dateutil.tz.tz import tzfile

from pandas.core.base import PandasObject
from arctic import Arctic, TICK_STORE, VERSION_STORE
from arctic.tickstore.tickstore import VERSION, TickStore
from arctic.store.version_store import VersionStore
from arctic.exceptions import NoDataFoundException
from typing import Tuple, List, Optional, Dict, TypeVar, Generic, Type, Union, cast, Set
from ib_insync.contract import Contract
from trader.data.data_access import SecurityDefinition


class Universe():
    def __init__(self, name: str, security_definitions: List[SecurityDefinition] = []):
        self.name: str = name
        self.historical_tick_store: str = name
        self.security_definitions: List[SecurityDefinition] = security_definitions

    @staticmethod
    def to_contract(definition: Union[SecurityDefinition, Contract]) -> Contract:
        if type(definition) is SecurityDefinition:
            contract = Contract(secType=definition.secType, conId=definition.conId, symbol=definition.symbol,
                                currency=definition.currency, exchange=definition.exchange,
                                primaryExchange=definition.primaryExchange)
            return contract
        elif type(definition) is Contract:
            return definition
        else:
            raise ValueError('unable to cast type to Contract')


class UniverseAccessor():
    def __init__(self, arctic_server_address: str, arctic_universe_library: str):
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_universe_library
        self.store = Arctic(self.arctic_server_address)
        self.store.initialize_library(self.arctic_library, lib_type=VERSION_STORE)
        self.library: VersionStore = self.store[self.arctic_library]

    def list_universes(self) -> List[str]:
        return self.library.list_symbols()

    def list_universes_count(self) -> Dict[str, int]:
        universes = self.get_all()
        result = {}
        for u in universes:
            result[u.name] = len(u.security_definitions)
        return result

    def get_all(self) -> List[Universe]:
        result: List[Universe] = []
        for name in self.list_universes():
            u = self.get(name)
            if u:
                result.append(u)
        return result

    def get(self, name: str) -> Universe:
        try:
            return self.library.read(name).data
        except NoDataFoundException:
            return Universe(name)

    def update(self, universe: Universe) -> None:
        self.library.write(universe.name, universe)

    def delete(self, name: str) -> None:
        self.library.delete(name)

    def update_from_csv_str(self, name: str, csv_str: str) -> int:
        reader = csv.DictReader(csv_str.splitlines())
        defs: List[SecurityDefinition] = []
        counter = 0
        for row in reader:
            security_definition = SecurityDefinition()
            for n in [field.name for field in fields(SecurityDefinition)]:
                try:
                    security_definition.__setattr__(n, row[n])
                except KeyError:
                    continue
            defs.append(security_definition)
            counter += 1
        self.update(Universe(name, defs))
        return counter
