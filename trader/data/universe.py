from dataclasses import dataclass, fields
import os
import csv
from types import resolve_bases
import numpy as np
import pandas as pd
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
        self.security_definitions: List[SecurityDefinition] = security_definitions

    @staticmethod
    def to_contract(definition: Union[SecurityDefinition, Contract]) -> Contract:
        if isinstance(definition, SecurityDefinition):
            contract = Contract(secType=definition.secType, conId=definition.conId, symbol=definition.symbol,
                                currency=definition.currency, exchange=definition.exchange,
                                primaryExchange=definition.primaryExchange)
            return contract
        elif isinstance(definition, Contract):
            return definition
        else:
            raise ValueError('unable to cast type to Contract')

    def find_contract(self, contract: Contract) -> Optional[SecurityDefinition]:
        for definition in self.security_definitions:
            if definition.conId == contract.conId:
                return definition
        return None

    def find_symbol(self, symbol: str) -> Optional[SecurityDefinition]:
        for definition in self.security_definitions:
            if definition.symbol == symbol:
                return definition
        return None


class UniverseAccessor():
    def __init__(self, arctic_server_address: str, arctic_universe_library: str):
        self.arctic_server_address = arctic_server_address
        self.arctic_library = arctic_universe_library
        self.store = Arctic(self.arctic_server_address)
        self.store.initialize_library(self.arctic_library, lib_type=VERSION_STORE)
        self.library: VersionStore = self.store[self.arctic_library]

    def list_universes(self) -> List[str]:
        result = self.library.list_symbols()
        # move the portfolio universe to the front
        if 'portfolio' in result:
            result.remove('portfolio')
            result.insert(0, 'portfolio')
        return result

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
        except NoDataFoundException as ex:
            universe = Universe(name)
            universe.security_definitions = []
            return universe

    def find_contract(self, contract: Contract) -> Optional[Universe]:
        for universe in self.get_all():
            for definition in universe.security_definitions:
                if contract.conId == definition.conId:
                    return universe
        return None

    def update(self, universe: Universe) -> None:
        self.library.write(universe.name, universe, prune_previous_version=True)

    def insert(self, universe_name: str, security_definition: SecurityDefinition):
        universe = self.get(universe_name)
        universe.security_definitions.append(security_definition)
        self.update(universe)

    def delete(self, name: str) -> None:
        self.library.delete(name)

    def update_from_csv_str(self, name: str, csv_str: str) -> int:
        reader = csv.DictReader(csv_str.splitlines())
        defs: List[SecurityDefinition] = []
        counter = 0
        for row in reader:
            args = {}
            for n in [field.name for field in fields(SecurityDefinition)]:
                try:
                    args[n] = row[n]
                except KeyError:
                    args[n] = ''
            security_definition = SecurityDefinition(**args)
            defs.append(security_definition)
            counter += 1
        self.update(Universe(name, defs))
        return counter
