from arctic import Arctic, VERSION_STORE
from arctic.exceptions import NoDataFoundException
from arctic.store.version_store import VersionStore
from dataclasses import fields
from ib_insync.contract import Contract
from trader.data.data_access import SecurityDefinition
from typing import cast, Dict, List, Optional, Tuple, Union

import csv


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

    def find_symbol(self, symbol: Union[int, str]) -> Optional[SecurityDefinition]:
        for definition in self.security_definitions:
            if type(symbol) is str and cast(str, symbol).isnumeric():
                if definition.conId == int(symbol):
                    return definition
            elif type(symbol) is str:
                if definition.symbol == symbol:
                    return definition
            else:
                if definition.conId == symbol:
                    return definition
        return None


class UniverseAccessor():
    def __init__(self, arctic_server_address: str, arctic_universe_library: str):
        self.arctic_server_address = arctic_server_address
        self.arctic_universe_library = arctic_universe_library
        self.store = Arctic(self.arctic_server_address)
        self.store.initialize_library(self.arctic_universe_library, lib_type=VERSION_STORE)
        self.library: VersionStore = self.store[self.arctic_universe_library]
        # reverse order
        self.sorted_names = ['LSE', 'ASX', 'NYSE', 'NASDAQ']
        self.sorted_types = ['STK', 'OPT', 'FUT', 'EFT']

        # todo: fix this at the source: Universe shouldn't have a list of SecurityDefinitions
        self._resolver_cache: Dict[int, Tuple[Universe, SecurityDefinition]] = {}

    def list_universes(self) -> List[str]:
        result = self.library.list_symbols()
        # move the portfolio universe to the front
        if 'portfolio' in result:
            result.remove('portfolio')
            result.insert(0, 'portfolio')

        for name in self.sorted_names:
            if name in result:
                result.remove(name)
                result.insert(0, name)
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

    def resolve_universe(
        self,
        symbol: Union[str, int],
        exchange: str = '',
        universe: str = '',
        sec_type: str = '',
        first_only: bool = False,
    ) -> List[Tuple[Universe, SecurityDefinition]]:
        def check_exchange(exchange, definition: SecurityDefinition) -> bool:
            if not exchange:
                return True
            if definition.exchange == exchange:
                return True
            if definition.primaryExchange == exchange:
                return True
            return False

        def check_sec_type(sec_type, definition: SecurityDefinition) -> bool:
            if not sec_type:
                return True
            if definition.secType == sec_type:
                return True
            return False

        # support the SYBOL.EXCHANGE format
        if type(symbol) is str and '.' in symbol and not exchange:
            symbol, exchange = symbol.split('.')

        results: List[Tuple[Universe, SecurityDefinition]] = []
        universes = []

        if type(symbol) is int and int(symbol) in self._resolver_cache:
            u, definition = self._resolver_cache[int(symbol)]
            if check_exchange(exchange, definition) and not universe or universe == u.name:
                return [(u, definition)]

        if universe:
            universes.append(self.get(universe))
        else:
            universes.extend(self.get_all())

        for u in universes:
            for definition in u.security_definitions:
                if (
                    type(symbol) is int
                    and int(symbol) == definition.conId
                    and check_exchange(exchange, definition)
                    and check_sec_type(sec_type, definition)
                ):
                    results.append((u, definition))
                    self._resolver_cache.update({definition.conId: (u, definition)})
                    if first_only: return results
                if (
                    type(symbol) is str
                    and symbol.isnumeric()
                    and int(symbol) == definition.conId
                    and check_exchange(exchange, definition)
                    and check_sec_type(sec_type, definition)
                ):
                    results.append((u, definition))
                    self._resolver_cache.update({definition.conId: (u, definition)})
                    if first_only: return results
                if (
                    type(symbol) is str
                    and symbol == definition.symbol
                    and check_exchange(exchange, definition)
                    and check_sec_type(sec_type, definition)
                ):
                    results.append((u, definition))
                    self._resolver_cache.update({definition.conId: (u, definition)})
                    if first_only: return results

        results.sort(
            key=lambda x: self.sorted_types.index(x[1].secType) if x[1].secType in self.sorted_types else len(self.sorted_types)
        )
        return results

    def resolve_universe_name(
        self,
        symbol: Union[str, int],
        exchange: str = '',
        universe: str = '',
        sec_type: str = '',
        first_only: bool = False,
    ) -> List[Tuple[str, SecurityDefinition]]:
        return [(u.name, d) for u, d in self.resolve_universe(symbol, exchange, universe, sec_type, first_only)]

    def resolve_symbol(
        self,
        symbol: Union[str, int],
        exchange: str = '',
        universe: str = '',
        sec_type: str = '',
        first_only: bool = False,
    ) -> List[SecurityDefinition]:
        # unique definitions via set comprehension
        result = {definition for _, definition in self.resolve_universe(symbol, exchange, universe, sec_type, first_only)}
        return list(result)

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

        universe = self.get(name)
        universe.security_definitions = universe.security_definitions + defs
        self.update(universe)

        return counter
