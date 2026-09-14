from dataclasses import fields
from ib_async.contract import Contract
from trader.data.data_access import SecurityDefinition
from trader.data.duckdb_store import DuckDBObjectStore, _default_db_path
from typing import cast, Dict, List, Optional, Tuple, Union

import csv
import threading


class Universe():
    def __init__(self, name: str, security_definitions: List[SecurityDefinition] = []):
        self.name: str = name
        self.security_definitions: List[SecurityDefinition] = security_definitions

    @staticmethod
    def to_contract(definition: Union[SecurityDefinition, Contract]) -> Contract:
        return SecurityDefinition.to_contract(definition)

    def find_contract(self, contract: Contract) -> Optional[SecurityDefinition]:
        for definition in self.security_definitions:
            if definition.conId == contract.conId:
                return definition
        return None

    def find_symbol(self, symbol: Union[int, str]) -> Optional[SecurityDefinition]:
        # A str is ALWAYS a ticker, even a numeric one — HK/JP tickers like
        # "0700" are symbols, not conIds. Only an int argument is a conId.
        # (The old `isnumeric() → conId` branch misresolved numeric tickers, the
        #  exact conId-vs-ticker confusion the precision principle warns about.)
        for definition in self.security_definitions:
            if isinstance(symbol, str):
                if definition.symbol == symbol:
                    return definition
            else:
                if definition.conId == symbol:
                    return definition
        return None


class UniverseAccessor():
    def __init__(self, duckdb_path: str, universe_library: str):
        self.duckdb_path = duckdb_path
        self.universe_library = universe_library
        if duckdb_path and (
            duckdb_path.endswith('.duckdb')
            or duckdb_path.startswith('/')
            or duckdb_path.startswith('~')
        ):
            db_path = duckdb_path
        else:
            db_path = _default_db_path()
        self.library = DuckDBObjectStore(db_path)
        # reverse order
        self.sorted_names = ['LSE', 'ASX', 'NYSE', 'NASDAQ']
        self.sorted_types = ['STK', 'OPT', 'FUT', 'EFT']

        # Cache complete query results: neither first_only nor a previous
        # symbol lookup may hide a conflicting definition of the same conId.
        self._resolver_cache: Dict[tuple, Tuple[Tuple[Universe, SecurityDefinition], ...]] = {}
        self._resolver_lock = threading.RLock()

    def list_universes(self) -> List[str]:
        result = [k for k in self.library.list_symbols() if not k.startswith('_')]
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
        data = self.library.read(name)
        if data is None:
            universe = Universe(name)
            universe.security_definitions = []
            return universe
        if not isinstance(data, Universe):
            return None
        return data

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
        # Keep numeric ticker strings distinct from integer contract IDs.
        if type(symbol) is str and '.' in symbol and not exchange:
            symbol, exchange = symbol.split('.')
        key = (type(symbol), symbol, exchange, universe, sec_type)
        # A concurrent update cannot invalidate then be overwritten by the
        # result of a query that began against the old local catalogue.
        with self._resolver_lock:
            cached = self._resolver_cache.get(key)
            if cached is None:
                results = []
                universes = [self.get(universe)] if universe else self.get_all()
                for u in universes:
                    for definition in u.security_definitions:
                        matches = ((type(symbol) is int and symbol == definition.conId)
                                   or (type(symbol) is str and symbol == definition.symbol))
                        if (matches
                                and (not exchange or exchange in (definition.exchange, definition.primaryExchange))
                                and (not sec_type or sec_type == definition.secType)):
                            results.append((u, definition))
                results.sort(key=lambda row: self.sorted_types.index(row[1].secType)
                             if row[1].secType in self.sorted_types else len(self.sorted_types))
                cached = tuple(results)
                SecurityDefinition.validate_multiplier_consistency([d for _, d in cached])
                # Cache HITS only. An empty result is "not in the catalogue
                # RIGHT NOW", and this accessor cannot see writes made through
                # another accessor or the object store directly (it only
                # invalidates on its own mutations). Caching the miss made a
                # symbol added by another process unresolvable here for the
                # life of the process; re-querying a miss costs one catalogue
                # read and is always correct.
                if cached:
                    self._resolver_cache[key] = cached
            # Validate before truncation, on a cache hit as well as a miss.
            SecurityDefinition.validate_multiplier_consistency([d for _, d in cached])
            return list(cached[:1] if first_only else cached)

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
        # Ordered deduplication keeps cold and warm venue preference stable.
        return list(dict.fromkeys(definition for _, definition in
                    self.resolve_universe(symbol, exchange, universe, sec_type, first_only)))

    def invalidate_resolver_cache(self) -> None:
        """Forget complete query results after changes made through this accessor.

        Other accessors or direct object-store writers must also invalidate
        their caches (or restart); this is an in-process resolution guarantee.
        """
        with self._resolver_lock:
            self._resolver_cache.clear()

    def update(self, universe: Universe) -> None:
        with self._resolver_lock:
            self.library.write(universe.name, universe)
            self.invalidate_resolver_cache()

    def insert(self, universe_name: str, security_definition: SecurityDefinition):
        with self._resolver_lock:
            universe = self.get(universe_name)
            universe.security_definitions.append(security_definition)
            self.update(universe)

    def delete(self, name: str) -> None:
        with self._resolver_lock:
            self.library.delete(name)
            self.invalidate_resolver_cache()

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
