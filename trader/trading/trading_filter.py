"""Trading filters — allowlist/denylist for symbols, exchanges, and instrument types."""

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import yaml


_DEFAULT_CONFIG_NAME = 'trading_filters.yaml'


class TradingFilterError(Exception):
    """Raised when a trading-filter config exists but cannot be parsed.

    We fail loudly rather than degrade to an empty allow-everything filter — a
    silently-dropped denylist would let orders through that the operator
    explicitly meant to block.
    """


# Routing destinations: where an order is SENT, never where an instrument lists.
_ROUTERS = frozenset({'SMART', 'IBKRATS', 'BEST'})


def _string_list(data: dict, key: str, filepath) -> list:
    """``data[key]`` as a list of strings, or raise.

    A hand-edited scalar (``denylist: AAPL``) used to be iterated character by
    character — the entry silently blocked nothing — and a bare number
    (``[700]``) raised AttributeError on every order instead of at load.
    """
    value = data.get(key)
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise TradingFilterError(
            f'{filepath}: `{key}` must be a list of strings (quote numeric tickers), '
            f'got {value!r}')
    return list(value)


def _default_path() -> Path:
    from trader.container import ensure_config_dir
    return ensure_config_dir() / _DEFAULT_CONFIG_NAME


@dataclass
class TradingFilter:
    denylist: list[str] = field(default_factory=list)
    allowlist: list[str] = field(default_factory=list)
    exchanges: list[str] = field(default_factory=list)
    deny_exchanges: list[str] = field(default_factory=list)
    sec_types: list[str] = field(default_factory=list)
    deny_sec_types: list[str] = field(default_factory=list)
    locations: list[str] = field(default_factory=list)
    deny_locations: list[str] = field(default_factory=list)
    min_price: float = 0.0

    def is_allowed(
        self,
        symbol: str,
        exchange: str = '',
        sec_type: str = '',
        price: float = 0.0,
        location: str = '',
        primary_exchange: str = '',
    ) -> tuple[bool, str]:
        """Check if an instrument passes all filter rules. Returns (allowed, reason).

        ``exchange`` on an order is usually the ROUTING destination (SMART),
        not where the instrument is listed, so the exchange rules are judged
        against the listing venue: ``primary_exchange`` when given, plus
        ``exchange`` when it names a real venue. Before 2026-11-14 only
        ``exchange`` was checked, so ``deny_exchanges: [ASX]`` blocked nothing
        that routed SMART and an ``exchanges`` allowlist had to contain SMART —
        which then admitted every SMART-routable venue.
        """
        sym = symbol.upper()

        # Exclusive allowlist
        if self.allowlist and sym not in {s.upper() for s in self.allowlist}:
            return False, f'{sym} not in allowlist'

        # Denylist
        if sym in {s.upper() for s in self.denylist}:
            return False, f'{sym} is on the denylist'

        # Exchange checks — against the listing venue(s), see docstring.
        routed = str(exchange or '').upper()
        primary = str(primary_exchange or '').upper()
        venues = [v for v in dict.fromkeys((primary, routed)) if v and v not in _ROUTERS]
        if not venues and routed:
            venues = [routed]            # only a router is known: judge that
        if venues:
            denied = {e.upper() for e in self.deny_exchanges}
            for venue in venues:
                if venue in denied:
                    return False, f'exchange {venue} is denied'
            if self.exchanges:
                allowed = {e.upper() for e in self.exchanges}
                if not any(venue in allowed for venue in venues):
                    return False, f'exchange {venues[0]} not in allowed exchanges'

        # SecType checks
        if sec_type:
            st = sec_type.upper()
            if self.deny_sec_types and st in {t.upper() for t in self.deny_sec_types}:
                return False, f'sec_type {st} is denied'
            if self.sec_types and st not in {t.upper() for t in self.sec_types}:
                return False, f'sec_type {st} not in allowed types'

        # Location checks (e.g. STK.CA, STK.AU.ASX)
        if location:
            loc = location.upper()
            if self.deny_locations and loc in {l.upper() for l in self.deny_locations}:
                return False, f'location {loc} is denied'
            if self.locations and loc not in {l.upper() for l in self.locations}:
                return False, f'location {loc} not in allowed locations'

        # Price floor
        if price > 0 and self.min_price > 0 and price < self.min_price:
            return False, f'price {price} below minimum {self.min_price}'

        return True, ''

    def is_empty(self) -> bool:
        """True if no filters are configured."""
        return (
            not self.denylist
            and not self.allowlist
            and not self.exchanges
            and not self.deny_exchanges
            and not self.sec_types
            and not self.deny_sec_types
            and not self.locations
            and not self.deny_locations
            and self.min_price <= 0
        )

    @staticmethod
    def default() -> 'TradingFilter':
        return TradingFilter()

    @staticmethod
    def load(path: Optional[str] = None) -> 'TradingFilter':
        """Load from YAML file. Returns default if file doesn't exist."""
        filepath = Path(path) if path else _default_path()
        # A missing file legitimately means "no filter configured" → default.
        if not filepath.exists():
            return TradingFilter.default()
        # A file that EXISTS but won't parse is an operator error, not an
        # invitation to trade with no denylist. Fail loudly.
        try:
            with open(filepath) as f:
                data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                raise TradingFilterError(
                    f'{filepath}: expected a mapping, got {type(data).__name__}')
            return TradingFilter(
                denylist=_string_list(data, 'denylist', filepath),
                allowlist=_string_list(data, 'allowlist', filepath),
                exchanges=_string_list(data, 'exchanges', filepath),
                deny_exchanges=_string_list(data, 'deny_exchanges', filepath),
                sec_types=_string_list(data, 'sec_types', filepath),
                deny_sec_types=_string_list(data, 'deny_sec_types', filepath),
                locations=_string_list(data, 'locations', filepath),
                deny_locations=_string_list(data, 'deny_locations', filepath),
                min_price=float(data.get('min_price', 0.0)),
            )
        except TradingFilterError:
            raise
        except Exception as ex:
            raise TradingFilterError(
                f'failed to parse trading filter {filepath}: {ex}') from ex

    def save(self, path: Optional[str] = None) -> None:
        """Save to YAML file."""
        filepath = Path(path) if path else _default_path()
        filepath.parent.mkdir(parents=True, exist_ok=True)
        content = (
            '# Trading Filters — allowlist/denylist for symbols, exchanges, and instrument types.\n'
            '# Edit this file or use the `filter` CLI command.\n\n'
        )
        content += yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)
        with open(filepath, 'w') as f:
            f.write(content)

    def to_dict(self) -> dict:
        return asdict(self)


class ReloadingTradingFilter:
    """The on-disk filter, re-read whenever the file changes.

    trader_service loaded the filter ONCE at connect. ``mmr filter deny NKLA``
    wrote the YAML, the CLI reported it and the ideas scanner honoured it, but
    the order path kept checking the object from startup: approvals and
    auto-executor opens in the denied name went through until the service was
    restarted. The filter is a control the operator reaches for mid-session;
    it has to take effect when it is saved.

    Each check stats the file (cheap) and reloads on a changed mtime/size. A
    file that no longer parses keeps the LAST GOOD filter and logs an error —
    a broken edit must not silently turn the denylist off. A deleted file
    means no filter is configured, as at startup.
    """

    def __init__(self, path: Optional[str] = None):
        import logging
        self._log = logging.getLogger(__name__)
        self._path = Path(path) if path else _default_path()
        self._stamp: Optional[tuple] = None
        self._filter = TradingFilter.default()
        self._broken: Optional[tuple] = None
        self._stamp = self._file_stamp()
        self._filter = TradingFilter.load(str(self._path))    # raises at startup, as before

    def _file_stamp(self) -> Optional[tuple]:
        try:
            stat = self._path.stat()
        except OSError:
            return None
        return (stat.st_mtime_ns, stat.st_size)

    def current(self) -> TradingFilter:
        stamp = self._file_stamp()
        if stamp != self._stamp:
            try:
                self._filter = TradingFilter.load(str(self._path))
                self._stamp = stamp
                self._broken = None
                self._log.warning('trading filter reloaded from %s', self._path)
            except TradingFilterError as ex:
                if self._broken != stamp:
                    self._broken = stamp
                    self._log.error(
                        'trading filter %s changed but does not parse (%s) — keeping the '
                        'previously loaded filter in force', self._path, ex)
        return self._filter

    def is_allowed(self, *args, **kwargs) -> tuple[bool, str]:
        return self.current().is_allowed(*args, **kwargs)

    def __getattr__(self, name: str):
        # Everything else (to_dict, is_empty, the list fields) reads through.
        # Private names never do: a half-built instance (copy, unpickle) would
        # otherwise recurse looking for its own _stamp.
        if name.startswith('_'):
            raise AttributeError(name)
        return getattr(self.current(), name)
