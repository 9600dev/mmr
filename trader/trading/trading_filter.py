"""Trading filters — allowlist/denylist for symbols, exchanges, and instrument types."""

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import yaml


_DEFAULT_CONFIG_NAME = 'trading_filters.yaml'


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
    ) -> tuple[bool, str]:
        """Check if an instrument passes all filter rules. Returns (allowed, reason)."""
        sym = symbol.upper()

        # Exclusive allowlist
        if self.allowlist and sym not in {s.upper() for s in self.allowlist}:
            return False, f'{sym} not in allowlist'

        # Denylist
        if sym in {s.upper() for s in self.denylist}:
            return False, f'{sym} is on the denylist'

        # Exchange checks
        if exchange:
            ex = exchange.upper()
            if self.deny_exchanges and ex in {e.upper() for e in self.deny_exchanges}:
                return False, f'exchange {ex} is denied'
            if self.exchanges and ex not in {e.upper() for e in self.exchanges}:
                return False, f'exchange {ex} not in allowed exchanges'

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
        if not filepath.exists():
            return TradingFilter.default()
        try:
            with open(filepath) as f:
                data = yaml.safe_load(f) or {}
            return TradingFilter(
                denylist=data.get('denylist') or [],
                allowlist=data.get('allowlist') or [],
                exchanges=data.get('exchanges') or [],
                deny_exchanges=data.get('deny_exchanges') or [],
                sec_types=data.get('sec_types') or [],
                deny_sec_types=data.get('deny_sec_types') or [],
                locations=data.get('locations') or [],
                deny_locations=data.get('deny_locations') or [],
                min_price=float(data.get('min_price', 0.0)),
            )
        except Exception:
            return TradingFilter.default()

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
