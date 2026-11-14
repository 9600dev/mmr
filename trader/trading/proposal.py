from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import datetime as dt
import math


class ProposalStatus(str, Enum):
    PENDING = 'PENDING'
    APPROVED = 'APPROVED'
    EXECUTED = 'EXECUTED'
    REJECTED = 'REJECTED'
    EXPIRED = 'EXPIRED'
    FAILED = 'FAILED'


class OrderType(str, Enum):
    MARKET = 'MARKET'
    LIMIT = 'LIMIT'


class ExitType(str, Enum):
    NONE = 'NONE'
    BRACKET = 'BRACKET'
    STOP_LOSS = 'STOP_LOSS'
    TRAILING_STOP = 'TRAILING_STOP'


def _price(value: Any) -> Optional[float]:
    """``value`` as a float when it is a finite positive number, else ``None``."""
    try:
        if (isinstance(value, (float, int)) and not isinstance(value, bool)
                and math.isfinite(value) and value > 0):
            return float(value)
    except (TypeError, ValueError):
        pass
    return None


def exit_geometry_errors(
    action: str,
    order_type: str,
    limit_price: Optional[float],
    exit_type: str,
    take_profit_price: Optional[float] = None,
    stop_loss_price: Optional[float] = None,
    trailing_stop_percent: Optional[float] = None,
    trailing_stop_amount: Optional[float] = None,
    reference_price: Optional[float] = None,
) -> List[str]:
    """Exit legs that sit on the WRONG SIDE of the entry. Pure; no I/O.

    A protective stop at or beyond the entry price is not protection: it
    triggers the moment the entry fills and sells (or covers) at market. A
    take-profit at or inside the entry is the same thing in limit form. Both
    were accepted until 2026-11-14 (`propose F BUY --limit 5 --stop-loss 50`,
    `--bracket 3 20` on a BUY @ 5 — found live on paper): the spec only
    checked that each price was finite and positive, never how the prices
    relate to each other or to the direction of the trade.

    The entry reference is the limit price for a LIMIT entry (a buy limit
    fills at or BELOW it, a sell limit at or ABOVE it, so a stop on the wrong
    side of the limit is on the wrong side of every possible fill). A MARKET
    entry has no price of its own; ``reference_price`` (a current quote) is
    used when the caller has one, and when it has none only the bracket's
    internal ordering can be checked. Unknown is never treated as wrong.
    """
    side = (action or '').upper()
    if side not in ('BUY', 'SELL') or exit_type in (None, '', 'NONE'):
        return []
    long_entry = side == 'BUY'
    errors: List[str] = []

    entry: Optional[float] = None
    entry_label = ''
    limit = _price(limit_price) if order_type == 'LIMIT' else None
    reference = _price(reference_price)
    if limit is not None:
        entry, entry_label = limit, f'limit entry {limit:g}'
    elif reference is not None:
        entry, entry_label = reference, f'current price {reference:g}'

    stop = _price(stop_loss_price)
    target = _price(take_profit_price)
    uses_stop = exit_type in ('BRACKET', 'STOP_LOSS')
    uses_target = exit_type == 'BRACKET'

    if uses_stop and stop is not None and entry is not None:
        if long_entry and stop >= entry:
            errors.append(
                f'stop_loss_price {stop:g} is at or above the {entry_label} for a BUY — '
                f'it would trigger as soon as the entry fills')
        if not long_entry and stop <= entry:
            errors.append(
                f'stop_loss_price {stop:g} is at or below the {entry_label} for a SELL — '
                f'it would trigger as soon as the entry fills')
    if uses_target and target is not None and entry is not None:
        if long_entry and target <= entry:
            errors.append(
                f'take_profit_price {target:g} is at or below the {entry_label} for a BUY — '
                f'it would exit immediately at no profit')
        if not long_entry and target >= entry:
            errors.append(
                f'take_profit_price {target:g} is at or above the {entry_label} for a SELL — '
                f'it would exit immediately at no profit')
    if uses_stop and uses_target and stop is not None and target is not None and not errors:
        if long_entry and stop >= target:
            errors.append(
                f'bracket is inverted for a BUY: stop_loss_price {stop:g} must be below '
                f'take_profit_price {target:g}')
        if not long_entry and stop <= target:
            errors.append(
                f'bracket is inverted for a SELL: stop_loss_price {stop:g} must be above '
                f'take_profit_price {target:g}')

    if exit_type == 'TRAILING_STOP':
        trail_pct = _price(trailing_stop_percent)
        trail_amt = _price(trailing_stop_amount)
        if trail_pct is not None and trail_pct >= 100:
            errors.append(
                f'trailing_stop_percent {trail_pct:g} must be below 100 — '
                f'a trail that wide can never trigger')
        if long_entry and entry is not None and trail_amt is not None and trail_amt >= entry:
            errors.append(
                f'trailing_stop_amount {trail_amt:g} is not below the {entry_label} — '
                f'the stop would sit at or below zero and never trigger')
    return errors


def protective_trigger_error(
    action: str,
    order_type: str,
    trigger: Optional[float],
    market_price: Optional[float],
) -> Optional[str]:
    """Why a STANDALONE protective order would execute at once, or ``None``.

    ``mmr protect IOVA --stop 50`` on a stock trading at 7.72 was accepted on
    2026-11-14 (paper): a SELL stop above the market is marketable the instant
    it is live, so the "protection" liquidates the position at the next print.
    The disaster stop the executor places is pinned against exactly this in
    ``protective_stop.py`` (never at or above entry); the manual path had no
    equivalent.

    ``order_type`` is 'STP', 'LMT' (take-profit) or 'TRAIL' (``trigger`` is the
    trailing percent). An unknown ``market_price`` returns a refusal too: the
    side cannot be judged, and guessing either way is how the position is
    lost or left believing it is protected. The caller offers the override.
    """
    side = (action or '').upper()
    kind = (order_type or '').upper()
    if side not in ('BUY', 'SELL'):
        return f'action must be BUY or SELL, got {action!r}'
    value = _price(trigger)
    if value is None:
        return f'{kind} value must be a finite positive number, got {trigger!r}'
    if kind == 'TRAIL':
        if value >= 100:
            return f'trailing percent {value:g} must be below 100 — a trail that wide can never trigger'
        return None
    if kind not in ('STP', 'LMT'):
        return f'unsupported protective order type {order_type!r}'
    market = _price(market_price)
    if market is None:
        return (f'no current market price to check the {kind} against — cannot tell whether '
                f'{value:g} would execute immediately')
    protects_long = side == 'SELL'
    if kind == 'STP':
        if protects_long and value >= market:
            return (f'SELL stop {value:g} is at or above the market ({market:g}) — it would '
                    f'trigger immediately and sell at market; a protective stop for a long '
                    f'sits BELOW the price')
        if not protects_long and value <= market:
            return (f'BUY stop {value:g} is at or below the market ({market:g}) — it would '
                    f'trigger immediately and buy at market; a protective stop for a short '
                    f'sits ABOVE the price')
        return None
    if protects_long and value <= market:
        return (f'SELL take-profit {value:g} is at or below the market ({market:g}) — it would '
                f'fill immediately; a take-profit for a long sits ABOVE the price')
    if not protects_long and value >= market:
        return (f'BUY take-profit {value:g} is at or above the market ({market:g}) — it would '
                f'fill immediately; a take-profit for a short sits BELOW the price')
    return None


@dataclass
class ExecutionSpec:
    """Declarative order specification — serialized as dict over RPC,
    translated to IB Order(s) server-side."""

    # Entry
    order_type: str = 'MARKET'
    limit_price: Optional[float] = None

    # Exit strategy
    exit_type: str = 'NONE'
    take_profit_price: Optional[float] = None
    stop_loss_price: Optional[float] = None
    trailing_stop_percent: Optional[float] = None
    trailing_stop_amount: Optional[float] = None

    # Order parameters
    tif: str = 'DAY'
    outside_rth: bool = True
    good_till_date: Optional[str] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, d: dict, *, ignore_unknown: bool = False) -> 'ExecutionSpec':
        """Strict at the RPC boundary; ``ignore_unknown=True`` for stored rows.

        A proposal persisted by a newer build can carry a field this build
        does not know. Refusing to load it made every proposal listing fail
        after a rollback; the store drops such fields (and says so) instead.
        """
        if not isinstance(d, dict):
            raise ValueError('execution spec must be a mapping')
        unknown = set(d) - set(cls.__dataclass_fields__)
        if unknown and not ignore_unknown:
            raise ValueError(f'unknown execution fields: {sorted(unknown)}')
        return cls(**{key: value for key, value in d.items() if key not in unknown})

    def validate(self, action: Optional[str] = None,
                 reference_price: Optional[float] = None) -> List[str]:
        """Return a list of validation errors (empty if valid).

        With ``action`` the exit legs are also checked against the direction
        of the trade (see ``exit_geometry_errors``); without it only the
        action-independent rules apply, as before.
        """
        errors = []
        if self.order_type not in {member.value for member in OrderType}:
            errors.append(f'unsupported order_type {self.order_type!r}')
        if self.exit_type not in {member.value for member in ExitType}:
            errors.append(f'unsupported exit_type {self.exit_type!r}')
        if self.tif not in {'DAY', 'GTC', 'IOC', 'FOK', 'GTD', 'OPG'}:
            errors.append(f'unsupported time in force {self.tif!r}')
        if self.tif == 'GTD' and not self.good_till_date:
            errors.append('GTD requires good_till_date')
        if not isinstance(self.outside_rth, bool):
            errors.append('outside_rth must be a boolean')
        for name in ('limit_price', 'take_profit_price', 'stop_loss_price',
                     'trailing_stop_percent', 'trailing_stop_amount'):
            value = getattr(self, name)
            if value is not None:
                try:
                    valid = (isinstance(value, (float, int)) and not isinstance(value, bool)
                             and math.isfinite(value) and value > 0)
                except (TypeError, ValueError):
                    valid = False
                if not valid:
                    errors.append(f'{name} must be finite and positive')
        if self.trailing_stop_percent is not None and self.trailing_stop_amount is not None:
            errors.append('specify only one trailing stop distance')
        if self.order_type == 'LIMIT' and self.limit_price is None:
            errors.append('LIMIT order requires limit_price')
        if self.exit_type == 'BRACKET':
            if self.take_profit_price is None:
                errors.append('BRACKET exit requires take_profit_price')
            if self.stop_loss_price is None:
                errors.append('BRACKET exit requires stop_loss_price')
        if self.exit_type == 'STOP_LOSS' and self.stop_loss_price is None:
            errors.append('STOP_LOSS exit requires stop_loss_price')
        if self.exit_type == 'TRAILING_STOP':
            if self.trailing_stop_percent is None and self.trailing_stop_amount is None:
                errors.append('TRAILING_STOP exit requires trailing_stop_percent or trailing_stop_amount')
            if self.order_type == 'MARKET':
                errors.append('TRAILING_STOP exit cannot be attached to a MARKET order (IB requires LIMIT or STOP-LIMIT parent)')
        if action is not None and not errors:
            errors.extend(exit_geometry_errors(
                action, self.order_type, self.limit_price, self.exit_type,
                take_profit_price=self.take_profit_price,
                stop_loss_price=self.stop_loss_price,
                trailing_stop_percent=self.trailing_stop_percent,
                trailing_stop_amount=self.trailing_stop_amount,
                reference_price=reference_price))
        return errors


@dataclass
class TradeProposal:
    """A proposed trade with execution specification and reasoning."""

    symbol: str
    action: str                          # 'BUY' or 'SELL'
    quantity: Optional[float] = None
    amount: Optional[float] = None
    execution: ExecutionSpec = field(default_factory=ExecutionSpec)
    reasoning: str = ''
    confidence: float = 0.0
    thesis: str = ''
    source: str = 'manual'
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Lifecycle (set by ProposalStore)
    id: Optional[int] = None
    status: str = 'PENDING'
    created_at: Optional[dt.datetime] = None
    updated_at: Optional[dt.datetime] = None
    order_ids: List[int] = field(default_factory=list)
    rejection_reason: str = ''
    sec_type: str = 'STK'
    exchange: str = ''     # Exchange hint (e.g. 'ASX', 'TSE', 'SEHK')
    currency: str = ''     # Currency hint (e.g. 'AUD', 'JPY', 'HKD')
    group: str = ''        # Position group name (e.g. 'mining', 'tech')
