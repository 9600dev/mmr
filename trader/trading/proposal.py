from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import datetime as dt


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
    def from_dict(cls, d: dict) -> 'ExecutionSpec':
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def validate(self) -> List[str]:
        """Return a list of validation errors (empty if valid)."""
        errors = []
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
