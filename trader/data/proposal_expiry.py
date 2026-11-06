"""Proposal expiry — the age rule for PENDING trade proposals.

A PENDING proposal carries the prices, sizing and reasoning of the moment it was
written. After enough time that context is wrong: a MARKET proposal approved
five weeks late executes at whatever the market is now, sized from a snapshot
that no longer describes the book. So a PENDING proposal has a time-to-live
(``proposal_ttl_hours`` in trader.yaml, default 24h). Past it, the proposal is
moved PENDING -> EXPIRED (an edge the state machine already allows) and can no
longer be approved; re-propose with fresh prices instead.

Only PENDING rows expire. An APPROVED row may already have an order at the
broker (an interrupted approve, resumable with ``approve --resume``), so ageing
it out would rewrite the audit trail of something that might have traded;
stale APPROVED rows are REPORTED (``stale_approved``) for an operator to
resolve, never changed automatically.

Pure: no I/O. ``ProposalStore.expire_stale`` and the approve path use these.
"""

import datetime as dt
from typing import Any, Mapping, Optional

DEFAULT_PROPOSAL_TTL_HOURS = 24.0


def proposal_ttl(hours: Any) -> Optional[dt.timedelta]:
    """Normalise a configured TTL in hours to a timedelta.

    ``None`` / missing -> the default (24h). ``0`` or negative -> expiry
    disabled (returns None). Anything non-numeric raises ValueError: a typo in
    a safety setting must fail loudly, not silently disable the rule.
    """
    if hours is None or hours == '':
        hours = DEFAULT_PROPOSAL_TTL_HOURS
    if isinstance(hours, bool):
        raise ValueError(f'proposal_ttl_hours must be a number of hours, got {hours!r}')
    try:
        value = float(hours)
    except (TypeError, ValueError):
        raise ValueError(f'proposal_ttl_hours must be a number of hours, got {hours!r}') from None
    if value != value or value in (float('inf'), float('-inf')):
        raise ValueError(f'proposal_ttl_hours must be finite, got {hours!r}')
    if value <= 0:
        return None
    return dt.timedelta(hours=value)


def proposal_ttl_from_config(cfg: Mapping[str, Any]) -> Optional[dt.timedelta]:
    """TTL from a trader.yaml-style mapping (key ``proposal_ttl_hours``)."""
    return proposal_ttl(cfg.get('proposal_ttl_hours', DEFAULT_PROPOSAL_TTL_HOURS))


def _naive(ts: dt.datetime) -> dt.datetime:
    # Proposal timestamps are written with dt.datetime.now() (naive local);
    # compare like with like so a tz-aware "now" can't raise or skew.
    if ts.tzinfo is not None:
        return ts.astimezone().replace(tzinfo=None)
    return ts


def is_expired(created_at: Optional[dt.datetime], now: dt.datetime,
               ttl: Optional[dt.timedelta]) -> bool:
    """True iff a proposal created at ``created_at`` is older than ``ttl`` at ``now``.

    Expiry disabled (ttl None) -> never expired. A missing created_at is
    treated as expired: an undatable proposal cannot be shown to be fresh, and
    approving on unknown-age context is exactly what this rule prevents.
    """
    if ttl is None:
        return False
    if created_at is None:
        return True
    return _naive(now) - _naive(created_at) > ttl


def describe_ttl(ttl: Optional[dt.timedelta]) -> str:
    if ttl is None:
        return 'disabled'
    return f'{ttl.total_seconds() / 3600:g}h'
