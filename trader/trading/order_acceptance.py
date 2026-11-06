"""Did the broker actually ACCEPT an order we just placed?

A placement receipt (ib_async's Trade) is not acceptance. IB validates new
orders asynchronously: a rejection such as 321 "Missing order exchange"
arrives tens of milliseconds AFTER placeOrder returned, and ib_async records
it by setting the status to ``ValidationError`` — a state it lists among the
ACTIVE states, because the same status is also used for harmless
informational codes (2109 "Outside Regular Trading Hours is ignored", any
2100-2199) on orders that go on to work normally.

Field incident 2026-11-05: ``mmr protect F --trail 5`` cancelled the existing
protective stop, the replacement was rejected with 321, and the CLI reported
success — the position was silently unprotected. This module is the verdict
the protective path now waits for before it reports anything.
"""

import asyncio
import time
from typing import Any, Iterable, Optional, Tuple

ACCEPTED = 'ACCEPTED'
REJECTED = 'REJECTED'
UNCONFIRMED = 'UNCONFIRMED'

_WORKING = {'Submitted', 'PreSubmitted', 'Filled'}
_DEAD = {'Cancelled', 'ApiCancelled', 'Inactive'}


def _is_informational(code: Optional[int]) -> bool:
    return code is not None and 2100 <= int(code) < 2200


def _hard_validation_entry(log: Iterable[Any]):
    """Last ValidationError log entry carrying a non-informational code."""
    found = None
    for entry in log or ():
        if getattr(entry, 'status', '') == 'ValidationError' and not _is_informational(
                getattr(entry, 'errorCode', None)):
            found = entry
    return found


def _ever_working(log: Iterable[Any]) -> bool:
    return any(getattr(e, 'status', '') in _WORKING for e in (log or ()))


def classify(trade: Any) -> Tuple[str, str]:
    """Instantaneous verdict for a freshly placed order.

    ACCEPTED   the broker reports it working (or filled)
    REJECTED   the broker refused it (dead state, or a hard validation error
               on an order that never reached a working state)
    UNCONFIRMED neither yet (PendingSubmit / informational warnings only)
    """
    status = str(getattr(getattr(trade, 'orderStatus', None), 'status', '') or '')
    log = getattr(trade, 'log', ()) or ()
    if status in _WORKING:
        return ACCEPTED, status
    if status in _DEAD:
        return REJECTED, _reason(log, status)
    if status == 'ValidationError':
        hard = _hard_validation_entry(log)
        if hard is not None and not _ever_working(log):
            return REJECTED, _reason(log, status)
        if _ever_working(log):
            return ACCEPTED, status
    return UNCONFIRMED, status


def _reason(log: Iterable[Any], status: str) -> str:
    for entry in reversed(list(log or ())):
        msg = getattr(entry, 'message', '')
        code = getattr(entry, 'errorCode', None)
        if msg and not _is_informational(code):
            return f'{status}: {msg}'
    return status


async def await_acceptance(trade: Any, timeout: float = 4.0,
                           poll: float = 0.05) -> Tuple[str, str]:
    """Wait up to ``timeout`` seconds for a decisive verdict (see ``classify``)."""
    deadline = time.monotonic() + timeout
    verdict = classify(trade)
    while verdict[0] == UNCONFIRMED and time.monotonic() < deadline:
        await asyncio.sleep(poll)
        verdict = classify(trade)
    return verdict
