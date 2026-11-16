"""IB message classification — the ONE place MMR decides how serious an IB code is.

Two related jobs live here:

1. ``ib_log_level(code, message)``: the log level MMR uses for an IB
   ``errorEvent`` message (``IBAIORx`` and ``IBHistoryWorker`` both route
   through it). IB delivers status chatter, warnings and real failures over
   the same ``error()`` callback; logging them all at ERROR (as before
   2026-11-05) buried the real failures and made every ``grep ::ERROR::``
   health scan noisy. The tables below are the whole policy.
2. ``install(ib)``: reclassify informational ORDER messages inside ib_async's
   wrapper so they don't cancel a live order (below).
3. ``quiet_handled_errors(ib)``: stop ib_async logging its own
   ``Error <code>, reqId ...`` line on an IB instance whose ``errorEvent`` MMR
   handles and logs (MMR's line carries the classification, so the raw line
   was a duplicate of every IB error).

Teach ib_async which IB order messages are informational, not fatal.

ib_async's ``Wrapper.error`` treats every order-scoped code that is not in its
small warning list (105/110/165/321/... and 2100-2199) as a rejection: it sets
the local trade to ``Cancelled`` and fires ``cancelledEvent`` — while IB keeps
working the order. Field incident 2026-11-05: ``mmr sell F --market`` got
10349 "Order TIF was set to DAY based on order preset", ib_async marked the
order Cancelled, the CLI reported ``status: Cancelled``, and the order filled.

For the codes below we record the message in ``trade.log`` and emit
``errorEvent`` (so MMR's own error handling still sees it), but leave the
order's status to IB's subsequent orderStatus messages.
"""

from typing import Any

import logging
import re

from ib_async.objects import TradeLogEntry

# IB order messages that describe an adjustment IB made, not a rejection.
INFORMATIONAL_ORDER_CODES = frozenset({
    10349,  # Order TIF was set to DAY based on order preset
})

# ---------------------------------------------------------------------------
# Log-level policy for IB errorEvent messages.
# ---------------------------------------------------------------------------

# "Connection is OK / inactive but available on demand" — routine chatter on
# every connect and farm reconnect. DEBUG. IBAIORx also does NOT forward these
# to its error_subject (they never signalled anything to a caller).
FARM_OK_CODES = frozenset({
    2104,  # Market data farm connection is OK
    2106,  # HMDS data farm connection is OK
    2107,  # HMDS data farm connection is inactive but should be available upon demand
    2158,  # Sec-def data farm connection is OK
})

# Per-farm status transitions (broken / connecting / inactive). Status, not
# errors: other farms stay up and trading continues (AUDIT G2). INFO, logged as
# "ibrx farm status:" — scripts/monitor_health.sh greps that phrase.
FARM_STATUS_CODES = frozenset({
    2103,  # Market data farm connection is broken
    2105,  # HMDS data farm connection is broken
    2108,  # Market data farm connection is inactive but should be available upon demand
    2119,  # Market data farm is connecting
    2157,  # Sec-def data farm connection is broken
})

# Informational: IB adjusted or acknowledged something; nothing to act on.
INFO_CODES = frozenset({
    2109,  # Outside Regular Trading Hours attribute ignored for this order type/destination
}) | INFORMATIONAL_ORDER_CODES

# Expected confirmations. DEBUG.
DEBUG_CODES = frozenset({
    202,   # Order cancelled — IB reports a requested cancel as an "error"
})

# User-actionable but expected: the request fails for a reason the operator can
# fix (buy the subscription). Not an MMR fault. WARNING.
WARNING_CODES = frozenset({
    10089,  # Requested market data requires additional subscription for API
    310,    # Can't find the subscribed market depth
})

# Code + message-substring cases: the same code also carries real failures.
_INFO_MESSAGES = (
    # 162 is IB's catch-all "Historical Market Data Service error"; only the
    # cancel acknowledgement after a successful scan is benign — "Market
    # Scanner is not configured for one of the chosen locations" and HMDS
    # failures stay ERROR.
    (162, 'API scanner subscription cancelled'),
    # 365 follows a cancelScannerSubscription for a scan IB already ended.
    (365, 'No scanner subscription found'),
    # 10148 answers a cancel for an order that is ALREADY cancelled. The legs
    # of a bracket are one OCA group: cancelling the take-profit cancels the
    # stop at IB, and the cancel MMR then sends for the stop gets this. The
    # order is in the state that was asked for (found 2026-11-16: one ERROR
    # line, and an errors_<ts>.log entry, for every bracket that was closed).
    # Any other state - above all "state: Filled", a cancel that lost the
    # race with a fill - stays ERROR.
    (10148, 'cannot be cancelled, state: Cancelled'),
)

# Same idea at WARNING: worth seeing, not a failure.
_WARNING_MESSAGES = (
    # 399 is IB's generic "order message". This text is its notice that an
    # order sent outside trading hours is HELD until the venue opens — it was
    # accepted and is not yet working. Every out-of-hours placement produced an
    # ERROR line (and an errors_<ts>.log entry the health monitor escalates);
    # found 2026-11-14 placing a bracket on a Saturday. Other 399 texts stay
    # ERROR. It stays visible because a held EXIT is not protection.
    (399, 'will not be placed at the exchange until'),
)


def ib_log_level(code: int, message: str = '') -> int:
    """Return the ``logging`` level MMR uses for an IB errorEvent message.

    Args:
        code: IB error/message code.
        message: IB message text (needed for codes whose meaning depends on it).

    Returns:
        ``logging.DEBUG`` / ``INFO`` / ``WARNING`` / ``ERROR``. Anything not
        classified here is a real failure: ERROR — except the rest of IB's
        documented 2100-2199 warning range, which is WARNING (ib_async treats
        that range as warnings too), and 399's "held until the venue opens"
        notice (``_WARNING_MESSAGES``).
    """
    if code in FARM_OK_CODES or code in DEBUG_CODES:
        return logging.DEBUG
    if code in FARM_STATUS_CODES or code in INFO_CODES:
        return logging.INFO
    for info_code, needle in _INFO_MESSAGES:
        if code == info_code and needle in (message or ''):
            return logging.INFO
    if code in WARNING_CODES or 2100 <= code < 2200:
        return logging.WARNING
    for warning_code, needle in _WARNING_MESSAGES:
        if code == warning_code and needle in (message or ''):
            return logging.WARNING
    return logging.ERROR


# ---------------------------------------------------------------------------
# What status to REPORT for an order ib_async has flagged with a warning.
# ---------------------------------------------------------------------------

# ib_async records an IB *warning* about an order by setting the trade's local
# status to this value until the next orderStatus callback ("the order is
# STILL LIVE at the broker", ib_async.wrapper.error).
ORDER_WARNING_STATUS = 'ValidationError'


def order_status_and_warnings(trade: Any) -> tuple:
    """``(status to report, [IB messages about the order])`` for a Trade.

    Every MARKET or STOP order this system sends carries the "outside regular
    trading hours" attribute, IB answers with warning 2109 ("attribute ...
    is ignored based on the order type") and ib_async flips the trade's status
    to ``ValidationError`` for the moment in which the CLI reads it. So `buy`,
    `sell` and `close` answered ``"status": "ValidationError"`` beside
    ``"success": true`` for orders IB had accepted and went on to fill (found
    live 2026-11-16) - a word that reads as a failure, to an operator and to an
    agent alike.

    When every message behind the marker is one this module classes as
    informational (DEBUG / INFO: 2109, 10349, ...), the status reported is the
    last one the broker actually gave before it, or ``PendingSubmit`` if there
    was none. When ANY message is a real warning or error - 321 "missing order
    exchange" arrived this way and the order was dead - the marker is reported
    as it is: unknown is never treated as fine. The messages are returned in
    both cases.
    """
    status = getattr(getattr(trade, 'orderStatus', None), 'status', None)
    entries = list(getattr(trade, 'log', None) or [])
    flagged = [entry for entry in entries if getattr(entry, 'status', None) == ORDER_WARNING_STATUS]
    messages = [str(getattr(entry, 'message', '') or '') for entry in flagged]
    messages = [message for message in messages if message]
    if status != ORDER_WARNING_STATUS:
        return status, messages
    benign = bool(flagged) and all(
        ib_log_level(int(getattr(entry, 'errorCode', 0) or 0), str(getattr(entry, 'message', '') or '')) <= logging.INFO
        for entry in flagged)
    if not benign:
        return status, messages
    earlier = [getattr(entry, 'status', None) for entry in entries]
    earlier = [value for value in earlier if value and value != ORDER_WARNING_STATUS]
    return (earlier[-1] if earlier else 'PendingSubmit'), messages


# ---------------------------------------------------------------------------
# De-duplicate ib_async's own error line on MMR-handled IB instances.
# ---------------------------------------------------------------------------

# The exact shape of the line ib_async.wrapper.error() logs for every message:
# f"{'Warning' if isWarning else 'Error'} {errorCode}, reqId {reqId}: ..."
_WRAPPER_ERROR_LINE = re.compile(r'^(?:Error|Warning) -?\d+, reqId -?\d+: ')


class _HandledErrorLogger:
    """Proxy for ``wrapper._logger`` that drops only the per-message
    ``Error/Warning <code>, reqId <id>: ...`` line (MMR logs that message
    itself, classified) and passes every other record through unchanged."""

    def __init__(self, inner: logging.Logger):
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    @staticmethod
    def _is_error_line(msg) -> bool:
        return isinstance(msg, str) and _WRAPPER_ERROR_LINE.match(msg) is not None

    def error(self, msg, *args, **kwargs):
        if not self._is_error_line(msg):
            self._inner.error(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        if not self._is_error_line(msg):
            self._inner.warning(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        if not self._is_error_line(msg):
            self._inner.info(msg, *args, **kwargs)


def quiet_handled_errors(ib: Any) -> None:
    """Suppress ib_async's raw error line on ``ib`` (idempotent).

    Only call this on an IB instance whose ``errorEvent`` is handled by an MMR
    handler that logs every message (``IBAIORx.__handle_error``,
    ``IBHistoryWorker.__handle_error``) — otherwise the message would go
    unlogged.
    """
    wrapper = getattr(ib, 'wrapper', None)
    if wrapper is None:
        return
    current = getattr(wrapper, '_logger', None)
    if current is None or isinstance(current, _HandledErrorLogger):
        return
    wrapper._logger = _HandledErrorLogger(current)


def install(ib: Any) -> None:
    """Wrap ``ib.wrapper.error`` once (idempotent)."""
    wrapper = getattr(ib, 'wrapper', None)
    if wrapper is None or getattr(wrapper, '_mmr_informational_installed', False):
        return
    original = wrapper.error

    def error(reqId, errorCode, errorString, advancedOrderRejectJson='', *args, **kwargs):
        if errorCode in INFORMATIONAL_ORDER_CODES and reqId != -1:
            trade = wrapper.trades.get((wrapper.clientId, reqId))
            if trade is not None and not trade.isDone():
                trade.log.append(TradeLogEntry(
                    wrapper.lastTime, trade.orderStatus.status,
                    f'Info {errorCode}: {errorString}', errorCode))
                contract = getattr(wrapper, '_reqId2Contract', {}).get(reqId)
                ib.errorEvent.emit(reqId, errorCode, errorString, contract)
                return None
        return original(reqId, errorCode, errorString, advancedOrderRejectJson, *args, **kwargs)

    wrapper.error = error
    wrapper._mmr_informational_installed = True
