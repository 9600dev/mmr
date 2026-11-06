"""Teach ib_async which IB order messages are informational, not fatal.

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

from ib_async.objects import TradeLogEntry

# IB order messages that describe an adjustment IB made, not a rejection.
INFORMATIONAL_ORDER_CODES = frozenset({
    10349,  # Order TIF was set to DAY based on order preset
})


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
