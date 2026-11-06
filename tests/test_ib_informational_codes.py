"""10349 'Order TIF was set to DAY based on order preset' must not kill a live order.

Field incident 2026-11-05: ib_async treated 10349 as a rejection and marked the
order Cancelled locally; the CLI reported `status: Cancelled` for a SELL that
filled. trader.listeners.ib_informational reclassifies it.
"""
from ib_async import IB, Stock
from ib_async.order import MarketOrder, OrderStatus, Trade

from trader.listeners import ib_informational


def _ib_with_trade(status='PreSubmitted'):
    ib = IB()
    ib_informational.install(ib)
    w = ib.wrapper
    order = MarketOrder('SELL', 1, orderId=42)
    trade = Trade(Stock('F', 'SMART', 'USD'), order, OrderStatus(orderId=42, status=status))
    w.trades[(w.clientId, 42)] = trade
    return ib, trade


def test_10349_keeps_order_live_and_is_logged():
    ib, trade = _ib_with_trade()
    seen = []
    ib.errorEvent += lambda *a: seen.append(a)
    ib.wrapper.error(42, 10349, 'Order TIF was set to DAY based on order preset.', '')
    assert trade.orderStatus.status == 'PreSubmitted'
    assert trade.log and trade.log[-1].errorCode == 10349
    assert seen and seen[-1][1] == 10349


def test_real_rejection_still_cancels():
    ib, trade = _ib_with_trade()
    ib.wrapper.error(42, 201, 'Order rejected - reason: insufficient funds', '')
    assert trade.orderStatus.status == 'Cancelled'


def test_install_is_idempotent():
    ib = IB()
    ib_informational.install(ib)
    first = ib.wrapper.error
    ib_informational.install(ib)
    assert ib.wrapper.error is first
