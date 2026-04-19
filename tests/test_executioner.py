"""Targeted tests for TradeExecutioner. Exercises the risk-gate and trading-
filter gating paths that sit between a raw order request and the IB place_order
call. A full IB round-trip is out of scope; we stub the ib_async boundary.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
import reactivex as rx

from trader.trading.executioner import TradeExecutioner
from trader.trading.risk_gate import RiskGateResult
from trader.objects import Action, ContractOrderPair, ExecutorCondition


def _make_executioner_with_trader(risk_gate=None, book_orders=None, ib_account='DU1'):
    """Assemble just enough of Trader for the executioner to exercise."""
    trader = MagicMock()
    trader.ib_account = ib_account
    trader.book = MagicMock()
    trader.book.get_orders.return_value = book_orders or []
    trader.event_store = MagicMock()
    trader.client = MagicMock()
    trader.client.subscribe_place_order = AsyncMock(return_value=rx.from_iterable([MagicMock()]))
    trader.client.get_snapshot = AsyncMock(return_value=MagicMock(bid=100, ask=101))
    if risk_gate is not None:
        trader.risk_gate = risk_gate
    ex = TradeExecutioner()
    ex.connect(trader)
    return ex, trader


class _Order:
    def __init__(self, action='BUY', account='DU1'):
        self.action = action
        self.totalQuantity = 10
        self.account = account
        self.lmtPrice = 100
        self.orderId = 0

    def __str__(self):
        return f'<Order {self.action} {self.totalQuantity}>'


class _Contract:
    def __init__(self, symbol='AMD', exchange='NASDAQ', sec_type='STK', conId=1):
        self.symbol = symbol
        self.exchange = exchange
        self.secType = sec_type
        self.conId = conId
        self.multiplier = None


@pytest.mark.asyncio
async def test_trading_filter_rejection_blocks_order():
    """A denylist hit from the trading filter → no IB call, event logged."""
    class _Gate:
        def check_instrument(self, symbol, exchange, sec_type):
            return RiskGateResult(approved=False, reason=f'{symbol} is blocked')

        def check_leverage(self, *a, **kw):
            return RiskGateResult(approved=True)

        def evaluate(self, **kw):
            return RiskGateResult(approved=True)

    ex, trader = _make_executioner_with_trader(risk_gate=_Gate())
    pair = ContractOrderPair(contract=_Contract(symbol='AMD'), order=_Order())

    observable = await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)

    # Place should never reach the IB client
    assert trader.client.subscribe_place_order.call_count == 0

    # And it should emit an error observable rather than a happy path
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'trading filter' in str(errors[0]).lower() or 'blocked' in str(errors[0]).lower()


@pytest.mark.asyncio
async def test_risk_gate_rejection_blocks_order():
    """A signal-rate or daily-loss rejection should also block the place."""
    class _Gate:
        def check_instrument(self, symbol, exchange, sec_type):
            return RiskGateResult(approved=True)

        def check_leverage(self, *a, **kw):
            return RiskGateResult(approved=True)

        def evaluate(self, **kw):
            return RiskGateResult(approved=False, reason='daily loss limit')

    ex, trader = _make_executioner_with_trader(risk_gate=_Gate())
    pair = ContractOrderPair(contract=_Contract(), order=_Order())

    observable = await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    assert trader.client.subscribe_place_order.call_count == 0
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'risk gate' in str(errors[0]).lower() or 'daily loss' in str(errors[0]).lower()


@pytest.mark.asyncio
async def test_account_mismatch_rejected():
    """The executioner must reject orders with a wrong account before IB."""
    ex, trader = _make_executioner_with_trader(ib_account='DU1')

    bad_order = _Order(account='DU_OTHER')
    observable = await ex.subscribe_place_order_direct(_Contract(), bad_order)
    errors = []
    observable.subscribe(on_next=lambda _: None, on_error=errors.append)
    assert len(errors) == 1
    assert 'account' in str(errors[0]).lower()
    # Must not have reached IB
    assert trader.client.subscribe_place_order.call_count == 0


@pytest.mark.asyncio
async def test_happy_path_reaches_ib():
    """When the gates approve, place_order should reach the IB client."""
    class _Gate:
        def check_instrument(self, **kw):
            return RiskGateResult(approved=True)

        def check_leverage(self, *a, **kw):
            return RiskGateResult(approved=True)

        def evaluate(self, **kw):
            return RiskGateResult(approved=True)

    ex, trader = _make_executioner_with_trader(risk_gate=_Gate())
    pair = ContractOrderPair(contract=_Contract(), order=_Order())

    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
    assert trader.client.subscribe_place_order.call_count == 1


@pytest.mark.asyncio
async def test_skip_risk_gate_bypasses_gates():
    """skip_risk_gate=True must bypass BOTH the filter and the evaluate call."""
    class _BoomGate:
        def check_instrument(self, **kw):
            raise AssertionError('should have been skipped')

        def evaluate(self, **kw):
            raise AssertionError('should have been skipped')

    ex, trader = _make_executioner_with_trader(risk_gate=_BoomGate())
    pair = ContractOrderPair(contract=_Contract(), order=_Order())

    await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS, skip_risk_gate=True)
    assert trader.client.subscribe_place_order.call_count == 1
