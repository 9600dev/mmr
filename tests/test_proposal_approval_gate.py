"""Tests for the require_proposal_approval gate on place_order_simple.

When the flag is on, direct buy/sell RPC calls must be rejected unless
the caller explicitly marked this as a liquidation (``skip_risk_gate=True``).
The approve() path uses place_expressive_order, which is not gated here.
"""

import asyncio

import pytest

from trader.common.reactivex import SuccessFail
from trader.messaging.trader_service_api import TraderServiceApi


class _FakeContract:
    def __init__(self, symbol='AAPL'):
        self.symbol = symbol
        self.conId = 1
        self.secType = 'STK'
        self.exchange = 'SMART'
        self.currency = 'USD'


class _FakeTrader:
    """Minimal Trader stand-in — only needs the gate flag and a
    place_order_simple stub. Real Trader has dozens of attrs."""
    def __init__(self, require_proposal_approval=False):
        self.require_proposal_approval = require_proposal_approval
        self.place_order_simple_call_count = 0

    async def place_order_simple(self, **kwargs):
        """If we get here, the gate let us through. Return an observable
        that emits a success immediately."""
        self.place_order_simple_call_count += 1
        import reactivex as rx
        return rx.of(object())  # placeholder "trade"


@pytest.mark.asyncio
async def test_gate_off_lets_direct_order_through():
    """Default config (gate off) — direct buy/sell passes through to
    trader.place_order_simple as before."""
    trader = _FakeTrader(require_proposal_approval=False)
    api = TraderServiceApi(trader)

    result = await api.place_order_simple(
        contract=_FakeContract(),
        action='BUY',
        equity_amount=None,
        quantity=10,
        limit_price=None,
        market_order=True,
    )
    # It doesn't matter what `result` is — the gate didn't reject.
    assert trader.place_order_simple_call_count == 1


@pytest.mark.asyncio
async def test_gate_on_rejects_direct_order():
    """Gate on — direct buy/sell is refused before trader.place_order_simple
    is even called. Error message names the proposal → approve fix."""
    trader = _FakeTrader(require_proposal_approval=True)
    api = TraderServiceApi(trader)

    result = await api.place_order_simple(
        contract=_FakeContract(),
        action='BUY',
        equity_amount=None,
        quantity=10,
        limit_price=None,
        market_order=True,
    )
    assert isinstance(result, SuccessFail)
    assert not result.is_success()
    # Real trader.place_order_simple must never have been invoked.
    assert trader.place_order_simple_call_count == 0
    # Error surface points at the fix.
    assert 'propose' in (result.error or '').lower()
    assert 'approve' in (result.error or '').lower()


@pytest.mark.asyncio
async def test_gate_on_skip_risk_gate_bypasses_for_liquidation():
    """Gate on + skip_risk_gate=True is the liquidation escape hatch
    (close-all, resize-positions). Must still work even when direct
    trading is locked down."""
    trader = _FakeTrader(require_proposal_approval=True)
    api = TraderServiceApi(trader)

    # close-all-style call: skip_risk_gate=True
    await api.place_order_simple(
        contract=_FakeContract(),
        action='SELL',
        equity_amount=None,
        quantity=10,
        limit_price=None,
        market_order=True,
        skip_risk_gate=True,
    )
    # Gate did NOT reject — trader's real place_order_simple ran.
    assert trader.place_order_simple_call_count == 1


def test_trader_constructor_accepts_flag():
    """Regression: Trader.__init__ must accept require_proposal_approval
    kwarg (DI container plucks it from the config YAML key of the same
    name, so signature compatibility matters)."""
    import inspect
    from trader.trading.trading_runtime import Trader
    params = inspect.signature(Trader.__init__).parameters
    assert 'require_proposal_approval' in params
    # Default must be False to preserve legacy behaviour on upgrades.
    assert params['require_proposal_approval'].default is False
