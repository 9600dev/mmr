"""Tests for the require_proposal_approval gate on place_order_simple.

When the flag is on, direct buy/sell RPC calls that would INCREASE exposure
must be rejected. Exit-class orders — those the trader itself verifies reduce
the live broker position (closes, resize trims) — are exempt. The
client-supplied ``skip_risk_gate`` flag is kept for wire compatibility but is
ignored: the server asks the trader, never the client.
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
    """Minimal Trader stand-in — only needs the gate flag, the splitter, and a
    place_order_simple stub. Real Trader has dozens of attrs.

    The gate used to ask ``order_reduces_exposure`` (a boolean over the WHOLE
    order) and now asks ``split_for_order`` (how much of it opens exposure).
    That change is the fix for the 2026-07-27 live hole: the boolean answered
    "exit" for an oversized close and exempted the opening remainder with it.
    ``is_exit`` is kept as this fake's input because these tests are about the
    pure-exit and pure-open cases, where the two questions agree; the flip
    case, where they do not, is pinned in
    ``tests/invariants/test_proposal_gate_split.py``.
    """
    def __init__(self, require_proposal_approval=False, is_exit=False):
        self.require_proposal_approval = require_proposal_approval
        self.place_order_simple_call_count = 0
        self._is_exit = is_exit
        self.exit_check_calls = []

    def order_reduces_exposure(self, contract, action, quantity):
        self.exit_check_calls.append((contract, action, quantity))
        return self._is_exit

    def split_for_order(self, contract, action, quantity):
        from trader.trading.order_split import SplitPlan
        self.exit_check_calls.append((contract, action, quantity))
        qty = float(quantity or 0.0)
        return SplitPlan(qty, 0.0) if self._is_exit else SplitPlan(0.0, qty)

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
async def test_gate_on_rejects_direct_open():
    """Gate on — a direct exposure-increasing order is refused before
    trader.place_order_simple is even called. Error message names the
    proposal → approve fix."""
    trader = _FakeTrader(require_proposal_approval=True, is_exit=False)
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
    # The server asked the trader (normalized action + quantity).
    assert trader.exit_check_calls, 'exit-class predicate was never consulted'
    _, action, quantity = trader.exit_check_calls[0]
    assert action == 'BUY'
    assert quantity == 10


@pytest.mark.asyncio
async def test_gate_on_exempts_exit_class_close():
    """Gate on — an order the trader verifies is exit-class (a close) is
    exempt: single `mmr close` and resize deltas work under
    require_proposal_approval true, with no client flag needed."""
    trader = _FakeTrader(require_proposal_approval=True, is_exit=True)
    api = TraderServiceApi(trader)

    await api.place_order_simple(
        contract=_FakeContract(),
        action='SELL',
        equity_amount=None,
        quantity=10,
        limit_price=None,
        market_order=True,
    )
    assert trader.place_order_simple_call_count == 1


@pytest.mark.asyncio
async def test_close_all_still_works_because_closes_are_exit_class():
    """Close-all passes skip_risk_gate=True on the wire (legacy). It keeps
    working — but because its orders are exit-class, not because of the
    flag."""
    trader = _FakeTrader(require_proposal_approval=True, is_exit=True)
    api = TraderServiceApi(trader)

    await api.place_order_simple(
        contract=_FakeContract(),
        action='SELL',
        equity_amount=None,
        quantity=10,
        limit_price=None,
        market_order=True,
        skip_risk_gate=True,
    )
    assert trader.place_order_simple_call_count == 1


@pytest.mark.asyncio
async def test_skip_risk_gate_flag_cannot_bypass_gate_for_opens():
    """The client flag is no longer trusted: skip_risk_gate=True on a
    non-exit-class order is still refused by the proposal gate."""
    trader = _FakeTrader(require_proposal_approval=True, is_exit=False)
    api = TraderServiceApi(trader)

    result = await api.place_order_simple(
        contract=_FakeContract(),
        action='BUY',
        equity_amount=None,
        quantity=10,
        limit_price=None,
        market_order=True,
        skip_risk_gate=True,
    )
    assert isinstance(result, SuccessFail)
    assert not result.is_success()
    assert trader.place_order_simple_call_count == 0


@pytest.mark.asyncio
async def test_exit_check_failure_treated_as_open():
    """If the exit-class check itself blows up, the order is treated as an
    open (gated) — never fail-open."""
    trader = _FakeTrader(require_proposal_approval=True)

    def _boom(contract, action, quantity):
        raise RuntimeError('positions unreadable')
    trader.order_reduces_exposure = _boom
    api = TraderServiceApi(trader)

    result = await api.place_order_simple(
        contract=_FakeContract(),
        action='SELL',
        equity_amount=None,
        quantity=10,
        limit_price=None,
        market_order=True,
    )
    assert not result.is_success()
    assert trader.place_order_simple_call_count == 0


@pytest.mark.asyncio
async def test_invalid_action_refused_before_gate():
    trader = _FakeTrader(require_proposal_approval=True, is_exit=True)
    api = TraderServiceApi(trader)

    result = await api.place_order_simple(
        contract=_FakeContract(),
        action='BYU',
        equity_amount=None,
        quantity=10,
        limit_price=None,
        market_order=True,
    )
    assert not result.is_success()
    assert 'invalid action' in (result.error or '').lower()
    assert trader.place_order_simple_call_count == 0


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
