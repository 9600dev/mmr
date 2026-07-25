"""Server-side notional-tier approver gate (Phase 2).

The enforcement axis is the SERVER-RECOMPUTED order notional — un-forgeable by
a proposer. Pinned here:
  * an open valued ABOVE the threshold with a wrong/empty key FAILS naming the
    threshold and places NO order;
  * the same open with the correct key proceeds;
  * an open valued BELOW the threshold proceeds regardless of key;
  * a non-evaluable notional while the tier is on FAILS CLOSED;
  * an exit-class SELL (reduces the live position) with the tier on and no key
    is NEVER refused (closes never need a key).

Trader is built via object.__new__ + stubbed IB (idiom from
tests/test_trading_runtime.py::test_bracket_rolls_back_when_tp_fails).
"""

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock

import pytest
import reactivex as rx
from ib_async.contract import Contract

from trader.common.reactivex import SuccessFail, SuccessFailEnum
from trader.trading.proposal import ExecutionSpec
from trader.trading.risk_gate import RiskGateResult, RiskInputs
from trader.trading.trading_runtime import Trader


class _ApproveAll:
    def check_instrument(self, **kw):
        return RiskGateResult(approved=True)

    def check_leverage(self, *a, **kw):
        return RiskGateResult(approved=True)

    def evaluate(self, *a, **kw):
        return RiskGateResult(approved=True, checks={'max_open_orders': 'pass', 'daily_loss': 'pass', 'concentration': 'pass', 'order_rate': 'pass'})


class _StubExecutioner:
    """Emits one fake Trade per placement; records how many orders were sent."""
    def __init__(self):
        self.calls = 0

    async def subscribe_place_order_direct(self, approved):
        self.calls += 1
        fake_trade = MagicMock()
        fake_trade.order = MagicMock()
        fake_trade.order.orderId = 9000 + self.calls
        return rx.from_iterable([fake_trade])


class _Tick:
    def __init__(self, price):
        # BUY values off ask; give a single usable price.
        self.ask = price
        self.bid = price
        self.last = price
        self.close = price


class _NoPriceTick:
    def __init__(self):
        self.ask = float('nan')
        self.bid = float('nan')
        self.last = None
        self.close = None


def _tier_trader(threshold, key, *, tick=None, is_exit=False):
    t = object.__new__(Trader)
    t.pnl_subscriptions = {}
    t._pnl_subscriptions_lock = threading.Lock()
    t._main_loop = None
    t.disposables = []
    t.ib_account = 'DU12345'
    t.approver_required_above_usd = threshold
    t.approver_key = key
    t.order_tracker = None  # skip the post-placement decisive-status wait

    t.order_reduces_exposure = MagicMock(return_value=is_exit)
    t.risk_gate = _ApproveAll()
    t.check_order_margin = AsyncMock(side_effect=Exception('skip margin'))
    t.gather_risk_inputs = MagicMock(return_value=RiskInputs(
        open_order_count=0,
        daily_pnl=0.0, daily_pnl_evaluable=True,
        portfolio_value=1_000_000.0, portfolio_value_evaluable=True,
    ))

    client = MagicMock()
    client.get_snapshot = AsyncMock(return_value=(tick if tick is not None else _Tick(100.0)))
    t.client = client
    t.executioner = _StubExecutioner()
    return t


def _contract():
    c = Contract()
    c.symbol = 'AMD'
    c.exchange = 'NASDAQ'
    c.secType = 'STK'
    c.conId = 4391
    return c


def _market_spec():
    return ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Above threshold: key required
# ---------------------------------------------------------------------------

def test_above_threshold_wrong_key_refused_no_order():
    # 20 shares * $100 = $2000 > $1000 threshold.
    t = _tier_trader(1000.0, 'correct-secret')
    result = _run(t.place_expressive_order(
        _contract(), 'BUY', 20, _market_spec(),
        algo_name='manual', approver_key='WRONG'))
    assert result.success_fail == SuccessFailEnum.FAIL
    assert '1,000.00' in result.error and '2,000.00' in result.error
    assert t.executioner.calls == 0  # NO order reached the broker


def test_above_threshold_empty_key_refused():
    t = _tier_trader(1000.0, 'correct-secret')
    result = _run(t.place_expressive_order(
        _contract(), 'BUY', 20, _market_spec(),
        algo_name='manual', approver_key=''))
    assert result.success_fail == SuccessFailEnum.FAIL
    assert t.executioner.calls == 0


def test_above_threshold_correct_key_proceeds():
    t = _tier_trader(1000.0, 'correct-secret')
    result = _run(t.place_expressive_order(
        _contract(), 'BUY', 20, _market_spec(),
        algo_name='manual', approver_key='correct-secret'))
    assert result.success_fail == SuccessFailEnum.SUCCESS
    assert t.executioner.calls == 1


def test_above_threshold_but_server_key_unset_refused():
    # Threshold on, but no key configured server-side: every above-threshold
    # open is refused (documented behaviour).
    t = _tier_trader(1000.0, '')
    result = _run(t.place_expressive_order(
        _contract(), 'BUY', 20, _market_spec(),
        algo_name='manual', approver_key='anything'))
    assert result.success_fail == SuccessFailEnum.FAIL
    assert t.executioner.calls == 0


# ---------------------------------------------------------------------------
# Below threshold: key irrelevant
# ---------------------------------------------------------------------------

def test_below_threshold_proceeds_without_key():
    # 5 shares * $100 = $500 < $1000 threshold.
    t = _tier_trader(1000.0, 'correct-secret')
    result = _run(t.place_expressive_order(
        _contract(), 'BUY', 5, _market_spec(),
        algo_name='manual', approver_key=''))
    assert result.success_fail == SuccessFailEnum.SUCCESS
    assert t.executioner.calls == 1


# ---------------------------------------------------------------------------
# Non-evaluable notional while the tier is on: fail closed
# ---------------------------------------------------------------------------

def test_non_evaluable_notional_fails_closed():
    t = _tier_trader(1000.0, 'correct-secret', tick=_NoPriceTick())
    result = _run(t.place_expressive_order(
        _contract(), 'BUY', 20, _market_spec(),
        algo_name='manual', approver_key='correct-secret'))
    assert result.success_fail == SuccessFailEnum.FAIL
    assert 'could not be valued' in result.error
    assert t.executioner.calls == 0


# ---------------------------------------------------------------------------
# Exit-class orders never refused (load-bearing)
# ---------------------------------------------------------------------------

def test_exit_class_sell_never_refused_by_tier():
    # A SELL that reduces the live position (is_exit=True), notional far above
    # the threshold, no key: the close must still place.
    t = _tier_trader(1000.0, 'correct-secret', is_exit=True)
    result = _run(t.place_expressive_order(
        _contract(), 'SELL', 500, _market_spec(),
        algo_name='manual', approver_key=''))
    assert result.success_fail == SuccessFailEnum.SUCCESS
    assert t.executioner.calls == 1


# ---------------------------------------------------------------------------
# Feature OFF (threshold 0): key never consulted
# ---------------------------------------------------------------------------

def test_feature_off_ignores_notional_and_key():
    t = _tier_trader(0.0, '')
    result = _run(t.place_expressive_order(
        _contract(), 'BUY', 20, _market_spec(),
        algo_name='manual', approver_key=''))
    assert result.success_fail == SuccessFailEnum.SUCCESS
    assert t.executioner.calls == 1


# ===========================================================================
# Direct order path (findings #1/#3): the tier must gate direct buy/sell too,
# not only the approve()/place_expressive_order path. Exercises the REAL
# Trader.enforce_approver_tier via a REAL TradeExecutioner (no stubbed tier).
# ===========================================================================

from trader.objects import ContractOrderPair, ExecutorCondition  # noqa: E402
from trader.trading.executioner import TradeExecutioner  # noqa: E402
from trader.trading.risk_gate import RiskGateResult  # noqa: E402


class _Position:
    def __init__(self, conid, symbol, qty):
        self.contract = Contract()
        self.contract.conId = conid
        self.contract.symbol = symbol
        self.position = qty


class _DirectOrder:
    """Minimal stand-in for an ib_async Order on the direct path."""
    def __init__(self, action='BUY', qty=20, account='DU12345',
                 order_type='MKT', lmt_price=0.0):
        self.action = action
        self.totalQuantity = qty
        self.account = account
        self.orderType = order_type
        self.lmtPrice = lmt_price
        self.orderId = 0
        self.orderRef = ''

    def __str__(self):
        return f'<Order {self.action} {self.totalQuantity}>'


def _direct_trader(threshold, key, *, positions=None):
    """A real (object.__new__) Trader wired to a real TradeExecutioner, so the
    REAL enforce_approver_tier runs on the executioner.place_order path.
    ``positions`` seeds the live broker position read (drives exit-class /
    flip classification); default [] => flat => every order opens."""
    import datetime as _dt
    t = object.__new__(Trader)
    t.ib_account = 'DU12345'
    t.startup_time = _dt.datetime.now()  # trader_exception() reads this on refusal
    t.approver_required_above_usd = threshold
    t.approver_key = key
    t.event_store = MagicMock()
    t.book = MagicMock()
    t.get_positions = MagicMock(return_value=positions if positions is not None else [])
    t.risk_gate = _ApproveAll()
    t.gather_risk_inputs = MagicMock(return_value=RiskInputs(
        open_order_count=0,
        daily_pnl=0.0, daily_pnl_evaluable=True,
        portfolio_value=1_000_000.0, portfolio_value_evaluable=True,
    ))
    client = MagicMock()
    client.get_snapshot = AsyncMock(return_value=_Tick(100.0))
    client.subscribe_place_order = AsyncMock(return_value=rx.from_iterable([MagicMock()]))
    t.client = client
    ex = TradeExecutioner()
    ex.connect(t)
    t.executioner = ex
    return t, ex, client


def _place_direct(ex, contract, order, approver_key=''):
    async def _run_it():
        obs = await ex.place_order(
            ContractOrderPair(contract, order),
            condition=ExecutorCondition.NO_CHECKS,
            approver_key=approver_key)
        errors = []
        obs.subscribe(on_next=lambda _: None, on_error=errors.append)
        return errors
    return asyncio.run(_run_it())


def test_direct_path_above_threshold_wrong_key_refused_no_order():
    # 20 * 100 = 2000 > 1000: a direct BUY open with the wrong key is refused
    # and NEVER reaches the IB placement chokepoint.
    t, ex, client = _direct_trader(1000.0, 'correct-secret')
    errors = _place_direct(ex, _contract(), _DirectOrder('BUY', 20), approver_key='WRONG')
    assert errors, 'above-threshold direct open with wrong key must be refused'
    assert client.subscribe_place_order.call_count == 0


def test_direct_path_above_threshold_empty_key_refused_no_order():
    t, ex, client = _direct_trader(1000.0, 'correct-secret')
    errors = _place_direct(ex, _contract(), _DirectOrder('BUY', 20), approver_key='')
    assert errors
    assert client.subscribe_place_order.call_count == 0


def test_direct_path_above_threshold_correct_key_proceeds():
    t, ex, client = _direct_trader(1000.0, 'correct-secret')
    errors = _place_direct(ex, _contract(), _DirectOrder('BUY', 20), approver_key='correct-secret')
    assert errors == []
    assert client.subscribe_place_order.call_count == 1


def test_direct_path_below_threshold_proceeds_without_key():
    # 5 * 100 = 500 < 1000: below the threshold, no key needed.
    t, ex, client = _direct_trader(1000.0, 'correct-secret')
    errors = _place_direct(ex, _contract(), _DirectOrder('BUY', 5), approver_key='')
    assert errors == []
    assert client.subscribe_place_order.call_count == 1


def test_place_order_simple_threads_approver_key_to_executioner():
    """Trader.place_order_simple forwards approver_key down to
    executioner.place_order (the RPC/SDK wiring that fixes findings #1/#3)."""
    t = object.__new__(Trader)
    t.ib_account = 'DU12345'
    captured = {}

    async def _fake_place_order(**kw):
        captured.update(kw)
        return rx.from_iterable([MagicMock()])

    ex = MagicMock()
    ex.place_order = _fake_place_order
    ex.helper_create_order = MagicMock(
        return_value=ContractOrderPair(_contract(), _DirectOrder('BUY', 20)))
    t.executioner = ex
    client = MagicMock()
    client.get_snapshot = AsyncMock(return_value=_Tick(100.0))
    t.client = client

    from trader.objects import Action
    _run(t.place_order_simple(
        contract=_contract(), action=Action.BUY, equity_amount=None,
        quantity=20, limit_price=None, market_order=True,
        stop_loss_percentage=0.0, approver_key='out-of-band-secret'))
    assert captured.get('approver_key') == 'out-of-band-secret'


# ===========================================================================
# Lowball limit (finding #2): the tier values the OPENING notional at
# max(client limit, live snapshot) — a proposer cannot forge it DOWN with a
# limit far below the market.
# ===========================================================================

def _limit_spec(limit_price):
    return ExecutionSpec(
        order_type='LIMIT', limit_price=limit_price, exit_type='NONE').to_dict()


def test_lowball_limit_sell_open_valued_at_snapshot_not_limit_refused():
    # SELL LIMIT open: limit $1 is far below the $100 snapshot bid. Valued at
    # the snapshot (20 * 100 = 2000 > 1000) → refused. Valued at the lowball
    # limit it would be 20 * 1 = 20 < 1000 and slip through — the bug.
    t = _tier_trader(1000.0, 'correct-secret')
    result = _run(t.place_expressive_order(
        _contract(), 'SELL', 20, _limit_spec(1.0),
        algo_name='manual', approver_key=''))
    assert result.success_fail == SuccessFailEnum.FAIL
    assert '1,000.00' in result.error and '2,000.00' in result.error
    assert t.executioner.calls == 0


def test_lowball_market_sell_open_refused_identically():
    # The same open as a MARKET order is valued off the same snapshot and
    # refused identically (no limit to lowball).
    t = _tier_trader(1000.0, 'correct-secret')
    result = _run(t.place_expressive_order(
        _contract(), 'SELL', 20, _market_spec(),
        algo_name='manual', approver_key=''))
    assert result.success_fail == SuccessFailEnum.FAIL
    assert '2,000.00' in result.error
    assert t.executioner.calls == 0


def test_truthful_limit_above_snapshot_uses_max_of_the_two():
    # A truthful (again-marketable) limit ABOVE the snapshot: 6 * max(200,100)
    # = 1200 > 1000 → refused (max(limit, snapshot) is used). At the snapshot
    # alone it would be 6 * 100 = 600 < 1000 and pass.
    t = _tier_trader(1000.0, 'correct-secret')
    result = _run(t.place_expressive_order(
        _contract(), 'BUY', 6, _limit_spec(200.0),
        algo_name='manual', approver_key=''))
    assert result.success_fail == SuccessFailEnum.FAIL
    assert '1,200.00' in result.error
    assert t.executioner.calls == 0


def test_truthful_limit_above_snapshot_correct_key_proceeds():
    t = _tier_trader(1000.0, 'correct-secret')
    result = _run(t.place_expressive_order(
        _contract(), 'BUY', 6, _limit_spec(200.0),
        algo_name='manual', approver_key='correct-secret'))
    assert result.success_fail == SuccessFailEnum.SUCCESS
    assert t.executioner.calls == 1


# ===========================================================================
# Flip remainder (finding #4): a SELL that crosses zero (sells MORE than the
# held long) is tier-gated on its net-new SHORT remainder; a pure exit
# (qty <= held) is NEVER gated regardless of notional.
# ===========================================================================

def _flip_trader(threshold, key, held_long):
    """Real Trader (object.__new__) whose broker read returns a +held_long
    position, so enforce_approver_tier can classify exit vs flip vs open."""
    t = object.__new__(Trader)
    t.ib_account = 'DU12345'
    t.approver_required_above_usd = threshold
    t.approver_key = key
    t.get_positions = MagicMock(return_value=[_Position(4391, 'AMD', held_long)])
    client = MagicMock()
    client.get_snapshot = AsyncMock(return_value=_Tick(100.0))
    t.client = client
    return t


def test_flip_is_exit_class_and_never_gated():
    # Hold +10 long. SELL 30 crosses zero (a flip). It is EXIT-CLASS
    # (order_reduces_exposure True), so it is NEVER gated by the tier — even
    # though it opens a 20-short remainder worth 2000 > 1000 and no key is
    # supplied. Rationale: an atomic reduction must never be blocked by an
    # approval requirement, and gating here could refuse a genuine exit under a
    # position-read race. The opening remainder is a documented residual.
    t = _flip_trader(1000.0, 'correct-secret', held_long=10)
    assert _run(t.enforce_approver_tier(_contract(), 'SELL', 30, 'MARKET', None, '')) is None
    # Contrast: the SAME-size SELL with NO long is a pure short-open and IS
    # gated — proving the exemption is exit-class, not a blanket pass.
    t_open = _flip_trader(1000.0, 'correct-secret', held_long=0)
    assert _run(t_open.enforce_approver_tier(_contract(), 'SELL', 30, 'MARKET', None, '')) is not None


def test_pure_exit_never_gated_regardless_of_notional():
    # Hold +10 long. SELL exactly 10 is a pure exit: opening_qty 0 → NEVER
    # gated, even though 10 * 100 = 1000 and a huge held long would dwarf the
    # threshold. Also an undersized partial exit (SELL 5) is never gated.
    t = _flip_trader(1000.0, 'correct-secret', held_long=10)
    assert _run(t.enforce_approver_tier(_contract(), 'SELL', 10, 'MARKET', None, '')) is None
    assert _run(t.enforce_approver_tier(_contract(), 'SELL', 5, 'MARKET', None, '')) is None
    # A massive held long with a massive matched exit: still never gated.
    t2 = _flip_trader(1000.0, 'correct-secret', held_long=100_000)
    assert _run(t2.enforce_approver_tier(_contract(), 'SELL', 100_000, 'MARKET', None, '')) is None


def test_opening_exposure_quantity_classification():
    # Unit-level: the opening-exposure split the tier keys on.
    t = _flip_trader(1000.0, 'k', held_long=10)
    # pure exit within the long
    assert t._opening_exposure_quantity(_contract(), 'SELL', 8) == 0.0
    # exact exit
    assert t._opening_exposure_quantity(_contract(), 'SELL', 10) == 0.0
    # flip: 30 - 10 = 20 net-new short
    assert t._opening_exposure_quantity(_contract(), 'SELL', 30) == 20.0
    # a BUY on top of a long is a full open (adds long)
    assert t._opening_exposure_quantity(_contract(), 'BUY', 15) == 15.0


# ===========================================================================
# Backward-compat: threshold 0 => enforce_approver_tier returns None for
# EVERYTHING (opens, flips, non-evaluable), byte-identical to feature OFF.
# ===========================================================================

def test_enforce_tier_off_returns_none_for_everything():
    t = _flip_trader(0.0, '', held_long=10)
    # A huge open, no key, above any notional: still None (feature OFF).
    assert _run(t.enforce_approver_tier(_contract(), 'BUY', 100_000, 'MARKET', None, '')) is None
    # A flip: None.
    assert _run(t.enforce_approver_tier(_contract(), 'SELL', 500, 'MARKET', None, '')) is None
    # Non-evaluable price while OFF: still None (the tier is never consulted).
    t.client.get_snapshot = AsyncMock(return_value=_NoPriceTick())
    assert _run(t.enforce_approver_tier(_contract(), 'BUY', 100_000, 'MARKET', None, '')) is None
