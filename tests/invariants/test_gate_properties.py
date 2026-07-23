"""Invariants of record: RiskGate and the exit-class trust boundary.

  * an APPROVED evaluation for an exposure-increasing order records no
    'fail' check (and a refusal always names a reason);
  * a critical input that could not be read (tri-state *_evaluable=False)
    REFUSES the open and the reason names the missing datum;
  * an exit-class order is NEVER refusable by gates — draconian limits, a
    denylisted symbol, unreadable inputs: the close still places;
  * the rate limit counts ORDER_SUBMITTED events only (SIGNAL never counts),
    and a count of zero — including a brand-new empty store — is a pass.

EventStore idiom mirrors tests/test_risk_gate.py (tmp DuckDB, real store).
The store fixture is function-scoped and shared across Hypothesis examples;
that is safe here because every example scopes its events under a unique
strategy name (count_since filters on strategy_name), hence the suppressed
function_scoped_fixture health check.
"""

import asyncio
import datetime as dt
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
import reactivex as rx
from hypothesis import HealthCheck, given, settings, strategies as st

from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.objects import Action, ContractOrderPair, ExecutorCondition
from trader.trading.executioner import TradeExecutioner
from trader.trading.risk_gate import RiskGate, RiskInputs, RiskLimits
from trader.trading.strategy import Signal
from trader.trading.trading_filter import TradingFilter


_SETTINGS = settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)


@pytest.fixture
def gate_event_store(tmp_path):
    return EventStore(str(tmp_path / f'gate_{uuid4().hex[:8]}.duckdb'))


def _signal(name='invariant_strat'):
    return Signal(source_name=name, action=Action.BUY, probability=0.8, risk=0.2, conid=4391)


def _limits(min_open_orders=1):
    return st.builds(
        RiskLimits,
        max_position_size_pct=st.floats(min_value=0.01, max_value=0.5),
        max_daily_loss=st.floats(min_value=100.0, max_value=10_000.0),
        max_open_orders=st.integers(min_value=min_open_orders, max_value=40),
        max_signals_per_hour=st.integers(min_value=1, max_value=40),
    )


# ---------------------------------------------------------------------------
# Approval ⇒ no failed check
# ---------------------------------------------------------------------------

@_SETTINGS
@given(
    limits=_limits(min_open_orders=0),
    open_orders=st.integers(min_value=0, max_value=50),
    daily_pnl=st.floats(min_value=-20_000.0, max_value=20_000.0),
    portfolio_value=st.floats(min_value=0.0, max_value=1e6),
    position_value=st.floats(min_value=0.0, max_value=3e5),
    daily_ok=st.booleans(),
    pv_ok=st.booleans(),
    posv_ok=st.booleans(),
)
def test_approval_implies_no_failed_check(
        gate_event_store, limits, open_orders, daily_pnl,
        portfolio_value, position_value, daily_ok, pv_ok, posv_ok):
    """An approved exposure-increasing order has no recorded 'fail' and no
    not-evaluable skip; a refusal always carries a reason and a failing or
    not-evaluable check (skipped:zero-notional alone never refuses)."""
    gate = RiskGate(limits=limits, event_store=gate_event_store)
    result = gate.evaluate(
        _signal(f'strat_{uuid4().hex[:12]}'),
        open_order_count=open_orders,
        daily_pnl=daily_pnl,
        portfolio_value=portfolio_value,
        position_value=position_value,
        daily_pnl_evaluable=daily_ok,
        portfolio_value_evaluable=pv_ok,
        position_value_evaluable=posv_ok,
    )
    if result.approved:
        assert all(v != 'fail' for v in result.checks.values())
        assert not any(
            v.startswith('skipped:') and v != 'skipped:zero-notional'
            for v in result.checks.values()
        )
    else:
        assert result.reason != ''
        assert any(
            v == 'fail' or (v.startswith('skipped:') and v != 'skipped:zero-notional')
            for v in result.checks.values()
        )


# ---------------------------------------------------------------------------
# Tri-state: not-evaluable critical input refuses, naming the missing datum
# ---------------------------------------------------------------------------

@_SETTINGS
@given(
    limits=_limits(),
    which=st.sampled_from(['daily_pnl', 'portfolio_value', 'position_value']),
)
def test_not_evaluable_critical_input_refuses_open_naming_it(
        gate_event_store, limits, which):
    """All other inputs are safe; exactly one critical input is unreadable.
    The open must be refused and the reason must name the missing datum —
    fail-closed, never a silent skip."""
    gate = RiskGate(limits=limits, event_store=gate_event_store)
    result = gate.evaluate(
        _signal(f'strat_{uuid4().hex[:12]}'),
        open_order_count=0,
        daily_pnl=0.0,
        portfolio_value=100_000.0,
        position_value=1_000.0,
        daily_pnl_evaluable=which != 'daily_pnl',
        portfolio_value_evaluable=which != 'portfolio_value',
        position_value_evaluable=which != 'position_value',
    )
    assert result.approved is False
    reason = result.reason.lower()
    if which == 'daily_pnl':
        assert 'daily pnl' in reason
        assert result.checks['daily_loss'] == 'skipped:not-evaluable'
    elif which == 'portfolio_value':
        assert 'portfolio value' in reason or 'netliquidation' in reason
        assert result.checks['concentration'] == 'skipped:portfolio-value-not-evaluable'
    else:
        assert 'price' in reason
        assert result.checks['concentration'] == 'skipped:no-price'


# ---------------------------------------------------------------------------
# Exit-class trust boundary: closes are never refusable by gates
# ---------------------------------------------------------------------------

class _Order:
    def __init__(self, action='SELL', account='DU1'):
        self.action = action
        self.totalQuantity = 10
        self.account = account
        self.lmtPrice = 0
        self.orderId = 0

    def __str__(self):
        return f'<Order {self.action} {self.totalQuantity}>'


class _Contract:
    def __init__(self, symbol='AMD'):
        self.symbol = symbol
        self.exchange = 'NASDAQ'
        self.secType = 'STK'
        self.conId = 1
        self.multiplier = None


def _exit_executioner(gate, inputs):
    """Stub-trader idiom from tests/test_executioner.py: real gate, exit-class
    predicate answering True (the order reduces the live broker position)."""
    trader = MagicMock()
    trader.ib_account = 'DU1'
    trader.event_store = MagicMock()
    trader.client = MagicMock()
    trader.client.subscribe_place_order = AsyncMock(
        return_value=rx.from_iterable([MagicMock()]))
    trader.risk_gate = gate
    trader.order_reduces_exposure = MagicMock(return_value=True)
    trader.gather_risk_inputs = MagicMock(return_value=inputs)
    ex = TradeExecutioner()
    ex.connect(trader)
    return ex, trader


@_SETTINGS
@given(
    limits=st.builds(
        RiskLimits,
        # Deliberately draconian: limits that would refuse ANY open.
        max_position_size_pct=st.floats(min_value=0.0, max_value=0.05),
        max_daily_loss=st.floats(min_value=0.0, max_value=100.0),
        max_open_orders=st.integers(min_value=0, max_value=2),
        max_signals_per_hour=st.integers(min_value=0, max_value=2),
    ),
    open_orders=st.integers(min_value=0, max_value=100),
    daily_pnl=st.floats(min_value=-1e6, max_value=0.0),
    denylisted=st.booleans(),
    inputs_readable=st.booleans(),
)
def test_exit_class_orders_are_never_refusable(
        gate_event_store, limits, open_orders, daily_pnl, denylisted, inputs_readable):
    """Regardless of limit settings — zero allowed open orders, a busted
    daily loss, a denylisted symbol, unreadable account inputs — an
    exit-class order reaches the broker. Refusing an exit is worse than any
    gate breach it could represent."""
    gate = RiskGate(limits=limits, event_store=gate_event_store)
    if denylisted:
        gate.trading_filter = TradingFilter(denylist=['AMD'])
    inputs = RiskInputs(
        open_order_count=open_orders,
        daily_pnl=daily_pnl,
        daily_pnl_evaluable=inputs_readable,
        portfolio_value=0.0,
        portfolio_value_evaluable=inputs_readable,
    )
    ex, trader = _exit_executioner(gate, inputs)
    pair = ContractOrderPair(contract=_Contract(symbol='AMD'), order=_Order(action='SELL'))

    async def _run():
        observable = await ex.place_order(pair, condition=ExecutorCondition.NO_CHECKS)
        errors = []
        observable.subscribe(on_next=lambda _: None, on_error=errors.append)
        assert errors == [], f'exit-class order was refused: {errors}'

    asyncio.run(_run())
    assert trader.client.subscribe_place_order.call_count == 1


# ---------------------------------------------------------------------------
# Rate limit: ORDER_SUBMITTED counts, SIGNAL doesn't, empty is a pass
# ---------------------------------------------------------------------------

@settings(max_examples=25, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    limit=st.integers(min_value=1, max_value=8),
    n_submitted=st.integers(min_value=0, max_value=12),
    n_signals=st.integers(min_value=0, max_value=6),
)
def test_rate_limit_counts_order_submitted_only(
        gate_event_store, limit, n_submitted, n_signals):
    """approved iff recent ORDER_SUBMITTED count < limit; SIGNAL events are
    invisible to the rate check (the old SIGNAL-based check was dead code)."""
    strat = f'strat_{uuid4().hex[:12]}'
    now = dt.datetime.now()
    for i in range(n_submitted):
        gate_event_store.append(TradingEvent(
            event_type=EventType.ORDER_SUBMITTED,
            timestamp=now - dt.timedelta(minutes=i),
            strategy_name=strat,
            conid=4391,
        ))
    for i in range(n_signals):
        gate_event_store.append(TradingEvent(
            event_type=EventType.SIGNAL,
            timestamp=now - dt.timedelta(minutes=i),
            strategy_name=strat,
            conid=4391,
        ))
    gate = RiskGate(
        limits=RiskLimits(max_signals_per_hour=limit),
        event_store=gate_event_store,
    )
    result = gate.evaluate(
        _signal(strat),
        open_order_count=0,
        daily_pnl=0.0,
        portfolio_value=100_000.0,
        position_value=0.0,
    )
    if n_submitted >= limit:
        assert result.approved is False
        assert result.checks['order_rate'] == 'fail'
        assert 'rate limit' in result.reason
    else:
        assert result.approved is True
        assert result.checks['order_rate'] == 'pass'


@_SETTINGS
@given(limits=_limits())
def test_zero_order_count_is_a_pass_not_not_evaluable(gate_event_store, limits):
    """A store with no ORDER_SUBMITTED events for the strategy — including a
    brand-new empty store on the first example — is a checked count of 0:
    'pass', never 'not evaluable'."""
    gate = RiskGate(limits=limits, event_store=gate_event_store)
    result = gate.evaluate(
        _signal(f'strat_{uuid4().hex[:12]}'),
        open_order_count=0,
        daily_pnl=0.0,
        portfolio_value=100_000.0,
        position_value=0.0,
    )
    assert result.approved is True
    assert result.checks['order_rate'] == 'pass'
