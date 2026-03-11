import datetime as dt
import pytest
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.objects import Action
from trader.trading.risk_gate import RiskGate, RiskGateResult, RiskLimits
from trader.trading.strategy import Signal


def _make_signal(name="test_strat"):
    return Signal(
        source_name=name,
        action=Action.BUY,
        probability=0.8,
        risk=0.2,
        conid=4391,
    )


class TestRiskGate:
    def test_approve_normal_signal(self, risk_gate):
        result = risk_gate.evaluate(_make_signal(), open_order_count=0, daily_pnl=0.0)
        assert result.approved is True

    def test_reject_max_open_orders(self, risk_gate):
        result = risk_gate.evaluate(
            _make_signal(),
            open_order_count=10,
        )
        assert result.approved is False
        assert "max open orders" in result.reason

    def test_reject_daily_loss(self, risk_gate):
        result = risk_gate.evaluate(
            _make_signal(),
            daily_pnl=-1500.0,
        )
        assert result.approved is False
        assert "daily loss" in result.reason

    def test_reject_position_concentration(self, risk_gate):
        result = risk_gate.evaluate(
            _make_signal(),
            portfolio_value=100_000.0,
            position_value=15_000.0,  # 15% > 10% default
        )
        assert result.approved is False
        assert "concentration" in result.reason

    def test_reject_signal_rate_limit(self, event_store, risk_gate):
        # Insert 20 signal events in the last hour
        for i in range(20):
            event_store.append(TradingEvent(
                event_type=EventType.SIGNAL,
                timestamp=dt.datetime.now() - dt.timedelta(minutes=i),
                strategy_name="test_strat",
                conid=4391,
            ))
        result = risk_gate.evaluate(_make_signal())
        assert result.approved is False
        assert "rate limit" in result.reason

    def test_custom_limits(self, event_store):
        gate = RiskGate(
            limits=RiskLimits(max_open_orders=2, max_daily_loss=500.0),
            event_store=event_store,
        )
        # 2 open orders is at limit
        result = gate.evaluate(_make_signal(), open_order_count=2)
        assert result.approved is False

        result = gate.evaluate(_make_signal(), daily_pnl=-501.0)
        assert result.approved is False
