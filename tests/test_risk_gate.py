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
            open_order_count=15,
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


class TestInstrumentCheck:
    def test_check_instrument_no_filter(self, risk_gate):
        """No trading filter set → always approved."""
        result = risk_gate.check_instrument('NKLA', exchange='NASDAQ', sec_type='STK')
        assert result.approved is True

    def test_check_instrument_denied(self, risk_gate):
        from trader.trading.trading_filter import TradingFilter
        risk_gate.trading_filter = TradingFilter(denylist=['NKLA'])
        result = risk_gate.check_instrument('NKLA')
        assert result.approved is False
        assert 'trading filter' in result.reason
        assert 'denylist' in result.reason

    def test_check_instrument_allowed(self, risk_gate):
        from trader.trading.trading_filter import TradingFilter
        risk_gate.trading_filter = TradingFilter(denylist=['NKLA'])
        result = risk_gate.check_instrument('AAPL', exchange='NASDAQ', sec_type='STK')
        assert result.approved is True


class TestLeverageCheck:
    def test_check_leverage_within_limit(self, risk_gate):
        """0.5x leverage with default 1.0x limit → approved."""
        margin_impact = {'initMarginAfter': 50000, 'equityWithLoanAfter': 95000}
        result = risk_gate.check_leverage(margin_impact, net_liquidation=100000)
        assert result.approved is True

    def test_check_leverage_exceeds_limit(self, risk_gate):
        """1.5x leverage with default 1.0x limit → rejected."""
        margin_impact = {'initMarginAfter': 150000, 'equityWithLoanAfter': 90000}
        result = risk_gate.check_leverage(margin_impact, net_liquidation=100000)
        assert result.approved is False
        assert 'leverage' in result.reason
        assert '1.50x' in result.reason
        assert '1.00x' in result.reason

    def test_check_leverage_custom_limit(self, event_store):
        """1.5x leverage with 2.0x limit → approved."""
        gate = RiskGate(
            limits=RiskLimits(max_leverage=2.0),
            event_store=event_store,
        )
        # equity=180k, margin=150k, net_liq=100k → cushion=(180k-150k)/100k=0.30 > 0.10
        margin_impact = {'initMarginAfter': 150000, 'equityWithLoanAfter': 180000}
        result = gate.check_leverage(margin_impact, net_liquidation=100000)
        assert result.approved is True

    def test_check_leverage_cushion_too_low(self, event_store):
        """Margin cushion below minimum → rejected."""
        gate = RiskGate(
            limits=RiskLimits(max_leverage=3.0, min_margin_cushion=0.20),
            event_store=event_store,
        )
        # equity=110k, margin=100k, net_liq=100k → cushion = (110k-100k)/100k = 0.10 < 0.20
        margin_impact = {'initMarginAfter': 100000, 'equityWithLoanAfter': 110000}
        result = gate.check_leverage(margin_impact, net_liquidation=100000)
        assert result.approved is False
        assert 'cushion' in result.reason

    def test_check_leverage_no_net_liq(self, risk_gate):
        """Zero net liquidation → skip check, approved."""
        margin_impact = {'initMarginAfter': 150000, 'equityWithLoanAfter': 90000}
        result = risk_gate.check_leverage(margin_impact, net_liquidation=0)
        assert result.approved is True

    def test_check_leverage_empty_impact(self, risk_gate):
        """Empty margin impact dict → approved (no data to check)."""
        result = risk_gate.check_leverage({}, net_liquidation=100000)
        assert result.approved is True
