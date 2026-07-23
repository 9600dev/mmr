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

    def test_reject_order_rate_limit(self, event_store, risk_gate):
        # The rate limit counts ORDER_SUBMITTED — the event the order paths
        # actually write (SIGNAL was never written there; the old check was
        # dead code).
        for i in range(20):
            event_store.append(TradingEvent(
                event_type=EventType.ORDER_SUBMITTED,
                timestamp=dt.datetime.now() - dt.timedelta(minutes=i),
                strategy_name="test_strat",
                conid=4391,
            ))
        result = risk_gate.evaluate(_make_signal())
        assert result.approved is False
        assert "rate limit" in result.reason
        assert result.checks["order_rate"] == "fail"

    def test_signal_events_do_not_count_toward_rate_limit(self, event_store, risk_gate):
        for i in range(30):
            event_store.append(TradingEvent(
                event_type=EventType.SIGNAL,
                timestamp=dt.datetime.now() - dt.timedelta(minutes=i),
                strategy_name="test_strat",
                conid=4391,
            ))
        result = risk_gate.evaluate(_make_signal())
        assert result.approved is True

    def test_empty_event_store_is_a_legitimate_zero(self, risk_gate):
        """An empty event store is a checked count of 0 — pass, not
        not-evaluable."""
        result = risk_gate.evaluate(_make_signal())
        assert result.approved is True
        assert result.checks["order_rate"] == "pass"

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


class TestOrderRateLimitExitClass:
    """The open-rate limit counts only exposure-INCREASING ORDER_SUBMITTED
    events attributable to the SAME source, so an active session's own
    closes/stops and other strategies' opens can't false-trip it."""

    def _submit(self, event_store, strategy_name, n, exit_class=False):
        for i in range(n):
            event_store.append(TradingEvent(
                event_type=EventType.ORDER_SUBMITTED,
                timestamp=dt.datetime.now() - dt.timedelta(minutes=i),
                strategy_name=strategy_name,
                conid=4391,
                metadata={'exit_class': True} if exit_class else {},
            ))

    def test_opens_over_limit_refused(self, event_store, risk_gate):
        self._submit(event_store, 'test_strat', 20, exit_class=False)
        result = risk_gate.evaluate(_make_signal('test_strat'))
        assert result.approved is False
        assert result.checks['order_rate'] == 'fail'

    def test_exit_class_submissions_do_not_trip_the_open_limit(self, event_store, risk_gate):
        """30 closes/stops from the source plus 5 opens: only the 5 opens
        count, so the open (limit 20) is still approved."""
        self._submit(event_store, 'test_strat', 30, exit_class=True)
        self._submit(event_store, 'test_strat', 5, exit_class=False)
        result = risk_gate.evaluate(_make_signal('test_strat'))
        assert result.approved is True
        assert result.checks['order_rate'] == 'pass'

    def test_two_sources_do_not_contaminate_each_other(self, event_store, risk_gate):
        """25 opens from strat_b must not push strat_a over its own limit."""
        self._submit(event_store, 'strat_b', 25, exit_class=False)
        self._submit(event_store, 'strat_a', 3, exit_class=False)
        result = risk_gate.evaluate(_make_signal('strat_a'))
        assert result.approved is True
        # strat_b, meanwhile, is over its own limit
        result_b = risk_gate.evaluate(_make_signal('strat_b'))
        assert result_b.approved is False
        assert result_b.checks['order_rate'] == 'fail'


class TestForexConcentrationExemption:
    """A forex order (sec_type CASH) is a currency conversion, not position
    concentration — it must be exempt from the concentration cap."""

    def test_cash_over_pct_cap_passes_concentration(self, risk_gate):
        # 20000 EUR ~ $21.6k against a $17k account = 127% > 10%.
        result = risk_gate.evaluate(
            _make_signal(), portfolio_value=17_000.0, position_value=21_600.0,
            sec_type='CASH')
        assert result.approved is True
        assert result.checks['concentration'] == 'skipped:forex-cash'

    def test_equivalent_stk_over_cap_still_refused(self, risk_gate):
        result = risk_gate.evaluate(
            _make_signal(), portfolio_value=17_000.0, position_value=21_600.0,
            sec_type='STK')
        assert result.approved is False
        assert result.checks['concentration'] == 'fail'


class TestTriStateInputs:
    """A critical input that could not be read must REFUSE an
    exposure-increasing order, naming the missing datum — never silently
    no-op the check against a default. A value that reads as exactly 0 with
    its evaluable flag set is a legitimate pass."""

    def test_unreadable_daily_pnl_refuses(self, risk_gate):
        result = risk_gate.evaluate(_make_signal(), daily_pnl_evaluable=False)
        assert result.approved is False
        assert "daily pnl" in result.reason.lower()
        assert result.checks["daily_loss"] == "skipped:not-evaluable"

    def test_daily_pnl_exactly_zero_is_a_pass(self, risk_gate):
        result = risk_gate.evaluate(_make_signal(), daily_pnl=0.0, daily_pnl_evaluable=True)
        assert result.approved is True
        assert result.checks["daily_loss"] == "pass"

    def test_unreadable_portfolio_value_refuses(self, risk_gate):
        result = risk_gate.evaluate(_make_signal(), portfolio_value_evaluable=False)
        assert result.approved is False
        assert "netliquidation" in result.reason.lower() or "portfolio value" in result.reason.lower()
        assert result.checks["concentration"] == "skipped:portfolio-value-not-evaluable"

    def test_no_price_for_concentration_refuses(self, risk_gate):
        result = risk_gate.evaluate(_make_signal(), position_value_evaluable=False)
        assert result.approved is False
        assert "price" in result.reason.lower()
        assert result.checks["concentration"] == "skipped:no-price"

    def test_zero_portfolio_value_with_real_position_refuses(self, risk_gate):
        """Evaluable portfolio_value of 0 against a real notional is infinite
        concentration — a fail, not a skip."""
        result = risk_gate.evaluate(
            _make_signal(), portfolio_value=0.0, position_value=5_000.0)
        assert result.approved is False
        assert "concentration" in result.reason
        assert result.checks["concentration"] == "fail"

    def test_checks_dict_records_tri_state_on_approval(self, risk_gate):
        result = risk_gate.evaluate(
            _make_signal(),
            open_order_count=1,
            daily_pnl=-100.0,
            portfolio_value=100_000.0,
            position_value=5_000.0,
        )
        assert result.approved is True
        assert result.checks == {
            "max_open_orders": "pass",
            "daily_loss": "pass",
            "concentration": "pass",
            "order_rate": "pass",
        }

    def test_zero_notional_concentration_is_skipped_not_failed(self, risk_gate):
        result = risk_gate.evaluate(
            _make_signal(), portfolio_value=100_000.0, position_value=0.0)
        assert result.approved is True
        assert result.checks["concentration"] == "skipped:zero-notional"

    def test_result_default_checks_empty(self):
        """Existing constructors/stubs that build RiskGateResult without
        checks keep working."""
        result = RiskGateResult(approved=True)
        assert result.checks == {}


class TestRiskLimitsLoad:
    def test_missing_file_returns_defaults(self, tmp_path):
        limits = RiskLimits.load(str(tmp_path / "nope.yaml"))
        assert limits == RiskLimits()

    def test_missing_section_returns_defaults(self, tmp_path):
        p = tmp_path / "trader.yaml"
        p.write_text("ib_server_address: localhost\n")
        assert RiskLimits.load(str(p)) == RiskLimits()

    def test_defaults_identical_to_hardcoded(self):
        limits = RiskLimits()
        assert limits.max_position_size_pct == 0.10
        assert limits.max_daily_loss == 1000.0
        assert limits.max_open_orders == 15
        assert limits.max_signals_per_hour == 20
        assert limits.max_leverage == 1.0
        assert limits.min_margin_cushion == 0.10

    def test_partial_override_keeps_other_defaults(self, tmp_path):
        p = tmp_path / "trader.yaml"
        p.write_text("risk_limits:\n  max_daily_loss: 250.0\n  max_open_orders: 4\n")
        limits = RiskLimits.load(str(p))
        assert limits.max_daily_loss == 250.0
        assert limits.max_open_orders == 4
        assert limits.max_position_size_pct == 0.10

    def test_unknown_key_falls_back_to_defaults_not_raises(self, tmp_path, caplog):
        """A typo'd key must NOT raise — load runs at connect() while live
        positions are held, and a raise would crash-loop the service through
        supervise's circuit breaker. Log an ERROR naming the bad key, ignore
        it, and keep valid keys + defaults."""
        p = tmp_path / "trader.yaml"
        p.write_text("risk_limits:\n  max_dialy_loss: 250.0\n  max_open_orders: 4\n")
        with caplog.at_level("ERROR"):
            limits = RiskLimits.load(str(p))
        assert isinstance(limits, RiskLimits)
        # bad key ignored → its field keeps the default
        assert limits.max_daily_loss == RiskLimits().max_daily_loss
        # valid keys still applied
        assert limits.max_open_orders == 4
        assert any("max_dialy_loss" in r.message for r in caplog.records)

    def test_malformed_value_falls_back_to_default_for_that_key(self, tmp_path, caplog):
        p = tmp_path / "trader.yaml"
        p.write_text("risk_limits:\n  max_daily_loss: lots\n  max_open_orders: 7\n")
        with caplog.at_level("ERROR"):
            limits = RiskLimits.load(str(p))
        assert limits.max_daily_loss == RiskLimits().max_daily_loss
        assert limits.max_open_orders == 7
        assert any("max_daily_loss" in r.message for r in caplog.records)

    def test_non_mapping_section_falls_back_to_defaults(self, tmp_path, caplog):
        p = tmp_path / "trader.yaml"
        p.write_text("risk_limits: [1, 2]\n")
        with caplog.at_level("ERROR"):
            limits = RiskLimits.load(str(p))
        assert limits == RiskLimits()
        assert any("mapping" in r.message for r in caplog.records)


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
