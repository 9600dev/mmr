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
        assert result.checks["daily_loss"] == "unevaluable:daily-pnl"

    def test_daily_pnl_exactly_zero_is_a_pass(self, risk_gate):
        result = risk_gate.evaluate(_make_signal(), daily_pnl=0.0, daily_pnl_evaluable=True)
        assert result.approved is True
        assert result.checks["daily_loss"] == "pass"

    def test_unreadable_portfolio_value_refuses(self, risk_gate):
        result = risk_gate.evaluate(_make_signal(), portfolio_value_evaluable=False)
        assert result.approved is False
        assert "netliquidation" in result.reason.lower() or "portfolio value" in result.reason.lower()
        assert result.checks["concentration"] == "unevaluable:portfolio-value"

    def test_no_price_for_concentration_refuses(self, risk_gate):
        result = risk_gate.evaluate(_make_signal(), position_value_evaluable=False)
        assert result.approved is False
        assert "price" in result.reason.lower()
        assert result.checks["concentration"] == "unevaluable:position-value"

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
        """Zero net liquidation → REFUSE (fail-closed, 2026-07-26 flip)."""
        margin_impact = {'initMarginAfter': 150000, 'equityWithLoanAfter': 90000}
        result = risk_gate.check_leverage(margin_impact, net_liquidation=0)
        assert result.approved is False
        assert 'NetLiquidation' in result.reason

    def test_check_leverage_empty_impact(self, risk_gate):
        """Empty margin impact dict → REFUSE (fail-closed, 2026-07-26 flip)."""
        result = risk_gate.check_leverage({}, net_liquidation=100000)
        assert result.approved is False
        assert 'fail-closed' in result.reason


class TestCheckLeverageMissingData:
    """The leverage check's behaviour when margin data is absent or degenerate.

    Mutation testing found five survivors clustered here (check_leverage mutants
    9, 12, 14, 21, 32) — every one a change to a missing-key default or a guard
    boundary. Nothing exercised those paths, so the check's behaviour with
    unreadable inputs was entirely unspecified.

    HISTORY, in three stages, each pinned by tests at the time:
      1. Originally these paths SILENTLY APPROVED — the five mutation survivors
         existed because nothing exercised them at all.
      2. The tri-state pass (2026-07-25) kept the approvals but RECORDED the
         skips, so the audit trail could at least see "never checked".
      3. 2026-07-26 (operator decision): FAIL-CLOSED. Unreadable inputs refuse
         the open like every other gate input; the skip states become the
         refusal's evidence. Exits never reach this method — they are routed
         away before the margin section (pinned in the invariants suite).
    """

    def _gate(self, event_store, max_leverage=2.0, min_margin_cushion=0.1):
        return RiskGate(
            limits=RiskLimits(max_leverage=max_leverage, min_margin_cushion=min_margin_cushion),
            event_store=event_store,
        )

    def test_missing_init_margin_key_refuses(self, event_store):
        """A missing initMarginAfter is unreadable data, not a zero — refuse,
        naming the missing datum, with the skip recorded as evidence."""
        gate = self._gate(event_store)
        result = gate.check_leverage({'equityWithLoanAfter': 0}, net_liquidation=1_000_000.0)
        assert result.approved is False
        assert 'initMarginAfter' in result.reason
        assert result.checks['leverage'] == 'unevaluable:margin-data'

    def test_missing_equity_key_refuses_after_leverage_passes(self, event_store):
        """Leverage evaluable and fine, cushion unreadable: the refusal names
        equityWithLoanAfter and the record shows leverage=pass beside the skip —
        partial evaluation stays visible, which is the tri-state's whole point."""
        gate = self._gate(event_store)
        result = gate.check_leverage({'initMarginAfter': 1.0}, net_liquidation=1_000_000.0)
        assert result.approved is False
        assert 'equityWithLoanAfter' in result.reason
        assert result.checks['leverage'] == 'pass'
        assert result.checks['margin_cushion'] == 'unevaluable:equity-data'

    def test_leverage_is_enforced_when_both_keys_are_present(self, event_store):
        """The control: with data present the limit really does bite."""
        gate = self._gate(event_store, max_leverage=2.0)
        result = gate.check_leverage(
            {'initMarginAfter': 900_000.0, 'equityWithLoanAfter': 1_000_000.0},
            net_liquidation=100_000.0)
        assert result.approved is False
        assert 'leverage' in (result.reason or '')

    @pytest.mark.parametrize('net_liq', [0.0, -1.0, -100_000.0])
    def test_non_positive_net_liquidation_refuses(self, event_store, net_liq):
        """An unreadable/zero NetLiquidation refuses the open, exactly as
        evaluate() does for its own inputs (the 2026-07-26 flip)."""
        gate = self._gate(event_store, max_leverage=0.001, min_margin_cushion=0.99)
        result = gate.check_leverage(
            {'initMarginAfter': 999_999.0, 'equityWithLoanAfter': 1.0},
            net_liquidation=net_liq)
        assert result.approved is False
        assert 'NetLiquidation' in result.reason

    @pytest.mark.parametrize('net_liq', [0.5, 1.0])
    def test_net_liquidation_between_zero_and_one_is_still_checked(self, event_store, net_liq):
        """Kills mutants 21/32 (`> 0` -> `> 1`). A sub-$1 account is degenerate
        but the guard is `> 0`, so the branch must still run — the mutants make
        it silently skip for 0 < net_liq <= 1."""
        gate = self._gate(event_store, max_leverage=2.0)
        result = gate.check_leverage(
            {'initMarginAfter': 100.0, 'equityWithLoanAfter': 100.0},
            net_liquidation=net_liq)
        assert result.approved is False, 'leverage branch was skipped for a positive net_liq'
        assert 'leverage' in (result.reason or '')

    def test_absent_init_margin_must_not_be_treated_as_a_real_value(self, event_store):
        """Kills mutant 9 (`get('initMarginAfter', 0)` -> default 1).

        A missing key must mean 'no data', never a synthetic value fed into the
        arithmetic. Post-flip both paths refuse, so the discriminator is the
        REASON and the record: an injected default of 1 at net_liq=1.0 and a
        0.5x limit computes 1.0x and refuses on LEVERAGE ('exceeds'), while the
        real path refuses on MISSING DATA with the skip recorded."""
        gate = self._gate(event_store, max_leverage=0.5)
        result = gate.check_leverage({'equityWithLoanAfter': 0}, net_liquidation=1.0)
        assert result.approved is False
        assert 'initMarginAfter' in result.reason
        assert 'exceeds' not in result.reason
        assert result.checks['leverage'] == 'unevaluable:margin-data'

    def test_cushion_branch_runs_for_any_positive_net_liquidation(self, event_store):
        """Kills mutant 32 (cushion guard `> 0` -> `> 1`).

        Needs the LEVERAGE branch to pass while the CUSHION branch bites, at
        0 < net_liq <= 1 — my earlier case refused on leverage first and never
        reached the cushion guard at all."""
        gate = self._gate(event_store, max_leverage=2.0, min_margin_cushion=0.1)
        result = gate.check_leverage(
            {'initMarginAfter': 1.0, 'equityWithLoanAfter': 1.0}, net_liquidation=1.0)
        assert result.approved is False, 'cushion branch was skipped for a positive net_liq'
        assert 'cushion' in (result.reason or '')

    def test_skipped_leverage_is_RECORDED_not_silent(self, event_store):
        """The skip states survive the flip — now as the refusal's evidence
        rather than an approval's footnote."""
        gate = self._gate(event_store)
        result = gate.check_leverage({}, net_liquidation=1_000_000.0)
        assert result.approved is False
        assert result.checks['leverage'] == 'unevaluable:margin-data'
        assert result.checks['margin_cushion'] == 'unevaluable:margin-data'

    def test_non_positive_net_liq_records_why_both_branches_were_skipped(self, event_store):
        gate = self._gate(event_store)
        result = gate.check_leverage(
            {'initMarginAfter': 1.0, 'equityWithLoanAfter': 1.0}, net_liquidation=0.0)
        assert result.approved is False
        assert result.checks == {
            'leverage': 'unevaluable:net-liq',
            'margin_cushion': 'unevaluable:net-liq',
        }

    def test_passing_checks_are_recorded_as_pass(self, event_store):
        gate = self._gate(event_store, max_leverage=2.0, min_margin_cushion=0.1)
        result = gate.check_leverage(
            {'initMarginAfter': 150_000, 'equityWithLoanAfter': 180_000},
            net_liquidation=100_000)
        assert result.approved is True
        assert result.checks == {'leverage': 'pass', 'margin_cushion': 'pass'}

    def test_a_refusal_records_which_dimension_failed(self, event_store):
        gate = self._gate(event_store, max_leverage=2.0)
        result = gate.check_leverage(
            {'initMarginAfter': 900_000.0, 'equityWithLoanAfter': 1_000_000.0},
            net_liquidation=100_000.0)
        assert result.approved is False
        assert result.checks['leverage'] == 'fail'

    def test_a_cushion_refusal_records_the_cushion_dimension(self, event_store):
        """The cushion-fail branch records its own dimension.

        Added because mutation testing showed SEVEN survivors in exactly this
        branch (73-77, 80, 83): the key, the value and the checks= argument
        could all be corrupted or dropped and no test noticed. The leverage-fail
        record was asserted; its cushion twin was not — a gap I introduced by
        adding the record and only asserting one half of it."""
        gate = self._gate(event_store, max_leverage=2.0, min_margin_cushion=0.1)
        result = gate.check_leverage(
            {'initMarginAfter': 1.0, 'equityWithLoanAfter': 1.0}, net_liquidation=1.0)
        assert result.approved is False
        assert 'cushion' in (result.reason or '')
        assert result.checks == {'leverage': 'pass', 'margin_cushion': 'fail'}

    def test_empty_margin_impact_refuses(self, event_store):
        """whatIfOrder returning {} — the shape closest to a real IB failure —
        refuses instead of silently approving (the name of the old version of
        this test was literally 'approves_silently')."""
        gate = self._gate(event_store, max_leverage=0.001, min_margin_cushion=0.99)
        result = gate.check_leverage({}, net_liquidation=1_000_000.0)
        assert result.approved is False
        assert 'fail-closed' in result.reason


class TestEvaluateUnassertedSurface:
    """Mutation gaps in evaluate() — the gate's actual decision function.

    31 survivors sat here unexamined. These cover the three groups that matter:
    the tri-state record on the REFUSAL branches (now load-bearing — the
    placement chokepoint refuses an opening order whose record is empty or
    contains a 'fail'), the rate-limit lookback WINDOW, and the concentration
    boundaries. The remaining survivors are reason-string case/text mutants and
    a logging call; the fail-closed contract in those messages is pinned below
    by substring, the rest are cosmetic.
    """

    def test_max_open_orders_refusal_records_its_dimension(self, event_store):
        """Kills the max_open_orders key mutants (12, 13)."""
        gate = RiskGate(limits=RiskLimits(max_open_orders=1), event_store=event_store)
        result = gate.evaluate(_make_signal(), open_order_count=5,
                               portfolio_value=100_000.0, position_value=100.0)
        assert result.approved is False
        assert result.checks == {'max_open_orders': 'fail'}

    def test_daily_loss_refusal_records_its_dimension_and_carries_the_record(self, event_store):
        """Kills the daily_loss key/value mutants (48-52) and the two that drop
        the record entirely (55: checks=None, 58: the kwarg removed).

        The record is not cosmetic: subscribe_place_order_direct refuses an
        exposure-increasing order whose checks are empty or contain a 'fail', so
        a dropped record changes what the chokepoint can see."""
        gate = RiskGate(limits=RiskLimits(max_daily_loss=100.0), event_store=event_store)
        result = gate.evaluate(_make_signal(), daily_pnl=-500.0,
                               portfolio_value=100_000.0, position_value=100.0)
        assert result.approved is False
        assert result.checks == {'max_open_orders': 'pass', 'daily_loss': 'fail'}

    def test_fail_closed_refusals_say_they_are_fail_closed(self, event_store):
        """The refusal message is the only explanation an operator gets. Pins
        the contract-bearing substrings of both fail-closed paths (kills the
        case/text mutants on those two reasons: 41-45, 88-92, 106-109)."""
        gate = RiskGate(limits=RiskLimits(), event_store=event_store)

        unreadable_pnl = gate.evaluate(_make_signal(), daily_pnl_evaluable=False)
        assert unreadable_pnl.approved is False
        assert 'daily PnL could not be read' in unreadable_pnl.reason
        assert 'fail-closed' in unreadable_pnl.reason

        unreadable_nav = gate.evaluate(_make_signal(), portfolio_value_evaluable=False)
        assert unreadable_nav.approved is False
        assert 'NetLiquidation' in unreadable_nav.reason
        assert 'fail-closed' in unreadable_nav.reason

        no_price = gate.evaluate(_make_signal(), position_value_evaluable=False)
        assert no_price.approved is False
        assert 'no price available' in no_price.reason
        assert 'fail-closed' in no_price.reason

    def test_rate_limit_window_is_one_hour_not_longer(self, event_store):
        """Kills mutant 154 (`timedelta(hours=1)` -> `hours=2`).

        Nothing asserted the WINDOW of the rate limiter — only that it counts.
        A 90-minute-old submission must fall OUTSIDE the hour and therefore not
        count; under a 2-hour window it would, and the order would be refused."""
        event_store.append(TradingEvent(
            event_type=EventType.ORDER_SUBMITTED,
            timestamp=dt.datetime.now() - dt.timedelta(minutes=90),
            strategy_name='test_strat',
            conid=4391,
        ))
        gate = RiskGate(limits=RiskLimits(max_signals_per_hour=1), event_store=event_store)
        result = gate.evaluate(_make_signal(), portfolio_value=100_000.0, position_value=100.0)
        assert result.approved is True, 'a 90-minute-old order counted inside the 1h window'
        assert result.checks['order_rate'] == 'pass'

    def test_a_sub_unit_position_value_is_still_concentration_checked(self, event_store):
        """Kills mutant 111 (`position_value > 0` -> `> 1`). A fractional
        position value must still be evaluated, not silently skipped."""
        gate = RiskGate(limits=RiskLimits(max_position_size_pct=0.10), event_store=event_store)
        result = gate.evaluate(_make_signal(), portfolio_value=100.0, position_value=0.5)
        assert result.approved is True
        assert result.checks['concentration'] == 'pass', 'concentration was never evaluated'

    def test_a_sub_unit_portfolio_value_is_not_treated_as_zero(self, event_store):
        """Kills mutant 113 (`portfolio_value <= 0` -> `<= 1`). A tiny but
        POSITIVE portfolio must go through the ratio, not the degenerate branch:
        0.05/1.0 = 5% is within a 10% cap and must approve."""
        gate = RiskGate(limits=RiskLimits(max_position_size_pct=0.10), event_store=event_store)
        result = gate.evaluate(_make_signal(), portfolio_value=1.0, position_value=0.05)
        assert result.approved is True, 'a positive sub-$1 portfolio hit the degenerate branch'
        assert result.checks['concentration'] == 'pass'


class TestCheckInstrumentForwardsEveryField:
    """check_instrument must pass exchange AND sec_type through to the filter.

    Mutation found four survivors here (8, 9, 11, 12) that drop or null those
    arguments. Nothing verified the forwarding, so an exchange-based or
    sec_type-based denylist rule could silently stop applying while every
    symbol-based test kept passing — the filter would look enforced and be
    half-blind.
    """

    def test_exchange_denylist_reaches_the_filter(self, event_store):
        """Kills mutants 8 and 11 (exchange -> None / dropped)."""
        from trader.trading.trading_filter import TradingFilter
        gate = RiskGate(limits=RiskLimits(), event_store=event_store)
        gate.trading_filter = TradingFilter(deny_exchanges=['ASX'])

        denied = gate.check_instrument(symbol='BHP', exchange='ASX', sec_type='STK')
        assert denied.approved is False, 'exchange never reached the filter'
        assert 'ASX' in (denied.reason or '')

        allowed = gate.check_instrument(symbol='AMD', exchange='NASDAQ', sec_type='STK')
        assert allowed.approved is True

    def test_sec_type_denylist_reaches_the_filter(self, event_store):
        """Kills mutants 9 and 12 (sec_type -> None / dropped)."""
        from trader.trading.trading_filter import TradingFilter
        gate = RiskGate(limits=RiskLimits(), event_store=event_store)
        gate.trading_filter = TradingFilter(deny_sec_types=['CASH'])

        denied = gate.check_instrument(symbol='EUR', exchange='IDEALPRO', sec_type='CASH')
        assert denied.approved is False, 'sec_type never reached the filter'
        assert 'CASH' in (denied.reason or '')

        allowed = gate.check_instrument(symbol='AMD', exchange='NASDAQ', sec_type='STK')
        assert allowed.approved is True


class TestDailyTurnoverCap:
    """Cumulative OPENING notional per trading day — the only limit here that
    bounds total activity rather than a single order.

    Every other check is per-order, so open/close/open/close passes all of them
    indefinitely while bleeding commissions, slippage and market impact.
    max_signals_per_hour is the closest existing control and counts ORDERS, not
    value: 20/hour is trivial at $500 each and $1M at $50k each.

    The cap applies to OPENS only, which is what makes it compatible with "an
    exit is never refusable". You cannot churn without opening, so bounding
    opens bounds the loop while leaving every close untouchable.
    """

    def _gate(self, event_store, **limits):
        from trader.trading.risk_gate import RiskGate, RiskLimits
        return RiskGate(limits=RiskLimits(**limits), event_store=event_store)

    def _submit(self, event_store, strategy, notional, exit_class=False,
                evaluable=True, price=0.0, quantity=0.0):
        """Record an ORDER_SUBMITTED exactly as the executioner does."""
        import datetime as dt
        from trader.data.event_store import EventType, TradingEvent
        meta = {'notional': notional, 'notional_evaluable': evaluable}
        if exit_class:
            meta['exit_class'] = True
        event_store.append(TradingEvent(
            event_type=EventType.ORDER_SUBMITTED,
            timestamp=dt.datetime.now(),
            strategy_name=strategy,
            conid=4391, symbol='AMD', action='BUY',
            quantity=quantity, price=price, order_id=1, metadata=meta))

    def _evaluate(self, gate, name='test_strat', position_value=1_000.0, **kw):
        return gate.evaluate(
            _make_signal(name), open_order_count=0, daily_pnl=0.0,
            portfolio_value=1_000_000.0, position_value=position_value, **kw)

    def test_off_by_default(self, event_store):
        """Deploying this must be byte-identical until an operator opts in."""
        gate = self._gate(event_store)
        assert gate.limits.max_daily_open_notional == 0.0
        assert gate.limits.max_daily_open_notional_per_strategy == 0.0
        self._submit(event_store, 'test_strat', 5_000_000.0)
        assert self._evaluate(gate).approved is True

    def test_account_cap_refuses_once_the_day_is_spent(self, event_store):
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'a', 6_000.0)
        self._submit(event_store, 'b', 3_500.0)
        result = self._evaluate(gate, position_value=1_000.0)
        assert not result.approved
        assert result.checks['daily_turnover'] == 'fail'
        assert '9,500' in result.reason and '10,000' in result.reason

    def test_account_cap_allows_an_order_that_fits(self, event_store):
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'a', 6_000.0)
        result = self._evaluate(gate, position_value=1_000.0)
        assert result.approved is True
        assert result.checks['daily_turnover'] == 'pass'

    def test_the_cap_counts_the_order_being_evaluated(self, event_store):
        """Not just history. A cap that only looked backwards would always
        allow one more order of unbounded size."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        assert not self._evaluate(gate, position_value=10_001.0).approved
        assert self._evaluate(gate, position_value=9_999.0).approved

    def test_per_strategy_cap_isolates_a_runaway(self, event_store):
        """One strategy burning its budget must not refuse the others."""
        gate = self._gate(event_store, max_daily_open_notional_per_strategy=5_000.0)
        self._submit(event_store, 'runaway', 4_800.0)
        refused = self._evaluate(gate, name='runaway', position_value=500.0)
        assert not refused.approved
        assert refused.checks['daily_turnover'] == 'fail'
        # The refusal must name the strategy and both figures: it is the only
        # window the operator has into a cap they cannot otherwise see.
        assert 'runaway' in refused.reason
        assert '4,800' in refused.reason and '5,000' in refused.reason
        assert self._evaluate(gate, name='innocent', position_value=500.0).approved

    def test_the_account_cap_catches_what_per_strategy_caps_miss(self, event_store):
        """Ten strategies each just under their own cap. This is why both
        scopes exist rather than only the per-strategy one."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0,
                          max_daily_open_notional_per_strategy=5_000.0)
        for i in range(10):
            self._submit(event_store, f'strat_{i}', 900.0)
        result = self._evaluate(gate, name='strat_0', position_value=2_000.0)
        assert not result.approved
        assert 'account turnover' in result.reason

    def test_exit_class_submissions_do_not_consume_the_budget(self, event_store):
        """A session's own closes and protective stops must not spend the
        opening budget — otherwise holding positions would starve new entries
        and, worse, the cap would effectively penalise exiting."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        for _ in range(20):
            self._submit(event_store, 'test_strat', 5_000.0, exit_class=True)
        assert self._evaluate(gate, position_value=1_000.0).approved is True

    def test_an_unvaluable_order_is_refused_upstream_not_by_this_cap(self, event_store):
        """The concentration check already fails closed on a non-evaluable
        position value, so the turnover cap never has to. Pinned so nobody adds
        a second, unreachable fail-closed branch here believing there is a
        hole."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        result = self._evaluate(gate, position_value=0.0,
                                position_value_evaluable=False)
        assert not result.approved
        assert result.checks['concentration'] == 'unevaluable:position-value'
        assert 'daily_turnover' not in result.checks

    def test_a_zero_notional_order_contributes_nothing(self, event_store):
        """A zero notional does reach the cap (concentration allows it as
        skipped:zero-notional). It must add nothing rather than refuse."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'test_strat', 10_000.0)   # exactly at the cap
        assert self._evaluate(gate, position_value=0.0).approved is True

    def test_unvaluable_history_refuses_rather_than_undercounting(self, event_store):
        """A lower bound is not a bound. History written by a build that did not
        record notionals would silently make the day look cheap."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'test_strat', 0.0, evaluable=False, price=0.0)
        result = self._evaluate(gate, position_value=100.0)
        assert not result.approved
        assert result.checks['daily_turnover'] == 'unevaluable:turnover-history'
        assert 'lower bound' in result.reason

    def test_the_unset_price_sentinel_does_not_become_a_huge_turnover(self, event_store):
        """A market order recorded by an older build carries
        price=sys.float_info.max. Treating that as a price would put ~1e308 into
        the running total and refuse every open forever."""
        import sys as _sys
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'test_strat', 0.0, evaluable=False,
                     price=_sys.float_info.max, quantity=1.0)
        result = self._evaluate(gate, position_value=100.0)
        # Refused as UNVALUABLE, not as a 1e308 breach — the distinction is the
        # whole point: one is an honest "cannot tell", the other is nonsense
        # presented as fact.
        assert result.checks['daily_turnover'] == 'unevaluable:turnover-history'

    def test_a_limit_order_from_older_history_is_valued_from_its_price(self, event_store):
        """The fallback that keeps this from refusing everything on day one:
        pre-existing LIMIT submissions have a real price column."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'test_strat', 0.0, evaluable=False,
                     price=100.0, quantity=100.0)
        result = self._evaluate(gate, position_value=100.0)
        assert not result.approved
        assert result.checks['daily_turnover'] == 'fail', result.checks
        assert '10,000' in result.reason


    def test_a_tiny_cap_is_honoured_not_treated_as_off(self, event_store):
        """0.0 means OFF and anything above it is a real cap. A boundary
        written as `<= 1` would silently disable a $1 cap — and a cap that
        cannot be set small is a cap that cannot be smoke-tested in place."""
        gate = self._gate(event_store, max_daily_open_notional=1.0)
        assert not self._evaluate(gate, position_value=500.0).approved
        gate = self._gate(event_store, max_daily_open_notional_per_strategy=1.0)
        assert not self._evaluate(gate, position_value=500.0).approved

    def test_an_order_landing_exactly_on_the_cap_is_allowed(self, event_store):
        """The cap is a ceiling, not a strict inequality: spending exactly the
        budget is permitted, exceeding it is not. Pinned because `>` vs `>=`
        here is invisible in every test that does not sit on the boundary."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'test_strat', 9_000.0)
        assert self._evaluate(gate, position_value=1_000.0).approved is True
        assert not self._evaluate(gate, position_value=1_000.01).approved

    def test_the_same_boundary_holds_for_the_per_strategy_cap(self, event_store):
        gate = self._gate(event_store, max_daily_open_notional_per_strategy=5_000.0)
        self._submit(event_store, 'test_strat', 4_000.0)
        assert self._evaluate(gate, position_value=1_000.0).approved is True
        assert not self._evaluate(gate, position_value=1_000.01).approved

    def test_a_strategy_with_no_history_starts_from_zero(self, event_store):
        """Not from a placeholder. A default of anything but 0.0 would make a
        strategy's first order of the day already partly spent."""
        gate = self._gate(event_store, max_daily_open_notional_per_strategy=1_000.0)
        assert self._evaluate(gate, name='never_traded',
                              position_value=1_000.0).approved is True

    def test_a_sub_dollar_order_still_counts_toward_the_cap(self, event_store):
        """Small orders are exactly how a churn loop stays under per-order
        limits, so they must not be rounded out of the total."""
        gate = self._gate(event_store, max_daily_open_notional=10.0)
        self._submit(event_store, 'test_strat', 9.5)
        assert not self._evaluate(gate, position_value=0.75).approved

    def test_only_order_submissions_count(self, event_store):
        """SIGNAL, ORDER_FILLED and ORDER_CANCELLED events must not be summed.
        Counting fills as well as submissions would double every order, and
        counting signals would charge the budget for orders never placed."""
        import datetime as dt
        from trader.data.event_store import EventType, TradingEvent
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        for event_type in (EventType.SIGNAL, EventType.ORDER_FILLED,
                           EventType.ORDER_CANCELLED):
            event_store.append(TradingEvent(
                event_type=event_type, timestamp=dt.datetime.now(),
                strategy_name='test_strat', conid=4391, symbol='AMD',
                action='BUY', quantity=1000.0, price=500.0, order_id=1,
                metadata={'notional': 500_000.0, 'notional_evaluable': True}))
        result = self._evaluate(gate, position_value=1_000.0)
        assert result.approved is True, (
            f'non-submission events were counted: {result.reason}')


    def test_an_exit_stops_nothing_that_follows_it(self, event_store):
        """`continue`, not `break`. A single exit-class submission early in the
        day would otherwise stop the count dead and make the rest of the day
        free — and exits are common, so the budget would nearly always look
        empty."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        # An exit on BOTH sides of the opening submission, so this holds
        # whichever direction the store iterates (it is newest-first today).
        self._submit(event_store, 'test_strat', 5_000.0, exit_class=True)
        self._submit(event_store, 'test_strat', 9_500.0)
        self._submit(event_store, 'test_strat', 5_000.0, exit_class=True)
        result = self._evaluate(gate, position_value=1_000.0)
        assert not result.approved, 'an exit stopped the count of what followed it'
        assert '9,500' in result.reason

    def test_a_malformed_recorded_notional_does_not_crash_the_gate(self, event_store):
        """Metadata is JSON from disk, so it can be anything. A gate that raises
        while reading history refuses nothing and breaks every open."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'test_strat', 'not-a-number', price=0.0)
        result = self._evaluate(gate, position_value=100.0)
        assert not result.approved
        assert result.checks['daily_turnover'] == 'unevaluable:turnover-history'

    def test_a_nonsense_recorded_notional_is_not_trusted(self, event_store):
        """NaN and negative recorded notionals must be treated as unreadable,
        not folded into the total (NaN would poison every comparison after it,
        and a negative would hand back budget)."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        for bad in (float('nan'), float('inf'), -5_000.0):
            store_gate = self._gate(event_store, max_daily_open_notional=10_000.0)
            self._submit(event_store, 'test_strat', bad, price=0.0)
            result = self._evaluate(store_gate, position_value=100.0)
            assert result.checks['daily_turnover'] == 'unevaluable:turnover-history', bad
            assert not result.approved

    def test_a_genuinely_zero_notional_record_is_readable_not_unvaluable(self, event_store):
        """Zero is a value. Treating a recorded $0 as unreadable would refuse
        every open for the rest of the day over an order that cost nothing."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'test_strat', 0.0, evaluable=True)
        result = self._evaluate(gate, position_value=100.0)
        assert result.approved is True, result.reason
        assert result.checks['daily_turnover'] == 'pass'

    def test_the_refusal_says_how_many_records_it_could_not_read(self, event_store):
        """The operator has to know whether this is one stale row or the whole
        day, because the fix differs."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'test_strat', 0.0, evaluable=False, price=0.0)
        self._submit(event_store, 'test_strat', 0.0, evaluable=False, price=0.0)
        result = self._evaluate(gate, position_value=100.0)
        assert result.reason.startswith('2 of'), result.reason

    def test_the_lower_bound_in_the_refusal_counts_what_it_could_read(self, event_store):
        """An unreadable row must not stop the sum: the operator is told
        '>= $X', and X has to include everything that WAS readable."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'test_strat', 0.0, evaluable=False, price=0.0)
        self._submit(event_store, 'test_strat', 5_000.0)
        result = self._evaluate(gate, position_value=100.0)
        assert '5,000' in result.reason, result.reason

    def test_a_strategys_orders_accumulate_against_its_own_budget(self, event_store):
        """Two orders from one strategy must sum. Keying the per-strategy total
        wrongly would leave each strategy showing only its most recent order."""
        gate = self._gate(event_store, max_daily_open_notional_per_strategy=10_000.0)
        self._submit(event_store, 'test_strat', 4_000.0)
        self._submit(event_store, 'test_strat', 5_500.0)
        result = self._evaluate(gate, position_value=1_000.0)
        assert not result.approved, 'the two orders did not accumulate'
        assert result.checks['daily_turnover'] == 'fail'
        assert '9,500' in result.reason and '1,000' in result.reason

    def test_every_refusal_carries_a_reason(self, event_store):
        """A refusal with no reason is unactionable, and this gate's refusals
        are the operator's only window into a cap they cannot see otherwise."""
        gate = self._gate(event_store, max_daily_open_notional=100.0,
                          max_daily_open_notional_per_strategy=100.0)
        self._submit(event_store, 'test_strat', 500.0)
        result = self._evaluate(gate, position_value=100.0)
        assert not result.approved
        assert result.reason and result.reason.strip()
        gate2 = self._gate(event_store, max_daily_open_notional=100.0)
        result2 = self._evaluate(gate2, position_value=100.0)
        assert result2.reason and result2.reason.strip()


    def _submit_legacy(self, event_store, strategy, price, quantity):
        """An ORDER_SUBMITTED exactly as builds before 2026-07-27 wrote it:
        no notional keys at all, and price = order.lmtPrice."""
        import datetime as dt
        from trader.data.event_store import EventType, TradingEvent
        event_store.append(TradingEvent(
            event_type=EventType.ORDER_SUBMITTED, timestamp=dt.datetime.now(),
            strategy_name=strategy, conid=4391, symbol='AMD', action='BUY',
            quantity=quantity, price=price, order_id=0, metadata={}))

    def test_history_from_before_notionals_does_not_block_the_day(self, event_store):
        """THE footgun, fixed. Enabling the cap used to refuse every open for
        the rest of the day, because the day already contained submissions
        written by a build that recorded no notional. That turns "switch on a
        safety control" into "stop trading", which is how a control gets
        switched off and left off.

        Old history is a migration artifact, not a live failure. Proceed on the
        lower bound, say so loudly, and let it clear at the day boundary.
        """
        import sys as _sys
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        # A market order as the old build recorded it: the unset sentinel.
        self._submit_legacy(event_store, 'test_strat', _sys.float_info.max, 1.0)
        result = self._evaluate(gate, position_value=1_000.0)
        assert result.approved is True, result.reason
        assert result.checks['daily_turnover'] == 'pass'

    def test_old_history_still_counts_whatever_can_be_valued(self, event_store):
        """Proceeding on a lower bound must still USE the bound — a legacy
        limit order has a real price column and belongs in the total."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit_legacy(event_store, 'test_strat', 100.0, 99.0)   # $9,900
        result = self._evaluate(gate, position_value=500.0)
        assert not result.approved
        assert '9,900' in result.reason

    def test_a_live_valuation_failure_still_refuses(self, event_store):
        """The fail-closed case is preserved. If THIS build placed an order it
        could not value, the input the cap runs on is genuinely degraded, and
        that is worth refusing over — it points at market data, not history."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'test_strat', 0.0, evaluable=False, price=0.0)
        result = self._evaluate(gate, position_value=100.0)
        assert not result.approved
        assert result.checks['daily_turnover'] == 'unevaluable:turnover-history'
        assert 'market data' in result.reason, result.reason

    def test_a_fill_values_a_submission_that_could_not_be_valued(self, event_store):
        """A market order placed with no cached price cannot be valued at
        placement, but its FILL carries a real avgFillPrice. Using it shrinks
        the refusing case to orders that never traded — which contributed no
        turnover anyway."""
        import datetime as dt
        from trader.data.event_store import EventType, TradingEvent
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        event_store.append(TradingEvent(
            event_type=EventType.ORDER_SUBMITTED, timestamp=dt.datetime.now(),
            strategy_name='test_strat', conid=4391, symbol='AMD', action='BUY',
            quantity=100.0, price=0.0, order_id=777,
            metadata={'notional': 0.0, 'notional_evaluable': False}))
        event_store.append(TradingEvent(
            event_type=EventType.ORDER_FILLED, timestamp=dt.datetime.now(),
            strategy_name='test_strat', conid=4391, symbol='AMD', action='BUY',
            quantity=100.0, price=99.0, order_id=777, metadata={}))
        result = self._evaluate(gate, position_value=200.0)
        assert not result.approved, 'the fill price should have valued it at $9,900'
        assert result.checks['daily_turnover'] == 'fail'
        assert '9,900' in result.reason

    def test_forex_is_exempt(self, event_store):
        """A currency conversion is not a position, exactly as the
        concentration check treats it. A known limitation, not a claim that
        forex turnover does not matter."""
        gate = self._gate(event_store, max_daily_open_notional=100.0)
        self._submit(event_store, 'test_strat', 50_000.0)
        assert self._evaluate(gate, position_value=50_000.0,
                              sec_type='CASH').approved is True


class TestTurnoverCapArithmeticBoundaries:
    """The full mutation pass put risk_gate at 89.2%, below its 91.9% floor,
    with survivors concentrated in _check_daily_turnover and
    _daily_open_notional. The module GREW by 47 mutants when the turnover caps
    landed - the tests still catch everything they used to, but the new code
    arrived with thinner coverage than the module's average, and re-baselining
    at the lower number would have banked that erosion.

    These pin the arithmetic boundaries rather than the behaviour the existing
    class already covers.
    """

    def _gate(self, event_store, **limits):
        from trader.trading.risk_gate import RiskGate, RiskLimits
        return RiskGate(limits=RiskLimits(**limits), event_store=event_store)

    def _submit(self, event_store, strategy, notional, exit_class=False,
                evaluable=True):
        import datetime as dt
        from trader.data.event_store import EventType, TradingEvent
        meta = {'notional': notional, 'notional_evaluable': evaluable}
        if exit_class:
            meta['exit_class'] = True
        event_store.append(TradingEvent(
            event_type=EventType.ORDER_SUBMITTED, timestamp=dt.datetime.now(),
            strategy_name=strategy, conid=4391, symbol='AMD', action='BUY',
            quantity=0.0, price=0.0, order_id=1, metadata=meta))

    def _evaluate(self, gate, name='test_strat', position_value=1_000.0, **kw):
        return gate.evaluate(
            _make_signal(name), open_order_count=0, daily_pnl=0.0,
            portfolio_value=1_000_000.0, position_value=position_value, **kw)

    def test_the_cap_is_a_strict_ceiling_not_an_inclusive_one(self):
        """Spending EXACTLY the cap is allowed; exceeding it is not. A mutant
        flipping > to >= refuses the order that lands precisely on the limit,
        which is the one an operator sizing to the cap will send."""
        from trader.data.event_store import EventStore
        import tempfile, os
        for spent, order, expect in ((9_000.0, 1_000.0, True),
                                     (9_000.0, 1_001.0, False)):
            path = os.path.join(tempfile.mkdtemp(), 'e.duckdb')
            es = EventStore(path)
            gate = self._gate(es, max_daily_open_notional=10_000.0)
            self._submit(es, 'a', spent)
            got = self._evaluate(gate, position_value=order).approved
            assert got is expect, (
                f'spent {spent} + order {order} vs cap 10,000: '
                f'approved={got}, expected {expect}')

    def test_an_exit_submission_never_counts_toward_the_cap(self, event_store):
        """The compatibility rule: bounding opens must never be reachable by
        closing. A mutant that dropped the exit_class filter would let a day of
        exits exhaust the budget for opens."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        for _ in range(20):
            self._submit(event_store, 'a', 5_000.0, exit_class=True)
        assert self._evaluate(gate, position_value=9_000.0).approved is True

    def test_a_zero_notional_order_consumes_no_budget(self, event_store):
        """Concentration records skipped:zero-notional and allows it, so such
        an order reaches here; it must contribute nothing rather than being
        treated as unvaluable."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'a', 9_999.0)
        assert self._evaluate(gate, position_value=0.0).approved is True

    def test_the_per_strategy_cap_is_scoped_to_its_own_strategy(self, event_store):
        """A mutant summing across strategies would make one strategy's
        activity refuse another's - the caps would stop being per-strategy
        while still passing any single-strategy test."""
        gate = self._gate(event_store, max_daily_open_notional_per_strategy=5_000.0)
        self._submit(event_store, 'other', 4_900.0)
        assert self._evaluate(gate, name='mine', position_value=4_000.0).approved is True

    def test_both_caps_apply_and_either_can_refuse(self, event_store):
        """With both set, the binding one refuses. A mutant checking only the
        account cap passes every per-strategy test in isolation."""
        gate = self._gate(event_store, max_daily_open_notional=1_000_000.0,
                          max_daily_open_notional_per_strategy=5_000.0)
        self._submit(event_store, 'test_strat', 4_900.0)
        r = self._evaluate(gate, position_value=1_000.0)
        assert r.approved is False
        assert 'test_strat' in r.reason

    def test_a_negative_position_value_does_not_credit_the_budget(self, event_store):
        """`this_order = position_value if > 0 else 0.0`. A mutant dropping the
        guard would let a negative value SUBTRACT from the day's spend and
        create budget out of nothing."""
        gate = self._gate(event_store, max_daily_open_notional=10_000.0)
        self._submit(event_store, 'a', 9_999.0)
        assert self._evaluate(gate, position_value=-50_000.0).approved is True
        # ...and it must not have manufactured room for a real order after it.
        self._submit(event_store, 'a', 1.0)
        assert self._evaluate(gate, position_value=5_000.0).approved is False


class TestDailyOpenNotionalAccumulation:
    """`_daily_open_notional` carried 12 survivors. It reads history off disk to
    survive restarts, which means every value it touches is JSON of arbitrary
    shape - and a gate that RAISES while reading history refuses nothing and
    breaks every open. These pin the accumulation itself."""

    def _gate(self, event_store, **limits):
        from trader.trading.risk_gate import RiskGate, RiskLimits
        return RiskGate(limits=RiskLimits(**limits), event_store=event_store)

    def _raw(self, event_store, strategy, meta, order_id=1, price=0.0,
             quantity=0.0):
        import datetime as dt
        from trader.data.event_store import EventType, TradingEvent
        event_store.append(TradingEvent(
            event_type=EventType.ORDER_SUBMITTED, timestamp=dt.datetime.now(),
            strategy_name=strategy, conid=4391, symbol='AMD', action='BUY',
            quantity=quantity, price=price, order_id=order_id, metadata=meta))

    def test_notionals_are_summed_not_maxed_or_counted(self, event_store):
        """Three orders of 1,000 must total 3,000. A mutant taking a max would
        report 1,000; one counting orders would report 3."""
        gate = self._gate(event_store)
        for i in range(3):
            self._raw(event_store, 'a',
                      {'notional': 1_000.0, 'notional_evaluable': True},
                      order_id=i + 1)
        total, per_strategy, unvaluable, legacy = gate._daily_open_notional()
        assert total == pytest.approx(3_000.0)
        assert per_strategy['a'] == pytest.approx(3_000.0)
        assert unvaluable == 0 and legacy == 0

    def test_per_strategy_totals_are_kept_apart(self, event_store):
        gate = self._gate(event_store)
        self._raw(event_store, 'a', {'notional': 1_000.0,
                                     'notional_evaluable': True}, order_id=1)
        self._raw(event_store, 'b', {'notional': 400.0,
                                     'notional_evaluable': True}, order_id=2)
        total, per_strategy, _, _ = gate._daily_open_notional()
        assert total == pytest.approx(1_400.0)
        assert per_strategy['a'] == pytest.approx(1_000.0)
        assert per_strategy['b'] == pytest.approx(400.0)

    def test_legacy_and_unvaluable_are_counted_separately(self, event_store):
        """The distinction the design turns on: legacy is history from before
        the feature (proceed on a lower bound), unvaluable is THIS build failing
        to value an order it placed (fail closed). A mutant merging them turns
        'enable the cap' into 'no opens for the rest of today'."""
        gate = self._gate(event_store)
        self._raw(event_store, 'a', {}, order_id=1)                  # legacy
        self._raw(event_store, 'a', {'notional': None,
                                     'notional_evaluable': False}, order_id=2)
        _, _, unvaluable, legacy = gate._daily_open_notional()
        assert legacy == 1, 'metadata with no notional key at all is LEGACY'
        assert unvaluable == 1, 'an explicit not-evaluable stamp is LIVE failure'

    def test_a_malformed_notional_does_not_raise(self, event_store):
        """Metadata is JSON off disk and can be any shape. A gate that raises
        while reading history refuses nothing and breaks every open."""
        gate = self._gate(event_store)
        for i, bad in enumerate(['abc', {}, [], float('nan'), float('inf'), -5.0]):
            self._raw(event_store, 'a',
                      {'notional': bad, 'notional_evaluable': True},
                      order_id=i + 1)
        total, _, unvaluable, _ = gate._daily_open_notional()
        assert total >= 0.0 and total < 1e12
        assert unvaluable >= 1, 'unusable values must be COUNTED, not ignored'

    def test_a_fill_price_rescues_a_submission_that_could_not_be_valued(
            self, event_store):
        """Most submissions that matter do fill, and a fill carries a real
        avgFillPrice - so the unvaluable set shrinks to orders that never
        traded. A mutant dropping this makes the cap fail closed far more often
        than it should."""
        import datetime as dt
        from trader.data.event_store import EventType, TradingEvent
        gate = self._gate(event_store)
        # A real quantity: the rescue values the order as price x quantity, so
        # a zero-quantity submission has nothing to rescue and correctly stays
        # unvaluable. (My first version of this test omitted the quantity and
        # blamed the implementation.)
        self._raw(event_store, 'a', {'notional': None,
                                     'notional_evaluable': False},
                  order_id=7, quantity=10.0)
        event_store.append(TradingEvent(
            event_type=EventType.ORDER_FILLED, timestamp=dt.datetime.now(),
            strategy_name='a', conid=4391, symbol='AMD', action='BUY',
            quantity=10.0, price=50.0, order_id=7, metadata={}))
        total, _, unvaluable, _ = gate._daily_open_notional()
        assert unvaluable == 0, 'the fill should have valued it'
        assert total > 0.0
