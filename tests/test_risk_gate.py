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


class TestCheckLeverageMissingData:
    """The leverage check's behaviour when margin data is absent or degenerate.

    Mutation testing found five survivors clustered here (check_leverage mutants
    9, 12, 14, 21, 32) — every one a change to a missing-key default or a guard
    boundary. Nothing exercised those paths, so the check's behaviour with
    unreadable inputs was entirely unspecified.

    It matters because the answer is SILENTLY APPROVE: absent margin keys or a
    non-positive NetLiquidation skip both branches and return approved, with
    nothing recorded to say the leverage limit was never applied. These tests
    pin that behaviour so it cannot drift unnoticed; whether it SHOULD be
    fail-closed (as evaluate() is for its own inputs) is a separate policy
    decision, deliberately not pre-empted here.
    """

    def _gate(self, event_store, max_leverage=2.0, min_margin_cushion=0.1):
        return RiskGate(
            limits=RiskLimits(max_leverage=max_leverage, min_margin_cushion=min_margin_cushion),
            event_store=event_store,
        )

    def test_missing_init_margin_key_skips_the_leverage_branch(self, event_store):
        """Kills mutant 9 (default 0 -> 1): with the key absent the branch must
        not run at all, regardless of how breachy net_liq would make it look."""
        gate = self._gate(event_store, max_leverage=0.001)   # any real check would refuse
        result = gate.check_leverage({'equityWithLoanAfter': 0}, net_liquidation=1_000_000.0)
        assert result.approved is True

    def test_missing_equity_key_skips_the_cushion_branch(self, event_store):
        """Kills mutants 12/14 (default 0 -> None / omitted)."""
        gate = self._gate(event_store, min_margin_cushion=0.99)  # any real check would refuse
        result = gate.check_leverage({'initMarginAfter': 1.0}, net_liquidation=1_000_000.0)
        assert result.approved is True

    def test_leverage_is_enforced_when_both_keys_are_present(self, event_store):
        """The control: with data present the limit really does bite."""
        gate = self._gate(event_store, max_leverage=2.0)
        result = gate.check_leverage(
            {'initMarginAfter': 900_000.0, 'equityWithLoanAfter': 1_000_000.0},
            net_liquidation=100_000.0)
        assert result.approved is False
        assert 'leverage' in (result.reason or '')

    @pytest.mark.parametrize('net_liq', [0.0, -1.0, -100_000.0])
    def test_non_positive_net_liquidation_skips_every_branch(self, event_store, net_liq):
        """An unreadable/zero NetLiquidation silently approves — the fail-OPEN
        that evaluate() explicitly refuses for its own inputs."""
        gate = self._gate(event_store, max_leverage=0.001, min_margin_cushion=0.99)
        result = gate.check_leverage(
            {'initMarginAfter': 999_999.0, 'equityWithLoanAfter': 1.0},
            net_liquidation=net_liq)
        assert result.approved is True

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

        Chosen so the two differ: with net_liq=1.0 and a 0.5x limit, a default of
        1 computes 1/1.0 = 1.0x and REFUSES, while the real default of 0 skips the
        branch and approves. My first attempt at this test used a large net_liq,
        where the injected 1 produced a ~0 leverage that passed anyway — both
        approved, and the mutant survived. A missing key must mean 'no data',
        never a synthetic value that can trip or satisfy a limit."""
        gate = self._gate(event_store, max_leverage=0.5)
        result = gate.check_leverage({'equityWithLoanAfter': 0}, net_liquidation=1.0)
        assert result.approved is True

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
        """The point of option B: outcomes unchanged, but a skip is now visible.

        Previously an absent initMarginAfter returned a bare approval and the
        audit trail could not distinguish 'leverage checked and fine' from
        'leverage never checked'."""
        gate = self._gate(event_store)
        result = gate.check_leverage({}, net_liquidation=1_000_000.0)
        assert result.approved is True
        assert result.checks['leverage'] == 'skipped:no-margin-data'
        assert result.checks['margin_cushion'] == 'skipped:no-equity-data'

    def test_non_positive_net_liq_records_why_both_branches_were_skipped(self, event_store):
        gate = self._gate(event_store)
        result = gate.check_leverage(
            {'initMarginAfter': 1.0, 'equityWithLoanAfter': 1.0}, net_liquidation=0.0)
        assert result.approved is True
        assert result.checks == {
            'leverage': 'skipped:net-liq-not-evaluable',
            'margin_cushion': 'skipped:net-liq-not-evaluable',
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

    def test_empty_margin_impact_approves_silently(self, event_store):
        """whatIfOrder returning {} — the shape closest to a real IB failure."""
        gate = self._gate(event_store, max_leverage=0.001, min_margin_cushion=0.99)
        assert gate.check_leverage({}, net_liquidation=1_000_000.0).approved is True


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
