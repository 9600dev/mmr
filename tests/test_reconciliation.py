"""Tests for the broker-truth reconciliation core (pure function)."""
import datetime as dt
import types

from trader.trading.reconciliation import reconcile


def _prop(pid, status, symbol='AAPL', action='BUY', order_ids=None, age_days=0):
    return types.SimpleNamespace(
        id=pid, status=status, symbol=symbol, action=action,
        quantity=10.0, order_ids=order_ids or [],
        updated_at=dt.datetime(2026, 1, 10) - dt.timedelta(days=age_days),
    )


_NOW = dt.datetime(2026, 1, 10)


def _kinds(report):
    return {f.kind for f in report.findings}


class TestExecutedProposals:
    def test_executed_and_filled_is_clean(self):
        r = reconcile(
            [_prop(1, 'EXECUTED', order_ids=[100])],
            open_orders=[], executions=[{'order_id': 100, 'symbol': 'AAPL', 'side': 'BOT'}],
            positions=[], now=_NOW)
        assert r.findings == []

    def test_executed_but_still_working_flags(self):
        r = reconcile(
            [_prop(1, 'EXECUTED', order_ids=[100])],
            open_orders=[{'order_id': 100, 'symbol': 'AAPL', 'action': 'BUY',
                          'orderType': 'MKT', 'status': 'Submitted', 'conId': 1}],
            executions=[], positions=[], now=_NOW)
        assert 'executed_still_working' in _kinds(r)

    def test_executed_no_broker_record_flags(self):
        r = reconcile(
            [_prop(1, 'EXECUTED', order_ids=[100])],
            open_orders=[], executions=[], positions=[], now=_NOW)
        assert 'executed_no_broker_record' in _kinds(r)

    def test_old_executed_proposal_is_skipped(self):
        # Too old to verify against same-session executions — must not false-flag.
        r = reconcile(
            [_prop(1, 'EXECUTED', order_ids=[100], age_days=30)],
            open_orders=[], executions=[], positions=[], now=_NOW)
        assert r.findings == []
        assert r.checked_proposals == 0


class TestApprovedProposals:
    def test_approved_with_working_order_is_critical(self):
        r = reconcile(
            [_prop(1, 'APPROVED')],
            open_orders=[{'order_id': 100, 'symbol': 'AAPL', 'action': 'BUY',
                          'orderType': 'MKT', 'status': 'Submitted', 'conId': 1}],
            executions=[], positions=[], now=_NOW)
        f = next(f for f in r.findings if f.proposal_id == 1)
        assert f.kind == 'approved_order_working' and f.severity == 'critical'

    def test_approved_with_matching_fill_is_critical(self):
        r = reconcile(
            [_prop(1, 'APPROVED')],
            open_orders=[], executions=[{'order_id': 9, 'symbol': 'AAPL', 'side': 'BOT'}],
            positions=[], now=_NOW)
        assert 'approved_likely_filled' in _kinds(r)

    def test_approved_no_record_is_warning(self):
        r = reconcile(
            [_prop(1, 'APPROVED')],
            open_orders=[], executions=[], positions=[], now=_NOW)
        assert 'approved_no_broker_record' in _kinds(r)


class TestPositionProtection:
    def test_unprotected_long_flags(self):
        r = reconcile(
            [], open_orders=[], executions=[],
            positions=[{'conId': 1, 'symbol': 'AAPL', 'position': 100}], now=_NOW)
        assert 'unprotected_position' in _kinds(r)

    def test_protected_long_is_clean(self):
        r = reconcile(
            [], open_orders=[{'order_id': 5, 'symbol': 'AAPL', 'action': 'SELL',
                              'orderType': 'STP', 'status': 'Submitted', 'conId': 1}],
            executions=[],
            positions=[{'conId': 1, 'symbol': 'AAPL', 'position': 100}], now=_NOW)
        assert 'unprotected_position' not in _kinds(r)

    def test_short_needs_buy_protective(self):
        # A short protected only by a same-side SELL stop is still unprotected.
        r = reconcile(
            [], open_orders=[{'order_id': 5, 'symbol': 'AAPL', 'action': 'SELL',
                              'orderType': 'STP', 'status': 'Submitted', 'conId': 1}],
            executions=[],
            positions=[{'conId': 1, 'symbol': 'AAPL', 'position': -100}], now=_NOW)
        assert 'unprotected_position' in _kinds(r)

    def test_flat_position_ignored(self):
        r = reconcile(
            [], open_orders=[], executions=[],
            positions=[{'conId': 1, 'symbol': 'AAPL', 'position': 0}], now=_NOW)
        assert r.checked_positions == 0


def test_report_diverged_flag_and_dict():
    r = reconcile([_prop(1, 'APPROVED')], [], [], [], now=_NOW)
    assert r.diverged is True
    d = r.to_dict()
    assert d['diverged'] is True and len(d['findings']) == 1
