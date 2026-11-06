"""Reconcile: stale APPROVED claims are always checked; sleeve members without
stops are info, not warnings (2026-11-05: checked_proposals=0 with #74 APPROVED
since July; 15 'unprotected_position' warnings for a stop-less rebalance sleeve)."""
import datetime as dt
from types import SimpleNamespace

from trader.trading.reconciliation import reconcile


def _p(pid, status, days_old, symbol='QBTS', action='BUY', order_ids=()):
    ts = dt.datetime.now() - dt.timedelta(days=days_old)
    return SimpleNamespace(id=pid, status=status, symbol=symbol, action=action, quantity=1,
                           order_ids=list(order_ids), updated_at=ts, created_at=ts)


def test_old_approved_is_checked():
    r = reconcile([_p(74, 'APPROVED', 100)], [], [], [])
    assert r.checked_proposals == 1
    assert [f.kind for f in r.findings] == ['approved_no_broker_record']


def test_old_executed_still_skipped():
    r = reconcile([_p(1, 'EXECUTED', 100, order_ids=[5])], [], [], [])
    assert r.checked_proposals == 0


def test_sleeve_member_without_stop_is_info():
    positions = [{'conId': 1, 'symbol': 'KR', 'position': 14.0},
                 {'conId': 2, 'symbol': 'F', 'position': 1.0}]
    r = reconcile([], [], [], positions, unprotected_by_design={'KR'})
    kinds = {f.symbol: (f.kind, f.severity) for f in r.findings}
    assert kinds['KR'] == ('unprotected_by_design', 'info')
    assert kinds['F'] == ('unprotected_position', 'warning')
    assert r.diverged  # F still diverges


def test_only_by_design_findings_do_not_diverge():
    r = reconcile([], [], [], [{'conId': 1, 'symbol': 'KR', 'position': 14.0}],
                  unprotected_by_design={'KR'})
    assert not r.diverged
