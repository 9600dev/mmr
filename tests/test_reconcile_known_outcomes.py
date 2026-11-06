"""Reconcile trusts the order journal for EXECUTED proposals IB no longer reports."""
import datetime as dt
from types import SimpleNamespace

from trader.trading.reconciliation import reconcile

NOW = dt.datetime(2026, 11, 6, 12, 0)


def _p(pid, oids):
    return SimpleNamespace(id=pid, status='EXECUTED', order_ids=oids, symbol='F', action='BUY',
                           quantity=1.0, updated_at=NOW - dt.timedelta(hours=20))


def test_journal_known_outcome_is_info_not_warning():
    rep = reconcile([_p(154, [1062]), _p(155, [1067])], [], [], [], now=NOW,
                    known_outcomes={154: 'cancelled_unfilled', 155: 'filled'})
    kinds = {(f.proposal_id, f.kind, f.severity) for f in rep.findings}
    assert kinds == {(154, 'executed_outcome_known', 'info'), (155, 'executed_outcome_known', 'info')}
    assert not rep.diverged


def test_unknown_outcome_still_warns():
    rep = reconcile([_p(154, [1062])], [], [], [], now=NOW, known_outcomes={154: 'unknown'})
    assert [(f.kind, f.severity) for f in rep.findings] == [('executed_no_broker_record', 'warning')]
    rep2 = reconcile([_p(154, [1062])], [], [], [], now=NOW)
    assert [f.kind for f in rep2.findings] == ['executed_no_broker_record']
