"""D13 journal: schema, reads that never create it, idempotent inserts, receipt matching, reconcile refusals."""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import pytest

from rebalance_fakes import T0, FakeGateway, evidence, executed, filled_order, observed, receipt
from trader.rebalance.fills import (
    SLEEVE_FILLS_DDL, ExecutionMismatch, ExecutionsIncomplete, SleeveFillsStore, WorkingOrder, match_executions,
    reconcile_sleeve, submitted_quantity, trade_date_of,
)
from trader.rebalance.plan import Refusal
from trader.rebalance.ports import Fill

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 29, 15, 0, tzinfo=UTC)


def _fill(exec_id, ticker='AAPL', qty=25.0, price=200.0, pid=1, sleeve='s2p'):
    return Fill(sleeve=sleeve, ticker=ticker, trade_date=dt.date(2026, 9, 1), quantity=qty, price=price,
                commission=1.0, exec_id=exec_id, order_id='901', proposal_id=pid,
                recorded_at=dt.datetime(2026, 9, 1, 15, 0, tzinfo=UTC))


def tables(path: str) -> set[str]:
    if not Path(path).exists():          # a refusal before any connection leaves no file at all
        return set()
    con = duckdb.connect(path, read_only=True)
    try:
        return {r[0] for r in con.execute('SELECT table_name FROM duckdb_tables()').fetchall()}
    finally:
        con.close()


# ---------------------------------------------------------------------------
# the store
# ---------------------------------------------------------------------------

def test_ddl_matches_the_d13_schema(tmp_path):
    con = duckdb.connect(str(tmp_path / 'schema.duckdb'))
    con.execute(SLEEVE_FILLS_DDL)
    cols = {row[0]: row[1] for row in con.execute('DESCRIBE sleeve_fills').fetchall()}
    assert cols == {'sleeve': 'VARCHAR', 'ticker': 'VARCHAR', 'trade_date': 'DATE', 'quantity': 'DOUBLE',
                    'price': 'DOUBLE', 'commission': 'DOUBLE', 'exec_id': 'VARCHAR', 'order_id': 'VARCHAR',
                    'proposal_id': 'BIGINT', 'recorded_at': 'TIMESTAMP WITH TIME ZONE'}
    pk = con.execute("SELECT constraint_text FROM duckdb_constraints() WHERE table_name='sleeve_fills' "
                     "AND constraint_type='PRIMARY KEY'").fetchone()
    assert pk and 'exec_id' in pk[0]


def test_reads_never_create_the_journal_table(tmp_duckdb_path):
    """A missing journal is an empty journal; only the write path runs DDL (dry runs create nothing)."""
    store = SleeveFillsStore(tmp_duckdb_path)                     # construction: no DDL
    assert store.holdings('s2p') == {} and store.list_fills('s2p') == [] and store.exists() is False
    assert 'sleeve_fills' not in tables(tmp_duckdb_path)
    assert store.record([]) == 0                                  # an empty batch creates nothing either
    assert 'sleeve_fills' not in tables(tmp_duckdb_path)
    assert store.record([_fill('e1')]) == 1                       # the first real write creates the schema
    assert 'sleeve_fills' in tables(tmp_duckdb_path) and store.exists() is True
    assert SleeveFillsStore(tmp_duckdb_path).holdings('s2p') == {'AAPL': 25.0}


def test_store_inserts_each_exec_id_once_and_sums_per_ticker(tmp_duckdb_path):
    store = SleeveFillsStore(tmp_duckdb_path)
    assert store.record([_fill('e1'), _fill('e2', qty=-5.0, price=210.0), _fill('e3', ticker='MSFT', qty=7.0)]) == 3
    assert store.record([_fill('e1'), _fill('e4', ticker='MSFT', qty=1.0)]) == 1          # e1 is a no-op
    assert store.holdings('s2p') == {'AAPL': 20.0, 'MSFT': 8.0}
    assert store.holdings('other') == {}
    rows = store.list_fills('s2p')
    assert [f.exec_id for f in rows] == ['e1', 'e2', 'e3', 'e4'] and rows[1].price == 210.0
    assert rows[0].recorded_at.tzinfo is not None
    assert SleeveFillsStore(tmp_duckdb_path).holdings('s2p') == {'AAPL': 20.0, 'MSFT': 8.0}


def test_null_price_and_commission_are_stored_as_null_not_zero(tmp_duckdb_path):
    store = SleeveFillsStore(tmp_duckdb_path)
    f = Fill(sleeve='s2p', ticker='AAPL', trade_date=dt.date(2026, 9, 1), quantity=3.0, price=None, commission=None,
             exec_id='e9', order_id='901', proposal_id=1, recorded_at=dt.datetime.now(UTC))
    store.record([f])
    (row,) = store.list_fills('s2p')
    assert row.price is None and row.commission is None and row.quantity == 3.0


def test_trade_date_is_the_us_session_date_of_the_execution_time():
    assert trade_date_of(dt.datetime(2026, 9, 1, 23, 30, tzinfo=UTC)) == dt.date(2026, 9, 1)      # 19:30 ET
    assert trade_date_of(dt.datetime(2026, 9, 2, 1, 30, tzinfo=UTC)) == dt.date(2026, 9, 1)       # 21:30 ET
    assert trade_date_of(dt.datetime(2026, 9, 2, 1, 30)) == dt.date(2026, 9, 1)                   # naive = UTC


# ---------------------------------------------------------------------------
# matching receipts to proposals
# ---------------------------------------------------------------------------

def _aapl_records():
    return [executed(1, 'AAPL', 'BUY', 25, order_ids=[901]), executed(2, 'MSFT', 'BUY', 7, order_ids=[902]),
            executed(3, 'NVDA', 'BUY', 4, status='PENDING'),
            executed(4, 'TSLA', 'BUY', 9, status='FAILED', order_ids=[904]),
            executed(5, 'GOOG', 'BUY', 2, order_ids=[])]


def test_match_journals_one_row_per_receipt_dated_by_ib_execution_time():
    late_sep_28 = dt.datetime(2026, 9, 28, 19, 59, 30, tzinfo=UTC)        # 15:59:30 ET on the 28th
    next_morning = dt.datetime(2026, 9, 29, 13, 31, tzinfo=UTC)           # 09:31 ET on the 29th
    ev = evidence(
        orders=[observed(901, 'AAPL', 'BUY', 'Filled', filled=25), observed(902, 'MSFT', 'BUY', 'Submitted', filled=0)],
        executions=[receipt('x1', 901, 20, 199.5, cum_qty=20, when=late_sep_28),
                    receipt('x2', 901, 5, 200.5, cum_qty=25, when=next_morning),
                    receipt('x4', 904, 9, 250.0),                            # FAILED proposal: never requested, ignored
                    receipt('x7', 777, 1, 100.0)])                           # not a sleeve order
    res = match_executions('s2p', _aapl_records(), ev, now=NOW)
    assert [(f.exec_id, f.ticker, f.quantity, f.price, f.trade_date, f.proposal_id, f.order_id) for f in res.fills] == [
        ('x1', 'AAPL', 20.0, 199.5, dt.date(2026, 9, 28), 1, '901'),
        ('x2', 'AAPL', 5.0, 200.5, dt.date(2026, 9, 29), 1, '901')]        # dated by execution, not observation
    assert all(f.sleeve == 's2p' and f.recorded_at == NOW and f.commission is None for f in res.fills)
    assert [(u.proposal_id, u.order_id, u.broker_status) for u in res.unfilled] == [(2, 902, 'Submitted')]
    assert res.unfilled_proposal_ids == [2] and list(res.proposals_without_order_ids) == [5] and res.warnings == ()
    assert res.working == (WorkingOrder(proposal_id=2, order_id=902, broker_status='Submitted', filled=0.0,
                                        total_quantity=0.0),)          # 901 is Filled: terminal, not working
    assert res.receipt_only_orders == ()


def test_match_signs_sells_negative_and_refuses_symbol_or_side_mismatch():
    obs, rc = filled_order(901, 'AAPL', 'SELL', 5, 201.0)
    res = match_executions('s2p', [executed(1, 'AAPL', 'SELL', 5, order_ids=[901])], evidence([obs], [rc]))
    assert res.fills[0].quantity == -5.0
    with pytest.raises(ExecutionMismatch, match='refusing to journal a mismatch'):
        match_executions('s2p', [executed(1, 'AAPL', 'BUY', 5, order_ids=[901])],
                         evidence([observed(901, 'MSFT', 'BUY', filled=5)], [receipt('x1', 901, 5)]))
    with pytest.raises(ExecutionMismatch, match='is a SELL but proposal'):
        match_executions('s2p', [executed(1, 'AAPL', 'BUY', 5, order_ids=[901])],
                         evidence([observed(901, 'AAPL', 'SELL', filled=5)], [receipt('x1', 901, 5)]))


def test_order_status_seen_before_its_exec_details_is_incomplete_not_a_fabricated_row():
    """The broker says 25 filled; only the 20-share receipt has arrived. Nothing is journaled for the 5."""
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901])]
    status_first = evidence([observed(901, 'AAPL', 'BUY', 'Filled', filled=25)],
                            [receipt('x1', 901, 20, 199.5, cum_qty=20)])
    with pytest.raises(ExecutionsIncomplete) as exc:
        match_executions('s2p', records, status_first)
    assert 'broker reports 25 filled' in str(exc.value) and 'receipts cover 20' in str(exc.value)
    # A receipt set with an internal gap is incomplete even if the status row is absent.
    gap = evidence([], [receipt('x2', 901, 5, 200.5, cum_qty=25)])
    with pytest.raises(ExecutionsIncomplete, match='receipts cover 5 of 25 cumulative shares'):
        match_executions('s2p', records, gap)
    # Once both receipts are in, both rows are journaled — one per IB execution.
    complete = evidence([observed(901, 'AAPL', 'BUY', 'Filled', filled=25)],
                        [receipt('x1', 901, 20, 199.5, cum_qty=20), receipt('x2', 901, 5, 200.5, cum_qty=25)])
    res = match_executions('s2p', records, complete)
    assert [(f.exec_id, f.quantity) for f in res.fills] == [('x1', 20.0), ('x2', 5.0)]


def test_prior_day_fill_replayed_without_receipts_is_refused_not_guessed():
    """completedOrders says Filled 25 but IB's bounded history returned no execution: refuse, journal nothing."""
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901])]
    ev = evidence([observed(901, 'AAPL', 'BUY', 'Filled', filled=25)], [])
    with pytest.raises(ExecutionsIncomplete, match="outside IB's history window"):
        match_executions('s2p', records, ev)
    gw = FakeGateway(proposals=records, evidence=ev, positions={'AAPL': 25})
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', gw)
    assert exc.value.code == 'EXECUTIONS_INCOMPLETE' and gw.recorded == [] and 'record_fills' not in gw.method_names


def test_orders_the_broker_positively_shows_unfilled_are_reported_not_journaled():
    """Cancelled (a DAY order that expired) or still Submitted with a KNOWN zero fill: resolved, nothing to journal."""
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901]), executed(2, 'MSFT', 'BUY', 7, order_ids=[902])]
    ev = evidence([observed(901, 'AAPL', 'BUY', 'Cancelled', filled=0, total=25),
                   observed(902, 'MSFT', 'BUY', 'Submitted', filled=0, total=7)], [])
    res = match_executions('s2p', records, ev)
    assert res.fills == ()
    assert [(u.order_id, u.broker_status) for u in res.unfilled] == [(901, 'Cancelled'), (902, 'Submitted')]
    assert [(w.order_id, w.broker_status, w.filled, w.total_quantity) for w in res.working] == [(902, 'Submitted', 0.0, 7.0)]


def test_an_executed_order_with_no_evidence_at_all_is_unknown_not_unfilled():
    """The critic's reproduction: order 901 absent from observations AND receipts must refuse, never pass as unfilled.

    Journaling nothing for an order that may have filled makes the next plan buy it again (D13 fail-closed).
    """
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901])]
    with pytest.raises(ExecutionsIncomplete, match=r'order 901 \(proposal #1\): unknown to the broker tracker'):
        match_executions('s2p', records, evidence([], []))
    gw = FakeGateway(proposals=records, evidence=evidence([], []), positions={'AAPL': 25})
    for dry_run in (True, False):
        with pytest.raises(Refusal) as exc:
            reconcile_sleeve('s2p', gw, dry_run=dry_run)
        assert exc.value.code == 'EXECUTIONS_INCOMPLETE' and 'cannot establish whether it filled' in exc.value.message
    assert gw.recorded == [] and 'record_fills' not in gw.method_names


def test_an_unknown_fill_quantity_is_refused_with_or_without_receipts():
    """A tracker row with fillQuantityKnown=False (status Unknown) proves nothing about what filled."""
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901])]
    unknown = observed(901, 'AAPL', 'BUY', 'Unknown', filled=0, known=False)
    with pytest.raises(ExecutionsIncomplete, match='cannot establish the fill quantity \(status Unknown\)'):
        match_executions('s2p', records, evidence([unknown], []))
    # Receipts alongside an Unknown observation cannot prove they are ALL the executions: still refused.
    with pytest.raises(ExecutionsIncomplete, match='cannot establish the fill quantity'):
        match_executions('s2p', records, evidence([observed(901, 'AAPL', 'BUY', 'Unknown', filled=25, known=False)],
                                                  [receipt('x1', 901, 25, 200.0)]))
    gw = FakeGateway(proposals=records, evidence=evidence([unknown], []), positions={'AAPL': 25})
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', gw, dry_run=True)
    assert exc.value.code == 'EXECUTIONS_INCOMPLETE' and gw.recorded == []


def test_receipts_exceeding_the_broker_fill_or_without_cum_qty_are_contradictions_not_fills():
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901])]
    with pytest.raises(ExecutionsIncomplete, match='receipts cover 25 but the broker reports only 20 filled'):
        match_executions('s2p', records, evidence([observed(901, 'AAPL', 'BUY', 'Submitted', filled=20, total=25)],
                                                  [receipt('x1', 901, 25, 200.0)]))
    with pytest.raises(ExecutionsIncomplete, match='carries no cumulative quantity'):
        match_executions('s2p', records, evidence([observed(901, 'AAPL', 'BUY', 'Filled', filled=25)],
                                                  [receipt('x1', 901, 25, 200.0, cum_qty=0)]))


def test_a_partially_filled_working_order_journals_its_receipts_and_is_reported_working():
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901])]
    res = match_executions('s2p', records, evidence([observed(901, 'AAPL', 'BUY', 'Submitted', filled=20, total=25)],
                                                    [receipt('x1', 901, 20, 199.5)]))
    assert [(f.exec_id, f.quantity) for f in res.fills] == [('x1', 20.0)] and res.unfilled == ()
    assert res.working == (WorkingOrder(proposal_id=1, order_id=901, broker_status='Submitted', filled=20.0,
                                        total_quantity=25.0),)


def test_receipts_without_an_order_observation_match_only_when_they_cover_the_whole_submission():
    """No tracker row, but IB receipts for every submitted share: the order cannot have filled more — accepted, flagged."""
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901])]           # metadata.submission_quantity = 25
    res = match_executions('s2p', records, evidence([], [receipt('x1', 901, 20, 200.0, cum_qty=20),
                                                         receipt('x2', 901, 5, 201.0, cum_qty=25)]))
    assert [(f.ticker, f.quantity) for f in res.fills] == [('AAPL', 20.0), ('AAPL', 5.0)]
    assert res.receipt_only_orders == (901,) and res.working == () and res.unfilled == ()
    assert len(res.warnings) == 1 and 'no broker order observation' in res.warnings[0] and 'whole submitted quantity 25' in res.warnings[0]
    # Partial coverage with no observation: the remainder's fate is unknown — refused, nothing journaled.
    with pytest.raises(ExecutionsIncomplete, match='receipts cover 20 of the submitted 25'):
        match_executions('s2p', records, evidence([], [receipt('x1', 901, 20, 200.0)]))
    # No usable submitted quantity: receipts alone can never prove completeness.
    bare = executed(1, 'AAPL', 'BUY', 25, order_ids=[901]).__class__(
        **{**executed(1, 'AAPL', 'BUY', 25, order_ids=[901]).__dict__, 'quantity': None, 'metadata': {}})
    assert submitted_quantity(bare) is None and submitted_quantity(records[0]) == 25.0
    with pytest.raises(ExecutionsIncomplete, match='of the submitted unknown'):
        match_executions('s2p', [bare], evidence([], [receipt('x1', 901, 25, 200.0)]))


def test_ambiguous_identity_timeless_or_empty_receipts_are_incomplete():
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901])]
    with pytest.raises(ExecutionsIncomplete, match='ambiguous'):
        match_executions('s2p', records, evidence([observed(901, 'AAPL', 'BUY', filled=25, ambiguous=True)],
                                                  [receipt('x1', 901, 25)]))
    timeless = receipt('x1', 901, 25).__class__(**{**receipt('x1', 901, 25).__dict__, 'time': None})
    with pytest.raises(ExecutionsIncomplete, match='carries no execution time'):
        match_executions('s2p', records, evidence([], [timeless]))
    with pytest.raises(ExecutionsIncomplete, match='reports 0 shares'):
        match_executions('s2p', records, evidence([], [receipt('x1', 901, 0, cum_qty=0)]))


# ---------------------------------------------------------------------------
# reconcile through the gateway
# ---------------------------------------------------------------------------

def _two_orders():
    obs1, rc1 = filled_order(901, 'AAPL', 'BUY', 25, 200.0)
    ev = evidence([obs1, observed(902, 'MSFT', 'BUY', 'Submitted', filled=0)], [rc1])
    return FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25, order_ids=[901]),
                                  executed(2, 'MSFT', 'BUY', 7, order_ids=[902])],
                       evidence=ev, positions={'AAPL': 25})


def test_reconcile_journals_once_reports_and_is_idempotent():
    gw = _two_orders()
    report = reconcile_sleeve('s2p', gw)
    assert report.inserted == 1 and report.matched_by_symbol == {'AAPL': 25.0}
    assert report.unfilled_proposals == (2,) and report.holdings == {'AAPL': 25.0}
    assert report.working == (WorkingOrder(2, 902, 'Submitted', 0.0, 0.0),) and report.broker_snapshot_complete
    assert [w for w in report.warnings] == ['still working at the broker (more executions can arrive): '
                                            'order 902 (proposal #2, Submitted, 0/0 filled)']
    assert report.evidence_observed_at == '2026-09-29T14:35:00+00:00' and report.replay_scope == 'broker_available_recent_history'
    assert ('execution_evidence', {'order_ids': [901, 902]}) in gw.calls          # scoped to the sleeve's orders
    again = reconcile_sleeve('s2p', gw)
    assert again.inserted == 0 and again.holdings == {'AAPL': 25.0} and len(gw.recorded) == 1
    assert not (gw.method_names & set(FakeGateway.FORBIDDEN))


def test_reconcile_dry_run_writes_nothing():
    gw = _two_orders()
    report = reconcile_sleeve('s2p', gw, dry_run=True)
    assert report.dry_run and report.inserted == 0 and len(report.matched) == 1 and gw.recorded == []
    assert 'record_fills' not in gw.method_names


def test_reconcile_warns_when_journal_exceeds_ib_or_goes_short():
    o1, r1 = filled_order(901, 'AAPL', 'BUY', 25)
    o2, r2 = filled_order(902, 'MSFT', 'SELL', 3)
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25, order_ids=[901]), executed(2, 'MSFT', 'SELL', 3, order_ids=[902])],
                     evidence=evidence([o1, o2], [r1, r2]), positions={'AAPL': 20})
    report = reconcile_sleeve('s2p', gw)
    assert any('AAPL: journal holds 25 but IB shows 20' in w for w in report.warnings)
    assert any('MSFT: journal is net short -3' in w for w in report.warnings)


@pytest.mark.parametrize('kwargs, needle', [
    ({'fail_evidence': True}, 'cannot read broker executions'),
    ({'evidence': evidence(receipts_available=False)}, 'execution receipts'),
    ({'evidence': evidence(history_complete=False)}, 'replay is incomplete'),
    ({'evidence': evidence(account_confirmed=False)}, 'not confirmed'),
    ({'evidence': evidence(orders_complete=False)}, 'open-orders refresh did not complete'),
])
def test_reconcile_refuses_when_the_broker_read_is_unavailable_or_incomplete(kwargs, needle):
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25, order_ids=[901])], positions={'AAPL': 25}, **kwargs)
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', gw)
    assert exc.value.code == 'EXECUTIONS_UNREADABLE' and needle in exc.value.message
    assert gw.recorded == [] and 'record_fills' not in gw.method_names


def test_reconcile_with_no_executed_proposals_needs_no_broker_history():
    gw = FakeGateway(proposals=[executed(3, 'NVDA', 'BUY', 4, status='PENDING')],
                     evidence=evidence(receipts_available=False, history_complete=False), positions={})
    report = reconcile_sleeve('s2p', gw)
    assert report.matched == () and report.inserted == 0 and report.holdings == {}


def test_reconcile_refuses_when_ib_cannot_be_read_or_an_observation_contradicts_its_proposal():
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', FakeGateway(fail_positions=True))
    assert exc.value.code == 'IB_UNREADABLE'
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 5, order_ids=[901])],
                     evidence=evidence([observed(901, 'MSFT', 'BUY', filled=5)], [receipt('x1', 901, 5)]))
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', gw)
    assert exc.value.code == 'EXECUTION_MISMATCH' and gw.recorded == []


def test_reconcile_honours_the_runtime_complete_verdict_unless_receipts_explain_it():
    """``execution_snapshot.complete`` is copied, not inferred: an unexplained False refuses; a receipt-only order explains it."""
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901])]
    obs, rc = filled_order(901, 'AAPL', 'BUY', 25, 200.0)
    gw = FakeGateway(proposals=records, evidence=evidence([obs], [rc], complete=False), positions={'AAPL': 25})
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', gw)
    assert exc.value.code == 'EXECUTIONS_INCOMPLETE' and 'reason the receipts do not explain' in exc.value.message
    assert gw.recorded == []
    # The runtime marks the snapshot incomplete precisely when a requested order is unknown to its tracker;
    # receipts covering the whole submission are the one accepted explanation, and the report says so.
    gw = FakeGateway(proposals=records, evidence=evidence([], [rc], complete=False), positions={'AAPL': 25})
    report = reconcile_sleeve('s2p', gw)
    assert report.inserted == 1 and not report.broker_snapshot_complete
    assert any('flagged the execution snapshot incomplete; orders resolved by receipts alone: 901' in w
               for w in report.warnings)
    # With no EXECUTED orders there is nothing the verdict could be about.
    gw = FakeGateway(proposals=[executed(3, 'NVDA', 'BUY', 4, status='PENDING')],
                     evidence=evidence(complete=False, orders_complete=False), positions={})
    assert reconcile_sleeve('s2p', gw).matched == ()
