"""D13 journal: schema, idempotent inserts, execution matching, and the reconcile refusals."""
from __future__ import annotations

import datetime as dt

from zoneinfo import ZoneInfo

import duckdb
import pytest

from rebalance_fakes import FakeGateway, executed, execution
from trader.rebalance.fills import (
    SLEEVE_FILLS_DDL, SleeveFillsStore, match_executions, reconcile_sleeve, trade_date_of,
)
from trader.rebalance.plan import Refusal
from trader.rebalance.ports import Fill

UTC = dt.timezone.utc


def _fill(exec_id, ticker='AAPL', qty=25.0, price=200.0, pid=1, sleeve='s2p'):
    return Fill(sleeve=sleeve, ticker=ticker, trade_date=dt.date(2026, 9, 1), quantity=qty, price=price,
                commission=1.0, exec_id=exec_id, order_id='901', proposal_id=pid,
                recorded_at=dt.datetime(2026, 9, 1, 15, 0, tzinfo=UTC))


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


def test_store_inserts_each_exec_id_once_and_sums_per_ticker(tmp_duckdb_path):
    store = SleeveFillsStore(tmp_duckdb_path)
    assert store.record([_fill('e1'), _fill('e2', qty=-5.0, price=210.0), _fill('e3', ticker='MSFT', qty=7.0)]) == 3
    assert store.record([_fill('e1'), _fill('e4', ticker='MSFT', qty=1.0)]) == 1          # e1 is a no-op
    assert store.holdings('s2p') == {'AAPL': 20.0, 'MSFT': 8.0}
    assert store.holdings('other') == {}
    rows = store.list_fills('s2p')
    assert [f.exec_id for f in rows] == ['e1', 'e2', 'e3', 'e4'] and rows[1].price == 210.0
    assert rows[0].recorded_at.tzinfo is not None
    # a second store instance on the same file sees the same journal (it is the shared DuckDB, not memory)
    assert SleeveFillsStore(tmp_duckdb_path).holdings('s2p') == {'AAPL': 20.0, 'MSFT': 8.0}
    assert SleeveFillsStore(tmp_duckdb_path).record([]) == 0


def test_null_price_and_commission_are_stored_as_null_not_zero(tmp_duckdb_path):
    store = SleeveFillsStore(tmp_duckdb_path)
    f = Fill(sleeve='s2p', ticker='AAPL', trade_date=dt.date(2026, 9, 1), quantity=3.0, price=None, commission=None,
             exec_id='e9', order_id='901', proposal_id=1, recorded_at=dt.datetime.now(UTC))
    store.record([f])
    (row,) = store.list_fills('s2p')
    assert row.price is None and row.commission is None and row.quantity == 3.0


def test_trade_date_is_the_us_session_date():
    assert trade_date_of(dt.datetime(2026, 9, 1, 23, 30, tzinfo=UTC)) == dt.date(2026, 9, 1)      # 19:30 ET
    assert trade_date_of(dt.datetime(2026, 9, 2, 1, 30, tzinfo=UTC)) == dt.date(2026, 9, 1)       # 21:30 ET
    naive = dt.datetime(2026, 9, 2, 13, 30)                                                      # naive = LOCAL
    assert trade_date_of(naive) == naive.astimezone(dt.timezone.utc).astimezone(ZoneInfo('America/New_York')).date()


def test_match_pairs_executions_to_executed_proposals_by_order_id():
    records = [executed(1, 'AAPL', 'BUY', 25, order_ids=[901]), executed(2, 'MSFT', 'BUY', 7, order_ids=[902]),
               executed(3, 'NVDA', 'BUY', 4, status='PENDING'), executed(4, 'TSLA', 'BUY', 9, status='FAILED', order_ids=[904]),
               executed(5, 'GOOG', 'BUY', 2, order_ids=[])]
    execs = [execution('x1', 901, 'AAPL', 'BUY', 20, 199.5), execution('x2', 901, 'AAPL', 'BUY', 5, 200.5),
             execution('x3', 904, 'TSLA', 'BUY', 9), execution('x4', 777, 'AMZN', 'BUY', 1)]
    now = dt.datetime(2026, 9, 2, tzinfo=UTC)
    fills, unfilled, without_ids = match_executions('s2p', records, execs, now=now)
    assert [(f.exec_id, f.ticker, f.quantity, f.price, f.proposal_id, f.order_id) for f in fills] == [
        ('x1', 'AAPL', 20.0, 199.5, 1, '901'), ('x2', 'AAPL', 5.0, 200.5, 1, '901')]
    assert all(f.sleeve == 's2p' and f.recorded_at == now and f.trade_date == dt.date(2026, 9, 1) for f in fills)
    assert unfilled == [2] and without_ids == [5]                      # FAILED #4 and PENDING #3 never count


def test_match_signs_sells_negative_and_refuses_symbol_or_side_mismatch():
    fills, _, _ = match_executions('s2p', [executed(1, 'AAPL', 'SELL', 5, order_ids=[901])],
                                   [execution('x1', 901, 'AAPL', 'SELL', 5, 201.0)])
    assert fills[0].quantity == -5.0
    with pytest.raises(ValueError, match='refusing to journal a mismatch'):
        match_executions('s2p', [executed(1, 'AAPL', 'BUY', 5, order_ids=[901])], [execution('x1', 901, 'MSFT', 'BUY', 5)])
    with pytest.raises(ValueError, match='is a SELL but proposal'):
        match_executions('s2p', [executed(1, 'AAPL', 'BUY', 5, order_ids=[901])], [execution('x1', 901, 'AAPL', 'SELL', 5)])


def test_reconcile_journals_once_reports_and_is_idempotent():
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25, order_ids=[901]), executed(2, 'MSFT', 'BUY', 7, order_ids=[902])],
                     executions=[execution('x1', 901, 'AAPL', 'BUY', 25, 200.0)], positions={'AAPL': 25})
    report = reconcile_sleeve('s2p', gw)
    assert report.inserted == 1 and report.matched_by_symbol == {'AAPL': 25.0}
    assert report.unfilled_proposals == (2,) and report.holdings == {'AAPL': 25.0} and report.warnings == ()
    again = reconcile_sleeve('s2p', gw)
    assert again.inserted == 0 and again.holdings == {'AAPL': 25.0} and len(gw.recorded) == 1
    assert not (gw.method_names & set(FakeGateway.FORBIDDEN))


def test_reconcile_dry_run_writes_nothing():
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25, order_ids=[901])],
                     executions=[execution('x1', 901, 'AAPL', 'BUY', 25)], positions={'AAPL': 25})
    report = reconcile_sleeve('s2p', gw, dry_run=True)
    assert report.dry_run and report.inserted == 0 and len(report.matched) == 1 and gw.recorded == []
    assert 'record_fills' not in gw.method_names


def test_reconcile_warns_when_journal_exceeds_ib_or_goes_short():
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25, order_ids=[901]), executed(2, 'MSFT', 'SELL', 3, order_ids=[902])],
                     executions=[execution('x1', 901, 'AAPL', 'BUY', 25), execution('x2', 902, 'MSFT', 'SELL', 3)],
                     positions={'AAPL': 20})
    report = reconcile_sleeve('s2p', gw)
    assert any('AAPL: journal holds 25 but IB shows 20' in w for w in report.warnings)
    assert any('MSFT: journal is net short -3' in w for w in report.warnings)


def test_reconcile_refuses_when_executions_or_ib_cannot_be_read():
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', FakeGateway(fail_executions=True))
    assert exc.value.code == 'EXECUTIONS_UNREADABLE'
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', FakeGateway(fail_positions=True))
    assert exc.value.code == 'IB_UNREADABLE'
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 5, order_ids=[901])], executions=[execution('x1', 901, 'MSFT', 'BUY', 5)])
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', gw)
    assert exc.value.code == 'EXECUTION_MISMATCH' and gw.recorded == []
