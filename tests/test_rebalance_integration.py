"""End to end through the REAL SDK propose path, a real DuckDB proposal store and fills journal.

IB, prices and the broker's ``execution_snapshot`` are faked at the gateway's
port boundary; the stores and ``MMR.propose`` are the production code. Proves
that ``propose`` lands PENDING rows with the sleeve group, batch source and
explicit quantity — that ``reconcile`` turns IB execution receipts into journal
rows exactly once while dry paths create no table — and that the next plan
reads holdings back from the journal.
"""
from __future__ import annotations

import datetime as dt
import io
from typing import Mapping, Optional
from unittest.mock import MagicMock

import duckdb
import pytest

from rebalance_fakes import AS_OF, TODAY, make_doc, write_targets
from test_rebalance_gateway import order_row, receipt_row, snapshot_reply
from trader.data.position_groups import PositionGroupStore
from trader.data.proposal_store import ProposalStore
from trader.rebalance.__main__ import main
from trader.rebalance.fills import SleeveFillsStore, reconcile_sleeve
from trader.rebalance.plan import PositionLimits, Refusal, make_plan
from trader.rebalance.propose import propose_plan
from trader.rebalance.sdk_gateway import SdkGateway
from trader.rebalance.targets import validate_targets
from trader.sdk import MMR

PRICES = {'AAPL': 200.0, 'MSFT': 400.0}
UTC = dt.timezone.utc


class ShellMMR(MMR):
    """A real MMR wired to real stores; the broker snapshot is scripted, RPC absent."""

    def execution_snapshot(self, intent_id='', order_ids=None, client_intent_id=''):
        return snapshot_reply(orders=list(self.broker_orders), executions=list(self.broker_receipts))


def _shell_mmr(duckdb_path: str) -> ShellMMR:
    mmr = ShellMMR.__new__(ShellMMR)
    mmr._client = None
    mmr._data_client = None
    mmr._massive_rest_client = None
    mmr._twelvedata_rest_client = None
    mmr._subscriptions = []
    mmr._position_map = {}
    mmr._contract_map = {}
    container = MagicMock()
    container.config.return_value = {'duckdb_path': duckdb_path}
    mmr._container = container
    mmr.broker_orders = []
    mmr.broker_receipts = []
    return mmr


class GatewayWithFakedIB(SdkGateway):
    """Real stores + real propose; IB positions, prices and NetLiquidation from the test."""

    def __init__(self, mmr, positions: Mapping[str, float], prices: Mapping[str, Optional[float]]):
        super().__init__(mmr)
        self._positions = dict(positions)
        self._prices = dict(prices)

    def stock_positions(self) -> Mapping[str, float]:
        return dict(self._positions)

    def price(self, ticker: str) -> Optional[float]:
        return self._prices.get(ticker)

    def account_net_liquidation(self) -> Optional[float]:
        return 100_000.0


@pytest.fixture
def gateway(tmp_duckdb_path):
    return GatewayWithFakedIB(_shell_mmr(tmp_duckdb_path), positions={}, prices=PRICES)


def tables(path: str) -> set[str]:
    con = duckdb.connect(path, read_only=True)
    try:
        return {r[0] for r in con.execute('SELECT table_name FROM duckdb_tables()').fetchall()}
    finally:
        con.close()


def _fill_everything(store: ProposalStore, mmr: ShellMMR, when: dt.datetime) -> None:
    """The operator approves by hand and the orders fill: drive the state machine as the SDK's approve
    does, and let the broker report one execution receipt per order, as trader_service replays them."""
    for p in store.query(limit=100):
        if p.status != 'PENDING':
            continue
        assert p.id is not None
        order_id = 900 + p.id
        store.update_metadata(p.id, {'submission_quantity': float(p.quantity or 0)})
        assert store.try_transition(p.id, 'PENDING', 'APPROVED')
        assert store.try_transition(p.id, 'APPROVED', 'EXECUTED', order_ids=[order_id])
        mmr.broker_orders.append(order_row(order_id, p.symbol, p.action, 'Filled', filled=float(p.quantity or 0)))
        mmr.broker_receipts.append(receipt_row(f'0000e.{order_id}.01', order_id, float(p.quantity or 0),
                                               PRICES[p.symbol], time=when.isoformat()))


def test_propose_lands_pending_rows_in_the_real_store(tmp_duckdb_path, gateway):
    t = validate_targets(make_doc(), path='/targets/s2p/2026-09-28.json')
    plan = make_plan(t, gateway, today=TODAY)
    result = propose_plan(t, plan, gateway, gateway)
    assert result.complete and len(result.proposal_ids) == 2

    store = ProposalStore(tmp_duckdb_path)
    rows = {p.symbol: p for p in store.query(limit=100)}
    assert set(rows) == {'AAPL', 'MSFT'}
    for sym, qty in (('AAPL', 25.0), ('MSFT', 7.0)):
        p = rows[sym]
        assert p.status == 'PENDING' and p.action == 'BUY' and p.quantity == qty and p.amount is None
        assert p.group == 'sleeve_s2p' and p.source == f'rebalance:s2p:{AS_OF}'
        assert p.execution.order_type == 'MARKET' and p.execution.tif == 'DAY' and p.execution.outside_rth is False
        assert '/targets/s2p/2026-09-28.json' in p.reasoning and 'a' * 64 in p.reasoning
        assert p.metadata['rebalance']['target_shares'] == int(qty)
        assert 'auto_sized' not in p.metadata                      # explicit quantity: the sizer never ran
    groups = PositionGroupStore(tmp_duckdb_path)
    assert groups.get_group('sleeve_s2p') is not None
    assert set(groups.get_members('sleeve_s2p')) == {'AAPL', 'MSFT'}
    assert 'sleeve_fills' not in tables(tmp_duckdb_path)             # planning and proposing never touch the journal

    # The very next run reads the PENDING batch back from the real store and refuses.
    with pytest.raises(Refusal) as exc:
        propose_plan(t, make_plan(t, gateway, today=TODAY), gateway, gateway)
    assert exc.value.code == 'DUPLICATE_BATCH'
    assert len(store.query(limit=100)) == 2                          # nothing new was created


def test_reconcile_journals_broker_receipts_and_the_next_plan_reads_the_journal(tmp_duckdb_path, gateway):
    t0 = validate_targets(make_doc(), path='/targets/s2p/2026-09-28.json')
    propose_plan(t0, make_plan(t0, gateway, today=TODAY), gateway, gateway)
    store, mmr = ProposalStore(tmp_duckdb_path), gateway._mmr
    _fill_everything(store, mmr, dt.datetime(2026, 9, 28, 19, 59, 30, tzinfo=UTC))    # 15:59:30 ET on the 28th
    gateway._positions = {'AAPL': 25.0, 'MSFT': 7.0, 'TSLA': 3.0}   # IB shows the fills (+ an outside name)

    # Before reconcile: EXECUTED proposals are submissions, not inventory — the sleeve is still flat (D13),
    # and neither the plan nor a dry-run reconcile creates the journal table.
    assert make_plan(t0, gateway, today=TODAY).holdings == {}
    assert reconcile_sleeve('s2p', gateway, dry_run=True).matched_by_symbol == {'AAPL': 25.0, 'MSFT': 7.0}
    assert 'sleeve_fills' not in tables(tmp_duckdb_path)

    report = reconcile_sleeve('s2p', gateway)
    assert report.inserted == 2 and report.matched_by_symbol == {'AAPL': 25.0, 'MSFT': 7.0}
    assert report.holdings == {'AAPL': 25.0, 'MSFT': 7.0} and report.unfilled_proposals == () and report.warnings == ()
    journal = SleeveFillsStore(tmp_duckdb_path).list_fills('s2p')
    assert [(f.ticker, f.quantity, f.price, f.trade_date, f.order_id, f.exec_id) for f in journal] == [
        ('AAPL', 25.0, 200.0, dt.date(2026, 9, 28), '901', '0000e.901.01'),
        ('MSFT', 7.0, 400.0, dt.date(2026, 9, 28), '902', '0000e.902.01')]     # dated by IB's execution time
    assert reconcile_sleeve('s2p', gateway).inserted == 0                # idempotent

    t1 = validate_targets(make_doc(as_of='2026-09-29', prices_as_of='2026-09-29', weights={'AAPL': 0.3}))
    plan = make_plan(t1, gateway, today=TODAY)
    assert plan.holdings == {'AAPL': 25, 'MSFT': 7}
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'MSFT', 7), ('SELL', 'AAPL', 10)]
    result = propose_plan(t1, plan, gateway, gateway)
    assert result.complete
    pending = [p for p in store.query(status='PENDING', limit=100)]
    assert sorted((p.symbol, p.action, p.quantity) for p in pending) == [('AAPL', 'SELL', 10.0), ('MSFT', 'SELL', 7.0)]
    assert all(p.source == 'rebalance:s2p:2026-09-29' for p in pending)

    # The sells fill; reconcile subtracts them and the sleeve is where the targets say.
    _fill_everything(store, mmr, dt.datetime(2026, 9, 29, 14, 31, tzinfo=UTC))
    gateway._positions = {'AAPL': 15.0, 'TSLA': 3.0}
    assert reconcile_sleeve('s2p', gateway).holdings == {'AAPL': 15.0, 'MSFT': 0.0}
    assert make_plan(t1, gateway, today=TODAY).orders == ()


def test_status_before_exec_details_journals_nothing_until_the_receipt_arrives(tmp_duckdb_path, gateway):
    t0 = validate_targets(make_doc(weights={'AAPL': 0.5}), path='/targets/s2p/2026-09-28.json')
    propose_plan(t0, make_plan(t0, gateway, today=TODAY), gateway, gateway)
    store, mmr = ProposalStore(tmp_duckdb_path), gateway._mmr
    (p,) = store.query(limit=10)
    assert store.try_transition(p.id, 'PENDING', 'APPROVED') and store.try_transition(p.id, 'APPROVED', 'EXECUTED', order_ids=[901])
    mmr.broker_orders.append(order_row(901, 'AAPL', 'BUY', 'Filled', filled=25.0))       # orderStatus arrived first
    gateway._positions = {'AAPL': 25.0}
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', gateway)
    assert exc.value.code == 'EXECUTIONS_INCOMPLETE' and 'sleeve_fills' not in tables(tmp_duckdb_path)
    mmr.broker_receipts.append(receipt_row('0000e.901.01', 901, 25.0, 200.0))            # then execDetails
    assert reconcile_sleeve('s2p', gateway).inserted == 1
    assert SleeveFillsStore(tmp_duckdb_path).holdings('s2p') == {'AAPL': 25.0}


def test_cli_propose_and_reconcile_end_to_end_over_the_real_store(tmp_path, tmp_duckdb_path, gateway):
    path = write_targets(tmp_path)
    out = io.StringIO()
    code = main(['propose', str(path), '--today', '2026-09-29'], gateway_factory=lambda: gateway,
                limits=PositionLimits(), out=out)
    assert code == 0 and 'PROPOSED 2 PENDING proposal(s) complete' in out.getvalue()
    assert {p.status for p in ProposalStore(tmp_duckdb_path).query(limit=10)} == {'PENDING'}
    code = main(['propose', str(path), '--today', '2026-09-29'], gateway_factory=lambda: gateway,
                limits=PositionLimits(), out=io.StringIO())
    assert code == 1
    assert len(ProposalStore(tmp_duckdb_path).query(limit=10)) == 2

    _fill_everything(ProposalStore(tmp_duckdb_path), gateway._mmr, dt.datetime(2026, 9, 29, 14, 31, tzinfo=UTC))
    gateway._positions = {'AAPL': 25.0, 'MSFT': 7.0}
    out = io.StringIO()
    assert main(['reconcile', '--sleeve', 's2p', '--dry-run'], gateway_factory=lambda: gateway, out=out) == 0
    assert 'DRY RUN' in out.getvalue() and 'sleeve_fills' not in tables(tmp_duckdb_path)
    out = io.StringIO()
    assert main(['reconcile', '--sleeve', 's2p'], gateway_factory=lambda: gateway, out=out) == 0
    assert 'journaled now 2' in out.getvalue()
    assert SleeveFillsStore(tmp_duckdb_path).holdings('s2p') == {'AAPL': 25.0, 'MSFT': 7.0}
    out = io.StringIO()
    assert main(['status', 's2p', '--targets-file', str(path)], gateway_factory=lambda: gateway, out=out) == 0
    assert 'market value $7,800.00' in out.getvalue()
