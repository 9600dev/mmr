"""SdkGateway: the only module touching trader.sdk uses reads, propose and the journal it owns — nothing else."""
from __future__ import annotations

import datetime as dt
from pathlib import Path
import sys
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

from trader.rebalance.fills import SleeveFillsStore, reconcile_sleeve
from trader.rebalance.plan import Refusal
from trader.rebalance.ports import Fill
from trader.rebalance.sdk_gateway import SdkGateway, evidence_from_snapshot, price_from_snapshot
from trader.trading.proposal import ExecutionSpec, TradeProposal

UTC = dt.timezone.utc
ACCOUNT = 'DUM422056'


class _Store:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def query(self, status=None, limit=50):
        self.calls.append(('query', status, limit))
        return list(self.rows)


class _Groups:
    def __init__(self):
        self.created = []

    def get_group(self, name):
        return None if name not in {n for n, _ in self.created} else object()

    def create_group(self, name, description='', **kw):
        self.created.append((name, description))


class FakeMMR:
    """Shape of trader.sdk.MMR as the gateway sees it; trading methods explode."""

    def __init__(self, rows, positions_df, snapshots, duckdb_path='', net_liq=(True, 40_000.0), exec_snapshot=None):
        self.store = _Store(rows)
        self.groups = _Groups()
        self._positions_df = positions_df
        self._snapshots = snapshots
        self._container = SimpleNamespace(config=lambda: {'duckdb_path': duckdb_path})
        self._net_liq = net_liq
        self._exec_snapshot = exec_snapshot if exec_snapshot is not None else snapshot_reply()
        self.calls = []
        self.closed = False

    def _proposal_store(self):
        self.calls.append('_proposal_store'); return self.store

    def _group_store(self):
        self.calls.append('_group_store'); return self.groups

    def _get_portfolio_state(self):
        self.calls.append('_get_portfolio_state')
        evaluable, value = self._net_liq
        return SimpleNamespace(net_liquidation=value, net_liquidation_evaluable=evaluable)

    def positions(self):
        self.calls.append('positions'); return self._positions_df

    def snapshot(self, symbol, **kw):
        self.calls.append('snapshot'); self.last_snapshot_kw = kw; return self._snapshots[symbol]

    def execution_snapshot(self, intent_id='', order_ids=None, client_intent_id=''):
        self.calls.append(('execution_snapshot', intent_id, list(order_ids or [])))
        return self._exec_snapshot

    def propose(self, **kw):
        self.calls.append('propose'); self.last_propose = kw; return (77, None, None)

    def close(self):
        self.closed = True

    def __getattr__(self, name):
        if name in ('approve', 'reject', 'buy', 'sell', 'cancel', 'cancel_all', 'close_position',
                    'place_protective_order', 'emergency_close_position', '_rpc'):
            raise AssertionError(f'gateway touched MMR.{name}')
        raise AttributeError(name)


def snapshot_reply(orders=(), executions=(), **over):
    """The dict trading_runtime.execution_snapshot returns (keys as the runtime writes them)."""
    reply = {'complete': True, 'positions_complete': True, 'account': ACCOUNT, 'account_confirmed': True,
             'client_id': 10, 'orders_complete': True, 'executions_complete': True,
             'execution_replay': {'scope': 'broker_available_recent_history', 'history_start': None,
                                  'requested_at': '2026-09-29T13:00:00+00:00', 'completed_at': '2026-09-29T13:00:02+00:00'},
             'observed_at': '2026-09-29T14:35:00+00:00', 'orders': list(orders), 'retry_safe': False,
             'intent_outcome': None, 'journal_healthy': True, 'execution_receipts_available': True,
             'positions': [], 'executions': list(executions)}
    reply.update(over)
    return reply


def order_row(order_id, symbol, action, status='Filled', filled=0.0, known=True, account=ACCOUNT, **over):
    row = {'orderId': order_id, 'status': status, 'brokerStatus': status, 'filled': filled, 'fillQuantityKnown': known,
           'remaining': 0.0, 'totalQuantity': filled, 'avgFillPrice': 0.0, 'orderRef': 'proposal', 'clientIntentId': '',
           'action': action, 'orderType': 'MKT', 'conId': 1, 'symbol': symbol, 'account': account, 'clientId': 10,
           'permId': order_id * 10, 'observed_at': '2026-09-29T14:34:00+00:00'}
    row.update(over)
    return row


def receipt_row(exec_id, order_id, shares, price, cum_qty=None, time='2026-09-29T14:31:05+00:00', account=ACCOUNT):
    return {'execId': exec_id, 'orderId': order_id, 'shares': shares, 'cumQty': shares if cum_qty is None else cum_qty,
            'price': price, 'account': account, 'permId': order_id * 10, 'time': time}


def _row(pid, symbol, action, qty, status, group='sleeve_s2p', source='rebalance:s2p:2026-08-31', meta=None, order_ids=()):
    return TradeProposal(id=pid, symbol=symbol, action=action, quantity=qty, status=status, source=source,
                         group=group, created_at=dt.datetime(2026, 9, 1), metadata=meta or {}, order_ids=list(order_ids))


def _mmr(duckdb_path='', **kw):
    rows = [_row(1, 'AAPL', 'BUY', None, 'EXECUTED', meta={'submission_quantity': 25.0}, order_ids=[901]),
            _row(2, 'MSFT', 'BUY', 7, 'EXECUTED', order_ids=[902]),
            _row(3, 'NVDA', 'BUY', 4, 'PENDING'),
            _row(4, 'TSLA', 'BUY', 9, 'EXECUTED', group='manual_ideas', order_ids=[904])]
    positions = pd.DataFrame([
        {'symbol': 'AAPL', 'secType': 'STK', 'position': 25.0},
        {'symbol': 'MSFT', 'secType': 'STK', 'position': 7.0},
        {'symbol': 'EUR', 'secType': 'CASH', 'position': 20000.0},
        {'symbol': 'AAPL', 'secType': 'STK', 'position': 5.0},         # second account line aggregates
    ])
    snaps = {'AAPL': {'last': 200.0, 'bid': 199.9, 'ask': 200.1},
             'MSFT': {'last': float('nan'), 'bid': 399.0, 'ask': 401.0},
             'NVDA': {'last': float('nan'), 'bid': float('nan'), 'ask': float('nan'), 'close': 120.0},
             'DEAD': {'last': float('nan'), 'bid': float('nan'), 'ask': float('nan'), 'close': float('nan')}}
    return FakeMMR(rows, positions, snaps, duckdb_path=duckdb_path, **kw)


def tables(path: str) -> set[str]:
    if not Path(path).exists():          # a refusal before any connection leaves no file at all
        return set()
    con = duckdb.connect(path, read_only=True)
    try:
        return {r[0] for r in con.execute('SELECT table_name FROM duckdb_tables()').fetchall()}
    finally:
        con.close()


def test_proposals_in_group_filters_by_group_and_carries_order_ids():
    gw = SdkGateway(_mmr())
    recs = gw.proposals_in_group('sleeve_s2p')
    assert [(r.id, r.symbol, r.status, tuple(r.order_ids)) for r in recs] == [
        (1, 'AAPL', 'EXECUTED', (901,)), (2, 'MSFT', 'EXECUTED', (902,)), (3, 'NVDA', 'PENDING', ())]
    assert gw._mmr.store.calls[0][2] >= 100_000                     # not the SDK's default limit of 50


def test_stock_positions_aggregates_stock_lines_and_drops_cash():
    gw = SdkGateway(_mmr())
    assert gw.stock_positions() == {'AAPL': 30.0, 'MSFT': 7.0}


def test_stock_positions_empty_frame_is_an_empty_book_not_an_error():
    m = _mmr(); m._positions_df = pd.DataFrame()
    assert SdkGateway(m).stock_positions() == {}


def test_account_net_liquidation_is_read_only_and_never_guessed():
    assert SdkGateway(_mmr()).account_net_liquidation() == 40_000.0
    assert SdkGateway(_mmr(net_liq=(False, 0.0))).account_net_liquidation() is None      # RPC failed: unreadable
    assert SdkGateway(_mmr(net_liq=(True, 0.0))).account_net_liquidation() is None       # measured zero: unusable
    assert SdkGateway(_mmr(net_liq=(True, float('nan')))).account_net_liquidation() is None


def test_price_prefers_last_then_mid_then_close_and_refuses_otherwise():
    gw = SdkGateway(_mmr())
    assert gw.price('AAPL') == 200.0
    assert gw.price('MSFT') == 400.0
    assert gw.price('NVDA') == 120.0
    assert gw.price('DEAD') is None
    assert price_from_snapshot(None) is None
    assert price_from_snapshot({'last': 'n/a', 'bid': -1, 'ask': 3}) is None
    assert price_from_snapshot({'bid': 10.0, 'ask': 9.0}) is None      # crossed quote is not a price


def test_execution_evidence_is_the_read_only_snapshot_scoped_to_the_sleeve_orders():
    reply = snapshot_reply(
        orders=[order_row(901, 'AAPL', 'BUY', 'Filled', filled=25.0),
                order_row(902, 'MSFT', 'BUY', 'Submitted', filled=0.0, known=False),
                order_row(904, 'TSLA', 'BUY', 'Filled', filled=9.0),                       # not requested
                order_row(901, 'AAPL', 'BUY', 'Filled', filled=25.0, account='U0000001'),   # another account
                order_row(902, 'MSFT', 'BUY', 'Filled', filled=7.0, identityAmbiguous=True)],
        executions=[receipt_row('0000e.901.01', 901, 20.0, 199.5, cum_qty=20.0, time='2026-09-28T19:59:30+00:00'),
                    receipt_row('0000e.901.02', 901, 5.0, 200.5, cum_qty=25.0),
                    receipt_row('0000e.901.03', 901, 1.0, sys.float_info.max, cum_qty=26.0),  # UNSET_DOUBLE: no price
                    receipt_row('', 901, 1.0, 1.0),                                          # no execId: not a receipt
                    receipt_row('0000e.904.01', 904, 9.0, 250.0),                            # not requested
                    receipt_row('0000e.901.09', 901, 5.0, 200.0, account='U0000001')])       # another account
    m = _mmr(exec_snapshot=reply)
    ev = SdkGateway(m).execution_evidence([902, 901])
    assert m.calls[-1] == ('execution_snapshot', '', [901, 902])
    assert ev.account == ACCOUNT and ev.account_confirmed and ev.receipts_available and ev.history_complete
    assert ev.orders_complete and ev.complete and ev.observed_at == '2026-09-29T14:35:00+00:00'
    assert ev.replay_scope == 'broker_available_recent_history'
    assert [(o.order_id, o.symbol, o.action, o.status, o.filled, o.fill_quantity_known, o.identity_ambiguous, o.total_quantity)
            for o in ev.orders] == [(901, 'AAPL', 'BUY', 'Filled', 25.0, True, False, 25.0),
                                    (902, 'MSFT', 'BUY', 'Submitted', 0.0, False, False, 0.0),
                                    (902, 'MSFT', 'BUY', 'Filled', 7.0, True, True, 7.0)]
    assert [(e.exec_id, e.order_id, e.shares, e.cum_qty, e.price, e.time) for e in ev.executions] == [
        ('0000e.901.01', 901, 20.0, 20.0, 199.5, dt.datetime(2026, 9, 28, 19, 59, 30, tzinfo=UTC)),
        ('0000e.901.02', 901, 5.0, 25.0, 200.5, dt.datetime(2026, 9, 29, 14, 31, 5, tzinfo=UTC)),
        ('0000e.901.03', 901, 1.0, 26.0, None, dt.datetime(2026, 9, 29, 14, 31, 5, tzinfo=UTC))]


def test_execution_evidence_copies_completeness_flags_and_never_infers_them():
    degraded = snapshot_reply(execution_receipts_available=False, executions_complete=False, account_confirmed=False,
                              orders_complete=False, complete=False)
    ev = SdkGateway(_mmr(exec_snapshot=degraded)).execution_evidence([901])
    assert not ev.receipts_available and not ev.history_complete and not ev.account_confirmed
    assert not ev.orders_complete and not ev.complete
    missing_keys = {'account': ACCOUNT, 'orders': [], 'executions': []}     # an old server: absent flags read False
    ev = evidence_from_snapshot(missing_keys, [901])
    assert not ev.receipts_available and not ev.history_complete and not ev.account_confirmed
    assert not ev.orders_complete and not ev.complete
    assert SdkGateway(_mmr()).execution_evidence([]).complete                # nothing requested: trivially complete
    m = _mmr()
    assert SdkGateway(m).execution_evidence([]).receipts_available            # nothing requested: no RPC at all
    assert not any(isinstance(c, tuple) and c[0] == 'execution_snapshot' for c in m.calls)
    with pytest.raises(ConnectionError):
        SdkGateway(_mmr(exec_snapshot=None)).execution_evidence([901]) if False else \
            SdkGateway(FakeMMR([], pd.DataFrame(), {}, exec_snapshot=[])).execution_evidence([901])


def test_reads_over_a_real_duckdb_never_create_the_journal_table(tmp_duckdb_path):
    """plan, status and reconcile --dry-run must leave a database without sleeve_fills untouched."""
    from rebalance_fakes import TODAY, make_doc
    from trader.rebalance.plan import make_plan
    from trader.rebalance.status import sleeve_status
    from trader.rebalance.targets import validate_targets
    reply = snapshot_reply(orders=[order_row(901, 'AAPL', 'BUY', filled=25.0), order_row(902, 'MSFT', 'BUY', filled=7.0)],
                           executions=[receipt_row('0000e.901.01', 901, 25.0, 200.0), receipt_row('0000e.902.01', 902, 7.0, 400.0)])
    gw = SdkGateway(_mmr(tmp_duckdb_path, exec_snapshot=reply))
    assert gw.sleeve_holdings('s2p') == {}
    t = validate_targets(make_doc(weights={'AAPL': 0.5, 'MSFT': 0.3}))
    plan = make_plan(t, gw, today=TODAY)
    assert plan.holdings == {} and [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('BUY', 'AAPL', 25), ('BUY', 'MSFT', 7)]
    assert sleeve_status('s2p', gw, targets_dir=str(tmp_duckdb_path) + '.nodir').market_value == 0.0
    dry = reconcile_sleeve('s2p', gw, dry_run=True)
    assert dry.matched_by_symbol == {'AAPL': 25.0, 'MSFT': 7.0} and dry.inserted == 0
    assert 'sleeve_fills' not in tables(tmp_duckdb_path)                     # three dry paths, no DDL
    with pytest.raises(Refusal, match='UNPRICEABLE'):
        make_plan(validate_targets(make_doc(weights={'DEAD': 0.1})), gw, today=TODAY)
    assert 'sleeve_fills' not in tables(tmp_duckdb_path)                     # a refusal creates nothing either
    wet = reconcile_sleeve('s2p', gw)
    assert wet.inserted == 2 and 'sleeve_fills' in tables(tmp_duckdb_path)   # only the write path creates it
    assert gw.sleeve_holdings('s2p') == {'AAPL': 25.0, 'MSFT': 7.0}
    assert make_plan(t, gw, today=TODAY).orders == ()


def test_journal_round_trips_through_the_real_store(tmp_duckdb_path):
    gw = SdkGateway(_mmr(tmp_duckdb_path))
    f = Fill(sleeve='s2p', ticker='AAPL', trade_date=dt.date(2026, 9, 1), quantity=25.0, price=200.0, commission=None,
             exec_id='0001.abc', order_id='901', proposal_id=1, recorded_at=dt.datetime.now(UTC))
    assert gw.record_fills([f]) == 1 and gw.record_fills([f]) == 0
    assert gw.sleeve_holdings('s2p') == {'AAPL': 25.0}
    assert SleeveFillsStore(tmp_duckdb_path).holdings('s2p') == {'AAPL': 25.0}


def test_propose_forwards_exactly_and_ensure_group_creates_once():
    m = _mmr(); gw = SdkGateway(m)
    gw.ensure_group('sleeve_s2p', 'desc'); gw.ensure_group('sleeve_s2p', 'desc')
    assert m.groups.created == [('sleeve_s2p', 'desc')]
    spec = ExecutionSpec(order_type='MARKET', tif='DAY', outside_rth=False)
    pid = gw.propose(symbol='AAPL', action='BUY', quantity=25, execution=spec, reasoning='why',
                     source='rebalance:s2p:2026-09-28', group='sleeve_s2p', metadata={'rebalance': {'x': 1}})
    assert pid == 77
    assert m.last_propose == {'symbol': 'AAPL', 'action': 'BUY', 'quantity': 25.0, 'execution': spec,
                              'reasoning': 'why', 'source': 'rebalance:s2p:2026-09-28', 'group': 'sleeve_s2p',
                              'metadata': {'rebalance': {'x': 1}}, 'exchange': 'SMART', 'currency': 'USD'}
    assert set(m.calls) <= {'_proposal_store', '_group_store', '_get_portfolio_state', 'positions', 'snapshot', 'propose'}


def test_regression_ambiguous_symbols_are_priced_and_proposed_as_the_us_listing():
    # Regression (2026-10-01, first live plan): EIX, LEN, SE and SYY each resolve to several IB
    # contracts (FWB/EUR, EBS/CHF, PURE/CAD, MEXI/MXN, ...) by bare symbol, so pricing refused the
    # whole plan as UNPRICEABLE and an approved proposal would have failed the same way. The sleeve
    # trades the US PIT universe only: every price and every proposal is pinned to SMART/USD.
    m = _mmr(); gw = SdkGateway(m)
    gw.price('AAPL')
    assert m.last_snapshot_kw == {'exchange': 'SMART', 'currency': 'USD'}
    spec = ExecutionSpec(order_type='MARKET', tif='DAY', outside_rth=False)
    gw.propose(symbol='EIX', action='BUY', quantity=3, execution=spec, reasoning='r', source='s',
               group='g', metadata={})
    assert (m.last_propose['exchange'], m.last_propose['currency']) == ('SMART', 'USD')


def test_close_only_disconnects_when_the_gateway_connected():
    m = _mmr()
    SdkGateway(m).close()
    assert not m.closed
    SdkGateway(m, connected=True).close()
    assert m.closed


# ---------------------------------------------------------------------------
# End-to-end: runtime-shaped snapshot replies through SdkGateway into reconcile_sleeve (D13 fail-closed)
# ---------------------------------------------------------------------------

def _sleeve_gateway(tmp_duckdb_path, reply, rows=None):
    """One EXECUTED AAPL BUY 25 (order 901) in sleeve_s2p over a real DuckDB, with the given snapshot reply."""
    rows = rows if rows is not None else [_row(1, 'AAPL', 'BUY', 25, 'EXECUTED', meta={'submission_quantity': 25.0}, order_ids=[901])]
    positions = pd.DataFrame([{'symbol': 'AAPL', 'secType': 'STK', 'position': 25.0}])
    snaps = {'AAPL': {'last': 200.0, 'bid': 199.9, 'ask': 200.1}}
    return SdkGateway(FakeMMR(rows, positions, snaps, duckdb_path=tmp_duckdb_path, exec_snapshot=reply))


def test_snapshot_with_zero_evidence_for_an_executed_order_refuses_and_journals_nothing(tmp_duckdb_path):
    """The critic's reproduction, end to end: trading_runtime.execution_snapshot returns no row and no receipt for
    requested order 901 (and therefore ``complete: False``, since ``expected <= known`` fails). The order may well
    have filled; treating it as unfilled would make the next plan buy AAPL again."""
    reply = snapshot_reply(orders=[], executions=[], complete=False)
    gw = _sleeve_gateway(tmp_duckdb_path, reply)
    for dry_run in (True, False):
        with pytest.raises(Refusal) as exc:
            reconcile_sleeve('s2p', gw, dry_run=dry_run)
        assert exc.value.code == 'EXECUTIONS_INCOMPLETE'
        assert 'order 901 (proposal #1): unknown to the broker tracker' in exc.value.message
    assert gw.sleeve_holdings('s2p') == {} and 'sleeve_fills' not in tables(tmp_duckdb_path)
    # Even a runtime that (wrongly) said complete=True would be refused: the per-order rule needs evidence.
    with pytest.raises(Refusal, match='EXECUTIONS_INCOMPLETE'):
        reconcile_sleeve('s2p', _sleeve_gateway(tmp_duckdb_path, snapshot_reply(orders=[], executions=[])), dry_run=True)
    assert 'sleeve_fills' not in tables(tmp_duckdb_path)


@pytest.mark.parametrize('reply_kwargs, code, needle', [
    # tracker row present but its fill quantity unknown (restored checkpoint / Filled with filled<=0): not "unfilled"
    (dict(orders=[order_row(901, 'AAPL', 'BUY', 'Unknown', filled=0.0, known=False, totalQuantity=25.0)]),
     'EXECUTIONS_INCOMPLETE', 'cannot establish the fill quantity (status Unknown)'),
    # Unknown row with a receipt: the receipt is real, but nothing proves it is the ONLY one
    (dict(orders=[order_row(901, 'AAPL', 'BUY', 'Unknown', filled=25.0, known=False, totalQuantity=25.0)],
          executions=[receipt_row('0000e.901.01', 901, 25.0, 200.0)]),
     'EXECUTIONS_INCOMPLETE', 'cannot establish the fill quantity'),
    # completedOrders says Filled 25, IB's bounded history returned no execution
    (dict(orders=[order_row(901, 'AAPL', 'BUY', 'Filled', filled=25.0, totalQuantity=25.0)]),
     'EXECUTIONS_INCOMPLETE', "outside IB's history window"),
    # open-orders refresh failed: statuses may be stale
    (dict(orders=[order_row(901, 'AAPL', 'BUY', 'Filled', filled=25.0, totalQuantity=25.0)],
          executions=[receipt_row('0000e.901.01', 901, 25.0, 200.0)], orders_complete=False, complete=False),
     'EXECUTIONS_UNREADABLE', 'open-orders refresh did not complete'),
    # the runtime's verdict is False for a reason the receipts do not explain (e.g. an ambiguous identity elsewhere)
    (dict(orders=[order_row(901, 'AAPL', 'BUY', 'Filled', filled=25.0, totalQuantity=25.0)],
          executions=[receipt_row('0000e.901.01', 901, 25.0, 200.0)], complete=False),
     'EXECUTIONS_INCOMPLETE', 'reason the receipts do not explain'),
    # same numeric id seen from another broker client
    (dict(orders=[order_row(901, 'AAPL', 'BUY', 'Filled', filled=25.0, totalQuantity=25.0, identityAmbiguous=True)],
          executions=[receipt_row('0000e.901.01', 901, 25.0, 200.0)], complete=False),
     'EXECUTIONS_INCOMPLETE', 'identity is ambiguous'),
])
def test_snapshot_outcomes_that_prove_nothing_refuse_end_to_end(tmp_duckdb_path, reply_kwargs, code, needle):
    gw = _sleeve_gateway(tmp_duckdb_path, snapshot_reply(**reply_kwargs))
    with pytest.raises(Refusal) as exc:
        reconcile_sleeve('s2p', gw)
    assert exc.value.code == code and needle in exc.value.message
    assert 'sleeve_fills' not in tables(tmp_duckdb_path)


def test_snapshot_outcomes_that_prove_the_order_resolve_end_to_end(tmp_duckdb_path):
    # (1) a Cancelled DAY order with a known zero fill: positively unfilled, nothing journaled, no refusal
    cancelled = snapshot_reply(orders=[order_row(901, 'AAPL', 'BUY', 'Cancelled', filled=0.0, totalQuantity=25.0)])
    report = reconcile_sleeve('s2p', _sleeve_gateway(tmp_duckdb_path, cancelled))
    assert report.matched == () and report.unfilled_proposals == (1,) and report.working == ()
    assert report.unfilled[0].broker_status == 'Cancelled' and 'sleeve_fills' not in tables(tmp_duckdb_path)
    # (2) still Submitted, 20 of 25 filled with its receipt: journal the 20, report the order as working
    partial = snapshot_reply(orders=[order_row(901, 'AAPL', 'BUY', 'Submitted', filled=20.0, totalQuantity=25.0)],
                             executions=[receipt_row('0000e.901.01', 901, 20.0, 199.5, cum_qty=20.0)])
    report = reconcile_sleeve('s2p', _sleeve_gateway(tmp_duckdb_path, partial))
    assert report.inserted == 1 and report.matched_by_symbol == {'AAPL': 20.0}
    assert [(w.order_id, w.broker_status, w.filled, w.total_quantity) for w in report.working] == [(901, 'Submitted', 20.0, 25.0)]
    assert any('still working at the broker' in w for w in report.warnings)
    # (3) the second execution arrives and the order is Filled: one more row, idempotent on the first
    done = snapshot_reply(orders=[order_row(901, 'AAPL', 'BUY', 'Filled', filled=25.0, totalQuantity=25.0)],
                          executions=[receipt_row('0000e.901.01', 901, 20.0, 199.5, cum_qty=20.0),
                                      receipt_row('0000e.901.02', 901, 5.0, 200.5, cum_qty=25.0)])
    report = reconcile_sleeve('s2p', _sleeve_gateway(tmp_duckdb_path, done))
    assert report.inserted == 1 and report.holdings == {'AAPL': 25.0} and report.working == () and report.broker_snapshot_complete
    # (4) receipt-only: the tracker never saw the order (runtime says complete=False) but IB receipts cover all 25
    receipt_only = snapshot_reply(orders=[], executions=[receipt_row('0000e.901.01', 901, 20.0, 199.5, cum_qty=20.0),
                                                        receipt_row('0000e.901.02', 901, 5.0, 200.5, cum_qty=25.0)],
                                  complete=False)
    report = reconcile_sleeve('s2p', _sleeve_gateway(tmp_duckdb_path, receipt_only))
    assert report.inserted == 0 and report.holdings == {'AAPL': 25.0} and not report.broker_snapshot_complete
    assert any('no broker order observation; the receipts account for the whole submitted quantity 25' in w
               for w in report.warnings)
    assert any('orders resolved by receipts alone: 901' in w for w in report.warnings)
    # (5) receipt-only but short of the submission: the remainder's fate is unknown — refused
    short = snapshot_reply(orders=[], executions=[receipt_row('0000e.901.01', 901, 20.0, 199.5, cum_qty=20.0)], complete=False)
    with pytest.raises(Refusal, match='receipts cover 20 of the submitted 25'):
        reconcile_sleeve('s2p', _sleeve_gateway(tmp_duckdb_path, short))
