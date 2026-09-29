"""SdkGateway: the only module touching trader.sdk uses reads, propose and the journal it owns — nothing else."""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pandas as pd
import pytest

from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.rebalance.fills import SleeveFillsStore
from trader.rebalance.plan import Refusal
from trader.rebalance.ports import Fill
from trader.rebalance.sdk_gateway import SdkGateway, price_from_snapshot
from trader.trading.proposal import ExecutionSpec, TradeProposal

UTC = dt.timezone.utc


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

    def __init__(self, rows, positions_df, snapshots, duckdb_path='', net_liq=(True, 40_000.0)):
        self.store = _Store(rows)
        self.groups = _Groups()
        self._positions_df = positions_df
        self._snapshots = snapshots
        self._container = SimpleNamespace(config=lambda: {'duckdb_path': duckdb_path})
        self._net_liq = net_liq
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
        self.calls.append('snapshot'); return self._snapshots[symbol]

    def propose(self, **kw):
        self.calls.append('propose'); self.last_propose = kw; return (77, None, None)

    def close(self):
        self.closed = True

    def __getattr__(self, name):
        if name in ('approve', 'reject', 'buy', 'sell', 'cancel', 'cancel_all', 'close_position',
                    'place_protective_order', 'emergency_close_position', '_rpc'):
            raise AssertionError(f'gateway touched MMR.{name}')
        raise AttributeError(name)


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


def test_executions_come_from_the_event_store_order_filled_journal(tmp_duckdb_path):
    events = EventStore(tmp_duckdb_path)
    t0 = dt.datetime(2026, 9, 1, 14, 30, tzinfo=UTC)
    events.append(TradingEvent(event_type=EventType.ORDER_FILLED, timestamp=t0, strategy_name='proposal', conid=1,
                               symbol='AAPL', action='BUY', quantity=20.0, price=199.5, order_id=901,
                               metadata={'execId': '0001.abc', 'price_evaluable': True}))
    events.append(TradingEvent(event_type=EventType.ORDER_FILLED, timestamp=t0, strategy_name='proposal', conid=1,
                               symbol='AAPL', action='BUY', quantity=5.0, price=0.0, order_id=901,
                               metadata={'price_evaluable': False}))                       # no execId, no price
    events.append(TradingEvent(event_type=EventType.ORDER_SUBMITTED, timestamp=t0, strategy_name='proposal', conid=1,
                               symbol='AAPL', action='BUY', quantity=25.0, price=0.0, order_id=901))
    events.append(TradingEvent(event_type=EventType.ORDER_FILLED, timestamp=t0, strategy_name='proposal', conid=2,
                               symbol='MSFT', action='BUY', quantity=7.0, price=400.0, order_id=902,
                               metadata={'execId': '0002.def', 'excluded_from_ledger': True}))
    execs = sorted(SdkGateway(_mmr(tmp_duckdb_path)).executions(), key=lambda e: e.exec_id)
    assert [(e.exec_id[:5], e.order_id, e.symbol, e.action, e.quantity, e.price) for e in execs] == [
        ('0001.', 901, 'AAPL', 'BUY', 20.0, 199.5), ('event', 901, 'AAPL', 'BUY', 5.0, None)]
    from trader.rebalance.fills import trade_date_of
    assert all(trade_date_of(e.time) == dt.date(2026, 9, 1) for e in execs)   # 14:30Z is 10:30 ET on Sep 1


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
                              'metadata': {'rebalance': {'x': 1}}}
    assert set(m.calls) <= {'_proposal_store', '_group_store', '_get_portfolio_state', 'positions', 'snapshot', 'propose'}


def test_close_only_disconnects_when_the_gateway_connected():
    m = _mmr()
    SdkGateway(m).close()
    assert not m.closed
    SdkGateway(m, connected=True).close()
    assert m.closed


def test_end_to_end_plan_over_the_sdk_shaped_fake(tmp_duckdb_path):
    from rebalance_fakes import TODAY, make_doc
    from trader.rebalance.plan import make_plan
    from trader.rebalance.targets import validate_targets
    gw = SdkGateway(_mmr(tmp_duckdb_path))
    now = dt.datetime.now(UTC)
    gw.record_fills([Fill('s2p', 'AAPL', dt.date(2026, 9, 1), 25.0, 200.0, None, 'e1', '901', 1, now),
                     Fill('s2p', 'MSFT', dt.date(2026, 9, 1), 7.0, 400.0, None, 'e2', '902', 2, now)])
    t = validate_targets(make_doc(weights={'AAPL': 0.5, 'MSFT': 0.3}))
    plan = make_plan(t, gw, today=TODAY)
    assert plan.holdings == {'AAPL': 25, 'MSFT': 7} and plan.orders == ()
    assert [r.id for r in plan.open_proposals] == [3]
    with pytest.raises(Refusal, match='UNPRICEABLE'):
        make_plan(validate_targets(make_doc(weights={'DEAD': 0.1})), gw, today=TODAY)
