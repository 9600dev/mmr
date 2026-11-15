"""`mmr strategies pnl`: what one closed trade is, and what the figures include.

AUDIT_2026-11-14 B5.7.

* A closed trade is ONE EXIT ORDER matched against an attributed lot. The
  order tracker writes one ORDER_FILLED row per cumulative-fill DELTA, so an
  exit that fills in three partials is three rows; counting rows made it three
  trades and three wins (or losses).
* The ledger is price x quantity. Fill events carry no commission, so every
  figure is gross and the report has to say so.
"""
import datetime as dt
from types import SimpleNamespace

import pytest

from trader.data.event_store import EventStore, EventType, TradingEvent, pair_fills_long_only

T0 = dt.datetime(2026, 11, 13, 10, 0)


def _fill(action, qty, price, order=None, strategy='orb_a', conid=1, seconds=0):
    row = (strategy, conid, action, qty, price, T0 + dt.timedelta(seconds=seconds))
    return row if order is None else row + (order,)


# ---------------------------------------------------------------------------
# pair_fills_long_only: partial fills of one exit order are one closed trade
# ---------------------------------------------------------------------------

def test_exit_filled_in_three_partials_is_one_closed_trade():
    closed, open_lots, unmatched = pair_fills_long_only([
        _fill('BUY', 30, 100.0, order='entry'),
        _fill('SELL', 10, 110.0, order='exit', seconds=1),
        _fill('SELL', 10, 111.0, order='exit', seconds=2),
        _fill('SELL', 10, 112.0, order='exit', seconds=3),
    ])
    assert len(closed) == 1
    trade = closed[0]
    assert trade['quantity'] == pytest.approx(30.0)
    assert trade['pnl'] == pytest.approx(10 * 10 + 10 * 11 + 10 * 12)
    assert trade['entry_price'] == pytest.approx(100.0)
    assert trade['exit_price'] == pytest.approx(111.0)      # volume-weighted
    assert trade['closed_at'] == T0 + dt.timedelta(seconds=3)   # the last fill
    assert trade['fills'] == 3
    assert open_lots == [] and unmatched == 0


def test_two_exit_orders_against_one_lot_are_two_closed_trades():
    # A stop that takes 4 and a close that takes the rest are two decisions.
    closed, open_lots, unmatched = pair_fills_long_only([
        _fill('BUY', 10, 100.0, order='entry'),
        _fill('SELL', 4, 95.0, order='stop', seconds=1),
        _fill('SELL', 3, 104.0, order='close', seconds=2),
        _fill('SELL', 3, 106.0, order='close', seconds=3),
    ])
    assert [(t['quantity'], t['fills']) for t in closed] == [(4.0, 1), (6.0, 2)]
    assert [t['pnl'] for t in closed] == [pytest.approx(-20.0), pytest.approx(30.0)]
    assert open_lots == [] and unmatched == 0


def test_fills_without_an_order_identity_are_not_merged():
    # Legacy rows (order_id 0, no broker identity) cannot be grouped: each
    # SELL row stays its own closed trade, as before.
    closed, _, _ = pair_fills_long_only([
        _fill('BUY', 10, 100.0),
        _fill('SELL', 4, 110.0),
        _fill('SELL', 6, 110.0),
    ])
    assert [t['quantity'] for t in closed] == [4.0, 6.0]
    assert [t['fills'] for t in closed] == [1, 1]


def test_a_buy_between_partials_starts_a_new_closed_trade():
    # The cost basis changed between the two fills, so they are not one
    # (entry, exit) pair even though the exit order is the same.
    closed, open_lots, _ = pair_fills_long_only([
        _fill('BUY', 10, 100.0, order='entry'),
        _fill('SELL', 5, 110.0, order='exit', seconds=1),
        _fill('BUY', 5, 120.0, order='add', seconds=2),
        _fill('SELL', 10, 110.0, order='exit', seconds=3),
    ])
    assert [t['quantity'] for t in closed] == [5.0, 10.0]
    assert closed[0]['entry_price'] == pytest.approx(100.0)
    assert closed[1]['entry_price'] == pytest.approx(110.0)
    assert open_lots == []


def test_one_unpriced_partial_makes_the_whole_exit_unevaluable():
    closed, _, _ = pair_fills_long_only([
        _fill('BUY', 10, 100.0, order='entry'),
        _fill('SELL', 5, 110.0, order='exit', seconds=1),
        _fill('SELL', 5, None, order='exit', seconds=2),
    ])
    assert len(closed) == 1
    assert closed[0]['quantity'] == pytest.approx(10.0)
    assert closed[0]['pnl'] is None and closed[0]['exit_price'] is None


def test_oversized_exit_is_capped_the_same_way_however_it_fills():
    # test_sell_capped_at_lot_counts_once pins: SELL 15 against a lot of 10 in
    # ONE fill closes 10 and is not an unmatched sell. The same order arriving
    # as 10 + 5 must not report something different.
    closed, open_lots, unmatched = pair_fills_long_only([
        _fill('BUY', 10, 100.0, order='entry'),
        _fill('SELL', 10, 110.0, order='exit', seconds=1),
        _fill('SELL', 5, 110.0, order='exit', seconds=2),
    ])
    assert len(closed) == 1 and closed[0]['quantity'] == pytest.approx(10.0)
    assert open_lots == [] and unmatched == 0


def test_a_different_order_with_no_lot_is_still_unmatched():
    closed, _, unmatched = pair_fills_long_only([
        _fill('BUY', 10, 100.0, order='entry'),
        _fill('SELL', 10, 110.0, order='exit', seconds=1),
        _fill('SELL', 5, 110.0, order='another', seconds=2),
    ])
    assert len(closed) == 1 and unmatched == 1


# ---------------------------------------------------------------------------
# EventStore.realized_pnl_by_strategy over rows shaped like the order tracker's
# ---------------------------------------------------------------------------

def _append(store, action, qty, price, order_id, *, seconds=0, strategy='orb_a',
            conid=1, identity=True, when=None):
    metadata = {'price_evaluable': True}
    if identity:
        metadata['broker_identity'] = f'U1:perm:{9000 + order_id}'
    store.append(TradingEvent(
        EventType.ORDER_FILLED, (when or dt.datetime.now()) + dt.timedelta(seconds=seconds),
        strategy, conid=conid, symbol='X', action=action, quantity=qty, price=price,
        order_id=order_id, metadata=metadata))


@pytest.fixture
def store(tmp_path):
    return EventStore(str(tmp_path / 'ledger.duckdb'))


def test_three_partials_count_as_one_trade_and_one_win(store):
    _append(store, 'BUY', 30, 100.0, 1)
    for i, price in enumerate((110.0, 111.0, 112.0), start=1):
        _append(store, 'SELL', 10, price, 2, seconds=i)

    report = store.realized_pnl_by_strategy()
    bucket = report['strategies']['orb_a']
    assert bucket['closed_trades'] == 1
    assert bucket['wins'] == 1
    assert bucket['realized_total'] == pytest.approx(330.0)
    assert bucket['realized_today'] == pytest.approx(330.0)
    assert len(report['closed_trades']) == 1
    assert report['closed_trades'][0]['fills'] == 3


def test_three_losing_partials_are_one_loss(store):
    _append(store, 'BUY', 30, 100.0, 1)
    for i in (1, 2, 3):
        _append(store, 'SELL', 10, 95.0, 2, seconds=i)
    _append(store, 'BUY', 10, 100.0, 3, seconds=10)
    _append(store, 'SELL', 10, 101.0, 4, seconds=11)

    bucket = store.realized_pnl_by_strategy()['strategies']['orb_a']
    assert bucket['closed_trades'] == 2
    assert bucket['wins'] == 1                 # 50%, not 1 of 4
    assert bucket['realized_total'] == pytest.approx(-150.0 + 10.0)


def test_order_id_groups_partials_when_there_is_no_broker_identity(store):
    _append(store, 'BUY', 20, 100.0, 1, identity=False)
    _append(store, 'SELL', 10, 110.0, 2, seconds=1, identity=False)
    _append(store, 'SELL', 10, 110.0, 2, seconds=2, identity=False)
    assert store.realized_pnl_by_strategy()['strategies']['orb_a']['closed_trades'] == 1


def test_reused_order_id_does_not_join_two_broker_orders(store):
    # IB re-issued order ids after a gateway recreate (B1.14). The broker
    # identity differs, so two exits that share a numeric id stay two trades.
    _append(store, 'BUY', 20, 100.0, 1)
    store.append(TradingEvent(
        EventType.ORDER_FILLED, dt.datetime.now() + dt.timedelta(seconds=1), 'orb_a',
        conid=1, action='SELL', quantity=10, price=110.0, order_id=7,
        metadata={'broker_identity': 'U1:perm:111'}))
    store.append(TradingEvent(
        EventType.ORDER_FILLED, dt.datetime.now() + dt.timedelta(seconds=2), 'orb_a',
        conid=1, action='SELL', quantity=10, price=110.0, order_id=7,
        metadata={'broker_identity': 'U1:perm:222'}))
    assert store.realized_pnl_by_strategy()['strategies']['orb_a']['closed_trades'] == 2


# ---------------------------------------------------------------------------
# Commissions: not in the fill events, so the ledger must say it is gross
# ---------------------------------------------------------------------------

def test_report_states_that_figures_exclude_commissions(store):
    _append(store, 'BUY', 10, 100.0, 1)
    _append(store, 'SELL', 10, 110.0, 2, seconds=1)
    report = store.realized_pnl_by_strategy()
    assert report['pnl_basis'] == 'gross'
    assert report['commissions_included'] is False


@pytest.fixture
def ledger_cli(tmp_path, monkeypatch):
    from trader import mmr_cli, sdk
    from trader.container import Container

    path = str(tmp_path / 'cli-ledger.duckdb')
    ledger = EventStore(path)
    monkeypatch.setattr(Container, 'instance', classmethod(
        lambda cls: SimpleNamespace(config=lambda: {'duckdb_path': path})))
    rpc = SimpleNamespace(rpc=lambda **kwargs: SimpleNamespace(get_portfolio=lambda: []))
    monkeypatch.setattr(sdk, 'MMR', lambda **kwargs: SimpleNamespace(
        connect=lambda: SimpleNamespace(_rpc=rpc)))
    rendered, titles, printed = [], [], []
    monkeypatch.setattr(mmr_cli, 'print_dict', lambda obj, **kwargs: rendered.append(obj))
    monkeypatch.setattr(mmr_cli, 'print_df', lambda frame, **kwargs: (
        rendered.append(frame.to_dict(orient='records')), titles.append(kwargs.get('title', ''))))
    monkeypatch.setattr(mmr_cli.console, 'print', lambda *a, **k: printed.append(' '.join(map(str, a))))
    return ledger, rendered, titles, printed, mmr_cli


def test_cli_json_says_commissions_are_not_recorded(ledger_cli, monkeypatch):
    ledger, rendered, _titles, _printed, cli = ledger_cli
    _append(ledger, 'BUY', 30, 100.0, 1)
    for i in (1, 2, 3):
        _append(ledger, 'SELL', 10, 110.0, 2, seconds=i)
    monkeypatch.setattr(cli, '_json_mode', True)
    cli._handle_strategy_pnl(SimpleNamespace(all=False))

    out = rendered[0]
    assert out['pnl_basis'] == 'gross'
    assert out['commissions_included'] is False
    assert 'not recorded' in out['commissions']
    row = out['rows'][0]
    assert row['trades'] == 1 and row['win_rate'] == '100%'
    assert row['realized_total'] == pytest.approx(300.0)     # key kept, value gross


def test_cli_table_says_gross_and_names_the_reason(ledger_cli, monkeypatch):
    ledger, _rendered, titles, printed, cli = ledger_cli
    _append(ledger, 'BUY', 10, 100.0, 1)
    _append(ledger, 'SELL', 10, 110.0, 2, seconds=1)
    monkeypatch.setattr(cli, '_json_mode', False)
    cli._handle_strategy_pnl(SimpleNamespace(all=False))

    assert 'gross' in titles[0].lower()
    assert any('commissions' in line and 'not recorded' in line for line in printed)
