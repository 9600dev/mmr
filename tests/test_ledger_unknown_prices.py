"""A recovered quantity is inventory evidence, not evidence of a zero price."""
import datetime as dt
import sys
from types import SimpleNamespace

import pytest

from trader.data.event_store import EventStore, EventType, TradingEvent, pair_fills_long_only


def _row(action, quantity, price):
    return ('strategy', 1, action, quantity, price, dt.datetime(2026, 9, 8, 10))


@pytest.mark.parametrize('unknown_price', [
    None, 0.0, -1.0, float('nan'), float('inf'), sys.float_info.max, 'missing', True,
])
def test_unpriced_round_trip_consumes_inventory_before_next_priced_trade(unknown_price):
    closed, open_lots, unmatched = pair_fills_long_only([
        _row('BUY', 10, unknown_price),
        _row('SELL', 4, 110),
        _row('SELL', 6, 120),
        _row('BUY', 5, 100),
        _row('SELL', 5, 110),
    ])
    assert [trade['quantity'] for trade in closed] == [4, 6, 5]
    assert [trade['pnl'] for trade in closed] == [None, None, 50]
    assert closed[0]['entry_price'] is None
    assert open_lots == []
    assert unmatched == 0


def test_unknown_add_contaminates_weighted_basis_until_flat():
    closed, open_lots, unmatched = pair_fills_long_only([
        _row('BUY', 10, 100),
        _row('BUY', 10, None),
        _row('SELL', 10, 120),
        _row('BUY', 5, 105),
        _row('SELL', 5, 120),
    ])
    assert [trade['pnl'] for trade in closed] == [None, None]
    assert open_lots == [{'strategy': 'strategy', 'conid': 1,
                          'quantity': 10, 'entry_price': None}]
    assert unmatched == 0


def test_unknown_exit_does_not_erase_remaining_entry_basis():
    closed, open_lots, unmatched = pair_fills_long_only([
        _row('BUY', 10, 100),
        _row('SELL', 4, None),
        _row('SELL', 2, 110),
    ])
    assert closed[0]['exit_price'] is None
    assert [trade['pnl'] for trade in closed] == [None, 20]
    assert open_lots == [{'strategy': 'strategy', 'conid': 1,
                          'quantity': 4, 'entry_price': 100}]
    assert unmatched == 0


def _append(store, action, quantity, price, timestamp, *, conid=1, metadata=None):
    store.append(TradingEvent(
        EventType.ORDER_FILLED, timestamp, 'strategy', conid=conid,
        action=action, quantity=quantity, price=price, metadata=metadata or {},
    ))


@pytest.mark.parametrize('unknown_today', [False, True])
def test_summary_marks_only_affected_periods_unavailable(tmp_path, unknown_today):
    store = EventStore(str(tmp_path / 'ledger.duckdb'))
    today = dt.datetime.now().replace(hour=10, minute=0, second=0, microsecond=0)
    unknown_at = today if unknown_today else today - dt.timedelta(days=1)
    # A positive field marked unproven must not silently become a cost basis.
    _append(store, 'BUY', 10, 99, unknown_at,
            metadata={'price_evaluable': False})
    _append(store, 'SELL', 10, 110, unknown_at + dt.timedelta(seconds=1))
    _append(store, 'BUY', 5, 100, today + dt.timedelta(seconds=2))
    _append(store, 'SELL', 5, 110, today + dt.timedelta(seconds=3))

    report = store.realized_pnl_by_strategy()
    strategy = report['strategies']['strategy']
    assert strategy['closed_trades'] == 2
    assert strategy['realized_total'] is None
    assert strategy['wins'] is None
    assert strategy['realized_today'] == (None if unknown_today else 50)
    assert strategy['unevaluable_trades'] == 1
    assert strategy['unevaluable_trades_today'] == int(unknown_today)
    assert strategy['open_lots'] == []
    assert report['unevaluable_trades'] == 1
    assert report['unmatched_sells'] == 0
    assert [trade['pnl'] for trade in report['closed_trades']] == [None, 50]


@pytest.fixture
def ledger_cli(tmp_path, monkeypatch):
    from trader import mmr_cli, sdk
    from trader.container import Container

    path = str(tmp_path / 'cli-ledger.duckdb')
    store = EventStore(path)
    monkeypatch.setattr(Container, 'instance', classmethod(
        lambda cls: SimpleNamespace(config=lambda: {'duckdb_path': path})))
    portfolio = []
    rpc = SimpleNamespace(rpc=lambda **kwargs: SimpleNamespace(
        get_portfolio=lambda: portfolio))
    connected = SimpleNamespace(_rpc=rpc)
    monkeypatch.setattr(sdk, 'MMR', lambda **kwargs: SimpleNamespace(connect=lambda: connected))
    rendered = []
    monkeypatch.setattr(mmr_cli, 'print_dict', lambda obj, **kwargs: rendered.append(obj))
    monkeypatch.setattr(mmr_cli, 'print_df', lambda frame, **kwargs:
                        rendered.append(frame.to_dict(orient='records')))
    return store, portfolio, rendered, mmr_cli


@pytest.mark.parametrize('json_mode', [False, True])
def test_cli_unknown_realized_pnl_never_becomes_zero(ledger_cli, monkeypatch, json_mode):
    store, portfolio, rendered, cli = ledger_cli
    now = dt.datetime.now()
    _append(store, 'BUY', 10, 0, now)  # legacy row without evaluability metadata
    _append(store, 'SELL', 10, 110, now + dt.timedelta(seconds=1))
    monkeypatch.setattr(cli, '_json_mode', json_mode)
    cli._handle_strategy_pnl(SimpleNamespace(all=False))

    row = rendered[0]['rows'][0] if json_mode else rendered[0][0]
    unavailable = None if json_mode else '-'
    assert row['realized_total'] == unavailable
    assert row['realized_today'] == unavailable
    assert row['win_rate'] == unavailable
    assert row['trades'] == 1
    assert row['unevaluable_trades'] == 1
    assert row['open_qty'] == 0


@pytest.mark.parametrize('missing_basis', [False, True])
def test_cli_requires_every_open_lot_to_be_evaluable(ledger_cli, monkeypatch, missing_basis):
    store, portfolio, rendered, cli = ledger_cli
    now = dt.datetime.now()
    _append(store, 'BUY', 10, 100, now)
    _append(store, 'BUY', 5, 0 if missing_basis else 100, now, conid=2)
    portfolio.append(SimpleNamespace(contract=SimpleNamespace(conId=1), marketPrice=110))
    if missing_basis:
        portfolio.append(SimpleNamespace(contract=SimpleNamespace(conId=2), marketPrice=120))
    monkeypatch.setattr(cli, '_json_mode', True)
    cli._handle_strategy_pnl(SimpleNamespace(all=False))

    assert rendered[0]['rows'][0]['unrealized'] is None
    assert rendered[0]['rows'][0]['open_qty'] == 15


@pytest.mark.parametrize('json_mode', [False, True])
def test_cli_fill_listing_displays_unknown_price(ledger_cli, monkeypatch, json_mode):
    store, portfolio, rendered, cli = ledger_cli
    _append(store, 'BUY', 10, 99, dt.datetime.now(), metadata={'price_evaluable': False})
    monkeypatch.setattr(cli, '_json_mode', json_mode)
    cli._handle_strategy_fills(SimpleNamespace(strategy='strategy', limit=100))
    row = rendered[0]['fills'][0] if json_mode else rendered[0][0]
    assert row['price'] == (None if json_mode else '-')
    assert row['qty'] == 10
