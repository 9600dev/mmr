"""`mmr orders` does not wait for a quote a closed venue will not send (audit B6).

``MMR.orders()`` enriches each working order with a live bid/ask. Outside the
venue's session IB sends ``last`` / ``close`` and no bid or ask, so the
snapshot path waits out its partial-wait (4 s, ``IBAIORx.__get_single_mrkt_data``)
for every symbol and the listing took about 5 s for a market context it could
not show. The caller now asks only for instruments whose venue is in session
(extended hours included) or whose venue it cannot establish.

Sessions come from the real calendars via ``trader.data.market_session``; the
clock is the only thing replaced.
"""
import asyncio
import datetime as dt
import math
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from ib_async import Contract, Forex, LimitOrder, Option, OrderStatus, Stock, Trade
from reactivex.subject import Subject

from trader import sdk
from trader.listeners.ibreactive import IBAIORx
from trader.sdk import MMR

UTC = dt.timezone.utc
# New York is UTC-5 on these dates (after the November clock change).
SUNDAY = dt.datetime(2026, 11, 15, 18, 0, tzinfo=UTC)             # Sunday 13:00 ET
REGULAR = dt.datetime(2026, 11, 16, 15, 0, tzinfo=UTC)            # Monday 10:00 ET
PRE_MARKET = dt.datetime(2026, 11, 16, 9, 30, tzinfo=UTC)         # Monday 04:30 ET
POST_MARKET = dt.datetime(2026, 11, 17, 0, 30, tzinfo=UTC)        # Monday 19:30 ET
OVERNIGHT = dt.datetime(2026, 11, 17, 2, 0, tzinfo=UTC)           # Monday 21:00 ET
THANKSGIVING = dt.datetime(2026, 11, 26, 15, 0, tzinfo=UTC)       # Thursday 10:00 ET, holiday
ASX_OPEN_US_CLOSED = dt.datetime(2026, 11, 16, 1, 0, tzinfo=UTC)  # Monday 12:00 Sydney, Sunday 20:00 ET

AAPL = Stock('AAPL', 'SMART', 'USD', primaryExchange='NASDAQ', conId=265598)
BHP = Stock('BHP', 'ASX', 'AUD', conId=4036812)


def _trade(order_id, contract, action='BUY', limit=9.0):
    order = LimitOrder(action, 10, limit)
    order.orderId = order_id
    return [Trade(contract=contract, order=order,
                  orderStatus=OrderStatus(status='Submitted', remaining=10))]


def _mmr(trades, quotes=None):
    """MMR over a recording RPC; ``quotes`` are what the snapshot batch returns."""
    m = object.__new__(MMR)
    rpc = MagicMock()
    rpc.get_trades.return_value = trades
    rpc.get_account_values.return_value = {}

    def batch(contracts, delayed):
        return [dict(symbol=c.symbol, conId=c.conId, **(quotes or {}).get(c.symbol, {}))
                for c in contracts]

    rpc.get_snapshots_batch.side_effect = batch
    client = MagicMock()
    client.is_setup = True
    client.rpc.return_value = rpc
    m._client = client
    return m, rpc


def _requested(rpc):
    """Symbols the snapshot batch was asked for (empty when it was not called)."""
    return [c.symbol for call in rpc.get_snapshots_batch.call_args_list for c in call.args[0]]


@pytest.fixture
def clock(monkeypatch):
    def set_now(now):
        monkeypatch.setattr(sdk, '_utc_now', lambda: now)
    return set_now


# ---------------------------------------------------------------------------
# The defect: the listing waited out the snapshot path's partial-wait.
# ---------------------------------------------------------------------------

class _OffHoursTraderService:
    """trader_service's batch-snapshot path — the real ``IBAIORx`` code — fed
    what IB sends for a US stock off-hours: last, close and volume, and never
    a bid or an ask."""

    def __init__(self):
        self.requested = []

    def get_snapshots_batch(self, contracts, delayed):
        self.requested.extend(c.symbol for c in contracts)

        async def serve():
            rx = IBAIORx('127.0.0.1', 1, 1)

            def subscribe(**kwargs):
                subject = Subject()
                asyncio.get_running_loop().call_later(0.05, subject.on_next, SimpleNamespace(
                    last=10.05, bid=math.nan, ask=math.nan, open=math.nan, high=math.nan,
                    low=math.nan, close=10.0, volume=0.0))
                return subject

            rx._IBAIORx__subscribe_contract = subscribe
            return await rx.get_snapshots_batch(contracts, delayed)

        return asyncio.run(serve())


def test_orders_off_hours_does_not_wait_out_the_quote(clock):
    clock(SUNDAY)
    m, rpc = _mmr({1: _trade(1, AAPL)})
    service = _OffHoursTraderService()
    rpc.get_snapshots_batch.side_effect = service.get_snapshots_batch

    started = time.monotonic()
    df = m.orders()
    elapsed = time.monotonic() - started

    assert elapsed < 1.5, f'orders() took {elapsed:.1f}s waiting for a quote the closed venue will not send'
    assert service.requested == []
    row = df.iloc[0]
    assert row['orderId'] == 1 and row['symbol'] == 'AAPL' and row['lmtPrice'] == 9.0
    assert pd.isna(row['bid']) and pd.isna(row['ask']) and pd.isna(row['last'])
    assert row['quote_notes'] == ['venue session closed: quote not requested']


@pytest.mark.parametrize('now', [SUNDAY, OVERNIGHT, THANKSGIVING])
def test_no_quote_is_requested_while_the_venue_is_closed(clock, now):
    clock(now)
    m, rpc = _mmr({1: _trade(1, AAPL)})
    df = m.orders()
    assert _requested(rpc) == []
    assert df.iloc[0]['quote_notes'] == ['venue session closed: quote not requested']


# ---------------------------------------------------------------------------
# In session — extended hours included — quotes are fetched as before.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('now', [REGULAR, PRE_MARKET, POST_MARKET])
def test_quotes_are_fetched_while_the_venue_is_in_session(clock, now):
    clock(now)
    m, rpc = _mmr({1: _trade(1, AAPL)}, quotes={'AAPL': dict(bid=10.0, ask=10.1, last=10.05)})
    df = m.orders()
    assert _requested(rpc) == ['AAPL']
    assert rpc.get_snapshots_batch.call_args.args[1] is False           # live, not delayed
    row = df.iloc[0]
    assert (row['bid'], row['ask'], row['last']) == (10.0, 10.1, 10.05)
    assert row['quote_notes'] is None


def test_each_instrument_is_judged_on_its_own_venue(clock):
    clock(ASX_OPEN_US_CLOSED)
    m, rpc = _mmr({1: _trade(1, AAPL), 2: _trade(2, BHP, limit=40.0)},
                  quotes={'BHP': dict(bid=41.0, ask=41.1, last=41.05)})
    df = m.orders().set_index('symbol')
    assert _requested(rpc) == ['BHP']
    assert df.loc['BHP', 'bid'] == 41.0 and df.loc['BHP', 'quote_notes'] is None
    assert pd.isna(df.loc['AAPL', 'bid']) and pd.isna(df.loc['AAPL', 'last'])
    assert df.loc['AAPL', 'quote_notes'] == ['venue session closed: quote not requested']


def test_json_listing_carries_the_note(clock, capsys, monkeypatch):
    import json

    from trader import mmr_cli

    clock(ASX_OPEN_US_CLOSED)
    m, _ = _mmr({1: _trade(1, AAPL), 2: _trade(2, BHP, limit=40.0)},
                quotes={'BHP': dict(bid=41.0, ask=41.1, last=41.05)})
    monkeypatch.setattr(mmr_cli, '_json_mode', True)
    mmr_cli.print_df(m.orders(), title='Orders')
    rows = {row['symbol']: row for row in json.loads(capsys.readouterr().out)['data']}
    assert rows['AAPL']['quote_notes'] == ['venue session closed: quote not requested']
    assert rows['AAPL']['bid'] is None and rows['AAPL']['last'] is None
    assert rows['BHP']['quote_notes'] is None and rows['BHP']['bid'] == 41.0


def test_smart_without_a_listing_venue_is_the_us_session_only_for_usd(clock):
    """An order loaded from IB after a restart has exchange SMART and no
    primaryExchange. SMART is a route, not a venue."""
    clock(ASX_OPEN_US_CLOSED)
    us = Stock('MSFT', 'SMART', 'USD', conId=272093)
    not_us = Stock('CBA', 'SMART', 'AUD', conId=4036818)
    m, rpc = _mmr({1: _trade(1, us), 2: _trade(2, not_us)})
    m.orders()
    assert _requested(rpc) == ['CBA']


# ---------------------------------------------------------------------------
# Anything not positively known to be a closed stock venue: today's behaviour.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('contract', [
    Stock('WALMEX', 'SMART', 'MXN', primaryExchange='MEXI', conId=11),      # unmapped listing venue
    Stock('WALMEX', 'MEXI', 'MXN', conId=12),                               # unmapped direct route
    Stock('XYZ', 'SMART', 'USD', primaryExchange='MEXI', conId=13),         # named venue wins over SMART
    Stock('NOVENUE', '', 'USD', conId=14),                                  # blank exchange
    Option('AAPL', '20261218', 200, 'C', 'SMART', currency='USD', conId=15),
    Forex('EURUSD', conId=16),
    Contract(secType='FUT', symbol='ES', exchange='CME', currency='USD', conId=17),
    SimpleNamespace(symbol='BARE', conId=18),                               # no venue fields at all
], ids=lambda c: f'{getattr(c, "secType", "?")}-{c.symbol}-{getattr(c, "exchange", "?")}')
def test_unknown_venue_or_non_stock_is_fetched_as_before(clock, contract):
    clock(SUNDAY)
    m, rpc = _mmr({1: _trade(1, contract)})
    m._to_contract = lambda c: c
    df = m.orders()
    assert _requested(rpc) == [contract.symbol]
    assert df.iloc[0]['quote_notes'] is None


def test_a_session_lookup_that_fails_fetches_as_before(clock, monkeypatch):
    from trader.data import market_session

    def broken(*args, **kwargs):
        raise RuntimeError('calendar unavailable')

    clock(SUNDAY)
    monkeypatch.setattr(market_session, 'in_session', broken)
    m, rpc = _mmr({1: _trade(1, AAPL)})
    df = m.orders()
    assert _requested(rpc) == ['AAPL']
    assert df.iloc[0]['quote_notes'] is None


def test_an_unreadable_calendar_fetches_as_before(clock, monkeypatch):
    """market_session fails OPEN on a calendar error; so does this."""
    from trader.data import market_session

    def broken(name):
        raise RuntimeError('calendar unavailable')

    clock(SUNDAY)
    monkeypatch.setattr(market_session, '_calendar', broken)
    monkeypatch.setattr(market_session, '_schedule_cache', {})
    m, rpc = _mmr({1: _trade(1, AAPL)})
    m.orders()
    assert _requested(rpc) == ['AAPL']
