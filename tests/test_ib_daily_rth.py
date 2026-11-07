"""IB daily-and-longer stock bars use the regular session (official close); intraday keeps extended hours."""
import asyncio
import datetime as dt

import pytest
from ib_async.contract import Contract

from trader.listeners.ib_history_worker import IBHistoryWorker, use_rth_for
from trader.objects import BarSize, WhatToShow


@pytest.mark.parametrize('bar_size', [BarSize.Days1, BarSize.Weeks1, BarSize.Months1, '1 day'])
def test_daily_stock_bars_use_rth(bar_size):
    assert use_rth_for(bar_size, 'STK') is True
    assert use_rth_for(bar_size, 'IND') is True


@pytest.mark.parametrize('bar_size', [BarSize.Mins1, BarSize.Mins5, BarSize.Hours1, '1 min'])
def test_intraday_keeps_extended_hours(bar_size):
    assert use_rth_for(bar_size, 'STK') is False


@pytest.mark.parametrize('sec_type', ['FUT', 'CASH', 'CRYPTO', '', None])
def test_non_equity_daily_keeps_full_session(sec_type):
    assert use_rth_for(BarSize.Days1, sec_type) is False


class _Event(list):
    def __iadd__(self, fn):
        self.append(fn)
        return self


class _FakeIB:
    def __init__(self):
        self.errorEvent = _Event()
        self.calls = []

    def isConnected(self):
        return True

    async def reqHistoricalDataAsync(self, contract, **kw):
        self.calls.append(kw)
        return []


def _request_kwargs(bar_size, sec_type='STK'):
    w = IBHistoryWorker('127.0.0.1', 0, 0)
    w.ib_client = _FakeIB()  # type: ignore[assignment]
    w.connected = True
    c = Contract(conId=265598, symbol='AAPL', secType=sec_type, exchange='SMART', currency='USD')
    end = dt.datetime(2026, 11, 6, tzinfo=dt.timezone.utc)
    try:
        asyncio.run(w.get_contract_history(c, WhatToShow.TRADES, bar_size,
                                           end - dt.timedelta(days=3), end))
    except Exception:
        pass  # empty fake result; only the request matters here
    assert w.ib_client.calls, 'no history request issued'
    return w.ib_client.calls[0]


def test_request_passes_rth_for_daily():
    assert _request_kwargs(BarSize.Days1)['useRTH'] is True


def test_request_extended_for_intraday():
    assert _request_kwargs(BarSize.Mins1)['useRTH'] is False
