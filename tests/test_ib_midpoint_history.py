"""IB MIDPOINT history (forex) reaches the store.

docs/AUDIT_2026-11-14.md B5.8. Strategy priming asks IB for MIDPOINT bars when
the contract is CASH (`strategy_runtime._whattoshow_for_contract`): spot FX
has no tape of trades to build a bar from. On a bar that was not built from
trades IB fills the three trade fields with -1 — volume, WAP ("average") and
the trade count. The frame went to `TickData.write` as delivered, and a
negative volume is (rightly) a bar that cannot describe a market, so every
forex bar was refused and quarantined: an FX strategy primed with nothing.

The rule is not what was wrong. -1 was never a volume; the request says what
the bar was built from, and the IB worker — the one place that knows IB's
convention — now says so before the frame leaves it.
"""
import asyncio
import datetime as dt
import math

import pandas as pd
import pytest
from ib_async.contract import Contract
from ib_async.objects import BarData

from trader.data.bar_quality import Bar, check_bar, impossible_mask
from trader.data.data_access import TickStorage
from trader.data.duckdb_store import DuckDBConnection
from trader.listeners.ib_history_worker import IBHistoryWorker
from trader.objects import BarSize, WhatToShow

UTC = dt.timezone.utc
EURUSD = Contract(conId=12087792, symbol='EUR', secType='CASH', exchange='IDEALPRO', currency='USD')
AAPL = Contract(conId=265598, symbol='AAPL', secType='STK', exchange='SMART', currency='USD')
FIRST = dt.datetime(2026, 11, 10, 14, 0, tzinfo=UTC)


class _Event(list):
    def __iadd__(self, fn):
        self.append(fn)
        return self


class _FakeIB:
    """Answers the first request with `bars`; the walk back in time gets nothing."""

    def __init__(self, bars):
        self.errorEvent = _Event()
        self.bars = bars
        self.calls = []

    def isConnected(self):
        return True

    async def reqHistoricalDataAsync(self, contract, **kw):
        self.calls.append(kw)
        return self.bars if len(self.calls) == 1 else []


def _ib_bars(volume, average, bar_count, n=5):
    """One-minute bars as `reqHistoricalData(formatDate=2)` returns them."""
    return [BarData(date=FIRST + dt.timedelta(minutes=i), open=1.0800 + i * 1e-4, high=1.0805 + i * 1e-4,
                    low=1.0795 + i * 1e-4, close=1.0801 + i * 1e-4, volume=volume, average=average,
                    barCount=bar_count) for i in range(n)]


def _history(contract, what_to_show, bars):
    worker = IBHistoryWorker('127.0.0.1', 0, 0)
    fake = _FakeIB(bars)
    setattr(worker, 'ib_client', fake)
    worker.connected = True
    frame = asyncio.run(worker.get_contract_history(
        contract, what_to_show, BarSize.Mins1, FIRST, FIRST + dt.timedelta(minutes=4),
        filter_between_dates=False))
    assert fake.calls[0]['whatToShow'] == str(what_to_show)
    return frame[frame['close'].notna()]


def _stored(path, conid):
    return DuckDBConnection.get_instance(path).execute(
        'SELECT volume, average, bar_count, what_to_show, close FROM tick_data '
        'WHERE symbol = ? ORDER BY date', [str(conid)], fetch='all')


def _quarantined(path):
    return DuckDBConnection.get_instance(path).execute(
        'SELECT count(*) FROM tick_data_quarantine', fetch='one')[0]


@pytest.mark.parametrize('what_to_show', [WhatToShow.MIDPOINT, WhatToShow.BID, WhatToShow.ASK])
def test_quote_bars_from_ib_are_stored(tmp_duckdb_path, what_to_show):
    frame = _history(EURUSD, what_to_show, _ib_bars(volume=-1.0, average=-1.0, bar_count=-1))
    assert len(frame) == 5

    tick_data = TickStorage(tmp_duckdb_path).get_tickdata(BarSize.Mins1)
    assert tick_data.write(EURUSD.conId, frame) == 5

    rows = _stored(tmp_duckdb_path, EURUSD.conId)
    assert len(rows) == 5 and _quarantined(tmp_duckdb_path) == 0
    for i, (volume, average, bar_count, stored_kind, close) in enumerate(rows):
        # No trades are aggregated in a quote bar, and the row says which
        # kind of bar it is. WAP and the trade count do not exist for it.
        assert volume == 0.0 and average is None and bar_count is None
        assert stored_kind == int(what_to_show)
        assert close == pytest.approx(1.0801 + i * 1e-4)

    read_back = tick_data.read(EURUSD.conId)
    assert len(read_back) == 5 and (read_back['volume'] == 0.0).all()
    assert not (read_back[['open', 'high', 'low', 'close']] <= 0).any().any()


def test_no_minus_one_is_stored_as_a_price_or_a_count(tmp_duckdb_path):
    """-1.0 in `average` is read by strategies as a VWAP of minus one dollar."""
    frame = _history(EURUSD, WhatToShow.MIDPOINT, _ib_bars(volume=-1.0, average=-1.0, bar_count=-1))
    assert not (frame[['volume', 'average', 'bar_count']] == -1).any().any()
    assert frame['average'].isna().all() and frame['bar_count'].isna().all()
    assert not impossible_mask(frame).any()
    # Judged by the spec itself, not only by its vectorised mask.
    for i, row in enumerate(frame.itertuples()):
        assert check_bar(Bar(float(i), row.open, row.high, row.low, row.close, row.volume), i) == []


def test_a_trades_bar_with_minus_one_volume_is_still_refused(tmp_duckdb_path):
    """The translation is for a request that was not for trades. On a TRADES
    bar -1 is not IB's "not applicable": it is a broken row."""
    frame = _history(AAPL, WhatToShow.TRADES, _ib_bars(volume=-1.0, average=-1.0, bar_count=-1))
    assert (frame['volume'] == -1.0).all() and (frame['average'] == -1.0).all()
    tick_data = TickStorage(tmp_duckdb_path).get_tickdata(BarSize.Mins1)
    assert tick_data.write(AAPL.conId, frame) == 0
    assert _stored(tmp_duckdb_path, AAPL.conId) == [] and _quarantined(tmp_duckdb_path) == 5


def test_a_trades_bar_is_passed_through_untouched(tmp_duckdb_path):
    frame = _history(AAPL, WhatToShow.TRADES, _ib_bars(volume=1200.0, average=1.0802, bar_count=17))
    assert (frame['volume'] == 1200.0).all() and (frame['average'] == 1.0802).all()
    assert (frame['bar_count'] == 17).all()
    tick_data = TickStorage(tmp_duckdb_path).get_tickdata(BarSize.Mins1)
    assert tick_data.write(AAPL.conId, frame) == 5
    assert {(row[0], row[1], row[2], row[3]) for row in _stored(tmp_duckdb_path, AAPL.conId)} == {
        (1200.0, 1.0802, 17, int(WhatToShow.TRADES))}


def test_only_the_exact_sentinel_is_translated_on_a_quote_bar(tmp_duckdb_path):
    """Any other negative volume on a MIDPOINT bar is not IB saying "not
    applicable", and real values (some venues do report them) are kept."""
    bars = _ib_bars(volume=-1.0, average=-1.0, bar_count=-1)
    bars[1].volume, bars[1].average, bars[1].barCount = -2.0, -0.5, -3
    bars[2].volume, bars[2].average, bars[2].barCount = 40.0, 1.0803, 6
    frame = _history(EURUSD, WhatToShow.MIDPOINT, bars)

    assert frame['volume'].tolist() == [0.0, -2.0, 40.0, 0.0, 0.0]
    assert frame['bar_count'].iloc[1] == -3 and frame['bar_count'].iloc[2] == 6
    assert frame['average'].iloc[1] == -0.5 and frame['average'].iloc[2] == 1.0803
    assert math.isnan(frame['average'].iloc[0]) and math.isnan(frame['bar_count'].iloc[0])
    assert impossible_mask(frame).tolist() == [False, True, False, False, False]

    tick_data = TickStorage(tmp_duckdb_path).get_tickdata(BarSize.Mins1)
    assert tick_data.write(EURUSD.conId, frame) == 4
    assert _quarantined(tmp_duckdb_path) == 1


def test_an_incoherent_quote_bar_is_still_refused(tmp_duckdb_path):
    """Being a quote bar excuses the missing trade fields and nothing else."""
    bars = _ib_bars(volume=-1.0, average=-1.0, bar_count=-1)
    bars[3].high = bars[3].close - 0.001        # a "high" below the close
    frame = _history(EURUSD, WhatToShow.MIDPOINT, bars)
    tick_data = TickStorage(tmp_duckdb_path).get_tickdata(BarSize.Mins1)
    assert tick_data.write(EURUSD.conId, frame) == 4
    assert _quarantined(tmp_duckdb_path) == 1


def test_no_data_markers_for_a_quote_series_are_unchanged():
    """The all-NULL marker rows the worker adds for empty days stay all-NULL."""
    worker = IBHistoryWorker('127.0.0.1', 0, 0)
    setattr(worker, 'ib_client', _FakeIB(_ib_bars(volume=-1.0, average=-1.0, bar_count=-1)))
    worker.connected = True
    frame = asyncio.run(worker.get_contract_history(
        EURUSD, WhatToShow.MIDPOINT, BarSize.Mins1, FIRST - dt.timedelta(days=2),
        FIRST + dt.timedelta(minutes=4), filter_between_dates=False))
    markers = frame[frame['close'].isna()]
    assert len(markers) >= 1
    assert markers[['open', 'high', 'low', 'close', 'volume', 'average', 'bar_count']].isna().all().all()
    assert not impossible_mask(frame).any()
    assert isinstance(frame.index, pd.DatetimeIndex)
