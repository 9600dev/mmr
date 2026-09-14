"""``trade_age_s`` survives the 2,048-tick raw-stream cap (2026-09-14).

``_cap_tick_stream`` trims the raw tick stream to its last 2,048 ticks once a
bar buffer exists, but ``build_runtime_status`` still derived ``trade_age_s``
(age of the last tick where cumulative volume ROSE) from that stream. On a
quote-heavy instrument the last trade rolled out of the window within
minutes and the pulse read "no trade seen" mid-session — the condition
docs/MONITORING.md escalates on. The ingest path now keeps a per-conId
scalar and the pulse reads that.
"""
import math

import numpy as np
import pandas as pd
import pytest
from ib_async import Contract, Ticker

from trader.data.market_data import NORMALIZED_COLUMNS
from trader.objects import BarSize
from trader.strategy.live_bars import LiveBarBuffer
from trader.strategy.strategy_runtime import (
    StrategyRuntime, _last_trade_age, build_runtime_status, format_pulse)

CONID = 1


def _runtime():
    rt = StrategyRuntime.__new__(StrategyRuntime)
    rt.streams, rt._hist_bars, rt._last_dispatched_bar = {}, {}, {}
    rt._oos_bars, rt._oos_logged, rt._oos_last = {}, set(), {}
    rt._tick_retention_days = 2
    rt.strategies, rt.strategy_implementations = {}, []
    rt._retired_strategies, rt._managed_contexts, rt._contracts = [], {}, {}
    rt._live_bar_buffers = {(CONID, BarSize.Mins1): LiveBarBuffer(None, '1min')}
    return rt


def _tick(stamp, last, volume):
    ticker = Ticker(contract=Contract(conId=CONID), time=stamp.to_pydatetime())
    ticker.last = last
    ticker.volume = volume
    return ticker


def _normalized(stamp, volume):
    frame = pd.DataFrame(index=pd.DatetimeIndex([stamp], name='date'), columns=NORMALIZED_COLUMNS,
                         dtype=float)
    frame['close'] = 100.0
    frame['volume'] = volume
    return frame


@pytest.mark.timeout(120)
def test_trade_age_is_reported_after_more_than_2048_quote_only_ticks():
    rt = _runtime()
    now = pd.Timestamp.now(tz='UTC').floor('s')
    start = now - pd.Timedelta(seconds=2300)
    rt.on_ticker_next(_tick(start, 100.0, 1000))
    traded_at = start + pd.Timedelta(seconds=1)
    rt.on_ticker_next(_tick(traded_at, 100.5, 1500))            # the only trade
    for n in range(2, 2202):                                     # 2,200 quote-only ticks
        rt.on_ticker_next(_tick(start + pd.Timedelta(seconds=n), 100.5, 1500))

    assert len(rt.streams[CONID]) == 2048, 'the raw stream is capped once a bar buffer exists'
    assert _last_trade_age(rt.streams[CONID], now) is None, 'the trade has rolled out of the raw stream'

    status = rt.runtime_status()
    expected = int((now - traded_at).total_seconds())
    assert abs(status['trade_age_s'][CONID] - expected) <= 5
    assert f'trade_age_s=[{CONID}:' in format_pulse(status)


def test_note_trade_follows_the_documented_volume_rule():
    rt = _runtime()
    t = pd.Timestamp('2026-09-14 14:00:00', tz='UTC')
    rt._note_trade(CONID, _normalized(t, 1000))
    assert CONID not in rt._last_trade_ts, 'a first observation establishes the base, not a trade'
    rt._note_trade(CONID, _normalized(t + pd.Timedelta(seconds=1), 1000))
    assert CONID not in rt._last_trade_ts, 'flat volume is a quote'
    rt._note_trade(CONID, _normalized(t + pd.Timedelta(seconds=2), 1100))
    assert rt._last_trade_ts[CONID] == t + pd.Timedelta(seconds=2)
    rt._note_trade(CONID, _normalized(t + pd.Timedelta(seconds=3), 10))       # day-boundary reset
    assert rt._last_trade_ts[CONID] == t + pd.Timedelta(seconds=2), 'a decrease is not a trade'
    rt._note_trade(CONID, _normalized(t + pd.Timedelta(seconds=4), math.nan))  # unknown volume
    rt._note_trade(CONID, _normalized(t + pd.Timedelta(seconds=5), 40))
    assert rt._last_trade_ts[CONID] == t + pd.Timedelta(seconds=5), 'the next increase after the reset counts'


def test_build_runtime_status_prefers_the_scalar_over_the_raw_stream():
    now = pd.Timestamp('2026-07-16 15:00:00', tz='UTC')
    quotes_only = pd.DataFrame({'close': [1.0, 1.0], 'volume': [900, 900]},
                               index=pd.DatetimeIndex([now - pd.Timedelta(seconds=90),
                                                       now - pd.Timedelta(seconds=30)]))
    status = build_runtime_status(now, [], {CONID: quotes_only}, {}, auto_exec_open=0,
                                  last_trade_ts={CONID: now - pd.Timedelta(seconds=600)})
    assert status['trade_age_s'] == {CONID: 600}
    legacy = build_runtime_status(now, [], {CONID: quotes_only}, {}, auto_exec_open=0)
    assert legacy['trade_age_s'] == {}, 'without the scalar the stream-derived answer is kept'
    empty = build_runtime_status(now, [], {CONID: quotes_only}, {}, auto_exec_open=0, last_trade_ts={})
    assert empty['trade_age_s'] == {}, 'no trade observed since subscription is reported as absent'
