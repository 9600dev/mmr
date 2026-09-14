"""Declared historical lookback is respected on a SHARED conId across a reload (2026-09-14).

``_prime_hist_bars`` sized the DuckDB read from the conId's CURRENT
subscribers. A source edit retired the 90-day strategy, the next tick
re-primed the shared frame with the remaining 5-day subscriber's window, and
``_reconcile_sync`` then subscribed the replacement to a frame too short for
its indicators — NaN signals, nothing logged. The window is now the maximum
declared by every strategy CONFIGURED for the conId (loaded, loading, being
replaced, subscribed, or a persisted exit policy), a new wider subscriber
invalidates a narrower primed frame, and a widening re-prime is logged.
"""
import datetime as dt
import logging
from unittest.mock import MagicMock, Mock

import numpy as np
import pandas as pd
import pytest
from ib_async import Contract

from trader.data.market_data import NORMALIZED_COLUMNS
from trader.objects import BarSize
from trader.strategy.strategy_runtime import StrategyRuntime
from trader.trading.strategy import Strategy, StrategyContext

CONID = 1


def _history(periods=3):
    index = pd.DatetimeIndex(pd.date_range('2026-09-08 13:30', periods=periods, freq='min', tz='UTC'),
                             name='date')
    frame = pd.DataFrame(index=index, columns=NORMALIZED_COLUMNS, dtype=float)
    for column in ('open', 'high', 'low', 'close', 'last', 'bid', 'ask', 'vwap'):
        frame[column] = 100.0
    frame['volume'] = np.arange(periods) + 100
    frame['last_size'] = 1.0
    return frame


def _runtime(tmp_path):
    rt = StrategyRuntime.__new__(StrategyRuntime)
    rt.streams = {}
    rt._hist_bars = {}
    rt._last_dispatched_bar = {}
    rt._oos_bars, rt._oos_logged, rt._oos_last = {}, set(), {}
    rt._tick_retention_days = 2
    rt.history_duckdb_path = str(tmp_path / 'history.duckdb')
    rt.strategies = {}
    rt.strategy_implementations = []
    rt._retired_strategies = []
    rt._published_conids = set()
    rt.trader_client = MagicMock()
    rt._deployment_lock = __import__('threading').RLock()
    rt._deployment_generations = {}
    rt._callback_workers = {}
    return rt


def _strategy(name, days, conids=(CONID,)):
    strategy = Strategy()
    strategy.install(StrategyContext(
        name=name, bar_size=BarSize.Mins1, conids=list(conids), universe=None,
        historical_days_prior=days, paper_only=False, storage=None, universe_accessor=None,
        logger=logging, params={}))
    strategy.enable()
    return strategy


def _requested_days(read: Mock) -> int:
    """Recover the declared lookback from the read window (start = end - (max(days,5)+5))."""
    kwargs = read.call_args.kwargs
    span = kwargs['end'] - kwargs['start']
    return int(round(span / dt.timedelta(days=1))) - 5


@pytest.fixture
def read(monkeypatch):
    read = Mock(return_value=_history())
    monkeypatch.setattr('trader.data.duckdb_store.DuckDBDataStore.read', read)
    return read


def test_reprime_after_replacement_keeps_the_replacements_declared_lookback(tmp_path, read):
    rt = _runtime(tmp_path)
    short, long_old = _strategy('short', 5), _strategy('long', 90)
    for strategy in (short, long_old):
        rt.strategy_implementations.append(strategy)
        rt.subscribe(strategy, Contract(conId=CONID))
    assert rt._strategy_frame(CONID, BarSize.Mins1) is not None
    assert _requested_days(read) == 90

    # A source edit: the old instance is retired, the replacement is loaded
    # but NOT yet subscribed (that happens a reconcile step later), and the
    # loader invalidates the conId's history.
    rt._retire_strategy(long_old)
    rt._retired_strategies.clear()                    # isolate: only the LOADED copy counts here
    long_new = _strategy('long', 90)
    rt.strategy_implementations.append(long_new)
    rt._invalidate_history(CONID, BarSize.Mins1)

    rt._strategy_frame(CONID, BarSize.Mins1)           # the next tick re-primes
    assert _requested_days(read) == 90, 'loaded-but-unsubscribed strategy must size the shared frame'

    rt.subscribe(long_new, Contract(conId=CONID))
    assert (CONID, BarSize.Mins1) in rt._hist_bars, 'the frame primed wide enough is kept as-is'


def test_strategy_being_replaced_still_counts_while_its_successor_loads(tmp_path, read):
    rt = _runtime(tmp_path)
    short, long_old = _strategy('short', 5), _strategy('long', 90)
    for strategy in (short, long_old):
        rt.strategy_implementations.append(strategy)
        rt.subscribe(strategy, Contract(conId=CONID))
    rt._retire_strategy(long_old)                     # in _retired_strategies, not subscribed
    rt._invalidate_history(CONID, BarSize.Mins1)
    rt._strategy_frame(CONID, BarSize.Mins1)
    assert _requested_days(read) == 90


def test_new_subscriber_declaring_more_history_invalidates_and_reprimes_wider(tmp_path, read, caplog):
    rt = _runtime(tmp_path)
    short = _strategy('short', 5)
    rt.strategy_implementations.append(short)
    rt.subscribe(short, Contract(conId=CONID))
    rt._strategy_frame(CONID, BarSize.Mins1)
    assert _requested_days(read) == 5
    narrow = rt._hist_bars[(CONID, BarSize.Mins1)]

    long_ = _strategy('long', 90)
    rt.strategy_implementations.append(long_)
    with caplog.at_level(logging.INFO):
        rt.subscribe(long_, Contract(conId=CONID))
        assert (CONID, BarSize.Mins1) not in rt._hist_bars, 'the 5-day frame is invalidated'
        rt._strategy_frame(CONID, BarSize.Mins1)
    assert _requested_days(read) == 90
    assert rt._hist_bars[(CONID, BarSize.Mins1)] is not narrow
    messages = [r.getMessage() for r in caplog.records]
    assert any('re-priming with the larger window' in m and 'long' in m for m in messages)
    assert any('re-primed history' in m and '5 -> 90' in m for m in messages)


def test_subscriber_declaring_less_history_does_not_churn_the_frame(tmp_path, read):
    rt = _runtime(tmp_path)
    long_ = _strategy('long', 90)
    rt.strategy_implementations.append(long_)
    rt.subscribe(long_, Contract(conId=CONID))
    rt._strategy_frame(CONID, BarSize.Mins1)
    primed = rt._hist_bars[(CONID, BarSize.Mins1)]
    short = _strategy('short', 5)
    rt.subscribe(short, Contract(conId=CONID))
    assert rt._hist_bars[(CONID, BarSize.Mins1)] is primed
    assert read.call_count == 1


def test_loading_and_universe_declared_conids_count_toward_the_window(tmp_path):
    rt = _runtime(tmp_path)
    assert rt._declared_lookback_days(CONID, BarSize.Mins1) == 0
    loading = _strategy('loading', 30)
    rt._loading_strategies = {'loading': loading}      # mid-load: not in implementations yet
    assert rt._declared_lookback_days(CONID, BarSize.Mins1) == 30
    via_universe = _strategy('universe', 60, conids=())  # subscribed through a universe
    via_universe._declared_conids = {CONID}
    rt.strategy_implementations.append(via_universe)
    assert rt._declared_lookback_days(CONID, BarSize.Mins1) == 60
    other_interval = _strategy('daily', 400)
    other_interval.ctx.bar_size = BarSize.Days1
    rt.strategy_implementations.append(other_interval)
    assert rt._declared_lookback_days(CONID, BarSize.Mins1) == 60, 'a different bar size is a different frame'
    assert rt._declared_lookback_days(CONID, BarSize.Days1) == 400
