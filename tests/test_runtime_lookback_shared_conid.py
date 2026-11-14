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


# --------------------------------------------------------------------------
# AUDIT_2026-11-14 B3.4: the history half of the frame was read once. Live bars
# are kept two days, so a service up for longer served a frame with a hole.
# --------------------------------------------------------------------------

KEY = (CONID, BarSize.Mins1)


def _primed(tmp_path, read):
    rt = _runtime(tmp_path)
    strategy = _strategy('short', 5)
    rt.strategy_implementations.append(strategy)
    rt.subscribe(strategy, Contract(conId=CONID))
    assert rt._strategy_frame(CONID, BarSize.Mins1) is not None
    assert read.call_count == 1
    return rt


def _age(rt, seconds):
    rt._hist_primed_at[KEY] -= seconds


def test_a_fresh_snapshot_is_not_read_again(tmp_path, read):
    rt = _primed(tmp_path, read)
    _age(rt, StrategyRuntime.HISTORY_SNAPSHOT_MAX_AGE_S - 60)
    for _ in range(5):
        rt._strategy_frame(CONID, BarSize.Mins1)
    assert read.call_count == 1


def test_an_aged_snapshot_is_read_again_and_the_new_bars_are_served(tmp_path, read):
    rt = _primed(tmp_path, read)
    assert len(rt._hist_bars[KEY]) == 3
    read.return_value = _history(periods=8)               # the nightly refresh has added bars since
    _age(rt, StrategyRuntime.HISTORY_SNAPSHOT_MAX_AGE_S + 1)

    frame = rt._strategy_frame(CONID, BarSize.Mins1)

    assert read.call_count == 2
    assert len(rt._hist_bars[KEY]) == 8 and len(frame) == 8
    rt._strategy_frame(CONID, BarSize.Mins1)
    assert read.call_count == 2, 'the refreshed snapshot is fresh again'


def test_a_refresh_that_reads_nothing_keeps_the_bars_it_has(tmp_path, read, caplog):
    rt = _primed(tmp_path, read)
    kept = rt._hist_bars[KEY]
    read.return_value = pd.DataFrame()
    _age(rt, StrategyRuntime.HISTORY_SNAPSHOT_MAX_AGE_S + 1)
    with caplog.at_level(logging.WARNING):
        frame = rt._strategy_frame(CONID, BarSize.Mins1)
    assert rt._hist_bars[KEY] is kept and len(frame) == 3
    assert any('keeping the current snapshot' in record.getMessage() for record in caplog.records)
    rt._strategy_frame(CONID, BarSize.Mins1)
    assert read.call_count == 2, 'an empty answer is not asked for again on every bar'


def test_a_refresh_that_fails_keeps_the_snapshot_and_is_not_retried_every_bar(tmp_path, read):
    rt = _primed(tmp_path, read)
    kept = rt._hist_bars[KEY]
    read.side_effect = RuntimeError('database is locked')
    _age(rt, StrategyRuntime.HISTORY_SNAPSHOT_MAX_AGE_S + 1)
    for _ in range(4):
        assert len(rt._strategy_frame(CONID, BarSize.Mins1)) == 3
    assert rt._hist_bars[KEY] is kept
    assert read.call_count == 2, 'one failed attempt, then the retry interval'

    read.side_effect = None
    read.return_value = _history(periods=6)
    rt._hist_refresh_retry_at[KEY] = 0.0                   # the retry interval has passed
    assert len(rt._strategy_frame(CONID, BarSize.Mins1)) == 6


def test_an_invalidated_frame_is_primed_from_scratch_not_refreshed(tmp_path, read):
    rt = _primed(tmp_path, read)
    rt._invalidate_history(CONID, BarSize.Mins1)
    assert KEY not in rt._hist_primed_at
    rt._strategy_frame(CONID, BarSize.Mins1)
    assert read.call_count == 2 and KEY in rt._hist_primed_at


def test_a_snapshot_of_unknown_age_is_left_alone(tmp_path, read):
    """Frames injected without going through the primer (tests, legacy paths)
    have no recorded age; they must not be re-read on a guess."""
    rt = _runtime(tmp_path)
    rt._hist_bars[KEY] = _history()
    strategy = _strategy('short', 5)
    rt.strategy_implementations.append(strategy)
    rt.subscribe(strategy, Contract(conId=CONID))
    rt._strategy_frame(CONID, BarSize.Mins1)
    assert read.call_count == 0
