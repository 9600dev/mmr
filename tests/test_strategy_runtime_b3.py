"""Strategy-runtime regressions from the 2026-11-14 audit (Part B3). No broker, no sockets.

B3.2  The startup history backfill was one pass in which the first instrument
      to raise ended priming for every later one, and nothing retried. Now a
      failure is that instrument's: the pass continues, the reconcile loop
      retries it (three attempts in total), and a strategy that declares
      history is not dispatched bars for an instrument while its backfill is
      still pending or due a retry.
B3.6  The callback worker's queue bound was the strategy's conId count AT
      WORKER START; a universe that grew later (or was empty then) overflowed
      it: ``CallbackQueueFull`` and a permanent ERROR for an ordinary bar.
B3.7  SIGNAL audit rows stored ``price=0.0`` and often ``symbol=''``; a reload
      RPC during the startup backfill raised ``AttributeError`` on
      ``_config_mtime``.
"""
import asyncio
import os
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import numpy as np
import pandas as pd
import pytest
from ib_async import Contract
from ib_async.ticker import Ticker

import trader.strategy.strategy_runtime as runtime_module
from trader.listeners.ib_history_worker import IBConnectivityError
from trader.objects import Action, BarSize
from trader.strategy.callback_worker import CallbackResult
from trader.strategy.strategy_runtime import (
    _MAX_BACKFILL_ATTEMPTS, _MAX_CALLBACK_PENDING, _MIN_CALLBACK_PENDING, StrategyRuntime)
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState

M1 = BarSize.Mins1


# --------------------------------------------------------------------------
# harness
# --------------------------------------------------------------------------

def _runtime(tmp_path, isolate=False):
    """A real ``StrategyRuntime`` (real ``__init__``), never connected."""
    config = tmp_path / 'strategy_runtime.yaml'
    config.write_text('strategies: []\n')
    rt = StrategyRuntime(
        ib_server_address='127.0.0.1', ib_server_port=1, strategy_runtime_ib_client_id=99,
        duckdb_path=str(tmp_path / 'mmr.duckdb'), universe_library='Universes',
        zmq_pubsub_server_address='tcp://127.0.0.1', zmq_pubsub_server_port=1,
        zmq_rpc_server_address='tcp://127.0.0.1', zmq_rpc_server_port=1,
        zmq_strategy_rpc_server_address='tcp://127.0.0.1', zmq_strategy_rpc_server_port=1,
        zmq_messagebus_server_address='tcp://127.0.0.1', zmq_messagebus_server_port=1,
        strategies_directory=str(tmp_path / 'strategies'), strategy_config_file=str(config),
        history_duckdb_path=str(tmp_path / 'history.duckdb'), paper_trading=True,
        strategy_isolate_callbacks=isolate,
    )
    rt.events = []
    rt.event_store = SimpleNamespace(append=rt.events.append)
    rt.zmq_messagebus_client = SimpleNamespace(write=lambda *args, **kwargs: None)
    rt.auto_executor = MagicMock()
    rt.auto_executor.open_entry_bar.return_value = None
    rt.auto_executor.managed_positions.return_value = []
    rt.auto_executor.open_count.return_value = 0
    rt.trader_client = MagicMock()
    rt.trader_client.rpc().get_status.return_value = {'boot_id': 'boot'}
    rt.trader_client.rpc().resolve_symbol.side_effect = lambda conId: [_contract(conId)]
    return rt


def _contract(conid, symbol=None):
    # CASH on IDEALPRO: the session gate fails open for it, so bars dispatch
    # whatever the wall clock says.
    return Contract(conId=conid, symbol=symbol or f'S{conid}', secType='CASH',
                    exchange='IDEALPRO', primaryExchange='IDEALPRO', currency='USD')


class Recorder(Strategy):
    def __init__(self, signal=None):
        super().__init__()
        self.calls = []
        self.signal = signal

    def on_prices(self, prices):
        self.calls.append(prices)
        if self.signal is None:
            return None
        return Signal(source_name='x', action=self.signal, probability=0.6, risk=0.4)


def _install(rt, strategy, conid=None, name='s', days=5, universe=None):
    context = StrategyContext(
        name=name, bar_size=M1, conids=[conid] if conid is not None else [], universe=universe,
        historical_days_prior=days, paper_only=False, storage=None, universe_accessor=None,
        logger=None, params={})
    strategy.install(context)
    strategy.state = StrategyState.RUNNING
    rt.strategy_implementations.append(strategy)
    if conid is not None:
        rt.strategies.setdefault(conid, []).append(strategy)
    return strategy


def _tick(contract, stamp, last=100.0, volume=float('nan')):
    ticker = Ticker(contract=contract)
    ticker.time = stamp.to_pydatetime()
    ticker.last, ticker.bid, ticker.ask, ticker.volume = last, last - 0.01, last + 0.01, volume
    return ticker


def _feed(rt, contract, start, minutes, price=lambda minute: 100.0, first_tick=0):
    """Three ticks a minute; returns the index of the next tick to send."""
    count = first_tick
    for k in range(first_tick, first_tick + 3 * minutes):
        rt.on_ticker_next(_tick(contract, start + pd.Timedelta(seconds=20 * k),
                                last=price(k // 3), volume=10.0 * (k + 1)))
        count = k + 1
    return count


def _history(end, bars=30, price=99.0):
    index = pd.date_range(end=end - pd.Timedelta(minutes=1), periods=bars, freq='1min', name='date')
    return pd.DataFrame({'open': price, 'high': price, 'low': price, 'close': price, 'volume': 1000.0,
                         'vwap': price, 'bar_count': 10.0, 'bid': np.nan, 'ask': np.nan,
                         'last': price, 'last_size': np.nan}, index=index)


def _start():
    return pd.Timestamp.now(tz='UTC').floor('min') - pd.Timedelta(minutes=30)


def _fetcher(rt, fail=lambda conid, call: None):
    """Replace the IB fetch; ``fail(conId, nth call for it)`` returns an exception or None."""
    attempted, calls = [], {}

    async def fetch(security, bar_size, historical_days, strategy_name):
        calls[security.conId] = calls.get(security.conId, 0) + 1
        attempted.append((security.conId, strategy_name, historical_days))
        error = fail(security.conId, calls[security.conId])
        if error is not None:
            raise error

    rt._fetch_history_with_resume = fetch
    rt._reconnect_historical_client = AsyncMock()
    return attempted


def _three_strategies(rt):
    for conid, name in ((1, 'global_like'), (2, 'orb_googl'), (3, 'orb_pltr')):
        _install(rt, Recorder(), conid, name=name, days=90)


FARM_NOTICE = 'error_code: 2108, error_string: Market data farm connection is inactive'


@pytest.fixture
def runtime_log(monkeypatch):
    """Messages the runtime logs, by level.

    Spied at the module's own logger rather than read through caplog: the
    runtime imports some modules lazily, each of which re-applies the logging
    config on first import and drops caplog's root handler mid-test.
    """
    seen = {'info': [], 'error': []}
    for level in seen:
        original = getattr(runtime_module.logging, level)

        def spy(message, *args, _level=level, _original=original, **kwargs):
            seen[_level].append(str(message) % args if args else str(message))
            return _original(message, *args, **kwargs)
        monkeypatch.setattr(runtime_module.logging, level, spy)
    return seen


# --------------------------------------------------------------------------
# B3.2 — per-instrument isolation, bounded retry
# --------------------------------------------------------------------------

class TestBackfillIsolation:
    def test_one_instrument_raising_does_not_end_the_pass(self, tmp_path, runtime_log):
        """The reviewer's reproduction: the first instrument raised and
        orb_googl / orb_pltr were never attempted."""
        rt = _runtime(tmp_path)
        _three_strategies(rt)
        attempted = _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE) if conid == 1 else None)

        asyncio.run(rt.get_historical_data())

        assert [name for _, name, _ in attempted] == ['global_like', 'orb_googl', 'orb_pltr']
        assert rt._backfill_failures[(1, M1)]['attempts'] == 1
        assert (1, M1) not in rt._backfill_done and {(2, M1), (3, M1)} <= rt._backfill_done
        assert not rt._backfill_pending
        failure = [message for message in runtime_log['error'] if 'history backfill for' in message]
        assert len(failure) == 1
        assert 'S1' in failure[0] and 'global_like' in failure[0] and '2108' in failure[0]
        assert 'will retry (attempt 1/3)' in failure[0]

    @pytest.mark.parametrize('error', [IBConnectivityError('error_code: 1100'), ConnectionError('not connected')])
    def test_connectivity_errors_still_propagate_from_the_startup_pass(self, tmp_path, error):
        rt = _runtime(tmp_path)
        _three_strategies(rt)
        attempted = _fetcher(rt, lambda conid, call: error if conid == 2 and call == 1 else None)

        with pytest.raises(type(error)):
            asyncio.run(rt.get_historical_data())
        assert [conid for conid, _, _ in attempted] == [1, 2]
        assert rt._backfill_pending == {(2, M1), (3, M1)}, 'unprimed instruments stay pending (held)'
        assert not rt._backfill_failures

        # _run_services reconnects and re-runs the pass: the primed instrument
        # is not fetched again, the rest are.
        asyncio.run(rt.get_historical_data())
        assert [conid for conid, _, _ in attempted] == [1, 2, 2, 3]
        assert not rt._backfill_pending and not rt._backfill_failures
        assert rt._backfill_done == {(1, M1), (2, M1), (3, M1)}

    def test_unresolvable_conid_is_skipped_not_retried(self, tmp_path):
        rt = _runtime(tmp_path)
        _three_strategies(rt)
        rt.trader_client.rpc().resolve_symbol.side_effect = (
            lambda conId: [] if conId == 2 else [_contract(conId)])
        attempted = _fetcher(rt)
        asyncio.run(rt.get_historical_data())
        assert [conid for conid, _, _ in attempted] == [1, 3]
        assert not rt._backfill_pending and not rt._backfill_failures
        assert not rt._backfill_due(2, M1)

    def test_resolve_failure_is_isolated_too(self, tmp_path):
        rt = _runtime(tmp_path)
        _three_strategies(rt)

        def resolve(conId):
            if conId == 1:
                raise TimeoutError('resolve_symbol timed out')
            return [_contract(conId)]
        rt.trader_client.rpc().resolve_symbol.side_effect = resolve
        attempted = _fetcher(rt)
        asyncio.run(rt.get_historical_data())
        assert [conid for conid, _, _ in attempted] == [2, 3]
        assert rt._backfill_failures[(1, M1)]['attempts'] == 1

    def test_a_universe_that_cannot_be_read_costs_only_that_strategy(self, tmp_path, runtime_log):
        rt = _runtime(tmp_path)
        _install(rt, Recorder(), None, name='broken_universe', days=90, universe='broken')
        _install(rt, Recorder(), None, name='portfolio', days=90, universe='portfolio')
        _install(rt, Recorder(), 3, name='orb_pltr', days=90)

        def get(name):
            if name == 'broken':
                raise OSError('universe store locked')
            return SimpleNamespace(security_definitions=[_contract(11), _contract(12)])
        rt.universe_accessor = SimpleNamespace(get=get)
        attempted = _fetcher(rt)
        asyncio.run(rt.get_historical_data())
        assert [conid for conid, _, _ in attempted] == [11, 12, 3]
        assert any('could not read universe broken' in message for message in runtime_log['error'])

    def test_a_shared_instrument_is_fetched_once_with_the_widest_window(self, tmp_path):
        rt = _runtime(tmp_path)
        _install(rt, Recorder(), 1, name='short', days=5)
        _install(rt, Recorder(), 1, name='long', days=90)
        attempted = _fetcher(rt)
        asyncio.run(rt.get_historical_data())
        assert attempted == [(1, 'short', 90)]

    def test_bars_fetched_but_not_stored_are_a_failed_backfill(self, tmp_path):
        """The real fetch path: a failed ``tick_data.write`` was a WARNING and
        the instrument counted as primed."""
        rt = _runtime(tmp_path)
        _install(rt, Recorder(), 1, name='orb', days=5)
        security = _contract(1)
        security.timeZoneId = 'US/Eastern'
        rt.trader_client.rpc().resolve_symbol.side_effect = lambda conId: [security]
        bars = _history(pd.Timestamp.now(tz='UTC').floor('min'))
        rt.historical_data_client = SimpleNamespace(get_contract_history=AsyncMock(return_value=bars))
        write = Mock(side_effect=OSError('database is locked'))
        rt.storage = SimpleNamespace(get_tickdata=lambda bar_size: SimpleNamespace(write=write))
        rt._try_get_exchange_calendar = lambda security: None      # full-window pull

        asyncio.run(rt.get_historical_data())
        assert write.call_count == 1
        record = rt._backfill_failures[(1, M1)]
        assert record['attempts'] == 1 and 'could not be stored' in record['error']
        assert rt._backfill_due(1, M1)

        write.side_effect = None                                   # the store recovers
        asyncio.run(rt.get_historical_data(retry_failed=True))
        assert not rt._backfill_failures and (1, M1) in rt._backfill_done


class TestBackfillRetry:
    def test_failed_instrument_is_retried_and_only_it(self, tmp_path, runtime_log):
        rt = _runtime(tmp_path)
        _three_strategies(rt)
        attempted = _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE) if conid == 1 and call == 1 else None)
        asyncio.run(rt.get_historical_data())
        assert rt._backfill_due(1, M1)

        asyncio.run(rt.get_historical_data(retry_failed=True))          # the reconcile-loop pass
        assert [conid for conid, _, _ in attempted] == [1, 2, 3, 1], 'primed instruments are not re-fetched'
        assert not rt._backfill_failures and (1, M1) in rt._backfill_done
        assert not rt._backfill_due(1, M1)
        assert any('succeeded on retry' in message for message in runtime_log['info'])

    def test_retries_are_bounded_then_left_alone(self, tmp_path, runtime_log):
        rt = _runtime(tmp_path)
        _three_strategies(rt)
        attempted = _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE) if conid == 1 else None)

        asyncio.run(rt.get_historical_data())                            # attempt 1 (startup)
        for expected in range(2, _MAX_BACKFILL_ATTEMPTS + 1):
            assert rt._backfill_due(1, M1)
            asyncio.run(rt.get_historical_data(retry_failed=True))
            assert rt._backfill_failures[(1, M1)]['attempts'] == expected

        assert _MAX_BACKFILL_ATTEMPTS == 3
        assert not rt._backfill_due(1, M1), 'the hold ends when the retries are exhausted'
        gave_up = [message for message in runtime_log['error'] if 'giving up' in message]
        assert len(gave_up) == 1 and 'S1' in gave_up[0] and 'global_like' in gave_up[0]

        before = len(attempted)
        asyncio.run(rt.get_historical_data(retry_failed=True))
        asyncio.run(rt.get_historical_data())
        assert len(attempted) == before, 'an exhausted instrument is not fetched again'
        status = rt.runtime_status()['history_backfill']
        assert status['failed'] == [{'conid': 1, 'bar_size': '1 min', 'symbol': 'S1', 'strategy': 'global_like',
                                     'attempts': 3, 'gave_up': True, 'error': f'Exception: {FARM_NOTICE}'}]

    def test_connectivity_error_ends_a_retry_pass_without_raising(self, tmp_path, runtime_log):
        rt = _runtime(tmp_path)
        _three_strategies(rt)
        attempted = _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE) if call == 1 else (
            IBConnectivityError('error_code: 1100') if call == 2 else None))
        asyncio.run(rt.get_historical_data())                # all three fail once
        runtime_log['error'].clear()
        asyncio.run(rt.get_historical_data(retry_failed=True))
        # The first retry hit a dead gateway: reconnect, stop hammering it.
        assert [conid for conid, _, _ in attempted] == [1, 2, 3, 1]
        rt._reconnect_historical_client.assert_awaited_once()
        # IB was unreachable for every instrument waiting, not just the one the
        # pass stopped on: each is charged, so each hold ends on the same tick.
        assert {key: record['attempts'] for key, record in rt._backfill_failures.items()} == {
            (1, M1): 2, (2, M1): 2, (3, M1): 2}
        assert len(runtime_log['error']) == 1, 'one line for the pass, not one per instrument'
        assert 'could not reach IB for 3 instrument(s)' in runtime_log['error'][0]

        asyncio.run(rt.get_historical_data(retry_failed=True))
        assert not any(rt._backfill_due(conid, M1) for conid in (1, 2, 3))

    def test_instruments_the_startup_pass_never_reached_go_to_the_retry(self, tmp_path, runtime_log):
        """IB unreachable through every reconnect: nothing was primed. They
        used to be dropped for the life of the process."""
        rt = _runtime(tmp_path)
        _three_strategies(rt)
        attempted = _fetcher(rt)
        rt._mark_backfill_pending()
        assert all(rt._backfill_due(conid, M1) for conid in (1, 2, 3))

        runtime_log['error'].clear()
        rt._abandon_pending_backfills('IBConnectivityError: error_code: 1100')
        assert not rt._backfill_pending
        assert len(runtime_log['error']) == 1 and 'did not finish at startup for 3 instrument(s)' in \
            runtime_log['error'][0]
        assert {key: record['attempts'] for key, record in rt._backfill_failures.items()} == {
            (1, M1): 1, (2, M1): 1, (3, M1): 1}
        assert all(rt._backfill_due(conid, M1) for conid in (1, 2, 3))

        asyncio.run(rt.get_historical_data(retry_failed=True))
        assert [conid for conid, _, _ in attempted] == [1, 2, 3]
        assert not rt._backfill_failures
        assert not any(rt._backfill_due(conid, M1) for conid in (1, 2, 3))

    def test_a_failure_nobody_is_configured_for_any_more_stops_holding(self, tmp_path):
        rt = _runtime(tmp_path)
        _three_strategies(rt)
        _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE) if conid == 1 else None)
        asyncio.run(rt.get_historical_data())
        rt.strategy_implementations = [s for s in rt.strategy_implementations if s.name != 'global_like']
        asyncio.run(rt.get_historical_data(retry_failed=True))
        assert not rt._backfill_failures and not rt._backfill_due(1, M1)

    @pytest.mark.asyncio
    async def test_reconcile_loop_schedules_one_background_retry(self, tmp_path):
        rt = _runtime(tmp_path)
        _three_strategies(rt)
        gate = asyncio.Event()
        attempted = []

        async def fetch(security, bar_size, historical_days, strategy_name):
            attempted.append(security.conId)
            if len(attempted) == 1:
                raise Exception(FARM_NOTICE)
            if security.conId == 1:
                await gate.wait()                                     # a slow IB fetch
        rt._fetch_history_with_resume = fetch

        await rt.get_historical_data()
        rt._schedule_backfill_retry()
        task = rt._backfill_task
        await asyncio.sleep(0)
        rt._schedule_backfill_retry()
        assert rt._backfill_task is task, 'a pass still fetching is not doubled'
        assert not task.done(), 'the retry runs in the background (it must not delay the pulse)'
        gate.set()
        await asyncio.wait_for(task, 5)
        assert attempted == [1, 2, 3, 1] and not rt._backfill_failures

        rt._schedule_backfill_retry()
        assert rt._backfill_task is task, 'nothing left to retry: no new task'

    @pytest.mark.asyncio
    async def test_a_crashing_retry_pass_still_counts_as_an_attempt(self, tmp_path):
        rt = _runtime(tmp_path)
        _three_strategies(rt)
        _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE) if conid == 1 else None)
        await rt.get_historical_data()
        rt._backfill_targets = Mock(side_effect=RuntimeError('unexpected'))
        for _ in range(_MAX_BACKFILL_ATTEMPTS):
            await rt._retry_backfills()
        assert not rt._backfill_due(1, M1), 'bounded whatever goes wrong'


# --------------------------------------------------------------------------
# B3.2 — dispatch is held while an instrument is not primed
# --------------------------------------------------------------------------

class TestDispatchHold:
    def _primed_frame_runtime(self, tmp_path, days=5):
        rt = _runtime(tmp_path)
        strategy = _install(rt, Recorder(), 7, name='orb', days=days)
        start = _start()
        # The DuckDB read of stored bars is not what is under test.
        rt._hist_bars[(7, M1)] = _history(start)
        return rt, strategy, _contract(7, 'TEST'), start

    def test_bars_are_held_until_the_backfill_finishes(self, tmp_path, runtime_log):
        rt, strategy, contract, start = self._primed_frame_runtime(tmp_path)
        _fetcher(rt)
        rt._mark_backfill_pending()                          # startup, before the first subscription

        sent = _feed(rt, contract, start, minutes=3)
        assert strategy.calls == [], 'no bar while today may still be missing from the frame'
        assert (7, 'orb') not in rt._last_dispatched_bar, 'held, not consumed'
        assert rt.auto_executor.open_entry_bar.called, 'position management is not held'
        held = [message for message in runtime_log['info'] if 'holding bars for strategy orb' in message]
        assert len(held) == 1, 'logged once, not per tick'

        asyncio.run(rt.get_historical_data())                # the instrument is primed
        _feed(rt, contract, start, minutes=1, first_tick=sent)
        assert len(strategy.calls) == 1
        frame = strategy.calls[0]
        assert len(frame) >= 30 + 3, 'released with history AND the live bars formed meanwhile'

    def test_held_while_a_retry_is_due_and_released_when_exhausted(self, tmp_path):
        rt, strategy, contract, start = self._primed_frame_runtime(tmp_path)
        _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE))
        asyncio.run(rt.get_historical_data())
        sent = _feed(rt, contract, start, minutes=2)
        assert strategy.calls == []

        for _ in range(_MAX_BACKFILL_ATTEMPTS - 1):
            asyncio.run(rt.get_historical_data(retry_failed=True))
        _feed(rt, contract, start, minutes=1, first_tick=sent)
        assert len(strategy.calls) == 1, 'a held strategy is not held forever'

    def test_a_strategy_that_declares_no_history_is_not_held(self, tmp_path):
        rt, strategy, contract, start = self._primed_frame_runtime(tmp_path, days=0)
        rt._mark_backfill_pending()
        _feed(rt, contract, start, minutes=2)
        assert len(strategy.calls) >= 1

    def test_an_instrument_with_no_planned_backfill_is_not_held(self, tmp_path):
        """A strategy deployed after startup: nothing planned, nothing to wait for."""
        rt, strategy, contract, start = self._primed_frame_runtime(tmp_path)
        _feed(rt, contract, start, minutes=2)
        assert len(strategy.calls) >= 1


# --------------------------------------------------------------------------
# startup sequence (B3.2 end to end, B3.7b)
# --------------------------------------------------------------------------

class _FakePubSub:
    def __class_getitem__(cls, item):
        return cls

    def __init__(self, *args, **kwargs):
        pass

    async def subscriber(self, topic):
        return SimpleNamespace(subscribe=lambda observer: SimpleNamespace(dispose=lambda: None))

    def subscriber_close(self):
        pass


class _FakeHistoryWorker:
    connected = True

    def __init__(self, *args, **kwargs):
        pass

    async def connect_async(self):
        pass

    def shutdown(self):
        pass


def _serviceable(rt, monkeypatch):
    """Everything ``run()`` needs from the network, faked; real sleeps shortened."""
    rt.zmq_messagebus_client = SimpleNamespace(connect=AsyncMock(), disconnect=AsyncMock(), write=Mock())
    rt.zmq_strategy_rpc_server = SimpleNamespace(serve=AsyncMock(), aclose=AsyncMock())
    rt.trader_client.connect = AsyncMock()
    monkeypatch.setattr(runtime_module, 'TopicPubSub', _FakePubSub)
    monkeypatch.setattr(runtime_module, 'IBHistoryWorker', _FakeHistoryWorker)
    real_sleep = asyncio.sleep

    async def fast_sleep(delay, *args, **kwargs):
        await real_sleep(min(delay, 0.01))
    monkeypatch.setattr(runtime_module.asyncio, 'sleep', fast_sleep)
    return real_sleep


async def _stop(task):
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


class TestStartupSequence:
    def test_reconcile_on_a_runtime_whose_backfill_has_not_finished(self, tmp_path):
        """``reload_strategies`` -> ``_reconcile_sync`` before ``_run_services``
        reached the line that set ``_config_mtime`` (it sat after the backfill)."""
        rt = _runtime(tmp_path)
        rt._reconcile_sync()                                  # AttributeError before the fix
        assert rt._config_mtime == os.path.getmtime(rt.strategy_config_file)

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_config_mtime_is_tracked_before_the_backfill_starts(self, tmp_path, monkeypatch):
        rt = _runtime(tmp_path)
        real_sleep = _serviceable(rt, monkeypatch)
        rt.__dict__.pop('_config_mtime')                      # only _run_services may provide it
        seen = {}

        async def backfill(retry_failed=False):
            seen['mtime'] = rt.__dict__.get('_config_mtime')
            await asyncio.to_thread(rt._reconcile_sync)       # a reload RPC arriving mid-backfill
            seen['reconciled'] = True
        rt.get_historical_data = backfill

        task = asyncio.ensure_future(rt.run())
        try:
            deadline = time.monotonic() + 20
            while 'reconciled' not in seen and not task.done() and time.monotonic() < deadline:
                await real_sleep(0.01)
        finally:
            await _stop(task)
        assert seen.get('mtime') == os.path.getmtime(rt.strategy_config_file)
        assert seen.get('reconciled') is True

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_startup_failure_is_retried_by_the_reconcile_loop(self, tmp_path, monkeypatch):
        """End to end through ``run()``: the first instrument's backfill fails,
        the others are primed in the same pass, the reconcile loop primes the
        first one on its next tick."""
        rt = _runtime(tmp_path)
        real_sleep = _serviceable(rt, monkeypatch)
        rt.config_loader = lambda path: None                  # strategies are installed directly
        _three_strategies(rt)
        attempted = _fetcher(rt, lambda conid, call: Exception(FARM_NOTICE) if conid == 1 and call == 1 else None)
        due_while_failed = []
        original = rt._schedule_backfill_retry

        def schedule():
            due_while_failed.append(rt._backfill_due(1, M1))
            original()
        rt._schedule_backfill_retry = schedule

        task = asyncio.ensure_future(rt.run())
        try:
            deadline = time.monotonic() + 20
            while (1, M1) not in rt._backfill_done and not task.done() and time.monotonic() < deadline:
                await real_sleep(0.01)
        finally:
            await _stop(task)
        assert [conid for conid, _, _ in attempted] == [1, 2, 3, 1]
        assert due_while_failed[0] is True, 'held between the failure and the retry'
        assert not rt._backfill_failures and not rt._backfill_pending

    @pytest.mark.asyncio
    @pytest.mark.timeout(60)
    async def test_gateway_down_at_startup_leaves_nothing_pending_forever(self, tmp_path, monkeypatch):
        rt = _runtime(tmp_path)
        real_sleep = _serviceable(rt, monkeypatch)
        rt.config_loader = lambda path: None
        _three_strategies(rt)
        rt._reconnect_historical_client = AsyncMock()
        attempted, after_startup = [], {}

        async def fetch(security, bar_size, historical_days, strategy_name):
            attempted.append(security.conId)
            if len(attempted) <= 5:                            # down through all five startup attempts
                raise IBConnectivityError('error_code: 1100')
            after_startup.setdefault('pending', set(rt._backfill_pending))
            after_startup.setdefault('failures', {key: record['attempts']
                                                  for key, record in rt._backfill_failures.items()})
        rt._fetch_history_with_resume = fetch

        task = asyncio.ensure_future(rt.run())
        try:
            deadline = time.monotonic() + 20
            while len(rt._backfill_done) < 3 and not task.done() and time.monotonic() < deadline:
                await real_sleep(0.01)
        finally:
            await _stop(task)
        assert attempted == [1, 1, 1, 1, 1, 1, 2, 3], 'five startup attempts, then the reconcile retry'
        assert after_startup['pending'] == set(), 'the startup pass gave up: nothing may stay pending'
        assert after_startup['failures'] == {(1, M1): 1, (2, M1): 1, (3, M1): 1}
        assert rt._backfill_done == {(1, M1), (2, M1), (3, M1)} and not rt._backfill_failures


# --------------------------------------------------------------------------
# B3.6 — the worker queue bound follows the live subscription count
# --------------------------------------------------------------------------

class TestCallbackQueueBound:
    def test_bound_is_the_live_subscription_count_with_a_floor_and_a_cap(self, tmp_path):
        rt = _runtime(tmp_path, isolate=True)
        strategy = _install(rt, Strategy(), None, name='global', days=0, universe='portfolio')
        assert rt._callback_pending_bound(strategy) == _MIN_CALLBACK_PENDING, 'empty universe at start'
        for conid in range(1, 13):                            # the universe grows
            rt.strategies.setdefault(conid, []).append(strategy)
        assert rt._callback_pending_bound(strategy) == 12
        for conid in range(13, 401):
            rt.strategies.setdefault(conid, []).append(strategy)
        assert rt._callback_pending_bound(strategy) == _MAX_CALLBACK_PENDING == 256
        other = _install(rt, Strategy(), 1, name='orb', days=0)
        assert rt._callback_pending_bound(other) == _MIN_CALLBACK_PENDING >= 2

    def test_every_submission_carries_the_current_bound(self, tmp_path):
        rt = _runtime(tmp_path, isolate=True)
        strategy = _install(rt, Strategy(), None, name='global', days=0, universe='portfolio')
        worker = SimpleNamespace(submit=Mock(return_value=True), pid=1, failed=False)
        rt._callback_workers['global'] = worker              # started while the universe was empty
        start = _start()
        for conid in range(1, 13):
            rt.strategies.setdefault(conid, []).append(strategy)
            _feed(rt, _contract(conid), start, minutes=2)
        assert worker.submit.call_count >= 12
        assert worker.submit.call_args.kwargs['max_pending'] == 12
        assert worker.submit.call_args_list[0].kwargs['max_pending'] == _MIN_CALLBACK_PENDING

    @pytest.mark.asyncio
    @pytest.mark.timeout(120)
    async def test_universe_grown_after_worker_start_does_not_overflow_the_queue(self, tmp_path, monkeypatch):
        """Real worker process. Twelve instruments complete a bar in the same
        instant for a strategy whose worker started with none subscribed:
        before the fix the second submission was ``CallbackQueueFull`` and the
        strategy went to ERROR for good."""
        import trader.container as container
        from review.test_review_strategy_contract import _make_runtime
        monkeypatch.setattr(container, 'MMR_CONFIG_DIR', tmp_path / 'config')
        directory = tmp_path / 'strategies'
        directory.mkdir()
        source = directory / 'slow.py'
        source.write_text(
            'import time\n'
            'from trader.trading.strategy import Strategy\n'
            'class Slow(Strategy):\n'
            '    def on_prices(self, frame):\n'
            '        time.sleep(0.05)\n'
            '        return None\n')
        rt = _make_runtime(tmp_path, directory)
        rt.duckdb_path = str(tmp_path / 'runtime.duckdb')
        rt._isolate_callbacks = True
        rt._loop = asyncio.get_running_loop()
        rt._load_enabled = lambda name: True
        rt._last_dispatched_bar, rt._oos_bars, rt._oos_last, rt._oos_logged = {}, {}, {}, set()
        rt._tick_retention_days = 2
        rt.auto_executor = MagicMock()
        rt.auto_executor.open_entry_bar.return_value = None
        await asyncio.to_thread(rt.load_strategy, 'slow', '1 min', [], None, 0, str(source), 'Slow', '')
        try:
            strategy = rt.get_strategy('slow')
            assert strategy is not None and strategy.state == StrategyState.RUNNING
            worker = rt._callback_workers['slow']
            results = []
            rt._callback_result = lambda s, result: results.append(result.conid)
            stamp = pd.Timestamp.now(tz='UTC').floor('min') - pd.Timedelta(minutes=2)
            rt._strategy_frame = lambda *args: pd.DataFrame({'close': [100.]}, index=pd.DatetimeIndex([stamp]))
            conids = list(range(1, 13))
            for conid in conids:                              # the universe grew after the worker started
                rt.strategies[conid] = [strategy]
            for conid in conids:                              # ...and all of them complete a bar at once
                rt.on_ticker_next(_tick(_contract(conid), stamp + pd.Timedelta(minutes=1), volume=100.0))
            deadline = asyncio.get_running_loop().time() + 30
            while len(results) < len(conids) and not worker.failed:
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(.01)
            assert not worker.failed
            assert sorted(results) == conids
            assert strategy.state == StrategyState.RUNNING
            assert worker.max_pending == 12
        finally:
            for worker in list(getattr(rt, '_callback_workers', {}).values()):
                await asyncio.to_thread(worker.stop)


# --------------------------------------------------------------------------
# B3.7a — SIGNAL audit rows carry the bar close and the symbol
# --------------------------------------------------------------------------

class TestSignalAudit:
    def test_in_process_signal_records_close_and_symbol(self, tmp_path):
        rt = _runtime(tmp_path)
        _install(rt, Recorder(signal=Action.BUY), 7, name='buyer', days=0)
        _feed(rt, _contract(7, 'TEST'), _start(), minutes=3, price=lambda minute: 123.45)
        assert rt.events, 'the strategy signalled'
        event = rt.events[-1]
        assert event.price == 123.45, 'the close of the bar the strategy acted on (was always 0.0)'
        assert event.symbol == 'TEST'
        assert event.conid == 7 and event.strategy_name == 'buyer'

    def test_symbol_is_known_from_the_subscription_alone(self, tmp_path):
        rt = _runtime(tmp_path)
        strategy = _install(rt, Recorder(), 9, name='s', days=0)
        rt.subscribe(strategy, _contract(9, 'SUBSCRIBED'))
        assert rt._symbol_for(9) == 'SUBSCRIBED'
        assert rt._symbol_for(404) == ''

    def test_worker_signal_records_the_close_of_its_own_bar(self, tmp_path):
        """Callback-worker path: the result comes back with only the bar
        timestamp — and possibly after a newer bar has been dispatched."""
        rt = _runtime(tmp_path, isolate=True)
        strategy = _install(rt, Strategy(), 7, name='isolated', days=0)
        rt._grant_generation(strategy)
        worker = SimpleNamespace(submit=Mock(return_value=True), pid=1, failed=False)
        rt._callback_workers['isolated'] = worker
        _feed(rt, _contract(7, 'TEST'), _start(), minutes=3, price=lambda minute: 100.0 + minute)
        assert worker.submit.call_count == 2                  # two completed bars dispatched

        generation = strategy.ctx.deployment_generation
        for call, close in zip(worker.submit.call_args_list, (100.0, 101.0)):
            conid, frame, bar_ts = call.args[:3]
            assert frame['close'].iloc[-1] == close
            rt._callback_result(strategy, CallbackResult(
                conid, bar_ts, generation, Signal('x', Action.BUY, 0.6, 0.4)))
        assert [event.price for event in rt.events] == [100.0, 101.0]
        assert {event.symbol for event in rt.events} == {'TEST'}

    def test_unknown_bar_is_recorded_as_zero_not_guessed(self, tmp_path):
        rt = _runtime(tmp_path)
        strategy = _install(rt, Recorder(), 7, name='s', days=0)
        rt._handle_signal(strategy, 7, Signal('x', Action.SELL, 0.6, 0.4), pd.Timestamp('2020-01-01', tz='UTC'))
        assert rt.events[-1].price == 0.0
