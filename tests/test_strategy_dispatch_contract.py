"""Deployment and incremental-feed regressions using the real runtime."""
import pandas as pd
import pytest
import yaml
from unittest.mock import Mock

from trader.data.market_data import resample_ticks_to_bars
from trader.objects import BarSize
from trader.strategy.live_bars import LiveBarBuffer, DailySessionBuckets
from trader.trading.strategy import StrategyState
from review.test_review_market_data_contract import _frame, _runtime, _ticker
from review.test_review_strategy_contract import runtime_and_strategy
from ib_async import Contract
from types import SimpleNamespace


def test_incremental_bars_match_batch_across_boundaries_and_volume_reset():
    times = pd.date_range('2026-09-08 13:30:10', periods=40, freq='10s', tz='UTC')
    ticks = _frame(times, prices=[100 + i % 5 for i in range(40)],
                   volumes=[1000 + i * 3 if i < 20 else (i - 20) * 3 for i in range(40)])
    buffer = LiveBarBuffer(ticks.iloc[:4], '1min')
    for n in range(4, len(ticks)):
        buffer.append(ticks.iloc[n:n + 1])
        expected = resample_ticks_to_bars(ticks.iloc[:n + 1], '1min')
        columns = ['open', 'high', 'low', 'close', 'volume']
        pd.testing.assert_frame_equal(buffer.completed[columns], expected[columns],
                                      check_dtype=False, check_freq=False)


def test_old_tick_cannot_rewrite_a_delivered_bar():
    ticks = _frame(pd.date_range('2026-09-08 13:30:10', periods=3, freq='min', tz='UTC'))
    buffer = LiveBarBuffer(ticks, '1min')
    before = buffer.completed.copy(deep=True)
    assert buffer.append(_frame([ticks.index[0]], prices=[99999])) is False
    pd.testing.assert_frame_equal(before, buffer.completed)


def test_daily_history_and_live_share_one_session_identity(tmp_path):
    runtime = _runtime(tmp_path)
    runtime._contracts = {1: Contract(conId=1, secType='STK', exchange='SMART', primaryExchange='NASDAQ')}
    runtime._hist_bars[(1, BarSize.Days1)] = _frame([
        pd.Timestamp('2026-09-08', tz='America/New_York').tz_convert('UTC')])
    runtime.streams[1] = _frame(pd.to_datetime(['2026-09-08T13:30Z', '2026-09-08T19:00Z',
                                               '2026-09-09T13:30Z']))
    frame = runtime._strategy_frame(1, BarSize.Days1)
    assert list(frame.index) == [pd.Timestamp('2026-09-08', tz='UTC')]


def test_asx_summer_daily_session_crosses_midnight_utc_without_splitting():
    ticks = _frame(pd.to_datetime(['2026-01-05T23:10Z', '2026-01-06T00:10Z',
                                  '2026-01-06T04:00Z', '2026-01-06T23:10Z']),
                   prices=[100, 110, 105, 120], volumes=[1000, 1010, 1020, 10])
    buffer = LiveBarBuffer(ticks, '1D', daily_sessions=DailySessionBuckets('ASX'))
    assert list(buffer.completed.index) == [pd.Timestamp('2026-01-06', tz='UTC')]
    bar = buffer.completed.iloc[0]
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (100, 110, 100, 105, 20)
    assert buffer.bucket == pd.Timestamp('2026-01-07', tz='UTC')


def test_daily_quote_outside_session_cannot_change_ohlcv():
    ticks = _frame(pd.to_datetime(['2026-09-08T13:30Z', '2026-09-08T19:00Z',
                                  '2026-09-09T02:00Z']),
                   prices=[100, 101, 999], volumes=[1000, 1010, 9999])
    buffer = LiveBarBuffer(ticks, '1D', daily_sessions=DailySessionBuckets('NASDAQ'))
    assert buffer.current['high'] == 101
    assert buffer.current['volume'] == 10


def test_weekend_gap_cannot_evict_the_bar_being_completed():
    ticks = _frame(pd.to_datetime(['2026-07-10T15:00Z', '2026-07-13T15:00Z']))
    buffer = LiveBarBuffer(None, '1D', retention_days=2,
                           daily_sessions=DailySessionBuckets('NASDAQ'))
    for n in range(len(ticks)):
        buffer.append(ticks.iloc[n:n + 1])
    assert list(buffer.completed.index) == [pd.Timestamp('2026-07-10', tz='UTC')]


def test_multiday_time_exit_counts_evicted_bars_across_executor_restart(tmp_path, monkeypatch):
    """Five observed daily bars remain five after a two-day cache eviction."""
    from review.test_review_strategy_contract import LifecycleSDK, make_work
    from trader.strategy.auto_executor import AutoExecutor, BarWork

    entry = pd.Timestamp('2026-07-06', tz='UTC')
    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self: entry.to_pydatetime())
    sdk = LifecycleSDK()
    path = str(tmp_path / 'held.duckdb')
    executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
    executor._process_signal(make_work(bar_ts=entry, bar_size_seconds=86400,
                                      quantity=4, max_hold_bars=5, close_by_time=None))
    executor.manage_positions()
    assert executor.open_entry_bar('orb_test', 1111) is not None
    runtime = _runtime(tmp_path)
    del runtime._check_time_exit  # exercise the actual management bridge
    runtime.auto_executor = executor
    strategy = SimpleNamespace(name='orb_test', _context=SimpleNamespace(params={}))
    ticks = _frame(pd.bdate_range(entry, periods=7) + pd.Timedelta(hours=15))
    buffer = LiveBarBuffer(None, '1D', retention_days=2,
                           daily_sessions=DailySessionBuckets('NASDAQ'))

    def drain_bar():
        work = runtime.auto_executor._queue.get(timeout=.1)
        assert isinstance(work, BarWork)
        runtime.auto_executor._process_bar(work)
        runtime.auto_executor.manage_positions()

    monkeypatch.setattr(executor, 'start', lambda: None)
    for n in range(len(ticks)):
        buffer.append(ticks.iloc[n:n + 1])
        if not buffer.completed.empty:
            runtime._dispatch_management_bar(strategy, 1111, buffer.completed)
            drain_bar()
        if n == 4:
            # The restarted executor must recover the three bars already seen;
            # its next observation window no longer contains those timestamps.
            executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
            executor.manage_positions()
            monkeypatch.setattr(executor, 'start', lambda: None)
            runtime.auto_executor = executor
        if n < 6:
            assert sdk.broker[1111] == 4

    assert len(buffer.completed) < 5
    assert sdk.broker[1111] == 0
    assert len(sdk.approve_calls) == 2
    assert executor.state.open_position('orb_test', 1111) is None


def test_refused_management_bar_is_retried_without_advancing_watermark(tmp_path):
    runtime = _runtime(tmp_path)
    del runtime._check_time_exit
    runtime.auto_executor = Mock()
    entry = pd.Timestamp('2026-07-06', tz='UTC')
    runtime.auto_executor.open_entry_bar.return_value = entry
    runtime.auto_executor.submit_bar.side_effect = [False, True]
    strategy = SimpleNamespace(name='held', _context=SimpleNamespace(params={}))
    frame = _frame(pd.date_range(entry + pd.Timedelta(days=1), periods=2, freq='D'))
    runtime._dispatch_management_bar(strategy, 1, frame)
    runtime._dispatch_management_bar(strategy, 1, frame)
    runtime._dispatch_management_bar(strategy, 1, frame)
    assert runtime.auto_executor.submit_bar.call_count == 2
    observed = runtime.auto_executor.submit_bar.call_args.kwargs['observed_bar_timestamps']
    assert list(observed) == list(frame.index)


def test_same_bar_ticks_do_not_repeat_history_resampling(tmp_path, monkeypatch):
    import trader.data.market_data as market_data
    runtime = _runtime(tmp_path)
    runtime._hist_bars[(1, BarSize.Mins1)] = pd.DataFrame()
    runtime.streams[1] = _frame(pd.date_range('2026-09-08 13:30:10', periods=3, freq='min', tz='UTC'))
    runtime.strategies[1] = [SimpleNamespace(name='test', state=StrategyState.RUNNING,
                                             bar_size=BarSize.Mins1, on_prices=Mock(return_value=None))]
    resample = Mock(wraps=market_data.resample_ticks_to_bars)
    monkeypatch.setattr(market_data, 'resample_ticks_to_bars', resample)
    contract = Contract(conId=1, secType='STK', exchange='SMART', primaryExchange='NASDAQ')
    for n in range(20):
        runtime.on_ticker_next(_ticker(contract, pd.Timestamp('2026-09-08 13:33:10', tz='UTC')
                                      + pd.Timedelta(seconds=n), 101, 1000 + n))
    assert resample.call_count == 1
    assert runtime.strategies[1][0].on_prices.call_count == 1


def test_history_invalidation_recovers_successfully_cached_empty_read(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path)
    history = _frame(pd.date_range('2026-09-08 13:30', periods=3, freq='min', tz='UTC'))
    read = Mock(side_effect=[pd.DataFrame(), history])
    monkeypatch.setattr('trader.data.duckdb_store.DuckDBDataStore.read', read)
    assert runtime._strategy_frame(1, BarSize.Mins1) is None
    runtime._invalidate_history(1, BarSize.Mins1)
    assert len(runtime._strategy_frame(1, BarSize.Mins1)) == 3


def test_reenable_rotates_authority_and_rejects_old_queued_signal(tmp_path):
    runtime, _ = runtime_and_strategy(tmp_path)
    strategy = runtime.get_strategy('review')
    strategy.ctx.auto_execute = True
    strategy.enable()
    old = strategy.ctx.deployment_generation
    assert runtime._opening_authorized('review', old)
    runtime.disable_strategy('review')
    assert not runtime._opening_authorized('review', old)
    runtime.enable_strategy('review')
    assert strategy.ctx.deployment_generation != old
    assert not runtime._opening_authorized('review', old)
    assert runtime._opening_authorized('review', strategy.ctx.deployment_generation)


def test_failed_replacement_revokes_old_generation_before_return(tmp_path):
    runtime, module = runtime_and_strategy(tmp_path)
    strategy = runtime.get_strategy('review')
    strategy.ctx.auto_execute = True
    strategy.enable()
    old = strategy.ctx.deployment_generation
    module.write_text('invalid python source!')
    config = dict(name='review', module=str(module), class_name='V1', bar_size='1 min', auto_execute=True)
    path = tmp_path / 'strategy_runtime.yaml'
    path.write_text(yaml.safe_dump({'strategies': [config]}))
    runtime.config_loader(str(path))
    assert not runtime._opening_authorized('review', old)
    assert runtime.get_strategy('review') is None
    assert strategy in runtime._retired_strategies


def test_source_edit_reloads_even_when_yaml_did_not_change(tmp_path):
    runtime, module = runtime_and_strategy(tmp_path)
    config = dict(name='review', module=str(module), class_name='V1', bar_size='1 min')
    path = tmp_path / 'strategy_runtime.yaml'
    path.write_text(yaml.safe_dump({'strategies': [config]}))
    runtime._reconcile_sync()
    before = runtime.get_strategy('review').ctx.deployment_generation
    module.write_text(module.read_text().replace('THRESHOLD = 20', 'THRESHOLD = 21'))
    runtime._reconcile_sync()
    after = runtime.get_strategy('review')
    assert after.THRESHOLD == 21
    assert after.ctx.deployment_generation != before


def test_unrecognized_uppercase_param_does_not_load_a_different_rule(tmp_path):
    runtime, module = runtime_and_strategy(tmp_path)
    runtime.load_strategy(name='typo', bar_size_str='1 min', conids=[], universe=None,
                          historical_days_prior=0, module=str(module), class_name='V1',
                          description='', params={'THRESHOLDD': 4})
    assert runtime.get_strategy('typo') is None


def test_native_open_recovers_session_and_interval_before_timer_exit(tmp_path, monkeypatch):
    """Actual entry payload -> durable fill -> restarted runtime exit policy.

    Only proposal allocation and the IB transport are adapters. Native Trades
    returned by the real coordinator are filled through the actual tracker;
    no acceptance dictionary or manually constructed opening payload is used.
    """
    import asyncio
    import datetime as dt
    import logging
    from review.test_review_order_contract import _coordinated_trader, _stock
    from review.test_review_strategy_contract import FakeResult
    from trader.strategy.auto_executor import AutoExecutor, BarWork, SignalWork
    from trader.trading.risk_gate import RiskGate, RiskLimits
    from trader.trading.strategy import Signal, Strategy, StrategyContext
    from trader.objects import Action

    owner, conid = 'recovered_sydney_entry', 100
    entry = pd.Timestamp('2026-07-06 15:00:00', tz='Australia/Sydney')
    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self: (entry + pd.Timedelta(seconds=10)).to_pydatetime())
    server = _coordinated_trader(tmp_path, held=100)  # independent manual inventory
    server.risk_gate = RiskGate(RiskLimits(), server.event_store)
    managers, proposals = [], []
    try:
        def propose(**kwargs):
            proposals.append(kwargs)
            return len(proposals), None, None

        def approve(proposal_id):
            proposal = proposals[proposal_id - 1]
            result = asyncio.run(server.place_expressive_order(
                _stock(), proposal['action'], proposal['quantity'],
                {'order_type': 'MARKET'}, algo_name=owner,
                client_intent_id=proposal['metadata']['client_intent_id']))
            ids = []
            if result.is_success():
                for trade in result.obj:
                    quantity = float(trade.order.totalQuantity)
                    trade.order.filledQuantity = quantity
                    trade.orderStatus.status = 'Filled'
                    trade.orderStatus.filled = quantity
                    trade.orderStatus.remaining = 0
                    trade.orderStatus.avgFillPrice = 10
                    server.inventory += quantity if trade.order.action == 'BUY' else -quantity
                    server.order_tracker.on_trade(trade)
                    ids.append(trade.order.orderId)
            return FakeResult(ok=result.is_success(), obj=ids, error=result.error)

        sdk = SimpleNamespace(
            execution_snapshot=lambda **kwargs: asyncio.run(server.execution_snapshot(**kwargs)),
            resolve=lambda symbol, **kwargs: [_stock()] if symbol in (conid, 'AUDIT') else [],
            propose=propose, approve=approve,
            _proposal_store=lambda: SimpleNamespace(get=lambda _pid: None))
        path = str(tmp_path / 'native_entry.duckdb')
        executor = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        managers.append(executor)
        monkeypatch.setattr(executor, 'start', lambda: None)
        runtime = _runtime(tmp_path)
        del runtime._check_time_exit
        runtime.storage = runtime.universe_accessor = None
        runtime.auto_executor = executor
        strategy = Strategy()
        strategy.install(StrategyContext(
            name=owner, bar_size=BarSize.Mins15, conids=[conid], universe=None,
            historical_days_prior=1, paper_only=False, storage=None,
            universe_accessor=None, logger=logging, auto_execute=True,
            params={'SESSION_TZ': 'Australia/Sydney'}))
        strategy.enable()
        runtime._submit_auto_execution(strategy, conid, Signal(
            source_name=owner, action=Action.BUY, probability=0.6, risk=0.4,
            quantity=4, close_by_time=dt.time(15, 30)), entry.tz_convert('UTC'))
        opening_work = executor._queue.get(timeout=.1)
        assert isinstance(opening_work, SignalWork)
        executor._process_signal(opening_work)
        assert server.inventory == 104
        assert executor.state.open_position(owner, conid)['quantity'] == 4
        executor.intents.journal.close()

        restarted = AutoExecutor(path, paper_trading=True, sdk_factory=lambda: sdk)
        managers.append(restarted)
        monkeypatch.setattr(restarted, 'start', lambda: None)
        runtime.auto_executor = restarted
        runtime._refresh_management_contexts()
        assert (owner, conid) in runtime._managed_contexts
        recovered = runtime._managed_contexts[(owner, conid)]
        assert recovered.bar_size == BarSize.Mins15
        assert recovered.state == StrategyState.DISABLED
        assert recovered.params['SESSION_TZ'] == 'Australia/Sydney'

        first = (entry + pd.Timedelta(minutes=15)).tz_convert('UTC')
        second = (entry + pd.Timedelta(minutes=30)).tz_convert('UTC')
        for observed, expected_inventory in (([first], 104), ([first, second], 100)):
            runtime._dispatch_management_bar(recovered, conid, _frame(observed))
            bar_work = restarted._queue.get(timeout=.1)
            assert isinstance(bar_work, BarWork)
            restarted._process_bar(bar_work)
            assert server.inventory == expected_inventory
        assert [trade.order.action for trade in server.placed] == ['BUY', 'SELL']
        assert [float(trade.order.totalQuantity) for trade in server.placed] == [4, 4]
        assert restarted.state.open_position(owner, conid) is None
    finally:
        try:
            for manager in managers:
                manager.intents.journal.close()
        finally:
            server.order_tracker.close(timeout=1)
            if server.order_tracker._journal is not None:
                server.order_tracker._journal.close()
            journal = getattr(server, '_server_order_journal', None)
            if journal is not None:
                journal.journal.close()
            if server.order_tracker._temporary is not None:
                server.order_tracker._temporary.cleanup()
