"""Exit policy for holdings without a recorded interval/session (review 2026-09-11).

Independent management rebuilt a strategy context from the latest OPEN intent
payload and DEFAULTED ``bar_size_seconds=60`` / ``session_tz='UTC'`` when the
payload had none. Every position opened by the previous build has no payload,
so a daily strategy's ``max_hold_bars=5`` fired after five minutes and a
``close_by_time=15:45`` fired at 15:45 UTC, the exact bug fixed on 2026-07-20.
The runtime now uses the loaded strategy's real interval and session, and
defers (with one error line) when the strategy is not loaded either.
"""
import logging
from types import SimpleNamespace

import pandas as pd

from review.test_review_market_data_contract import _runtime
from trader.objects import BarSize
from trader.trading.strategy import Strategy, StrategyContext, StrategyState

NAME, CONID = 'legacy_daily', 5


def _record(**overrides):
    entry = pd.Timestamp('2026-09-01', tz='UTC')
    record = dict(strategy_name=NAME, conid=CONID, quantity=3.0, proposal_id=1,
                  ownership_epoch=None, ownership_started_at=None, protective_order_id=None,
                  entry_bar=entry, entry_bar_ts=entry, bar_size_seconds=None, session_tz=None,
                  close_by_time=None, max_hold_bars=5, fill_checkpoints={})
    record.update(overrides)
    return record


def _loaded(bar_size, tz):
    strategy = Strategy()
    strategy.install(StrategyContext(
        name=NAME, bar_size=bar_size, conids=[CONID], universe=None, historical_days_prior=1,
        paper_only=False, storage=None, universe_accessor=None, logger=logging,
        params={'SESSION_TZ': tz}))
    return strategy


def _managed_runtime(tmp_path, records, loaded=None):
    runtime = _runtime(tmp_path)
    runtime.storage = runtime.universe_accessor = None
    runtime._managed_contexts = {}
    runtime._invalidate_history = lambda *args, **kwargs: None
    runtime.strategy_implementations = [loaded] if loaded is not None else []
    runtime.auto_executor = SimpleNamespace(managed_positions=lambda: records)
    return runtime


def test_policy_without_recorded_interval_uses_the_loaded_strategy(tmp_path):
    runtime = _managed_runtime(tmp_path, [_record()], _loaded(BarSize.Days1, 'Australia/Sydney'))

    runtime._refresh_management_contexts()

    context = runtime._managed_contexts[(NAME, CONID)]
    assert context.bar_size == BarSize.Days1, 'five daily bars, not five minutes'
    assert context.params['SESSION_TZ'] == 'Australia/Sydney'
    assert context.state == StrategyState.DISABLED


def test_policy_without_recorded_interval_and_no_loaded_strategy_is_deferred(tmp_path, caplog):
    runtime = _managed_runtime(tmp_path, [_record()])

    with caplog.at_level(logging.ERROR):
        runtime._refresh_management_contexts()
        runtime._refresh_management_contexts()

    assert runtime._managed_contexts == {}, 'no fabricated one-minute UTC policy'
    deferred = [r for r in caplog.records if 'time exits are deferred' in r.getMessage()]
    assert len(deferred) == 1, 'logged once, not once per second'


def test_recorded_policy_is_used_as_recorded(tmp_path):
    misleading = _loaded(BarSize.Days1, 'Australia/Sydney')  # must NOT be consulted
    runtime = _managed_runtime(
        tmp_path, [_record(bar_size_seconds=900.0, session_tz='America/New_York')], misleading)

    runtime._refresh_management_contexts()

    context = runtime._managed_contexts[(NAME, CONID)]
    assert context.bar_size == BarSize.Mins15
    assert context.params['SESSION_TZ'] == 'America/New_York'
