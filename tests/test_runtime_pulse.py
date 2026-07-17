"""Tests for the heartbeat pulse / runtime_status snapshot (Cluster A1/A3).

Covers the pure builders that feed the strategy_service pulse line and the
``runtime_status`` RPC, plus `mmr verify`'s roster assertion. These are the
pieces the LLM monitoring loop keys off — ``ticks_60s`` going to zero during
market hours is the escalate condition, so the counting/timezone math must
be exact.
"""
from types import SimpleNamespace

import pandas as pd
import pytest

from trader.strategy.strategy_runtime import (
    _age_seconds,
    _count_recent,
    build_runtime_status,
    format_pulse,
)
from trader.trading.strategy import StrategyState

NOW_UTC = pd.Timestamp('2026-07-16 15:00:00', tz='UTC')


def _strategy(name, state=StrategyState.RUNNING, auto_execute=True):
    return SimpleNamespace(
        name=name,
        state=state,
        _context=SimpleNamespace(auto_execute=auto_execute),
    )


def _stream(timestamps, tz='UTC'):
    idx = pd.DatetimeIndex(pd.to_datetime(timestamps))
    if tz is not None:
        idx = idx.tz_localize(tz)
    return pd.DataFrame({'close': range(len(idx))}, index=idx)


class TestCountRecent:
    def test_counts_only_window(self):
        df = _stream([
            '2026-07-16 14:58:00',   # 120s old — outside
            '2026-07-16 14:59:30',   # 30s old — inside
            '2026-07-16 14:59:59',   # 1s old — inside
        ])
        assert _count_recent(df.index, NOW_UTC, 60) == 2

    def test_naive_index_treated_as_utc(self):
        # _strategy_frame's convention: naive timestamps are UTC. A naive
        # index must NOT be compared against local wall clock (7h off on a
        # PDT host would zero the count during market hours).
        df = _stream(['2026-07-16 14:59:30'], tz=None)
        assert _count_recent(df.index, NOW_UTC, 60) == 1

    def test_empty_and_none(self):
        assert _count_recent(None, NOW_UTC, 60) == 0
        assert _count_recent(pd.DatetimeIndex([]), NOW_UTC, 60) == 0

    def test_all_stale(self):
        df = _stream(['2026-07-16 13:00:00'])
        assert _count_recent(df.index, NOW_UTC, 60) == 0


class TestAgeSeconds:
    def test_aware(self):
        assert _age_seconds(pd.Timestamp('2026-07-16 14:59:00', tz='UTC'), NOW_UTC) == 60

    def test_naive_treated_as_utc(self):
        assert _age_seconds(pd.Timestamp('2026-07-16 14:58:00'), NOW_UTC) == 120

    def test_garbage_returns_none(self):
        assert _age_seconds('not a timestamp', NOW_UTC) is None


class TestBuildRuntimeStatus:
    def test_full_snapshot(self):
        strategies = [
            _strategy('orb_googl'),
            _strategy('orb_bhp', state=StrategyState.DISABLED),
            _strategy('vwap_cat', auto_execute=False),
        ]
        streams = {
            208813719: _stream(['2026-07-16 14:59:30', '2026-07-16 14:59:45']),
            4036812: _stream(['2026-07-16 13:00:00']),          # stale
        }
        last_bar = {
            (208813719, 'orb_googl'): pd.Timestamp('2026-07-16 14:59:00', tz='UTC'),
            (208813719, 'vwap_cat'): pd.Timestamp('2026-07-16 14:55:00', tz='UTC'),
        }
        status = build_runtime_status(NOW_UTC, strategies, streams, last_bar, auto_exec_open=2)

        assert status['strategies_running'] == 2
        assert status['strategies_total'] == 3
        assert status['strategies']['orb_googl']['state_name'] == 'RUNNING'
        assert status['strategies']['orb_googl']['auto_execute'] is True
        assert status['strategies']['orb_bhp']['state_name'] == 'DISABLED'
        assert status['strategies']['vwap_cat']['auto_execute'] is False
        assert status['ticks_60s'] == {208813719: 2, 4036812: 0}
        # min age across strategies on the same conid: 60s (orb_googl) wins
        assert status['bar_age_s'] == {208813719: 60}
        assert status['auto_exec_open'] == 2

    def test_empty_runtime(self):
        status = build_runtime_status(NOW_UTC, [], {}, {}, auto_exec_open=0)
        assert status['strategies_running'] == 0
        assert status['strategies_total'] == 0
        assert status['ticks_60s'] == {}
        assert status['bar_age_s'] == {}

    def test_bogus_bar_ts_skipped(self):
        status = build_runtime_status(
            NOW_UTC, [], {}, {(123, 's'): 'garbage'}, auto_exec_open=0)
        assert status['bar_age_s'] == {}

    def test_strategy_without_context(self):
        s = SimpleNamespace(name='bare', state=StrategyState.RUNNING, _context=None)
        status = build_runtime_status(NOW_UTC, [s], {}, {}, auto_exec_open=0)
        assert status['strategies']['bare']['auto_execute'] is False


class TestFormatPulse:
    def test_greppable_line(self):
        status = build_runtime_status(
            NOW_UTC,
            [_strategy('orb_googl')],
            {208813719: _stream(['2026-07-16 14:59:30'])},
            {(208813719, 'orb_googl'): pd.Timestamp('2026-07-16 14:59:00', tz='UTC')},
            auto_exec_open=1,
        )
        line = format_pulse(status)
        assert line.startswith('pulse ')
        assert 'strategies=1/1' in line
        assert 'ticks_60s=[208813719:1]' in line
        assert 'bar_age_s=[208813719:60]' in line
        assert 'auto_exec_open=1' in line

    def test_empty_pulse_never_raises(self):
        assert format_pulse({}) == 'pulse strategies=0/0 ticks_60s=[] bar_age_s=[] auto_exec_open=0'


class TestVerifyRosterCheck:
    def _runtime(self, states):
        return {
            'strategies': {n: {'state_name': s} for n, s in states.items()},
            'strategies_running': sum(1 for s in states.values() if s == 'RUNNING'),
        }

    def test_all_running_pass(self):
        from trader.mmr_cli import _verify_roster_check
        status, detail = _verify_roster_check(
            ['a', 'b'], self._runtime({'a': 'RUNNING', 'b': 'RUNNING'}))
        assert status == 'PASS'

    def test_some_stopped_warn_not_fail(self):
        # enabled-state persists in the DB, so a deliberately disabled
        # strategy still has auto_execute: true in the YAML — that must not
        # fail the whole verify.
        from trader.mmr_cli import _verify_roster_check
        status, detail = _verify_roster_check(
            ['a', 'b'], self._runtime({'a': 'RUNNING', 'b': 'DISABLED'}))
        assert status == 'WARN'
        assert 'b' in detail

    def test_missing_strategy_warn(self):
        from trader.mmr_cli import _verify_roster_check
        status, detail = _verify_roster_check(
            ['a', 'ghost'], self._runtime({'a': 'RUNNING'}))
        assert status == 'WARN'
        assert 'ghost' in detail

    def test_none_running_fail(self):
        from trader.mmr_cli import _verify_roster_check
        status, detail = _verify_roster_check(
            ['a', 'b'], self._runtime({'a': 'ERROR', 'b': 'DISABLED'}))
        assert status == 'FAIL'

    def test_expect_running_mismatch_fail(self):
        from trader.mmr_cli import _verify_roster_check
        status, detail = _verify_roster_check(
            ['a'], self._runtime({'a': 'RUNNING'}), expect_running=5)
        assert status == 'FAIL'
        assert 'expected 5' in detail


class TestAutoExecutorOpenCount:
    def test_open_count_tracks_view(self, tmp_path):
        from trader.strategy.auto_executor import AutoExecutor
        db = str(tmp_path / 'pulse_test.duckdb')
        ex = AutoExecutor(duckdb_path=db, paper_trading=True)
        assert ex.open_count() == 0
        ex.state.record_open('s1', 123, 10.0, pd.Timestamp('2026-07-16 14:00:00'),
                             proposal_id=1, close_by_time=None, max_hold_bars=None)
        ex._load_open_view()
        assert ex.open_count() == 1
        ex.state.record_close('s1', 123, 'CLOSED', 'test')
        ex._load_open_view()
        assert ex.open_count() == 0
