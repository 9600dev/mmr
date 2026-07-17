"""Tests for the G6 signal auto-executor (trader/strategy/auto_executor.py).

Covers the pure decision logic (long-only semantics matching the backtester),
time-exit triggering, persistent state (attribution + per-bar dedup), and the
full worker pipeline against a fake SDK — including the safety rails:
kill switch, paper_only, precision round-trip refusal, broker reconciliation,
and close-clamping to the live position.
"""
import datetime as dt
import logging
from types import SimpleNamespace

import pandas as pd
import pytest

from trader.objects import Action
from trader.strategy.auto_executor import (
    AutoExecState,
    AutoExecutor,
    AutoExecutionError,
    BarWork,
    SignalWork,
    check_time_exit,
    decide_signal,
)

TS = pd.Timestamp('2026-07-06 10:39:00')


def make_work(**kwargs) -> SignalWork:
    defaults = dict(
        strategy_name='orb_test', conid=1111, action=Action.BUY, bar_ts=TS,
        probability=0.6, risk=0.4, quantity=0.0,
        auto_execute=True, paper_only=False, state_running=True,
    )
    defaults.update(kwargs)
    return SignalWork(**defaults)


def decide(work, *, kill_switch=False, paper_trading=True, held_qty=0.0,
           already_executed_bar=False, cooldown_active=False):
    return decide_signal(
        work, kill_switch=kill_switch, paper_trading=paper_trading,
        held_qty=held_qty, already_executed_bar=already_executed_bar,
        cooldown_active=cooldown_active)


# ---------------------------------------------------------------------------
# decide_signal — pure long-only decision logic
# ---------------------------------------------------------------------------

class TestDecideSignal:
    def test_buy_while_flat_opens(self):
        d = decide(make_work())
        assert d.kind == 'open'
        assert d.quantity is None  # auto-size

    def test_buy_with_explicit_quantity_passes_through(self):
        d = decide(make_work(quantity=25.0))
        assert d.kind == 'open'
        assert d.quantity == 25.0

    def test_buy_while_holding_skips_no_pyramiding(self):
        d = decide(make_work(), held_qty=100.0)
        assert d.kind == 'skip'
        assert 'pyramiding' in d.reason

    def test_sell_while_holding_closes_attributed_qty(self):
        d = decide(make_work(action=Action.SELL), held_qty=140.0)
        assert d.kind == 'close'
        assert d.quantity == 140.0

    def test_sell_while_flat_is_noop(self):
        d = decide(make_work(action=Action.SELL))
        assert d.kind == 'skip'
        assert 'long-only' in d.reason

    def test_kill_switch_blocks_everything(self):
        d = decide(make_work(), kill_switch=True)
        assert d.kind == 'skip'
        assert 'kill switch' in d.reason

    def test_auto_execute_false_skips(self):
        d = decide(make_work(auto_execute=False))
        assert d.kind == 'skip'

    def test_not_running_skips(self):
        d = decide(make_work(state_running=False))
        assert d.kind == 'skip'

    def test_paper_only_strategy_refuses_live_mode(self):
        d = decide(make_work(paper_only=True), paper_trading=False)
        assert d.kind == 'skip'
        assert 'paper_only' in d.reason

    def test_paper_only_strategy_trades_in_paper_mode(self):
        d = decide(make_work(paper_only=True), paper_trading=True)
        assert d.kind == 'open'

    def test_bar_dedup_blocks_reexecution(self):
        d = decide(make_work(), already_executed_bar=True)
        assert d.kind == 'skip'
        assert 'already executed' in d.reason

    def test_cooldown_blocks_open(self):
        d = decide(make_work(), cooldown_active=True)
        assert d.kind == 'skip'
        assert 'cooldown' in d.reason

    def test_cooldown_never_blocks_close(self):
        d = decide(make_work(action=Action.SELL), held_qty=50.0, cooldown_active=True)
        assert d.kind == 'close'


# ---------------------------------------------------------------------------
# check_time_exit — mirrors backtester bar-timestamp semantics
# ---------------------------------------------------------------------------

class TestCheckTimeExit:
    def test_no_conditions_never_triggers(self):
        assert check_time_exit(TS, 999, close_by_time=None, max_hold_bars=None) is None

    def test_max_hold_bars_triggers_at_threshold(self):
        assert check_time_exit(TS, 19, close_by_time=None, max_hold_bars=20) is None
        assert check_time_exit(TS, 20, close_by_time=None, max_hold_bars=20) == 'max_hold_bars=20'
        assert check_time_exit(TS, 21, close_by_time=None, max_hold_bars=20) == 'max_hold_bars=20'

    def test_close_by_time_uses_bar_time_of_day(self):
        cbt = dt.time(15, 45)
        before = pd.Timestamp('2026-07-06 15:44:00')
        at = pd.Timestamp('2026-07-06 15:45:00')
        after = pd.Timestamp('2026-07-06 15:46:00')
        assert check_time_exit(before, 0, close_by_time=cbt, max_hold_bars=None) is None
        assert check_time_exit(at, 0, close_by_time=cbt, max_hold_bars=None) == f'close_by_time={cbt}'
        assert check_time_exit(after, 0, close_by_time=cbt, max_hold_bars=None) == f'close_by_time={cbt}'

    def test_max_hold_bars_wins_when_both_trigger(self):
        reason = check_time_exit(
            pd.Timestamp('2026-07-06 16:00:00'), 30,
            close_by_time=dt.time(15, 45), max_hold_bars=20)
        assert reason == 'max_hold_bars=20'


# ---------------------------------------------------------------------------
# AutoExecState — persistence
# ---------------------------------------------------------------------------

@pytest.fixture
def state(tmp_path):
    return AutoExecState(str(tmp_path / 'auto_exec_test.duckdb'))


class TestAutoExecState:
    def test_open_position_roundtrip(self, state):
        assert state.open_position('s1', 1) is None
        state.record_open('s1', 1, 120.0, TS, proposal_id=7,
                          close_by_time=dt.time(15, 45), max_hold_bars=60)
        pos = state.open_position('s1', 1)
        assert pos['quantity'] == 120.0
        assert pos['close_by_time'] == dt.time(15, 45)
        assert pos['max_hold_bars'] == 60
        assert pos['proposal_id'] == 7

    def test_close_removes_from_open(self, state):
        state.record_open('s1', 1, 120.0, TS, 7, None, None)
        state.record_close('s1', 1, 'CLOSED', 'SELL signal', close_proposal_id=8)
        assert state.open_position('s1', 1) is None
        assert state.all_open() == []

    def test_positions_are_keyed_per_strategy_and_conid(self, state):
        state.record_open('s1', 1, 10.0, TS, 1, None, None)
        state.record_open('s2', 1, 20.0, TS, 2, None, None)
        state.record_open('s1', 2, 30.0, TS, 3, None, None)
        assert state.open_position('s1', 1)['quantity'] == 10.0
        assert state.open_position('s2', 1)['quantity'] == 20.0
        assert state.open_position('s1', 2)['quantity'] == 30.0
        state.record_close('s1', 1, 'CLOSED', 'x')
        assert state.open_position('s1', 1) is None
        assert state.open_position('s2', 1) is not None

    def test_bar_dedup_counts_only_executions(self, state):
        state.log_decision('s1', 1, TS, 'BUY', 'skip', 'cooldown')
        assert not state.executed_for_bar('s1', 1, TS)
        state.log_decision('s1', 1, TS, 'BUY', 'open', 'proposal #1')
        assert state.executed_for_bar('s1', 1, TS)
        assert not state.executed_for_bar('s1', 1, TS + pd.Timedelta(minutes=1))
        assert not state.executed_for_bar('s2', 1, TS)

    def test_tz_aware_bar_ts_dedups_against_naive(self, state):
        aware = pd.Timestamp('2026-07-06 10:39:00', tz='Australia/Sydney')
        state.log_decision('s1', 1, aware, 'BUY', 'open', 'x')
        assert state.executed_for_bar('s1', 1, aware)
        assert state.executed_for_bar('s1', 1, aware.tz_localize(None))


# ---------------------------------------------------------------------------
# AutoExecutor worker pipeline — fake SDK, direct (synchronous) processing
# ---------------------------------------------------------------------------

class FakeResult:
    def __init__(self, ok=True, obj=None, error=None):
        self._ok = ok
        self.obj = obj or []
        self.error = error
    def is_success(self):
        return self._ok


class FakeSDK:
    """Records propose/approve calls; resolves conid 1111 <-> 'WDS' exactly."""

    def __init__(self):
        self.secdef = SimpleNamespace(
            symbol='WDS', exchange='ASX', primaryExchange='ASX',
            currency='AUD', secType='STK', conId=1111)
        self.proposals = {}
        self.approve_results = {}
        self.next_id = 100
        self.broker = {}          # conid -> qty
        self.propose_calls = []
        self.approve_calls = []
        self.fill_qty = 140.0     # totalQuantity assigned to placed orders

    def resolve(self, symbol, sec_type='STK', exchange='', universe='', currency=''):
        if symbol == 1111 or symbol == 'WDS':
            return [self.secdef]
        return []

    def propose(self, **kwargs):
        pid = self.next_id
        self.next_id += 1
        self.propose_calls.append(kwargs)
        self.proposals[pid] = SimpleNamespace(
            quantity=kwargs.get('quantity'), metadata=kwargs.get('metadata') or {})
        return pid, None, None

    def approve(self, pid):
        self.approve_calls.append(pid)
        result = self.approve_results.get(pid, FakeResult(ok=True, obj=[pid * 10]))
        if result.is_success():
            p = self.proposals[pid]
            qty = p.quantity if p.quantity else self.fill_qty
            action = self.propose_calls[-1]['action']
            cur = self.broker.get(1111, 0.0)
            self.broker[1111] = cur + qty if action == 'BUY' else cur - qty
        return result

    def positions(self):
        rows = [{'conId': c, 'position': q} for c, q in self.broker.items() if q != 0]
        return pd.DataFrame(rows)

    def trades(self):
        rows = [{'orderId': pid * 10, 'totalQuantity':
                 (self.proposals[pid].quantity or self.fill_qty)}
                for pid in self.approve_calls]
        return pd.DataFrame(rows)

    def _proposal_store(self):
        proposals = self.proposals
        class _S:
            def get(self, pid):
                return proposals.get(pid)
        return _S()


@pytest.fixture
def executor(tmp_path):
    sdk = FakeSDK()
    ex = AutoExecutor(
        duckdb_path=str(tmp_path / 'exec_test.duckdb'),
        paper_trading=True,
        cooldown_seconds=300.0,
        sdk_factory=lambda: sdk,
    )
    ex._reconciled = True  # tests drive reconcile explicitly
    return ex, sdk


class TestAutoExecutorPipeline:
    def test_buy_signal_opens_position(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())
        assert len(sdk.propose_calls) == 1
        call = sdk.propose_calls[0]
        assert call['action'] == 'BUY'
        assert call['symbol'] == 'WDS'
        assert call['exchange'] == 'ASX'
        assert call['currency'] == 'AUD'
        assert call['quantity'] is None          # auto-sized
        assert call['source'] == 'strategy:orb_test'
        assert call['metadata']['auto_executed'] is True
        pos = ex.state.open_position('orb_test', 1111)
        assert pos['quantity'] == 140.0          # from order totalQuantity
        assert ex.open_entry_bar('orb_test', 1111) is not None

    def test_sell_signal_closes_attributed_quantity(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())
        ex._process_signal(make_work(
            action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=30)))
        assert len(sdk.propose_calls) == 2
        close = sdk.propose_calls[1]
        assert close['action'] == 'SELL'
        assert close['quantity'] == 140.0
        assert ex.state.open_position('orb_test', 1111) is None
        assert ex.open_entry_bar('orb_test', 1111) is None

    def test_close_clamps_to_live_broker_position(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())
        sdk.broker[1111] = 90.0  # 50 shares sold manually out from under us
        ex._process_signal(make_work(
            action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=30)))
        assert sdk.propose_calls[1]['quantity'] == 90.0

    def test_close_when_broker_flat_marks_external(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())
        sdk.broker[1111] = 0.0
        ex._process_signal(make_work(
            action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=30)))
        assert len(sdk.propose_calls) == 1  # no close order placed
        assert ex.state.open_position('orb_test', 1111) is None

    def test_same_bar_signal_executes_once(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())
        ex._process_signal(make_work())  # duplicate bar
        assert len(sdk.propose_calls) == 1

    def test_second_buy_next_bar_skips_while_holding(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())
        ex._process_signal(make_work(bar_ts=TS + pd.Timedelta(minutes=1)))
        assert len(sdk.propose_calls) == 1

    def test_sell_while_flat_places_nothing(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work(action=Action.SELL))
        assert sdk.propose_calls == []

    def test_failed_approve_leaves_no_attributed_position(self, executor):
        ex, sdk = executor
        sdk.approve_results[100] = FakeResult(
            ok=False, error='risk gate rejected: max position size')
        ex._process_signal(make_work())
        assert ex.state.open_position('orb_test', 1111) is None
        # A later signal on a new bar may retry
        ex._process_signal(make_work(bar_ts=TS + pd.Timedelta(minutes=5)))
        assert len(sdk.propose_calls) == 2

    def test_stale_conid_refuses_to_trade(self, executor, caplog):
        ex, sdk = executor
        with caplog.at_level(logging.ERROR):
            ex._process_signal(make_work(conid=9999))
        assert sdk.propose_calls == []
        assert 'refused' in caplog.text  # deliberate refusal, not a crash

    def test_precision_roundtrip_mismatch_refuses(self, executor):
        ex, sdk = executor
        sdk.secdef = SimpleNamespace(
            symbol='WDS', exchange='ASX', primaryExchange='ASX',
            currency='AUD', secType='STK', conId=2222)  # round-trips to wrong id
        with pytest.raises(AutoExecutionError):
            ex._resolve_exact(1111)

    def test_kill_switch_env_blocks(self, executor, monkeypatch):
        ex, sdk = executor
        monkeypatch.setenv(AutoExecutor.KILL_SWITCH_ENV, '1')
        ex._process_signal(make_work())
        assert sdk.propose_calls == []

    def test_time_exit_close_by_time(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work(close_by_time=dt.time(15, 45)))
        assert ex.state.open_position('orb_test', 1111) is not None
        # Bar before the cutoff: no close
        ex._process_bar(BarWork('orb_test', 1111,
                                pd.Timestamp('2026-07-06 15:30:00'), 10))
        assert len(sdk.propose_calls) == 1
        # Bar at the cutoff: close fires
        ex._process_bar(BarWork('orb_test', 1111,
                                pd.Timestamp('2026-07-06 15:45:00'), 11))
        assert len(sdk.propose_calls) == 2
        assert sdk.propose_calls[1]['action'] == 'SELL'
        assert 'time exit' in sdk.propose_calls[1]['reasoning']
        assert ex.state.open_position('orb_test', 1111) is None

    def test_time_exit_max_hold_bars(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work(max_hold_bars=20))
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=19), 19))
        assert len(sdk.propose_calls) == 1
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=20), 20))
        assert len(sdk.propose_calls) == 2
        assert ex.state.open_position('orb_test', 1111) is None

    def test_time_exit_same_bar_fires_once(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work(max_hold_bars=5))
        bar = BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=6), 6)
        ex._process_bar(bar)
        ex._process_bar(bar)
        assert len(sdk.propose_calls) == 2  # one open + one close

    def test_time_exit_noop_without_open_position(self, executor):
        ex, sdk = executor
        ex._process_bar(BarWork('orb_test', 1111, TS, 100))
        assert sdk.propose_calls == []

    def test_reconcile_marks_externally_closed(self, tmp_path):
        sdk = FakeSDK()
        db = str(tmp_path / 'reconcile_test.duckdb')
        pre = AutoExecState(db)
        pre.record_open('orb_test', 1111, 140.0, TS, 1, None, None)   # stale: broker is flat
        ex = AutoExecutor(duckdb_path=db, paper_trading=True,
                          sdk_factory=lambda: sdk)
        assert ex.open_entry_bar('orb_test', 1111) is not None  # loaded from disk
        ex._reconcile_once()
        assert ex.state.open_position('orb_test', 1111) is None
        assert ex.open_entry_bar('orb_test', 1111) is None

    def test_reconcile_keeps_positions_broker_confirms(self, tmp_path):
        sdk = FakeSDK()
        sdk.broker[1111] = 140.0
        db = str(tmp_path / 'reconcile_keep.duckdb')
        pre = AutoExecState(db)
        pre.record_open('orb_test', 1111, 140.0, TS, 1, None, None)
        ex = AutoExecutor(duckdb_path=db, paper_trading=True,
                          sdk_factory=lambda: sdk)
        ex._reconcile_once()
        assert ex.state.open_position('orb_test', 1111) is not None

    def test_restart_recovers_open_position_and_time_exit(self, tmp_path):
        """Full restart survival: open in one executor, time-exit in a fresh one."""
        sdk = FakeSDK()
        db = str(tmp_path / 'restart_test.duckdb')
        ex1 = AutoExecutor(duckdb_path=db, paper_trading=True,
                           sdk_factory=lambda: sdk)
        ex1._reconciled = True
        ex1._process_signal(make_work(close_by_time=dt.time(15, 45)))
        assert sdk.broker[1111] == 140.0

        ex2 = AutoExecutor(duckdb_path=db, paper_trading=True,
                           sdk_factory=lambda: sdk)
        ex2._reconciled = True
        assert ex2.open_entry_bar('orb_test', 1111) is not None
        ex2._process_bar(BarWork('orb_test', 1111,
                                 pd.Timestamp('2026-07-06 15:45:00'), 300))
        assert sdk.propose_calls[-1]['action'] == 'SELL'
        assert ex2.state.open_position('orb_test', 1111) is None


class TestWorkerThread:
    def test_worker_processes_queue(self, tmp_path):
        import time
        sdk = FakeSDK()
        ex = AutoExecutor(duckdb_path=str(tmp_path / 'worker_test.duckdb'),
                          paper_trading=True, sdk_factory=lambda: sdk)
        ex.submit_signal(make_work())
        deadline = time.time() + 10
        while time.time() < deadline:
            if ex.state.open_position('orb_test', 1111) is not None:
                break
            time.sleep(0.05)
        ex.stop()
        assert ex.state.open_position('orb_test', 1111) is not None
        assert len(sdk.propose_calls) == 1

    def test_worker_survives_processing_errors(self, tmp_path):
        import time
        calls = []
        def broken_factory():
            calls.append(1)
            raise RuntimeError('sdk unavailable')
        ex = AutoExecutor(duckdb_path=str(tmp_path / 'worker_err.duckdb'),
                          paper_trading=True, sdk_factory=broken_factory)
        ex._reconciled = True
        ex.submit_signal(make_work())
        ex.submit_signal(make_work(bar_ts=TS + pd.Timedelta(minutes=1)))
        deadline = time.time() + 10
        while time.time() < deadline and len(calls) < 2:
            time.sleep(0.05)
        ex.stop()
        assert len(calls) == 2  # second item still processed after first blew up
