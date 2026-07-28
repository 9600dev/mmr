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
    Directive,
    SignalWork,
    check_manifest,
    check_time_exit,
    decide_signal,
)

# A FRESH bar. These tests drive the OPEN path, and the stale-bar gate refuses
# an open whose bar is older than 3x the bar size. The hardcoded past date this
# replaced only ever passed because an unset `bar_size_seconds` disabled the
# gate, so the suite was exercising opens under conditions that cannot occur
# live. Naive here means UTC, which is how bar_age_seconds reads it.
TS = pd.Timestamp.now(tz='UTC').tz_localize(None).floor('min')


def make_work(**kwargs) -> SignalWork:
    defaults = dict(
        strategy_name='orb_test', conid=1111, action=Action.BUY, bar_ts=TS,
        bar_size_seconds=60.0,
        probability=0.6, risk=0.4, quantity=0.0,
        auto_execute=True, paper_only=False, state_running=True,
    )
    defaults.update(kwargs)
    return SignalWork(**defaults)


# A healthy bar age. Leaving this unset used to mean "undatable", which the
# stale-bar gate silently let through; it now refuses. Tests that are not
# about bar freshness should present a NORMAL bar, not an unreadable one.
def decide(work, *, kill_switch=False, paper_trading=True, held_qty=0.0,
           already_executed_bar=False, cooldown_active=False,
           bar_age_seconds=10.0):
    return decide_signal(
        work, kill_switch=kill_switch, paper_trading=paper_trading,
        held_qty=held_qty, already_executed_bar=already_executed_bar,
        cooldown_active=cooldown_active, bar_age_seconds=bar_age_seconds)


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
        self.avg_cost = 100.0
        self.protective_calls = []
        self.cancel_calls = []
        self.next_protective_id = 900
        self.protective_result = None   # override to fail placement
        self.open_orders = []           # rows for the own-protective scan

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
        rows = [{'conId': c, 'position': q, 'avgCost': self.avg_cost}
                for c, q in self.broker.items() if q != 0]
        return pd.DataFrame(rows)

    def place_protective_order(self, **kwargs):
        self.protective_calls.append(kwargs)
        if self.protective_result is not None:
            return self.protective_result
        oid = self.next_protective_id
        self.next_protective_id += 1
        return FakeResult(ok=True, obj=SimpleNamespace(order=SimpleNamespace(orderId=oid)))

    def cancel(self, order_id):
        self.cancel_calls.append(order_id)
        return FakeResult(ok=True)

    def trades(self):
        rows = [{'orderId': pid * 10, 'orderRef': '', 'conId': 0,
                 'status': 'Filled', 'action': 'BUY',
                 'totalQuantity': (self.proposals[pid].quantity or self.fill_qty)}
                for pid in self.approve_calls]
        # open_orders: rows the _own_live_protectives scan sees (orderRef-owned
        # live stops, possibly created externally e.g. by a resize re-create).
        return pd.DataFrame(rows + list(self.open_orders))

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

    def test_close_with_an_unreadable_broker_position_places_no_order(self, executor):
        """A NaN broker quantity defeated the inline min()/<=0 pair: min keeps
        140.0 and nan <= 0 is False, so the executor would have sold the full
        attributed size against a position it could not read. Exit-class orders
        are exempt from every gate, so nothing downstream would have stopped
        it."""
        ex, sdk = executor
        ex._process_signal(make_work())
        sdk.broker[1111] = float('nan')
        ex._process_signal(make_work(
            action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=30)))
        assert len(sdk.propose_calls) == 1  # the open only; no close placed
        assert ex.state.open_position('orb_test', 1111) is None

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
        sdk.broker[2222] = 10.0   # the book is readable; OUR conid is what's absent
        ex = AutoExecutor(duckdb_path=db, paper_trading=True,
                          sdk_factory=lambda: sdk)
        assert ex.open_entry_bar('orb_test', 1111) is not None  # loaded from disk
        ex._reconcile_once()
        assert ex.state.open_position('orb_test', 1111) is None
        assert ex.open_entry_bar('orb_test', 1111) is None

    def test_reconcile_does_not_believe_a_first_empty_broker_read(self, tmp_path):
        """The startup race. get_positions falls back to MMR's portfolio cache
        when ib.positions() is empty, and both are empty for a moment after
        trader_service connects — which is when the first bars arrive and the
        executor reconciles. Believing that read strips attribution AND cancels
        the protective stop on positions that are really there, permanently."""
        sdk = FakeSDK()
        db = str(tmp_path / 'reconcile_empty.duckdb')
        pre = AutoExecState(db)
        pre.record_open('orb_test', 1111, 140.0, TS, 1, None, None)
        pre.set_protective('orb_test', 1111, 942)
        ex = AutoExecutor(duckdb_path=db, paper_trading=True, sdk_factory=lambda: sdk)
        ex._reconcile_once()                       # broker dict is empty
        assert ex.state.open_position('orb_test', 1111) is not None
        assert sdk.cancel_calls == []
        assert ex._reconciled is False             # will retry

    def test_reconcile_believes_a_persistently_empty_broker(self, tmp_path):
        """...but it must still converge. A book that really is flat (everything
        closed while the service was down) has to reconcile eventually — a stale
        attribution blocks new opens, so refusing forever trades one silent
        failure for another."""
        sdk = FakeSDK()
        db = str(tmp_path / 'reconcile_empty2.duckdb')
        pre = AutoExecState(db)
        pre.record_open('orb_test', 1111, 140.0, TS, 1, None, None)
        ex = AutoExecutor(duckdb_path=db, paper_trading=True, sdk_factory=lambda: sdk)
        ex._reconcile_once()                       # first empty read: inconclusive
        assert ex.state.open_position('orb_test', 1111) is not None
        # ...the grace period elapses (backdated rather than slept)
        ex._first_empty_broker_read -= ex.empty_broker_grace_seconds + 1
        ex._reconcile_once()
        assert ex.state.open_position('orb_test', 1111) is None
        assert ex._reconciled is True

    def test_a_readable_book_resets_the_empty_read_clock(self, tmp_path):
        """Two empty reads separated by a good one are not evidence of a flat
        book; they are two separate blips."""
        sdk = FakeSDK()
        db = str(tmp_path / 'reconcile_empty3.duckdb')
        pre = AutoExecState(db)
        pre.record_open('orb_test', 1111, 140.0, TS, 1, None, None)
        ex = AutoExecutor(duckdb_path=db, paper_trading=True, sdk_factory=lambda: sdk)
        ex._reconcile_once()
        assert ex._first_empty_broker_read is not None
        sdk.broker[1111] = 140.0                   # the feed comes good
        ex._reconcile_once()
        assert ex._first_empty_broker_read is None
        assert ex.state.open_position('orb_test', 1111) is not None

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


# ---------------------------------------------------------------------------
# Protective (disaster) stops — placed on open, cancelled on close,
# self-healed per bar, orphan-cancelled on reconcile.
# ---------------------------------------------------------------------------

class TestTradeAmountPassThrough:
    """Per-strategy fixed notional (trade_amount in strategy_runtime.yaml).

    Exists because auto-sizing on a high-priced instrument can land below one
    share forever: CAT at ~$1,250 vs a $646 auto-sized amount refused on
    whole-share conversion on every signal (2026-07-27, correctly — but the
    strategy was permanently signal-only). trade_amount matches the
    backtester's live-semantics trade_notional, per strategy.
    """

    def test_fixed_amount_reaches_propose(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work(trade_amount=2000.0))
        assert sdk.propose_calls[0]['amount'] == 2000.0

    def test_zero_means_auto_size(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())
        assert sdk.propose_calls[0].get('amount') is None

    def test_explicit_share_quantity_wins_over_amount(self, executor):
        """A strategy that names a share count has decided; the notional knob
        must not override it."""
        ex, sdk = executor
        ex._process_signal(make_work(quantity=25.0, trade_amount=2000.0))
        assert sdk.propose_calls[0]['quantity'] == 25.0
        assert sdk.propose_calls[0].get('amount') is None

    def test_closes_never_carry_the_amount(self, executor):
        """Closes are sized by attribution, full stop — a fixed notional on a
        SELL would resize an exit."""
        ex, sdk = executor
        ex._process_signal(make_work(trade_amount=2000.0))
        ex._process_signal(make_work(action=Action.SELL, trade_amount=2000.0,
                                     bar_ts=TS + pd.Timedelta(minutes=1)))
        close = sdk.propose_calls[-1]
        assert close['action'] == 'SELL'
        assert close.get('amount') is None


class TestProtectiveStops:
    def test_open_places_gtc_stop_with_attribution(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())
        assert len(sdk.protective_calls) == 1
        call = sdk.protective_calls[0]
        assert call['action'] == 'SELL'
        assert call['order_type'] == 'STP'
        assert call['tif'] == 'GTC'
        assert call['quantity'] == 140.0
        # 8% (default) below the broker avgCost of 100.0
        assert call['aux_price'] == pytest.approx(92.0)
        # orderRef = strategy name so a fired stop's fill is ledger-attributed
        assert call['order_ref'] == 'orb_test'
        pos = ex.state.open_position('orb_test', 1111)
        assert pos['protective_order_id'] == 900

    def test_close_cancels_stop_first(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())
        ex._process_signal(make_work(action=Action.SELL,
                                     bar_ts=TS + pd.Timedelta(minutes=1)))
        assert sdk.cancel_calls == [900]
        assert ex.state.open_position('orb_test', 1111) is None

    def test_disabled_via_env(self, executor, monkeypatch):
        monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
        ex, sdk = executor
        ex._process_signal(make_work())
        assert sdk.protective_calls == []

    def test_placement_failure_retries_on_next_bar(self, executor):
        ex, sdk = executor
        sdk.protective_result = FakeResult(ok=False, error='no route')
        ex._process_signal(make_work())
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] is None
        # next bar self-heals once placement works again
        sdk.protective_result = None
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] == 900

    def test_preexisting_position_gets_stop_on_bar(self, executor):
        ex, sdk = executor
        # position opened before the feature shipped: attribution exists,
        # broker holds it, no protective tracked
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        sdk.broker[1111] = 140.0
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert len(sdk.protective_calls) == 1
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] == 900

    def test_an_unreadable_broker_quantity_places_no_stop(self, executor):
        """A NaN position is not a position. min(140.0, nan) is 140.0 and
        nan <= 0 is False, so the old inline clamp would have sized a
        protective SELL off attribution alone."""
        ex, sdk = executor
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        sdk.broker[1111] = float('nan')
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls == []

    def test_stop_is_clamped_to_the_broker_position_not_attribution(self, executor):
        """Attribution can outrun the broker — a partial fill, or a position
        trimmed by hand between the open and the next bar. The protective SELL
        must cover what is actually held, because an exit-class order is exempt
        from every gate and an oversized one would open a SHORT out of the
        mechanism whose job is to close one."""
        ex, sdk = executor
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        sdk.broker[1111] = 60.0        # only part of it is really there
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls[0]['quantity'] == 60.0

    def test_a_five_cent_entry_gets_a_stop_below_it_not_at_it(self, executor):
        """Regression. round(0.05 * 0.92, 2) == 0.05 — the stop landed ON the
        entry price, so the disaster stop sold at market the moment IB accepted
        it. Flooring puts it at 0.04, which is what a stop is for."""
        ex, sdk = executor
        sdk.avg_cost = 0.05
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        sdk.broker[1111] = 140.0
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls[0]['aux_price'] == pytest.approx(0.04)
        assert sdk.protective_calls[0]['aux_price'] < 0.05

    def test_no_stop_is_placed_when_none_can_sit_below_entry(self, executor):
        """At a 1-cent entry there is no two-decimal price strictly below it and
        above zero. Placing nothing (and retrying each bar) is honest; placing a
        stop at or below zero is an order IB rejects, and one at the entry is an
        instant market exit."""
        ex, sdk = executor
        sdk.avg_cost = 0.01
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        sdk.broker[1111] = 140.0
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls == []
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] is None

    def test_reconcile_cancels_orphaned_stop(self, executor):
        ex, sdk = executor
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        ex.state.set_protective('orb_test', 1111, 942)
        sdk.broker.pop(1111, None)   # position gone at broker
        sdk.broker[2222] = 10.0      # ...but the book itself reads fine
        ex._reconciled = False
        ex._reconcile_once()
        assert sdk.cancel_calls == [942]
        assert ex.state.open_position('orb_test', 1111) is None


# ---------------------------------------------------------------------------
# Bounded pyramiding — pyramid_max_adds allows fixed-lot adds up to the cap
# ---------------------------------------------------------------------------

class TestPyramidingDecision:
    def test_default_zero_keeps_single_lot(self):
        d = decide(make_work(), held_qty=100.0)
        assert d.kind == 'skip' and 'pyramiding' in d.reason

    def test_add_allowed_under_cap(self):
        d = decide_signal(
            make_work(pyramid_max_adds=3), kill_switch=False, paper_trading=True,
            held_qty=100.0, already_executed_bar=False, cooldown_active=False,
            held_lots=1)
        assert d.kind == 'open'
        assert 'pyramid add (lot 2)' in d.reason

    def test_stack_full_refuses(self):
        d = decide_signal(
            make_work(pyramid_max_adds=3), kill_switch=False, paper_trading=True,
            held_qty=400.0, already_executed_bar=False, cooldown_active=False,
            held_lots=4)
        assert d.kind == 'skip' and 'stack full' in d.reason

    def test_cooldown_applies_to_adds(self):
        d = decide_signal(
            make_work(pyramid_max_adds=3), kill_switch=False, paper_trading=True,
            held_qty=100.0, already_executed_bar=False, cooldown_active=True,
            held_lots=1)
        assert d.kind == 'skip' and 'cooldown' in d.reason

    def test_live_double_arm_gates_adds_too(self):
        d = decide_signal(
            make_work(pyramid_max_adds=3), kill_switch=False, paper_trading=False,
            held_qty=100.0, already_executed_bar=False, cooldown_active=False,
            live_armed=False, held_lots=1)
        assert d.kind == 'skip' and 'not armed' in d.reason


class TestPyramidingState:
    def test_record_add_folds_into_open_row(self, state):
        state.record_open('s1', 1, 10.0, TS, 7, None, None)
        state.record_add('s1', 1, 12.0, TS + pd.Timedelta(days=1), 8,
                         dt.time(15, 45), 60)
        pos = state.open_position('s1', 1)
        assert pos['quantity'] == 22.0
        assert pos['lots'] == 2
        # latest-BUY-wins: time-exit rules + entry bar come from the add
        assert pos['close_by_time'] == dt.time(15, 45)
        assert pos['max_hold_bars'] == 60
        assert pos['proposal_id'] == 8

    def test_close_clears_whole_stack(self, state):
        state.record_open('s1', 1, 10.0, TS, 7, None, None)
        state.record_add('s1', 1, 12.0, TS, 8, None, None)
        state.record_close('s1', 1, 'CLOSED', 'SELL signal')
        assert state.open_position('s1', 1) is None

    def test_premigration_row_reads_as_one_lot(self, state):
        state.record_open('s1', 1, 10.0, TS, 7, None, None)
        state.db.execute(
            "UPDATE auto_exec_positions SET lots = NULL WHERE strategy='s1'")
        assert state.open_position('s1', 1)['lots'] == 1


class TestPyramidingPipeline:
    def test_add_executes_and_recovers_protective(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work(pyramid_max_adds=3))
        assert ex.state.open_position('orb_test', 1111)['lots'] == 1
        first_stop = ex.state.open_position('orb_test', 1111)['protective_order_id']
        ex.cooldown_seconds = 0.0
        ex._process_signal(make_work(pyramid_max_adds=3,
                                     bar_ts=TS + pd.Timedelta(minutes=10)))
        pos = ex.state.open_position('orb_test', 1111)
        assert pos['lots'] == 2
        assert pos['quantity'] == 280.0  # two 140-share lots
        # old stop cancelled, new stop covers the whole stack
        assert sdk.cancel_calls == [first_stop]
        assert len(sdk.protective_calls) == 2
        assert sdk.protective_calls[-1]['quantity'] == 280.0

    def test_sell_closes_whole_stack(self, executor):
        ex, sdk = executor
        ex.cooldown_seconds = 0.0
        ex._process_signal(make_work(pyramid_max_adds=3))
        ex._process_signal(make_work(pyramid_max_adds=3,
                                     bar_ts=TS + pd.Timedelta(minutes=10)))
        ex._process_signal(make_work(action=Action.SELL,
                                     bar_ts=TS + pd.Timedelta(minutes=20)))
        assert ex.state.open_position('orb_test', 1111) is None
        sell = sdk.propose_calls[-1]
        assert sell['action'] == 'SELL' and sell['quantity'] == 280.0

    def test_cap_enforced_in_pipeline(self, executor):
        ex, sdk = executor
        ex.cooldown_seconds = 0.0
        for i in range(4):
            ex._process_signal(make_work(pyramid_max_adds=1,
                                         bar_ts=TS + pd.Timedelta(minutes=10 * i)))
        pos = ex.state.open_position('orb_test', 1111)
        assert pos['lots'] == 2  # initial + 1 add, extra signals refused
        opens = [c for c in sdk.propose_calls if c['action'] == 'BUY']
        assert len(opens) == 2


class TestDisarmedCloseNotStranded:
    """auto_execute=false must not strand attributed positions — mirrors the
    double-arm principle (closes are never gated by disarming)."""

    def test_disarmed_sell_with_position_closes(self):
        d = decide(make_work(action=Action.SELL, auto_execute=False), held_qty=18.0)
        assert d.kind == 'close'
        assert d.quantity == 18.0

    def test_disarmed_buy_still_refused(self):
        d = decide(make_work(auto_execute=False))
        assert d.kind == 'skip' and 'auto_execute' in d.reason

    def test_disarmed_sell_while_flat_still_refused(self):
        d = decide(make_work(action=Action.SELL, auto_execute=False))
        assert d.kind == 'skip' and 'auto_execute' in d.reason

    def test_kill_switch_still_blocks_disarmed_close(self):
        d = decide(make_work(action=Action.SELL, auto_execute=False),
                   held_qty=18.0, kill_switch=True)
        assert d.kind == 'skip' and 'kill switch' in d.reason


# ---------------------------------------------------------------------------
# Phase 2 approver notional tier vs. the auto-executor (operational contract)
# ---------------------------------------------------------------------------
#
# The auto-executor routes opens through propose -> approve ->
# place_expressive_order with algo_name=<strategy> and NO approver_key. The
# design keeps its sized notional BELOW the operator threshold, so it carries
# no credential and is unaffected. These tests exercise place_expressive_order
# directly with an auto-exec-shaped call:
#   * feature ON, threshold ABOVE the sized notional, no MMR_APPROVER_KEY in
#     env -> the open executes (auto-executor lifecycle is sacred);
#   * an above-threshold (mis-sized) auto open IS refused — documenting the
#     operational requirement to set the threshold above the auto max notional.

class TestAutoExecutorApproverTier:
    def _stub_trader(self, threshold):
        import threading
        from unittest.mock import AsyncMock, MagicMock
        import reactivex as rx
        from trader.trading.risk_gate import RiskGateResult, RiskInputs
        from trader.trading.trading_runtime import Trader

        class _ApproveAll:
            def check_instrument(self, **kw): return RiskGateResult(approved=True)
            def check_leverage(self, *a, **kw): return RiskGateResult(approved=True)
            def evaluate(self, *a, **kw): return RiskGateResult(approved=True, checks={'max_open_orders': 'pass', 'daily_loss': 'pass', 'concentration': 'pass', 'order_rate': 'pass'})

        class _Exec:
            def __init__(self): self.calls = 0
            async def subscribe_place_order_direct(self, approved):
                self.calls += 1
                ft = MagicMock(); ft.order = MagicMock(); ft.order.orderId = 1
                return rx.from_iterable([ft])

        class _Tick:
            ask = bid = last = close = 100.0

        t = object.__new__(Trader)
        t.pnl_subscriptions = {}
        t._pnl_subscriptions_lock = threading.Lock()
        t._main_loop = None
        t.disposables = []
        t.ib_account = 'DU12345'
        t.approver_required_above_usd = threshold
        # No key configured server-side either — the point is that below-threshold
        # auto opens never consult it.
        t.approver_key = ''
        t.order_tracker = None
        t.order_reduces_exposure = MagicMock(return_value=False)
        t.risk_gate = _ApproveAll()
        # Benign margin data, NOT a raising stub: check_order_margin failing is no
        # longer a skip — it refuses the open (fail-closed), which would make every
        # test here exercise the margin gate instead of its actual subject.
        t.check_order_margin = AsyncMock(return_value={'initMarginAfter': 1000.0, 'equityWithLoanAfter': 2000.0})
        t.gather_risk_inputs = MagicMock(return_value=RiskInputs(
            open_order_count=0, daily_pnl=0.0, daily_pnl_evaluable=True,
            portfolio_value=1e7, portfolio_value_evaluable=True))
        client = MagicMock()
        client.get_snapshot = AsyncMock(return_value=_Tick())
        t.client = client
        t.executioner = _Exec()
        return t

    def _contract(self):
        from ib_async.contract import Contract
        c = Contract(); c.symbol = 'PLTR'; c.exchange = 'NASDAQ'
        c.secType = 'STK'; c.conId = 4391
        return c

    def _spec(self):
        from trader.trading.proposal import ExecutionSpec
        return ExecutionSpec(order_type='MARKET', exit_type='NONE').to_dict()

    def test_below_threshold_auto_open_executes_without_key(self, monkeypatch):
        import asyncio
        from trader.common.reactivex import SuccessFailEnum
        monkeypatch.delenv('MMR_APPROVER_KEY', raising=False)
        # Sized notional 20 * 100 = $2000, threshold $5000 (above it).
        t = self._stub_trader(5000.0)
        result = asyncio.run(t.place_expressive_order(
            self._contract(), 'BUY', 20, self._spec(),
            algo_name='pltr_orb'))  # note: no approver_key passed at all
        assert result.success_fail == SuccessFailEnum.SUCCESS
        assert t.executioner.calls == 1

    def test_above_threshold_mis_sized_auto_open_refused(self, monkeypatch):
        import asyncio
        from trader.common.reactivex import SuccessFailEnum
        monkeypatch.delenv('MMR_APPROVER_KEY', raising=False)
        # Mis-sized: 100 * 100 = $10000 > $5000 threshold. Refused (no key).
        t = self._stub_trader(5000.0)
        result = asyncio.run(t.place_expressive_order(
            self._contract(), 'BUY', 100, self._spec(),
            algo_name='pltr_orb'))
        assert result.success_fail == SuccessFailEnum.FAIL
        assert t.executioner.calls == 0


# ---------------------------------------------------------------------------
# Strategy manifest — pure check_manifest gate (opens only)
# ---------------------------------------------------------------------------

_OPEN = Directive('open', 'BUY while flat', quantity=None)


class TestCheckManifestUniverse:
    def test_conid_in_allowed_passes(self):
        w = make_work(conid=1111, manifest_allowed_conids=[1111, 2222])
        assert check_manifest(w, _OPEN, opens_today=0, opens_hour=0) is None

    def test_conid_outside_allowed_refused(self):
        w = make_work(conid=9999, manifest_allowed_conids=[1111, 2222])
        d = check_manifest(w, _OPEN, opens_today=0, opens_hour=0)
        assert d is not None and d.kind == 'refused'
        assert '9999' in d.reason and 'allowed_conids' in d.reason

    def test_allowed_none_is_unchecked(self):
        w = make_work(conid=9999, manifest_allowed_conids=None)
        assert check_manifest(w, _OPEN, opens_today=0, opens_hour=0) is None

    def test_empty_allowed_list_refuses_everything(self):
        # An explicit empty whitelist means "trade nothing" — a deliberate,
        # not-None declaration, distinct from None (unchecked).
        w = make_work(conid=1111, manifest_allowed_conids=[])
        d = check_manifest(w, _OPEN, opens_today=0, opens_hour=0)
        assert d is not None and d.kind == 'refused'


class TestCheckManifestTurnover:
    def test_under_daily_cap_passes(self):
        w = make_work(manifest_max_opens_per_day=3)
        assert check_manifest(w, _OPEN, opens_today=2, opens_hour=0) is None

    def test_at_daily_cap_refused(self):
        w = make_work(manifest_max_opens_per_day=3)
        d = check_manifest(w, _OPEN, opens_today=3, opens_hour=0)
        assert d is not None and d.kind == 'refused'
        assert 'per_day' in d.reason

    def test_over_daily_cap_refused(self):
        w = make_work(manifest_max_opens_per_day=3)
        d = check_manifest(w, _OPEN, opens_today=5, opens_hour=0)
        assert d is not None and d.kind == 'refused'

    def test_hourly_cap_binds_independently(self):
        w = make_work(manifest_max_opens_per_day=100, manifest_max_opens_per_hour=2)
        # Under daily but at hourly => refused.
        d = check_manifest(w, _OPEN, opens_today=5, opens_hour=2)
        assert d is not None and 'per_hour' in d.reason

    def test_none_caps_unchecked(self):
        w = make_work(manifest_max_opens_per_day=None, manifest_max_opens_per_hour=None)
        assert check_manifest(w, _OPEN, opens_today=999, opens_hour=999) is None

    def test_pyramid_add_open_counts_toward_turnover(self):
        # A pyramid add is an 'open' directive; the gate treats it identically
        # to a fresh open (both increase exposure), so a full daily count
        # refuses the add.
        add = Directive('open', 'pyramid add (lot 2)', quantity=None)
        w = make_work(pyramid_max_adds=3, manifest_max_opens_per_day=2)
        assert check_manifest(w, add, opens_today=1, opens_hour=0) is None
        d = check_manifest(w, add, opens_today=2, opens_hour=0)
        assert d is not None and d.kind == 'refused'


class TestCheckManifestDirection:
    def test_long_declaration_allows_long_open(self):
        w = make_work(action=Action.BUY, manifest_direction='long')
        assert check_manifest(w, _OPEN, opens_today=0, opens_hour=0) is None

    def test_long_declaration_refuses_synthetic_short_open(self):
        # Today unreachable (decide_signal never emits an 'open' for SELL);
        # synthesise the future short-path regression and prove the gate
        # catches it.
        short_open = Directive('open', 'SHORT entry (hypothetical)', quantity=None)
        w = make_work(action=Action.SELL, manifest_direction='long')
        d = check_manifest(w, short_open, opens_today=0, opens_hour=0)
        assert d is not None and d.kind == 'refused'
        assert d.reason.startswith('manifest: direction')

    def test_direction_none_does_not_refuse_short_open(self):
        short_open = Directive('open', 'SHORT entry (hypothetical)', quantity=None)
        w = make_work(action=Action.SELL, manifest_direction=None)
        assert check_manifest(w, short_open, opens_today=0, opens_hour=0) is None


class TestCountOpensSince:
    def test_counts_only_open_decisions(self, state):
        now = dt.datetime.now()
        since = now - dt.timedelta(hours=24)
        state.log_decision('s1', 1, TS, 'BUY', 'open', 'proposal #1')
        state.log_decision('s1', 1, TS, 'BUY', 'skip', 'cooldown')
        state.log_decision('s1', 1, TS, 'SELL', 'close', 'proposal #2')
        state.log_decision('s1', 1, TS, 'BUY', 'refused', 'manifest')
        state.log_decision('s1', 1, TS, 'BUY', 'open', 'proposal #3')
        assert state.count_opens_since('s1', since) == 2
        # Other strategies don't count.
        assert state.count_opens_since('s2', since) == 0

    def test_window_excludes_old_opens(self, state):
        # Force an old 'created' timestamp by writing directly.
        old = dt.datetime.now() - dt.timedelta(hours=48)
        state.db.execute(
            "INSERT INTO auto_exec_bar_log VALUES (?, ?, ?, ?, ?, ?, ?)",
            ['s1', 1, TS.to_pydatetime(), 'BUY', 'open', 'old', old])
        state.log_decision('s1', 1, TS, 'BUY', 'open', 'recent')
        since = dt.datetime.now() - dt.timedelta(hours=24)
        assert state.count_opens_since('s1', since) == 1


class TestManifestPipelineOpen:
    """End-to-end through _process_signal — a refused open places NO order but
    logs a 'refused' decision; an allowed open proceeds normally."""

    def test_out_of_universe_open_refused_no_order(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work(conid=1111, manifest_allowed_conids=[2222]))
        assert sdk.propose_calls == []  # no order placed
        assert ex.state.open_position('orb_test', 1111) is None

    def test_in_universe_open_proceeds(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work(conid=1111, manifest_allowed_conids=[1111]))
        assert len(sdk.propose_calls) == 1
        assert ex.state.open_position('orb_test', 1111) is not None

    def test_turnover_cap_refuses_after_limit(self, executor):
        ex, sdk = executor
        ex.cooldown_seconds = 0.0
        # cap of 1 open/day: first opens, close, then a second open on a new
        # bar is refused (one 'open' already logged in the rolling window).
        ex._process_signal(make_work(manifest_max_opens_per_day=1))
        assert len(sdk.propose_calls) == 1
        ex._process_signal(make_work(action=Action.SELL,
                                     bar_ts=TS + pd.Timedelta(minutes=5),
                                     manifest_max_opens_per_day=1))
        # close placed (exits are never manifest-checked)
        assert len(sdk.propose_calls) == 2
        # second open refused by the daily cap
        ex._process_signal(make_work(bar_ts=TS + pd.Timedelta(minutes=10),
                                     manifest_max_opens_per_day=1))
        opens = [c for c in sdk.propose_calls if c['action'] == 'BUY']
        assert len(opens) == 1  # the second open never fired

    def test_no_manifest_is_noop_fast_path(self, executor):
        ex, sdk = executor
        # No manifest fields => _manifest_gate returns None without a DB query.
        ex._process_signal(make_work())
        assert len(sdk.propose_calls) == 1


class TestExternalStopReplacementSurvival:
    """The live resize test (2026-07-27) cancelled the executor's tracked
    stops (227/289) and re-created replacements it knew nothing about. Two
    consequences, both now handled by orderRef ownership: a dead tracked id
    with a live ref-stamped replacement is ADOPTED; on close, any OTHER live
    ref-owned order is SWEPT — without the sweep, the close left a GTC stop
    live against a flat position, where a later trigger fires into a SHORT
    with no gate re-check."""

    def _own_row(self, oid, ref='orb_test', conid=1111, status='PreSubmitted'):
        return {'orderId': oid, 'orderRef': ref, 'conId': conid,
                'status': status, 'action': 'SELL', 'totalQuantity': 140.0}

    def test_dead_tracked_id_with_live_replacement_is_adopted(self, executor):
        ex, sdk = executor
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        ex.state.set_protective('orb_test', 1111, 942)          # dead — not in open orders
        sdk.broker[1111] = 140.0
        sdk.open_orders.append(self._own_row(970))              # the replacement
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] == 970
        assert sdk.protective_calls == []                       # adopted, not re-placed

    def test_dead_tracked_id_with_no_replacement_replaces(self, executor):
        ex, sdk = executor
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        ex.state.set_protective('orb_test', 1111, 942)
        sdk.broker[1111] = 140.0
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert len(sdk.protective_calls) == 1
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] == 900

    def test_live_tracked_id_is_left_alone(self, executor):
        ex, sdk = executor
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        ex.state.set_protective('orb_test', 1111, 942)
        sdk.broker[1111] = 140.0
        sdk.open_orders.append(self._own_row(942))              # tracked AND live
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls == []
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] == 942

    def test_close_sweeps_untracked_ref_owned_stop(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())                          # open + stop 900
        sdk.open_orders.append(self._own_row(971))               # external re-create
        ex._process_signal(make_work(action=Action.SELL,
                                     bar_ts=TS + pd.Timedelta(minutes=1)))
        assert 971 in sdk.cancel_calls, 'the untracked own stop was not swept'
        assert ex.state.open_position('orb_test', 1111) is None

    def test_close_never_touches_manual_orders(self, executor):
        """A stop with someone else's ref (or none) is not ours to cancel."""
        ex, sdk = executor
        ex._process_signal(make_work())
        sdk.open_orders.append(self._own_row(972, ref=''))       # manual
        sdk.open_orders.append(self._own_row(973, ref='other_strategy'))
        ex._process_signal(make_work(action=Action.SELL,
                                     bar_ts=TS + pd.Timedelta(minutes=1)))
        assert 972 not in sdk.cancel_calls
        assert 973 not in sdk.cancel_calls


class TestTheStaleBarGateAnnouncesWhenItCannotRun:
    """The gate needs a datable bar AND a known interval. Missing either makes
    it silently not fire, and an open proceeds on a bar of unknown age.

    Letting the open through is deliberate: refusing every trade over a
    timestamp quirk is worse than acting on one. Doing it SILENTLY was not
    deliberate, and left the gate indistinguishable from a working one. Both
    conditions should be impossible in normal operation (`bar_ts` comes from
    the frame index, `bar_size` is parsed to an enum at load), so either one
    firing means something upstream is wrong.

    These tests assert the announcement, not a refusal. Changing the gate to
    refuse is a behaviour change on the live trading path and is a separate,
    deliberate decision.
    """

    def _executor(self):
        from unittest.mock import MagicMock
        from trader.strategy.auto_executor import AutoExecutor
        ex = object.__new__(AutoExecutor)
        ex._stale_gate_warned = {}
        return ex

    def _work(self, bar_size_seconds=60.0, bar_ts='2026-07-28T13:45:00Z'):
        from unittest.mock import MagicMock
        w = MagicMock()
        w.strategy_name = 'orb_probe'
        w.conid = 4391
        w.bar_size_seconds = bar_size_seconds
        w.bar_ts = bar_ts
        return w

    def test_an_unknown_bar_interval_is_announced(self, caplog):
        import logging as stdlib_logging
        ex = self._executor()
        with caplog.at_level(stdlib_logging.WARNING):
            ex._warn_if_stale_gate_inert(self._work(bar_size_seconds=0.0), 12.0)
        assert any('STALE-BAR GATE INERT' in r.message for r in caplog.records)
        assert any('bar interval unknown' in r.message for r in caplog.records)

    def test_an_undatable_bar_is_announced(self, caplog):
        import logging as stdlib_logging
        ex = self._executor()
        with caplog.at_level(stdlib_logging.WARNING):
            ex._warn_if_stale_gate_inert(self._work(), None)
        assert any('not datable' in r.message for r in caplog.records)

    def test_a_healthy_gate_says_nothing(self, caplog):
        """No noise in the normal case, or the warning stops meaning anything."""
        import logging as stdlib_logging
        ex = self._executor()
        with caplog.at_level(stdlib_logging.WARNING):
            ex._warn_if_stale_gate_inert(self._work(), 12.0)
        assert not [r for r in caplog.records if 'STALE-BAR' in r.message]

    def test_a_persistent_condition_logs_once_not_once_per_bar(self, caplog):
        """A 1-min strategy would otherwise emit 1,440 identical warnings a
        day, which is how a real signal gets filtered out by whoever reads the
        log."""
        import logging as stdlib_logging
        ex = self._executor()
        work = self._work(bar_size_seconds=0.0)
        with caplog.at_level(stdlib_logging.WARNING):
            for _ in range(25):
                ex._warn_if_stale_gate_inert(work, 12.0)
        assert len([r for r in caplog.records if 'STALE-BAR' in r.message]) == 1

    def test_each_instrument_is_reported_separately(self, caplog):
        import logging as stdlib_logging
        ex = self._executor()
        a, b = self._work(bar_size_seconds=0.0), self._work(bar_size_seconds=0.0)
        b.conid = 9999
        with caplog.at_level(stdlib_logging.WARNING):
            ex._warn_if_stale_gate_inert(a, 12.0)
            ex._warn_if_stale_gate_inert(b, 12.0)
        assert len([r for r in caplog.records if 'STALE-BAR' in r.message]) == 2

    def test_a_changed_reason_is_reported_again(self, caplog):
        """Dedup is per (instrument, reason). A different failure on the same
        instrument is new information."""
        import logging as stdlib_logging
        ex = self._executor()
        with caplog.at_level(stdlib_logging.WARNING):
            ex._warn_if_stale_gate_inert(self._work(bar_size_seconds=0.0), 12.0)
            ex._warn_if_stale_gate_inert(self._work(), None)
        assert len([r for r in caplog.records if 'STALE-BAR' in r.message]) == 2
