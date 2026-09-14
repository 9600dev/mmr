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

# A fixed bar timestamp, kept deterministic on purpose. These tests drive the
# OPEN path, which the stale-bar gate guards; the age they are judged against
# is frozen by the _freeze_bar_age fixture below rather than taken from the
# wall clock, so the same code always produces the same result.
from trader.strategy import auto_executor as auto_executor_module

TS = pd.Timestamp('2026-07-06 10:39:00')

@pytest.fixture(autouse=True)
def _freeze_clock(monkeypatch):
    """Pin "now" for this module, without stubbing anything real.

    The stale-bar gate measures a bar against the wall clock, so these tests
    are time-dependent. Making the fixture timestamp dynamic fixed that and
    made the mutation score move without the code moving (~30 mutants flipping
    between identical runs). Stubbing `bar_age_seconds` fixed THAT and cost ~26
    points of coverage, because the function's own mutants became unreachable.

    Overriding only the clock costs neither: the real `bar_age_seconds` runs
    against the real fixture timestamp, and the age it computes is a fixed 10
    seconds. Tests that are actually about staleness call `decide_signal`
    directly with an explicit age and are untouched by this.
    """
    frozen = TS.tz_localize('UTC') + pd.Timedelta(seconds=10)
    monkeypatch.setattr(auto_executor_module.AutoExecutor, '_now_utc',
                        lambda self: frozen.to_pydatetime())



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
        assert state.executed_for_bar('s1', 1, aware.tz_convert('UTC').tz_localize(None))
        assert not state.executed_for_bar('s1', 1, aware.tz_localize(None))


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
        self.executions = []
        self.positions_complete = None

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
            self.executions.append(dict(orderId=pid * 10, orderRef=p.metadata.get('strategy', ''),
                                        conId=1111, status='Filled', action=action,
                                        totalQuantity=qty, filled=qty, avgFillPrice=self.avg_cost,
                                        clientIntentId=p.metadata.get('client_intent_id', '')))
        return result

    def positions(self):
        rows = [{'conId': c, 'position': q, 'avgCost': self.avg_cost}
                for c, q in self.broker.items() if q != 0]
        frame = pd.DataFrame(rows)
        if self.positions_complete is not None:
            frame.attrs['complete'] = self.positions_complete
        return frame

    def place_protective_order(self, **kwargs):
        self.protective_calls.append(kwargs)
        if self.protective_result is not None:
            return self.protective_result
        oid = self.next_protective_id
        self.next_protective_id += 1
        self.open_orders.append(dict(orderId=oid, orderRef=kwargs['order_ref'], conId=1111,
                                     status='Submitted', action='SELL', orderType='STP',
                                     totalQuantity=kwargs['quantity'], filled=0,
                                     clientIntentId=kwargs.get('client_intent_id', '')))
        return FakeResult(ok=True, obj=SimpleNamespace(order=SimpleNamespace(orderId=oid)))

    def cancel(self, order_id):
        self.cancel_calls.append(order_id)
        for row in self.open_orders:
            if row['orderId'] == order_id:
                row['status'] = 'Cancelled'
        return FakeResult(ok=True)

    def trades(self):
        return pd.DataFrame(self.executions + list(self.open_orders))

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
        assert ex.state.open_position('orb_test', 1111) is not None

    def test_close_when_broker_flat_marks_external(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())
        sdk.broker[1111] = 0.0
        sdk.positions_complete = True
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

    def test_worker_survives_processing_errors(self, tmp_path, monkeypatch):
        import threading
        calls = []
        errors = []
        processed = threading.Event()

        def capture_error(message, *args, **kwargs):
            # Mutmut expands this module into a very large source file. Rich
            # traceback rendering can exceed the worker test's deadline;
            # capture the actual error report without timing its formatting.
            errors.append((message, args))
            if len(errors) == 2:
                processed.set()

        monkeypatch.setattr(auto_executor_module.logging, 'exception', capture_error)

        def broken_factory():
            calls.append(1)
            raise RuntimeError('sdk unavailable')
        ex = AutoExecutor(duckdb_path=str(tmp_path / 'worker_err.duckdb'),
                          paper_trading=True, sdk_factory=broken_factory)
        ex._reconciled = True
        try:
            ex.submit_signal(make_work())
            ex.submit_signal(make_work(bar_ts=TS + pd.Timedelta(minutes=1)))
            assert processed.wait(10), 'worker did not report both failed work items'
        finally:
            ex.stop()
            ex._worker.join(timeout=10)
        assert not ex._worker.is_alive(), 'worker must stop before fixture state is released'
        assert len(calls) == 2  # second item still processed after first blew up
        assert len(errors) == 2
        assert all('error processing' in message for message, _ in errors)
        assert [args[0].bar_ts for _, args in errors] == [TS, TS + pd.Timedelta(minutes=1)]


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


def _open_priced_fixture_without_protection(executor, monkeypatch):
    """Record the scripted matched fill price before exercising stop recovery."""
    with monkeypatch.context() as disabled:
        disabled.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
        executor._process_signal(make_work())


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
        # 8% (default) below the scripted matched fill price of 100.0
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
        sdk.protective_result = FakeResult(ok=False, error='broker rejected: no route')
        ex._process_signal(make_work())
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] is None
        # next bar self-heals once placement works again
        sdk.protective_result = None
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] == 900

    def test_preexisting_position_without_fill_cost_defers_new_stop(self, executor):
        ex, sdk = executor
        # Legacy attribution has no matched fill-price checkpoint. The
        # account's average cost cannot supply the missing ownership basis.
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        sdk.broker[1111] = 140.0
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls == []
        position = ex.state.open_position('orb_test', 1111)
        assert position['protective_order_id'] is None
        assert position['quantity'] == 140.0
        assert position['avg_cost'] is None
        assert position['cost_evaluable'] is False

    def test_an_unreadable_broker_quantity_places_no_stop(self, executor, monkeypatch):
        """A NaN position is not a position. min(140.0, nan) is 140.0 and
        nan <= 0 is False, so the old inline clamp would have sized a
        protective SELL off attribution alone."""
        ex, sdk = executor
        _open_priced_fixture_without_protection(ex, monkeypatch)
        sdk.broker[1111] = float('nan')
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls == []

    def test_stop_is_clamped_to_the_broker_position_not_attribution(self, executor, monkeypatch):
        """Attribution can outrun the broker — a partial fill, or a position
        trimmed by hand between the open and the next bar. The protective SELL
        must cover what is actually held, because an exit-class order is exempt
        from every gate and an oversized one would open a SHORT out of the
        mechanism whose job is to close one."""
        ex, sdk = executor
        _open_priced_fixture_without_protection(ex, monkeypatch)
        sdk.broker[1111] = 60.0        # only part of it is really there
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls[0]['quantity'] == 60.0

    def test_a_five_cent_entry_gets_a_stop_below_it_not_at_it(self, executor, monkeypatch):
        """Regression. round(0.05 * 0.92, 2) == 0.05 — the stop landed ON the
        entry price, so the disaster stop sold at market the moment IB accepted
        it. Flooring puts it at 0.04, which is what a stop is for."""
        ex, sdk = executor
        sdk.avg_cost = 0.05
        _open_priced_fixture_without_protection(ex, monkeypatch)
        sdk.broker[1111] = 140.0
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls[0]['aux_price'] == pytest.approx(0.04)
        assert sdk.protective_calls[0]['aux_price'] < 0.05

    def test_no_stop_is_placed_when_none_can_sit_below_entry(self, executor, monkeypatch):
        """At a 1-cent entry there is no two-decimal price strictly below it and
        above zero. Placing nothing (and retrying each bar) is honest; placing a
        stop at or below zero is an order IB rejects, and one at the entry is an
        instant market exit."""
        ex, sdk = executor
        sdk.avg_cost = 0.01
        _open_priced_fixture_without_protection(ex, monkeypatch)
        sdk.broker[1111] = 140.0
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls == []
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] is None

    def test_reconcile_cancels_orphaned_stop(self, executor):
        ex, sdk = executor
        ex.state.record_open('orb_test', 1111, 140.0, TS, None, None, None)
        ex.state.set_protective('orb_test', 1111, 942)
        # Model the current server's exact native intent/claim provenance;
        # an older unproven stop instead remains reserved for reconciliation.
        sdk.open_orders.append(dict(orderId=942, orderRef='orb_test', conId=1111,
                                    status='Submitted', action='SELL', orderType='STP',
                                    totalQuantity=140.0, filled=0.0,
                                    clientIntentId='protective:orphan-942',
                                    brokerIntentCreatedAt=dt.datetime.now(dt.timezone.utc).timestamp()))
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
            held_lots=1, bar_age_seconds=10.0)
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
            held_lots=1, bar_age_seconds=10.0)
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
        c.secType = 'STK'; c.conId = 4391; c.currency = 'USD'
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
            "INSERT INTO auto_exec_bar_log (strategy, conid, bar_ts, action, decision, reason, created) VALUES (?, ?, ?, ?, ?, ?, ?)",
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
                'status': status, 'action': 'SELL', 'orderType': 'STP',
                'totalQuantity': 140.0, 'filled': 0.0}

    def test_dead_tracked_id_with_live_replacement_is_adopted(self, executor, monkeypatch):
        ex, sdk = executor
        _open_priced_fixture_without_protection(ex, monkeypatch)
        ex.state.set_protective('orb_test', 1111, 942)          # dead — not in open orders
        sdk.broker[1111] = 140.0
        sdk.open_orders.append(self._own_row(970))              # the replacement
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] == 970
        assert sdk.protective_calls == []                       # adopted, not re-placed

    def test_dead_tracked_id_with_no_replacement_replaces(self, executor, monkeypatch):
        ex, sdk = executor
        _open_priced_fixture_without_protection(ex, monkeypatch)
        ex.state.set_protective('orb_test', 1111, 942)
        sdk.broker[1111] = 140.0
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert len(sdk.protective_calls) == 1
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] == 900

    def test_live_tracked_id_is_left_alone(self, executor, monkeypatch):
        ex, sdk = executor
        _open_priced_fixture_without_protection(ex, monkeypatch)
        ex.state.set_protective('orb_test', 1111, 942)
        sdk.broker[1111] = 140.0
        sdk.open_orders.append(self._own_row(942))              # tracked AND live
        ex._process_bar(BarWork('orb_test', 1111, TS + pd.Timedelta(minutes=1), 1))
        assert sdk.protective_calls == []
        assert ex.state.open_position('orb_test', 1111)['protective_order_id'] == 942

    def test_close_sweeps_untracked_ref_owned_stop(self, executor):
        ex, sdk = executor
        ex._process_signal(make_work())                          # open + stop 900
        # Preserve the external workflow's original claim time in the
        # normalized broker row; never synthesize it on a later replay.
        sdk.open_orders.append(dict(self._own_row(971),
            clientIntentId='protective:external-971',
            brokerIntentCreatedAt=dt.datetime.now(dt.timezone.utc).timestamp()))
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
    """Opening freshness requires a datable bar and a known positive interval.

    An unavailable input needs an actionable warning naming its strategy and
    instrument. These tests pin the warning and per-owner deduplication;
    separate decision tests pin refusal of unverifiable opens while exits
    retain their existing authority. Valid one-second bars must not produce
    an unavailable-interval warning.
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
        assert any('STALE-BAR INPUT UNAVAILABLE' in r.message for r in caplog.records)
        assert any('Opens are refused' in r.message for r in caplog.records)
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


def _native_open_proposal_boundary(tmp_path, monkeypatch):
    """Use the actual offline proposer and store; revoke before any approval.

    The authority changes during proposal creation, a supported race. This
    isolates proposal sizing/audit from exchange execution and makes no fill
    or server-approval claim.
    """
    from trader.data.proposal_store import ProposalStore
    from trader.sdk import MMR
    from trader.trading.position_sizing import PortfolioState, PositionSizingConfig

    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    config = PositionSizingConfig(
        base_position_usd=1000, min_position_usd=1, max_position_usd=10000,
        min_confidence_scale=0, volatility_adjustment=False)
    monkeypatch.setattr(PositionSizingConfig, 'load', staticmethod(lambda path=None: config))
    proposer = MMR.__new__(MMR)
    proposer._prop_store = ProposalStore(str(tmp_path / 'proposals.duckdb'))
    proposer.snapshot = lambda *args, **kwargs: None
    proposer._get_portfolio_state = lambda: PortfolioState()
    sdk = FakeSDK()
    sdk.propose = proposer.propose
    sdk._proposal_store = proposer._proposal_store
    authority_reads = []

    def opening_authority(strategy, generation):
        authority_reads.append((strategy, generation))
        return len(authority_reads) == 1

    executor = AutoExecutor(
        str(tmp_path / 'entry.duckdb'), paper_trading=True,
        sdk_factory=lambda: sdk, authority_check=opening_authority)
    return executor, sdk, proposer._proposal_store(), authority_reads


@pytest.mark.parametrize(
    'probability,trade_amount,expected_confidence,expected_amount',
    [(0.25, 0, 0.25, 250), (1.5, 0, 1.0, 1000), (0.25, 0.5, 0.25, 0.5)],
    ids=['signal_confidence', 'defensive_confidence_ceiling', 'small_fixed_budget'])
def test_open_preserves_native_proposal_budget_confidence_and_risk_audit(
        tmp_path, monkeypatch, probability, trade_amount,
        expected_confidence, expected_amount):
    executor, sdk, store, authority_reads = _native_open_proposal_boundary(tmp_path, monkeypatch)
    try:
        executor._process_signal(make_work(
            quantity=0, probability=probability, risk=0.75,
            trade_amount=trade_amount, deployment_generation='entry-generation'))
        opening, = executor.intents.all(kind='OPEN')
        proposal = store.get(opening['payload']['proposal_id'])

        assert proposal.quantity is None
        assert proposal.amount == expected_amount
        assert proposal.confidence == expected_confidence
        assert proposal.metadata['risk_level'] == 0.75
        assert proposal.metadata['client_intent_id'] == opening['intent_id']
        assert proposal.metadata['strategy'] == 'orb_test'
        if trade_amount:
            assert 'auto_sized' not in proposal.metadata
        else:
            assert proposal.metadata['auto_sized'] is True
            assert proposal.metadata['sizing_result']['amount'] == expected_amount
        assert opening['status'] == 'REJECTED'
        assert 'authority revoked' in opening['payload']['error']
        assert authority_reads == [('orb_test', 'entry-generation')] * 2
        assert sdk.approve_calls == [] and sdk.broker == {}
    finally:
        executor.intents.journal.close()


def test_native_zero_position_snapshot_allows_an_automated_open_proposal(tmp_path, monkeypatch):
    """A native reqPositions response can contain the zero-position callback.

    IB removes zero from its standing cache but retains that same Position in
    the in-flight request result. The real server snapshot must not turn it
    into a short-position refusal. Proposal authority is revoked before wire.
    """
    import asyncio
    from ib_async import IB, Stock
    from review.test_review_order_contract import _coordinated_trader

    server = _coordinated_trader(tmp_path, held=0)
    executor = None
    try:
        async def native_zero_positions():
            broker = IB()
            request = broker.wrapper.startReq('positions')
            broker.wrapper.position(server.ib_account,
                                    Stock('WDS', 'ASX', 'AUD', conId=1111), 0, 10)
            broker.wrapper.positionEnd()
            rows = await request
            assert broker.wrapper.positions[server.ib_account] == {}
            assert len(rows) == 1 and rows[0].position == 0
            return rows

        server.client.ib.reqPositionsAsync = native_zero_positions
        executor, sdk, store, _authority = _native_open_proposal_boundary(tmp_path, monkeypatch)
        sdk.execution_snapshot = lambda **kwargs: asyncio.run(server.execution_snapshot(**kwargs))
        snapshot = sdk.execution_snapshot()
        assert snapshot['positions_complete'] is True
        assert [(row['conId'], row['position']) for row in snapshot['positions']] == [(1111, 0)]

        executor._process_signal(make_work(quantity=4, deployment_generation='entry-generation'))

        openings = executor.intents.all(kind='OPEN')
        assert len(openings) == 1, 'an observed zero position must not refuse a long-only entry'
        proposal = store.get(openings[0]['payload']['proposal_id'])
        assert proposal.action == 'BUY' and proposal.quantity == 4
        assert sdk.approve_calls == [] and server.placed == []
    finally:
        try:
            if executor is not None:
                executor.intents.journal.close()
        finally:
            server.order_tracker.close(timeout=1)
            if server.order_tracker._journal is not None:
                server.order_tracker._journal.close()
            if server.order_tracker._temporary is not None:
                server.order_tracker._temporary.cleanup()


@pytest.fixture
def signal_decision_owner(tmp_path, monkeypatch):
    """Real typed work and stores through the existing external SDK adapter.

    This covers executor decision/coordination contracts. The historical test
    adapter supplies broker observations; it is not a new native IB acceptance
    or server-risk test, and no acceptance map is patched to choose a result.
    """
    from review.test_review_strategy_contract import LifecycleSDK

    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    sdk = LifecycleSDK()
    sdk.broker[1111] = 100  # independent manual inventory
    path = str(tmp_path / 'signal_decisions.duckdb')
    manager = AutoExecutor(path, paper_trading=True, cooldown_seconds=0,
                           sdk_factory=lambda: sdk)
    try:
        yield SimpleNamespace(manager=manager, sdk=sdk, path=path)
    finally:
        manager.intents.journal.close()


def _signal_nonexecution_rows(ctx, work):
    # Reopen the real state store: an in-memory call capture cannot establish
    # that a useful explanation survived the worker and a later reader.
    state = AutoExecState(ctx.path)
    return state.db.execute(
        "SELECT strategy, conid, bar_ts, action, decision, reason "
        "FROM auto_exec_bar_log WHERE strategy=? AND conid=? AND bar_ts=? "
        "AND decision NOT IN ('open', 'close')",
        [work.strategy_name, work.conid, work.bar_ts.to_pydatetime()], fetch='all')


def test_flat_legacy_request_is_retired_before_an_unrelated_native_open(signal_decision_owner):
    """Compatibility input only: an old unbound explicit request is not new authority."""
    from trader.strategy.execution_intents import timestamp_text

    ctx = signal_decision_owner
    old = ctx.manager.intents.create('orb_test', 1111, 'CLOSE', dict(
        bar_ts=timestamp_text(TS - pd.Timedelta(minutes=10)), quantity=40,
        reason='historical explicit exit', ident=ctx.manager._resolve_exact(1111),
        exit_request_active=True), status='REJECTED')
    # REJECTED is an actual terminal no-send status and remains terminal in
    # reconciliation. A bare CANCELLED row without physical ownership proof
    # would correctly become UNKNOWN and mask this compatibility boundary.
    assert 'explicit_exit_scope' not in old['payload']
    assert ctx.manager.state.open_position('orb_test', 1111) is None

    ctx.manager._process_signal(make_work(quantity=40))
    ctx.manager.manage_positions()

    retired, = [item for item in ctx.manager.intents.all(kind='CLOSE')
                 if item['intent_id'] == old['intent_id']]
    assert retired['status'] == 'REJECTED'
    assert retired['payload']['exit_request_active'] is False
    assert [call['action'] for call in ctx.sdk.propose_calls] == ['BUY']
    assert ctx.manager.state.open_position('orb_test', 1111)['quantity'] == 40
    assert ctx.sdk.broker[1111] == 140


@pytest.mark.parametrize('other_work', [
    'none', 'foreign_owner_open', 'foreign_instrument_open', 'own_protective'])
def test_same_bar_sell_dedup_uses_only_its_own_pending_open(
        signal_decision_owner, monkeypatch, other_work):
    ctx = signal_decision_owner
    ctx.manager._process_signal(make_work(quantity=40))
    opening, = ctx.manager.intents.all(strategy='orb_test', conid=1111, kind='OPEN')
    assert opening['status'] == 'FILLED'
    assert ctx.manager.intents.all(strategy='orb_test', conid=1111, kind='OPEN', active=True) == []

    if other_work.startswith('foreign_'):
        owner = 'independent_owner' if other_work == 'foreign_owner_open' else 'orb_test'
        conid = 1111 if other_work == 'foreign_owner_open' else 2222
        if conid == 2222:
            other = SimpleNamespace(**dict(vars(ctx.sdk.secdef), symbol='SECOND', conId=2222))
            resolve = ctx.sdk.resolve
            monkeypatch.setattr(ctx.sdk, 'resolve', lambda symbol, **kwargs:
                [other] if symbol in (2222, 'SECOND') else resolve(symbol, **kwargs))

        class ProposalBoundaryStop(BaseException):
            pass

        def stop_before_allocation(**kwargs):
            raise ProposalBoundaryStop()

        # Run the actual OPEN producer to its committed SUBMITTING boundary.
        # A process-stop exception precedes proposal allocation and any wire
        # request; no synthetic accepted order for another namespace is needed.
        with monkeypatch.context() as pause:
            pause.setattr(ctx.sdk, 'propose', stop_before_allocation)
            with pytest.raises(ProposalBoundaryStop):
                ctx.manager._execute_open(
                    make_work(strategy_name=owner, conid=conid, quantity=10),
                    Directive('open', 'ordinary pending entry', quantity=10))
        foreign, = ctx.manager.intents.all(strategy=owner, conid=conid, kind='OPEN', active=True)
        assert foreign['status'] == 'SUBMITTING'
        assert not foreign['payload'].get('proposal_id')
    elif other_work == 'own_protective':
        monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '8')
        ctx.manager._ensure_protective('orb_test', 1111)
        protective, = ctx.manager.intents.all(strategy='orb_test', conid=1111,
                                              kind='PROTECTIVE', active=True)
        assert protective['status'] == 'WORKING'
        assert ctx.sdk.active_stops

    work = make_work(action=Action.SELL)
    ctx.manager._process_signal(work)

    assert [call['action'] for call in ctx.sdk.propose_calls] == ['BUY']
    assert ctx.manager.intents.all(strategy='orb_test', conid=1111, kind='CLOSE') == []
    assert ctx.manager.state.open_position('orb_test', 1111)['quantity'] == 40
    assert ctx.sdk.broker[1111] == 140
    row, = _signal_nonexecution_rows(ctx, work)
    assert row[3] == 'SELL' and 'skip' in row[4].casefold()
    assert 'already executed' in row[5].casefold()


def test_recent_native_close_keeps_cooldown_after_the_open_stamp_expires(
        signal_decision_owner, monkeypatch):
    ctx = signal_decision_owner
    real_datetime = dt.datetime
    clock = [TS.tz_localize('UTC').timestamp() + 10]

    class ClockDatetime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return real_datetime.fromtimestamp(clock[0], tz)

    # Advance wall-clock seams, never either cooldown operand or its decision.
    # The dt proxy is local to this production module, preserving the native
    # datetime type checks in the independently imported intent serializer.
    clock_module = SimpleNamespace(**vars(dt))
    clock_module.datetime = ClockDatetime
    monkeypatch.setattr(auto_executor_module, 'dt', clock_module)
    monkeypatch.setattr(auto_executor_module.time, 'time', lambda: clock[0])
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
        real_datetime.fromtimestamp(clock[0], dt.timezone.utc))
    ctx.manager.cooldown_seconds = 300
    ctx.manager._process_signal(make_work(quantity=40))
    clock[0] += 600
    ctx.manager._process_signal(make_work(action=Action.SELL,
                                          bar_ts=TS + pd.Timedelta(minutes=10)))
    assert ctx.manager.state.open_position('orb_test', 1111) is None
    assert ctx.sdk.broker[1111] == 100
    assert not ctx.manager.intents.submitted_recently('orb_test', 1111, 300)
    clock[0] += 1
    work = make_work(quantity=40, bar_ts=TS + pd.Timedelta(minutes=10, seconds=1))

    ctx.manager._process_signal(work)

    assert [call['action'] for call in ctx.sdk.propose_calls] == ['BUY', 'SELL']
    assert ctx.manager.state.open_position('orb_test', 1111) is None
    assert ctx.sdk.broker[1111] == 100
    row, = _signal_nonexecution_rows(ctx, work)
    assert row[3] == 'BUY' and 'skip' in row[4].casefold()
    assert 'cooldown' in row[5].casefold()


@pytest.mark.parametrize('pending_entry', [False, True], ids=['filled-entry', 'pending-entry'])
def test_signal_close_retains_its_trigger_through_the_durable_request(
        signal_decision_owner, pending_entry):
    ctx = signal_decision_owner
    ctx.sdk.fill_next = 0 if pending_entry else None
    ctx.manager._process_signal(make_work(quantity=40))
    ctx.sdk.fill_next = None
    ctx.manager._process_signal(make_work(action=Action.SELL,
                                          bar_ts=TS + pd.Timedelta(minutes=1)))

    closing, = ctx.manager.intents.all(strategy='orb_test', conid=1111, kind='CLOSE')
    reason = closing['payload']['reason']
    assert isinstance(reason, str)
    assert 'sell' in reason.casefold() and 'signal' in reason.casefold()
    if pending_entry:
        assert 'pending' in reason.casefold() and 'entry' in reason.casefold()
    else:
        proposal = ctx.sdk.propose_calls[-1]
        assert proposal['action'] == 'SELL'
        assert proposal['metadata']['close_reason'] == reason
        assert reason in proposal['reasoning']
    assert ctx.sdk.broker[1111] == 100


def test_double_armed_long_manifest_accepts_fresh_work_without_false_input_warning(
        signal_decision_owner, monkeypatch, caplog):
    ctx = signal_decision_owner
    monkeypatch.setenv('MMR_AUTO_EXECUTE_LIVE', '1')
    ctx.manager.paper_trading = False
    with caplog.at_level(logging.INFO):
        ctx.manager._process_signal(make_work(quantity=40, manifest_direction='long'))

    assert [call['action'] for call in ctx.sdk.propose_calls] == ['BUY']
    assert len(ctx.sdk.approve_calls) == 1
    assert ctx.manager.state.open_position('orb_test', 1111)['quantity'] == 40
    assert ctx.sdk.broker[1111] == 140
    assert not any('input unavailable' in record.getMessage().casefold()
                   or 'not datable' in record.getMessage().casefold()
                   for record in caplog.records)


def test_configured_stale_bar_multiple_reaches_the_durable_decision(
        signal_decision_owner, monkeypatch):
    ctx = signal_decision_owner
    monkeypatch.setenv('MMR_STALE_BAR_MULTIPLE', '1')
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
        (TS.tz_localize('UTC') + pd.Timedelta(seconds=120)).to_pydatetime())
    work = make_work(quantity=40, bar_size_seconds=60)

    ctx.manager._process_signal(work)

    assert ctx.sdk.propose_calls == ctx.sdk.approve_calls == []
    assert ctx.manager.intents.all(kind='OPEN') == []
    row, = _signal_nonexecution_rows(ctx, work)
    assert 'skip' in row[4].casefold()
    assert 'stale' in row[5].casefold()


def test_signal_skip_keeps_identity_action_and_cause_in_log_and_reopened_store(
        signal_decision_owner, caplog):
    ctx = signal_decision_owner
    work = make_work(auto_execute=False)
    with caplog.at_level(logging.INFO):
        ctx.manager._process_signal(work)

    assert ctx.sdk.propose_calls == ctx.sdk.approve_calls == []
    row, = _signal_nonexecution_rows(ctx, work)
    assert row[:4] == ('orb_test', 1111, TS.to_pydatetime(), 'BUY')
    assert 'skip' in row[4].casefold()
    assert isinstance(row[5], str)
    assert 'auto_execute' in row[5] and 'false' in row[5].casefold()
    messages = [record.getMessage() for record in caplog.records
                if record.levelno == logging.INFO and 'skip' in record.getMessage().casefold()]
    message, = messages
    assert all(fact in message.casefold() for fact in ('orb_test', '1111', 'buy', 'auto_execute', 'false'))


def test_manifest_refusal_keeps_identity_action_and_policy_in_log_and_reopened_store(
        signal_decision_owner, caplog):
    ctx = signal_decision_owner
    work = make_work(manifest_allowed_conids=frozenset({2222}))
    with caplog.at_level(logging.INFO):
        ctx.manager._process_signal(work)

    assert ctx.sdk.propose_calls == ctx.sdk.approve_calls == []
    row, = _signal_nonexecution_rows(ctx, work)
    assert row[:4] == ('orb_test', 1111, TS.to_pydatetime(), 'BUY')
    assert 'refused' in row[4].casefold()
    assert isinstance(row[5], str)
    assert all(fact in row[5].casefold() for fact in ('manifest', '1111', 'allowed'))
    messages = [record.getMessage() for record in caplog.records
                if record.levelno == logging.WARNING and 'auto-executor' in record.getMessage().casefold()]
    message, = messages
    assert all(fact in message.casefold() for fact in ('orb_test', '1111', 'buy', 'manifest', 'allowed'))
    # Keep the identity field distinct from an ID repeated in the refusal cause.
    assert any(record.levelno == logging.WARNING
               and record.getMessage() == message
               and isinstance(record.args, tuple) and len(record.args) > 1
               and record.args[1] == work.conid
               for record in caplog.records)


def test_resolution_refusal_keeps_exact_identity_action_and_cause_after_restart(
        signal_decision_owner, monkeypatch, caplog):
    ctx = signal_decision_owner
    monkeypatch.setattr(ctx.sdk, 'resolve', lambda *args, **kwargs: [])
    work = make_work(quantity=40)
    with caplog.at_level(logging.INFO):
        ctx.manager._process_signal(work)

    assert ctx.sdk.propose_calls == ctx.sdk.approve_calls == []
    row, = _signal_nonexecution_rows(ctx, work)
    assert row[:4] == ('orb_test', 1111, TS.to_pydatetime(), 'BUY')
    assert 'refused' in row[4].casefold()
    assert isinstance(row[5], str)
    assert '1111' in row[5] and 'not found' in row[5].casefold()
    messages = [record.getMessage() for record in caplog.records
                if record.levelno == logging.ERROR and 'refused' in record.getMessage().casefold()]
    message, = messages
    assert all(fact in message.casefold() for fact in ('orb_test', '1111', 'not found'))
    # Keep the identity field distinct from an ID repeated in the refusal cause.
    assert any(record.levelno == logging.ERROR
               and record.getMessage() == message
               and isinstance(record.args, tuple) and len(record.args) > 1
               and record.args[1] == work.conid
               for record in caplog.records)


def test_worker_kill_switch_log_retains_the_actionable_setting(
        signal_decision_owner, monkeypatch, caplog):
    ctx = signal_decision_owner
    ctx.manager._process_signal(make_work(quantity=40))
    monkeypatch.setenv('MMR_AUTO_EXECUTE_DISABLED', '1')
    caplog.clear()

    with caplog.at_level(logging.INFO):
        ctx.manager._process_signal(make_work(
            action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))

    assert any(record.levelno == logging.INFO
               and 'MMR_AUTO_EXECUTE_DISABLED' in record.getMessage()
               for record in caplog.records)
    assert [call['action'] for call in ctx.sdk.propose_calls] == ['BUY']
    assert ctx.manager._emergency_exits == {}
    assert ctx.sdk.broker[1111] == 140


def test_worker_storage_outage_retains_sell_cause_with_owned_reduction(
        signal_decision_owner, monkeypatch):
    ctx = signal_decision_owner
    ctx.manager._process_signal(make_work(quantity=40))
    calls = []

    def emergency_close_position(**kwargs):
        # The existing external SDK seam represents the independent endpoint.
        # No accepted-order mapping or executor decision is replaced.
        calls.append(kwargs)
        ctx.sdk.broker[1111] -= kwargs['quantity']
        return FakeResult(obj=[1999])

    def unavailable(*args, **kwargs):
        raise OSError('local execution storage unavailable')

    monkeypatch.setattr(ctx.sdk, 'emergency_close_position',
                        emergency_close_position, raising=False)
    monkeypatch.setattr(ctx.manager.intents, 'all', unavailable)
    monkeypatch.setattr(ctx.manager.state, 'open_position', unavailable)

    ctx.manager._process_signal(make_work(
        action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))

    assert len(calls) == 1
    assert calls[0]['quantity'] == 40
    assert calls[0]['strategy_name'] == 'orb_test'
    assert calls[0]['con_id'] == 1111
    assert ctx.sdk.broker[1111] == 100
    pending = ctx.manager._emergency_exits[('orb_test', 1111)]
    assert pending['attempted'] is True
    assert pending['intent_id'] == calls[0]['client_intent_id']
    assert isinstance(pending['reason'], str)
    assert all(fact in pending['reason'].casefold()
               for fact in ('sell', 'local state', 'unavailable'))
    # The wire endpoint does not receive this reason. It is retained on the
    # request for later durable restoration; no durable-write claim is made
    # while both local read boundaries remain unavailable.


def test_native_one_second_bar_opens_without_unknown_interval_warning(
        signal_decision_owner, monkeypatch, caplog):
    from trader.objects import BarSize

    ctx = signal_decision_owner
    seconds = float(pd.Timedelta(BarSize.to_pandas_freq(BarSize.Secs1)).total_seconds())
    assert seconds == 1.0
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
        (TS.tz_localize('UTC') + pd.Timedelta(milliseconds=250)).to_pydatetime())
    caplog.clear()

    ctx.manager._process_signal(make_work(quantity=40, bar_size_seconds=seconds))

    assert [call['action'] for call in ctx.sdk.propose_calls] == ['BUY']
    assert ctx.manager.state.open_position('orb_test', 1111)['quantity'] == 40
    assert ctx.sdk.broker[1111] == 140
    assert not any('input unavailable' in record.getMessage().casefold()
                   or 'interval unknown' in record.getMessage().casefold()
                   for record in caplog.records)


def test_unavailable_bar_warning_keeps_owner_instrument_and_cause(
        signal_decision_owner, caplog):
    ctx = signal_decision_owner
    work = make_work(bar_size_seconds=0.0)
    caplog.clear()

    # This directly checks the existing warning contract. It makes no claim
    # that the old zero-interval worker decision is already fail-closed;
    # separate source-regression tests cover that pending repair.
    ctx.manager._warn_if_stale_gate_inert(work, 10.0)
    ctx.manager._warn_if_stale_gate_inert(work, 10.0)

    records = [record for record in caplog.records
               if record.levelno == logging.WARNING
               and 'input unavailable' in record.getMessage().casefold()]
    assert len(records) == 1
    message = records[0].getMessage().casefold()
    assert 'orb_test' in message and '1111' in message
    assert 'interval unknown' in message and 'bar_size_seconds' in message
    assert ctx.sdk.propose_calls == ctx.sdk.approve_calls == []
    assert ctx.sdk.broker[1111] == 100


def test_bar_age_default_clock_returns_elapsed_time_for_an_aware_native_bar():
    # The worker normally supplies _now_utc explicitly. This exercises the
    # documented default of the helper itself, using native aware datetimes.
    before = dt.datetime.now(dt.timezone.utc)
    bar = before - dt.timedelta(seconds=2)

    age = auto_executor_module.bar_age_seconds(bar)

    after = dt.datetime.now(dt.timezone.utc)
    assert isinstance(age, float)
    assert (before - bar).total_seconds() <= age <= (after - bar).total_seconds()


def test_nonrunning_signal_keeps_its_refusal_cause_after_reopen(signal_decision_owner):
    """A native SELL callback can arrive after its strategy leaves RUNNING."""
    ctx = signal_decision_owner
    work = make_work(action=Action.SELL, state_running=False)

    ctx.manager._process_signal(work)

    assert ctx.sdk.propose_calls == ctx.sdk.approve_calls == []
    assert ctx.manager.intents.all() == []
    rows = _signal_nonexecution_rows(ctx, work)
    assert len(rows) == 1
    row = rows[0]
    assert row[:4] == ('orb_test', 1111, TS.to_pydatetime(), 'SELL')
    assert row[4] == 'skip'
    assert isinstance(row[5], str)
    assert 'not' in row[5].casefold() and 'running' in row[5].casefold()
    assert ctx.sdk.broker[1111] == 100


def test_one_filled_share_still_reserves_the_single_lot(
        signal_decision_owner, monkeypatch):
    """A completed one-share fill is a holding, not permission for another lot."""
    ctx = signal_decision_owner
    monkeypatch.setenv('MMR_STALE_BAR_MULTIPLE', '3')
    ctx.manager._process_signal(make_work(quantity=1))
    opening, = ctx.manager.intents.all(strategy='orb_test', conid=1111, kind='OPEN')
    assert opening['status'] == 'FILLED'
    assert ctx.manager.state.open_position('orb_test', 1111)['quantity'] == 1
    assert ctx.sdk.broker[1111] == 101

    next_bar = TS + pd.Timedelta(minutes=1)
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
        (next_bar.tz_localize('UTC') + pd.Timedelta(seconds=10)).to_pydatetime())
    work = make_work(quantity=1, bar_ts=next_bar, pyramid_max_adds=0)
    ctx.manager._process_signal(work)

    assert [call['action'] for call in ctx.sdk.propose_calls] == ['BUY']
    assert len(ctx.sdk.approve_calls) == 1
    current, = ctx.manager.intents.all(strategy='orb_test', conid=1111, kind='OPEN')
    assert current['intent_id'] == opening['intent_id'] and current['status'] == 'FILLED'
    assert ctx.manager.state.open_position('orb_test', 1111)['quantity'] == 1
    assert ctx.sdk.broker[1111] == 101
    rows = _signal_nonexecution_rows(ctx, work)
    assert len(rows) == 1
    row = rows[0]
    assert row[:4] == ('orb_test', 1111, next_bar.to_pydatetime(), 'BUY')
    assert row[4] == 'skip'
    assert isinstance(row[5], str) and 'pyramiding' in row[5].casefold()


@pytest.mark.parametrize('age_seconds, permitted', [(180, True), (181, False)],
                         ids=['exact-limit', 'past-limit'])
def test_freshness_limit_uses_the_actual_bar_age_inclusively(
        signal_decision_owner, monkeypatch, age_seconds, permitted):
    """An age equal to three valid intervals is allowed; older work is refused."""
    ctx = signal_decision_owner
    monkeypatch.setenv('MMR_STALE_BAR_MULTIPLE', '3')
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self:
        (TS.tz_localize('UTC') + pd.Timedelta(seconds=age_seconds)).to_pydatetime())
    work = make_work(quantity=1, bar_size_seconds=60)

    ctx.manager._process_signal(work)

    if permitted:
        assert [call['action'] for call in ctx.sdk.propose_calls] == ['BUY']
        assert len(ctx.sdk.approve_calls) == 1
        assert ctx.manager.state.open_position('orb_test', 1111)['quantity'] == 1
        assert ctx.sdk.broker[1111] == 101
    else:
        assert ctx.sdk.propose_calls == ctx.sdk.approve_calls == []
        assert ctx.manager.intents.all(kind='OPEN') == []
        assert ctx.manager.state.open_position('orb_test', 1111) is None
        assert ctx.sdk.broker[1111] == 100
        rows = _signal_nonexecution_rows(ctx, work)
        assert len(rows) == 1
        row = rows[0]
        assert row[4] == 'skip'
        assert isinstance(row[5], str) and 'stale' in row[5].casefold()


def test_native_neutral_signal_keeps_its_noop_decision_after_reopen(signal_decision_owner):
    """NEUTRAL is a supported Action member and needs an auditable no-op."""
    ctx = signal_decision_owner
    work = make_work(action=Action.NEUTRAL)

    ctx.manager._process_signal(work)

    assert ctx.sdk.propose_calls == ctx.sdk.approve_calls == []
    assert ctx.manager.intents.all() == []
    assert ctx.manager.state.open_position('orb_test', 1111) is None
    assert ctx.sdk.broker[1111] == 100
    rows = _signal_nonexecution_rows(ctx, work)
    assert len(rows) == 1
    row = rows[0]
    assert row[:4] == ('orb_test', 1111, TS.to_pydatetime(), 'NEUTRAL')
    assert row[4] == 'skip'
    assert isinstance(row[5], str)
    assert all(fact in row[5].casefold() for fact in ('unsupported', 'action', 'neutral'))


@pytest.fixture
def native_submit_owner(tmp_path, monkeypatch):
    """Real SDK proposal/approval and executor journals across an offline RPC seam.

    The existing coordinated Trader supplies a fake broker transport, account,
    and quotes. Its native coordinator, receipt tracker and snapshot producer
    still decide what was submitted. The executor audit and proposal stores
    are real, isolated databases. No acceptance map or SDK result is invented.
    """
    import asyncio
    import reactivex as rx
    from review.test_review_order_contract import _coordinated_trader, _stock
    from trader.data.event_store import EventStore
    from trader.data.proposal_store import ProposalStore
    from trader.messaging.trader_service_api import TraderServiceApi
    from trader.sdk import MMR
    from trader.trading.order_lifecycle import OrderLifecycleTracker
    from trader.trading.position_sizing import PortfolioState, PositionSizingConfig
    from trader.trading.risk_gate import RiskGate, RiskLimits

    monkeypatch.delenv('MMR_AUTO_EXECUTE_DISABLED', raising=False)
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    config = PositionSizingConfig(
        base_position_usd=1000, min_position_usd=1, max_position_usd=10000,
        min_confidence_scale=0, volatility_adjustment=False)
    monkeypatch.setattr(PositionSizingConfig, 'load', staticmethod(lambda path=None: config))
    server = _coordinated_trader(tmp_path, held=100)
    manager = None
    try:
        # The lightweight coordinator uses an in-memory event list. Its
        # query compares naive cutoffs with aware lifecycle receipts and is
        # unsuitable once a filled order precedes another opening. Use the
        # real durable EventStore and its native timestamp/query semantics.
        previous_tracker = server.order_tracker
        previous_tracker.close(timeout=1)
        if previous_tracker._journal is not None:
            previous_tracker._journal.close()
        if previous_tracker._temporary is not None:
            previous_tracker._temporary.cleanup()
        server.event_store = EventStore(server.duckdb_path)
        server.order_tracker = OrderLifecycleTracker(server.event_store)
        server.risk_gate = RiskGate(RiskLimits(), server.event_store)
        server.require_proposal_approval = True
        api = TraderServiceApi(server)
        proposals = ProposalStore(server.duckdb_path)
        path = str(tmp_path / 'native_submit_executor.duckdb')
        event_path = str(tmp_path / 'native_submit_events.duckdb')
        events = EventStore(event_path)
        ctx = SimpleNamespace(
            server=server, api=api, proposals=proposals, events=events,
            path=path, event_path=event_path, contract=_stock(), owner='native_submit',
            rpc_calls=[])

        def place_expressive_order(**kwargs):
            ctx.rpc_calls.append(dict(kwargs))
            return asyncio.run(api.place_expressive_order(**kwargs))

        def execution_snapshot(**kwargs):
            return asyncio.run(api.execution_snapshot(**kwargs))

        rpc = SimpleNamespace(
            place_expressive_order=place_expressive_order,
            execution_snapshot=execution_snapshot,
            get_snapshot=lambda contract, generic: server.client.ib.tickers()[0],
            get_fx_rates=lambda: {'USD': 1.0},
            get_account_values=lambda: {},
            resolve_contract=lambda contract: (
                [ctx.contract] if contract.symbol == ctx.contract.symbol else []),
        )
        sdk = MMR.__new__(MMR)
        sdk._prop_store = proposals
        sdk._client = SimpleNamespace(is_setup=True, rpc=lambda **kwargs: rpc)
        # The universe and optional quote interfaces are external inputs. All
        # exact-contract checks, proposal metadata and approval remain native.
        sdk.resolve = lambda symbol, **kwargs: (
            [ctx.contract] if symbol in (ctx.contract.conId, ctx.contract.symbol) else [])
        sdk.snapshot = lambda *args, **kwargs: None
        sdk._get_portfolio_state = lambda: PortfolioState()
        manager = AutoExecutor(path, paper_trading=True, event_store=events,
                               cooldown_seconds=0, sdk_factory=lambda: sdk)

        def work(**kwargs):
            values = dict(strategy_name=ctx.owner, conid=ctx.contract.conId, quantity=4)
            values.update(kwargs)
            return make_work(**values)

        def fill(trade, quantity=None, *, status='Filled'):
            # This models the external broker fill, only for a Trade actually
            # returned by native placement. No execution identity is guessed.
            assert any(existing is trade for existing in server.placed)
            cumulative = float(trade.order.totalQuantity if quantity is None else quantity)
            prior = float(trade.orderStatus.filled or 0)
            assert prior <= cumulative <= float(trade.order.totalQuantity)
            server.inventory += (cumulative - prior) * (1 if trade.order.action == 'BUY' else -1)
            trade.order.filledQuantity = cumulative
            trade.orderStatus.filled = cumulative
            trade.orderStatus.remaining = float(trade.order.totalQuantity) - cumulative
            trade.orderStatus.avgFillPrice = 10
            trade.orderStatus.status = status
            server.order_tracker.on_trade(trade)
            assert server.order_tracker.flush(timeout=1)

        ctx.manager, ctx.sdk, ctx.rpc = manager, sdk, rpc
        ctx.work, ctx.fill = work, fill
        yield ctx
    finally:
        try:
            if manager is not None:
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


@pytest.fixture
def native_protection_owner(native_submit_owner, monkeypatch):
    """Extend the native submission fixture with real protective SDK/API paths."""
    import asyncio
    import reactivex as rx
    from trader.trading.book import BookSubject

    ctx = native_submit_owner
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '8')
    ctx.sdk._contract_map = {}
    ctx.protection_calls = []
    ctx.cancel_calls = []
    book = BookSubject()
    monkeypatch.setattr(ctx.server, 'book', book)
    place = ctx.server.client.subscribe_place_order.side_effect

    async def place_and_update_book(contract, order):
        result = await place(contract, order)
        for trade in ctx.server.placed:
            if trade.order.orderId == order.orderId:
                book.add_update_trade(trade)
                break
        return result

    monkeypatch.setattr(ctx.server.client.subscribe_place_order, 'side_effect', place_and_update_book)

    def place_standalone_order(**kwargs):
        ctx.protection_calls.append(dict(kwargs))
        return asyncio.run(ctx.api.place_standalone_order(**kwargs))

    def cancel_order(order_id):
        ctx.cancel_calls.append(order_id)
        # MMR.cancel returns its synchronous RPC result directly.
        return ctx.api.cancel_order(order_id)

    monkeypatch.setattr(ctx.rpc, 'place_standalone_order', place_standalone_order, raising=False)
    monkeypatch.setattr(ctx.rpc, 'cancel_order', cancel_order, raising=False)
    yield ctx


def _native_protection_intents(ctx, *, active=False):
    from trader.strategy.execution_intents import IntentStore

    reopened = IntentStore(ctx.path)
    try:
        return reopened.all(strategy=ctx.owner, conid=ctx.contract.conId,
                            kind='PROTECTIVE', active=active)
    finally:
        reopened.journal.close()


def _native_open_with_protection(ctx, *, quantity=40):
    ctx.manager._process_signal(ctx.work(quantity=quantity))
    assert len(ctx.server.placed) == 1
    opening, = ctx.server.placed
    assert opening.order.action == 'BUY' and opening.order.totalQuantity == quantity
    ctx.fill(opening)
    ctx.manager.manage_positions()
    assert len(ctx.server.placed) == 2
    opening, stop = ctx.server.placed
    assert stop.order.orderType == 'STP' and stop.order.action == 'SELL'
    assert stop.order.totalQuantity == quantity
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position['quantity'] == quantity and position['protective_order_id'] == stop.order.orderId
    protective_records = _native_protection_intents(ctx, active=True)
    assert len(protective_records) == 1
    protective, = protective_records
    assert protective['status'] == 'WORKING'
    assert protective['payload']['order_ids'] == [stop.order.orderId]
    assert protective['payload']['ownership_epoch'] == position['ownership_epoch']
    assert ctx.server.inventory == 100 + quantity
    return stop, protective


def _native_submit_read_intents(ctx, *, kind='OPEN'):
    from trader.strategy.execution_intents import IntentStore

    reopened = IntentStore(ctx.path)
    try:
        return reopened.all(strategy=ctx.owner, conid=ctx.contract.conId, kind=kind)
    finally:
        reopened.journal.close()


@pytest.mark.parametrize(
    'failure,quantity,cause',
    [('operator', 4, 'reject'), ('operator', 0, 'reject'),
     ('margin', 4, 'refus'), ('risk', 4, 'risk gate')],
    ids=['operator-sized', 'operator-auto-sized', 'margin-unavailable', 'risk-open-order-cap'])
def test_native_submission_refusal_retains_phase_direction_quantity_and_cause(
        native_submit_owner, monkeypatch, failure, quantity, cause):
    from trader.data.event_store import EventStore, EventType
    from trader.trading.risk_gate import RiskInputs

    ctx = native_submit_owner
    if failure == 'operator':
        propose = ctx.sdk.propose

        def propose_then_reject(**kwargs):
            result = propose(**kwargs)
            assert ctx.sdk.reject(result[0], 'operator declined this proposal')
            return result

        monkeypatch.setattr(ctx.sdk, 'propose', propose_then_reject)
    elif failure == 'margin':
        async def unavailable_margin(*args, **kwargs):
            return {}

        monkeypatch.setattr(ctx.server, 'check_order_margin', unavailable_margin)
    else:
        ctx.server.risk_gate.limits.max_open_orders = 1
        monkeypatch.setattr(ctx.server, 'gather_risk_inputs', lambda: RiskInputs(
            open_order_count=1, daily_pnl=0, daily_pnl_evaluable=True,
            portfolio_value=100000, portfolio_value_evaluable=True))

    work = ctx.work(quantity=quantity)
    ctx.manager._process_signal(work)

    intent, = _native_submit_read_intents(ctx)
    proposal = ctx.proposals.get(intent['payload']['proposal_id'])
    assert intent['status'] == 'REJECTED'
    error = intent['payload']['error']
    assert isinstance(error, str) and cause in error.lower()
    assert proposal.status == ('REJECTED' if failure == 'operator' else 'FAILED')
    if quantity == 0:
        assert proposal.quantity is None and proposal.amount > 0
    else:
        assert proposal.quantity == quantity
    assert ctx.server.placed == [] and ctx.server.inventory == 100
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None

    state = AutoExecState(ctx.path)
    rows = state.db.execute(
        'SELECT strategy, conid, action, decision, reason FROM auto_exec_bar_log '
        'WHERE strategy=? AND conid=? AND bar_ts=?',
        [ctx.owner, ctx.contract.conId, work.bar_ts.to_pydatetime()], fetch='all')
    assert rows == [(ctx.owner, ctx.contract.conId, 'BUY', 'open_failed', error)]
    assert not state.executed_for_bar(ctx.owner, ctx.contract.conId, work.bar_ts)
    rejected = EventStore(ctx.event_path).query_since(
        dt.datetime(2000, 1, 1), event_type=EventType.ORDER_REJECTED)
    assert len(rejected) == 1
    event, = rejected
    assert event.strategy_name == ctx.owner and event.conid == ctx.contract.conId
    assert event.action == 'BUY' and event.quantity == quantity
    assert event.metadata['note'] == error


def test_native_submission_receipt_survives_unavailable_scoped_replay(native_submit_owner, monkeypatch):
    """A received order ID is durable evidence of submission, not of a fill."""
    ctx = native_submit_owner
    place = ctx.rpc.place_expressive_order
    snapshot = ctx.rpc.execution_snapshot
    failed_scoped_reads = []

    def unavailable_scoped_snapshot(**kwargs):
        if kwargs.get('intent_id'):
            failed_scoped_reads.append(dict(kwargs))
            raise ConnectionError('intent-scoped receipt replay temporarily unavailable')
        return snapshot(**kwargs)

    def place_then_delay_scoped_replay(**kwargs):
        result = place(**kwargs)
        monkeypatch.setattr(ctx.rpc, 'execution_snapshot', unavailable_scoped_snapshot)
        return result

    monkeypatch.setattr(ctx.rpc, 'place_expressive_order', place_then_delay_scoped_replay)
    work = ctx.work()
    ctx.manager._process_signal(work)

    intent, = _native_submit_read_intents(ctx)
    trade, = ctx.server.placed
    proposal = ctx.proposals.get(intent['payload']['proposal_id'])
    assert failed_scoped_reads
    assert intent['status'] == 'WORKING'
    assert intent['payload']['order_ids'] == [trade.order.orderId]
    assert proposal.status == 'EXECUTED' and proposal.order_ids == [trade.order.orderId]
    assert trade.orderStatus.status == 'Submitted' and trade.orderStatus.filled == 0
    assert ctx.server.inventory == 100
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None

    state = AutoExecState(ctx.path)
    row, = state.db.execute(
        'SELECT action, decision, reason FROM auto_exec_bar_log '
        'WHERE strategy=? AND conid=? AND bar_ts=?',
        [ctx.owner, ctx.contract.conId, work.bar_ts.to_pydatetime()], fetch='all')
    assert row[:2] == ('BUY', 'open')
    assert isinstance(row[2], str)
    assert f"proposal #{proposal.id}" in row[2] and 'awaiting fills' in row[2]
    assert state.executed_for_bar(ctx.owner, ctx.contract.conId, work.bar_ts)
    assert state.count_opens_since(ctx.owner, dt.datetime(2000, 1, 1)) == 1

    # Returning history does not itself invent a fill. Only the subsequent
    # actual placed Trade callback increases attributed inventory.
    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', snapshot)
    ctx.manager.manage_positions()
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
    ctx.fill(trade)
    ctx.manager.manage_positions()
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 4
    assert ctx.server.inventory == 104 and len(ctx.server.placed) == 1


def test_native_uncertain_submission_result_retains_canonical_unknown(native_submit_owner, monkeypatch):
    """A failed placement transport is converted by the real SDK into UNKNOWN."""
    from trader.data.event_store import EventStore, EventType

    ctx = native_submit_owner
    attempts = []

    def lose_placement_reply(**kwargs):
        attempts.append(dict(kwargs))
        raise ConnectionError('connection lost while returning the placement reply')

    monkeypatch.setattr(ctx.rpc, 'place_expressive_order', lose_placement_reply)
    work = ctx.work()
    ctx.manager._process_signal(work)

    intent, = _native_submit_read_intents(ctx)
    proposal = ctx.proposals.get(intent['payload']['proposal_id'])
    assert len(attempts) == 1
    assert intent['status'] == 'UNKNOWN'
    assert proposal.status == 'APPROVED'
    assert intent['payload'].get('order_ids', []) == []
    error = intent['payload']['error']
    assert isinstance(error, str) and 'UNKNOWN:' in error
    assert 'connection lost while returning the placement reply' in error
    rows = AutoExecState(ctx.path).db.execute(
        'SELECT action, decision, reason FROM auto_exec_bar_log '
        'WHERE strategy=? AND conid=? AND bar_ts=?',
        [ctx.owner, ctx.contract.conId, work.bar_ts.to_pydatetime()], fetch='all')
    assert rows == [('BUY', 'open_failed', error)]
    assert EventStore(ctx.event_path).query_since(
        dt.datetime(2000, 1, 1), event_type=EventType.ORDER_REJECTED) == []
    assert ctx.server.placed == [] and ctx.server.inventory == 100


def _submit_middle_reopened_intents(ctx):
    """Read committed execution state through a fresh journal owner."""
    from trader.strategy.execution_intents import IntentStore

    store = IntentStore(ctx.path)
    try:
        return store.all(kind='OPEN')
    finally:
        store.journal.close()


def _submit_middle_runtime_work(ctx, tmp_path, monkeypatch, *, live):
    from review.test_review_strategy_contract import runtime_and_strategy

    runtime, _ = runtime_and_strategy(tmp_path)
    strategy = runtime.get_strategy('review')
    strategy.ctx.auto_execute = True
    strategy.enable()
    runtime._grant_generation(strategy)
    ctx.manager.authority_check = runtime._opening_authorized
    ctx.manager.paper_trading = not live
    monkeypatch.setenv('MMR_AUTO_EXECUTE_LIVE', '1')
    monkeypatch.setenv('MMR_STALE_BAR_MULTIPLE', '3')
    work = make_work(strategy_name='review', quantity=4,
                     deployment_generation=strategy.ctx.deployment_generation)
    return runtime, strategy, work


def _submit_middle_close_runtime(runtime):
    for strategy in runtime.strategy_implementations:
        runtime._stop_callback_worker(strategy)
    pool = getattr(runtime, '_history_pool', None)
    if pool is not None:
        pool.shutdown(wait=True, cancel_futures=True)


@pytest.mark.parametrize('live', [False, True], ids=['paper', 'double-armed-live'])
def test_submission_rechecks_the_actual_current_deployment_generation(
        signal_decision_owner, tmp_path, monkeypatch, live):
    """Native runtime authority, real stores, historical external SDK adapter.

    The mode flag exercises local policy only; this fixture has no network
    connection and makes no native server-approval or real-account claim.
    """
    ctx = signal_decision_owner
    runtime, strategy, work = _submit_middle_runtime_work(
        ctx, tmp_path, monkeypatch, live=live)
    try:
        assert runtime._opening_authorized(strategy.name, work.deployment_generation)
        ctx.manager._process_signal(work)

        rows = _submit_middle_reopened_intents(ctx)
        assert len(rows) == 1
        opening, = rows
        assert opening['strategy'] == strategy.name and opening['conid'] == work.conid
        assert opening['status'] == 'FILLED'
        assert opening['payload']['deployment_generation'] == work.deployment_generation
        assert len(ctx.sdk.propose_calls) == len(ctx.sdk.approve_calls) == 1
        assert ctx.sdk.propose_calls[0]['metadata']['client_intent_id'] == opening['intent_id']
        assert ctx.manager.state.open_position(strategy.name, work.conid)['quantity'] == 4
        assert ctx.sdk.broker[work.conid] == 104
    finally:
        _submit_middle_close_runtime(runtime)


@pytest.mark.parametrize('revocation', ['generation', 'kill-switch', 'live-arm'])
def test_submission_retains_the_cause_when_authority_changes_during_proposal(
        signal_decision_owner, tmp_path, monkeypatch, revocation):
    ctx = signal_decision_owner
    runtime, strategy, work = _submit_middle_runtime_work(
        ctx, tmp_path, monkeypatch, live=revocation == 'live-arm')
    adapter_propose = ctx.sdk.propose

    def change_authority_after_proposal(**kwargs):
        result = adapter_propose(**kwargs)
        if revocation == 'generation':
            runtime._revoke_opening(strategy)
        elif revocation == 'kill-switch':
            monkeypatch.setenv('MMR_AUTO_EXECUTE_DISABLED', '1')
        else:
            monkeypatch.setenv('MMR_AUTO_EXECUTE_LIVE', '0')
        return result

    monkeypatch.setattr(ctx.sdk, 'propose', change_authority_after_proposal)
    try:
        assert runtime._opening_authorized(strategy.name, work.deployment_generation)
        ctx.manager._process_signal(work)

        rows = _submit_middle_reopened_intents(ctx)
        assert len(rows) == 1
        opening, = rows
        assert opening['status'] == 'REJECTED'
        assert opening['payload']['proposal_id'] == next(iter(ctx.sdk.proposals))
        cause = opening['payload'].get('error')
        assert isinstance(cause, str)
        assert all(fact in cause.casefold() for fact in ('opening', 'authority', 'revoked', 'approval'))
        assert len(ctx.sdk.propose_calls) == 1 and ctx.sdk.approve_calls == []
        assert ctx.manager.state.open_position(strategy.name, work.conid) is None
        assert ctx.sdk.broker[work.conid] == 100
    finally:
        _submit_middle_close_runtime(runtime)


def _submit_middle_partial_native_rejection(ctx, monkeypatch):
    """A real placed Trade fills partly before IB reports its terminal refusal.

    Only the external broker callback schedule changes. The SDK, service API,
    server journal, failure envelope and executor reconciliation remain native.
    """
    place = ctx.server.client.subscribe_place_order.side_effect

    async def place_then_partially_reject(contract, order):
        response = await place(contract, order)
        trade = next(trade for trade in ctx.server.placed if trade.order is order)
        ctx.fill(trade, 10, status='Inactive')
        return response

    monkeypatch.setattr(ctx.server.client.subscribe_place_order, 'side_effect',
                        place_then_partially_reject)


def test_native_approval_entry_read_failure_retains_unknown_intent_and_cause(
        native_submit_owner, monkeypatch, caplog):
    """Failure at approval entry is not proof of an unattempted approval.

    This specific control has not sent an order. It pins the executor's
    conservative boundary without inventing a prior broker submission or
    depending on an SDK bookkeeping exception escaping after placement.
    """
    ctx = native_submit_owner
    propose = ctx.sdk.propose
    get = ctx.proposals.get
    unavailable = OSError('proposal journal temporarily unreadable')
    observed_reads = []

    def unavailable_proposal_read(proposal_id):
        observed_reads.append(proposal_id)
        raise unavailable

    def propose_then_lose_read(**kwargs):
        result = propose(**kwargs)
        monkeypatch.setattr(ctx.proposals, 'get', unavailable_proposal_read)
        return result

    monkeypatch.setattr(ctx.sdk, 'propose', propose_then_lose_read)
    with caplog.at_level(logging.ERROR, logger=auto_executor_module.logging.name):
        ctx.manager._process_signal(ctx.work())
    monkeypatch.setattr(ctx.proposals, 'get', get)
    monkeypatch.setattr(ctx.sdk, 'propose', propose)

    intent, = _submit_middle_reopened_intents(ctx)
    proposal_id = intent['payload']['proposal_id']
    assert observed_reads == [proposal_id]
    assert intent['status'] == 'UNKNOWN'
    cause = intent['payload'].get('error')
    assert isinstance(cause, str) and str(unavailable) in cause
    assert get(proposal_id).status == 'PENDING'
    assert ctx.rpc_calls == [] and ctx.server.placed == []
    assert ctx.server.inventory == 100
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
    records = [record for record in caplog.records
               if record.name == auto_executor_module.logging.name
               and record.exc_info and record.exc_info[1] is unavailable]
    assert records and all(record.exc_info[2] is not None for record in records)
    assert any(intent['intent_id'] in record.getMessage() for record in records)

    # A new, still-fresh bar cannot turn absent approval evidence into another
    # opening. This is independent of the original bar's execution dedup.
    ctx.manager._process_signal(ctx.work(bar_ts=TS + pd.Timedelta(seconds=1)))
    assert [row['intent_id'] for row in _submit_middle_reopened_intents(ctx)] == [intent['intent_id']]
    assert [proposal.id for proposal in ctx.proposals.query()] == [proposal_id]
    assert ctx.server.placed == []


@pytest.mark.parametrize('scoped_replay_available', [True, False],
                         ids=['replayed-partial-fill', 'scoped-replay-unavailable'])
def test_native_partial_rejection_preserves_fill_lifetime_and_replay_reservation(
        native_submit_owner, monkeypatch, scoped_replay_available):
    ctx = native_submit_owner
    _submit_middle_partial_native_rejection(ctx, monkeypatch)
    snapshot = ctx.rpc.execution_snapshot
    failed_scoped_reads = []

    def scoped_transport_failure(**kwargs):
        if kwargs.get('intent_id'):
            failed_scoped_reads.append(dict(kwargs))
            raise ConnectionError('intent-scoped history temporarily unavailable')
        return snapshot(**kwargs)

    if not scoped_replay_available:
        monkeypatch.setattr(ctx.rpc, 'execution_snapshot', scoped_transport_failure)
    ctx.manager._process_signal(ctx.work(quantity=40))

    intent, = _submit_middle_reopened_intents(ctx)
    trade, = ctx.server.placed
    assert trade.order.action == 'BUY' and trade.order.totalQuantity == 40
    assert trade.orderStatus.filled == 10 and ctx.server.inventory == 110
    cause = intent['payload'].get('error')
    assert isinstance(cause, str)
    assert 'unknown' in cause.casefold() and 'rejected' in cause.casefold()
    assert ctx.proposals.get(intent['payload']['proposal_id']).status == 'APPROVED'
    if scoped_replay_available:
        assert intent['status'] == 'CANCELLED'
        assert intent['payload']['order_ids'] == [trade.order.orderId]
        assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 10
    else:
        assert failed_scoped_reads
        assert intent['status'] == 'UNKNOWN'
        assert not intent['payload'].get('order_ids')
        assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None

    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', snapshot)
    ctx.manager.manage_positions()
    recovered, = _submit_middle_reopened_intents(ctx)
    assert recovered['intent_id'] == intent['intent_id']
    assert recovered['status'] == 'CANCELLED'
    assert recovered['payload']['order_ids'] == [trade.order.orderId]
    assert recovered['payload']['error'] == cause
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 10
    ctx.manager.manage_positions()
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 10
    assert len(ctx.rpc_calls) == len(ctx.server.placed) == 1 and ctx.server.inventory == 110


def test_native_failed_submission_commit_survives_restart_before_reconciliation(
        native_submit_owner, monkeypatch):
    """Stop between two durable operations, then reopen the actual executor.

    The sentinel represents process interruption, not an invented broker or
    storage result. It does not inspect the status whose persistence is tested.
    """
    ctx = native_submit_owner
    _submit_middle_partial_native_rejection(ctx, monkeypatch)
    reconcile = ctx.manager._reconcile_intent

    class StopBeforeReplay(BaseException):
        pass

    def stop_after_submission_result(intent):
        if intent['kind'] == 'OPEN' and ctx.server.placed:
            raise StopBeforeReplay()
        return reconcile(intent)

    monkeypatch.setattr(ctx.manager, '_reconcile_intent', stop_after_submission_result)
    with pytest.raises(StopBeforeReplay):
        ctx.manager._process_signal(ctx.work(quantity=40))
    ctx.manager.intents.journal.close()
    restarted = AutoExecutor(ctx.path, paper_trading=True, event_store=ctx.events,
                             cooldown_seconds=0, sdk_factory=lambda: ctx.sdk)
    try:
        intent, = _submit_middle_reopened_intents(ctx)
        trade, = ctx.server.placed
        assert intent['status'] == 'UNKNOWN'
        assert restarted.status_metrics()['unknown_intents'] == 1
        assert restarted.status_metrics()['pending_intents'] == 1
        assert isinstance(intent['payload'].get('error'), str)
        assert 'rejected' in intent['payload']['error'].casefold()
        assert trade.orderStatus.filled == 10 and ctx.server.inventory == 110
        assert restarted.state.open_position(ctx.owner, ctx.contract.conId) is None

        restarted.manage_positions()
        recovered, = _submit_middle_reopened_intents(ctx)
        assert recovered['intent_id'] == intent['intent_id'] and recovered['status'] == 'CANCELLED'
        assert recovered['payload']['order_ids'] == [trade.order.orderId]
        assert restarted.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 10
        restarted.manage_positions()
        assert restarted.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 10
        assert len(ctx.rpc_calls) == len(ctx.server.placed) == 1 and ctx.server.inventory == 110
    finally:
        restarted.intents.journal.close()


def test_native_competing_approval_remains_pending_until_its_actual_receipt(
        native_submit_owner, monkeypatch):
    """Model a real compare-and-swap winner, without fabricating its row."""
    ctx = native_submit_owner
    transition = ctx.proposals.try_transition
    competing_winner = []

    def another_approver_claims_first(proposal_id, from_status, to_status, **kwargs):
        if from_status == 'PENDING' and to_status == 'APPROVED' and not competing_winner:
            assert transition(proposal_id, from_status, to_status, **kwargs)
            competing_winner.append(proposal_id)
        return transition(proposal_id, from_status, to_status, **kwargs)

    monkeypatch.setattr(ctx.proposals, 'try_transition', another_approver_claims_first)
    ctx.manager._process_signal(ctx.work())
    monkeypatch.setattr(ctx.proposals, 'try_transition', transition)

    intent, = _submit_middle_reopened_intents(ctx)
    proposal_id = intent['payload']['proposal_id']
    assert competing_winner == [proposal_id]
    assert ctx.proposals.get(proposal_id).status == 'APPROVED'
    assert intent['status'] == 'UNKNOWN'
    assert 'claimed' in intent['payload']['error'].casefold()
    assert ctx.server.placed == [] and ctx.rpc_calls == []

    ctx.manager._process_signal(ctx.work(bar_ts=TS + pd.Timedelta(seconds=1)))
    assert [row['intent_id'] for row in _submit_middle_reopened_intents(ctx)] == [intent['intent_id']]
    assert [proposal.id for proposal in ctx.proposals.query()] == [proposal_id]
    assert ctx.server.inventory == 100 and ctx.server.placed == []

    # The winning APPROVED row resumes through the actual SDK/API/server.
    # A submission receipt alone still owns no inventory; the fill supplies it.
    reply = ctx.sdk.approve(proposal_id, resume=True)
    assert reply.is_success(), reply.error
    trade, = ctx.server.placed
    assert reply.obj == [trade.order.orderId]
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
    ctx.fill(trade)
    ctx.manager.manage_positions()
    completed, = _submit_middle_reopened_intents(ctx)
    assert completed['intent_id'] == intent['intent_id'] and completed['status'] == 'FILLED'
    assert completed['payload']['order_ids'] == [trade.order.orderId]
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 4
    assert ctx.server.inventory == 104 and len(ctx.server.placed) == 1


@pytest.mark.parametrize('security_type', ['STK', 'OPT'], ids=['stock', 'option'])
def test_native_submission_preserves_exact_typed_identity_and_open_audit(
        native_submit_owner, monkeypatch, security_type):
    """The numeric identity must survive a genuinely ambiguous symbol lookup."""
    import copy
    from ib_async import Option

    ctx = native_submit_owner
    if security_type == 'OPT':
        ctx.contract = Option('AUDIT', '20261218', 200, 'C', 'SMART',
                              multiplier='100', currency='USD', conId=200)
    alias = copy.copy(ctx.contract)
    alias.conId += 1000
    resolve = ctx.sdk.resolve

    def catalog(symbol, **kwargs):
        definitions = resolve(symbol, **kwargs)
        if symbol == ctx.contract.symbol:
            return definitions + [alias]
        return definitions

    # Both catalogue rows have native contract shapes and matching hints. The
    # executor's round trip includes the exact conId; SDK approval must retain
    # that identity rather than falling back to this ambiguous symbol.
    monkeypatch.setattr(ctx.sdk, 'resolve', catalog)
    work = ctx.work()
    ctx.manager._process_signal(work)

    opening, = ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN')
    proposal = ctx.proposals.get(opening['payload']['proposal_id'])
    assert proposal.status == 'EXECUTED'
    assert proposal.sec_type == security_type
    assert proposal.metadata['con_id'] == ctx.contract.conId
    assert proposal.metadata['bar_ts'] == work.bar_ts.replace(tzinfo=dt.timezone.utc).isoformat()
    assert 'open' in proposal.reasoning.lower().split() and ctx.owner in proposal.reasoning
    detail = ctx.sdk.proposal_detail(proposal.id)
    assert detail['sec_type'] == security_type
    assert detail['untrusted']['metadata']['bar_ts'] == work.bar_ts.replace(tzinfo=dt.timezone.utc).isoformat()
    trade, = ctx.server.placed
    assert trade.contract.conId == ctx.contract.conId and trade.contract.secType == security_type
    assert trade.order.action == 'BUY' and trade.order.totalQuantity == 4
    assert proposal.order_ids == [trade.order.orderId]
    assert trade.orderStatus.status == 'Submitted' and trade.orderStatus.filled == 0
    assert ctx.server.inventory == 100


def test_native_close_proposal_retains_structured_bar_and_exit_cause(native_submit_owner):
    """An ordinary SELL after a real owned fill retains its public audit facts."""
    ctx = native_submit_owner
    ctx.manager._process_signal(ctx.work())
    entry, = ctx.server.placed
    ctx.fill(entry)
    ctx.manager.manage_positions()
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 4
    assert ctx.server.inventory == 104

    work = ctx.work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1))
    ctx.manager._process_signal(work)
    closing, = ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='CLOSE')
    proposal = ctx.proposals.get(closing['payload']['proposal_id'])
    assert proposal.status == 'EXECUTED' and proposal.action == 'SELL'
    assert proposal.quantity == 4
    assert proposal.metadata['bar_ts'] == work.bar_ts.replace(tzinfo=dt.timezone.utc).isoformat()
    assert proposal.metadata['close_reason'] == closing['payload']['reason']
    assert 'SELL' in proposal.metadata['close_reason']
    assert 'close' in proposal.reasoning.lower().split() and ctx.owner in proposal.reasoning
    detail = ctx.sdk.proposal_detail(proposal.id)
    assert detail['untrusted']['metadata']['close_reason'] == closing['payload']['reason']
    exit_trade, = [trade for trade in ctx.server.placed if trade.order.action == 'SELL']
    assert exit_trade.order.totalQuantity == 4 and exit_trade.orderStatus.filled == 0
    assert proposal.order_ids == [exit_trade.order.orderId]
    assert ctx.server.inventory == 104

    ctx.fill(exit_trade)
    ctx.manager.manage_positions()
    assert ctx.server.inventory == 100
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None


def test_native_unqualified_protective_cannot_alias_instrument_one(native_submit_owner, monkeypatch):
    """Defensive native input: an incomplete contract retains its explicit zero.

    This does not claim that a qualified placement normally emits conId zero.
    The native tracker accepts that incomplete callback; global observation
    must not turn it into authority over the distinct positive instrument.
    """
    from ib_async import Order, OrderStatus, Stock, Trade

    ctx = native_submit_owner
    trades = []
    for conid, order_id, permanent_id in [(0, 61, 10001), (1, 62, 10002)]:
        contract = Stock('OBSERVED', 'SMART', 'USD', conId=conid)
        order = Order(
            orderId=order_id, clientId=7, permId=permanent_id,
            account=ctx.server.ib_account, action='SELL', totalQuantity=4,
            orderType='STP', auxPrice=9, orderRef=ctx.owner)
        trade = Trade(contract, order, OrderStatus(
            orderId=order_id, clientId=7, permId=permanent_id,
            status='Submitted', remaining=4))
        trades.append(trade)
        ctx.server.order_tracker.on_trade(trade)
    assert ctx.server.order_tracker.flush(timeout=1)

    async def observed_open_orders():
        return trades

    monkeypatch.setattr(ctx.server.client.ib, 'reqOpenOrdersAsync', observed_open_orders)
    snapshot = ctx.sdk.execution_snapshot()
    assert snapshot['complete'] and snapshot['positions_complete']
    observed = {row['orderId']: row for row in snapshot['orders']}
    assert set(observed) == {61, 62}
    assert (observed[61]['conId'], observed[61]['permId']) == (0, 10001)
    assert (observed[62]['conId'], observed[62]['permId']) == (1, 10002)
    assert all(row['orderRef'] == ctx.owner for row in observed.values())
    assert ctx.manager._own_live_protectives(ctx.sdk, ctx.owner, 1) == [62]
    assert ctx.server.placed == [] and ctx.server.inventory == 100


def test_native_zero_session_id_cannot_alias_order_one(native_submit_owner):
    """Completed manual history can keep a permanent ID and omit session ID.

    The global snapshot legitimately includes both rows. A missing reference
    permits a numeric fallback only to the row's actual numeric identifier.
    """
    from ib_async import Order, OrderStatus, Trade

    ctx = native_submit_owner
    trades = []
    for order_id, permanent_id, client_id in [(0, 11001, 0), (1, 11002, 7)]:
        order = Order(
            orderId=order_id, clientId=client_id, permId=permanent_id,
            account=ctx.server.ib_account, action='BUY', totalQuantity=4,
            filledQuantity=4, orderType='MKT', orderRef='')
        # completedOrder omits the session/client IDs and status quantities.
        # The positive-ID control is an ordinary filled trade callback.
        status = (OrderStatus(status='Filled') if order_id == 0 else OrderStatus(
            orderId=order_id, clientId=client_id, permId=permanent_id,
            status='Filled', filled=4, remaining=0))
        trade = Trade(ctx.contract, order, status)
        trades.append(trade)
        ctx.server.order_tracker.on_trade(trade, completed=order_id == 0)
    assert ctx.server.order_tracker.flush(timeout=1)
    snapshot = ctx.sdk.execution_snapshot()
    assert snapshot['complete'] and snapshot['positions_complete']
    observed = {row['permId']: row for row in snapshot['orders']}
    assert set(observed) == {11001, 11002}
    assert observed[11001]['orderId'] == 0 and observed[11002]['orderId'] == 1
    assert all(row['clientIntentId'] == '' and row['conId'] == ctx.contract.conId
               for row in observed.values())
    assert all(row['status'] == 'Filled' and row['fillQuantityKnown'] is True
               for row in observed.values())
    selected = [row for row in snapshot['orders']
                if ctx.manager._matches_intent_order(row, 'unobserved-local-intent', {1})]
    assert [(row['orderId'], row['permId']) for row in selected] == [(1, 11002)]
    assert ctx.server.placed == [] and ctx.server.inventory == 100


def _install_native_exact_catalogue(ctx, tmp_path, monkeypatch, contracts):
    """Real local universe and SDK resolution through an offline RPC adapter."""
    import asyncio
    import reactivex as rx
    from ib_async import ContractDetails, Ticker
    from trader.data.data_access import SecurityDefinition
    from trader.data.universe import Universe, UniverseAccessor
    from trader.sdk import MMR

    accessor = UniverseAccessor(str(tmp_path / 'exact_catalogue.duckdb'), 'exact')
    definitions = [SecurityDefinition.from_contract_details(ContractDetails(contract=c))
                   for c in contracts]
    accessor.update(Universe('exact', definitions))
    monkeypatch.setattr(ctx.server, 'universe_accessor', accessor, raising=False)
    monkeypatch.setattr(ctx.sdk, 'resolve', MMR.resolve.__get__(ctx.sdk, MMR))
    monkeypatch.setattr(ctx.sdk, 'snapshot', MMR.snapshot.__get__(ctx.sdk, MMR))

    def resolve_symbol(symbol, exchange='', universe='', sec_type=''):
        return asyncio.run(ctx.api.resolve_symbol(symbol, exchange, universe, sec_type))

    quoted = []

    def get_snapshot(contract, delayed):
        quoted.append(contract)
        price = 10.0 if contract.conId == 100 else 20.0
        ticker = Ticker(contract=contract)
        # Native initialization clears quote fields; the external quote arrives afterward.
        ticker.last, ticker.bid, ticker.ask = price, price - 1, price + 1
        return ticker

    monkeypatch.setattr(ctx.rpc, 'resolve_symbol', resolve_symbol, raising=False)
    monkeypatch.setattr(ctx.rpc, 'get_snapshot', get_snapshot)
    # This local-catalogue test does not discover or qualify an external listing.
    # Missing exact local matches remain missing at this explicit RPC seam.
    monkeypatch.setattr(ctx.rpc, 'resolve_contract', lambda contract: [])
    return SimpleNamespace(accessor=accessor, definitions=definitions,
                           resolve_symbol=resolve_symbol, quoted=quoted)


@pytest.mark.parametrize('use_primary', [False, True],
                         ids=['direct-exchange', 'primary-exchange'])
def test_native_exact_venue_keeps_distinct_listing_quote_in_proposal(
        native_submit_owner, tmp_path, monkeypatch, use_primary):
    from ib_async import Contract

    ctx = native_submit_owner
    # SecurityDefinition.from_contract_details preserves the optional empty
    # primary venue. Both definitions use ordinary native Contract members.
    # The local universe supports distinct listings sharing symbol/currency.
    first = Contract(conId=100, symbol='DUAL', secType='STK', currency='USD',
                     exchange='SMART' if use_primary else 'NYSE',
                     primaryExchange='NYSE' if use_primary else '')
    second = Contract(conId=200, symbol='DUAL', secType='STK', currency='USD',
                      exchange='SMART' if use_primary else 'NASDAQ',
                      primaryExchange='NASDAQ' if use_primary else '')
    catalogue = _install_native_exact_catalogue(ctx, tmp_path, monkeypatch, [first, second])
    ident = ctx.manager._resolve_exact(100)
    proposal_id, _leverage, snapshot = ctx.sdk.propose(
        symbol=ident['symbol'], action='BUY', quantity=4,
        metadata={'con_id': 100}, sec_type=ident['sec_type'],
        exchange=ident['exchange'], currency=ident['currency'])
    proposal = ctx.proposals.get(proposal_id)

    assert snapshot is not None
    assert snapshot['last'] == 10.0
    assert proposal.metadata['snapshot']['last'] == 10.0
    assert proposal.exchange == 'NYSE'
    assert proposal.currency == 'USD'
    assert [contract.conId for contract in catalogue.quoted] == [100]
    assert proposal.status == 'PENDING'
    assert ctx.server.placed == []


def test_native_exact_cash_type_survives_durable_proposal_and_resolution(
        native_submit_owner, tmp_path, monkeypatch):
    from ib_async import Contract

    ctx = native_submit_owner
    cash = Contract(conId=100, symbol='EUR', secType='CASH', currency='USD',
                    exchange='IDEALPRO', primaryExchange='')
    _install_native_exact_catalogue(ctx, tmp_path, monkeypatch, [cash])
    ident = ctx.manager._resolve_exact(100)
    proposal_id, _leverage, _snapshot = ctx.sdk.propose(
        symbol=ident['symbol'], action='BUY', quantity=4,
        metadata={'con_id': 100}, sec_type=ident['sec_type'],
        exchange=ident['exchange'], currency=ident['currency'])
    proposal = ctx.proposals.get(proposal_id)

    assert proposal.sec_type == 'CASH'
    resolved = ctx.sdk._resolve_contract(
        proposal.metadata['con_id'], sec_type=proposal.sec_type,
        exchange=proposal.exchange, currency=proposal.currency)
    assert resolved.conId == 100
    assert resolved.secType == 'CASH'
    assert resolved.exchange == 'IDEALPRO'
    assert proposal.status == 'PENDING'
    assert ctx.server.placed == []


def test_native_catalogue_change_keeps_precision_refusal_facts(
        native_submit_owner, tmp_path, monkeypatch):
    from ib_async import Contract, ContractDetails
    from trader.data.data_access import SecurityDefinition
    from trader.data.universe import Universe

    ctx = native_submit_owner
    first = Contract(conId=100, symbol='DUAL', secType='STK', currency='USD',
                     exchange='SMART', primaryExchange='NYSE')
    replacement = Contract(conId=200, symbol='DUAL', secType='STK', currency='USD',
                           exchange='SMART', primaryExchange='NYSE')
    catalogue = _install_native_exact_catalogue(ctx, tmp_path, monkeypatch, [first])
    replacement_definition = SecurityDefinition.from_contract_details(
        ContractDetails(contract=replacement))
    original_rpc = catalogue.resolve_symbol

    def replace_after_integer_lookup(symbol, exchange='', universe='', sec_type=''):
        # The result is produced by the real server/accessor before a normal
        # durable universe replacement invalidates its resolver cache.
        result = original_rpc(symbol, exchange, universe, sec_type)
        if type(symbol) is int:
            catalogue.accessor.update(Universe('exact', [replacement_definition]))
        return result

    monkeypatch.setattr(ctx.rpc, 'resolve_symbol', replace_after_integer_lookup)
    with pytest.raises(AutoExecutionError) as caught:
        ctx.manager._resolve_exact(100)

    message = str(caught.value)
    assert all(fact in message for fact in ('DUAL', 'NYSE', 'USD', '100', '200'))
    assert 'precision' in message and 'round-trip' in message
    assert [definition.conId for definition in catalogue.accessor.resolve_symbol('DUAL')] == [200]
    assert ctx.server.placed == []


@pytest.mark.parametrize('broker_status', ['Cancelled', 'Submitted'],
                         ids=['already-terminal', 'cancel-during-repair'])
def test_native_terminal_protection_waits_for_fill_checkpoint(
        native_protection_owner, monkeypatch, broker_status):
    """Terminal status alone cannot authorize replacing stale owned quantity.

    The external broker has sold 30 of 40 owned shares. Its remaining 110
    shares include 100 manual shares. Only the intent-scoped history endpoint
    fails; global and order-ID-only reads continue returning native receipts.
    """
    ctx = native_protection_owner
    stop, protective = _native_open_with_protection(ctx)
    ctx.fill(stop, 30, status=broker_status)
    assert ctx.server.inventory == 110
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 40
    snapshot = ctx.rpc.execution_snapshot
    reads = []

    def unavailable_protective_history(**kwargs):
        reads.append(dict(kwargs))
        if kwargs.get('intent_id') == protective['intent_id']:
            raise ConnectionError('protective fill checkpoint replay temporarily unavailable')
        return snapshot(**kwargs)

    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', unavailable_protective_history)
    ctx.manager.manage_positions()

    assert any(row.get('intent_id') == protective['intent_id'] for row in reads)
    assert any(not row.get('intent_id') and row.get('order_ids') == [stop.order.orderId]
               for row in reads)
    assert stop.orderStatus.status == 'Cancelled' and stop.orderStatus.filled == 30
    assert ctx.cancel_calls == ([stop.order.orderId] if broker_status == 'Submitted' else [])
    # Read the actual reservation again from disk. A server-side total-position
    # cap cannot distinguish these ten owned shares from the hundred manual ones.
    active, = _native_protection_intents(ctx, active=True)
    assert active['intent_id'] == protective['intent_id'] and active['status'] == 'WORKING'
    assert active['payload'].get('cumulative_filled', 0) == 0
    assert len(_native_protection_intents(ctx)) == 1
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position['quantity'] == 40 and position['protective_order_id'] == stop.order.orderId
    assert len(ctx.server.placed) == 2 and len(ctx.protection_calls) == 1
    assert ctx.server.inventory == 110

    # Recreate the actual executor while the scoped transport is still down.
    # A fresh IntentStore/cache and ownership view must retain the reservation.
    before_restart = ctx.manager
    failed_reads_before_restart = sum(
        row.get('intent_id') == protective['intent_id'] for row in reads)
    before_restart.intents.journal.close()
    ctx.manager = AutoExecutor(ctx.path, paper_trading=True, event_store=ctx.events,
                               cooldown_seconds=0, sdk_factory=lambda: ctx.sdk)
    try:
        assert ctx.manager is not before_restart and ctx.manager.intents is not before_restart.intents
        ctx.manager.manage_positions()
        assert sum(row.get('intent_id') == protective['intent_id'] for row in reads) > failed_reads_before_restart
        active, = _native_protection_intents(ctx, active=True)
        assert active['intent_id'] == protective['intent_id'] and active['status'] == 'WORKING'
        assert active['payload'].get('cumulative_filled', 0) == 0
        restarted_position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
        assert restarted_position['quantity'] == 40
        assert restarted_position['protective_order_id'] == stop.order.orderId
        assert len(ctx.server.placed) == 2 and len(ctx.protection_calls) == 1
        assert ctx.server.inventory == 110

        monkeypatch.setattr(ctx.rpc, 'execution_snapshot', snapshot)
        ctx.manager.manage_positions()
        records = {row['intent_id']: row for row in _native_protection_intents(ctx)}
        assert records[protective['intent_id']]['status'] == 'CANCELLED'
        assert records[protective['intent_id']]['payload']['cumulative_filled'] == 30
        replacement, = _native_protection_intents(ctx, active=True)
        assert replacement['intent_id'] != protective['intent_id'] and replacement['status'] == 'WORKING'
        position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
        assert position['quantity'] == 10
        assert ctx.server.inventory == 110 and ctx.server.inventory - position['quantity'] == 100
        assert len(ctx.server.placed) == 3 and len(ctx.protection_calls) == 2
        new_stop = ctx.server.placed[-1]
        assert new_stop.order.action == 'SELL' and new_stop.order.totalQuantity == 10
        assert position['protective_order_id'] == new_stop.order.orderId
        assert replacement['payload']['order_ids'] == [new_stop.order.orderId]
    finally:
        ctx.manager.intents.journal.close()
        ctx.manager = before_restart


def _native_owned_without_protection(ctx, monkeypatch, *, quantity=40, filled=None, status='Filled'):
    with monkeypatch.context() as setup:
        setup.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
        ctx.manager._process_signal(ctx.work(quantity=quantity))
        assert len(ctx.server.placed) == 1
        opening, = ctx.server.placed
        ctx.fill(opening, filled, status=status)
        ctx.manager.manage_positions()
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position['quantity'] == (quantity if filled is None else filled)
    assert ctx.protection_calls == []
    return opening


def _native_add_one_before_protective_repair(ctx):
    ctx.manager._process_signal(ctx.work(
        quantity=1, bar_ts=TS + pd.Timedelta(seconds=1), pyramid_max_adds=1))
    added = ctx.server.placed[-1]
    assert added.order.action == 'BUY' and added.order.totalQuantity == 1
    ctx.fill(added)
    # The fill arrives after the previous worker step. Start the next step
    # with fresh replay, as manage_positions does before protective repair.
    ctx.manager._snapshot_cache = None
    return added


def _native_extra_protection_contract(ctx, monkeypatch, conid):
    from ib_async import Position, Stock

    if conid == ctx.contract.conId:
        return ctx.contract
    other = Stock('PROTECTION_OTHER', 'SMART', 'USD', conId=conid)
    resolve = ctx.sdk.resolve
    positions = ctx.server.get_positions
    monkeypatch.setattr(ctx.sdk, 'resolve', lambda symbol, **kwargs: (
        [other] if symbol in (other.conId, other.symbol) else resolve(symbol, **kwargs)))
    # An explicit external manual holding supplies the other contract's broker
    # balance. It never becomes executor ownership merely by being visible.
    monkeypatch.setattr(ctx.server, 'get_positions', lambda: positions() + [
        Position(ctx.server.ib_account, other, 100, 10)])
    return other


def _native_claim_close_before_advance(ctx, monkeypatch, owner, conid):
    from trader.strategy.execution_intents import IntentStore

    _native_extra_protection_contract(ctx, monkeypatch, conid)

    class AfterDurableCloseClaim(BaseException):
        pass

    def pause_before_advance(intent):
        raise AfterDurableCloseClaim()

    with monkeypatch.context() as boundary:
        boundary.setattr(ctx.manager, '_advance_close', pause_before_advance)
        with pytest.raises(AfterDurableCloseClaim):
            ctx.manager._execute_close_durable(
                owner, conid, TS + pd.Timedelta(seconds=2), 0, 'retained close request')
    reopened = IntentStore(ctx.path)
    try:
        pending_records = reopened.all(strategy=owner, conid=conid, kind='CLOSE', active=True)
        assert len(pending_records) == 1
        pending, = pending_records
    finally:
        reopened.journal.close()
    assert pending['status'] == 'WAITING' and not pending['payload'].get('order_ids')
    return pending


def _native_restore_unproven_other_protection(ctx, monkeypatch, owner, conid):
    """An actual external protective receipt, without local ownership proof."""
    contract = _native_extra_protection_contract(ctx, monkeypatch, conid)
    result = ctx.sdk.place_protective_order(
        symbol=contract.symbol, action='SELL', quantity=1, order_type='STP',
        aux_price=9, sec_type=contract.secType, exchange=contract.exchange,
        currency=contract.currency, con_id=contract.conId, order_ref=owner)
    assert result.is_success()
    trade = result.obj
    observed = ctx.sdk.execution_snapshot(order_ids=[trade.order.orderId])
    observed_rows = [item for item in observed['orders'] if item['orderId'] == trade.order.orderId]
    assert len(observed_rows) == 1
    row, = observed_rows
    # The actual adoption producer represents an older/unproven owner binding;
    # complete broker receipt fields alone do not supply a local ownership epoch.
    pending = ctx.manager.intents.adopt_protective(
        owner, conid, row, TS, broker_intent_id=row['clientIntentId'],
        attribution_unresolved=True)
    assert pending['status'] == 'UNKNOWN' and pending['payload']['attribution_unresolved'] is True
    # Compare subsequent state to the committed row, including its update time.
    # adopt_protective returns the row decoded before its final SQL UPDATE.
    pending, = ctx.manager.intents.all(
        strategy=owner, conid=conid, kind='PROTECTIVE', active=True)
    return trade, pending


def test_native_one_percent_protection_is_enabled(native_protection_owner, monkeypatch):
    ctx = native_protection_owner
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '1')
    stop, _ = _native_open_with_protection(ctx)
    assert stop.order.auxPrice == pytest.approx(9.90)
    assert len(ctx.protection_calls) == 1


def test_native_pending_close_preserves_working_stop_during_resize(native_protection_owner, monkeypatch):
    ctx = native_protection_owner
    stop, _ = _native_open_with_protection(ctx)
    _native_add_one_before_protective_repair(ctx)
    pending = _native_claim_close_before_advance(ctx, monkeypatch, ctx.owner, ctx.contract.conId)
    ctx.manager._ensure_protective(ctx.owner, ctx.contract.conId)
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 41
    assert ctx.cancel_calls == [] and len(ctx.protection_calls) == 1
    assert stop.orderStatus.status == 'Submitted' and stop.order.totalQuantity == 40
    assert ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId,
                                   kind='CLOSE', active=True)[0]['intent_id'] == pending['intent_id']


@pytest.mark.parametrize('difference', ['owner', 'instrument'])
def test_native_unrelated_close_cannot_block_protection(native_protection_owner, monkeypatch, difference):
    ctx = native_protection_owner
    _native_owned_without_protection(ctx, monkeypatch)
    owner = 'other_protection_owner' if difference == 'owner' else ctx.owner
    conid = 202 if difference == 'instrument' else ctx.contract.conId
    pending = _native_claim_close_before_advance(ctx, monkeypatch, owner, conid)
    ctx.manager._ensure_protective(ctx.owner, ctx.contract.conId)
    active_records = _native_protection_intents(ctx, active=True)
    assert len(active_records) == 1
    active, = active_records
    assert active['status'] == 'WORKING' and active['payload']['quantity'] == 40
    assert len(ctx.protection_calls) == 1 and ctx.protection_calls[0]['contract'].conId == ctx.contract.conId
    assert ctx.cancel_calls == []
    assert ctx.manager.intents.all(strategy=owner, conid=conid, kind='CLOSE', active=True)[0] == pending


@pytest.mark.parametrize('difference', ['owner', 'instrument'])
def test_native_unrelated_uncertainty_cannot_block_protective_resize(
        native_protection_owner, monkeypatch, difference):
    ctx = native_protection_owner
    stop, _ = _native_open_with_protection(ctx)
    _native_add_one_before_protective_repair(ctx)
    owner = 'other_protection_owner' if difference == 'owner' else ctx.owner
    conid = 202 if difference == 'instrument' else ctx.contract.conId
    foreign, pending = _native_restore_unproven_other_protection(ctx, monkeypatch, owner, conid)
    ctx.manager._ensure_protective(ctx.owner, ctx.contract.conId)
    active_records = _native_protection_intents(ctx, active=True)
    assert len(active_records) == 1
    active, = active_records
    assert active['status'] == 'WORKING' and active['payload']['quantity'] == 41
    assert active['payload']['order_ids'] != [stop.order.orderId]
    assert ctx.cancel_calls == [stop.order.orderId]
    assert foreign.orderStatus.status == 'Submitted'
    assert ctx.manager.intents.all(strategy=owner, conid=conid, kind='PROTECTIVE', active=True)[0] == pending
    assert ctx.server.inventory == 141


def test_native_fractional_protection_does_not_invent_one_share_coverage(native_protection_owner, monkeypatch):
    ctx = native_protection_owner
    _native_owned_without_protection(ctx, monkeypatch, quantity=1, filled=0.5, status='Cancelled')
    ctx.manager._ensure_protective(ctx.owner, ctx.contract.conId)
    active_records = _native_protection_intents(ctx, active=True)
    assert len(active_records) == 1
    active, = active_records
    assert len(active['payload']['order_ids']) == 1
    original_id, = active['payload']['order_ids']
    ctx.manager.manage_positions()
    current_records = _native_protection_intents(ctx, active=True)
    assert len(current_records) == 1
    current, = current_records
    assert current['payload']['order_ids'] == [original_id]
    assert current['payload']['quantity'] == 0.5
    assert ctx.cancel_calls == [] and len(ctx.protection_calls) == 1
    assert ctx.server.inventory == 100.5


def test_native_partial_stop_fill_keeps_exact_remaining_coverage(native_protection_owner):
    ctx = native_protection_owner
    stop, _ = _native_open_with_protection(ctx)
    ctx.fill(stop, 10, status='Submitted')
    ctx.manager.manage_positions()
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position['quantity'] == 30 and position['protective_order_id'] == stop.order.orderId
    assert stop.order.totalQuantity == 40 and stop.orderStatus.remaining == 30
    assert stop.orderStatus.status == 'Submitted'
    assert ctx.cancel_calls == [] and len(ctx.protection_calls) == 1
    assert ctx.server.inventory == 130


def test_native_one_share_add_resizes_protection(native_protection_owner):
    ctx = native_protection_owner
    stop, _ = _native_open_with_protection(ctx)
    _native_add_one_before_protective_repair(ctx)
    ctx.manager.manage_positions()
    current_records = _native_protection_intents(ctx, active=True)
    assert len(current_records) == 1
    current, = current_records
    assert current['payload']['quantity'] == 41
    assert current['payload']['order_ids'] != [stop.order.orderId]
    assert ctx.cancel_calls == [stop.order.orderId] and len(ctx.protection_calls) == 2
    assert ctx.server.inventory == 141


def test_native_pending_resize_warning_retains_its_fill_context(native_protection_owner, monkeypatch, caplog):
    ctx = native_protection_owner
    stop, _ = _native_open_with_protection(ctx)
    _native_add_one_before_protective_repair(ctx)

    def pending_cancel(order):
        stop.orderStatus.status = 'PendingCancel'
        ctx.server.order_tracker.on_trade(stop)
        assert ctx.server.order_tracker.flush(timeout=1)
        return stop

    monkeypatch.setattr(ctx.server.client.ib.cancelOrder, 'side_effect', pending_cancel)
    with caplog.at_level(logging.WARNING):
        ctx.manager.manage_positions()
    messages = [record.getMessage().lower() for record in caplog.records]
    assert any(str(stop.order.orderId) in message and 'resize' in message
               and 'actual fills' in message for message in messages)
    assert stop.orderStatus.status == 'PendingCancel'
    assert ctx.cancel_calls == [stop.order.orderId] and len(ctx.protection_calls) == 1


def test_native_terminal_probe_retries_newly_available_scoped_history(native_protection_owner, monkeypatch):
    ctx = native_protection_owner
    stop, prior = _native_open_with_protection(ctx)
    ctx.fill(stop, 0, status='Cancelled')
    snapshot = ctx.rpc.execution_snapshot
    restored = []
    failures = []

    def delayed_scoped_history(**kwargs):
        if kwargs.get('intent_id') == prior['intent_id'] and not restored:
            failures.append(dict(kwargs))
            raise ConnectionError('scoped history unavailable until terminal refresh')
        result = snapshot(**kwargs)
        if not kwargs.get('intent_id') and kwargs.get('order_ids') == [stop.order.orderId]:
            restored.append(dict(kwargs))
        return result

    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', delayed_scoped_history)
    ctx.manager.manage_positions()
    assert failures and restored
    records = {row['intent_id']: row for row in _native_protection_intents(ctx)}
    assert records[prior['intent_id']]['status'] == 'CANCELLED'
    assert records[prior['intent_id']]['payload']['cumulative_filled'] == 0
    active_records = _native_protection_intents(ctx, active=True)
    assert len(active_records) == 1
    active, = active_records
    assert active['intent_id'] != prior['intent_id'] and active['payload']['quantity'] == 40
    assert len(ctx.protection_calls) == 2 and ctx.server.inventory == 140


def _native_external_protective(ctx, quantity):
    """A real authorized SDK workflow, outside this executor's intent writer."""
    contract = ctx.contract
    result = ctx.sdk.place_protective_order(
        symbol=contract.symbol, action='SELL', quantity=quantity, order_type='STP',
        aux_price=9, sec_type=contract.secType, exchange=contract.exchange,
        currency=contract.currency, con_id=contract.conId, order_ref=ctx.owner)
    assert result.is_success(), result.error
    trade = result.obj
    assert any(placed is trade for placed in ctx.server.placed)
    return trade


def _native_completed_cancel_without_quantity(ctx, trade, perm_id):
    """Native completedOrder defaults preserve identity but omit fill proof."""
    import copy
    import sys
    from ib_async import OrderStatus, Trade

    # The preceding openOrder/status callback enriches the actual placed Trade
    # with its permanent identity. completedOrder then omits scoped numeric IDs.
    trade.order.permId = perm_id
    trade.orderStatus.permId = perm_id
    ctx.server.order_tracker.on_trade(trade)
    assert ctx.server.order_tracker.flush(timeout=1)
    trade.orderStatus.status = 'Cancelled'
    order = copy.deepcopy(trade.order)
    order.orderId = 0
    order.clientId = 0
    order.filledQuantity = sys.float_info.max
    completed = Trade(trade.contract, order, OrderStatus(orderId=0, status='Cancelled'))
    ctx.server.order_tracker.on_trade(completed, completed=True)
    assert ctx.server.order_tracker.flush(timeout=1)
    return completed


def test_native_full_stop_fill_clarification_finishes_without_replacement(
        native_protection_owner, monkeypatch, caplog):
    ctx = native_protection_owner
    stop, prior = _native_open_with_protection(ctx)
    ctx.fill(stop, 40, status='Filled')
    snapshot = ctx.rpc.execution_snapshot
    terminal_refreshes = []
    deferred = []

    def delayed_scoped_history(**kwargs):
        if kwargs.get('intent_id') == prior['intent_id'] and not terminal_refreshes:
            deferred.append(dict(kwargs))
            raise ConnectionError('scoped full-fill history unavailable until terminal refresh')
        result = snapshot(**kwargs)
        if not kwargs.get('intent_id') and kwargs.get('order_ids') == [stop.order.orderId]:
            terminal_refreshes.append(dict(kwargs))
        return result

    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', delayed_scoped_history)
    with caplog.at_level(logging.WARNING):
        ctx.manager.manage_positions()
    assert deferred and terminal_refreshes
    records = {item['intent_id']: item for item in _native_protection_intents(ctx)}
    assert prior['intent_id'] in records
    assert records[prior['intent_id']]['status'] == 'FILLED'
    assert records[prior['intent_id']]['payload']['cumulative_filled'] == 40
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
    assert _native_protection_intents(ctx, active=True) == []
    assert len(ctx.protection_calls) == 1 and ctx.server.inventory == 100
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


def test_native_late_recovered_emergency_close_keeps_its_reservation(
        native_protection_owner, monkeypatch):
    import asyncio

    ctx = native_protection_owner
    _native_owned_without_protection(ctx, monkeypatch)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None
    emergency_id = ctx.manager._emergency_identity(ownership_epoch=position['ownership_epoch'])
    snapshot = ctx.rpc.execution_snapshot
    global_reads = []
    physical = []

    def emergency_between_global_reads(**kwargs):
        if not kwargs.get('intent_id') and not kwargs.get('order_ids'):
            global_reads.append(dict(kwargs))
            if len(global_reads) == 2:
                result = asyncio.run(ctx.api.emergency_close_position(
                    con_id=ctx.contract.conId, quantity=40,
                    strategy_name=ctx.owner, client_intent_id=emergency_id))
                assert result.is_success(), result.error
                physical.append(ctx.server.placed[-1])
        return snapshot(**kwargs)

    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', emergency_between_global_reads)
    ctx.manager.manage_positions()
    closings = ctx.manager.intents.all(
        strategy=ctx.owner, conid=ctx.contract.conId, kind='CLOSE', active=True)
    assert len(closings) == 1 and len(physical) == 1
    closing, = closings
    trade, = physical
    assert closing['intent_id'] == emergency_id and closing['status'] == 'WORKING'
    assert closing['payload']['exit_request_active'] is True
    assert closing['payload']['ownership_epoch'] == position['ownership_epoch']
    assert closing['payload']['order_ids'] == [trade.order.orderId]
    assert trade.order.action == 'SELL' and trade.order.totalQuantity == 40
    assert trade.orderStatus.status == 'Submitted' and trade.orderStatus.filled == 0
    current = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert current is not None and current['quantity'] == 40
    assert ctx.protection_calls == [] and ctx.server.inventory == 140


def test_native_new_unknown_final_quantity_reserves_protection(
        native_protection_owner, monkeypatch):
    ctx = native_protection_owner
    stop, prior = _native_open_with_protection(ctx)
    ctx.fill(stop, 0, status='Cancelled')
    snapshot = ctx.rpc.execution_snapshot
    terminal_refreshes = []
    deferred = []
    new_physical = []
    completed_receipts = []

    def unknown_receipt_after_terminal_refresh(**kwargs):
        if kwargs.get('intent_id') == prior['intent_id'] and not terminal_refreshes:
            deferred.append(dict(kwargs))
            raise ConnectionError('scoped history unavailable until terminal refresh')
        if (terminal_refreshes and not new_physical
                and not kwargs.get('intent_id') and not kwargs.get('order_ids')):
            replacement = _native_external_protective(ctx, 40)
            new_physical.append(replacement)
            completed_receipts.append(_native_completed_cancel_without_quantity(ctx, replacement, 7101))
        result = snapshot(**kwargs)
        if not kwargs.get('intent_id') and kwargs.get('order_ids') == [stop.order.orderId]:
            terminal_refreshes.append(dict(kwargs))
        return result

    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', unknown_receipt_after_terminal_refresh)
    ctx.manager.manage_positions()
    assert deferred and terminal_refreshes and len(completed_receipts) == 1
    assert len(new_physical) == 1
    replacement, = new_physical
    active = _native_protection_intents(ctx, active=True)
    assert len(active) == 1
    pending, = active
    assert pending['status'] == 'UNKNOWN' and pending['payload']['adopted'] is True
    assert pending['payload']['attribution_unresolved'] is False
    assert pending['payload']['order_ids'] == [replacement.order.orderId]
    assert pending['payload']['ownership_epoch'] == prior['payload']['ownership_epoch']
    proof = ctx.sdk.execution_snapshot(intent_id=pending['payload']['broker_intent_id'])
    assert len(proof['orders']) == 1
    row, = proof['orders']
    assert proof['complete'] and row['brokerStatus'] == 'Cancelled'
    assert row['status'] == 'Unknown' and row['fillQuantityKnown'] is False
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 40
    assert len(ctx.protection_calls) == 2 and ctx.server.inventory == 140


def test_native_final_global_protection_survives_new_holding_after_flat(
        native_protection_owner, monkeypatch):
    ctx = native_protection_owner
    stop, prior = _native_open_with_protection(ctx)
    # Submit a native pyramid add but leave it unfilled. The existing stop is
    # still exact coverage until another authorized workflow exposes its leg.
    ctx.manager._process_signal(ctx.work(
        quantity=1, bar_ts=TS + pd.Timedelta(seconds=1), pyramid_max_adds=1))
    assert len(ctx.server.placed) == 3
    added = ctx.server.placed[-1]
    assert added.order.action == 'BUY' and added.order.totalQuantity == 1
    pending_opens = ctx.manager.intents.all(
        strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN', active=True)
    assert len(pending_opens) == 1
    pending_open, = pending_opens
    assert pending_open['payload']['order_ids'] == [added.order.orderId]
    # A separate native workflow can have an overlapping one-share stop while
    # a resize is in flight. This makes the observed coverage require repair.
    overlap = _native_external_protective(ctx, 1)
    snapshot = ctx.rpc.execution_snapshot
    new_holding_fills = []
    final_physical = []

    def fill_or_cancel_actual_order(order):
        if order.orderId == stop.order.orderId:
            ctx.fill(stop, 40, status='Filled')
            return stop
        assert order.orderId == overlap.order.orderId
        ctx.fill(overlap, 0, status='Cancelled')
        return overlap

    def late_open_then_new_protection(**kwargs):
        if (kwargs.get('intent_id') == pending_open['intent_id'] and not new_holding_fills
                and ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
                and stop.orderStatus.status == 'Filled' and overlap.orderStatus.status == 'Cancelled'):
            ctx.fill(added)
            new_holding_fills.append(added.order.orderId)
        if (new_holding_fills and not final_physical
                and not kwargs.get('intent_id') and not kwargs.get('order_ids')):
            position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
            assert position is not None and position['quantity'] == 1
            assert position['ownership_epoch'] != prior['payload']['ownership_epoch']
            final_physical.append(_native_external_protective(ctx, 1))
        return snapshot(**kwargs)

    monkeypatch.setattr(ctx.server.client.ib.cancelOrder, 'side_effect', fill_or_cancel_actual_order)
    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', late_open_then_new_protection)
    ctx.manager.manage_positions()
    assert len(final_physical) == 1
    final_stop, = final_physical
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert new_holding_fills == [added.order.orderId]
    assert position is not None
    assert position['quantity'] == 1 and position['ownership_epoch'] != prior['payload']['ownership_epoch']
    assert set(ctx.cancel_calls) == {stop.order.orderId, overlap.order.orderId}
    assert stop.orderStatus.status == 'Filled' and overlap.orderStatus.status == 'Cancelled'
    assert final_stop.orderStatus.status == 'Submitted' and final_stop.order.totalQuantity == 1
    assert len(ctx.protection_calls) == 3 and ctx.server.inventory == 101
    # The next ordinary work cycle adopts that same physical protection. It
    # must neither mint a fourth stop nor transfer the old holding's epoch.
    ctx.manager.manage_positions()
    active = _native_protection_intents(ctx, active=True)
    assert len(active) == 1
    current, = active
    assert current['payload']['order_ids'] == [final_stop.order.orderId]
    assert current['payload']['ownership_epoch'] == position['ownership_epoch']
    restored = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert restored is not None and restored['protective_order_id'] == final_stop.order.orderId
    assert len(ctx.protection_calls) == 3 and ctx.server.inventory == 101


@pytest.mark.parametrize('security_type', ['STK', 'OPT'], ids=['stock', 'option'])
def test_native_protective_submission_preserves_typed_contract(
        native_protection_owner, monkeypatch, security_type):
    """Exercise exact typed resolution through the actual protective SDK API.

    The option case checks the contract identity, not IB cost/quote units.
    Account cost and quotes are explicit external fixture inputs.
    """
    from dataclasses import replace
    from ib_async import Option, Position
    from trader.trading.position_sizing import PortfolioState

    ctx = native_protection_owner
    if security_type == 'OPT':
        ctx.contract = Option('AUDIT', '20261218', 200, 'C', 'SMART',
                              multiplier='100', currency='USD', conId=200)
        monkeypatch.setattr(ctx.server, 'get_positions', lambda: [
            Position(ctx.server.ib_account, ctx.contract, ctx.server.inventory, 10)])
        # A hundred manual option contracts plus forty new ones cost 140k
        # at this fixture quote/multiplier. Supply a healthy external account
        # consistently; the native concentration and leverage gates stay on.
        account_value = 2_000_000
        risk_inputs = ctx.server.gather_risk_inputs()
        monkeypatch.setattr(ctx.server, 'gather_risk_inputs',
                            lambda: replace(risk_inputs, portfolio_value=account_value))
        monkeypatch.setattr(ctx.server.client.ib, 'accountValues', lambda: [
            SimpleNamespace(tag='NetLiquidation', currency='USD',
                            account=ctx.server.ib_account, value=str(account_value))])
        monkeypatch.setattr(ctx.server.check_order_margin, 'return_value', {
            'initMarginAfter': 100, 'equityWithLoanAfter': account_value})
        monkeypatch.setattr(ctx.sdk, '_get_portfolio_state', lambda: PortfolioState(
            net_liquidation=account_value, net_liquidation_evaluable=True,
            gross_position_value=100_000, available_funds=account_value))
    stop, protective = _native_open_with_protection(ctx)
    assert stop.contract.conId == ctx.contract.conId
    assert stop.contract.secType == security_type
    call, = ctx.protection_calls
    assert call['contract'].conId == ctx.contract.conId
    assert call['contract'].secType == security_type
    assert call['client_intent_id'] == protective['intent_id']
    assert stop.orderStatus.filled == 0 and ctx.server.inventory == 140


def test_native_protection_repairs_two_owners_in_one_management_pass(native_protection_owner):
    """A newly placed stop must leave the next owner's snapshot cache usable."""
    ctx = native_protection_owner
    owners = (ctx.owner, 'native_protection_second')
    quantities = (4, 6)
    for owner, quantity in zip(owners, quantities):
        ctx.manager._process_signal(ctx.work(strategy_name=owner, quantity=quantity))
        opening = ctx.server.placed[-1]
        assert opening.order.action == 'BUY' and opening.order.totalQuantity == quantity
        ctx.fill(opening)
    assert len(ctx.server.placed) == 2 and ctx.protection_calls == []

    ctx.manager.manage_positions()

    stops = [trade for trade in ctx.server.placed if trade.order.orderType == 'STP']
    assert len(stops) == 2 and len(ctx.protection_calls) == 2
    assert sorted(trade.order.totalQuantity for trade in stops) == [4, 6]
    for owner, quantity in zip(owners, quantities):
        position = ctx.manager.state.open_position(owner, ctx.contract.conId)
        intent, = ctx.manager.intents.all(strategy=owner, conid=ctx.contract.conId,
                                          kind='PROTECTIVE', active=True)
        assert position['quantity'] == quantity and intent['status'] == 'WORKING'
        assert intent['payload']['order_ids'] == [position['protective_order_id']]
        assert any(trade.order.orderId == position['protective_order_id'] for trade in stops)
    assert ctx.server.inventory == 110


def test_native_protective_transport_failure_retains_durable_cause(
        native_protection_owner, monkeypatch):
    """Unknown placement retains its real cause after reopening the journal."""
    ctx = native_protection_owner
    ctx.manager._process_signal(ctx.work())
    opening, = ctx.server.placed
    ctx.fill(opening)
    cause = 'protective transport reply unavailable'

    def unavailable(**kwargs):
        raise ConnectionError(cause)

    monkeypatch.setattr(ctx.rpc, 'place_standalone_order', unavailable)
    ctx.manager.manage_positions()

    intent, = _native_protection_intents(ctx, active=True)
    assert intent['status'] == 'UNKNOWN'
    assert isinstance(intent['payload'].get('error'), str)
    assert cause in intent['payload']['error']
    assert intent['intent_id'] in intent['payload']['error']
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 104
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position['quantity'] == 4 and position['protective_order_id'] is None


def test_native_protective_repair_error_retains_owner_instrument_and_trace(
        native_protection_owner, monkeypatch, caplog):
    """An unavailable catalogue must leave a correlated deferred-repair error."""
    ctx = native_protection_owner
    ctx.manager._process_signal(ctx.work())
    opening, = ctx.server.placed
    ctx.fill(opening)
    monkeypatch.setattr(ctx.sdk, 'resolve', lambda symbol, **kwargs: [])

    ctx.manager.manage_positions()

    records = [record for record in caplog.records
               if 'protection repair deferred' in record.getMessage().lower()]
    record, = records
    assert record.args == (ctx.owner, ctx.contract.conId)
    assert record.exc_info is not None and isinstance(record.exc_info[1], AutoExecutionError)
    assert record.exc_info[2] is not None
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 104
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)['quantity'] == 4


@pytest.fixture
def native_snapshot_owner(native_protection_owner, monkeypatch):
    """Use actual SDK frame projections alongside the native snapshot RPC."""
    ctx = native_protection_owner
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    # These synchronous RPC methods are the real API over the native book and
    # external position cache, just as MMR.trades()/positions() expect.
    monkeypatch.setattr(ctx.rpc, 'get_trades', ctx.api.get_trades, raising=False)
    monkeypatch.setattr(ctx.rpc, 'get_positions', ctx.api.get_positions, raising=False)
    return ctx


def _native_snapshot_entry(ctx):
    ctx.manager._process_signal(ctx.work())
    assert len(ctx.server.placed) == 1
    trade, = ctx.server.placed
    assert trade.order.action == 'BUY' and trade.order.totalQuantity == 4
    ctx.fill(trade)
    ctx.manager.manage_positions()
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert ctx.server.inventory == 104
    return trade


def _use_native_historical_frames(ctx, monkeypatch, *, orders_complete=None, positions_complete=None):
    """Declare only the supported legacy frame protocol/completeness metadata.

    Every row still comes from the actual native MMR projection. The adapter
    deliberately lacks the newer execution_snapshot method; no broker order,
    acceptance status, reference or quantity is manufactured here.
    """
    class HistoricalFrames:
        execution_snapshot = None

        def __getattr__(self, name):
            return getattr(ctx.sdk, name)

        def trades(self):
            frame = ctx.sdk.trades()
            if orders_complete is not None:
                frame.attrs['complete'] = orders_complete
            return frame

        def positions(self):
            frame = ctx.sdk.positions()
            if positions_complete is not None:
                frame.attrs['complete'] = positions_complete
            return frame

    adapter = HistoricalFrames()
    monkeypatch.setattr(ctx.manager, '_sdk', adapter)
    return adapter


def test_native_incomplete_position_refresh_retains_owned_inventory(native_snapshot_owner, monkeypatch):
    ctx = native_snapshot_owner
    _native_snapshot_entry(ctx)

    async def unavailable_positions():
        raise ConnectionError('position refresh unavailable after confirmed entry')

    monkeypatch.setattr(ctx.server.client.ib, 'reqPositionsAsync', unavailable_positions)
    ctx.manager.manage_positions()

    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert ctx.server.inventory == 104
    assert len(ctx.server.placed) == 1 and ctx.cancel_calls == []
    stored = ctx.manager.state.db.execute(
        'SELECT status FROM auto_exec_positions WHERE strategy=? AND conid=?',
        [ctx.owner, ctx.contract.conId], fetch='one')
    assert stored is not None and stored[0] == 'OPEN'


def test_explicit_incomplete_legacy_position_frame_retains_owned_inventory(native_snapshot_owner, monkeypatch):
    ctx = native_snapshot_owner
    _native_snapshot_entry(ctx)
    # An external cache read can be empty without changing broker inventory.
    # The older adapter explicitly states that this read is not complete.
    monkeypatch.setattr(ctx.server, 'get_positions', lambda: [])
    adapter = _use_native_historical_frames(ctx, monkeypatch, positions_complete=False)
    frame = adapter.positions()
    assert frame.empty and frame.attrs['complete'] is False

    ctx.manager.manage_positions()

    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert ctx.server.inventory == 104
    assert len(ctx.server.placed) == 1 and ctx.cancel_calls == []


def test_native_trade_reference_recovers_lost_receipt_through_legacy_frames(native_snapshot_owner, monkeypatch):
    ctx = native_snapshot_owner
    place = ctx.rpc.place_expressive_order
    snapshot = ctx.rpc.execution_snapshot

    def placed_reply_lost(**kwargs):
        place(**kwargs)  # The actual native API commits its physical receipt.
        raise ConnectionError('native placement reply lost before local IDs')

    def scoped_temporarily_unavailable(**kwargs):
        if kwargs.get('intent_id'):
            raise ConnectionError('intent-scoped replay temporarily unavailable')
        return snapshot(**kwargs)

    with monkeypatch.context() as outage:
        outage.setattr(ctx.rpc, 'place_expressive_order', placed_reply_lost)
        outage.setattr(ctx.rpc, 'execution_snapshot', scoped_temporarily_unavailable)
        ctx.manager._process_signal(ctx.work())
    assert len(ctx.server.placed) == 1
    trade, = ctx.server.placed
    opening_rows = ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN')
    assert len(opening_rows) == 1
    opening, = opening_rows
    assert opening['status'] == 'UNKNOWN' and not opening['payload'].get('order_ids')
    ctx.fill(trade)
    adapter = _use_native_historical_frames(ctx, monkeypatch)
    frame = adapter.trades()
    assert len(frame) == 1 and 'clientIntentId' not in frame and 'brokerOrderRef' not in frame
    assert frame.iloc[0]['orderRef'] == trade.order.orderRef
    assert frame.iloc[0]['orderRef'].endswith('|mmr:' + opening['intent_id'])

    ctx.manager.manage_positions()

    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    saved = ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN')
    assert len(saved) == 1
    assert saved[0]['status'] == 'FILLED'
    assert saved[0]['payload']['order_ids'] == [trade.order.orderId]
    assert ctx.server.inventory == 104 and len(ctx.server.placed) == 1


def _reconciliation_reopened_intent(ctx, identity):
    from trader.strategy.execution_intents import IntentStore

    reopened = IntentStore(ctx.path)
    try:
        rows = [row for row in reopened.all() if row['intent_id'] == identity]
        assert len(rows) == 1
        return rows[0]
    finally:
        reopened.journal.close()


def _reconciliation_receipt(ctx, identity, order_id, *, conid=None, permanent_id=0,
                            action='BUY', total=4, filled=0, status='Submitted',
                            encoded=True, completed=False):
    """Construct an external native callback, not an asserted placement result."""
    from ib_async import Order, OrderStatus, Stock, Trade

    contract = Stock(ctx.contract.symbol, 'SMART', 'USD',
                     conId=ctx.contract.conId if conid is None else conid)
    order = Order(orderId=order_id, clientId=0 if completed else 7,
                  permId=permanent_id, account=ctx.server.ib_account,
                  action=action, totalQuantity=total, filledQuantity=filled,
                  orderType='STP' if action == 'SELL' else 'MKT',
                  orderRef=ctx.owner + ('|mmr:' + identity if encoded else ''))
    if completed:
        # Native completedOrder omits order/client IDs from OrderStatus and
        # carries its authoritative cumulative quantity on Order instead.
        observed = OrderStatus(orderId=0, status=status)
    else:
        observed = OrderStatus(orderId=order_id, clientId=7, permId=permanent_id,
                               status=status, filled=filled,
                               remaining=max(0, total - filled), avgFillPrice=10)
    return Trade(contract, order, observed)


def _reconciliation_ingest(ctx, trade, *, completed=False):
    ctx.server.order_tracker.on_trade(trade, completed=completed)
    assert ctx.server.order_tracker.flush(timeout=1)
    ctx.server.book.add_update_trade(trade)


def _reconciliation_declared_open_history(ctx, order_ids, *, quantity=4, conid=None):
    """Bind a declared persisted physical topology through real journal APIs.

    These parser-to-state controls replay external native Trade observations;
    they do not claim the current executor placed a split order or recreated
    a historical proposal. Separate placement controls use _process_signal.
    """
    from trader.strategy.execution_intents import timestamp_text

    conid = ctx.contract.conId if conid is None else conid
    intent = ctx.manager.intents.create(
        ctx.owner, conid, 'OPEN',
        dict(bar_ts=timestamp_text(ctx.work().bar_ts), bar_size_seconds=60,
             quantity=quantity, order_ids=list(order_ids)), status='UNKNOWN')
    journal = ctx.server.server_order_journal()
    assert journal.claim(intent['intent_id'], 'declared-replay-' + intent['intent_id'],
                         ctx.server.ib_account)
    for order_id in order_ids:
        journal.reserve_order(intent['intent_id'], order_id, 7)
    journal.finish(intent['intent_id'], 'SUBMITTED')
    return intent


def _reconciliation_pending_native_open(ctx):
    ctx.manager._process_signal(ctx.work())
    assert len(ctx.server.placed) == 1
    trade, = ctx.server.placed
    assert trade.order.action == 'BUY' and trade.order.totalQuantity == 4
    rows = ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN')
    assert len(rows) == 1
    intent, = rows
    assert intent['status'] == 'WORKING' and intent['payload']['order_ids'] == [trade.order.orderId]
    return trade, intent


def test_native_unbound_protective_warning_retains_owner_instrument_and_identity(
        native_snapshot_owner, caplog):
    ctx = native_snapshot_owner
    _native_snapshot_entry(ctx)
    # This unencoded external stop has no durable creation/ownership proof.
    # Its actual native callback can be reserved, but cannot debit this holding.
    external = _reconciliation_receipt(ctx, '', 91, permanent_id=9101,
                                       action='SELL', encoded=False)
    _reconciliation_ingest(ctx, external)
    observed = [row for row in ctx.sdk.execution_snapshot()['orders'] if row['orderId'] == 91]
    assert len(observed) == 1 and observed[0]['status'] == 'Submitted'
    intent = ctx.manager.intents.adopt_protective(
        ctx.owner, ctx.contract.conId, observed[0], ctx.work().bar_ts,
        attribution_unresolved=True)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=auto_executor_module.logging.name):
        ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'UNKNOWN' and saved['payload']['attribution_unresolved'] is True
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert any('ownership' in message.lower() and ctx.owner in message
               and str(ctx.contract.conId) in message and intent['intent_id'] in message
               for message in warnings)
    assert ctx.cancel_calls == [] and ctx.protection_calls == []
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 104


def test_native_scoped_reconciliation_failure_retains_reservation_and_cause(
        native_snapshot_owner, monkeypatch, caplog):
    ctx = native_snapshot_owner
    trade, intent = _reconciliation_pending_native_open(ctx)
    actual_snapshot = ctx.rpc.execution_snapshot
    cause = 'scoped observation transport unavailable during receipt recovery'

    def failed_scoped_observation(**kwargs):
        if kwargs.get('intent_id') == intent['intent_id']:
            raise ConnectionError(cause)
        return actual_snapshot(**kwargs)

    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', failed_scoped_observation)
    ctx.manager._snapshot_cache = None
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=auto_executor_module.logging.name):
        ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'WORKING' and saved['payload']['order_ids'] == [trade.order.orderId]
    assert ctx.manager.intents.has_pending_open(ctx.owner, ctx.contract.conId)
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert any('reconciliation' in message.lower() and cause in message for message in warnings)
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 100


def test_native_unencoded_session_one_history_reconciles_through_actual_legacy_frames(
        native_snapshot_owner, monkeypatch):
    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [1])
    # A prior physical receipt, not a new order placed by this process.
    receipt = _reconciliation_receipt(ctx, '', 1, permanent_id=9201,
                                      filled=4, status='Filled', encoded=False)
    _reconciliation_ingest(ctx, receipt)
    ctx.server.inventory = 104  # The external position includes this reported fill.
    adapter = _use_native_historical_frames(ctx, monkeypatch)
    frame = adapter.trades()
    assert list(frame['orderId']) == [1] and list(frame['filled']) == [4]
    assert list(frame['orderRef']) == [ctx.owner]
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'FILLED' and saved['payload']['cumulative_filled'] == 4
    assert saved['payload']['order_ids'] == [1]
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert not ctx.manager.intents.has_pending_open(ctx.owner, ctx.contract.conId)
    assert ctx.server.placed == [] and ctx.rpc_calls == []


def test_incomplete_native_contract_cannot_attribute_fill_to_instrument_one(native_snapshot_owner):
    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [91], conid=1)
    # Incomplete Contract input is accepted by native callback ingestion, but
    # it is not a qualified placement and cannot establish exact conId1.
    receipt = _reconciliation_receipt(ctx, intent['intent_id'], 91, conid=0,
                                      permanent_id=9301, filled=4, status='Filled')
    _reconciliation_ingest(ctx, receipt)
    native = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'], order_ids=[91])
    assert native['complete'] is True and len(native['orders']) == 1
    assert native['orders'][0]['conId'] == 0 and native['orders'][0]['filled'] == 4
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'UNKNOWN' and 'cumulative_filled' not in saved['payload']
    assert ctx.manager.state.open_position(ctx.owner, 1) is None
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
    assert ctx.server.placed == [] and ctx.rpc_calls == []


def test_native_completed_zero_adoption_keeps_zero_out_of_durable_cancel_ids(native_snapshot_owner):
    ctx = native_snapshot_owner
    _native_snapshot_entry(ctx)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    identity = 'historical-protective-completed-zero'
    journal = ctx.server.server_order_journal()
    assert journal.claim(identity, 'declared-single-protective-receipt', ctx.server.ib_account)
    journal.reserve_order(identity, 91, 7)
    journal.finish(identity, 'SUBMITTED')
    completed = _reconciliation_receipt(ctx, identity, 0, permanent_id=9401,
                                        action='SELL', filled=1, status='Cancelled', completed=True)
    _reconciliation_ingest(ctx, completed, completed=True)
    ctx.server.inventory = 103
    proof = ctx.sdk.execution_snapshot(intent_id=identity)
    assert proof['complete'] is True and len(proof['orders']) == 1
    row, = proof['orders']
    assert row['orderId'] == 0 and row['filled'] == 1 and row['fillQuantityKnown'] is True
    intent = ctx.manager.intents.adopt_protective(
        ctx.owner, ctx.contract.conId, row, position['entry_bar_ts'],
        broker_intent_id=identity, ownership_epoch=position['ownership_epoch'],
        ownership_started_at=position['ownership_started_at'])
    assert intent['payload']['order_ids'] == [0]
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'CANCELLED' and saved['payload']['cumulative_filled'] == 1
    assert saved['payload']['order_ids'] == []
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 3
    assert len(ctx.server.placed) == 1 and ctx.cancel_calls == []


def test_native_completed_zero_cannot_erase_previously_proven_session_one(native_snapshot_owner):
    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [1])
    # The local/server journals retain the old positive physical identity;
    # the fresh broker observation has only its permanent ID and reference.
    completed = _reconciliation_receipt(ctx, intent['intent_id'], 0, permanent_id=9501,
                                        filled=4, status='Filled', completed=True)
    _reconciliation_ingest(ctx, completed, completed=True)
    ctx.server.inventory = 104
    native = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'], order_ids=[1])
    assert native['complete'] is True and len(native['orders']) == 1
    assert native['orders'][0]['orderId'] == 0 and native['orders'][0]['filled'] == 4
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'FILLED' and saved['payload']['cumulative_filled'] == 4
    assert saved['payload']['order_ids'] == [1]
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert ctx.server.placed == [] and ctx.rpc_calls == []


def test_native_two_physical_receipts_sum_once_into_owned_inventory(native_snapshot_owner):
    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [21, 22], quantity=5)
    first = _reconciliation_receipt(ctx, intent['intent_id'], 21, permanent_id=9601,
                                    total=2, filled=2, status='Filled')
    second = _reconciliation_receipt(ctx, intent['intent_id'], 22, permanent_id=9602,
                                     total=3, filled=3, status='Filled')
    _reconciliation_ingest(ctx, first)
    _reconciliation_ingest(ctx, second)
    ctx.server.inventory = 105
    native = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'], order_ids=[21, 22])
    assert native['complete'] is True
    assert {(row['orderId'], row['filled']) for row in native['orders']} == {(21, 2), (22, 3)}
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(intent)
    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'FILLED' and saved['payload']['cumulative_filled'] == 5
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 5
    # Repeated receipt recovery uses the actual atomic checkpoint.
    ctx.manager._reconcile_intent(saved)
    again = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert again is not None and again['quantity'] == 5
    assert ctx.server.placed == [] and ctx.rpc_calls == []


@pytest.mark.parametrize('bad_fill', [-1.0, float('nan'), float('inf')],
                         ids=['negative', 'nan', 'infinite'])
def test_actual_legacy_raw_invalid_fill_keeps_opening_unresolved(
        native_snapshot_owner, monkeypatch, bad_fill):
    ctx = native_snapshot_owner
    trade, intent = _reconciliation_pending_native_open(ctx)
    # Model an invalid external OrderStatus callback. The modern tracker
    # sanitizes it; the supported old MMR.trades projection preserves it.
    trade.orderStatus.status = 'Cancelled'
    trade.orderStatus.filled = bad_fill
    _reconciliation_ingest(ctx, trade)
    adapter = _use_native_historical_frames(ctx, monkeypatch)
    frame = adapter.trades()
    assert len(frame) == 1 and frame.iloc[0]['status'] == 'Cancelled'
    import math
    raw_fill = frame.iloc[0]['filled']
    assert raw_fill < 0 or not math.isfinite(raw_fill)
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'WORKING' and saved['payload'].get('cumulative_filled', 0) == 0
    assert ctx.manager.intents.has_pending_open(ctx.owner, ctx.contract.conId)
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
    assert ctx.server.inventory == 100 and len(ctx.server.placed) == 1


@pytest.mark.parametrize('terminal_status', ['ApiCancelled', 'Inactive'])
def test_native_terminal_no_fill_releases_opening_reservation(native_snapshot_owner, terminal_status):
    ctx = native_snapshot_owner
    trade, intent = _reconciliation_pending_native_open(ctx)
    ctx.fill(trade, 0, status=terminal_status)
    native = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'])
    assert native['complete'] is True and len(native['orders']) == 1
    assert native['orders'][0]['status'] == terminal_status
    assert native['orders'][0]['filled'] == 0 and native['orders'][0]['fillQuantityKnown'] is True
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'CANCELLED' and saved['payload']['cumulative_filled'] == 0
    assert not ctx.manager.intents.has_pending_open(ctx.owner, ctx.contract.conId)
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
    assert ctx.server.inventory == 100 and len(ctx.server.placed) == 1


def test_native_zero_fill_cancel_does_not_expand_cached_explicit_exit_authority(
        native_snapshot_owner, monkeypatch):
    ctx = native_snapshot_owner
    _native_snapshot_entry(ctx)
    held = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert held is not None and held['quantity'] == 4
    later = ctx.work().bar_ts + pd.Timedelta(minutes=1)
    monkeypatch.setattr(ctx.manager, '_now_utc',
                        lambda: (later.tz_localize('UTC') + pd.Timedelta(seconds=10)).to_pydatetime())
    ctx.manager._process_signal(ctx.work(bar_ts=later, pyramid_max_adds=1))
    assert len(ctx.server.placed) == 2
    pending = ctx.server.placed[1]
    assert pending.order.action == 'BUY' and pending.orderStatus.filled == 0
    rows = ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN', active=True)
    assert len(rows) == 1
    intent, = rows
    ctx.sdk.cancel(pending.order.orderId)
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(intent)
    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'CANCELLED' and saved['payload']['cumulative_filled'] == 0

    def owned_read_unavailable(*args, **kwargs):
        raise ConnectionError('owned position read unavailable after zero-fill cancellation')

    with monkeypatch.context() as local:
        local.setattr(ctx.manager.state, 'open_position', owned_read_unavailable)
        scope = ctx.manager._capture_exit_scope(ctx.owner, ctx.contract.conId)
    assert scope['openings'] == {}
    assert scope['positions'] == [dict(entry_bar_ts=ctx.manager._entry_text(held['entry_bar_ts']),
                                       proposal_id=held['proposal_id'])]
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert ctx.server.inventory == 104 and len(ctx.server.placed) == 2


def _reconciliation_normalized_checkpoint(trade):
    from trader.trading.order_lifecycle import OrderLifecycleTracker

    normalizer = OrderLifecycleTracker()
    try:
        normalizer.on_trade(trade)
        rows = normalizer.snapshot()
        assert len(rows) == 1
        return rows[0]
    finally:
        # A normalization-only tracker has no journal or persistence worker.
        normalizer.close(timeout=0)


@pytest.mark.parametrize('ambiguous_first', [True, False], ids=['ambiguous-first', 'proven-first'])
def test_restored_ambiguous_aliases_do_not_hide_an_independent_physical_fill(
        native_snapshot_owner, monkeypatch, tmp_path, ambiguous_first):
    import json
    from trader.data.event_store import EventStore
    from trader.data.execution_journal import ExecutionJournal
    from trader.trading.order_lifecycle import OrderLifecycleTracker

    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [21, 22], quantity=7)
    provisional = _reconciliation_receipt(ctx, intent['intent_id'], 21,
                                          total=4, filled=0, status='Cancelled')
    permanent = _reconciliation_receipt(ctx, intent['intent_id'], 21, permanent_id=9701,
                                        total=4, filled=0, status='Cancelled')
    independent = _reconciliation_receipt(ctx, intent['intent_id'], 22, permanent_id=9702,
                                          total=3, filled=3, status='Filled')
    snapshots = [_reconciliation_normalized_checkpoint(trade)
                 for trade in (provisional, permanent, independent)]
    assert len({row['identity'] for row in snapshots}) == 3
    store = EventStore(str(tmp_path / 'reconciliation_ambiguous_history.duckdb'))
    journal = ExecutionJournal(store.duckdb_path)
    try:
        # An explicitly historical duplicate: separate provisional/permanent
        # checkpoints under the same physical scope. Every row was normalized
        # by actual native callback ingress; current writers are not claimed
        # to create this old duplicate layout. Startup must retain uncertainty.
        with journal.transaction() as conn:
            for row in snapshots:
                conn.execute('INSERT INTO broker_order_progress VALUES (?, ?, ?, ?, ?)',
                             [row['identity'], row['filled'], row['filled'] * row['avgFillPrice'],
                              row['status'], json.dumps(row)])
    finally:
        journal.close()
    restarted = OrderLifecycleTracker(store)
    try:
        restored = restarted.snapshot()
        assert {row['orderId'] for row in restored if row.get('identityAmbiguous')} == {21}
        assert len([row for row in restored if row.get('identityAmbiguous')]) == 2
        assert restarted.health['replay_required'] is True
        actual_snapshot = restarted.snapshot

        def ordered_native_rows(*args, **kwargs):
            # Exercise both permitted enumeration orders while preserving
            # every native snapshot row and all completeness/identity fields.
            rows = actual_snapshot(*args, **kwargs)
            return sorted(rows, key=lambda row: (
                not row.get('identityAmbiguous', False) if ambiguous_first
                else bool(row.get('identityAmbiguous', False)), row['identity']))

        async def completed_window(_api_only):
            return []

        async def executions_window():
            return []

        ctx.server.inventory = 103
        with monkeypatch.context() as native:
            native.setattr(ctx.server, 'order_tracker', restarted)
            native.setattr(restarted, 'snapshot', ordered_native_rows)
            native.setattr(ctx.server.client.ib, 'reqCompletedOrdersAsync', completed_window, raising=False)
            native.setattr(ctx.server.client.ib, 'reqExecutionsAsync', executions_window, raising=False)
            native.setattr(ctx.server.client.ib, 'trades', lambda: [], raising=False)
            proof = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'], order_ids=[21, 22])
            assert proof['complete'] is False and len(proof['orders']) == 3
            assert bool(proof['orders'][0].get('identityAmbiguous', False)) is ambiguous_first
            assert {(row['orderId'], row['filled']) for row in proof['orders']
                    if not row.get('identityAmbiguous', False)} == {(22, 3)}
            ctx.manager._snapshot_cache = None
            ctx.manager._reconcile_intent(intent)
            saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
            assert saved['status'] == 'UNKNOWN' and saved['payload']['cumulative_filled'] == 3
            position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
            assert position is not None and position['quantity'] == 3
            assert ctx.manager.intents.has_pending_open(ctx.owner, ctx.contract.conId)
            ctx.manager._reconcile_intent(saved)
            again = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
            assert again is not None and again['quantity'] == 3
            assert ctx.server.placed == [] and ctx.rpc_calls == []
    finally:
        restarted.close(timeout=1)
        if restarted._journal is not None:
            restarted._journal.close()
        if restarted._temporary is not None:
            restarted._temporary.cleanup()


def test_native_unencoded_session_one_remains_in_scoped_adoption_snapshot(
        native_snapshot_owner, monkeypatch):
    ctx = native_snapshot_owner
    # Reserve a different native session ID for this process's actual OPEN.
    sequence = iter(range(11, 1000))
    monkeypatch.setattr(ctx.server.client.ib.client, 'getReqId', lambda: next(sequence))
    _native_snapshot_entry(ctx)
    external = _reconciliation_receipt(
        ctx, '', 1, permanent_id=9801, action='SELL', encoded=False)
    _reconciliation_ingest(ctx, external)
    global_rows = [row for row in ctx.sdk.execution_snapshot()['orders']
                   if row['orderId'] == 1]
    assert len(global_rows) == 1 and global_rows[0]['clientIntentId'] == ''
    # Actual adoption records this unencoded external stop as unresolved;
    # the test does not invent an ownership epoch or attribute its fills.
    intent = ctx.manager.intents.adopt_protective(
        ctx.owner, ctx.contract.conId, global_rows[0], ctx.work().bar_ts,
        attribution_unresolved=True)
    assert intent['payload']['order_ids'] == [1]
    assert intent['payload']['attribution_unresolved'] is True
    ctx.manager._snapshot_cache = None

    scoped = ctx.manager._execution_snapshot(intent)

    assert scoped['complete'] is True and len(scoped['orders']) == 1
    assert scoped['orders'][0]['orderId'] == 1
    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'UNKNOWN' and saved['payload']['attribution_unresolved'] is True
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert len(ctx.server.placed) == 1 and ctx.cancel_calls == []


def test_declared_incomplete_legacy_order_frame_retains_opening_reservation(
        native_snapshot_owner, monkeypatch):
    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [21])
    receipt = _reconciliation_receipt(
        ctx, intent['intent_id'], 21, permanent_id=9802, filled=4, status='Filled')
    _reconciliation_ingest(ctx, receipt)
    ctx.server.inventory = 104
    adapter = _use_native_historical_frames(ctx, monkeypatch, orders_complete=False)
    frame = adapter.trades()
    assert frame.attrs['complete'] is False and list(frame['filled']) == [4]
    ctx.manager._snapshot_cache = None

    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'UNKNOWN' and saved['payload']['cumulative_filled'] == 4
    assert ctx.manager.intents.has_pending_open(ctx.owner, ctx.contract.conId)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert ctx.server.inventory == 104 and ctx.server.placed == [] and ctx.rpc_calls == []


def test_missing_legacy_session_one_leg_keeps_known_partial_opening_reserved(
        native_snapshot_owner, monkeypatch):
    ctx = native_snapshot_owner
    # The actual journals retain two physical IDs, but bounded historical
    # Book/MMR replay currently contains only the second leg's real callback.
    intent = _reconciliation_declared_open_history(ctx, [1, 2], quantity=5)
    receipt = _reconciliation_receipt(
        ctx, intent['intent_id'], 2, permanent_id=9803, total=3, filled=3, status='Filled')
    _reconciliation_ingest(ctx, receipt)
    ctx.server.inventory = 103
    adapter = _use_native_historical_frames(ctx, monkeypatch)
    frame = adapter.trades()
    assert list(frame['orderId']) == [2] and list(frame['filled']) == [3]
    ctx.manager._snapshot_cache = None

    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['payload']['order_ids'] == [1, 2]
    assert saved['status'] == 'UNKNOWN' and saved['payload']['cumulative_filled'] == 3
    assert ctx.manager.intents.has_pending_open(ctx.owner, ctx.contract.conId)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 3
    assert ctx.server.inventory == 103 and ctx.server.placed == [] and ctx.rpc_calls == []


def test_completed_zero_id_is_not_a_required_legacy_physical_leg(
        native_snapshot_owner, monkeypatch):
    ctx = native_snapshot_owner
    _native_snapshot_entry(ctx)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    identity = 'legacy-completed-zero-no-fill'
    journal = ctx.server.server_order_journal()
    assert journal.claim(identity, 'declared-single-zero-fill-stop', ctx.server.ib_account)
    journal.reserve_order(identity, 91, 7)
    journal.finish(identity, 'SUBMITTED')
    completed = _reconciliation_receipt(
        ctx, identity, 0, permanent_id=9804, action='SELL', filled=0,
        status='Cancelled', completed=True)
    _reconciliation_ingest(ctx, completed, completed=True)
    proof = ctx.sdk.execution_snapshot(intent_id=identity)
    assert proof['complete'] is True and len(proof['orders']) == 1
    row, = proof['orders']
    assert row['orderId'] == 0 and row['filled'] == 0 and row['fillQuantityKnown'] is True
    intent = ctx.manager.intents.adopt_protective(
        ctx.owner, ctx.contract.conId, row, position['entry_bar_ts'],
        broker_intent_id=identity, ownership_epoch=position['ownership_epoch'],
        ownership_started_at=position['ownership_started_at'])
    assert intent['payload']['order_ids'] == [0]
    # Here both the native cumulative quantity and the old raw status field
    # are explicitly zero. This does not generalize raw default zero to a
    # partially filled completed order's authoritative execution quantity.
    adapter = _use_native_historical_frames(ctx, monkeypatch)
    frame = adapter.trades()
    zero_rows = frame[frame['orderId'] == 0]
    assert len(zero_rows) == 1 and zero_rows.iloc[0]['filled'] == 0
    ctx.manager._snapshot_cache = None

    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'CANCELLED' and saved['payload']['cumulative_filled'] == 0
    assert saved['payload']['order_ids'] == []
    assert not ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId,
                                       kind='PROTECTIVE', active=True)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert ctx.server.inventory == 104 and len(ctx.server.placed) == 1 and ctx.cancel_calls == []


def test_resolved_legacy_close_without_never_sent_proof_still_counts_its_fill(
        native_snapshot_owner, monkeypatch):
    import asyncio
    import reactivex as rx
    from trader.strategy.execution_intents import timestamp_text

    ctx = native_snapshot_owner
    _native_snapshot_entry(ctx)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    # Explicit old journal shape: RESOLVED lacks transport metadata and a
    # never_submitted proof. Current WAITING retirement is not claimed to
    # produce it. A real native callback now proves one reducing execution.
    prior = ctx.manager.intents.create(
        ctx.owner, ctx.contract.conId, 'CLOSE',
        dict(bar_ts=timestamp_text(ctx.work().bar_ts), quantity=1,
             ownership_epoch=position['ownership_epoch'],
             ownership_started_at=position['ownership_started_at']), status='RESOLVED')
    receipt = _reconciliation_receipt(
        ctx, prior['intent_id'], 91, permanent_id=9805, action='SELL',
        total=1, filled=1, status='Filled')
    _reconciliation_ingest(ctx, receipt)
    ctx.server.inventory = 103
    saved_prior = _reconciliation_reopened_intent(ctx, prior['intent_id'])
    assert saved_prior['status'] == 'RESOLVED' and 'never_submitted' not in saved_prior['payload']
    assert not saved_prior['payload'].get('order_ids') and not saved_prior['payload'].get('proposal_id')

    def native_emergency_close(**kwargs):
        return asyncio.run(ctx.api.emergency_close_position(**kwargs))

    monkeypatch.setattr(ctx.rpc, 'emergency_close_position', native_emergency_close, raising=False)
    ctx.manager._remember_emergency_exit(
        ctx.owner, ctx.contract.conId, ctx.work().bar_ts,
        'explicit recovery after historical reducing receipt')
    ctx.manager._snapshot_cache = None
    ctx.manager._retry_emergency_exits()

    assert len(ctx.server.placed) == 2
    close = ctx.server.placed[1]
    assert close.order.action == 'SELL' and close.order.totalQuantity == 3
    assert close.contract.conId == ctx.contract.conId
    # Submission alone does not claim the newly requested reduction filled.
    assert ctx.server.inventory == 103 and close.orderStatus.filled == 0
    assert ctx.manager._emergency_exits[(ctx.owner, ctx.contract.conId)]['attempted'] is True


@pytest.fixture
def native_emergency_adoption_owner(native_snapshot_owner):
    """Real owned entry plus native external emergency-receipt recovery."""
    ctx = native_snapshot_owner
    _native_snapshot_entry(ctx)
    ctx.adoption_restarts = []
    try:
        yield ctx
    finally:
        for manager in ctx.adoption_restarts:
            manager.intents.journal.close()


def _adoption_restart(ctx):
    manager = AutoExecutor(ctx.path, paper_trading=True, event_store=ctx.events,
                           cooldown_seconds=0, sdk_factory=lambda: ctx.sdk)
    ctx.adoption_restarts.append(manager)
    ctx.manager = manager
    return manager


def _adoption_external_emergency(ctx, monkeypatch, *, owner=None, conid=None,
                                 order_id=91, permanent_id=12001, total=4,
                                 filled=0, status='Submitted', parent=None,
                                 at_entry=False, completed=False, durable_leg=False,
                                 position_conid=None):
    """A declared older receipt, passed through the native callback producer.

    No placement is claimed by this helper. The actual ownership came from a
    separate native entry. Optional server topology is a declared persisted
    single leg, as for a completed-order replay after reconnect.
    """
    from trader.strategy.execution_intents import timestamp_text

    instrument = ctx.contract.conId if position_conid is None else position_conid
    position = ctx.manager.state.open_position(ctx.owner, instrument)
    assert position is not None
    with monkeypatch.context() as clock:
        if at_entry:
            when = dt.datetime.fromisoformat(timestamp_text(position['entry_bar_ts'])).timestamp()
            clock.setattr(auto_executor_module.time, 'time', lambda: when)
        identity = ctx.manager._emergency_identity(parent, position['ownership_epoch'])
    if durable_leg:
        journal = ctx.server.server_order_journal()
        assert journal.claim(identity, 'declared-emergency-replay-' + identity,
                             ctx.server.ib_account)
        journal.reserve_order(identity, 91, 7)
        journal.finish(identity, 'SUBMITTED')
    trade = _reconciliation_receipt(
        ctx, identity, order_id, conid=conid, permanent_id=permanent_id,
        action='SELL', total=total, filled=filled, status=status, completed=completed)
    trade.order.orderType = 'MKT'
    if owner is not None:
        trade.order.orderRef = owner + '|mmr:' + identity
    _reconciliation_ingest(ctx, trade, completed=completed)
    ctx.manager._snapshot_cache = None
    return identity, trade


def _adoption_closed_row(ctx):
    state = AutoExecState(ctx.path)
    row = state.db.execute(
        'SELECT quantity,status,closed_reason FROM auto_exec_positions '
        'WHERE strategy=? AND conid=?', [ctx.owner, ctx.contract.conId], fetch='one')
    assert row is not None
    return row


@pytest.mark.parametrize('difference', ['owner', 'instrument'])
def test_native_emergency_recovery_does_not_consume_another_scope(
        native_emergency_adoption_owner, monkeypatch, difference):
    ctx = native_emergency_adoption_owner
    other_owner = 'another_emergency_owner' if difference == 'owner' else ctx.owner
    other_conid = 202 if difference == 'instrument' else ctx.contract.conId
    identity, _ = _adoption_external_emergency(
        ctx, monkeypatch, owner=other_owner, conid=other_conid)
    observed = [row for row in ctx.sdk.execution_snapshot()['orders']
                if row['clientIntentId'] == identity]
    assert len(observed) == 1
    assert observed[0]['conId'] == other_conid
    assert observed[0]['orderRef'] == other_owner
    assert observed[0]['clientIntentId'] == identity
    assert observed[0]['brokerOrderRef'] == other_owner + '|mmr:' + identity

    _adoption_restart(ctx)._adopt_observed_protectives(ctx.owner, ctx.contract.conId)

    assert ctx.manager.intents.all(kind='CLOSE') == []
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 104


def test_native_incomplete_emergency_contract_does_not_alias_instrument_one(
        native_emergency_adoption_owner, monkeypatch):
    ctx = native_emergency_adoption_owner
    # An explicitly persisted separate holding supplies the target conId1.
    # It is never inferred from the incomplete broker Contract below.
    ctx.manager.state.record_open(ctx.owner, 1, 2, TS, 5001, None, None)
    identity, _ = _adoption_external_emergency(
        ctx, monkeypatch, conid=0, position_conid=1, permanent_id=12002)
    rows = [row for row in ctx.sdk.execution_snapshot()['orders']
            if row['clientIntentId'] == identity]
    assert len(rows) == 1 and rows[0]['conId'] == 0

    _adoption_restart(ctx)._adopt_observed_protectives(ctx.owner, 1)

    assert ctx.manager.intents.all(kind='CLOSE') == []
    position = ctx.manager.state.open_position(ctx.owner, 1)
    assert position is not None and position['quantity'] == 2
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 104


def test_native_same_instant_emergency_recovery_persists_its_fill_cause(
        native_emergency_adoption_owner, monkeypatch):
    ctx = native_emergency_adoption_owner
    identity, _ = _adoption_external_emergency(
        ctx, monkeypatch, at_entry=True, filled=4, status='Filled')
    ctx.server.inventory = 100  # External cumulative SELL4, not a new placement.
    _adoption_restart(ctx)._adopt_observed_protectives(ctx.owner, ctx.contract.conId)
    restored = _reconciliation_reopened_intent(ctx, identity)
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(restored)

    saved = _reconciliation_reopened_intent(ctx, identity)
    assert saved['status'] == 'FILLED' and saved['payload']['cumulative_filled'] == 4
    quantity, status, cause = _adoption_closed_row(ctx)
    assert quantity == 0 and status == 'CLOSED'
    assert isinstance(cause, str) and 'recover' in cause.lower() and 'emergency' in cause.lower()
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 100


def test_native_zero_id_recovery_requires_retry_proof_after_checkpoint_outage(
        native_emergency_adoption_owner, monkeypatch):
    ctx = native_emergency_adoption_owner
    identity, _ = _adoption_external_emergency(
        ctx, monkeypatch, order_id=0, completed=True, durable_leg=True,
        status='Cancelled', permanent_id=12003)
    _adoption_restart(ctx)._adopt_observed_protectives(ctx.owner, ctx.contract.conId)
    restored = _reconciliation_reopened_intent(ctx, identity)

    def failed_checkpoint(*args, **kwargs):
        raise OSError('fill transaction unavailable after physical identity normalization')

    with monkeypatch.context() as outage:
        outage.setattr(ctx.manager.state, 'apply_fill', failed_checkpoint)
        with pytest.raises(OSError, match='fill transaction unavailable'):
            ctx.manager._reconcile_intent(restored)
    saved = _reconciliation_reopened_intent(ctx, identity)
    assert saved['status'] == 'WORKING' and saved['payload']['order_ids'] == []
    assert 'cumulative_filled' not in saved['payload']
    actual_snapshot = ctx.rpc.execution_snapshot
    actual_propose = ctx.sdk.propose
    proposals = []

    def lost_scoped_replay(**kwargs):
        if kwargs.get('intent_id') == identity:
            raise ConnectionError('retry proof unavailable after executor restart')
        return actual_snapshot(**kwargs)

    def observed_proposal(**kwargs):
        proposals.append(dict(kwargs))
        return actual_propose(**kwargs)

    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', lost_scoped_replay)
    monkeypatch.setattr(ctx.sdk, 'propose', observed_proposal)
    _adoption_restart(ctx)._advance_close(_reconciliation_reopened_intent(ctx, identity))

    assert proposals == []
    saved = _reconciliation_reopened_intent(ctx, identity)
    assert saved['status'] == 'WORKING' and not saved['payload'].get('proposal_id')
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 104


def test_recovered_positive_order_id_remains_a_legacy_completeness_obligation(
        native_emergency_adoption_owner, monkeypatch):
    from trader.trading.book import BookSubject

    ctx = native_emergency_adoption_owner
    identity, _ = _adoption_external_emergency(ctx, monkeypatch)
    _adoption_restart(ctx)._adopt_observed_protectives(ctx.owner, ctx.contract.conId)
    restored = _reconciliation_reopened_intent(ctx, identity)
    # A later bounded legacy cache contains only a completed callback with no
    # session ID. Its actual MMR.trades projection does not prove the old91 leg.
    completed = _reconciliation_receipt(
        ctx, identity, 0, permanent_id=12004, action='SELL', filled=0,
        status='Cancelled', completed=True)
    completed.order.orderType = 'MKT'
    fresh_book = BookSubject()
    fresh_book.add_update_trade(completed)
    monkeypatch.setattr(ctx.server, 'book', fresh_book)
    adapter = _use_native_historical_frames(ctx, monkeypatch)
    frame = adapter.trades()
    assert list(frame['orderId']) == [0] and list(frame['filled']) == [0]
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(restored)

    saved = _reconciliation_reopened_intent(ctx, identity)
    assert saved['status'] == 'UNKNOWN'
    assert saved['payload']['order_ids'] == [91]
    active = ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId,
                                     kind='CLOSE', active=True)
    assert {row['intent_id'] for row in active} == {identity}
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4


def test_later_explicit_close_updates_the_recovered_attempt_fill_cause(
        native_emergency_adoption_owner, monkeypatch):
    ctx = native_emergency_adoption_owner
    identity, trade = _adoption_external_emergency(ctx, monkeypatch)
    _adoption_restart(ctx)._adopt_observed_protectives(ctx.owner, ctx.contract.conId)
    _reconciliation_reopened_intent(ctx, identity)
    cause = 'operator SELL after emergency recovery'
    ctx.manager._execute_close_durable(
        ctx.owner, ctx.contract.conId, TS + pd.Timedelta(seconds=2), 4,
        cause, entry_bar_ts=None)
    trade.order.filledQuantity = 4
    trade.orderStatus.filled = 4
    trade.orderStatus.remaining = 0
    trade.orderStatus.status = 'Filled'
    _reconciliation_ingest(ctx, trade)
    ctx.server.inventory = 100
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(_reconciliation_reopened_intent(ctx, identity))

    quantity, status, closed_reason = _adoption_closed_row(ctx)
    assert quantity == 0 and status == 'CLOSED' and closed_reason == cause
    saved = _reconciliation_reopened_intent(ctx, identity)
    assert saved['payload']['successor_exit']['reason'] == cause
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 100


def test_request_only_legacy_parent_cannot_close_a_later_native_add_after_recovery(
        native_emergency_adoption_owner, monkeypatch):
    from trader.strategy.execution_intents import timestamp_text

    ctx = native_emergency_adoption_owner
    ctx.manager._process_signal(ctx.work(
        quantity=1, bar_ts=TS + pd.Timedelta(seconds=1), pyramid_max_adds=1))
    assert len(ctx.server.placed) == 2
    addition = ctx.server.placed[-1]
    assert addition.order.action == 'BUY' and addition.order.totalQuantity == 1
    # Supported pre-scope request representation: the sole authority binding
    # is the original entry. A real ADD was already in flight before it arose.
    parent = ctx.manager.intents.create(ctx.owner, ctx.contract.conId, 'CLOSE', dict(
        bar_ts=timestamp_text(TS), quantity=4, reason='legacy entry A exit',
        request_entry_bar_ts=timestamp_text(TS), exit_request_active=True), status='WAITING')
    identity, _ = _adoption_external_emergency(
        ctx, monkeypatch, parent=parent['intent_id'], filled=4, status='Filled')
    ctx.fill(addition)
    ctx.server.inventory -= 4
    ctx.manager._snapshot_cache = None
    opening = [row for row in ctx.manager.intents.all(kind='OPEN')
               if addition.order.orderId in row['payload'].get('order_ids', [])]
    assert len(opening) == 1
    ctx.manager._reconcile_intent(opening[0])
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 5
    assert pd.Timestamp(position['entry_bar_ts']) == TS + pd.Timedelta(seconds=1)
    ctx.manager._advance_close(parent)
    assert parent['status'] == 'RESOLVED' and parent['payload']['never_submitted'] is True

    _adoption_restart(ctx)._adopt_observed_protectives(ctx.owner, ctx.contract.conId)
    child = _reconciliation_reopened_intent(ctx, identity)
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(child)
    ctx.manager._finish_close_request(child)

    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 1
    assert len(ctx.server.placed) == 2 and ctx.server.inventory == 101
    assert {row['intent_id'] for row in ctx.manager.intents.all(kind='CLOSE')} == {
        parent['intent_id'], identity}


def test_missing_parent_recovery_reports_the_exact_receipt_and_authority_cause(
        native_emergency_adoption_owner, monkeypatch, caplog):
    ctx = native_emergency_adoption_owner
    identity, _ = _adoption_external_emergency(ctx, monkeypatch, parent='missing-durable-parent')
    _adoption_restart(ctx)
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger=auto_executor_module.logging.name):
        ctx.manager._adopt_observed_protectives(ctx.owner, ctx.contract.conId)

    child = _reconciliation_reopened_intent(ctx, identity)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert not ctx.manager._close_request_pending(child, position)
    errors = [record.getMessage() for record in caplog.records if record.levelno == logging.ERROR]
    assert any(identity in message and 'parent' in message.lower()
               and 'unique' in message.lower() for message in errors)
    assert position is not None and position['quantity'] == 4
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 104


def test_two_native_emergency_receipts_survive_one_recovery_pass(
        native_emergency_adoption_owner, monkeypatch):
    ctx = native_emergency_adoption_owner
    first, _ = _adoption_external_emergency(ctx, monkeypatch, total=1)
    second, _ = _adoption_external_emergency(
        ctx, monkeypatch, order_id=92, permanent_id=12005, total=1)
    assert first != second

    _adoption_restart(ctx)._adopt_observed_protectives(ctx.owner, ctx.contract.conId)

    rows = ctx.manager.intents.all(kind='CLOSE')
    assert {row['intent_id'] for row in rows} == {first, second}
    assert all(row['status'] == 'WORKING' for row in rows)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 104


from contextlib import contextmanager as _constructor_contextmanager


@_constructor_contextmanager
def _constructed_executor(path, *, paper_trading=True, event_store=None,
                          sdk_factory=None, authority_check=None):
    # Keep a handle even if the constructor's startup diagnostic raises, so
    # the real journal opened earlier in __init__ is still explicitly closed.
    owner = AutoExecutor.__new__(AutoExecutor)
    try:
        AutoExecutor.__init__(owner, str(path), paper_trading=paper_trading,
                              event_store=event_store, sdk_factory=sdk_factory,
                              authority_check=authority_check)
        yield owner
    finally:
        worker = getattr(owner, '_worker', None)
        if worker is not None and worker.ident is not None:
            owner.stop()
            worker.join(timeout=3)
            assert not worker.is_alive(), 'owned constructor-control worker did not stop'
        intents = getattr(owner, 'intents', None)
        if intents is not None:
            intents.journal.close()


def test_default_opening_cooldown_expires_at_three_hundred_seconds_after_restart(
        native_submit_owner, tmp_path, monkeypatch):
    """Both the original worker clock and its durable restart retain the budget."""
    ctx = native_submit_owner
    original_datetime = dt.datetime
    wall = [(TS.tz_localize('UTC') + pd.Timedelta(seconds=10)).timestamp()]

    class Clock(original_datetime):
        @classmethod
        def now(cls, tz=None):
            return original_datetime.fromtimestamp(wall[0], tz)

    module_clock = SimpleNamespace(**vars(dt))
    module_clock.datetime = Clock
    monkeypatch.setattr(auto_executor_module, 'dt', module_clock)
    # execution_intents uses this same real time module. No timestamp or
    # private last-execution map is manufactured after the actual submission.
    monkeypatch.setattr(auto_executor_module.time, 'time', lambda: wall[0])
    monkeypatch.setattr(AutoExecutor, '_now_utc',
                        lambda self: original_datetime.fromtimestamp(wall[0], dt.timezone.utc))
    path = tmp_path / 'constructor_default_cooldown.duckdb'
    first_work = ctx.work(pyramid_max_adds=1)
    with _constructed_executor(path, event_store=ctx.events,
                               sdk_factory=lambda: ctx.sdk) as owner:
        owner._process_signal(first_work)
        assert len(ctx.server.placed) == 1
        first, = ctx.server.placed
        ctx.fill(first)
        owner.manage_positions()
        position = owner.state.open_position(ctx.owner, ctx.contract.conId)
        assert position is not None and position['quantity'] == 4
        first_intents = owner.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN')
        assert len(first_intents) == 1 and first_intents[0]['status'] == 'FILLED'
        submitted_at = first_intents[0]['payload']['submitted_at']
        wall[0] += 299
        owner._process_signal(ctx.work(
            bar_ts=TS + pd.Timedelta(seconds=299), pyramid_max_adds=1))
        assert len(ctx.server.placed) == 1
        rows = owner.state.db.execute(
            'SELECT decision,reason FROM auto_exec_bar_log WHERE strategy=? AND conid=? '
            'AND bar_ts=?', [ctx.owner, ctx.contract.conId,
                            (TS + pd.Timedelta(seconds=299)).to_pydatetime()], fetch='all')
        assert len(rows) == 1 and rows[0][0] == 'skip' and 'cooldown' in rows[0][1].casefold()

    # Reopen the real journal; the earlier physical order stays terminal and
    # the next distinct eligible bar may create one new opening at the boundary.
    wall[0] += 1
    with _constructed_executor(path, event_store=ctx.events,
                               sdk_factory=lambda: ctx.sdk) as restarted:
        restarted._process_signal(ctx.work(
            bar_ts=TS + pd.Timedelta(seconds=300), pyramid_max_adds=1))
        assert len(ctx.server.placed) == 2
        previous, added = ctx.server.placed
        assert previous is first and previous.orderStatus.status == 'Filled'
        assert added.order.action == 'BUY' and added.order.totalQuantity == 4
        assert added.order.orderId != previous.order.orderId
        actual = restarted.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN')
        assert len(actual) == 2
        assert {row['status'] for row in actual} == {'FILLED', 'WORKING'}
        prior = [row for row in actual if row['intent_id'] == first_intents[0]['intent_id']]
        assert len(prior) == 1 and prior[0]['payload']['submitted_at'] == submitted_at
        assert ctx.server.inventory == 104


@pytest.mark.timeout(10)
def test_first_management_submission_starts_recovery_without_any_signal_or_bar(
        native_submit_owner, monkeypatch):
    import threading

    ctx = native_submit_owner
    ctx.manager._process_signal(ctx.work())
    assert len(ctx.server.placed) == 1
    placed, = ctx.server.placed
    ctx.fill(placed)
    # The old owner has received no reconciliation call since the actual fill.
    # Construction reloads that real pending intent; only management starts
    # the new worker. No signal/bar or manual start is sent to the new owner.
    with _constructed_executor(ctx.path, event_store=ctx.events,
                               sdk_factory=lambda: ctx.sdk) as restarted:
        assert restarted.state.open_position(ctx.owner, ctx.contract.conId) is None
        completed = threading.Event()
        native_manage = restarted.manage_positions

        def observe_completed_management():
            try:
                native_manage()
            finally:
                completed.set()

        monkeypatch.setattr(restarted, 'manage_positions', observe_completed_management)
        restarted.submit_management()
        assert completed.wait(3), 'management-only startup did not process the pending native receipt'
        position = restarted.state.open_position(ctx.owner, ctx.contract.conId)
        assert position is not None and position['quantity'] == 4
        assert restarted.open_count() == 1
        rows = restarted.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN')
        assert len(rows) == 1 and rows[0]['status'] == 'FILLED'
        assert ctx.server.placed == [placed] and ctx.server.inventory == 104


def test_constructor_retains_native_runtime_opening_authority(
        native_submit_owner, tmp_path):
    from review.test_review_strategy_contract import runtime_and_strategy

    ctx = native_submit_owner
    runtime, _ = runtime_and_strategy(tmp_path)
    try:
        strategy = runtime.get_strategy('review')
        strategy.ctx.auto_execute = True
        strategy.enable()
        runtime._grant_generation(strategy)
        work = ctx.work(strategy_name=strategy.name,
                        deployment_generation=strategy.ctx.deployment_generation)
        assert runtime._opening_authorized(strategy.name, work.deployment_generation)
        with _constructed_executor(tmp_path / 'constructor_authority.duckdb',
                                   event_store=ctx.events, sdk_factory=lambda: ctx.sdk,
                                   authority_check=runtime._opening_authorized) as owner:
            owner._process_signal(work)
            assert len(ctx.server.placed) == 1
            trade, = ctx.server.placed
            rows = owner.intents.all(strategy=strategy.name, conid=ctx.contract.conId, kind='OPEN')
            assert len(rows) == 1 and rows[0]['status'] == 'WORKING'
            assert rows[0]['payload']['deployment_generation'] == work.deployment_generation
            proposal = ctx.proposals.get(rows[0]['payload']['proposal_id'])
            assert proposal.status == 'EXECUTED' and proposal.order_ids == [trade.order.orderId]
            ctx.fill(trade)
            owner.manage_positions()
            position = owner.state.open_position(strategy.name, ctx.contract.conId)
            assert position is not None and position['quantity'] == 4
    finally:
        _submit_middle_close_runtime(runtime)


def test_constructed_owner_reports_real_undatable_work_once(tmp_path, caplog):
    work = make_work(strategy_name='constructor_undatable', bar_ts=None)
    age = auto_executor_module.bar_age_seconds(work.bar_ts, TS.tz_localize('UTC').to_pydatetime())
    assert age is None
    with _constructed_executor(tmp_path / 'constructor_undatable.duckdb') as owner:
        with caplog.at_level(logging.WARNING, logger=auto_executor_module.logging.name):
            owner._warn_if_stale_gate_inert(work, age)
            owner._warn_if_stale_gate_inert(work, age)
        records = [record for record in caplog.records
                   if record.name == auto_executor_module.logging.name and 'not datable' in record.getMessage()]
        assert len(records) == 1
        message = records[0].getMessage()
        assert work.strategy_name in message and str(work.conid) in message
        assert all(fact in message.casefold() for fact in ('bar', 'unavailable', 'opens', 'refused'))
        assert owner.intents.all() == [] and owner.open_count() == 0


@pytest.mark.parametrize('paper,armed', [(True, False), (True, True), (False, True), (False, False)],
                         ids=['paper-disarmed', 'paper-armed', 'live-armed', 'live-disarmed'])
def test_constructor_live_arm_warning_identifies_only_actual_refusal(tmp_path, monkeypatch, caplog, paper, armed):
    monkeypatch.setenv('MMR_AUTO_EXECUTE_LIVE', '1' if armed else '0')
    with caplog.at_level(logging.WARNING, logger=auto_executor_module.logging.name):
        with _constructed_executor(tmp_path / 'constructor_arm.duckdb', paper_trading=paper):
            records = [record for record in caplog.records
                       if record.name == auto_executor_module.logging.name and record.levelno >= logging.WARNING]
            if not paper and not armed:
                assert len(records) == 1
                message = records[0].getMessage()
                assert AutoExecutor.LIVE_ARM_ENV in message
                assert all(fact in message.casefold() for fact in
                           ('live', 'disarmed', 'opens', 'refused', 'logged', 'closes', 'unaffected'))
            else:
                assert records == []


def test_native_stock_protection_prices_the_owned_fill_separately_from_manual_cost(
        native_protection_owner, monkeypatch):
    """A same-direction manual lot must not price this owner's disaster stop.

    The broker position is an explicit stock-account input: 100 older manual
    shares at $20, followed by 40 strategy shares actually filled at $10. Its
    aggregate basis is therefore $2400 / 140. Native receipt ingestion, SDK
    placement, ownership reconciliation and the resulting STP are real local
    paths. The test does not model whether a live broker would trigger it.
    """
    from ib_async import CommissionReport, Execution, Fill, Position
    from trader.data.event_store import EventType

    ctx = native_protection_owner
    manual_quantity, manual_price = 100.0, 20.0
    owned_quantity, entry_price = 40.0, 10.0

    def broker_stock_positions():
        strategy_filled = ctx.server.inventory - manual_quantity
        account_cost = (manual_quantity * manual_price
                        + strategy_filled * entry_price) / ctx.server.inventory
        return [Position(ctx.server.ib_account, ctx.contract,
                         ctx.server.inventory, account_cost)]

    monkeypatch.setattr(ctx.server, 'get_positions', broker_stock_positions)
    assert ctx.contract.secType == 'STK'
    assert ctx.server.inventory == manual_quantity
    ctx.manager._process_signal(ctx.work(quantity=owned_quantity))
    assert len(ctx.server.placed) == 1
    opening, = ctx.server.placed
    assert opening.order.action == 'BUY'
    assert opening.order.totalQuantity == owned_quantity

    # Deliver one actual native Execution for the Trade produced above. The
    # external broker position and order status now describe that same fill.
    now = dt.datetime.now(dt.timezone.utc)
    execution = Execution(
        execId='owned-stock-entry-cost', time=now,
        acctNumber=ctx.server.ib_account, exchange='SMART', side='BOT',
        shares=owned_quantity, price=entry_price, cumQty=owned_quantity,
        avgPrice=entry_price, orderId=opening.order.orderId,
        clientId=opening.order.clientId, permId=opening.order.permId,
        orderRef=opening.order.orderRef)
    fill = Fill(ctx.contract, execution,
                CommissionReport(execId=execution.execId, currency='USD'), now)
    ctx.server.inventory += owned_quantity
    opening.order.filledQuantity = owned_quantity
    opening.orderStatus.filled = owned_quantity
    opening.orderStatus.remaining = 0
    opening.orderStatus.avgFillPrice = entry_price
    opening.orderStatus.status = 'Filled'
    opening.fills.append(fill)
    ctx.server.order_tracker.on_execution(opening, fill)
    assert ctx.server.order_tracker.flush(timeout=1)
    receipts = ctx.server.order_tracker.execution_receipts([opening.order.orderId])
    assert len(receipts) == 1
    assert (receipts[0]['shares'], receipts[0]['price']) == (owned_quantity, entry_price)
    fills = [event for event in ctx.server.event_store.query_by_strategy(ctx.owner)
             if event.event_type == EventType.ORDER_FILLED]
    assert len(fills) == 1
    assert (fills[0].quantity, fills[0].price) == (owned_quantity, entry_price)
    assert fills[0].metadata['price_evaluable'] is True

    ctx.manager.manage_positions()
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None
    assert position['quantity'] == owned_quantity
    assert len(ctx.server.placed) == 2
    stop = ctx.server.placed[1]
    assert (stop.order.action, stop.order.orderType) == ('SELL', 'STP')
    assert stop.order.totalQuantity == owned_quantity
    assert position['protective_order_id'] == stop.order.orderId
    assert ctx.server.inventory == manual_quantity + owned_quantity
    # The existing account-cost path produces about $15.77 here. The owned
    # fill is $10, so even a stock stop can sit above this strategy's entry.
    assert stop.order.auxPrice < entry_price
    assert stop.order.auxPrice == pytest.approx(9.2)


def _publish_owned_cost_status(ctx, trade, cumulative, average):
    """A broker status update for an order actually produced by this fixture."""
    assert any(item is trade for item in ctx.server.placed)
    previous = float(trade.orderStatus.filled or 0)
    assert previous <= cumulative <= float(trade.order.totalQuantity)
    ctx.server.inventory += (cumulative - previous) * (1 if trade.order.action == 'BUY' else -1)
    trade.order.filledQuantity = cumulative
    trade.orderStatus.filled = cumulative
    trade.orderStatus.remaining = float(trade.order.totalQuantity) - cumulative
    trade.orderStatus.avgFillPrice = average
    trade.orderStatus.status = 'Filled' if trade.orderStatus.remaining == 0 else 'Submitted'
    ctx.server.order_tracker.on_trade(trade)
    assert ctx.server.order_tracker.flush(timeout=1)


def test_native_filled_open_recovers_later_price_without_attributing_twice(native_protection_owner):
    ctx = native_protection_owner
    ctx.manager._process_signal(ctx.work(quantity=4))
    assert len(ctx.server.placed) == 1
    opening = ctx.server.placed[0]
    _publish_owned_cost_status(ctx, opening, 4, 0)
    ctx.manager.manage_positions()
    first = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert first['quantity'] == 4 and first['cost_evaluable'] is False
    assert len(ctx.server.placed) == 1
    intents = _native_submit_read_intents(ctx)
    assert len(intents) == 1 and intents[0]['status'] == 'FILLED'
    # Only price changes. The later management pass must revisit this Filled
    # intent without adding its four-share execution a second time.
    _publish_owned_cost_status(ctx, opening, 4, 10)
    ctx.manager.manage_positions()
    recovered = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert recovered['quantity'] == 4 and recovered['avg_cost'] == 10
    assert recovered['ownership_epoch'] == first['ownership_epoch']
    assert len(ctx.server.placed) == 2
    stop = ctx.server.placed[1]
    assert (stop.order.totalQuantity, stop.order.auxPrice) == (4, 9.2)
    assert ctx.server.inventory == 104


def test_unpriced_add_retains_working_protection_and_warns_once(native_protection_owner, caplog):
    ctx = native_protection_owner
    stop, _ = _native_open_with_protection(ctx, quantity=4)
    ctx.manager._process_signal(ctx.work(
        quantity=2, pyramid_max_adds=1, bar_ts=TS + pd.Timedelta(minutes=1)))
    additions = [trade for trade in ctx.server.placed
                 if trade.order.action == 'BUY' and trade is not ctx.server.placed[0]]
    assert len(additions) == 1
    _publish_owned_cost_status(ctx, additions[0], 2, 0)
    caplog.clear()
    with caplog.at_level('WARNING', logger='auto_executor'):
        ctx.manager.manage_positions()
        ctx.manager.manage_positions()
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position['quantity'] == 6 and position['cost_evaluable'] is False
    assert position['protective_order_id'] == stop.order.orderId
    assert stop.orderStatus.status == 'Submitted'
    assert stop.order.totalQuantity == 4
    assert len(ctx.protection_calls) == 1
    assert ctx.cancel_calls == []
    warnings = [record.getMessage() for record in caplog.records
                if 'owned fill price unavailable' in record.getMessage()]
    assert len(warnings) == 1
    assert ctx.owner in warnings[0] and str(ctx.contract.conId) in warnings[0]
    assert warnings[0] == f"auto-executor: owned fill price unavailable for {ctx.owner} conId {ctx.contract.conId}; protective repair deferred for holding {position['ownership_epoch']} ({position['cost_unavailable_reason']})"


def test_priced_one_cent_holding_is_not_reported_as_unpriced(native_protection_owner, caplog):
    ctx = native_protection_owner
    ctx.manager._process_signal(ctx.work(quantity=1))
    assert len(ctx.server.placed) == 1
    opening = ctx.server.placed[0]
    _publish_owned_cost_status(ctx, opening, 1, 0.01)
    caplog.clear()
    with caplog.at_level('WARNING', logger='auto_executor'):
        ctx.manager.manage_positions()
        ctx.manager.manage_positions()
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None
    assert position['quantity'] == 1 and position['avg_cost'] == 0.01
    assert position['cost_evaluable'] is True
    # A positive cent-priced stop cannot sit below a one-cent entry. That
    # does not turn the actual, known opening price into missing evidence.
    assert ctx.protection_calls == [] and ctx.cancel_calls == []
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 101
    warnings = [record.getMessage().casefold() for record in caplog.records
                if 'owned fill price unavailable' in record.getMessage().casefold()]
    assert warnings == []


def test_active_price_conflict_warning_identifies_holding_and_cause(
        native_protection_owner, monkeypatch, caplog):
    ctx = native_protection_owner
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    ctx.manager._process_signal(ctx.work(quantity=4))
    assert len(ctx.server.placed) == 1
    opening = ctx.server.placed[0]
    _publish_owned_cost_status(ctx, opening, 2, 10)
    ctx.manager.manage_positions()
    priced = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert priced is not None
    assert priced['quantity'] == 2 and priced['avg_cost'] == 10
    epoch = priced['ownership_epoch']
    assert isinstance(epoch, str) and epoch
    # This is a still-working native order, not an attempt to reopen a
    # finalized priced fill. The broker now reports another positive price
    # at the same two-share endpoint, without proving a revision order.
    _publish_owned_cost_status(ctx, opening, 2, 11)
    intents = _native_submit_read_intents(ctx)
    assert len(intents) == 1 and intents[0]['status'] == 'WORKING'
    snapshot = ctx.sdk.execution_snapshot(intent_id=intents[0]['intent_id'])
    assert snapshot['complete'] is True and len(snapshot['orders']) == 1
    observed = snapshot['orders'][0]
    assert observed['filled'] == 2 and observed['avgFillPrice'] == 11
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '8')
    caplog.clear()
    with caplog.at_level('WARNING', logger='auto_executor'):
        ctx.manager.manage_positions()
        ctx.manager.manage_positions()
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None
    assert position['quantity'] == 2 and position['ownership_epoch'] == epoch
    assert position['avg_cost'] is None and position['cost_evaluable'] is False
    assert 'contradict' in position['cost_unavailable_reason'].casefold()
    warnings = [record.getMessage() for record in caplog.records
                if 'owned fill price unavailable' in record.getMessage().casefold()]
    assert len(warnings) == 1
    assert ctx.owner in warnings[0] and str(ctx.contract.conId) in warnings[0]
    assert epoch in warnings[0]
    assert 'contradict' in warnings[0].casefold()
    assert ctx.protection_calls == [] and ctx.cancel_calls == []
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 102


def test_successive_unpriced_holdings_each_warn_once(
        native_protection_owner, monkeypatch, caplog):
    ctx = native_protection_owner
    now = [TS.tz_localize('UTC') + pd.Timedelta(seconds=10)]
    monkeypatch.setattr(ctx.manager, '_now_utc', lambda: now[0].to_pydatetime())
    with caplog.at_level('WARNING', logger='auto_executor'):
        ctx.manager._process_signal(ctx.work(quantity=4))
        assert len(ctx.server.placed) == 1
        first_open = ctx.server.placed[0]
        _publish_owned_cost_status(ctx, first_open, 4, 0)
        ctx.manager.manage_positions()
        ctx.manager.manage_positions()
        first = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
        assert first is not None
        assert first['quantity'] == 4 and first['cost_evaluable'] is False
        first_epoch = first['ownership_epoch']
        assert isinstance(first_epoch, str) and first_epoch
        warnings = [record.getMessage() for record in caplog.records
                    if 'owned fill price unavailable' in record.getMessage().casefold()]
        assert len(warnings) == 1 and first_epoch in warnings[0]

        close_bar = TS + pd.Timedelta(minutes=1)
        now[0] = close_bar.tz_localize('UTC') + pd.Timedelta(seconds=10)
        ctx.manager._process_signal(ctx.work(action=Action.SELL, bar_ts=close_bar))
        assert len(ctx.server.placed) == 2
        closing = ctx.server.placed[1]
        assert closing.order.action == 'SELL' and closing.order.totalQuantity == 4
        ctx.fill(closing)
        ctx.manager.manage_positions()
        assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
        assert ctx.server.inventory == 100

        next_bar = TS + pd.Timedelta(minutes=2)
        now[0] = next_bar.tz_localize('UTC') + pd.Timedelta(seconds=10)
        ctx.manager._process_signal(ctx.work(quantity=4, bar_ts=next_bar))
        assert len(ctx.server.placed) == 3
        second_open = ctx.server.placed[2]
        assert second_open.order.action == 'BUY' and second_open.order.totalQuantity == 4
        _publish_owned_cost_status(ctx, second_open, 4, 0)
        ctx.manager.manage_positions()
        ctx.manager.manage_positions()
    second = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert second is not None
    assert second['quantity'] == 4 and second['cost_evaluable'] is False
    second_epoch = second['ownership_epoch']
    assert isinstance(second_epoch, str) and second_epoch != first_epoch
    warnings = [record.getMessage() for record in caplog.records
                if 'owned fill price unavailable' in record.getMessage().casefold()]
    assert len(warnings) == 2
    assert sum(first_epoch in message for message in warnings) == 1
    assert sum(second_epoch in message for message in warnings) == 1
    assert ctx.protection_calls == [] and ctx.cancel_calls == []
    assert len(ctx.server.placed) == 3 and ctx.server.inventory == 104


def test_native_last_fill_price_does_not_reprice_the_prior_execution(
        native_submit_owner, monkeypatch):
    """A last execution price is not the cumulative price of two executions."""
    from ib_async import CommissionReport, Execution, Fill
    from trader.data.event_store import EventType

    ctx = native_submit_owner
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    ctx.manager._process_signal(ctx.work(quantity=2))
    assert len(ctx.server.placed) == 1
    opening = ctx.server.placed[0]
    assert opening.order.action == 'BUY' and opening.order.totalQuantity == 2

    def execution_receipt(identity, cumulative, price, average):
        now = dt.datetime.now(dt.timezone.utc)
        execution = Execution(
            execId=identity, time=now, acctNumber=ctx.server.ib_account,
            exchange='SMART', side='BOT', shares=1, price=price,
            cumQty=cumulative, avgPrice=average, orderId=opening.order.orderId,
            clientId=opening.order.clientId, permId=opening.order.permId,
            orderRef=opening.order.orderRef)
        fill = Fill(ctx.contract, execution,
                    CommissionReport(execId=identity, currency='USD'), now)
        ctx.server.inventory += 1
        opening.order.filledQuantity = cumulative
        opening.orderStatus.filled = cumulative
        opening.orderStatus.remaining = 2 - cumulative
        opening.orderStatus.avgFillPrice = average
        opening.orderStatus.status = 'Filled' if cumulative == 2 else 'Submitted'
        opening.fills.append(fill)
        ctx.server.order_tracker.on_execution(opening, fill)
        assert ctx.server.order_tracker.flush(timeout=1)

    execution_receipt('known-first-fill', 1, 10, 10)
    ctx.manager.manage_positions()
    first = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert first is not None and first['quantity'] == 1
    assert first['cost_evaluable'] is True and first['avg_cost'] == 10

    # Native Execution and OrderStatus both represent missing cumulative
    # averages as zero. The second execution still proves its own one share
    # at $20; it says nothing about the price of the earlier share.
    execution_receipt('known-second-slice-only', 2, 20, 0)
    tracker_rows = ctx.server.order_tracker.snapshot([opening.order.orderId])
    assert len(tracker_rows) == 1 and tracker_rows[0]['filled'] == 2
    ctx.manager.manage_positions()
    final = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert final is not None and final['quantity'] == 2
    assert final['ownership_epoch'] == first['ownership_epoch']
    fills = [event for event in ctx.server.event_store.query_by_strategy(ctx.owner)
             if event.event_type == EventType.ORDER_FILLED]
    assert sorted((event.quantity, event.price) for event in fills) == [(1.0, 10.0), (1.0, 20.0)]
    assert all(event.metadata['price_evaluable'] for event in fills)
    known_execution_cost = sum(event.quantity * event.price for event in fills)
    assert known_execution_cost == 30
    assert ctx.server.inventory == 102 and len(ctx.server.placed) == 1
    # Conservatively unknown cumulative cost is acceptable. Claiming an
    # evaluable value requires the actual two execution prices, not $20*2.
    assert not final['cost_evaluable'] or final['avg_cost'] == pytest.approx(known_execution_cost / 2)
    assert final['cost_evaluable'] is False and final['avg_cost'] is None

    # A later aligned cumulative status provides the missing endpoint. The
    # actual current-price recovery path revisits the already Filled intent
    # without recording either execution quantity or audit fill again.
    _publish_owned_cost_status(ctx, opening, 2, 15)
    ctx.manager.manage_positions()
    recovered = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert recovered is not None and recovered['quantity'] == 2
    assert recovered['ownership_epoch'] == first['ownership_epoch']
    assert recovered['cost_evaluable'] is True
    assert recovered['avg_cost'] == pytest.approx(known_execution_cost / 2)
    after_fills = [event for event in ctx.server.event_store.query_by_strategy(ctx.owner)
                   if event.event_type == EventType.ORDER_FILLED]
    assert len(after_fills) == len(fills) == 2
    assert sorted((event.quantity, event.price) for event in after_fills) == [(1.0, 10.0), (1.0, 20.0)]
    assert sum(event.quantity for event in after_fills) == 2
    assert all(event.metadata['price_evaluable'] for event in after_fills)
    receipts = ctx.server.order_tracker.execution_receipts([opening.order.orderId])
    assert len(receipts) == 2
    assert {row['execId'] for row in receipts} == {'known-first-fill', 'known-second-slice-only'}
    assert ctx.server.inventory == 102 and ctx.server.placed == [opening]


def test_native_startup_retains_ownership_until_terminal_protective_fill_is_checkpointed(
        native_protection_owner, monkeypatch):
    """A complete flat broker snapshot cannot replace an unapplied owned fill."""
    ctx = native_protection_owner
    # Start with no manual holding, so the actual stop fill makes this broker
    # account flat. The shared helper's manual100 setup would mask startup's
    # externally-closed branch, even though its repair controls remain useful.
    ctx.server.inventory = 0
    ctx.manager._process_signal(ctx.work(quantity=4))
    assert len(ctx.server.placed) == 1
    opening = ctx.server.placed[0]
    assert opening.order.action == 'BUY' and opening.order.totalQuantity == 4
    ctx.fill(opening)
    ctx.manager.manage_positions()
    assert len(ctx.server.placed) == 2
    stop = ctx.server.placed[1]
    assert stop.order.action == 'SELL' and stop.order.orderType == 'STP'
    assert stop.order.totalQuantity == 4 and ctx.server.inventory == 4
    protective_rows = _native_protection_intents(ctx, active=True)
    assert len(protective_rows) == 1
    protective = protective_rows[0]
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    epoch = position['ownership_epoch']
    assert position['protective_order_id'] == stop.order.orderId
    assert protective['status'] == 'WORKING'
    assert protective['payload']['ownership_epoch'] == epoch
    checkpoint_sql = 'SELECT quantity FROM auto_exec_fill_progress WHERE intent_id=?'
    assert ctx.manager.state.db.execute(
        checkpoint_sql, [protective['intent_id']], fetch='one') is None

    # The broker has actually filled the placed stop. Do not run executor
    # reconciliation before reopening its durable state for startup recovery.
    ctx.fill(stop)
    assert stop.orderStatus.status == 'Filled' and stop.orderStatus.filled == 4
    assert ctx.server.inventory == 0
    snapshot = ctx.rpc.execution_snapshot
    reads = []

    def unavailable_protective_history(**kwargs):
        reads.append(dict(kwargs))
        if kwargs.get('intent_id') == protective['intent_id']:
            raise ConnectionError('protective fill checkpoint replay temporarily unavailable')
        return snapshot(**kwargs)

    monkeypatch.setattr(ctx.rpc, 'execution_snapshot', unavailable_protective_history)
    ctx.manager.intents.journal.close()
    with _constructed_executor(ctx.path, event_store=ctx.events,
                               sdk_factory=lambda: ctx.sdk) as restarted:
        restarted._reconcile_once()

        assert any(row.get('intent_id') == protective['intent_id'] for row in reads)
        assert any(not row.get('intent_id') and row.get('order_ids') == [stop.order.orderId]
                   for row in reads)
        retained = restarted.state.open_position(ctx.owner, ctx.contract.conId)
        assert retained is not None, 'terminal status alone must not discard owned fill attribution'
        assert retained['quantity'] == 4 and retained['ownership_epoch'] == epoch
        assert retained['protective_order_id'] == stop.order.orderId
        saved_rows = _native_protection_intents(ctx)
        assert len(saved_rows) == 1
        saved = saved_rows[0]
        assert saved['intent_id'] == protective['intent_id'] and saved['status'] == 'WORKING'
        assert saved['payload'].get('cumulative_filled', 0) == 0
        assert restarted.state.db.execute(
            checkpoint_sql, [protective['intent_id']], fetch='one') is None
        assert ctx.cancel_calls == [] and len(ctx.server.placed) == 2

        # Restore only the transport seam. Actual scoped replay now commits
        # the protective fill and quantity reduction in the native stores.
        monkeypatch.setattr(ctx.rpc, 'execution_snapshot', snapshot)
        restarted.manage_positions()
        saved_rows = _native_protection_intents(ctx)
        assert len(saved_rows) == 1
        saved = saved_rows[0]
        assert saved['intent_id'] == protective['intent_id'] and saved['status'] == 'FILLED'
        assert saved['payload']['cumulative_filled'] == 4
        assert restarted.state.open_position(ctx.owner, ctx.contract.conId) is None
        assert restarted.state.db.execute(
            checkpoint_sql, [protective['intent_id']], fetch='one') == (4.0,)
        closed_sql = ('SELECT quantity,status,ownership_epoch FROM auto_exec_positions '
                      'WHERE strategy=? AND conid=?')
        assert restarted.state.db.execute(
            closed_sql, [ctx.owner, ctx.contract.conId], fetch='all') == [(0.0, 'CLOSED', epoch)]
        restarted.manage_positions()
        assert restarted.state.db.execute(
            checkpoint_sql, [protective['intent_id']], fetch='one') == (4.0,)
        assert restarted.state.db.execute(
            closed_sql, [ctx.owner, ctx.contract.conId], fetch='all') == [(0.0, 'CLOSED', epoch)]
        assert _native_protection_intents(ctx, active=True) == []
        assert len(ctx.server.placed) == 2 and len(ctx.protection_calls) == 1
        assert ctx.cancel_calls == [] and ctx.server.inventory == 0


def test_native_one_interval_freshness_allows_the_requested_open(
        native_submit_owner, monkeypatch):
    """A valid multiplier of one permits work exactly one interval old."""
    ctx = native_submit_owner
    monkeypatch.setenv('MMR_STALE_BAR_MULTIPLE', '1')
    now = (TS.tz_localize('UTC') + pd.Timedelta(seconds=60)).to_pydatetime()
    monkeypatch.setattr(AutoExecutor, '_now_utc', lambda self: now)
    work = ctx.work(quantity=4, bar_size_seconds=60)
    assert ctx.manager.stale_bar_multiple == 1.0
    assert auto_executor_module.bar_age_seconds(work.bar_ts, now) == 60.0

    ctx.manager._process_signal(work)

    assert len(ctx.server.placed) == 1
    trade, = ctx.server.placed
    assert trade.contract.conId == ctx.contract.conId
    assert trade.order.action == 'BUY' and trade.order.totalQuantity == 4
    openings = ctx.manager.intents.all(
        strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN')
    assert len(openings) == 1
    opening, = openings
    assert opening['status'] == 'WORKING'
    assert opening['payload']['order_ids'] == [trade.order.orderId]
    assert len(ctx.rpc_calls) == 1
    assert ctx.manager.state.open_position(ctx.owner, ctx.contract.conId) is None
    assert ctx.server.inventory == 100


def test_native_cancelled_resize_reports_newly_unpriced_owned_cost(
        native_protection_owner, monkeypatch, caplog):
    """A later unpriced ADD endpoint can defer replacement after cancellation."""
    ctx = native_protection_owner
    stop, original = _native_open_with_protection(ctx, quantity=4)
    first = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert first is not None and first['avg_cost'] == 10
    epoch = first['ownership_epoch']
    assert isinstance(epoch, str) and epoch
    ctx.manager._process_signal(ctx.work(
        quantity=2, pyramid_max_adds=1, bar_ts=TS + pd.Timedelta(seconds=1)))
    assert len(ctx.server.placed) == 3
    addition = ctx.server.placed[-1]
    assert addition.order.action == 'BUY' and addition.order.totalQuantity == 2
    _publish_owned_cost_status(ctx, addition, 1, 10)

    cancel = ctx.rpc.cancel_order
    cancellation_observations = []

    def cancel_then_observe_unpriced_add(order_id):
        result = cancel(order_id)
        position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
        cancellation_observations.append((order_id, stop.orderStatus.status,
                                         position['quantity'], position['avg_cost']))
        # The native cancel result is preserved. This independent broker
        # status advances the actual ADD's cumulative fill without an average.
        # An earlier one-share average cannot price the new two-share endpoint.
        _publish_owned_cost_status(ctx, addition, 2, 0)
        return result

    monkeypatch.setattr(ctx.rpc, 'cancel_order', cancel_then_observe_unpriced_add)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger='auto_executor'):
        ctx.manager.manage_positions()

    assert cancellation_observations == [(stop.order.orderId, 'Cancelled', 5, 10)]
    assert ctx.cancel_calls == [stop.order.orderId]
    assert len(ctx.protection_calls) == 1 and len(ctx.server.placed) == 3
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None
    assert position['quantity'] == 6 and position['ownership_epoch'] == epoch
    assert position['avg_cost'] is None and position['cost_evaluable'] is False
    assert position['protective_order_id'] is None
    assert 'unavailable' in position['cost_unavailable_reason'].casefold()
    # This particular stop is terminal. The final no-plan branch defers its
    # replacement; it does not claim that the cancelled stop still protects us.
    assert stop.orderStatus.status == 'Cancelled' and stop.orderStatus.filled == 0
    assert _native_protection_intents(ctx, active=True) == []
    records = _native_protection_intents(ctx)
    assert len(records) == 1
    assert records[0]['intent_id'] == original['intent_id']
    assert records[0]['status'] == 'CANCELLED'
    assert records[0]['payload']['cumulative_filled'] == 0
    assert ctx.server.inventory == 106
    assert ctx.server.inventory - position['quantity'] == 100
    warnings = [record.getMessage() for record in caplog.records
                if 'owned fill price unavailable' in record.getMessage().casefold()]
    assert len(warnings) == 1
    assert ctx.owner in warnings[0] and f"conId {ctx.contract.conId}" in warnings[0]
    assert epoch in warnings[0]
    assert position['cost_unavailable_reason'] in warnings[0]
    assert 'repair deferred' in warnings[0].casefold()
    assert 'existing working protection is retained' not in warnings[0].casefold()

    # A later aligned cumulative average restores cost without inventing a
    # second execution delta. Normal management can now replace the old stop.
    monkeypatch.setattr(ctx.rpc, 'cancel_order', cancel)
    _publish_owned_cost_status(ctx, addition, 2, 10)
    ctx.manager.manage_positions()
    recovered = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert recovered is not None
    assert recovered['quantity'] == 6 and recovered['ownership_epoch'] == epoch
    assert recovered['avg_cost'] == 10 and recovered['cost_evaluable'] is True
    assert len(ctx.server.placed) == 4 and len(ctx.protection_calls) == 2
    replacement = ctx.server.placed[-1]
    assert replacement.order.orderId != stop.order.orderId
    assert (replacement.order.action, replacement.order.orderType) == ('SELL', 'STP')
    assert (replacement.order.totalQuantity, replacement.order.auxPrice) == (6, 9.2)
    assert recovered['protective_order_id'] == replacement.order.orderId
    assert ctx.cancel_calls == [stop.order.orderId] and ctx.server.inventory == 106
    assert ctx.server.inventory - recovered['quantity'] == 100


def test_declared_incomplete_receipt_keeps_owned_quantity_without_inventing_cost(
        native_snapshot_owner):
    ctx = native_snapshot_owner
    # Replay a declared historical two-leg intent, not a new placement.
    intent = _reconciliation_declared_open_history(ctx, [21, 22], quantity=2)
    receipt = _reconciliation_receipt(ctx, intent['intent_id'], 21,
                                      permanent_id=10201, total=1, filled=1, status='Filled')
    receipt.orderStatus.avgFillPrice = 10
    _reconciliation_ingest(ctx, receipt)
    snapshot = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'], order_ids=[21, 22])
    assert snapshot['complete'] is False
    assert [(row['orderId'], row['filled']) for row in snapshot['orders']] == [(21, 1)]

    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'UNKNOWN' and saved['payload']['cumulative_filled'] == 1
    assert ctx.manager.intents.has_pending_open(ctx.owner, ctx.contract.conId)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 1
    assert position['avg_cost'] is None and position['cost_evaluable'] is False
    assert ctx.server.placed == [] and ctx.rpc_calls == []


def test_unfilled_declared_leg_does_not_erase_the_filled_legs_owned_cost(
        native_snapshot_owner):
    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [21, 22], quantity=2)
    filled = _reconciliation_receipt(ctx, intent['intent_id'], 21,
                                     permanent_id=10301, total=1, filled=1, status='Filled')
    waiting = _reconciliation_receipt(ctx, intent['intent_id'], 22,
                                      permanent_id=10302, total=1, filled=0, status='Submitted')
    filled.orderStatus.avgFillPrice = 10
    waiting.orderStatus.avgFillPrice = 0
    _reconciliation_ingest(ctx, filled)
    _reconciliation_ingest(ctx, waiting)
    snapshot = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'], order_ids=[21, 22])
    assert snapshot['complete'] is True
    assert {(row['orderId'], row['filled']) for row in snapshot['orders']} == {(21, 1), (22, 0)}

    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'WORKING' and saved['payload']['cumulative_filled'] == 1
    assert ctx.manager.intents.has_pending_open(ctx.owner, ctx.contract.conId)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 1
    assert position['cost_evaluable'] is True and position['avg_cost'] == pytest.approx(10)
    assert ctx.server.placed == [] and ctx.rpc_calls == []


def test_one_unpriced_filled_leg_keeps_combined_owned_cost_unknown(native_snapshot_owner):
    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [21, 22], quantity=2)
    priced = _reconciliation_receipt(ctx, intent['intent_id'], 21,
                                     permanent_id=10401, total=1, filled=1, status='Filled')
    unpriced = _reconciliation_receipt(ctx, intent['intent_id'], 22,
                                       permanent_id=10402, total=1, filled=1, status='Filled')
    priced.orderStatus.avgFillPrice = 10
    unpriced.orderStatus.avgFillPrice = 0
    _reconciliation_ingest(ctx, priced)
    _reconciliation_ingest(ctx, unpriced)
    snapshot = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'], order_ids=[21, 22])
    assert snapshot['complete'] is True
    assert {(row['filled'], row['avgFillPrice']) for row in snapshot['orders']} == {(1, 10), (1, 0)}

    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'FILLED' and saved['payload']['cumulative_filled'] == 2
    assert not ctx.manager.intents.has_pending_open(ctx.owner, ctx.contract.conId)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 2
    assert position['avg_cost'] is None and position['cost_evaluable'] is False
    assert ctx.server.placed == [] and ctx.rpc_calls == []


def test_all_priced_physical_legs_contribute_to_owned_average_cost(native_snapshot_owner):
    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [21, 22], quantity=2)
    first = _reconciliation_receipt(ctx, intent['intent_id'], 21,
                                    permanent_id=10501, total=1, filled=1, status='Filled')
    second = _reconciliation_receipt(ctx, intent['intent_id'], 22,
                                     permanent_id=10502, total=1, filled=1, status='Filled')
    first.orderStatus.avgFillPrice = 10
    second.orderStatus.avgFillPrice = 20
    _reconciliation_ingest(ctx, first)
    _reconciliation_ingest(ctx, second)
    snapshot = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'], order_ids=[21, 22])
    assert snapshot['complete'] is True
    assert {row['avgFillPrice'] for row in snapshot['orders']} == {10, 20}

    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'FILLED' and saved['payload']['cumulative_filled'] == 2
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 2
    assert position['cost_evaluable'] is True and position['avg_cost'] == pytest.approx(15)
    # Replaying the same complete endpoint cannot add quantity or cost twice.
    ctx.manager._reconcile_intent(saved)
    again = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert again is not None and again['quantity'] == 2
    assert again['cost_evaluable'] is True and again['avg_cost'] == pytest.approx(15)
    assert ctx.server.placed == [] and ctx.rpc_calls == []


def test_positive_subdollar_receipt_has_evaluable_owned_cost(native_snapshot_owner):
    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [21], quantity=1)
    receipt = _reconciliation_receipt(ctx, intent['intent_id'], 21,
                                      permanent_id=10601, total=1, filled=1, status='Filled')
    receipt.orderStatus.avgFillPrice = 0.5
    _reconciliation_ingest(ctx, receipt)
    snapshot = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'], order_ids=[21])
    assert snapshot['complete'] is True and len(snapshot['orders']) == 1
    assert snapshot['orders'][0]['filled'] == 1 and snapshot['orders'][0]['avgFillPrice'] == 0.5

    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'FILLED' and saved['payload']['cumulative_filled'] == 1
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 1
    assert position['cost_evaluable'] is True and position['avg_cost'] == pytest.approx(0.5)
    assert ctx.server.placed == [] and ctx.rpc_calls == []


def test_native_unset_price_sentinel_cannot_establish_owned_cost(native_snapshot_owner):
    import sys

    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [21], quantity=1)
    receipt = _reconciliation_receipt(ctx, intent['intent_id'], 21,
                                      permanent_id=10701, total=1, filled=1, status='Filled')
    # The broker's finite UNSET_DOUBLE sentinel is not a quoted fill price.
    # No promise is made about which layer rejects it before owned state.
    receipt.orderStatus.avgFillPrice = sys.float_info.max
    _reconciliation_ingest(ctx, receipt)
    snapshot = ctx.sdk.execution_snapshot(intent_id=intent['intent_id'], order_ids=[21])
    assert snapshot['complete'] is True and len(snapshot['orders']) == 1
    assert snapshot['orders'][0]['filled'] == 1

    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'FILLED' and saved['payload']['cumulative_filled'] == 1
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 1
    assert position['avg_cost'] is None and position['cost_evaluable'] is False
    assert ctx.server.placed == [] and ctx.rpc_calls == []


def test_actual_legacy_trade_frame_does_not_invent_missing_average_price(
        native_snapshot_owner, monkeypatch):
    ctx = native_snapshot_owner
    intent = _reconciliation_declared_open_history(ctx, [21], quantity=1)
    receipt = _reconciliation_receipt(ctx, intent['intent_id'], 21,
                                      permanent_id=10801, total=1, filled=1, status='Filled')
    receipt.orderStatus.avgFillPrice = 10
    _reconciliation_ingest(ctx, receipt)
    adapter = _use_native_historical_frames(ctx, monkeypatch)
    frame = adapter.trades()
    assert len(frame) == 1 and list(frame['filled']) == [1]
    assert 'avgFillPrice' not in frame.columns
    snapshot = ctx.manager._execution_snapshot(intent)
    assert snapshot['complete'] is True

    ctx.manager._reconcile_intent(intent)

    saved = _reconciliation_reopened_intent(ctx, intent['intent_id'])
    assert saved['status'] == 'FILLED' and saved['payload']['cumulative_filled'] == 1
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 1
    assert position['avg_cost'] is None and position['cost_evaluable'] is False
    assert ctx.server.placed == [] and ctx.rpc_calls == []


@pytest.mark.timeout(10)
def test_native_worker_refuses_new_add_to_unresolved_legacy_holding(
        native_submit_owner, tmp_path, monkeypatch):
    """Legacy fill replay stays possible; a fresh ADD needs ownership proof."""
    import threading
    from review.test_review_strategy_contract import runtime_and_strategy
    from trader.data.duckdb_store import DuckDBConnection
    from trader.trading.strategy import Signal

    ctx = native_submit_owner
    runtime, _ = runtime_and_strategy(tmp_path)
    try:
        strategy = runtime.get_strategy('review')
        strategy.ctx.auto_execute = True
        strategy.ctx.pyramid_max_adds = 1
        strategy.enable()
        runtime._grant_generation(strategy)
        generation = strategy.ctx.deployment_generation
        assert runtime._opening_authorized(strategy.name, generation)

        # This is a persisted pre-epoch table, not a new record_open call:
        # current record_open correctly creates an ownership declaration.
        path = tmp_path / 'legacy_admission.duckdb'
        legacy = DuckDBConnection(str(path))
        legacy.execute('''CREATE TABLE auto_exec_positions (
            strategy VARCHAR NOT NULL, conid BIGINT NOT NULL,
            quantity DOUBLE NOT NULL, entry_bar_ts TIMESTAMP,
            entry_time TIMESTAMP NOT NULL, proposal_id BIGINT,
            close_by_time VARCHAR, max_hold_bars BIGINT,
            status VARCHAR NOT NULL, closed_reason VARCHAR,
            close_proposal_id BIGINT, updated TIMESTAMP NOT NULL)''', fetch='none')
        entry = (TS - pd.Timedelta(days=1)).to_pydatetime()
        legacy.execute(
            'INSERT INTO auto_exec_positions '
            '(strategy, conid, quantity, entry_bar_ts, entry_time, status, updated) '
            "VALUES (?, ?, ?, ?, ?, 'OPEN', ?)",
            [strategy.name, ctx.contract.conId, 4, entry, entry, entry], fetch='none')
        before_columns = {row[1] for row in legacy.execute("PRAGMA table_info('auto_exec_positions')", fetch='all')}
        assert 'ownership_epoch' not in before_columns
        assert 'ownership_started_at' not in before_columns
        # The external broker inventory comprises this legacy four-share
        # holding plus 100 manual shares; no opening fill is invented here.
        ctx.server.inventory = 104
        assert ctx.server.placed == [] and ctx.proposals.query() == []

        with _constructed_executor(path, event_store=ctx.events,
                                   sdk_factory=lambda: ctx.sdk,
                                   authority_check=runtime._opening_authorized) as owner:
            runtime.auto_executor = owner
            migrated = owner.state.open_position(strategy.name, ctx.contract.conId)
            assert migrated is not None and migrated['quantity'] == 4
            assert migrated['lots'] == 1 and migrated['ownership_epoch'] is None
            assert migrated['ownership_started_at'] is None
            assert migrated['avg_cost'] is None and migrated['cost_evaluable'] is False
            assert owner.intents.all() == []
            assert not owner._started and not owner._reconciled
            completed = threading.Event()
            observed_work = []
            native_process = owner._process_signal

            def observe_worker_completion(work):
                observed_work.append(work)
                try:
                    return native_process(work)
                finally:
                    completed.set()

            monkeypatch.setattr(owner, '_process_signal', observe_worker_completion)
            signal = Signal(strategy.name, Action.BUY, 0.6, 0.4,
                            conid=ctx.contract.conId, quantity=2)
            runtime._submit_auto_execution(strategy, ctx.contract.conId, signal, TS)
            assert completed.wait(3), 'native signal worker did not finish legacy ADD admission'
            assert owner._started and owner._reconciled
            assert len(observed_work) == 1
            work = observed_work[0]
            assert work.pyramid_max_adds == 1 and work.quantity == 2
            assert work.deployment_generation == generation
            assert work.auto_execute and work.state_running and work.bar_size_seconds == 60
            assert runtime._opening_authorized(strategy.name, generation)

            # Desired admission boundary: do not create a fresh proposal or
            # executable attempt for an ownership-unknown legacy holding.
            assert ctx.server.placed == []
            assert ctx.rpc_calls == [] and ctx.proposals.query() == []
            assert owner.intents.all() == []
            preserved = owner.state.open_position(strategy.name, ctx.contract.conId)
            assert preserved is not None and preserved['quantity'] == 4
            assert preserved['lots'] == 1 and preserved['ownership_epoch'] is None
            assert preserved['ownership_started_at'] is None
            assert preserved['entry_bar_ts'] == entry
            assert ctx.server.inventory == 104
            assert ctx.server.inventory - preserved['quantity'] == 100
            rows = owner.state.db.execute(
                'SELECT strategy, conid, action, decision, reason FROM auto_exec_bar_log '
                'WHERE strategy=? AND conid=?', [strategy.name, ctx.contract.conId], fetch='all')
            assert len(rows) == 1
            assert rows[0][:4] == (strategy.name, ctx.contract.conId, 'BUY', 'refused')
            reason = rows[0][4]
            assert isinstance(reason, str)
            assert 'ownership' in reason.casefold() and 'reconcil' in reason.casefold()
    finally:
        _submit_middle_close_runtime(runtime)


def test_native_close_reconciliation_survives_optional_cost_index_outage(
        native_submit_owner, monkeypatch, caplog):
    ctx = native_submit_owner
    ctx.manager._process_signal(ctx.work())
    assert len(ctx.server.placed) == 1
    entry, = ctx.server.placed
    ctx.fill(entry)
    ctx.manager.manage_positions()
    opening_rows = ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN')
    assert len(opening_rows) == 1 and opening_rows[0]['status'] == 'FILLED'
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert ctx.server.inventory == 104

    ctx.manager._process_signal(ctx.work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
    exits = [trade for trade in ctx.server.placed if trade.order.action == 'SELL']
    assert len(exits) == 1
    closing_trade, = exits
    assert closing_trade.order.totalQuantity == 4 and closing_trade.orderStatus.filled == 0
    closes = ctx.manager.intents.all(strategy=ctx.owner, conid=ctx.contract.conId, kind='CLOSE')
    assert len(closes) == 1 and closes[0]['status'] == 'WORKING'
    closing = closes[0]
    ctx.fill(closing_trade, 2, status='Submitted')
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert ctx.server.inventory == 102

    cause = 'optional cost index read unavailable during native close replay'
    failures = []

    def unavailable_cost_index(*, strategy=None, conid=None):
        failures.append((strategy, conid))
        raise ConnectionError(cause)

    def recovery_warnings():
        return [record.getMessage() for record in caplog.records
                if record.levelno == logging.WARNING
                and all(word in record.getMessage().casefold()
                        for word in ('owned', 'fill', 'price', 'recovery'))]

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=auto_executor_module.logging.name):
        with monkeypatch.context() as outage:
            outage.setattr(ctx.manager.state, 'unpriced_open_intents', unavailable_cost_index)
            for _ in range(2):
                # A new observation cycle must read the newly reported fill.
                ctx.manager._snapshot_cache = None
                ctx.manager._reconcile_intents()
                position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
                assert position is not None and position['quantity'] == 2
                assert ctx.server.inventory == 100 + position['quantity']
                saved = _reconciliation_reopened_intent(ctx, closing['intent_id'])
                assert saved['status'] == 'WORKING' and saved['payload']['cumulative_filled'] == 2
                assert len(recovery_warnings()) == 1 and cause in recovery_warnings()[0]
        # One actual successful optional lookup ends this outage episode.
        ctx.manager._snapshot_cache = None
        ctx.manager._reconcile_intents()
        assert len(recovery_warnings()) == 1
        with monkeypatch.context() as outage:
            outage.setattr(ctx.manager.state, 'unpriced_open_intents', unavailable_cost_index)
            ctx.manager._snapshot_cache = None
            ctx.manager._reconcile_intents()
        assert len(recovery_warnings()) == 2
        assert all(cause in warning for warning in recovery_warnings())

    assert failures == [(None, None)] * 3
    checkpoint = ctx.manager.state.db.execute(
        'SELECT quantity FROM auto_exec_fill_progress WHERE intent_id=?',
        [closing['intent_id']], fetch='one')
    assert checkpoint == (2,)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 2
    assert ctx.server.inventory == 102 and len(ctx.server.placed) == 2


def test_native_tracked_validation_warning_recovers_owned_fill_proof(
        native_protection_owner, monkeypatch):
    """A live broker warning cannot hide newly restored ownership proof."""
    import asyncio
    from ib_async import IB, OrderStatus

    ctx = native_protection_owner
    ctx.manager._process_signal(ctx.work(quantity=4))
    assert len(ctx.server.placed) == 1
    opening = ctx.server.placed[0]
    assert opening.order.action == 'BUY' and opening.order.totalQuantity == 4
    ctx.fill(opening)
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intents(ctx.owner, ctx.contract.conId)
    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 4
    assert position['cost_evaluable'] and position['avg_cost'] == 10
    epoch, origin = position['ownership_epoch'], position['ownership_started_at']
    assert epoch is not None and origin is not None
    assert ctx.server.inventory == 104

    # Another authorized workflow supplies the real stop and its native
    # server claim; the executor has no locally submitted protective intent.
    reference = 'protective:validation-proof-recovery'
    result = asyncio.run(ctx.api.place_standalone_order(
        ctx.contract, 'SELL', 4, 'STP', aux_price=9.2, order_ref=ctx.owner,
        client_intent_id=reference))
    assert result.is_success(), result.error
    assert len(ctx.server.placed) == 2
    stop = ctx.server.placed[-1]
    assert stop.order.action == 'SELL' and stop.order.orderType == 'STP'
    assert stop.order.totalQuantity == 4 and stop.orderStatus.status == 'Submitted'
    assert _native_protection_intents(ctx) == []
    journal = ctx.server.server_order_journal()
    outages = []

    def unavailable_provenance(*args, **kwargs):
        outages.append(True)
        raise OSError('creation provenance temporarily unavailable')

    # This native IB instance is only the disconnected callback producer.
    # Register the actual placed Trade and the same strong-reference event
    # hook used by Trader; no socket, broker reply or snapshot is fabricated.
    callback_ib = IB()
    handler = ctx.server.order_tracker.on_trade
    callback_ib.orderStatusEvent.connect(handler, keep_ref=True)
    try:
        assert not callback_ib.isConnected()
        callback_ib.wrapper.clientId = stop.order.clientId
        callback_ib.wrapper.trades[(stop.order.clientId, stop.order.orderId)] = stop
        with monkeypatch.context() as patch:
            patch.setattr(journal, 'get_many', unavailable_provenance)
            ctx.manager.manage_positions()
            pending = _native_protection_intents(ctx)
            assert outages and len(pending) == 1
            initial = pending[0]
            identity = initial['intent_id']
            assert initial['status'] == 'UNKNOWN'
            assert initial['payload']['adopted'] is True
            assert initial['payload']['attribution_unresolved'] is True
            assert initial['payload']['broker_intent_id'] == reference
            assert initial['payload']['ownership_epoch'] == epoch
            assert initial['payload']['ownership_started_at'] == origin
            tracked = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
            assert tracked is not None and tracked['quantity'] == 4
            assert tracked['protective_order_id'] == stop.order.orderId
            assert len(ctx.server.placed) == 2 and ctx.cancel_calls == []

            # IB validation warnings on an existing order leave it live.
            # Invoke the installed producer rather than assigning its status.
            callback_ib.wrapper.error(
                stop.order.orderId, 321, 'validation failed for order modification', '')
            assert stop.orderStatus.status == OrderStatus.ValidationError
            assert stop.isActive() and not stop.isDone()
            assert ctx.server.order_tracker.flush(timeout=1)

        # Creation provenance is readable again. A subsequent real partial
        # fill retains the warning status produced above and the same order.
        ctx.fill(stop, 1, status=stop.orderStatus.status)
        snapshot = ctx.sdk.execution_snapshot()
        assert snapshot['complete'] and snapshot['positions_complete']
        observed = [row for row in snapshot['orders'] if row['clientIntentId'] == reference]
        assert len(observed) == 1
        row = observed[0]
        assert row['status'] == row['brokerStatus'] == OrderStatus.ValidationError
        assert row['filled'] == 1 and row['fillQuantityKnown'] is True
        assert row['orderId'] == stop.order.orderId and row['conId'] == ctx.contract.conId
        assert row['brokerIntentCreatedAt'] >= origin

        ctx.manager._snapshot_cache = None
        ctx.manager._reconcile_intents(ctx.owner, ctx.contract.conId)
        recovered = _native_protection_intents(ctx)
        assert len(recovered) == 1
        saved = recovered[0]
        assert saved['payload']['attribution_unresolved'] is False
        assert saved['intent_id'] == identity and saved['status'] == 'WORKING'
        assert saved['payload']['ownership_epoch'] == epoch
        assert saved['payload']['ownership_started_at'] == origin
        assert saved['payload']['broker_intent_id'] == reference
        assert saved['payload']['order_ids'] == [stop.order.orderId]
        assert saved['payload']['cumulative_filled'] == 1
        positions, checkpoints = ctx.manager.state.ownership_snapshot(recovered)
        assert len(positions) == 1 and positions[0]['quantity'] == 3
        assert positions[0]['ownership_epoch'] == epoch and checkpoints == {identity: 1}
        assert ctx.server.inventory == 103 and ctx.server.inventory - positions[0]['quantity'] == 100

        with _constructed_executor(ctx.path, event_store=ctx.events,
                                   sdk_factory=lambda: ctx.sdk) as restarted:
            restarted.manage_positions()
            restarted.manage_positions()
            replayed = restarted.intents.all(
                strategy=ctx.owner, conid=ctx.contract.conId, kind='PROTECTIVE')
            assert len(replayed) == 1 and replayed[0]['intent_id'] == identity
            assert replayed[0]['status'] == 'WORKING'
            assert replayed[0]['payload']['attribution_unresolved'] is False
            assert replayed[0]['payload']['ownership_epoch'] == epoch
            positions, checkpoints = restarted.state.ownership_snapshot(replayed)
            assert len(positions) == 1 and positions[0]['quantity'] == 3
            assert positions[0]['ownership_epoch'] == epoch and checkpoints == {identity: 1}
        assert len(ctx.server.placed) == 2
        assert ctx.cancel_calls == [] and ctx.protection_calls == []
        ctx.server.client.ib.cancelOrder.assert_not_called()
        assert stop.orderStatus.remaining == 3 and stop.isActive()
        assert ctx.server.inventory == 103 and ctx.server.inventory - 3 == 100
        assert not callback_ib.isConnected()
    finally:
        callback_ib.orderStatusEvent.disconnect(handler)
        callback_ib.disconnect()


def test_native_zero_incremental_cost_keeps_owned_basis_unknown(native_submit_owner):
    """Positive cumulative prices can still give an invalid incremental cost."""
    ctx = native_submit_owner
    ctx.manager._process_signal(ctx.work(quantity=2))
    assert len(ctx.server.placed) == 1
    trade = ctx.server.placed[0]
    assert trade.order.action == 'BUY' and trade.order.totalQuantity == 2
    openings = ctx.manager.intents.all(
        strategy=ctx.owner, conid=ctx.contract.conId, kind='OPEN')
    assert len(openings) == 1
    opening = openings[0]

    _publish_owned_cost_status(ctx, trade, 1, 20)
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(opening)
    first = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert first is not None and first['quantity'] == 1
    assert first['cost_evaluable'] is True and first['avg_cost'] == 20
    epoch = first['ownership_epoch']
    assert epoch is not None and ctx.server.inventory == 101

    # This second broker status has a positive cumulative average and
    # notional, but its notional is unchanged from the one-share endpoint.
    # The second share's price cannot be inferred from that contradiction.
    _publish_owned_cost_status(ctx, trade, 2, 10)
    snapshot = ctx.sdk.execution_snapshot(
        intent_id=opening['intent_id'], order_ids=[trade.order.orderId])
    assert snapshot['complete'] is True and len(snapshot['orders']) == 1
    row = snapshot['orders'][0]
    assert row['filled'] == 2 and row['avgFillPrice'] == 10
    assert row['fillQuantityKnown'] is True
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(opening)

    position = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert position is not None and position['quantity'] == 2
    assert position['cost_evaluable'] is False and position['avg_cost'] is None
    assert 'contradictory' in position['cost_unavailable_reason']
    assert position['ownership_epoch'] == epoch
    endpoints = ctx.manager.state.db.execute(
        'SELECT end_quantity,cumulative_quote_notional FROM auto_exec_cost_events '
        'WHERE intent_id=? ORDER BY sequence', [opening['intent_id']], fetch='all')
    assert endpoints == [(1.0, 20.0), (2.0, 20.0)]
    saved = _native_submit_read_intents(ctx)
    assert len(saved) == 1 and saved[0]['status'] == 'FILLED'
    assert saved[0]['payload']['cumulative_filled'] == 2
    positions, checkpoints = ctx.manager.state.ownership_snapshot(saved)
    assert len(positions) == 1 and positions[0]['quantity'] == 2
    assert checkpoints == {opening['intent_id']: 2}

    with _constructed_executor(ctx.path, event_store=ctx.events,
                               sdk_factory=lambda: ctx.sdk) as restarted:
        restarted._reconcile_intents(ctx.owner, ctx.contract.conId)
        restarted._snapshot_cache = None
        restarted._reconcile_intents(ctx.owner, ctx.contract.conId)
        replayed = restarted.state.open_position(ctx.owner, ctx.contract.conId)
        assert replayed == position
        assert restarted.state.db.execute(
            'SELECT quantity FROM auto_exec_fill_progress WHERE intent_id=?',
            [opening['intent_id']], fetch='one') == (2.0,)
    assert len(ctx.server.placed) == 1 and ctx.server.inventory == 102
    assert ctx.server.inventory - position['quantity'] == 100


def test_native_fractional_close_replay_preserves_owned_average(native_submit_owner):
    """Partial fills of a whole requested close remove proportional owned cost."""
    ctx = native_submit_owner
    ctx.manager._process_signal(ctx.work(quantity=2))
    assert len(ctx.server.placed) == 1
    entry = ctx.server.placed[0]
    ctx.fill(entry)
    ctx.manager.manage_positions()
    first = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert first is not None and first['quantity'] == 2
    assert first['cost_evaluable'] is True and first['avg_cost'] == 10
    epoch = first['ownership_epoch']
    assert epoch is not None and ctx.server.inventory == 102

    ctx.manager._process_signal(ctx.work(
        action=Action.SELL, quantity=2, bar_ts=TS + pd.Timedelta(minutes=1)))
    closes = ctx.manager.intents.all(
        strategy=ctx.owner, conid=ctx.contract.conId, kind='CLOSE')
    assert len(closes) == 1 and len(ctx.server.placed) == 2
    closing = closes[0]
    trade = ctx.server.placed[-1]
    assert trade.order.action == 'SELL' and trade.order.totalQuantity == 2
    assert closing['payload']['order_ids'] == [trade.order.orderId]
    assert closing['payload']['ownership_epoch'] == epoch

    _publish_owned_cost_status(ctx, trade, 1, 10)
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(closing)
    one = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert one is not None and one['quantity'] == 1
    assert one['cost_evaluable'] is True and one['avg_cost'] == 10
    assert one['ownership_epoch'] == epoch and ctx.server.inventory == 101

    _publish_owned_cost_status(ctx, trade, 1.5, 10)
    ctx.manager._snapshot_cache = None
    ctx.manager._reconcile_intent(closing)
    half = ctx.manager.state.open_position(ctx.owner, ctx.contract.conId)
    assert half is not None and half['quantity'] == 0.5
    assert half['cost_evaluable'] is True and half['avg_cost'] == 10
    assert half['ownership_epoch'] == epoch and ctx.server.inventory == 100.5
    assert closing['status'] == 'WORKING' and closing['payload']['cumulative_filled'] == 1.5
    assert trade.orderStatus.remaining == 0.5
    reductions = ctx.manager.state.db.execute(
        'SELECT attributed_delta FROM auto_exec_cost_events WHERE intent_id=? ORDER BY sequence',
        [closing['intent_id']], fetch='all')
    assert reductions == [(-1.0,), (-0.5,)]

    with _constructed_executor(ctx.path, event_store=ctx.events,
                               sdk_factory=lambda: ctx.sdk) as restarted:
        restarted._reconcile_intents(ctx.owner, ctx.contract.conId)
        restarted._snapshot_cache = None
        restarted._reconcile_intents(ctx.owner, ctx.contract.conId)
        assert restarted.state.open_position(ctx.owner, ctx.contract.conId) == half
        saved = restarted.intents.all(
            strategy=ctx.owner, conid=ctx.contract.conId, kind='CLOSE')
        assert len(saved) == 1 and saved[0]['intent_id'] == closing['intent_id']
        assert saved[0]['status'] == 'WORKING' and saved[0]['payload']['cumulative_filled'] == 1.5
        positions, checkpoints = restarted.state.ownership_snapshot(saved)
        assert len(positions) == 1 and positions[0]['quantity'] == 0.5
        assert checkpoints == {closing['intent_id']: 1.5}
    assert len(ctx.server.placed) == 2 and ctx.server.inventory == 100.5
    assert ctx.server.inventory - half['quantity'] == 100
