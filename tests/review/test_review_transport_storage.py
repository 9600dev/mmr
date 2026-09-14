"""2026-09 review regressions for transport and durable state.

All sockets use ephemeral localhost ports and all databases are temporary.
These regressions pin the repaired execution contract and are separate from
the human-owned invariant suite.
"""

import asyncio
import datetime as dt
import json
import os
import selectors
import subprocess
import sys
import textwrap
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from trader.data.bar_quality import Bar, check_bar
from trader.data.data_access import TickData
from trader.data.duckdb_store import DuckDBConnection
from trader.data.proposal_store import ProposalStore
from trader.strategy.auto_executor import AutoExecState
from trader.trading.order_lifecycle import OrderLifecycleTracker
from trader.trading.proposal import TradeProposal


def _bars(minutes):
    return pd.DataFrame(
        {
            'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0,
            'volume': 100.0, 'bar_size': '1 min',
        },
        index=pd.DatetimeIndex(
            [pd.Timestamp('2026-01-02T16:00Z') + pd.Timedelta(minutes=m)
             for m in minutes],
            name='date',
        ),
    )


def _probe_after_ready(script, timeout, startup_timeout=60.0):
    """Time only the operation, excluding subprocess imports; always clean up."""
    child = subprocess.Popen(
        [sys.executable, '-u', '-c', textwrap.dedent(script)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        env={**os.environ, 'MMR_PYTEST': '1'},
    )
    captured = b''
    try:
        # Read the descriptor directly so text buffering cannot hide READY
        # after a preceding log line has emptied the kernel pipe.
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + startup_timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise RuntimeError('probe did not initialize: ' + captured.decode(errors='replace'))
                chunk = os.read(child.stdout.fileno(), 65536)
                if not chunk:
                    raise RuntimeError('probe exited before READY: ' + captured.decode(errors='replace'))
                captured += chunk
                if b'READY' in captured.splitlines():
                    break
        try:
            output, _ = child.communicate(timeout=timeout)
            return True, captured.decode() + output, child.returncode
        except subprocess.TimeoutExpired:
            return False, captured.decode(), None
    finally:
        if child.poll() is None:
            child.terminate()
        try:
            child.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            child.kill()
            child.communicate(timeout=3)


@pytest.mark.timeout(100)
def test_default_rpc_deadline_also_bounds_sending():
    finished, output, code = _probe_after_ready(
        '''
        import asyncio
        import socket
        from trader.messaging.clientserver import RPCClient
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        client = RPCClient(zmq_server_port=port)
        asyncio.run(client.connect())
        print('READY', flush=True)
        try:
            client.rpc().ping()
        except (ConnectionError, TimeoutError):
            print('BOUNDED_FAILURE', flush=True)
        finally:
            client.close()
            client.ctx.term()
        ''',
        timeout=11.5,  # documented/default response budget is 10 seconds
    )
    assert finished, 'RPC to absent peer still blocked after its 10s budget'
    assert code == 0 and 'BOUNDED_FAILURE' in output


def test_concurrent_history_merges_preserve_both_updates(tmp_path, monkeypatch):
    ticks = TickData(str(tmp_path / 'history.duckdb'), '1 min')
    ticks.write(123, _bars([0, 10]))
    original_read = ticks.read
    read_barrier = threading.Barrier(2)

    def read_same_snapshot(*args, **kwargs):
        result = original_read(*args, **kwargs)
        read_barrier.wait(timeout=10)
        return result

    monkeypatch.setattr(ticks, 'read', read_same_snapshot)
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(ticks.write_resolve_overlap, 123, _bars([minute]))
                for minute in (5, 6)]
        for job in jobs:
            job.result(timeout=15)
    persisted = original_read(123)
    assert sorted(persisted.index.minute.tolist()) == [0, 5, 6, 10]


@pytest.mark.parametrize('missing_column', ['high', 'volume'])
def test_incomplete_bar_cannot_enter_queryable_history(tmp_path, missing_column):
    ticks = TickData(str(tmp_path / 'history.duckdb'), '1 min')
    try:
        ticks.write(123, _bars([0]).drop(columns=missing_column))
    except (TypeError, ValueError):
        pass  # Loud rejection is also an acceptable fail-closed result.
    persisted = ticks.read(123)
    errors = []
    for ts, row in persisted.iterrows():
        bar = Bar(ts.timestamp(), row.open, row.high, row.low, row.close, row.volume)
        errors.extend(f.rule for f in check_bar(bar, 0) if f.severity == 'error')
    assert not errors, f'invalid bars were committed to tick_data: {errors}'


class _RecoveringEventStore:
    def __init__(self):
        self.calls = 0
        self.events = []

    def append(self, event):
        self.calls += 1
        if self.calls == 1:
            raise OSError('temporary audit-store write failure')
        self.events.append(event)


def _filled_trade():
    return SimpleNamespace(
        order=SimpleNamespace(orderId=1, totalQuantity=10, orderRef='review', action='BUY'),
        orderStatus=SimpleNamespace(status='Filled', filled=10, avgFillPrice=100),
        contract=SimpleNamespace(conId=123, symbol='AUDIT'),
    )


def test_terminal_fill_is_retried_after_transient_persistence_failure():
    store = _RecoveringEventStore()
    tracker = OrderLifecycleTracker(store)
    trade = _filled_trade()
    tracker.on_trade(trade)  # the first database append fails
    tracker.on_trade(trade)  # same terminal observation after storage recovers
    assert len(store.events) == 1
    assert store.events[0].quantity == 10


def test_bar_dedup_matches_the_same_instant_in_different_zones(tmp_path):
    state = AutoExecState(str(tmp_path / 'state.duckdb'))
    utc = dt.datetime(2026, 9, 8, 16, tzinfo=dt.timezone.utc)
    pacific = utc.astimezone(ZoneInfo('America/Vancouver'))
    state.log_decision('review', 123, utc, 'BUY', 'open', '')
    assert state.executed_for_bar('review', 123, utc)
    assert state.executed_for_bar('review', 123, pacific)


def test_bar_dedup_distinguishes_both_occurrences_of_dst_hour(tmp_path):
    state = AutoExecState(str(tmp_path / 'state.duckdb'))
    zone = ZoneInfo('America/Vancouver')
    first = dt.datetime(2025, 11, 2, 1, 30, tzinfo=zone, fold=0)
    second = dt.datetime(2025, 11, 2, 1, 30, tzinfo=zone, fold=1)
    assert first.timestamp() != second.timestamp()
    state.log_decision('review', 123, first, 'BUY', 'open', '')
    assert not state.executed_for_bar('review', 123, second)


def test_failed_atomic_callback_preserves_previous_disabled_state(tmp_path):
    db = DuckDBConnection.get_instance(str(tmp_path / 'state.duckdb'))
    db.execute('CREATE TABLE strategy_state(name VARCHAR PRIMARY KEY, enabled BOOLEAN)')
    db.execute("INSERT INTO strategy_state VALUES ('review', false)")

    def interrupted_update(conn):
        conn.execute("DELETE FROM strategy_state WHERE name = 'review'")
        raise OSError('interruption before replacement INSERT')

    with pytest.raises(OSError):
        db.execute_atomic(interrupted_update)
    assert db.execute('SELECT enabled FROM strategy_state', fetch='all') == [(False,)]


@pytest.mark.timeout(100)
def test_publisher_bind_failure_returns_to_the_caller():
    finished, output, code = _probe_after_ready(
        '''
        import zmq
        from trader.messaging.clientserver import MultithreadedTopicPubSub
        ctx = zmq.Context()
        holder = ctx.socket(zmq.PUB)
        port = holder.bind_to_random_port('tcp://127.0.0.1')
        publisher = MultithreadedTopicPubSub('tcp://127.0.0.1', port)
        print('READY', flush=True)
        try:
            publisher.start()
        except Exception:
            print('BIND_FAILED', flush=True)
        finally:
            holder.close(linger=0)
            ctx.term()
        ''',
        timeout=2,
    )
    assert finished, 'publisher worker died but caller remains blocked in start()'
    assert code == 0 and 'BIND_FAILED' in output


def test_control_sequential_history_merges_preserve_updates(tmp_path):
    ticks = TickData(str(tmp_path / 'history.duckdb'), '1 min')
    ticks.write(123, _bars([0, 10]))
    ticks.write_resolve_overlap(123, _bars([5]))
    ticks.write_resolve_overlap(123, _bars([6]))
    assert sorted(ticks.read(123).index.minute.tolist()) == [0, 5, 6, 10]


def test_control_invalid_full_ohlc_bar_is_quarantined(tmp_path):
    ticks = TickData(str(tmp_path / 'history.duckdb'), '1 min')
    bad = _bars([0])
    bad['high'] = 8.0
    ticks.write(123, bad)
    assert ticks.read(123).empty
    count = ticks.library._db.execute('SELECT count(*) FROM tick_data_quarantine', fetch='one')
    assert count == (1,)


@pytest.mark.timeout(120)
def test_control_proposal_claim_is_exclusive_across_processes(tmp_path):
    dbpath = str(tmp_path / 'proposals.duckdb')
    store = ProposalStore(dbpath)
    proposal_id = store.add(TradeProposal('AUDIT', 'BUY', quantity=1))
    release = tmp_path / 'release'
    child_script = '''
import json
import sys
import time
from pathlib import Path
from trader.data.proposal_store import ProposalStore
store = ProposalStore(sys.argv[1])
Path(sys.argv[3]).touch()
deadline = time.monotonic() + 60
while not Path(sys.argv[4]).exists():
    if time.monotonic() >= deadline:
        raise TimeoutError('parent did not release concurrent claim')
    time.sleep(.01)
result = store.try_transition(int(sys.argv[2]), 'PENDING', 'APPROVED')
print(json.dumps({'claimed': result}))
'''
    children = []
    ready_paths = [tmp_path / 'ready0', tmp_path / 'ready1']
    try:
        for ready in ready_paths:
            children.append(subprocess.Popen(
                [sys.executable, '-u', '-c', child_script, dbpath, str(proposal_id),
                 str(ready), str(release)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            ))
        deadline = time.monotonic() + 60
        while not all(path.exists() for path in ready_paths):
            if not all(child.poll() is None for child in children):
                raise RuntimeError('claim worker failed before barrier')
            if time.monotonic() >= deadline:
                raise RuntimeError('claim workers did not initialize')
            time.sleep(.01)
        release.touch()
        outcomes = []
        for child in children:
            output, error = child.communicate(timeout=15)
            assert child.returncode == 0, error
            outcomes.append(json.loads(output.strip().splitlines()[-1])['claimed'])
        assert sorted(outcomes) == [False, True]
        assert store.get(proposal_id).status == 'APPROVED'
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
            try:
                child.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                child.kill()
                child.communicate(timeout=3)
