#!/usr/bin/env python3
"""Measure storage costs for the architecture review without operational data.

    .venv/bin/python scripts/review_storage_benchmark.py > /tmp/mmr-review-storage-performance.json

All database files live in an automatically removed temporary directory. The
only child process opens that temporary database for one second; it never
connects to a broker or an MMR service. Results are descriptive measurements,
not pass/fail performance thresholds.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import os
from pathlib import Path
import platform
import selectors
import statistics
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _finish_child(child: subprocess.Popen) -> None:
    if child.poll() is None:
        child.terminate()
    try:
        child.communicate(timeout=3)
    except subprocess.TimeoutExpired:
        child.kill()
        child.communicate(timeout=3)


async def _measure_broker_loop_contention(db_path: str) -> dict:
    from trader.data.event_store import EventStore
    from trader.trading.order_lifecycle import OrderLifecycleTracker

    tracker = OrderLifecycleTracker(EventStore(db_path))
    child = subprocess.Popen(
        [sys.executable, '-u', '-c',
         'import duckdb,sys,time; c=duckdb.connect(sys.argv[1]); '
         'print("READY",flush=True);time.sleep(1.0);c.close()', db_path],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    pulse = None
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + 15.0
            captured = b''
            while b'READY' not in captured.splitlines():
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise RuntimeError('temporary database holder did not initialize')
                chunk = os.read(child.stdout.fileno(), 65536)
                if not chunk:
                    raise RuntimeError('temporary database holder failed before readiness')
                captured += chunk
        heartbeats = []

        async def heartbeat():
            while True:
                heartbeats.append(time.monotonic())
                await asyncio.sleep(.01)

        pulse = asyncio.create_task(heartbeat())
        await asyncio.sleep(.01)
        trade = SimpleNamespace(
            order=SimpleNamespace(orderId=1, totalQuantity=10, orderRef='review', action='BUY'),
            orderStatus=SimpleNamespace(status='Filled', filled=10, avgFillPrice=100),
            contract=SimpleNamespace(conId=123, symbol='AUDIT'),
        )
        started = time.monotonic()
        tracker.on_trade(trade)
        elapsed = time.monotonic() - started
        await asyncio.sleep(.025)
        gaps = [b - a for a, b in zip(heartbeats, heartbeats[1:])]
        _, error = child.communicate(timeout=3)
        if child.returncode != 0:
            raise RuntimeError(error.decode())
        return {
            'other_process_holds_connection_s': 1.0,
            'heartbeat_requested_interval_s': .01,
            'on_trade_elapsed_s': elapsed,
            'max_event_loop_heartbeat_gap_s': max(gaps),
        }
    finally:
        if pulse is not None:
            pulse.cancel()
            await asyncio.gather(pulse, return_exceptions=True)
        _finish_child(child)
        tracker.close()


def _measure_select(db_path: str) -> dict:
    from trader.data.duckdb_store import DuckDBConnection

    db = DuckDBConnection.get_instance(db_path)
    samples = []
    for _ in range(50):
        started = time.monotonic()
        db.execute('SELECT 1', fetch='one')
        samples.append(time.monotonic() - started)
    return {
        'samples': len(samples),
        'median_ms': statistics.median(samples) * 1000,
        'p95_ms': sorted(samples)[47] * 1000,
        'description': 'One SELECT 1 with connection open and close per call',
    }


def _measure_history_merge(db_path: str) -> dict:
    import pandas as pd
    from trader.data.data_access import TickData

    ticks = TickData(db_path, '1 min')
    bars = pd.DataFrame(
        {
            'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.0,
            'volume': 100.0, 'bar_size': '1 min',
        },
        index=pd.date_range('2025-01-01', periods=100000, freq='min', tz='UTC', name='date'),
    )
    ticks.write(123, bars)
    update = bars.iloc[-10:].copy()
    samples = []
    for _ in range(3):
        started = time.monotonic()
        ticks.write_resolve_overlap(123, update)
        samples.append(time.monotonic() - started)
    return {
        'existing_bars': len(bars),
        'incoming_bars': len(update),
        'samples_s': samples,
        'median_s': statistics.median(samples),
        'description': 'Overwrite the final 10 of 100000 valid one-minute bars',
    }


def main() -> None:
    with tempfile.TemporaryDirectory(prefix='mmr-review-storage-') as directory:
        # Logging must be configured before the first trader import. Keep both
        # its files and terminal output separate from operational logs / JSON.
        os.environ['MMR_LOG_DIR'] = str(Path(directory) / 'logs')
        os.environ['LOG_CFG'] = str(Path(__file__).resolve().parents[1] / 'config_defaults' / 'logging.yaml')
        # setup_logging also seeds missing configuration files on import.
        # Redirect that initialization before importing any logging consumer.
        import trader.container as container

        container.MMR_CONFIG_DIR = Path(directory) / 'config'
        with contextlib.redirect_stdout(sys.stderr):
            import duckdb

            event_db = str(Path(directory) / 'events.duckdb')
            result = {
                'recorded_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
                'environment': {
                    'python': platform.python_version(),
                    'duckdb': duckdb.__version__,
                    'platform': platform.platform(),
                },
                'broker_event_loop_contention': asyncio.run(_measure_broker_loop_contention(event_db)),
                'select_open_close': _measure_select(event_db),
                'history_merge': _measure_history_merge(str(Path(directory) / 'history.duckdb')),
            }
        print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
