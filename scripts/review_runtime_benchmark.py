#!/usr/bin/env python3
"""Measure the real tick-dispatch path with synthetic data and no services.

    .venv/bin/python scripts/review_runtime_benchmark.py --repeats 9

This is a local microbenchmark, not an IB throughput or latency guarantee.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import statistics
import sys
import tempfile
import time
from types import SimpleNamespace

os.environ["MMR_LOG_DIR"] = str(Path(tempfile.gettempdir()) / "mmr-review-logs")
os.environ.setdefault("MMR_PYTEST", "1")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
from ib_async import Contract, Ticker

def benchmark(tick_count: int, strategy_count: int, repeats: int) -> dict:
    from trader.data.market_data import NORMALIZED_COLUMNS
    from trader.objects import BarSize
    from trader.strategy.strategy_runtime import StrategyRuntime
    from trader.trading.strategy import StrategyState

    runtime = StrategyRuntime.__new__(StrategyRuntime)
    index = pd.date_range("2026-09-08 13:30", periods=tick_count, freq="100ms", tz="UTC", name="date")
    ticks = pd.DataFrame(100.0, index=index, columns=NORMALIZED_COLUMNS)
    ticks["volume"] = np.arange(tick_count, dtype=float)
    runtime.streams = {1: ticks}
    runtime._hist_bars = {(1, BarSize.Mins1): pd.DataFrame()}
    runtime._last_dispatched_bar = {}
    runtime._tick_retention_days = 2
    # Exclude calendar lookup and executor/DB work from these measurements;
    # retain normalization, append, full-frame resample, dedup, and dispatch.
    runtime._bar_in_session = lambda contract, timestamp: True
    runtime._check_time_exit = lambda *args: None
    runtime.strategies = {1: [SimpleNamespace(
        name=f"bench_{i}", state=StrategyState.RUNNING, bar_size=BarSize.Mins1,
        on_prices=lambda frame: None,
    ) for i in range(strategy_count)]}
    contract = Contract(conId=1, symbol="SYNTHETIC", secType="STK")
    durations = []
    # Warm pandas resampling once; each subsequent tick is on the same bar,
    # precisely the common path on which strategy evaluation should be cheap.
    for iteration in range(repeats + 1):
        ticker = Ticker(contract=contract,
                        time=(index[-1] + pd.Timedelta(microseconds=iteration + 1)).to_pydatetime())
        ticker.last = 100.0
        ticker.volume = float(tick_count + iteration)
        start = time.perf_counter()
        runtime.on_ticker_next(ticker)
        elapsed_ms = (time.perf_counter() - start) * 1000
        if iteration:
            durations.append(elapsed_ms)
    return {
        "seed_ticks": tick_count,
        "retained_ticks": len(runtime.streams[1]),
        "strategies_on_same_instrument_and_interval": strategy_count,
        "seed_frame_bytes": int(ticks.memory_usage(index=True, deep=True).sum()),
        "retained_frame_bytes": int(runtime.streams[1].memory_usage(index=True, deep=True).sum()),
        "samples": repeats,
        "median_ms_per_tick": round(statistics.median(durations), 3),
        "p95_ms_per_tick": round(float(np.percentile(durations, 95)), 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=9)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    # Logging bootstraps config on import; keep that bootstrap out of the
    # operator's config directory as well as keeping its log files separate.
    import trader.container as container

    with tempfile.TemporaryDirectory(prefix="mmr-review-runtime-") as directory:
        container.MMR_CONFIG_DIR = Path(directory) / "config"
        os.environ["LOG_CFG"] = str(container.MMR_CONFIG_DIR / "logging.yaml")
        result = {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "dependencies": {name: importlib.metadata.version(name) for name in ("pandas", "numpy", "ib_async")},
            "scope": "Synthetic same-bar tick dispatch; excludes broker, DB, calendars, signal execution and network.",
            "measurements": [benchmark(n, strategies, args.repeats)
                             for n in (10_000, 50_000, 200_000) for strategies in (1, 4)],
        }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
