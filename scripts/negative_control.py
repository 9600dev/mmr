#!/usr/bin/env python3
"""What does the backtest pipeline report when there is definitionally nothing?

WHY THIS EXISTS
    Reading a backtest without knowing what the same machine produces from
    noise is like reading a scale without knowing its zero. The sweep pipeline
    runs N configurations and keeps the best; the best of N draws from a random
    walk looks good, and looks better the larger N is. Until you have measured
    that, "profit factor 1.8, Sharpe 3.4" is a number without a unit.

    So this runs the REAL pipeline — same strategy class, same parameter grid,
    same Backtester, same execution semantics — over instruments whose returns
    are i.i.d. zero-mean by construction. Every dollar it reports is
    manufactured. The distribution of those results is the calibration floor:
    any live candidate must clear what the machine invents from nothing, and
    the gap between the two is the only part that could be edge.

    Prompted by finding PBO 58-70% on the ORB family across four independent
    sweeps (2026-07-28) — the in-sample winner landing in the BOTTOM half out
    of sample more often than not. That is what an uninformative search looks
    like, and this quantifies how much apparent performance such a search can
    generate.

ISOLATION
    Synthetic bars are written to their own DuckDB under the scratch path and
    NEVER to the live store. Fake bars in the real history would be a far worse
    problem than the one this script exists to measure, and unlike a bad vendor
    bar they would carry no signature that says so.

USAGE
    python3 scripts/negative_control.py --instruments 20 --days 250
    python3 scripts/negative_control.py --strategy strategies/vwap_reclaim.py \
        --class VwapReclaim --grid '{"MIN_DIP_PCT":[0.2,0.4,0.6]}'
"""
from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import os
import statistics
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np  # noqa: E402

from trader.data.data_access import TickData, TickStorage  # noqa: E402
from trader.objects import BarSize  # noqa: E402
from trader.simulation.backtester import BacktestConfig, Backtester  # noqa: E402
from trader.simulation.synthetic_markets import driftless_session_bars  # noqa: E402


# Conids far outside any real IB range, so a stray row is unmistakable.
_BASE_CONID = 900_000_000


def build_null_store(path: str, instruments: int, days: int,
                     start_date: str) -> list[int]:
    """Generate and persist ``instruments`` independent edge-free series."""
    storage = TickStorage(path)
    tick: TickData = storage.get_tickdata(BarSize.Mins1)
    conids = []
    for i in range(instruments):
        conid = _BASE_CONID + i
        df = driftless_session_bars(days=days, seed=1000 + i,
                                    start_price=50.0 + 5.0 * i,
                                    start_date=start_date)
        # Goes through the same write chokepoint as real data, so the bars are
        # validated by bar_quality on the way in. A generator that produced
        # impossible bars would be refused here rather than quietly measured.
        tick.write(str(conid), df)
        conids.append(conid)
        print(f'  wrote {len(df):,} bars for synthetic conid {conid}', flush=True)
    return conids


def _run_one(db: str, config, strategy: str, class_name: str,
             conid: int, cell: dict):
    """One (instrument, parameter-cell) backtest, in its own process.

    Each worker builds its own TickStorage: DuckDB serialises access per file
    and `execute_atomic` retries on contention, so concurrent readers are safe.
    """
    storage = TickStorage(db)
    bt = Backtester(storage, config)
    r = bt.run_from_module(strategy, class_name, [conid], params=dict(cell))
    return {
        'conid': conid, 'params': cell,
        'sharpe': r.sharpe_ratio, 'profit_factor': r.profit_factor,
        'total_return': r.total_return, 'trades': r.total_trades,
        'win_rate': r.win_rate, 'max_drawdown': r.max_drawdown,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--strategy', default='strategies/opening_range_breakout.py')
    ap.add_argument('--class', dest='class_name', default='OpeningRangeBreakout')
    ap.add_argument('--grid', default='{"RANGE_MINUTES":[15,30,45],'
                                      '"VOLUME_MULT":[1.2,1.5,2.0]}')
    ap.add_argument('--instruments', type=int, default=20)
    ap.add_argument('--days', type=int, default=250)
    ap.add_argument('--start-date', default='2025-07-01')
    ap.add_argument('--live-semantics', action='store_true',
                    help='AutoExecutor semantics (no pyramiding, fixed notional)')
    ap.add_argument('--db', default=None, help='where to put the throwaway store')
    ap.add_argument('--json-out', default=None)
    ap.add_argument('--workers', type=int, default=max(1, (os.cpu_count() or 4) - 2),
                    help='parallel backtest processes (DuckDB reads are '
                         'concurrent-safe; each worker opens its own connection)')
    args = ap.parse_args()

    grid = json.loads(args.grid)
    cells = [dict(zip(grid, combo)) for combo in itertools.product(*grid.values())]

    db = args.db or os.path.join(
        tempfile.gettempdir(), 'mmr_negative_control.duckdb')
    if os.path.exists(db):
        os.remove(db)

    print(f'negative control: {args.class_name} over {args.instruments} '
          f'edge-free instruments x {len(cells)} parameter cells '
          f'= {args.instruments * len(cells)} runs')
    print(f'store: {db}  (isolated — the live history is never touched)\n')

    conids = build_null_store(db, args.instruments, args.days, args.start_date)

    storage = TickStorage(db)
    start = dt.datetime.fromisoformat(args.start_date)
    config = BacktestConfig(
        start_date=start - dt.timedelta(days=2),
        end_date=start + dt.timedelta(days=int(args.days * 1.5)),
        bar_size=BarSize.Mins1,
        execution_mode='live' if args.live_semantics else 'accumulate',
    )

    jobs = [(c, cell) for c in conids for cell in cells]
    print(f'\nrunning {len(jobs)} backtests ({config.execution_mode} semantics, '
          f'{args.workers} workers)...', flush=True)
    results = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(_run_one, db, config, args.strategy, args.class_name,
                        conid, cell): (conid, cell)
            for conid, cell in jobs
        }
        for fut in as_completed(futures):
            conid, cell = futures[fut]
            try:
                res = fut.result()
            except Exception as exc:                     # noqa: BLE001
                print(f'  run failed conid={conid} {cell}: {exc}', flush=True)
                continue
            if res is not None:
                results.append(res)
            if len(results) % 20 == 0:
                print(f'  {len(results)}/{len(jobs)} done', flush=True)

    if not results:
        print('no runs completed', file=sys.stderr)
        return 1

    def col(k):
        return [r[k] for r in results
                if r[k] is not None and np.isfinite(r[k])]

    sharpes, pfs, rets = col('sharpe'), col('profit_factor'), col('total_return')
    best = max(results, key=lambda r: r['sharpe'] if r['sharpe'] is not None else -1e18)

    print('\n' + '=' * 72)
    print('WHAT THE PIPELINE FOUND IN DATA WITH NOTHING IN IT')
    print('=' * 72)
    print(f'  runs completed        {len(results)}')
    print(f'  median trades/run     {statistics.median(col("trades")):.0f}')
    print()
    for name, vals in (('Sharpe', sharpes), ('profit factor', pfs),
                       ('total return', rets)):
        if not vals:
            continue
        print(f'  {name:<16} median {statistics.median(vals):>8.3f}   '
              f'p90 {np.percentile(vals, 90):>8.3f}   '
              f'max {max(vals):>8.3f}')
    print()
    print(f'  BEST RUN: Sharpe {best["sharpe"]:.2f}, '
          f'PF {best["profit_factor"]:.2f}, '
          f'return {best["total_return"]:.1%}, '
          f'{best["trades"]} trades, params {best["params"]}')
    print()
    print('  Every one of those numbers came from a driftless random walk.')
    print('  Read them as the pipeline\'s zero: a real candidate has to clear')
    print('  this, not merely be positive.')

    if args.json_out:
        with open(args.json_out, 'w') as fh:
            json.dump({'config': vars(args), 'results': results}, fh, indent=2)
        print(f'\n  detail written to {args.json_out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
