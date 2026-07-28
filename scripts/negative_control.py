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


def _run_instrument(db_dir: str, config, strategy: str, class_name: str,
                    index: int, days: int, start_date: str, cells: list,
                    annual_drift: float = 0.0):
    """Generate ONE instrument and run every parameter cell against it.

    Each worker owns its own single-instrument DuckDB. That is not a
    micro-optimisation: pointing 14 processes at one shared store deadlocked
    the first attempt at 0% CPU, because DuckDB takes a per-file lock and every
    worker opens read-write. Per-worker files remove the contention entirely
    and cost nothing, since the data is generated deterministically from the
    seed rather than shared.
    """
    conid = _BASE_CONID + index
    path = os.path.join(db_dir, f'nc_{conid}.duckdb')
    if os.path.exists(path):
        os.remove(path)

    df = driftless_session_bars(days=days, seed=1000 + index,
                                start_price=50.0 + 5.0 * index,
                                start_date=start_date,
                                annual_drift=annual_drift)
    storage = TickStorage(path)
    # Written through the ordinary chokepoint, so bar_quality validates these
    # bars exactly as it would a vendor's.
    storage.get_tickdata(BarSize.Mins1).write(str(conid), df)

    out = []
    for cell in cells:
        bt = Backtester(storage, config)
        try:
            r = bt.run_from_module(strategy, class_name, [conid],
                                   params=dict(cell))
        except Exception as exc:                         # noqa: BLE001
            out.append({'conid': conid, 'params': cell, 'error': str(exc)})
            continue
        out.append({
            'conid': conid, 'params': cell,
            'sharpe': r.sharpe_ratio, 'profit_factor': r.profit_factor,
            'total_return': r.total_return, 'trades': r.total_trades,
            'win_rate': r.win_rate, 'max_drawdown': r.max_drawdown,
        })
    try:
        os.remove(path)
    except OSError:
        pass
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--strategy', default='strategies/opening_range_breakout.py')
    ap.add_argument('--class', dest='class_name', default='OpeningRangeBreakout')
    ap.add_argument('--grid', default='{"RANGE_MINUTES":[15,30,45],'
                                      '"VOLUME_MULT":[1.2,1.5,2.0]}')
    ap.add_argument('--instruments', type=int, default=20)
    ap.add_argument('--days', type=int, default=250)
    ap.add_argument('--start-date', default='2025-07-01')
    ap.add_argument('--annual-drift', type=float, default=0.0,
                    help='annualised drift for the null. 0 = pure noise. Set to '
                         'the market return over the window to strip beta out '
                         'of the comparison, so what remains above the control '
                         'cannot be explained by the market having risen.')
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

    db_dir = args.db or os.path.join(tempfile.gettempdir(), 'mmr_negative_control')
    os.makedirs(db_dir, exist_ok=True)

    print(f'negative control: {args.class_name} over {args.instruments} '
          f'edge-free instruments x {len(cells)} parameter cells '
          f'= {args.instruments * len(cells)} runs')
    print(f'stores: {db_dir}/  (isolated — the live history is never touched)')
    print(f'null: driftless random walk'
          + (f' + {args.annual_drift:.1%}/yr drift (beta-matched)'
             if args.annual_drift else ' (pure noise, no drift)'))

    start = dt.datetime.fromisoformat(args.start_date)
    config = BacktestConfig(
        start_date=start - dt.timedelta(days=2),
        end_date=start + dt.timedelta(days=int(args.days * 1.5)),
        bar_size=BarSize.Mins1,
        execution_mode='live' if args.live_semantics else 'accumulate',
    )
    print(f'running with {args.workers} workers '
          f'({config.execution_mode} semantics)...', flush=True)

    results = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(_run_instrument, db_dir, config, args.strategy,
                        args.class_name, i, args.days, args.start_date, cells,
                        args.annual_drift): i
            for i in range(args.instruments)
        }
        for fut in as_completed(futures):
            i = futures[fut]
            try:
                batch = fut.result()
            except Exception as exc:                     # noqa: BLE001
                print(f'  instrument {i} failed: {exc}', flush=True)
                continue
            ok = [r for r in batch if 'error' not in r]
            results.extend(ok)
            print(f'  instrument {i}: {len(ok)}/{len(cells)} cells '
                  f'({len(results)}/{args.instruments * len(cells)} total)',
                  flush=True)

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
