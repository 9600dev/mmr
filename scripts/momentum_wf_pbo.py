#!/usr/bin/env python3
"""Long-only 12-1 momentum on the point-in-time universe: PBO + walk-forward.

THE QUESTION
    The 2026-07-29 PIT re-derivation left one positive number undischarged:
    long-only momentum at Sharpe 0.75 (costs charged, survivorship-free
    universe). Every other number that looked that good this month died under
    one of two tests — PBO (was the winning cell information or the best of N
    draws?) and walk-forward (what does the RULE earn when it must choose
    parameters before seeing the window it is judged on?). This script runs
    both. Either outcome is decisive: survive and it is the first deployable
    candidate; die and the price-signal well is dry.

WHAT IS HELD IDENTICAL TO THE PRIOR WORK
    Same panel (`load_pit_panel`, monthly membership from the PRIOR month's
    dollar-volume rank), same execution (`Backtester.run_panel`: fill at t+1's
    open, 5bps slippage, $0.005/sh commission), same strategy class
    (`XsMomentum`, SHORT_ENABLED=0), same grid as the L/S PBO run
    (lookback 189/252/315 x rebalance 5/10/21). Long-only, so borrow does not
    apply.

HONESTY NOTES, WRITTEN BEFORE THE RESULT
    * The grid prices 9 trials. It does NOT price the choices made upstream
      after seeing data: long-only itself (chosen after L/S disappointed),
      quintiles, the 21-day skip. PBO here indicts the lookback x rebalance
      selection only; the true trial count is larger and unknowable.
    * Fold 0's warm-up is slightly short (the panel starts 2016-08; the first
      training window needs history from ~2016-04). Cells with lookback 315
      start trading a few weeks late inside a 2-year training window. Noted
      rather than fixed: shifting folds later would discard the most recent
      year, which is the data that matters most.
    * The walk-forward benchmark is an equal-weight book of the SAME panel
      over the SAME stitched test windows through the SAME execution path.
      Long-only momentum must beat owning the universe, or the ranking added
      nothing.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np      # noqa: E402
import pandas as pd     # noqa: E402

GRID = [{'LOOKBACK': lb, 'REBALANCE_EVERY': rb}
        for lb in (189, 252, 315) for rb in (5, 10, 21)]
WARMUP_CAL_DAYS = 460          # 315 trading days x ~1.45, rounded up
TRAIN_CAL_DAYS = 730           # 2 years
TEST_CAL_DAYS = 365            # 1 year
ANN = math.sqrt(252.0)


def _load_class(path, name):
    spec = importlib.util.spec_from_file_location(name.lower(), path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, name)


class _EqualWeightPanel:
    """Inline EW benchmark: strategies/equal_weight.py casts conids to int,
    which a ticker-keyed panel cannot survive. Same book otherwise."""
    REBALANCE_EVERY = 21

    def precompute_panel(self, panel):
        return {'close': panel['close']}

    def on_panel(self, panel, state, index):
        if self.REBALANCE_EVERY > 1 and index % self.REBALANCE_EVERY:
            return None
        row = state['close'].iloc[index].dropna()
        if len(row) < 2:
            return None
        return {c: 1.0 / len(row) for c in row.index}


def _metrics(curve: pd.Series, measure_start) -> dict | None:
    sub = curve[curve.index >= pd.Timestamp(measure_start)]
    if len(sub) < 30:
        return None
    rets = sub.pct_change().dropna()
    sd = rets.std(ddof=1)
    sharpe = float(rets.mean() / sd * ANN) if sd > 0 else 0.0
    total = float(sub.iloc[-1] / sub.iloc[0] - 1.0)
    running = sub.cummax()
    max_dd = float(((sub - running) / running).min())
    return {'sharpe': sharpe, 'return': total, 'max_dd': max_dd,
            'n_days': len(sub), 'daily_returns': rets}


class Runner:
    def __init__(self):
        from pit_panel import load_pit_panel
        from trader.objects import BarSize
        from trader.simulation.backtester import Backtester, BacktestConfig

        close, opens = load_pit_panel(top_n=500)
        self.tickers = list(close.columns)
        self.frames = {}
        for t in self.tickers:
            df = pd.DataFrame({'open': opens[t], 'close': close[t]}).dropna(
                subset=['close'])
            if len(df) >= 2:
                self.frames[t] = df
        self.panel_start = close.index[0]
        self.panel_end = close.index[-1]
        self.xs_cls = _load_class(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'strategies', 'xs_momentum.py'), 'XsMomentum')
        self.config = BacktestConfig(
            start_date=self.panel_start.to_pydatetime(),
            end_date=self.panel_end.to_pydatetime(),
            initial_capital=1_000_000.0,
            bar_size=BarSize.Days1,
            slippage_bps=5.0,
            commission_per_share=0.005,
        )
        self.bt = Backtester(config=self.config, storage=None)
        self._runs = 0

    def _slice(self, start, end):
        out = {}
        s, e = pd.Timestamp(start), pd.Timestamp(end)
        for t, df in self.frames.items():
            w = df.loc[(df.index >= s) & (df.index < e)]
            if len(w) >= 2 and w['close'].notna().any():
                out[t] = w
        return out

    def run_cell(self, cell, data_start, end, measure_start):
        frames = self._slice(data_start, end)
        if len(frames) < 2:
            return None
        strat = self.xs_cls()
        strat.SHORT_ENABLED = 0
        for k, v in cell.items():
            setattr(strat, k, v)
        result = self.bt.run_panel(strat, [], frames=frames)
        self._runs += 1
        return _metrics(result.equity_curve, measure_start)

    def run_ew(self, data_start, end, measure_start):
        frames = self._slice(data_start, end)
        if len(frames) < 2:
            return None
        result = self.bt.run_panel(_EqualWeightPanel(), [], frames=frames)
        self._runs += 1
        return _metrics(result.equity_curve, measure_start)


def main() -> int:
    from trader.simulation.selection_bias import deflated_sharpe, pbo_cscv
    from trader.simulation.walk_forward import (
        plan_fold_offsets, resolve_folds, run_walk_forward,
        selection_stability)

    t0 = dt.datetime.now()
    r = Runner()
    print(f'panel {r.panel_start.date()} -> {r.panel_end.date()}, '
          f'{len(r.frames)} tradeable names', flush=True)

    # ---- Phase A: full-period grid -> PBO + DSR --------------------------
    print('\n=== PHASE A: full-period grid (9 cells, long-only) ===',
          flush=True)
    measure_start = r.panel_start + pd.Timedelta(days=WARMUP_CAL_DAYS)
    cols, cells_out = {}, []
    for cell in GRID:
        m = r.run_cell(cell, r.panel_start, r.panel_end + pd.Timedelta(days=1),
                       measure_start)
        label = f"lb{cell['LOOKBACK']}/rb{cell['REBALANCE_EVERY']}"
        if m is None:
            print(f'  {label:<12} FAILED', flush=True)
            continue
        cols[label] = m['daily_returns']
        cells_out.append({'cell': cell, 'label': label,
                          'sharpe': m['sharpe'], 'return': m['return'],
                          'max_dd': m['max_dd']})
        print(f"  {label:<12} sharpe {m['sharpe']:>5.2f}  "
              f"return {100*m['return']:>8.1f}%  maxDD {100*m['max_dd']:>6.1f}%",
              flush=True)

    ew_full = r.run_ew(r.panel_start, r.panel_end + pd.Timedelta(days=1),
                       measure_start)
    if ew_full:
        print(f"  {'equal-weight':<12} sharpe {ew_full['sharpe']:>5.2f}  "
              f"return {100*ew_full['return']:>8.1f}%  "
              f"maxDD {100*ew_full['max_dd']:>6.1f}%", flush=True)

    matrix = pd.DataFrame(cols).dropna()
    print(f'\n  returns matrix {matrix.shape[0]} days x {matrix.shape[1]} '
          f'trials', flush=True)
    pbo_results = {}
    for s in (8, 12, 16):
        res = pbo_cscv(matrix.values, n_splits=s)
        if res is not None:
            pbo_results[s] = res
            cv = f'  [{res.caveat}]' if res.caveat else ''
            print(f'  PBO (S={s:>2}): {res.pbo:.3f}  '
                  f'median OOS rank of IS winner: {res.median_oos_rank:.2f}{cv}',
                  flush=True)

    best = max(cells_out, key=lambda c: c['sharpe'])
    trial_sharpes = [c['sharpe'] for c in cells_out]
    dsr = deflated_sharpe(matrix[best['label']].values, trial_sharpes)
    print(f"  best cell {best['label']} sharpe {best['sharpe']:.2f} -> "
          f"DSR {dsr if dsr is None else round(dsr, 3)} "
          f'(9 priced trials; long-only/quintile/skip choices are unpriced)',
          flush=True)

    # ---- Phase B: walk-forward ------------------------------------------
    print('\n=== PHASE B: walk-forward (rolling, train 2y, test 1y) ===',
          flush=True)
    span = TRAIN_CAL_DAYS + 7 * TEST_CAL_DAYS
    wf_start = (r.panel_end - pd.Timedelta(days=span)).date()
    offsets = plan_fold_offsets(span, TRAIN_CAL_DAYS, TEST_CAL_DAYS)
    folds = resolve_folds(wf_start, offsets, warmup_days=WARMUP_CAL_DAYS)
    print(f'  {len(folds)} folds, first test {folds[0].test_start}, '
          f'last test ends {folds[-1].test_end}', flush=True)

    def run_backtest(cell, data_start, end):
        ms = pd.Timestamp(data_start) + pd.Timedelta(days=WARMUP_CAL_DAYS)
        return r.run_cell(cell, data_start, end, ms)

    def score(metrics):
        return metrics.get('sharpe') if metrics else None

    def on_fold(fr):
        ts = fr.test_metrics.get('sharpe')
        tr = fr.test_metrics.get('return')
        tr_s = 'n/a' if fr.train_score is None else f'{fr.train_score:.2f}'
        print(f'  fold {fr.fold.index}: chose {fr.chosen} '
              f'(train sharpe {tr_s}) -> test sharpe '
              f'{ts if ts is None else round(ts, 2)}, return '
              f'{tr if tr is None else f"{100*tr:+.1f}%"}', flush=True)

    results = run_walk_forward(folds, GRID, run_backtest, score,
                               on_fold=on_fold)

    stability = selection_stability(results)
    stitched = pd.concat([fr.test_metrics['daily_returns']
                          for fr in results if fr.test_metrics])
    oos_curve = (1 + stitched).cumprod()
    oos_sharpe = float(stitched.mean() / stitched.std(ddof=1) * ANN) \
        if stitched.std(ddof=1) > 0 else 0.0
    oos_total = float(oos_curve.iloc[-1] - 1.0)
    years = len(stitched) / 252.0
    oos_cagr = float((1 + oos_total) ** (1 / years) - 1) if years > 0 else 0.0
    running = oos_curve.cummax()
    oos_dd = float(((oos_curve - running) / running).min())

    print('\n  EW benchmark over the same test windows:', flush=True)
    ew_rets = []
    for fr in results:
        f = fr.fold
        m = r.run_ew(f.test_data_start, f.test_end, f.test_start)
        if m:
            ew_rets.append(m['daily_returns'])
    ew_stitched = pd.concat(ew_rets)
    ew_sharpe = float(ew_stitched.mean() / ew_stitched.std(ddof=1) * ANN) \
        if ew_stitched.std(ddof=1) > 0 else 0.0
    ew_total = float((1 + ew_stitched).prod() - 1.0)
    ew_cagr = float((1 + ew_total) ** (1 / years) - 1) if years > 0 else 0.0

    print(f'\n=== VERDICT INPUTS ===')
    print(f'  walk-forward OOS : sharpe {oos_sharpe:.2f}, '
          f'CAGR {100*oos_cagr:+.2f}%, maxDD {100*oos_dd:.1f}%, '
          f'{len(stitched)} days over {len(results)} folds')
    print(f'  equal-weight     : sharpe {ew_sharpe:.2f}, '
          f'CAGR {100*ew_cagr:+.2f}% over the same windows')
    print(f'  selection stability: '
          f'{stability if stability is None else round(stability, 2)}')
    print(f'  PBO: ' + ', '.join(f'S={s}: {p.pbo:.2f}'
                                 for s, p in pbo_results.items()))
    print(f'  runtime {dt.datetime.now() - t0}, {r._runs} panel runs',
          flush=True)

    out = {
        'generated': dt.datetime.now().isoformat(),
        'phase_a': {'cells': [{k: v for k, v in c.items()}
                              for c in cells_out],
                    'equal_weight_full': None if not ew_full else
                    {k: v for k, v in ew_full.items() if k != 'daily_returns'},
                    'pbo': {s: {'pbo': p.pbo, 'median_oos_rank':
                                p.median_oos_rank, 'caveat': p.caveat}
                            for s, p in pbo_results.items()},
                    'best': best['label'], 'dsr': dsr},
        'phase_b': {
            'folds': [{'index': fr.fold.index,
                       'test_start': str(fr.fold.test_start),
                       'test_end': str(fr.fold.test_end),
                       'chosen': fr.chosen, 'train_score': fr.train_score,
                       'test_sharpe': fr.test_metrics.get('sharpe'),
                       'test_return': fr.test_metrics.get('return')}
                      for fr in results],
            'stability': stability,
            'oos': {'sharpe': oos_sharpe, 'cagr': oos_cagr,
                    'total_return': oos_total, 'max_dd': oos_dd,
                    'n_days': int(len(stitched))},
            'equal_weight': {'sharpe': ew_sharpe, 'cagr': ew_cagr,
                             'total_return': ew_total}},
    }
    path = os.path.expanduser(
        '~/.local/share/mmr/reports/momentum_wf_pbo.json')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as fh:
        json.dump(out, fh, indent=2, default=str)
    print(f'  results -> {path}', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
