"""The evaluation protocol (D01) for one registered candidate (D02).

Order of operations, and why it is fixed:
  1. registry lookup - a missing family, or an id it does not contain, still
     gets a (schema-complete) ledger line;
  2. panel load (research DBs only, D03) covering dev period + warm-up;
  3. lookahead probes on EVERY executable cell and EVERY benchmark - BEFORE
     any metric exists; there is no bypass;
  4. rolling walk-forward: selection on train windows only; the chosen cell
     and every benchmark run on the test window through the same path and
     costs, the benchmark owning the CHOSEN cell's coverage; a fold in which
     no cell scores errors the evaluation (a fold is never skipped);
  5. 3x cost stress of the stitched OOS windows (no re-selection);
  6. full-period run of the report cell and of every cell (PBO);
  7. active-return statistics, DSR deflated by the family trial count;
  8. results JSON written, ledger line appended, THEN the caller prints.
"""
from __future__ import annotations

import datetime as dt
import json
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from trader.research.lowfreq import config as _cfg
from trader.research.lowfreq import engine, ledger, stats
from trader.research.lowfreq.costs import STRESS_MULTIPLIER, Costs, base_costs
from trader.research.lowfreq.loaders import PricePanel, load_equity_panel, load_etf_panel, to_date
from trader.research.lowfreq.registry import (SEGMENT_RE, Family, RegistryError, default_cell,
                                             entry_sha256, expand_grid, is_safe_segment, load_family)
from trader.research.lowfreq.verdict import GateInputs, decide
from trader.simulation.selection_bias import pbo_cscv
from trader.simulation.walk_forward import (Fold, FoldResult, plan_fold_offsets, resolve_folds,
                                            run_walk_forward, selection_stability)


def _entry_setting(fam: Family, entry: Dict[str, Any], key: str, default):
    return entry.get(key, fam.raw.get(key, default))


def _dates(fam: Family, entry: Dict[str, Any]) -> Tuple[dt.date, dt.date, int, int, int]:
    dev = entry.get('dev_period') or fam.raw.get('dev_period') or {}
    wf = entry.get('walk_forward') or fam.raw.get('walk_forward') or {}
    start = to_date(dev.get('start', fam.dev_start))
    end = to_date(dev.get('end', fam.dev_end))
    return (start, end, int(wf.get('train_days', fam.train_days)), int(wf.get('test_days', fam.test_days)),
            int(_entry_setting(fam, entry, 'warmup_days', fam.warmup_days)))


def _load_panel(fam: Family, entry: Dict[str, Any], data_start: dt.date, end: dt.date) -> PricePanel:
    universe = str(_entry_setting(fam, entry, 'universe', fam.universe))
    if universe == 'etf':
        return load_etf_panel(entry.get('tickers') or None, data_start, end)
    if universe == 'equity':
        return load_equity_panel(data_start, end, top_n=int(_entry_setting(fam, entry, 'top_n', fam.top_n)))
    raise RegistryError(f'unknown universe {universe!r}')


def _stitch(parts: List[pd.Series]) -> pd.Series:
    parts = [p for p in parts if p is not None and len(p)]
    if not parts:
        return pd.Series(dtype=float)
    return pd.concat(parts).sort_index()


def _series_json(s: pd.Series) -> Dict[str, float]:
    return {str(to_date(k)): float(v) for k, v in s.items()}


def _cell_key(cell: Optional[Dict[str, Any]]) -> str:
    return json.dumps(cell or {}, sort_keys=True, default=str)


def _elapsed(t0: float) -> float:
    return round(time.time() - t0, 1)


def _error_text(exc: BaseException) -> str:
    return f'{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}'


def _strategy_sha(entry: Dict[str, Any]) -> Optional[str]:
    try:
        return engine.strategy_source_sha256(
            engine.load_strategy_class(str(entry['strategy']['module']), str(entry['strategy']['class'])))
    except Exception:                                  # noqa: BLE001 - the row records what it can
        return None


def evaluate(target: str, fixed_params: Optional[Dict[str, Any]] = None, start: Optional[str] = None,
             end: Optional[str] = None, root=None) -> Dict[str, Any]:
    """Run the protocol; write results; append the ledger line; return the result dict.

    Exactly one ledger line per call. A target that is not two safe path
    segments (``^[a-z0-9_]+$`` each) is ``refused`` at the root-level ledger
    before any file under the root is touched; a family that cannot be
    loaded, or an id it does not contain, is ``refused`` in the family ledger;
    everything after the entry is resolved - a corrupt ledger, an unhashable
    entry, any failure during the run (including writing the results file) -
    is ``error``; ``ok`` is appended only after the results JSON is on disk.
    """
    t0 = time.time()
    mode = 'fixed' if fixed_params is not None else 'walk_forward'
    root_path = Path(root) if root is not None else _cfg.research_root()
    parts = target.split('/')
    if len(parts) != 2 or not all(is_safe_segment(x) for x in parts):
        # No (safe) family to file it under: the root-level ledger records the call.
        msg = (f'target must be <family>/<candidate_id> with each segment matching '
               f'{SEGMENT_RE.pattern}, got {target!r}')
        ledger.append(root_path / 'trials.jsonl',
                      {'family': None, 'id': target, 'mode': mode, 'status': 'refused', 'error': msg,
                       'fixed_params': dict(fixed_params) if fixed_params is not None else None,
                       'start': start, 'end': end, 'elapsed_s': _elapsed(t0)})
        raise ValueError(msg)
    fam_name, cid = parts
    row: Dict[str, Any] = {'family': fam_name, 'id': cid, 'mode': mode,
                           'fixed_params': dict(fixed_params) if fixed_params is not None else None}
    if fixed_params is not None:
        row['start'], row['end'] = start, end
    try:
        fam = load_family(fam_name, root_path)
    except Exception as exc:                           # noqa: BLE001 - refused, but ledgered
        ledger.append(root_path / fam_name / 'trials.jsonl',
                      dict(row, status='refused', error=_error_text(exc), elapsed_s=_elapsed(t0)))
        raise
    entry: Optional[Dict[str, Any]] = None
    composed: Optional[Dict[str, Any]] = None
    rows: Optional[List[Dict[str, Any]]] = None
    try:
        rows = ledger.read(fam.ledger_path)            # a corrupt ledger is an error row, appended
        row['n_trials'] = ledger.trial_count(rows)
        entry = fam.get(cid)                           # RegistryError -> refused (below)
        row['role'] = entry['role']
        sha = entry_sha256(entry)
        row.update(entry_sha256=sha, grid=entry.get('params') or {},
                   n_trials=ledger.trial_count(rows, including=sha if entry['role'] == 'candidate' else None))
        if fixed_params is None:
            try:
                d0, d1, *_ = _dates(fam, entry)
                row['start'], row['end'] = str(d0), str(d1)
            except Exception:                          # noqa: BLE001 - _evaluate reports it
                pass
        result = _evaluate(fam, entry, sha, fixed_params, start, end, rows)
        result['elapsed_s'] = _elapsed(t0)
        name = (f'{cid}.json' if mode == 'walk_forward'
                else f"{cid}__fixed_{result['window']['start']}_{result['window']['end']}.json")
        path = fam.results_dir / name
        result['results_path'] = str(path)
        composed = ledger.compose(dict(
            row, status='ok', verdict=result['verdict'], start=result['window']['start'],
            end=result['window']['end'], strategy_sha256=result['strategy_sha256'],
            metrics=result['ledger_metrics'], n_trials=result['dsr']['n_trials'],
            elapsed_s=result['elapsed_s'], results_path=str(path)))
        result['ledger_row'] = composed
        fam.results_dir.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as fh:
            json.dump(result, fh, indent=1, sort_keys=True, default=str)
    except Exception as exc:                           # noqa: BLE001 - errors are trials too (D02)
        status = 'refused' if entry is None and isinstance(exc, RegistryError) else 'error'
        ledger.append(fam.ledger_path, dict(row, status=status, error=_error_text(exc),
                                            strategy_sha256=_strategy_sha(entry) if entry is not None else None,
                                            elapsed_s=_elapsed(t0)))
        raise
    assert composed is not None
    ledger.append(fam.ledger_path, composed)
    return result


def _evaluate(fam: Family, entry: Dict[str, Any], sha: str, fixed_params, start, end,
              rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    dev_start, dev_end, train_days, test_days, warmup = _dates(fam, entry)
    if fixed_params is not None:
        dev_start, dev_end = to_date(start), to_date(end)
    if dev_end <= dev_start:
        raise ValueError(f'window end {dev_end} must be after start {dev_start}')
    universe = str(_entry_setting(fam, entry, 'universe', fam.universe))
    costs = base_costs(universe)
    stressed = costs.scaled(STRESS_MULTIPLIER)
    registered_cells = expand_grid(entry)
    if fixed_params is not None:
        report_cell = dict(fixed_params)
        if report_cell not in registered_cells:
            raise RegistryError(
                f'{entry["id"]}: fixed cell {report_cell} is not one of the {len(registered_cells)} '
                f'registered grid cells; fixed-cell mode evaluates pre-registered cells only (D02)')
        cells = [report_cell]
    else:
        report_cell = default_cell(entry)
        cells = registered_cells
    bench_ids = list(entry.get('benchmarks') or [])
    bench_entries = {b: fam.get(b) for b in bench_ids}
    warm = dt.timedelta(days=warmup)
    end_excl = dev_end + dt.timedelta(days=1)

    panel = _load_panel(fam, entry, dev_start - warm, dev_end)
    dv = panel.fields.get('dollar_volume')
    if dv is not None:                                    # capacity diagnostic (D05), backward-looking
        panel.fields['adv_usd_21d'] = dv.rolling(21, min_periods=5).median()
    cls = engine.load_strategy_class(entry['strategy']['module'], entry['strategy']['class'])
    strategy_sha = engine.strategy_source_sha256(cls)

    # ---- lookahead: every executable cell and every benchmark, BEFORE any metric --
    # The full-panel precompute state of each cell also yields its published
    # coverage frame (D05): the decisive benchmark must own exactly the names
    # the cell that was actually chosen could have picked from.
    coverage_by_cell: Dict[str, Optional[pd.DataFrame]] = {}
    n_prefixes = len(engine.sampled_prefixes(len(panel.close)))
    for cell in cells:
        state = engine.lookahead_check(entry, cell, panel)          # raises on lookahead
        coverage_by_cell[_cell_key(cell)] = engine.candidate_coverage(state)
    # Benchmarks are checked in every form they will actually be executed in:
    # once per DISTINCT coverage frame a candidate cell may hand them (and
    # bare, when some cell publishes none).
    distinct_cov: List[Optional[pd.DataFrame]] = []
    for cov in coverage_by_cell.values():
        if not any((cov is None and c is None) or (cov is not None and c is not None and cov.equals(c))
                   for c in distinct_cov):
            distinct_cov.append(cov)
    for be in bench_entries.values():
        for cov in distinct_cov:
            engine.lookahead_check(be, default_cell(be), panel, coverage=cov)
    lookahead_report = {'cells_checked': len(cells), 'benchmarks_checked': bench_ids,
                        'benchmark_coverage_frames_checked': len(distinct_cov),
                        'mode': 'sampled_prefixes_fresh_state', 'prefixes_replayed': n_prefixes,
                        'reference_indices': max(len(panel.close) - 1, 0)}

    def run(e: Dict[str, Any], cell: Dict[str, Any], data_start, meas_start, stop, c: Costs,
            cov: Optional[pd.DataFrame] = None) -> engine.WindowResult:
        return engine.run_window(e, cell, panel, data_start, meas_start, stop, c, coverage=cov)

    # ---- windows: walk-forward folds or the single fixed window --------------
    folds: List[Fold]
    fold_results: List[FoldResult] = []
    windows: List[Tuple[Fold, Dict[str, Any], engine.WindowResult]] = []
    if fixed_params is None:
        total_days = (dev_end - dev_start).days + 1
        folds = resolve_folds(dev_start, plan_fold_offsets(total_days, train_days, test_days), warmup)
        if not folds:
            raise ValueError(f'dev period {dev_start}..{dev_end} cannot hold one {train_days}+{test_days}-day fold')
        cache: Dict[Tuple[str, str, str], engine.WindowResult] = {}

        def run_backtest(cell, data_start, stop):
            # Any failure propagates: a window that cannot be run is an error
            # of the evaluation, never a fold quietly left out of the average.
            key = (_cell_key(cell), str(data_start), str(stop))
            if key not in cache:
                cache[key] = run(entry, cell, data_start, data_start + warm, stop, costs)
            r = cache[key]
            return {'sharpe': stats.ann_sharpe(r.returns), 'result': r}

        if len(cells) > 1:
            fold_results = run_walk_forward(folds, cells, run_backtest, lambda m: m.get('sharpe'))
        else:
            # One cell: nothing to select from, so no fit is run on the train
            # windows (a fold's test_end is the next fold's train_end, which is
            # why this is a separate branch and not a lookup on the end date).
            fold_results = [FoldResult(f, dict(cells[0]), None,
                                       run_backtest(cells[0], f.test_data_start, f.test_end))
                            for f in folds]
        for fr in fold_results:
            chosen_cell, test_res = fr.chosen, fr.test_metrics.get('result')
            if chosen_cell is None or test_res is None:
                raise ValueError(
                    f'{entry["id"]}: fold {fr.fold.index} ({fr.fold.train_start}..{fr.fold.train_end}) '
                    f'scored no cell on its train window, so its test window {fr.fold.test_start}..'
                    f'{fr.fold.test_end} cannot be judged; a fold is never skipped (D01 fails closed) '
                    f'- check warmup_days and the grid')
            windows.append((fr.fold, dict(chosen_cell), test_res))
    else:
        f = Fold(0, dev_start, dev_start, dev_start, end_excl, dev_start - warm, dev_start - warm)
        folds = [f]
        windows.append((f, report_cell, run(entry, report_cell, f.test_data_start, dev_start, end_excl, costs)))

    # ---- benchmarks on the identical windows, decisive = better per fold -------
    oos_parts, bench_parts, decisive_parts, decisive_ids = [], {b: [] for b in bench_ids}, [], []
    stress_parts, stress_decisive = [], []
    fold_table = []
    for fold, chosen, res in windows:
        cov = res.coverage                    # D05: derived from THIS window's candidate run, on ITS bars
        b_res = {b: run(bench_entries[b], default_cell(bench_entries[b]), fold.test_data_start, fold.test_start,
                        fold.test_end, costs, cov) for b in bench_ids}
        b_sharpes = {b: stats.ann_sharpe(r.returns) for b, r in b_res.items()}
        if bench_ids:
            def _score(b: str) -> float:
                v = b_sharpes.get(b)
                return v if v is not None else float('-inf')
            dec = max(bench_ids, key=_score)
            dec_ret = b_res[dec].returns
        else:
            dec, dec_ret = 'cash', pd.Series(0.0, index=res.returns.index)
        oos_parts.append(res.returns)
        for b in bench_ids:
            bench_parts[b].append(b_res[b].returns)
        decisive_parts.append(dec_ret.reindex(res.returns.index).fillna(0.0))
        decisive_ids.append(dec)
        s_res = run(entry, chosen, fold.test_data_start, fold.test_start, fold.test_end, stressed)
        stress_parts.append(s_res.returns)
        if dec == 'cash':
            stress_decisive.append(pd.Series(0.0, index=s_res.returns.index))
        else:
            sd = run(bench_entries[dec], default_cell(bench_entries[dec]), fold.test_data_start, fold.test_start,
                     fold.test_end, stressed, cov)
            stress_decisive.append(sd.returns.reindex(s_res.returns.index).fillna(0.0))
        fold_table.append({'fold': fold.index, 'train_start': str(fold.train_start), 'train_end': str(fold.train_end),
                           'test_start': str(fold.test_start), 'test_end': str(fold.test_end), 'chosen': chosen,
                           'coverage_forwarded': cov is not None,
                           'oos': res.metrics(), 'benchmarks': {b: r.metrics() for b, r in b_res.items()},
                           'decisive': dec})

    oos = _stitch(oos_parts)
    decisive = _stitch(decisive_parts).reindex(oos.index).fillna(0.0)
    active = oos - decisive
    bench_oos = {b: _stitch(v) for b, v in bench_parts.items()}
    stress_oos = _stitch(stress_parts)
    stress_active = stress_oos - _stitch(stress_decisive).reindex(stress_oos.index).fillna(0.0)

    # ---- full period: report cell, every cell (PBO), benchmarks -------------------
    full = run(entry, report_cell, dev_start - warm, dev_start, end_excl, costs)
    report_cov = full.coverage                # D05: the full-period run's own published coverage
    full_bench = {b: run(bench_entries[b], default_cell(bench_entries[b]), dev_start - warm, dev_start, end_excl,
                         costs, report_cov) for b in bench_ids}
    full_cells: Dict[str, Dict[str, Any]] = {}
    pbo: Optional[Dict[str, Any]] = None
    pbo_value: Optional[float] = None
    if fixed_params is None and len(cells) >= 4:
        cols = []
        for cell in cells:
            key = _cell_key(cell)
            r = full if cell == report_cell else run(entry, cell, dev_start - warm, dev_start, end_excl, costs)
            full_cells[key] = r.metrics()
            cols.append(r.returns.rename(key))
        matrix = pd.concat(cols, axis=1).fillna(0.0).to_numpy()
        p = pbo_cscv(matrix, n_splits=16)
        if p is not None:
            pbo_value = float(p.pbo)
            pbo = {'pbo': pbo_value, 'n_splits': p.n_splits, 'n_trials': p.n_trials,
                   'n_observations': p.n_observations, 'caveat': p.caveat}
    stability = selection_stability(fold_results) if fold_results else None

    # ---- statistics, trial count, verdict ------------------------------------------
    n_trials = ledger.trial_count(rows, including=sha if entry['role'] == 'candidate' else None)
    sharpes = ledger.trial_sharpes(rows)
    act_sharpe = stats.ann_sharpe(active)
    if entry['role'] == 'candidate' and act_sharpe is not None:
        sharpes[sha] = act_sharpe
    t_nw, lags = stats.newey_west_t(active)
    years = stats.per_calendar_year(active)
    bench_sharpes = {b: stats.ann_sharpe(s) for b, s in bench_oos.items()}
    finite_b = [v for v in bench_sharpes.values() if v is not None]
    gates = GateInputs(
        oos_sharpe=stats.ann_sharpe(oos),
        benchmark_oos_sharpe_max=(max(finite_b) if finite_b else (0.0 if not bench_ids else None)),
        active_ann_return=stats.ann_mean(active), active_t_nw=t_nw,
        active_dsr=stats.deflated_sharpe_n(active, max(n_trials, 1), list(sharpes.values())),
        oos_max_drawdown=stats.max_drawdown(oos), benchmark_max_drawdown=stats.max_drawdown(decisive),
        fraction_positive_years=stats.fraction_positive_years(years),
        active_ann_return_stressed=stats.ann_mean(stress_active), n_cells=len(cells),
        pbo=pbo_value, selection_stability=stability)
    decision = decide(gates)
    oos_summary = stats.summarise(oos)
    ledger_metrics = {'oos_sharpe': oos_summary['sharpe'], 'oos_cagr': oos_summary['cagr'],
                      'oos_max_drawdown': oos_summary['max_drawdown'], 'active_ann_return': gates.active_ann_return,
                      'active_t_nw': t_nw, 'oos_active_sharpe': act_sharpe, 'active_dsr': gates.active_dsr,
                      'benchmark_oos_sharpe_max': gates.benchmark_oos_sharpe_max,
                      'active_ann_return_3x': gates.active_ann_return_stressed, 'pbo': pbo_value,
                      'selection_stability': stability, 'full_sharpe': stats.ann_sharpe(full.returns),
                      'full_min_cash_fraction': full.min_cash_fraction, 'full_scaled_buy_fills': full.scaled_buy_fills,
                      'oos_days': int(len(oos))}
    return {
        'family': fam.name, 'id': entry['id'], 'role': entry['role'],
        'mode': 'fixed' if fixed_params is not None else 'walk_forward',
        'entry_sha256': sha, 'strategy_sha256': strategy_sha, 'entry': entry, 'universe': universe,
        'window': {'start': str(dev_start), 'end': str(dev_end), 'warmup_days': warmup,
                   'train_days': train_days, 'test_days': test_days, 'n_folds': len(folds)},
        'costs': {'base': costs.as_dict(), 'stress_multiplier': STRESS_MULTIPLIER},
        'grid': {'cells': len(cells), 'registered_cells': len(registered_cells), 'report_cell': report_cell},
        'lookahead': lookahead_report,
        'coverage': ({'forwarded_to_benchmarks': True, 'per_chosen_cell': True, 'per_window': True,
                      'candidate_buys_validated': True,
                      'median_names_covered_per_day': float(report_cov.sum(axis=1).median())}
                     if report_cov is not None else {'forwarded_to_benchmarks': False, 'per_chosen_cell': True,
                                                     'per_window': True, 'candidate_buys_validated': False}),
        'folds': fold_table, 'decisive_per_fold': decisive_ids,
        'oos': oos_summary, 'oos_benchmarks': {b: stats.summarise(s) for b, s in bench_oos.items()},
        'active': {'ann_return': gates.active_ann_return, 'sharpe': act_sharpe, 't_nw': t_nw, 'nw_lags': lags,
                   'per_year': years, 'fraction_positive_years': gates.fraction_positive_years,
                   'ann_return_3x_cost': gates.active_ann_return_stressed,
                   'max_drawdown_benchmark': gates.benchmark_max_drawdown},
        'dsr': {'value': gates.active_dsr, 'n_trials': n_trials, 'trial_sharpes': sharpes},
        'pbo': pbo, 'selection_stability': stability,
        'full_period': {'report_cell': full.metrics(), 'benchmarks': {b: r.metrics() for b, r in full_bench.items()},
                        'cells': full_cells},
        'stopped_names': {'rule': 'D07: a holding is sold at the close of its last stored print; no open is fabricated',
                          'full_period_forced_exits': full.forced_exits,
                          'full_period_forced_exit_value_fraction': full.forced_exit_value_fraction,
                          'oos_forced_exits': int(sum((r.forced_exits or 0) for _, _, r in windows)),
                          'benchmark_full_period_forced_exits': {b: r.forced_exits for b, r in full_bench.items()}},
        'verdict': decision['verdict'], 'gates': decision['gates'], 'gate_inputs': decision['inputs'],
        'ledger_metrics': ledger_metrics,
        'series': {'oos_returns': _series_json(oos), 'decisive_returns': _series_json(decisive)},
    }


def _f(v, fmt='{:+.3f}') -> str:
    return 'n/a' if v is None or (isinstance(v, float) and v != v) else fmt.format(v)


def render_summary(r: Dict[str, Any]) -> str:
    w, o, a, d, g = r['window'], r['oos'], r['active'], r['dsr'], r['grid']
    cells = f"{g['cells']}" if g['cells'] == g.get('registered_cells', g['cells']) else f"{g['cells']}/{g['registered_cells']}"
    lines = [f"== {r['family']}/{r['id']} ({r['role']}, {r['mode']}) {w['start']}..{w['end']} "
             f"folds={w['n_folds']} cells={cells} report_cell={g['report_cell']}",
             f"OOS       sharpe {_f(o['sharpe'])}  cagr {_f(o['cagr'], '{:+.2%}')}  maxDD {_f(o['max_drawdown'], '{:.1%}')}  days {o['days']}"]
    for b, s in r['oos_benchmarks'].items():
        lines.append(f"  bench {b:<14} sharpe {_f(s['sharpe'])}  cagr {_f(s['cagr'], '{:+.2%}')}  maxDD {_f(s['max_drawdown'], '{:.1%}')}")
    lines.append(f"active    ann {_f(a['ann_return'], '{:+.2%}')}  t(NW,{a['nw_lags']}) {_f(a['t_nw'], '{:.2f}')}  "
                 f"DSR {_f(d['value'], '{:.2f}')} (n_trials={d['n_trials']})  3x-cost ann {_f(a['ann_return_3x_cost'], '{:+.2%}')}  "
                 f"pos-years {_f(a['fraction_positive_years'], '{:.0%}')}")
    yrs = ' '.join(f"{y}:{v['active_return']:+.1%}{'' if v['counted'] else '*'}" for y, v in sorted(a['per_year'].items()))
    lines.append(f"per-year  {yrs}")
    fp = r['full_period']['report_cell']
    lines.append(f"full      sharpe {_f(fp['sharpe'])}  cagr {_f(fp['cagr'], '{:+.2%}')}  maxDD {_f(fp['max_drawdown'], '{:.1%}')}  "
                 f"turnover/yr {_f(fp['turnover_per_year'], '{:.2f}')}  PBO {_f((r['pbo'] or {}).get('pbo'), '{:.2f}')}  "
                 f"stability {_f(r['selection_stability'], '{:.2f}')}")
    lines.append(f"capacity  names {_f(fp.get('median_names_held'), '{:.0f}')}  median $ADV {_f(fp.get('median_holding_adv_usd'), '{:,.0f}')}  "
                 f"min cash {_f(fp.get('min_cash_fraction'), '{:+.2f}')}x  frozen@end {_f(fp.get('frozen_value_fraction_end'), '{:.1%}')} "
                 f"({fp.get('n_frozen_holdings_end')} names)  cash-guard scaled buys {_f(fp.get('scaled_buy_fills'), '{:d}')} "
                 f"on {_f(fp.get('scaled_rebalances'), '{:d}')} dates (D06)")
    la = r.get('lookahead', {})
    lines.append(f"lookahead cells checked {la.get('cells_checked')}  benchmarks {la.get('benchmarks_checked')} "
                 f"x {la.get('benchmark_coverage_frames_checked')} coverage frame(s)  "
                 f"{la.get('prefixes_replayed')} prefixes replayed with fresh state of {la.get('reference_indices')} indices")
    sn = r.get('stopped_names', {})
    lines.append(f"stopped   forced exits at last print: full {sn.get('full_period_forced_exits')} "
                 f"(value {_f(sn.get('full_period_forced_exit_value_fraction'), '{:.2%}')} of capital)  "
                 f"oos {sn.get('oos_forced_exits')}  benchmarks {sn.get('benchmark_full_period_forced_exits')} (D07)")
    lines.append('gates     ' + '  '.join(f"{k}={'n/a' if v is None else ('ok' if v else 'FAIL')}" for k, v in r['gates'].items()))
    lines.append(f"results   {r.get('results_path', '')}")
    lines.append(f"VERDICT   {r['verdict']}")
    return '\n'.join(lines)
