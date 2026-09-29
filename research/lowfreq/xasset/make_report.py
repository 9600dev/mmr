"""Render the tables of research/lowfreq/xasset/REPORT.md from the ledger and
results JSONs, so every number in the report is copied from a file, never
typed. Champion = PREREG.md rule (verdict class, then active t(NW), then OOS
Sharpe, then registry order); final cell = modal per-fold chosen cell, a tie
going to the latest fold whose choice is one of the tied cells (PREREG A1.3).

Usage (from the repo root):
  uv run python research/lowfreq/xasset/make_report.py            # tables (stdout)
  uv run python research/lowfreq/xasset/make_report.py snapshot pass1   # PREREG A1.2 step 2/3
  uv run python research/lowfreq/xasset/make_report.py check      # pass1 vs pass2 determinism
  uv run python research/lowfreq/xasset/make_report.py folds      # per-fold Sharpe vs both benchmarks
  uv run python research/lowfreq/xasset/make_report.py holdings   # dated top holdings per fold (needs the research DBs)
"""
from __future__ import annotations

import json
import shutil
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

HERE = Path(__file__).resolve().parent
VERDICT_RANK = {'PASS': 0, 'PROMISING': 1, 'FAIL': 2}
FOLD_TIE = 0.005   # fold-Sharpe differences at or below this are ties in fold_table (below the table's precision)
# Fields that must be byte-identical between pass 1 and pass 2 (everything the
# trial count does not touch); `dsr`, gate (c), the verdict and bookkeeping may differ.
DETERMINISTIC_FIELDS = ('oos', 'oos_benchmarks', 'folds', 'decisive_per_fold', 'pbo', 'selection_stability',
                        'full_period', 'stopped_names', 'grid', 'entry_sha256', 'strategy_sha256', 'lookahead')
ACTIVE_DETERMINISTIC = ('ann_return', 'sharpe', 't_nw', 'nw_lags', 'per_year', 'fraction_positive_years',
                        'ann_return_3x_cost', 'max_drawdown_benchmark')


def _f(v, fmt='{:+.3f}', na='n/a'):
    return na if v is None or (isinstance(v, float) and v != v) else fmt.format(v)


def load(here: Path = HERE):
    reg = yaml.safe_load((here / 'candidates.yaml').read_text())
    order = [e['id'] for e in reg['candidates'] if e['role'] == 'candidate']
    rows = [json.loads(l) for l in (here / 'trials.jsonl').read_text().splitlines() if l.strip()]
    results = {}
    for cid in order:
        p = here / 'results' / f'{cid}.json'
        if p.exists():
            results[cid] = json.load(open(p))
    return reg, order, rows, results


def pass_numbers(rows: List[Dict[str, Any]]) -> List[Optional[int]]:
    """k for the k-th status-ok row of each (id, entry hash); None otherwise."""
    seen: Counter = Counter()
    out: List[Optional[int]] = []
    for r in rows:
        if r.get('status') == 'ok' and r.get('role') == 'candidate':
            key = (r.get('id'), r.get('entry_sha256'))
            seen[key] += 1
            out.append(seen[key])
        else:
            out.append(None)
    return out


def ledger_table(order, rows):
    passes = pass_numbers(rows)
    out = ['| # | id | role | status | verdict | entry sha256[:12] | pass | n_trials | OOS Sharpe | active ann | t(NW) | DSR | timestamp (UTC) |',
           '|---|---|---|---|---|---|---|---|---|---|---|---|---|']
    for k, (r, p) in enumerate(zip(rows, passes), 1):
        m = r.get('metrics') or {}
        out.append(f"| {k} | {r.get('id')} | {r.get('role')} | {r.get('status')} | {r.get('verdict') or '-'} | "
                   f"{(r.get('entry_sha256') or '')[:12]} | {p or '-'} | {r.get('n_trials')} | {_f(m.get('oos_sharpe'))} | "
                   f"{_f(m.get('active_ann_return'), '{:+.2%}')} | {_f(m.get('active_t_nw'), '{:.2f}')} | "
                   f"{_f(m.get('active_dsr'), '{:.2f}')} | {str(r.get('timestamp'))[:19]} |")
    n_cand = distinct_candidate_hashes(rows)
    out.append(f'\nCandidate-role trial count (distinct entry hashes): **{n_cand}** (budget 40, D04); '
               f'{sum(1 for r in rows if r.get("role") == "candidate")} candidate rows in total.')
    return '\n'.join(out)


def distinct_candidate_hashes(rows) -> int:
    return len({r['entry_sha256'] for r in rows if r.get('role') == 'candidate' and r.get('entry_sha256')})


def candidate_table(order, results):
    out = ['| id | verdict | cells | OOS Sharpe | OOS CAGR | OOS maxDD | decisive maxDD | active ann | t(NW) | DSR (N) | 3x-cost active | pos. years | PBO | stability | gates failed |',
           '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|']
    for cid in order:
        r = results.get(cid)
        if r is None:
            out.append(f'| {cid} | not evaluated | | | | | | | | | | | | | |')
            continue
        g, a, o, d = r['gate_inputs'], r['active'], r['oos'], r['dsr']
        failed = [k[0] for k in r['gates'] if r['gates'][k] is False and k != 'b_active_t_ge_1']   # D01 gate letters a-g
        out.append(f"| {cid} | **{r['verdict']}** | {r['grid']['cells']} | {_f(o['sharpe'])} | "
                   f"{_f(o['cagr'], '{:+.2%}')} | {_f(o['max_drawdown'], '{:.1%}')} | {_f(g['benchmark_max_drawdown'], '{:.1%}')} | "
                   f"{_f(a['ann_return'], '{:+.2%}')} | {_f(a['t_nw'], '{:.2f}')} | {_f(d['value'], '{:.2f}')} ({d['n_trials']}) | "
                   f"{_f(a['ann_return_3x_cost'], '{:+.2%}')} | {_f(a['fraction_positive_years'], '{:.0%}')} | "
                   f"{_f((r['pbo'] or {}).get('pbo'), '{:.2f}')} | {_f(r['selection_stability'], '{:.2f}')} | {''.join(failed) or '-'} |")
    return '\n'.join(out)


def champion(order, results) -> Optional[Tuple[str, Dict[str, Any]]]:
    """PREREG rule: verdict class, then active t(NW) desc, then OOS Sharpe desc, then registry order."""
    elig = [(i, cid, results[cid]) for i, cid in enumerate(order) if cid in results]
    if not elig:
        return None

    def key(item):
        i, cid, r = item
        t = r['active']['t_nw']
        s = r['oos']['sharpe']
        return (VERDICT_RANK.get(r['verdict'], 9), -(t if t is not None else -1e9), -(s if s is not None else -1e9), i)
    _, cid, r = min(elig, key=key)
    return cid, r


def final_cell(r) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """Modal per-fold chosen cell; a tie goes to the LATEST fold whose choice is
    one of the tied cells (PREREG A1.3) - never to a non-modal last-fold choice."""
    chosen = [json.dumps(f['chosen'], sort_keys=True) for f in r['folds']]
    counts = Counter(chosen)
    best = max(counts.values())
    modal = {c for c, n in counts.items() if n == best}
    pick = next(c for c in reversed(chosen) if c in modal)
    return json.loads(pick), dict(counts)


def champion_section(cid, r):
    o, a, d = r['oos'], r['active'], r['dsr']
    cell, counts = final_cell(r)
    lines = [f'### Champion: `{cid}` — verdict **{r["verdict"]}**', '',
             f"Chosen by the PREREG rule (verdict class, then active t(NW)). Results file: `results/{cid}.json` "
             f"(entry sha256 `{r['entry_sha256'][:12]}`, strategy sha256 `{(r.get('strategy_sha256') or '')[:12]}`).", '',
             '| series | OOS Sharpe | OOS CAGR | OOS max DD | OOS days |', '|---|---|---|---|---|',
             f"| {cid} | {_f(o['sharpe'])} | {_f(o['cagr'], '{:+.2%}')} | {_f(o['max_drawdown'], '{:.1%}')} | {o['days']} |"]
    for b, s in r['oos_benchmarks'].items():
        lines.append(f"| {b} | {_f(s['sharpe'])} | {_f(s['cagr'], '{:+.2%}')} | {_f(s['max_drawdown'], '{:.1%}')} | {s['days']} |")
    lines += ['', f"Decisive benchmark per fold: {', '.join(r['decisive_per_fold'])}.",
              f"Active (vs decisive): ann {_f(a['ann_return'], '{:+.2%}')}, t(NW, {a['nw_lags']} lags) {_f(a['t_nw'], '{:.2f}')}, "
              f"active Sharpe {_f(a['sharpe'])}, DSR {_f(d['value'], '{:.2f}')} with N = {d['n_trials']} trials, "
              f"3x-cost active ann {_f(a['ann_return_3x_cost'], '{:+.2%}')}, positive years {_f(a['fraction_positive_years'], '{:.0%}')}, "
              f"PBO {_f((r['pbo'] or {}).get('pbo'), '{:.2f}')}, selection stability {_f(r['selection_stability'], '{:.2f}')}.", '',
              '| year | active return | counted |', '|---|---|---|']
    for y, v in sorted(a['per_year'].items()):
        lines.append(f"| {y} | {v['active_return']:+.2%} | {'yes' if v['counted'] else 'no (<60 days)'} |")
    lines += ['', f"Per-fold chosen cells: {counts}. **Final parameter cell (modal; tie -> latest fold in the tied set): "
              f"`{json.dumps(cell)}`**; pre-registered default cell: `{json.dumps(r['grid']['report_cell'])}`.", '',
              'Gates (D01): ' + '; '.join(f"{k} {'n/a' if v is None else ('ok' if v else 'FAIL')}" for k, v in r['gates'].items()) + '.']
    fp = r['full_period']['report_cell']
    lines += ['', f"Full dev period, default cell: Sharpe {_f(fp['sharpe'])}, CAGR {_f(fp['cagr'], '{:+.2%}')}, max DD "
              f"{_f(fp['max_drawdown'], '{:.1%}')}, turnover/yr {_f(fp['turnover_per_year'], '{:.2f}')}, names held (median) "
              f"{_f(fp.get('median_names_held'), '{:.0f}')}, min cash {_f(fp.get('min_cash_fraction'), '{:+.2f}')}x capital, "
              f"cash-guard scaled buys {fp.get('scaled_buy_fills')} (D06), forced exits {fp.get('forced_exits')} (D07)."]
    return '\n'.join(lines)


def snapshot(tag: str, here: Path = HERE) -> List[Path]:
    """PREREG A1.2: copy every candidate's results/<id>.json to results/snapshots/<id>__<tag>.json; refuses to overwrite."""
    _, order, _, _ = load(here)
    dst_dir = here / 'results' / 'snapshots'
    dst_dir.mkdir(parents=True, exist_ok=True)
    out = []
    for cid in order:
        src = here / 'results' / f'{cid}.json'
        if not src.exists():
            continue
        dst = dst_dir / f'{cid}__{tag}.json'
        if dst.exists():
            raise FileExistsError(f'{dst} exists; snapshots are immutable')
        shutil.copyfile(src, dst)
        out.append(dst)
    return out


def compare_passes(a: Dict[str, Any], b: Dict[str, Any]) -> List[str]:
    """Names of deterministic fields that differ between two results files."""
    diffs = [k for k in DETERMINISTIC_FIELDS if json.dumps(a.get(k), sort_keys=True, default=str)
             != json.dumps(b.get(k), sort_keys=True, default=str)]
    diffs += [f'active.{k}' for k in ACTIVE_DETERMINISTIC
              if json.dumps(a['active'].get(k), sort_keys=True) != json.dumps(b['active'].get(k), sort_keys=True)]
    return diffs


def check_passes(here: Path = HERE) -> str:
    _, order, _, results = load(here)
    lines = ['| id | pass-1 DSR (N) | pass-2 DSR (N) | pass-1 verdict | pass-2 verdict | deterministic fields differing |',
             '|---|---|---|---|---|---|']
    for cid in order:
        p1, p2 = (here / 'results' / 'snapshots' / f'{cid}__pass{k}.json' for k in (1, 2))
        if not (p1.exists() and p2.exists()):
            lines.append(f'| {cid} | missing | missing | | | not comparable |')
            continue
        a, b = json.load(open(p1)), json.load(open(p2))
        final = results.get(cid)
        # Integration: a later reproduction (`evaluate xasset/<id>` after the two passes) rewrites
        # results/<id>.json with new run provenance only (timestamp, git sha, elapsed time, ledger
        # row). It IS the pass-2 result when no deterministic field, the verdict and the DSR
        # (value, N) differ; byte identity is not required.
        same_final = (final is not None and not compare_passes(final, b) and final.get('verdict') == b.get('verdict')
                      and (final['dsr']['value'], final['dsr']['n_trials']) == (b['dsr']['value'], b['dsr']['n_trials']))
        diffs = compare_passes(a, b)
        lines.append(f"| {cid} | {_f(a['dsr']['value'], '{:.2f}')} ({a['dsr']['n_trials']}) | {_f(b['dsr']['value'], '{:.2f}')} ({b['dsr']['n_trials']}) | "
                     f"{a['verdict']} | {b['verdict']} | {', '.join(diffs) if diffs else 'none'}{'' if same_final else ' (results/<id>.json != pass2 snapshot!)'} |")
    return '\n'.join(lines)


def fold_table(order, results) -> str:
    """One row per series, one column per walk-forward fold: the candidate's
    OOS Sharpe inside that test window (`folds[k].oos.sharpe`) and both
    benchmarks' Sharpe over the SAME window (`folds[k].benchmarks.<b>.sharpe`).
    `*` marks a candidate above 60/40's fold Sharpe by more than FOLD_TIE
    (a difference visible at the table's two decimals), `!` above both
    benchmarks' by that margin; a smaller difference is a tie, not a win
    (`xa_trend_6040` equals 60/40 to ~1e-4 in every fold it never switched,
    the residue of whole-share rounding). Column k is the zero-based fold
    index of the JSON (`folds[k].fold`), labelled with the test window's start
    and end years. Fold windows and benchmark values are asserted identical
    across candidates."""
    evaluated = [cid for cid in order if cid in results]
    if not evaluated:
        return 'No candidate evaluated yet.'
    ref = results[evaluated[0]]['folds']
    for cid in evaluated[1:]:
        for a, b in zip(ref, results[cid]['folds']):
            if (a['test_start'], a['test_end']) != (b['test_start'], b['test_end']) or \
               any(abs(a['benchmarks'][k]['sharpe'] - b['benchmarks'][k]['sharpe']) > 1e-9 for k in a['benchmarks']):
                raise ValueError(f'fold windows or benchmark Sharpes differ between {evaluated[0]} and {cid}')
    labels = [f"{f['fold']}:{f['test_start'][2:4]}-{f['test_end'][2:4]}" for f in ref]   # "8:20-21" = fold 8, mid-2020..mid-2021
    n = len(ref)
    out = ['| series | ' + ' | '.join(labels) + ' | >60/40 | >both |', '|---|' + '---|' * n + '---|---|']
    for b, name in (('etf_6040', '60/40'), ('etf_ew', 'EW')):
        out.append(f'| {name} | ' + ' | '.join(f"{f['benchmarks'][b]['sharpe']:+.2f}" for f in ref) + ' | - | - |')
    for cid in evaluated:
        cells, n_6040, n_both = [], 0, 0
        for k, f in enumerate(ref):
            b6040, bew = f['benchmarks']['etf_6040']['sharpe'], f['benchmarks']['etf_ew']['sharpe']
            sh = results[cid]['folds'][k]['oos']['sharpe']
            beats_6040, beats_both = sh > b6040 + FOLD_TIE, sh > max(b6040, bew) + FOLD_TIE
            n_6040 += beats_6040
            n_both += beats_both
            cells.append(f"{sh:+.2f}{'*' if beats_6040 else ''}{'!' if beats_both else ''}")
        out.append(f"| {cid.replace('xa_', '')} | " + ' | '.join(cells) + f' | {n_6040}/{n} | {n_both}/{n} |')
    return '\n'.join(out)


def positive_years_table(order, results) -> str:
    """How many candidates had a positive calendar-year active return (vs their
    decisive series, `active.per_year`) in each OOS year, and which."""
    evaluated = [cid for cid in order if cid in results]
    if not evaluated:
        return 'No candidate evaluated yet.'
    years = sorted({y for cid in evaluated for y in results[cid]['active']['per_year']})
    out = ['| year | ' + ' | '.join(cid.replace('xa_', '') for cid in evaluated) + ' | positive |',
           '|---|' + '---|' * len(evaluated) + '---|']
    for y in years:
        vals = [results[cid]['active']['per_year'].get(y, {}).get('active_return') for cid in evaluated]
        out.append(f"| {y} | " + ' | '.join(_f(v, '{:+.1%}') for v in vals)
                   + f" | {sum(1 for v in vals if v is not None and v > 0)}/{len(evaluated)} |")
    return '\n'.join(out)


def composition(here: Path = HERE, oos_start: str = '2012-06-30') -> Dict[str, Any]:
    """Average book composition of each candidate's DEFAULT cell on its OOS
    rebalance days (weights from precompute_panel on the real dev panel; no
    execution): invested fraction, share in bonds/BIL, in SPY. Written to
    results/composition_diagnostics.json so the report's prose numbers are
    traceable to a file. Needs the research databases."""
    from typing import cast

    import numpy as np
    import pandas as pd
    from trader.research.lowfreq import engine, load_etf_panel
    from trader.research.lowfreq.registry import default_cell, load_family
    fam = load_family(here.name, here.parent)
    panel = load_etf_panel(None, '2006-02-01', str(fam.dev_end))
    wide = engine.wide_panel(panel.frames())
    oos = wide.index >= oos_start
    bonds = [c for c in ('AGG', 'IEF', 'IEI', 'SHY', 'TLT', 'TIP', 'LQD', 'MUB', 'HYG', 'EMB') if c in wide['close'].columns]
    out: Dict[str, Any] = {'command': 'uv run python research/lowfreq/xasset/make_report.py composition',
                           'oos_start': oos_start, 'bond_tickers': bonds, 'candidates': {}}
    for cid, e in fam.entries.items():
        if e['role'] != 'candidate':
            continue
        s = cast(Any, engine.make_strategy(e, default_cell(e), panel))   # XAssetBase: has due()
        w = s.precompute_panel(wide)['weights']
        due = np.array([s.due(i, wide) for i in range(len(wide))]) & oos
        rows = w[due]
        bil = rows['BIL'] if 'BIL' in rows else pd.Series(0.0, index=rows.index)
        out['candidates'][cid] = {
            'cell': default_cell(e), 'rebalance_days': int(len(rows)),
            'invested_mean': round(float((1.0 - bil).mean()), 4),
            'bonds_plus_bil_mean': round(float((rows[bonds].sum(axis=1) + bil).mean()), 4),
            'spy_mean': round(float(rows['SPY'].mean()) if 'SPY' in rows else 0.0, 4),
            'names_held_mean': round(float((rows > 0).sum(axis=1).mean()), 2)}
    path = here / 'results' / 'composition_diagnostics.json'
    path.write_text(json.dumps(out, indent=1, sort_keys=True))
    out['path'] = str(path)
    return out


def holdings(here: Path = HERE, top: int = 12) -> Dict[str, Any]:
    """Dated holdings: for every candidate and every walk-forward fold, the
    mean weight of each ticker over the rebalance days inside the fold's TEST
    window, computed from `precompute_panel` with the cell the walk-forward
    actually CHOSE for that fold (`folds[k].chosen`), on the real dev panel.
    Written to results/holdings_diagnostics.json so any statement about what
    a book held in a given year is traceable to a file. Needs the research
    databases. (Weights are targets on decision bars, not executed fills.)"""
    from typing import cast

    import numpy as np
    import pandas as pd
    from trader.research.lowfreq import engine, load_etf_panel
    from trader.research.lowfreq.registry import load_family
    fam = load_family(here.name, here.parent)
    _, order, _, results = load(here)
    panel = load_etf_panel(None, '2006-02-01', str(fam.dev_end))
    wide = engine.wide_panel(panel.frames())
    idx = pd.DatetimeIndex(wide.index)
    out: Dict[str, Any] = {'command': 'uv run python research/lowfreq/xasset/make_report.py holdings',
                           'weights_are': 'mean target weight per ticker over the rebalance (decision) days inside the fold test window, '
                                          'using the cell the walk-forward chose for that fold',
                           'top': top, 'candidates': {}}
    for cid in order:
        r = results.get(cid)
        e = fam.entries.get(cid)
        if r is None or e is None:
            continue
        cache: Dict[str, Any] = {}
        folds_out = []
        for f in r['folds']:
            key = json.dumps(f['chosen'], sort_keys=True)
            if key not in cache:
                s = cast(Any, engine.make_strategy(e, dict(f['chosen']), panel))   # XAssetBase: has due()
                w = s.precompute_panel(wide)['weights']
                due = np.array([s.due(i, wide) for i in range(len(wide))])
                cache[key] = (w, due)
            w, due = cache[key]
            inside = (idx >= pd.Timestamp(f['test_start'])) & (idx < pd.Timestamp(f['test_end'])) & due
            rows = w[inside]
            mean = rows.mean().sort_values(ascending=False)
            folds_out.append({'fold': f['fold'], 'test_start': f['test_start'], 'test_end': f['test_end'],
                              'chosen': f['chosen'], 'rebalance_days': int(len(rows)),
                              'top_holdings': [{'ticker': t, 'mean_weight': round(float(v), 4)}
                                               for t, v in mean.head(top).items() if v > 0]})
        out['candidates'][cid] = {'folds': folds_out}
    path = here / 'results' / 'holdings_diagnostics.json'
    path.write_text(json.dumps(out, indent=1, sort_keys=True))
    out['path'] = str(path)
    return out


def holdings_table(here: Path = HERE, picks: Optional[List[Tuple[str, int]]] = None, max_names: Optional[int] = None) -> str:
    """Rows of results/holdings_diagnostics.json for the (candidate, fold)
    pairs the report discusses; default = every pair (long). `max_names` caps
    the names shown per row (the JSON keeps `top`); a capped row ends with
    '+k more'."""
    data = json.load(open(here / 'results' / 'holdings_diagnostics.json'))
    out = ['| candidate | fold | test window | chosen cell | top holdings (mean target weight) |', '|---|---|---|---|---|']
    for cid, block in data['candidates'].items():
        for f in block['folds']:
            if picks is not None and (cid, f['fold']) not in picks:
                continue
            shown = f['top_holdings'] if max_names is None else f['top_holdings'][:max_names]
            hold = ', '.join(f"{h['ticker']} {h['mean_weight']:.0%}" for h in shown)
            if len(shown) < len(f['top_holdings']):
                hold += f" +{len(f['top_holdings']) - len(shown)} more"
            out.append(f"| {cid} | {f['fold']} | {f['test_start']}..{f['test_end']} | `{json.dumps(f['chosen'], sort_keys=True)}` | {hold} |")
    return '\n'.join(out)


def main(argv: List[str]) -> int:
    if len(argv) >= 2 and argv[1] == 'composition':
        print(json.dumps(composition(), indent=1, sort_keys=True))
        return 0
    if len(argv) >= 2 and argv[1] == 'snapshot':
        for p in snapshot(argv[2]):
            print(p)
        return 0
    if len(argv) >= 2 and argv[1] == 'check':
        print(check_passes())
        return 0
    if len(argv) >= 2 and argv[1] == 'folds':
        _, order, _, results = load()
        print(fold_table(order, results))
        print()
        print(positive_years_table(order, results))
        return 0
    if len(argv) >= 2 and argv[1] == 'holdings':
        holdings()
        picks = None
        if len(argv) > 2:      # e.g. holdings xa_dual_mom:10 xa_carry:7
            picks = [(a.split(':')[0], int(a.split(':')[1])) for a in argv[2:]]
        print(holdings_table(picks=picks))
        return 0
    _, order, rows, results = load()
    print('## Every trial (research/lowfreq/xasset/trials.jsonl)\n')
    print(ledger_table(order, rows))
    print('\n## Candidates (results/<id>.json, pass 2)\n')
    print(candidate_table(order, results))
    ch = champion(order, results)
    print()
    print('No candidate evaluated yet.' if ch is None else champion_section(*ch))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
