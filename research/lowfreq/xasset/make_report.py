"""Render the tables of research/lowfreq/xasset/REPORT.md from the ledger and
results JSONs, so every number in the report is copied from a file, never
typed. Champion = PREREG.md rule (verdict class, then active t(NW), then OOS
Sharpe, then registry order); final cell = modal per-fold chosen cell, a tie
going to the latest fold whose choice is one of the tied cells (PREREG A1.3).

Usage (from the repo root):
  uv run python research/lowfreq/xasset/make_report.py            # tables (stdout)
  uv run python research/lowfreq/xasset/make_report.py snapshot pass1   # PREREG A1.2 step 2/3
  uv run python research/lowfreq/xasset/make_report.py check      # pass1 vs pass2 determinism
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
    out = ['| # | id | role | status | verdict | entry sha256[:12] | pass | snapshot | n_trials | OOS Sharpe | active ann | t(NW) | DSR | timestamp (UTC) |',
           '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|']
    for k, (r, p) in enumerate(zip(rows, passes), 1):
        m = r.get('metrics') or {}
        snap = f"snapshots/{r.get('id')}__pass{p}.json" if p else '-'
        out.append(f"| {k} | {r.get('id')} | {r.get('role')} | {r.get('status')} | {r.get('verdict') or '-'} | "
                   f"{(r.get('entry_sha256') or '')[:12]} | {p or '-'} | {snap} | {r.get('n_trials')} | {_f(m.get('oos_sharpe'))} | "
                   f"{_f(m.get('active_ann_return'), '{:+.2%}')} | {_f(m.get('active_t_nw'), '{:.2f}')} | "
                   f"{_f(m.get('active_dsr'), '{:.2f}')} | {str(r.get('timestamp'))[:19]} |")
    n_cand = distinct_candidate_hashes(rows)
    out.append(f'\nCandidate-role trial count (distinct entry hashes): **{n_cand}** (budget 40, D04); '
               f'{sum(1 for r in rows if r.get("role") == "candidate")} candidate rows in total.')
    return '\n'.join(out)


def distinct_candidate_hashes(rows) -> int:
    return len({r['entry_sha256'] for r in rows if r.get('role') == 'candidate' and r.get('entry_sha256')})


def candidate_table(order, results):
    out = ['| id | verdict | cells | OOS Sharpe | bench max Sharpe | OOS CAGR | OOS maxDD | decisive maxDD | active ann | t(NW) | DSR (N) | 3x-cost active | pos. years | PBO | stability | gates failed |',
           '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|']
    for cid in order:
        r = results.get(cid)
        if r is None:
            out.append(f'| {cid} | not evaluated | | | | | | | | | | | | | | |')
            continue
        g, a, o, d = r['gate_inputs'], r['active'], r['oos'], r['dsr']
        failed = [k for k, v in r['gates'].items() if v is False and k != 'b_active_t_ge_1']
        out.append(f"| {cid} | **{r['verdict']}** | {r['grid']['cells']} | {_f(o['sharpe'])} | {_f(g['benchmark_oos_sharpe_max'])} | "
                   f"{_f(o['cagr'], '{:+.2%}')} | {_f(o['max_drawdown'], '{:.1%}')} | {_f(g['benchmark_max_drawdown'], '{:.1%}')} | "
                   f"{_f(a['ann_return'], '{:+.2%}')} | {_f(a['t_nw'], '{:.2f}')} | {_f(d['value'], '{:.2f}')} ({d['n_trials']}) | "
                   f"{_f(a['ann_return_3x_cost'], '{:+.2%}')} | {_f(a['fraction_positive_years'], '{:.0%}')} | "
                   f"{_f((r['pbo'] or {}).get('pbo'), '{:.2f}')} | {_f(r['selection_stability'], '{:.2f}')} | {', '.join(failed) or '-'} |")
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
              '| gate | result |', '|---|---|']
    for k, v in r['gates'].items():
        lines.append(f"| {k} | {'n/a' if v is None else ('ok' if v else 'FAIL')} |")
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
        same_final = final is not None and json.dumps(final, sort_keys=True, default=str) == json.dumps(b, sort_keys=True, default=str)
        diffs = compare_passes(a, b)
        lines.append(f"| {cid} | {_f(a['dsr']['value'], '{:.2f}')} ({a['dsr']['n_trials']}) | {_f(b['dsr']['value'], '{:.2f}')} ({b['dsr']['n_trials']}) | "
                     f"{a['verdict']} | {b['verdict']} | {', '.join(diffs) if diffs else 'none'}{'' if same_final else ' (results/<id>.json != pass2 snapshot!)'} |")
    return '\n'.join(lines)


def main(argv: List[str]) -> int:
    if len(argv) >= 2 and argv[1] == 'snapshot':
        for p in snapshot(argv[2]):
            print(p)
        return 0
    if len(argv) >= 2 and argv[1] == 'check':
        print(check_passes())
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
