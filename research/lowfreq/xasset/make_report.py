"""Render the tables of research/lowfreq/xasset/REPORT.md from the ledger and
results JSONs, so every number in the report is copied from a file, never
typed. Champion = PREREG.md rule (verdict class, then active t(NW), then OOS
Sharpe, then registry order); final cell = modal per-fold chosen cell (tie ->
last fold). Usage: uv run python research/lowfreq/xasset/make_report.py [> tables.md]
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
VERDICT_RANK = {'PASS': 0, 'PROMISING': 1, 'FAIL': 2}


def _f(v, fmt='{:+.3f}', na='n/a'):
    return na if v is None or (isinstance(v, float) and v != v) else fmt.format(v)


def load():
    reg = yaml.safe_load((HERE / 'candidates.yaml').read_text())
    order = [e['id'] for e in reg['candidates'] if e['role'] == 'candidate']
    rows = [json.loads(l) for l in (HERE / 'trials.jsonl').read_text().splitlines() if l.strip()]
    results = {}
    for cid in order:
        p = HERE / 'results' / f'{cid}.json'
        if p.exists():
            results[cid] = json.load(open(p))
    return reg, order, rows, results


def ledger_table(order, rows):
    out = ['| # | id | role | mode | status | verdict | entry sha256[:12] | n_trials | OOS Sharpe | active ann | t(NW) | DSR | timestamp |',
           '|---|---|---|---|---|---|---|---|---|---|---|---|---|']
    for k, r in enumerate(rows, 1):
        m = r.get('metrics') or {}
        out.append(f"| {k} | {r.get('id')} | {r.get('role')} | {r.get('mode')} | {r.get('status')} | {r.get('verdict') or '-'} | "
                   f"{(r.get('entry_sha256') or '')[:12]} | {r.get('n_trials')} | {_f(m.get('oos_sharpe'))} | "
                   f"{_f(m.get('active_ann_return'), '{:+.2%}')} | {_f(m.get('active_t_nw'), '{:.2f}')} | "
                   f"{_f(m.get('active_dsr'), '{:.2f}')} | {str(r.get('timestamp'))[:19]} |")
    n_cand = len({r['entry_sha256'] for r in rows if r.get('role') == 'candidate' and r.get('entry_sha256')})
    out.append(f'\nCandidate-role trial count (distinct entry hashes): **{n_cand}** (budget 40, D04).')
    return '\n'.join(out)


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


def champion(order, results):
    elig = [(cid, results[cid]) for cid in order if cid in results]
    if not elig:
        return None
    def key(item):
        i, (cid, r) = item
        t = r['active']['t_nw']; s = r['oos']['sharpe']
        return (VERDICT_RANK.get(r['verdict'], 9), -(t if t is not None else -1e9), -(s if s is not None else -1e9), i)
    return min(enumerate(elig), key=key)[1]


def final_cell(r):
    chosen = [json.dumps(f['chosen'], sort_keys=True) for f in r['folds']]
    counts = Counter(chosen)
    best = max(counts.values())
    modal = [c for c, n in counts.items() if n == best]
    pick = modal[0] if len(modal) == 1 else chosen[-1]
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
    lines += ['', f"Per-fold chosen cells: {counts}. **Final parameter cell (modal, tie -> last fold): `{json.dumps(cell)}`**; "
              f"pre-registered default cell: `{json.dumps(r['grid']['report_cell'])}`.", '',
              '| gate | result |', '|---|---|']
    for k, v in r['gates'].items():
        lines.append(f"| {k} | {'n/a' if v is None else ('ok' if v else 'FAIL')} |")
    fp = r['full_period']['report_cell']
    lines += ['', f"Full dev period, default cell: Sharpe {_f(fp['sharpe'])}, CAGR {_f(fp['cagr'], '{:+.2%}')}, max DD "
              f"{_f(fp['max_drawdown'], '{:.1%}')}, turnover/yr {_f(fp['turnover_per_year'], '{:.2f}')}, names held (median) "
              f"{_f(fp.get('median_names_held'), '{:.0f}')}, min cash {_f(fp.get('min_cash_fraction'), '{:+.2f}')}x capital, "
              f"cash-guard scaled buys {fp.get('scaled_buy_fills')} (D06), forced exits {fp.get('forced_exits')} (D07)."]
    return '\n'.join(lines)


def main():
    reg, order, rows, results = load()
    print('## Every trial (research/lowfreq/xasset/trials.jsonl)\n')
    print(ledger_table(order, rows))
    print('\n## Candidates (results/<id>.json)\n')
    print(candidate_table(order, results))
    ch = champion(order, results)
    print()
    if ch is None:
        print('No candidate evaluated yet.')
    else:
        print(champion_section(*ch))


if __name__ == '__main__':
    sys.exit(main())
