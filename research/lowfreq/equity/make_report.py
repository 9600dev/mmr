#!/usr/bin/env python3
"""Generate REPORT.md for the equity family from the ledger and results files.

    uv run python research/lowfreq/equity/make_report.py

Every number in REPORT.md is read from ``trials.jsonl``, ``results/<id>.json``
or ``results/<id>__d07_stress.json``; the champion is chosen by the rule in
PREREG.md (rules 1-4 and addendum R2 item 3), applied here in code.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

HERE = Path(__file__).resolve().parent
CONTROLS = ('s2p_frozen_revenue', 'quality_value_frozen', 'accruals_frozen')
SIZE_TILTS = ('size_smallcap', 'size_small_assets', 'size_small_revenue')
GUARDS = {                                   # PREREG rule 2 + addendum R2 item 3
    's2p_top_quintile': ['size_smallcap', 's2p_frozen_revenue'],
    'pead_earnings': ['size_small_assets'],
    'pead_revenue': ['size_small_revenue'],
    'quality_value_blend': ['s2p_top_quintile', 'size_smallcap', 'size_small_assets', 'quality_value_frozen'],
    'low_accruals': ['size_small_assets', 'accruals_frozen'],
    'value_momentum_filter': ['s2p_top_quintile', 'size_smallcap'],
    'low_volatility': [],
}
HYP = {'s2p_top_quintile': 'H1', 'size_smallcap': 'D1', 's2p_frozen_revenue': 'D2', 'pead_earnings': 'H2',
       'pead_revenue': 'H3', 'quality_value_blend': 'H4', 'value_momentum_filter': 'H5', 'low_volatility': 'H6',
       'low_accruals': 'H7', 'size_small_assets': 'D3', 'size_small_revenue': 'D4', 'quality_value_frozen': 'D5',
       'accruals_frozen': 'D6'}
TIER = {'PASS': 2, 'PROMISING': 1, 'FAIL': 0}


def f(v, fmt='{:+.2f}', na='n/a'):
    if v is None or (isinstance(v, float) and v != v):
        return na
    try:
        return fmt.format(v)
    except (TypeError, ValueError):
        return str(v)


def pct(v, signed=True):
    return f(v, '{:+.1%}' if signed else '{:.1%}')


def load() -> Dict[str, Any]:
    reg = yaml.safe_load((HERE / 'candidates.yaml').read_text())
    ids = [e['id'] for e in reg['candidates'] if e['role'] == 'candidate']
    rows = [json.loads(l) for l in (HERE / 'trials.jsonl').read_text().splitlines() if l.strip()]
    results = {}
    for cid in ids:
        p = HERE / 'results' / f'{cid}.json'
        if p.exists():
            results[cid] = json.loads(p.read_text())
    stress = {p.name.split('__')[0]: json.loads(p.read_text()) for p in (HERE / 'results').glob('*__d07_stress.json')}
    return {'reg': reg, 'ids': ids, 'rows': rows, 'results': results, 'stress': stress}


def active_ann(r: Optional[Dict[str, Any]]) -> Optional[float]:
    return None if r is None else r['active']['ann_return']


def choose_champion(ids: List[str], results: Dict[str, Any]):
    """Returns (champion_id, eligibility table)."""
    table = []
    eligible = []

    def gt(a, b):
        return a is not None and b is not None and a > b

    guarded_ok = set()
    for cid in ids:
        r = results.get(cid)
        if cid in CONTROLS:
            table.append((cid, 'ineligible', 'decomposition control (never a champion)')); continue
        if cid in SIZE_TILTS:
            continue                                   # decided after the guarded hypotheses
        if r is None:
            table.append((cid, 'ineligible', 'no results file')); continue
        fails = [c for c in GUARDS.get(cid, []) if not gt(active_ann(r), active_ann(results.get(c)))]
        if fails:
            table.append((cid, 'ineligible', 'active return does not exceed control(s): ' + ', '.join(fails)))
        else:
            table.append((cid, 'eligible', 'exceeds every control' if GUARDS.get(cid) else 'no ratio, no control'))
            eligible.append(cid); guarded_ok.add(cid)
    s2p_failed = 's2p_top_quintile' in ids and 's2p_top_quintile' not in guarded_ok
    for cid in SIZE_TILTS:
        if cid not in ids:
            continue
        if cid == 'size_smallcap' and results.get(cid) is not None and (s2p_failed or not guarded_ok):
            why = ('stands in for s2p_top_quintile, which failed its decomposition guard (rule 2)' if s2p_failed
                   else 'no guarded hypothesis is eligible (addendum R2 item 3)')
            table.append((cid, 'eligible', why)); eligible.append(cid)
        else:
            table.append((cid, 'ineligible', 'pure size tilt / denominator control'))
    if not eligible:
        return None, table
    def key(cid):
        r = results[cid]
        fp = r['full_period']['report_cell']
        return (TIER.get(r['verdict'], -1), r['active']['t_nw'] if r['active']['t_nw'] is not None else -1e9,
                -(fp.get('turnover_per_year') or 1e9))
    return max(eligible, key=key), table


def ledger_table(rows: List[Dict[str, Any]]) -> List[str]:
    out = ['| # | timestamp (UTC) | id | status | N at run | OOS Sharpe | active ann | t(NW) | DSR | verdict | git sha |',
           '|---|---|---|---|---|---|---|---|---|---|---|']
    for n, r in enumerate(rows, 1):
        m = r.get('metrics') or {}
        out.append(f"| {n} | {str(r.get('timestamp', ''))[:19]} | {r.get('id')} | {r.get('status')} | {r.get('n_trials')} | "
                   f"{f(m.get('oos_sharpe'))} | {pct(m.get('active_ann_return'))} | {f(m.get('active_t_nw'), '{:.2f}')} | "
                   f"{f(m.get('active_dsr'), '{:.2f}')} | {r.get('verdict') or '-'} | {str(r.get('git_sha', ''))[:7]} |")
    return out


def candidate_table(ids, results) -> List[str]:
    out = ['| id | H | OOS Sharpe | covered-EW Sharpe | active ann | t(NW) | DSR (N) | maxDD cand / bench | pos-years | 3x-cost active | turnover/yr | names | median $ADV | forced exits full (value) | verdict | failing gates |',
           '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|']
    for cid in ids:
        r = results.get(cid)
        if r is None:
            out.append(f'| {cid} | {HYP.get(cid, "")} | no results | | | | | | | | | | | | error | |'); continue
        b = r['decisive_per_fold'][0]
        fp = r['full_period']['report_cell']
        sn = r['stopped_names']
        failing = [k for k, v in r['gates'].items() if v is False]
        out.append(f"| {cid} | {HYP.get(cid, '')} | {f(r['oos']['sharpe'])} | {f(r['oos_benchmarks'][b]['sharpe'])} | "
                   f"{pct(r['active']['ann_return'])} | {f(r['active']['t_nw'], '{:.2f}')} | {f(r['dsr']['value'], '{:.2f}')} ({r['dsr']['n_trials']}) | "
                   f"{pct(r['oos']['max_drawdown'], False)} / {pct(r['active']['max_drawdown_benchmark'], False)} | "
                   f"{pct(r['active']['fraction_positive_years'], False)} | {pct(r['active']['ann_return_3x_cost'])} | "
                   f"{f(fp.get('turnover_per_year'), '{:.2f}')} | {f(fp.get('median_names_held'), '{:.0f}')} | "
                   f"{f(fp.get('median_holding_adv_usd'), '{:,.0f}')} | {sn.get('full_period_forced_exits')} ({pct(sn.get('full_period_forced_exit_value_fraction'), False)}) | "
                   f"**{r['verdict']}** | {', '.join(failing) or '-'} |")
    return out


def decomposition_tables(results) -> List[str]:
    groups = [('H1 sales-to-price vs its parts', ['s2p_top_quintile', 'size_smallcap', 's2p_frozen_revenue']),
              ('H2/H3 surprise ratios vs their denominators', ['pead_earnings', 'size_small_assets', 'pead_revenue', 'size_small_revenue']),
              ('H4 quality/value blend vs its parts', ['quality_value_blend', 'quality_value_frozen', 's2p_top_quintile', 'size_smallcap', 'size_small_assets']),
              ('H7 low accruals vs its parts', ['low_accruals', 'accruals_frozen', 'size_small_assets']),
              ('H5 value+momentum vs plain value and size', ['value_momentum_filter', 's2p_top_quintile', 'size_smallcap'])]
    out = []
    for title, members in groups:
        out += [f'**{title}**', '', '| id | role | OOS Sharpe | active ann | t(NW) | full-period Sharpe | verdict |', '|---|---|---|---|---|---|---|']
        for cid in members:
            r = results.get(cid)
            role = 'control' if cid in CONTROLS or cid in SIZE_TILTS else 'hypothesis'
            if r is None:
                out.append(f'| {cid} | {role} | no results | | | | |'); continue
            out.append(f"| {cid} | {role} | {f(r['oos']['sharpe'])} | {pct(r['active']['ann_return'])} | "
                       f"{f(r['active']['t_nw'], '{:.2f}')} | {f(r['full_period']['report_cell']['sharpe'])} | {r['verdict']} |")
        out.append('')
    return out


def per_year(r) -> List[str]:
    out = ['| year | active return | counted |', '|---|---|---|']
    for y, v in sorted(r['active']['per_year'].items()):
        out.append(f"| {y} | {pct(v['active_return'])} | {'yes' if v['counted'] else 'no (< 60 OOS days)'} |")
    return out


def stress_block(s: Optional[Dict[str, Any]]) -> List[str]:
    if s is None:
        return ['Not run (no `results/<id>__d07_stress.json`).']
    rc = s['reproduction_check']
    out = [f"Source: `{Path(s['source_results']).name}`; command: `{s['command']}`; stress file "
           f"`results/{s['id']}__d07_stress.json`. Reproduction check (unstressed re-run vs results JSON): OOS Sharpe "
           f"{f(rc['oos_sharpe_recomputed_unstressed'])} vs {f(rc['oos_sharpe_from_results_json'])}; active ann "
           f"{pct(rc['active_ann_return_recomputed_unstressed'])} vs {pct(rc['active_ann_return_from_results_json'])}.", '',
           f"Forced exits in the OOS windows: candidate {s['total_candidate_forced_exits']} "
           f"(proceeds ${s['total_candidate_forced_proceeds']:,.0f} of $1,000,000 capital), benchmark "
           f"{s['total_benchmark_forced_exits']} (${s['total_benchmark_forced_proceeds']:,.0f}). Marked to ZERO:", '',
           '| series | OOS Sharpe | ann return | t(NW) | max DD |', '|---|---|---|---|---|']
    for label, key in (('candidate unstressed', ('candidate', 'unstressed')), ('candidate stressed', ('candidate', 'stressed')),
                       ('benchmark unstressed', ('benchmark', 'unstressed')), ('benchmark stressed', ('benchmark', 'stressed')),
                       ('active unstressed', ('active', 'unstressed')), ('active (i) candidate stressed only', ('active', 'i_candidate_stressed_only')),
                       ('active (ii) both stressed', ('active', 'ii_both_stressed'))):
        m = s[key[0]][key[1]]
        out.append(f"| {label} | {f(m['sharpe'])} | {pct(m['ann_mean'])} | {f(m['t_nw'], '{:.2f}')} | {pct(m['max_drawdown'], False)} |")
    out += ['', f"Believed if: {s['believed_if']} → **{'holds' if s['passes_stress'] else 'FAILS'}**."]
    return out


def main() -> int:
    d = load()
    ids, results, rows = d['ids'], d['results'], d['rows']
    champ, elig = choose_champion(ids, results)
    notes = (HERE / 'REPORT_NOTES.md').read_text() if (HERE / 'REPORT_NOTES.md').exists() else ''
    L: List[str] = []
    L += ['# Equity family (C03, D05) — evaluation report', '',
          'Generated by `uv run python research/lowfreq/equity/make_report.py` from `trials.jsonl` and `results/*.json`; '
          'every number below is read from those files. Hypotheses, rules and the champion-selection rule were '
          'pre-registered in `PREREG.md` (checkpoint `92653bf`, before the first ledger row).', '']
    cr = results.get(champ) if champ else None
    L += ['## Verdict', '']
    if cr is None:
        L += ['No eligible champion (see the eligibility table).']
    else:
        L += [f"**Champion by the pre-registered rule: `{champ}` ({HYP.get(champ)}) — harness verdict `{cr['verdict']}`** "
              f"(`results/{champ}.json`, final parameter cell `{json.dumps(cr['grid']['report_cell'])}`, N = {cr['dsr']['n_trials']} trials).", '']
        if cr['verdict'] != 'PASS':
            L += [f"The family did NOT produce a PASS. The null stands: no pre-registered long-only tilt on the covered "
                  f"point-in-time universe beat monthly equal weight of its own covered set at the D01 bar; the champion is the "
                  f"best `{cr['verdict']}` evidence and is reported as such.", '']
    n_cand_hashes = len({r['entry_sha256'] for r in rows if r.get('role') == 'candidate' and r.get('entry_sha256')})
    L += ['## Protocol', '',
          f"D05 walk-forward (train 1095 / test 365 days, rolling, dev 2016-08-01..2024-06-28, {cr['window']['n_folds'] if cr else '?'} folds, 400 days warm-up), "
          'fills at t+1 open, 5 bps + $0.005/share (3x for the stress), long-only, gross 1.0, monthly rebalance, top-20 % of the covered set '
          'equal-weighted, ≥ 30 names. Decisive benchmark `eq_ew_covered` owns each candidate\'s published coverage. '
          f"Every candidate is a single pre-registered cell (no grid; gate (g) n/a; nothing selected on train windows). "
          f"Distinct candidate hashes in the ledger: {n_cand_hashes} (budget 40). Two passes (addendum R2 item 4): pass 1 fills the ledger, "
          'pass 2 re-evaluates every candidate so each final results file deflates by the same trial count.', '']
    L += ['## Every trial (ledger, in order)', ''] + ledger_table(rows) + ['']
    L += ['## Candidates — final results (`results/<id>.json`)', ''] + candidate_table(ids, results) + ['',
          'maxDD = OOS max drawdown of the stitched OOS equity (candidate / decisive benchmark); pos-years = fraction of counted OOS '
          'calendar years with positive active return; names / $ADV / turnover / forced exits from the full-period run of the report cell.', '']
    L += ['## Decompositions (PREREG rule 2, addendum R2 items 2-3)', ''] + decomposition_tables(results)
    L += ['## Champion selection (rule applied in `make_report.choose_champion`)', '', '| id | eligibility | reason |', '|---|---|---|']
    L += [f'| {c} | {e} | {w} |' for c, e, w in elig]
    L += ['', 'Ranking of eligible entries: verdict tier (PASS > PROMISING > FAIL), then active t(NW), then lower turnover.', '']
    if cr is not None:
        L += [f'### Champion `{champ}` — OOS detail', '',
              f"OOS Sharpe {f(cr['oos']['sharpe'])} vs covered-EW {f(cr['oos_benchmarks'][cr['decisive_per_fold'][0]]['sharpe'])}; "
              f"active {pct(cr['active']['ann_return'])}/yr, t(NW, {cr['active']['nw_lags']} lags) {f(cr['active']['t_nw'], '{:.2f}')}, "
              f"DSR {f(cr['dsr']['value'], '{:.2f}')} at N = {cr['dsr']['n_trials']}; 3x-cost active {pct(cr['active']['ann_return_3x_cost'])}; "
              f"max DD {pct(cr['oos']['max_drawdown'], False)} vs {pct(cr['active']['max_drawdown_benchmark'], False)}; "
              f"full-period turnover {f(cr['full_period']['report_cell'].get('turnover_per_year'), '{:.2f}')}/yr, median names held "
              f"{f(cr['full_period']['report_cell'].get('median_names_held'), '{:.0f}')}, median holding 21d $ADV "
              f"${f(cr['full_period']['report_cell'].get('median_holding_adv_usd'), '{:,.0f}', 'n/a')}.", '',
              'Gates: ' + ', '.join(f"{k}={'ok' if v else ('FAIL' if v is False else 'n/a')}" for k, v in cr['gates'].items()), '',
              'Per OOS calendar year (active = champion − decisive benchmark):', ''] + per_year(cr) + ['']
        L += ['## D07 terminal-loss stress (champion)', ''] + stress_block(d['stress'].get(champ)) + ['']
    if notes:
        L += [notes.rstrip(), '']
    (HERE / 'REPORT.md').write_text('\n'.join(L))
    words = len((HERE / 'REPORT.md').read_text().split())
    print(f'REPORT.md written: {words} words; champion={champ}; results={len(results)}/{len(ids)}; ledger rows={len(rows)}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
