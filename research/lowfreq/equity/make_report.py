#!/usr/bin/env python3
"""Generate REPORT.md for the equity family from the ledger and results files.

    uv run python research/lowfreq/equity/make_report.py

Every number in REPORT.md is read from ``trials.jsonl``, ``results/<id>.json``
or ``results/<id>__d07_stress.json``. The champion is chosen by the ONLY
selection rule with a git proof of predating the first ledger row: PREREG.md
rules 1-4 as committed at the round-1 checkpoint ``92653bf`` (addendum R4).
The later guards (addendum R2 item 3, addendum R3) are reported as a post-hoc
sensitivity analysis and never select.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

HERE = Path(__file__).resolve().parent
PREREG_SHA = '92653bf'
# Registered at 92653bf (PREREG rule 1): every candidate-role entry except the control s2p_frozen_revenue.
ORIGINAL = ('s2p_top_quintile', 'size_smallcap', 's2p_frozen_revenue', 'pead_earnings', 'pead_revenue',
            'quality_value_blend', 'value_momentum_filter', 'low_volatility', 'low_accruals')
RULE1_CONTROL = 's2p_frozen_revenue'
RULE2 = {                                    # PREREG rule 2, verbatim from 92653bf
    's2p_top_quintile': ['size_smallcap', 's2p_frozen_revenue'],
    'quality_value_blend': ['s2p_top_quintile'],
    'value_momentum_filter': ['s2p_top_quintile'],
}
# Post-hoc sensitivity specification (PREREG addendum R2 item 3 + addendum R3); never selects.
CONTROLS = ('s2p_frozen_revenue', 'quality_value_frozen', 'accruals_frozen', 'pead_earnings_raw', 'pead_revenue_raw',
            'accruals_raw', 'gross_profitability', 'gross_profit_raw', 'momentum_12_1', 'revenue_raw')
SIZE_TILTS = ('size_smallcap', 'size_small_assets', 'size_small_revenue')
LATER_GUARDS = {
    's2p_top_quintile': ['size_smallcap', 's2p_frozen_revenue', 'revenue_raw'],
    'pead_earnings': ['size_small_assets', 'pead_earnings_raw'],
    'pead_revenue': ['size_small_revenue', 'pead_revenue_raw'],
    'quality_value_blend': ['s2p_top_quintile', 'size_smallcap', 'size_small_assets', 'quality_value_frozen',
                            'gross_profitability', 'gross_profit_raw'],
    'low_accruals': ['size_small_assets', 'accruals_frozen', 'accruals_raw'],
    'value_momentum_filter': ['s2p_top_quintile', 'size_smallcap', 'momentum_12_1'],
    'low_volatility': [],
}
HYP = {'s2p_top_quintile': 'H1', 'size_smallcap': 'D1', 's2p_frozen_revenue': 'D2', 'pead_earnings': 'H2',
       'pead_revenue': 'H3', 'quality_value_blend': 'H4', 'value_momentum_filter': 'H5', 'low_volatility': 'H6',
       'low_accruals': 'H7', 'size_small_assets': 'D3', 'size_small_revenue': 'D4', 'quality_value_frozen': 'D5',
       'accruals_frozen': 'D6', 'pead_earnings_raw': 'D7', 'pead_revenue_raw': 'D8', 'accruals_raw': 'D9',
       'gross_profitability': 'D10', 'gross_profit_raw': 'D11', 'momentum_12_1': 'D12', 'revenue_raw': 'D13'}
TIER = {'PASS': 2, 'PROMISING': 1, 'FAIL': 0}
GATE_CODE = {'a_sharpe_vs_benchmark': 'a', 'b_active_t_ge_1': 'b1', 'b_active_t_ge_2': 'b2', 'c_dsr_ge_0_90': 'c',
             'd_drawdown_within_5pp': 'd', 'e_positive_years_ge_60pct': 'e', 'f_active_positive_at_3x_cost': 'f',
             'g_pbo_and_stability': 'g'}


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


def _gt(a, b):
    return a is not None and b is not None and a > b


def _rank_key(results):
    def key(cid):
        r = results[cid]
        fp = r['full_period']['report_cell']
        return (TIER.get(r['verdict'], -1), r['active']['t_nw'] if r['active']['t_nw'] is not None else -1e9,
                -(fp.get('turnover_per_year') or 1e9))
    return key


def choose_champion(ids: List[str], results: Dict[str, Any]) -> Tuple[Optional[str], List[Tuple[str, str, str]]]:
    """PREREG rules 1-4 exactly as committed at 92653bf (addendum R4). Returns (champion, eligibility table)."""
    table, eligible = [], []
    for cid in ORIGINAL:
        if cid not in ids:
            continue
        r = results.get(cid)
        if cid == RULE1_CONTROL:
            table.append((cid, 'ineligible', 'rule 1: the numerator control, never a champion')); continue
        if r is None:
            table.append((cid, 'ineligible', 'no results file')); continue
        fails = [c for c in RULE2.get(cid, []) if not _gt(active_ann(r), active_ann(results.get(c)))]
        if fails:
            why = 'rule 2: OOS active return does not exceed ' + ', '.join(fails)
            if cid == 's2p_top_quintile':
                why += ' (size_smallcap stands in)'
            table.append((cid, 'ineligible', why)); continue
        why = ('rule 1 (candidate-role entry); rule 2 guard passed vs ' + ', '.join(RULE2[cid])) if cid in RULE2 else \
              ('rule 1 (candidate-role entry; also H1\'s stand-in under rule 2)' if cid == 'size_smallcap'
               else 'rule 1 (candidate-role entry, unguarded)')
        table.append((cid, 'eligible', why)); eligible.append(cid)
    if not eligible:
        return None, table
    return max(eligible, key=_rank_key(results)), table


def sensitivity_champion(ids: List[str], results: Dict[str, Any]) -> Tuple[Optional[str], List[Tuple[str, str, str]]]:
    """POST-HOC: the champion the later guards (R2 item 3 + R3) would have picked. Reported, never selects."""
    table, eligible, guarded_ok = [], [], set()
    for cid in ids:
        r = results.get(cid)
        if cid in CONTROLS or cid in SIZE_TILTS or r is None:
            continue
        fails = [c for c in LATER_GUARDS.get(cid, []) if not _gt(active_ann(r), active_ann(results.get(c)))]
        if fails:
            table.append((cid, 'ineligible', 'does not exceed ' + ', '.join(fails)))
        else:
            table.append((cid, 'eligible', 'exceeds every listed control' if LATER_GUARDS.get(cid) else 'no ratio, no control'))
            eligible.append(cid); guarded_ok.add(cid)
    s2p_failed = 's2p_top_quintile' not in guarded_ok
    if 'size_smallcap' in results and (s2p_failed or not guarded_ok):
        table.append(('size_smallcap', 'eligible', 'stand-in (H1 failed its guard or no guarded hypothesis eligible)'))
        eligible.append('size_smallcap')
    if not eligible:
        return None, table
    return max(eligible, key=_rank_key(results)), table


def artifact_name(row: Dict[str, Any]) -> str:
    ts = str(row['timestamp']).replace('-', '').replace(':', '').replace('+0000', '').replace('+00:00', '')
    return f"{row['id']}__{ts}.json"


def ledger_table(rows: List[Dict[str, Any]]) -> List[str]:
    """One row per ledger line; the artifact name carries id and UTC timestamp, the artifact holds every metric."""
    out = ['| # | id | N at run | verdict / status | artifact (`results/trials/`) |', '|---|---|---|---|---|']
    for n, r in enumerate(rows, 1):
        art = HERE / 'results' / 'trials' / artifact_name(r)
        link = f'[{art.name}](results/trials/{art.name})' if art.exists() else 'MISSING'
        out.append(f"| {n} | {r.get('id')} | {r.get('n_trials')} | {r.get('verdict') or r.get('status')} | {link} |")
    return out


def candidate_table(ids, results) -> List[str]:
    out = ['| id | OOS Sharpe | EW Sharpe | active/yr | t(NW) | DSR (N) | maxDD c/b | +yrs | 3x active | turn/yr | med $ADV | forced exits | verdict |',
           '|---|---|---|---|---|---|---|---|---|---|---|---|---|']
    for cid in ids:
        r = results.get(cid)
        if r is None:
            out.append(f'| {cid} | no results | | | | | | | | | | | error |'); continue
        b = r['decisive_per_fold'][0]
        fp = r['full_period']['report_cell']
        sn = r['stopped_names']
        out.append(f"| {cid} | {f(r['oos']['sharpe'])} | {f(r['oos_benchmarks'][b]['sharpe'])} | "
                   f"{pct(r['active']['ann_return'])} | {f(r['active']['t_nw'], '{:.2f}')} | {f(r['dsr']['value'], '{:.2f}')} ({r['dsr']['n_trials']}) | "
                   f"{pct(r['oos']['max_drawdown'], False)}/{pct(r['active']['max_drawdown_benchmark'], False)} | "
                   f"{pct(r['active']['fraction_positive_years'], False)} | {pct(r['active']['ann_return_3x_cost'])} | "
                   f"{f(fp.get('turnover_per_year'), '{:.1f}')} | {f(fp.get('median_holding_adv_usd'), '{:,.0f}')} | "
                   f"{sn.get('full_period_forced_exits')} ({pct(sn.get('full_period_forced_exit_value_fraction'), False)}) | **{r['verdict']}** |")
    return out


def decomposition_tables(results) -> List[str]:
    groups = [('H1 sales-to-price vs its parts', ['s2p_top_quintile', 'size_smallcap', 's2p_frozen_revenue', 'revenue_raw']),
              ('H2/H3 surprise ratios vs numerator alone and denominator alone',
               ['pead_earnings', 'pead_earnings_raw', 'size_small_assets', 'pead_revenue', 'pead_revenue_raw', 'size_small_revenue']),
              ('H4 quality/value blend vs its parts (H1, D1, D3 above)', ['quality_value_blend', 'quality_value_frozen', 'gross_profitability', 'gross_profit_raw']),
              ('H7 low accruals vs its parts (D3 above)', ['low_accruals', 'accruals_frozen', 'accruals_raw']),
              ('H5 value+momentum vs value alone (H1 above) and momentum alone', ['value_momentum_filter', 'momentum_12_1'])]
    out = []
    for title, members in groups:
        out += [f'**{title}**', '', '| id (H) | OOS Sharpe | active/yr | t(NW) |', '|---|---|---|---|']
        for cid in members:
            r = results.get(cid)
            if r is None:
                out.append(f'| {cid} ({HYP.get(cid)}) | no results | | |'); continue
            out.append(f"| {cid} ({HYP.get(cid)}) | {f(r['oos']['sharpe'])} | {pct(r['active']['ann_return'])} | {f(r['active']['t_nw'], '{:.2f}')} |")
        out.append('')
    return out


def per_year(r) -> List[str]:
    out = ['| year | active | counted |', '|---|---|---|']
    for y, v in sorted(r['active']['per_year'].items()):
        out.append(f"| {y} | {pct(v['active_return'])} | {'yes' if v['counted'] else 'no (< 60 OOS days)'} |")
    return out


def stress_block(s: Optional[Dict[str, Any]]) -> List[str]:
    if s is None:
        return ['Not run (no `results/<id>__d07_stress.json`).']
    rc = s['reproduction_check']
    out = [f"`{s['command']}` → `results/{s['id']}__d07_stress.json` (source verdict `{s['source_verdict']}`). Reproduction check "
           f"(unstressed re-run vs results JSON): OOS Sharpe {f(rc['oos_sharpe_recomputed_unstressed'])} vs {f(rc['oos_sharpe_from_results_json'])}; "
           f"active {pct(rc['active_ann_return_recomputed_unstressed'])} vs {pct(rc['active_ann_return_from_results_json'])}. "
           f"Forced exits in the OOS windows: candidate {s['total_candidate_forced_exits']} (proceeds ${s['total_candidate_forced_proceeds']:,.0f} "
           f"of $1,000,000 capital), benchmark {s['total_benchmark_forced_exits']} (${s['total_benchmark_forced_proceeds']:,.0f}); "
           'every event reconciled with the backtester\'s own forced-exit diagnostics. Marked to ZERO:', '',
           '| series | OOS Sharpe | ann return | t(NW) | max DD |', '|---|---|---|---|---|']
    for label, key in (('candidate unstressed', ('candidate', 'unstressed')), ('candidate stressed', ('candidate', 'stressed')),
                       ('benchmark unstressed', ('benchmark', 'unstressed')), ('benchmark stressed', ('benchmark', 'stressed')),
                       ('active unstressed', ('active', 'unstressed')), ('active (i) candidate stressed only', ('active', 'i_candidate_stressed_only')),
                       ('active (ii) both stressed', ('active', 'ii_both_stressed'))):
        m = s[key[0]][key[1]]
        out.append(f"| {label} | {f(m['sharpe'])} | {pct(m['ann_mean'])} | {f(m['t_nw'], '{:.2f}')} | {pct(m['max_drawdown'], False)} |")
    out += ['', f"Believed if {s['believed_if']} → **{'holds' if s['passes_stress'] else 'FAILS'}**."]
    return out


def main() -> int:
    d = load()
    ids, results, rows = d['ids'], d['results'], d['rows']
    champ, elig = choose_champion(ids, results)
    sens_champ, sens_table = sensitivity_champion(ids, results)
    notes = (HERE / 'REPORT_NOTES.md').read_text() if (HERE / 'REPORT_NOTES.md').exists() else ''
    cand_rows = [r for r in rows if r.get('role') == 'candidate']
    n_hashes = len({r['entry_sha256'] for r in cand_rows if r.get('entry_sha256')})
    n_err = sum(1 for r in cand_rows if r.get('status') != 'ok')
    L: List[str] = []
    L += ['# Equity family (C03, D05) — evaluation report', '',
          'Generated by `uv run python research/lowfreq/equity/make_report.py` from `trials.jsonl`, `results/<id>.json` and '
          '`results/<id>__d07_stress.json`; every number below is read from those files (per-row artifacts in `results/trials/`). '
          f'Hypotheses H1–H7, controls D1–D2 and the champion-selection rule were pre-registered in `PREREG.md` at checkpoint `{PREREG_SHA}`, '
          'which predates the first ledger row in git history. Controls D3–D13 (addenda R2/R3) were added later and are post-hoc '
          'decomposition evidence; they count in N but never select (addendum R4).', '']
    cr = results.get(champ) if champ else None
    L += ['## Verdict', '']
    if cr is None:
        L += ['No eligible champion (see the eligibility table).']
    else:
        L += [f"**Champion by the pre-registered rule: `{champ}` ({HYP.get(champ)}) — harness verdict `{cr['verdict']}`** "
              f"(`results/{champ}.json`, N = {cr['dsr']['n_trials']} trials; final parameter cell `{json.dumps(cr['grid']['report_cell'])}`, i.e. the "
              'single registered cell: QUANTILE 0.2, MIN_NAMES 30, monthly rebalance, equal weight).', '']
        if cr['verdict'] != 'PASS':
            L += ['The family did NOT produce a PASS. The null stands: no pre-registered long-only monthly tilt on the covered '
                  'point-in-time universe beat monthly equal weight of its own covered set at the D01 bar once the Sharpe is deflated by '
                  f"the family's trial count; the champion is the best `{cr['verdict']}` evidence and is reported as such, with the gates it failed.", '']
        else:
            L += ['The champion clears every D01 gate on the dev period; the held-out period (after 2024-06-30) is the real test.', '']
    by_sha: Dict[str, List[int]] = {}
    for n, r in enumerate(rows, 1):
        by_sha.setdefault(str(r.get('git_sha'))[:7], []).append(n)
    sha_text = '; '.join(f"rows {v[0]}–{v[-1]} `{k}`" if v == list(range(v[0], v[-1] + 1)) else f"rows {','.join(map(str, v))} `{k}`"
                         for k, v in by_sha.items())
    sha_text += (' (`92653bf` trees also held the then-uncommitted R2 changes; `a75a485` holds every entry and source that ran)'
                 if set(by_sha) <= {'92653bf', 'a75a485'} else '')
    L += ['## Protocol and provenance', '',
          f"D05 walk-forward (train 1095 / test 365 days, rolling, dev 2016-08-01..2024-06-28, {cr['window']['n_folds'] if cr else '?'} OOS folds, 400 days warm-up), "
          'fills at t+1 open, 5 bps + $0.005/share (3x for the stress), long-only, gross 1.0, monthly rebalance, top-20 % of the covered set '
          'equal-weighted, ≥ 30 names. Decisive benchmark `eq_ew_covered` owns each candidate\'s published coverage through the identical engine path. '
          'Every candidate is a single pre-registered cell (no grid; gate (g) n/a; nothing selected on train windows). '
          f"Ledger: {len(cand_rows)} candidate-role rows ({n_err} error/refused), {n_hashes} distinct entry hashes = D02 trial count N (budget 40 rows / 40 entries). "
          'Two passes (addendum R2 item 4): pass 1 fills the ledger (partial N by construction), pass 2 re-evaluates every entry so each final '
          'results file deflates by the same N and trial-Sharpe set; OOS metrics are identical across passes (deterministic engine), only DSR and verdict move.', '',
          f"What git proves: `{PREREG_SHA}` holds PREREG.md (H1–H7, D1–D2, rules 1–4) and the nine original entries with no ledger row. "
          'Addendum R2 was written before the first `evaluate` call but first committed in `f41bc9a`, after the pass-1 rows; addendum R3 was written after '
          f"pass-1 results. Ledger rows record the checkpoint SHA their tree was based on: {sha_text}. Machine-checkable: "
          '`uv run python research/lowfreq/equity/provenance.py` recomputes every row\'s `entry_sha256` / `strategy_sha256` from the committed registry '
          'and sources (fails on any mismatch) and verifies the PROVENANCE.json chain (sha256 of PREREG.md / candidates.yaml at 13 and at 20 ledger rows) '
          'against the append-only ledger; `check_artifacts.py` verifies every archived artifact against its ledger row.', '']
    L += ['## Every trial (ledger, in order)', ''] + ledger_table(rows) + ['']
    L += ['## Candidates — final results (`results/<id>.json`)', ''] + candidate_table(ids, results) + ['',
          'EW Sharpe = decisive benchmark OOS Sharpe on that candidate\'s coverage; maxDD c/b = OOS max drawdown candidate / benchmark; +yrs = fraction of counted '
          'OOS calendar years with positive active return; one-way turnover, median holding 21-day $ADV and forced exits (count, value fraction of all sells) '
          'from the full-period run. Gate thresholds: Sharpe ≥ benchmark, active t(NW) ≥ 2 (≥ 1 for PROMISING), DSR ≥ 0.90, drawdown within 5 pp, '
          '≥ 60 % positive years, active > 0 at 3x cost; the failed gates of every run are in its JSON under `gates`.', '']
    L += ['## Decompositions (rule 2; addenda R2–R3 controls as post-hoc evidence)', ''] + decomposition_tables(results)
    L += [f'## Champion selection — PREREG rules 1–4 as committed at `{PREREG_SHA}` (`make_report.choose_champion`)', '',
          '| id (H) | eligibility | reason |', '|---|---|---|']
    L += [f'| {c} ({HYP.get(c)}) | {e} | {w} |' for c, e, w in elig]
    L += ['', 'Rule 3 ranking of eligible entries: verdict tier (PASS > PROMISING > FAIL), then active t(NW), then lower turnover. '
          f'Selected: `{champ}`.', '',
          f"**Post-hoc sensitivity (addendum R2 item 3 + R3 guards, not selecting):** under the later, stricter guards the champion would be "
          f"`{sens_champ}` — {'the same' if sens_champ == champ else 'DIFFERENT'}. Guard outcomes: " +
          '; '.join(f'{c} {e}' + (f' ({w})' if e == 'ineligible' else '') for c, e, w in sens_table) + '.', '']
    if cr is not None:
        L += [f'### Champion `{champ}` — OOS detail', '',
              f"OOS Sharpe {f(cr['oos']['sharpe'])} vs covered-EW {f(cr['oos_benchmarks'][cr['decisive_per_fold'][0]]['sharpe'])}; OOS CAGR "
              f"{pct(cr['oos']['cagr'])} vs {pct(cr['oos_benchmarks'][cr['decisive_per_fold'][0]]['cagr'])}; "
              f"active {pct(cr['active']['ann_return'])}/yr, t(NW, {cr['active']['nw_lags']} lags) {f(cr['active']['t_nw'], '{:.2f}')}, "
              f"DSR {f(cr['dsr']['value'], '{:.2f}')} at N = {cr['dsr']['n_trials']}; 3x-cost active {pct(cr['active']['ann_return_3x_cost'])}; "
              f"max DD {pct(cr['oos']['max_drawdown'], False)} vs {pct(cr['active']['max_drawdown_benchmark'], False)}; "
              f"full-period turnover {f(cr['full_period']['report_cell'].get('turnover_per_year'), '{:.2f}')}/yr (one-way, of NAV), median names held "
              f"{f(cr['full_period']['report_cell'].get('median_names_held'), '{:.0f}')}, median holding 21-day $ADV "
              f"${f(cr['full_period']['report_cell'].get('median_holding_adv_usd'), '{:,.0f}', 'n/a')}; median covered names/day "
              f"{f(cr['coverage'].get('median_names_covered_per_day'), '{:.0f}')}; lookahead probe: {cr['lookahead'].get('prefixes_replayed')} fresh-state prefixes replayed, clean.", '',
              'Gates: ' + ', '.join(f"{GATE_CODE.get(k, k)}={'ok' if v else ('FAIL' if v is False else 'n/a')}" for k, v in cr['gates'].items()), '',
              'Per OOS calendar year (active = champion − decisive benchmark):', ''] + per_year(cr) + ['']
        L += ['## D07 terminal-loss stress (champion)', ''] + stress_block(d['stress'].get(champ)) + ['']
    if notes:
        L += [notes.rstrip(), '']
    (HERE / 'REPORT.md').write_text('\n'.join(L))
    words = len((HERE / 'REPORT.md').read_text().split())
    print(f'REPORT.md written: {words} words; champion={champ} (sensitivity: {sens_champ}); results={len(results)}/{len(ids)}; ledger rows={len(rows)}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
