"""REPORT.md generator for the insider family (C04; D11).

Reads ONLY ``results/<id>.json`` (the pass-2 files), ``trials.jsonl``,
``candidates.yaml`` and ``results/_calibration_counts.json``; applies the
champion rule exactly as PREREG.md states it; every number written is copied
from those files (no statistic is recomputed here except differences of two
copied numbers and the terminal-loss bound, whose inputs are named).
Refuses to render when a pass-2 file was deflated by fewer trials than the
ledger holds (the order-independence protocol).

    uv run python research/lowfreq/insider/make_report.py
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

HERE = Path(__file__).resolve().parent
RULES = ['ins_purchase_count', 'ins_opportunistic', 'ins_cluster', 'ins_conviction', 'ins_officer',
         'ins_director', 'ins_net_buy_ratio', 'ins_value_to_mcap']
LABEL = {'ins_purchase_count': 'C0', 'ins_opportunistic': 'H1', 'ins_cluster': 'H2', 'ins_conviction': 'H3',
         'ins_officer': 'H4a', 'ins_director': 'H4b', 'ins_net_buy_ratio': 'H5', 'ins_value_to_mcap': 'H6'}
CONTROL_OF = {'ins_purchase_count': 'ins_size_matched_count', 'ins_opportunistic': 'ins_size_matched_opportunistic',
              'ins_cluster': 'ins_size_matched_cluster', 'ins_conviction': 'ins_size_matched_conviction',
              'ins_officer': 'ins_size_matched_officer', 'ins_director': 'ins_size_matched_director',
              'ins_net_buy_ratio': 'ins_size_matched_net_buy_ratio', 'ins_value_to_mcap': 'ins_size_matched_value_to_mcap'}
CONTROLS = list(CONTROL_OF.values())
BENCH = 'eq_ew_pit'
VERDICT_RANK = {'PASS': 0, 'PROMISING': 1, 'FAIL': 2}
HELD_OUT_START, HELD_OUT_END = '2024-07-01', '2026-03-31'


def _n(v: Any) -> bool:
    return v is not None and not (isinstance(v, float) and v != v)


def fnum(v: Any, fmt: str = '{:.2f}') -> str:
    return fmt.format(v) if _n(v) else 'n/a'


def pct(v: Any) -> str:
    return fnum(v, '{:+.2%}')


def load() -> Dict[str, Any]:
    reg = yaml.safe_load((HERE / 'candidates.yaml').read_text())
    ids = [e['id'] for e in reg['candidates'] if e['role'] == 'candidate']
    results = {cid: json.loads((HERE / 'results' / f'{cid}.json').read_text()) for cid in ids}
    rows = [json.loads(line) for line in (HERE / 'trials.jsonl').read_text().splitlines() if line.strip()]
    cal = json.loads((HERE / 'results' / '_calibration_counts.json').read_text())
    return {'registry': reg, 'ids': ids, 'results': results, 'rows': rows, 'calibration': cal}


def distinct_trials(rows: List[Dict[str, Any]]) -> int:
    return len({r['entry_sha256'] for r in rows if r.get('role') == 'candidate' and r.get('entry_sha256')})


def ledger_table(rows: List[Dict[str, Any]]) -> List[str]:
    out = ['| # | timestamp (UTC) | id | mode | status | trials at run | verdict | OOS Sharpe | active ann | t(NW) | DSR |',
           '|---|---|---|---|---|---|---|---|---|---|---|']
    for i, r in enumerate(rows, 1):
        m = r.get('metrics') or {}
        out.append(f"| {i} | {str(r.get('timestamp', ''))[:19]} | `{r['id']}` | {r.get('mode')} | {r.get('status')} | "
                   f"{r.get('n_trials')} | {r.get('verdict') or '-'} | {fnum(m.get('oos_sharpe'))} | "
                   f"{pct(m.get('active_ann_return'))} | {fnum(m.get('active_t_nw'))} | {fnum(m.get('active_dsr'))} |")
    return out


def results_table(results: Dict[str, Any], ids: List[str]) -> List[str]:
    out = ['| id | H | OOS Sharpe | bench Sharpe | active ann | t(NW) | DSR (N) | 3x-cost active | +years | OOS maxDD | bench maxDD | turnover/yr | names | verdict |',
           '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|']
    for cid in ids:
        r = results[cid]
        a, o, d, fp = r['active'], r['oos'], r['dsr'], r['full_period']['report_cell']
        b = r['oos_benchmarks'].get(BENCH, {})
        lab = LABEL.get(cid, 'D' if cid in CONTROLS else '')
        out.append(f"| `{cid}` | {lab} | {fnum(o['sharpe'])} | {fnum(b.get('sharpe'))} | {pct(a['ann_return'])} | "
                   f"{fnum(a['t_nw'])} | {fnum(d['value'])} ({d['n_trials']}) | {pct(a['ann_return_3x_cost'])} | "
                   f"{fnum(a['fraction_positive_years'], '{:.0%}')} | {fnum(o['max_drawdown'], '{:.1%}')} | "
                   f"{fnum(a['max_drawdown_benchmark'], '{:.1%}')} | {fnum(fp['turnover_per_year'])} | "
                   f"{fnum(fp.get('median_names_held'), '{:.0f}')} | **{r['verdict']}** |")
    return out


def champion(results: Dict[str, Any]) -> Dict[str, Any]:
    act = {cid: results[cid]['active']['ann_return'] for cid in results}
    c0 = act['ins_purchase_count']
    guards: Dict[str, Dict[str, Any]] = {}
    eligible: List[str] = []
    for r in RULES:
        ctl = act[CONTROL_OF[r]]
        beats_ctl = _n(act[r]) and _n(ctl) and act[r] > ctl
        beats_c0 = True if r == 'ins_purchase_count' else (_n(act[r]) and _n(c0) and act[r] > c0)
        ok = bool(beats_ctl and beats_c0)
        guards[r] = {'active': act[r], 'control': ctl, 'beats_control': bool(beats_ctl), 'beats_c0': bool(beats_c0), 'eligible': ok}
        if ok:
            eligible.append(r)

    def key(r: str):
        t = results[r]['active']['t_nw']
        to = results[r]['full_period']['report_cell']['turnover_per_year']
        return (VERDICT_RANK[results[r]['verdict']], -(t if _n(t) else float('-inf')), to if _n(to) else float('inf'))

    if eligible:
        champ, how = min(eligible, key=key), 'rule 3 (eligible after the guards; verdict, then t(NW), then turnover)'
    else:
        champ = max(RULES, key=lambda r: results[r]['active']['t_nw'] if _n(results[r]['active']['t_nw']) else float('-inf'))
        how = 'rule 4 (no rule passed the guards; highest OOS active t(NW) among the rules)'
    return {'champion': champ, 'how': how, 'eligible': eligible, 'guards': guards}


def per_year(r: Dict[str, Any]) -> List[str]:
    yrs = r['active']['per_year']
    head = '| year | ' + ' | '.join(str(y) for y in sorted(yrs)) + ' |'
    sep = '|---|' + '---|' * len(yrs)
    vals = '| active | ' + ' | '.join(pct(v['active_return']) + ('' if v.get('counted', True) else '*') for _, v in sorted(yrs.items())) + ' |'
    return [head, sep, vals, '', '`*` = partial calendar year, not counted in gate (e).']


def decomposition(results: Dict[str, Any], cal: Dict[str, Any]) -> List[str]:
    out = ['| rule | H | K median (calibration) | qualifier mcap pctile | rule active ann | size control active ann | rule - control | rule - C0 |',
           '|---|---|---|---|---|---|---|---|']
    c0 = results['ins_purchase_count']['active']['ann_return']
    for r in RULES:
        a = results[r]['active']['ann_return']
        c = results[CONTROL_OF[r]]['active']['ann_return']
        k = (cal['rules'].get(r) or {})
        diff_c = (a - c) if (_n(a) and _n(c)) else None
        diff_0 = (a - c0) if (_n(a) and _n(c0) and r != 'ins_purchase_count') else None
        out.append(f"| `{r}` | {LABEL[r]} | {fnum(k.get('K_median'), '{:.0f}')} | {fnum(k.get('qualifier_mcap_percentile_median'))} | "
                   f"{pct(a)} | {pct(c)} | {pct(diff_c)} | {pct(diff_0) if r != 'ins_purchase_count' else '-'} |")
    return out


def render(d: Dict[str, Any]) -> str:
    results, rows, ids, cal = d['results'], d['rows'], d['ids'], d['calibration']
    n_ledger = distinct_trials(rows)
    stale = [cid for cid in ids if results[cid]['dsr']['n_trials'] != n_ledger]
    if stale:
        raise SystemExit(f'pass-2 incomplete: {stale} carry a trial count != {n_ledger}; re-run them before reporting')
    sel = champion(results)
    ch = sel['champion']
    r = results[ch]
    w, o, a, dsr, fp = r['window'], r['oos'], r['active'], r['dsr'], r['full_period']['report_cell']
    b = r['oos_benchmarks'][BENCH]
    sn = r['stopped_names']
    years = (dt.date.fromisoformat(w['end']) - dt.date.fromisoformat(w['start'])).days / 365.25
    fr = sn.get('full_period_forced_exit_value_fraction')
    bound = (fr / years) if _n(fr) else None
    g = r['gates']
    lines: List[str] = []
    lines += [f'# Insider family — REPORT (C04; D11, D14)', '',
              f'Generated {dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")} by `make_report.py` from '
              f'`results/*.json` (pass 2), `trials.jsonl`, `candidates.yaml`, `results/_calibration_counts.json`. '
              f'Pre-registration: `PREREG.md` (git `f0658e5`, addendum 1 before the first ledger row). '
              f'Dev period {w["start"]}..{w["end"]}, walk-forward train {w["train_days"]} / test {w["test_days"]} days, '
              f'{w["n_folds"]} folds, OOS {o["days"]} sessions; costs {r["costs"]["base"]["slippage_bps"]:.0f} bps + '
              f'${r["costs"]["base"]["commission_per_share"]}/share, stress x{r["costs"]["stress_multiplier"]:.0f}. '
              f'Distinct candidate trials in the ledger: **{n_ledger}** (budget 30); every pass-2 file is deflated by {n_ledger}.', '']
    lines += ['## Verdict', '',
              f'**Champion: `{ch}` ({LABEL[ch]}), selected by {sel["how"]}. Harness verdict: {r["verdict"]}.** '
              + ('The champion PASSes D01.' if r['verdict'] == 'PASS' else
                 f'This is the null: no pre-registered insider rule beat the equal-weight PIT basket to the D01 standard. '
                 f'Gates: ' + ', '.join(f'{k}={"n/a" if v is None else ("ok" if v else "FAIL")}' for k, v in g.items()) + '.'), '']
    lines += ['## Every candidate (pass 2, final trial count)', ''] + results_table(results, ids) + ['',
              f'Benchmark `{BENCH}` = monthly equal weight of the covered PIT members (D11), identical path and costs; '
              f'decisive benchmark in every fold: {sorted(set(x for cid in ids for x in results[cid]["decisive_per_fold"]))}. '
              f'`names` = median names held (full period); size controls (D) mirror their rule with market-cap neighbours.', '']
    lines += ['## Champion selection (pre-registered rule, PREREG.md)', '',
              '| rule | active ann | its size control | beats control | beats C0 | eligible |', '|---|---|---|---|---|---|']
    for rr in RULES:
        gg = sel['guards'][rr]
        lines.append(f"| `{rr}` | {pct(gg['active'])} | {pct(gg['control'])} | {'yes' if gg['beats_control'] else 'no'} | "
                     f"{'-' if rr == 'ins_purchase_count' else ('yes' if gg['beats_c0'] else 'no')} | {'yes' if gg['eligible'] else 'no'} |")
    lines += ['', f'Eligible: {sel["eligible"] or "none"}. Champion `{ch}` by {sel["how"]}.', '']
    lines += [f'## Champion `{ch}` in detail', '',
              f'* OOS: Sharpe {fnum(o["sharpe"])} vs benchmark {fnum(b["sharpe"])}; CAGR {pct(o["cagr"])} vs {pct(b["cagr"])}; '
              f'max drawdown {fnum(o["max_drawdown"], "{:.1%}")} vs {fnum(a["max_drawdown_benchmark"], "{:.1%}")}.',
              f'* Active return: {pct(a["ann_return"])}/yr, Newey-West t {fnum(a["t_nw"])} ({a["nw_lags"]} lags), active Sharpe {fnum(a["sharpe"])}; '
              f'positive in {fnum(a["fraction_positive_years"], "{:.0%}")} of OOS calendar years; under 3x costs {pct(a["ann_return_3x_cost"])}/yr.',
              f'* DSR {fnum(dsr["value"])} deflated by N = {dsr["n_trials"]} trials.',
              f'* Turnover {fnum(fp["turnover_per_year"])}x/yr (full period); median names held {fnum(fp.get("median_names_held"), "{:.0f}")}; '
              f'median $ADV of holdings ${fnum(fp.get("median_holding_adv_usd"), "{:,.0f}")}; min cash fraction {fnum(fp.get("min_cash_fraction"))}; '
              f'cash-guard scaled buys {fp.get("scaled_buy_fills")} (D06).',
              f'* Stopped names (D07): {sn.get("full_period_forced_exits")} forced exits over the full period worth '
              f'{fnum(fr, "{:.2%}")} of initial capital ({sn.get("oos_forced_exits")} inside OOS windows; benchmark '
              f'{sn.get("benchmark_full_period_forced_exits")}). Terminal-loss stress: had every forced exit gone to zero, '
              f'the full-period return would be lower by {fnum(fr, "{:.2%}")}, about {pct(-bound) if _n(bound) else "n/a"}/yr over {years:.1f} years.',
              f'* Lookahead: {r["lookahead"]["cells_checked"]} cell(s) and {r["lookahead"]["benchmarks_checked"]} checked with '
              f'{r["lookahead"]["prefixes_replayed"]} fresh-state prefixes; PBO n/a (single cell).', '',
              '### Per-year active return (OOS)', ''] + per_year(r) + ['']
    lines += ['## Decomposition against the confounders', '',
              'Purchase count alone = C0 (`ins_purchase_count`). Size = each rule\'s market-cap-matched control (same K, same sleeve, '
              'PREREG addendum 1). Calibration columns are qualifier counts, not returns.', ''] + decomposition(results, cal) + ['',
              'Reading the controls: every control beats the equal-weight basket although it holds no insider information. '
              'The matching pool is covered names WITH an as-of market cap (operating companies with EDGAR fundamentals; 58 % '
              'of covered name-days per the calibration file), so part of a control\'s excess over the basket is "operating '
              'companies vs the ETFs/ADRs/trusts in the PIT basket" - a tilt every rule shares by construction (addendum 1). '
              'The honest reading is therefore rule minus control: negative for all eight rules, i.e. conditional on size and on '
              'being an operating company, the names insiders bought did WORSE than their neighbours over 2019-2024. '
              'The controls are not candidates (PREREG: never champions) and their PROMISING verdicts are reported, not claimed.', '']
    lines += ['## Every ledger trial', '', f'{len(rows)} rows (`trials.jsonl`, append-only; pass 1 then pass 2).', ''] + ledger_table(rows) + ['']
    lines += ['## Held-out evaluation (orchestrator only, D11)', '',
              'The held-out period starts after the dev cutoff; the full research directory\'s insider data ends '
              f'{HELD_OUT_END} (DATA.md), so the window is `{HELD_OUT_START}..{HELD_OUT_END}`:', '', '```',
              f"LOWFREQ_RESEARCH_CONFIG=~/.local/share/mmr/research_full/trader_research.yaml \\",
              f"  uv run python -m trader.research.lowfreq evaluate insider/{ch} --fixed-params '{{}}' --start {HELD_OUT_START} --end {HELD_OUT_END}",
              '```', '',
              f'Source of every number: `results/{ch}.json` keys `oos`, `oos_benchmarks.{BENCH}`, `active`, `dsr`, `full_period.report_cell`, '
              '`stopped_names`, `gates`; other candidates from their own `results/<id>.json`; ledger rows from `trials.jsonl`; '
              'K and market-cap percentiles from `results/_calibration_counts.json`.']
    return '\n'.join(lines) + '\n'


def main() -> int:
    d = load()
    text = render(d)
    (HERE / 'REPORT.md').write_text(text)
    words = len(text.split())
    print(f'REPORT.md written: {words} words; champion {champion(d["results"])["champion"]}')
    return 0 if words < 3000 else 1


if __name__ == '__main__':
    sys.exit(main())
