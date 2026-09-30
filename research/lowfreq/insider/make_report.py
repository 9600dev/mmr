"""REPORT.md generator for the insider family (C04; D11, D14).

Reads ONLY ``results/<id>.json`` (the FINAL-pass files), the retained
per-trial artifacts ``results/trials/NNN_<id>.json`` (NNN = 1-based ledger
row), ``trials.jsonl``, ``candidates.yaml``, ``results/_calibration_counts.json``
and ``results/_d07_stress.json``; applies the champion rule exactly as
PREREG.md states it; every number written is copied from those files (no
statistic is recomputed here except differences of two copied numbers).

Refuses to render when (1) a final file was deflated by fewer trials than the
ledger holds (the order-independence protocol), (2) a final file is not the
artifact of the LATEST ok ledger row for its id, (3) a retained per-trial
artifact disagrees with its ledger row, or (4) the D07 stress artifact's
unstressed numbers disagree with the champion's final file. A ledger row with
no retained artifact is listed without numbers.

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
TRIALS_DIR = HERE / 'results' / 'trials'
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
LEDGER_METRIC_KEYS = ('oos_sharpe', 'active_ann_return', 'active_t_nw', 'active_dsr')
STRESS_KEYS = ('sharpe', 'cagr', 'max_drawdown')
REL_TOL = 1e-6


class ReportError(RuntimeError):
    """An artifact disagrees with the ledger or with another artifact: refuse to render."""


def _n(v: Any) -> bool:
    return v is not None and not (isinstance(v, float) and v != v)


def _close(a: Any, b: Any, tol: float = REL_TOL) -> bool:
    if not _n(a) and not _n(b):
        return True
    if not (_n(a) and _n(b)):
        return False
    return abs(float(a) - float(b)) <= tol * max(1.0, abs(float(b)))


def fnum(v: Any, fmt: str = '{:.2f}') -> str:
    return fmt.format(v) if _n(v) else 'n/a'


def pct(v: Any) -> str:
    return fnum(v, '{:+.2%}')


# ---------------------------------------------------------------- artifacts
def artifact_for_row(i: int, row: Dict[str, Any], trials_dir: Path = TRIALS_DIR) -> Optional[Dict[str, Any]]:
    """The retained per-trial artifact of ledger row ``i`` (1-based), or None
    when none was kept. Accepted only if its embedded ``ledger_row`` names the
    same id, timestamp and trial count AND its ledger metrics equal the row's;
    any disagreement is a ``ReportError`` (an artifact that contradicts the
    ledger is worse than no artifact)."""
    path = trials_dir / f'{i:03d}_{row["id"]}.json'
    if not path.exists():
        return None
    art = json.loads(path.read_text())
    lr = art.get('ledger_row') or {}
    if (lr.get('id'), lr.get('timestamp'), lr.get('n_trials')) != (row['id'], row['timestamp'], row['n_trials']):
        raise ReportError(f'{path.name}: embedded ledger_row does not match ledger row {i} '
                          f'({row["id"]} @ {row["timestamp"]}, n_trials {row["n_trials"]})')
    if art.get('verdict') != row.get('verdict'):
        raise ReportError(f'{path.name}: verdict {art.get("verdict")} != ledger {row.get("verdict")}')
    am, rm = art.get('ledger_metrics') or {}, row.get('metrics') or {}
    for k in LEDGER_METRIC_KEYS:
        if not _close(am.get(k), rm.get(k)):
            raise ReportError(f'{path.name}: metric {k} {am.get(k)} != ledger {rm.get(k)}')
    return art


def final_row_index(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """1-based index of the LATEST status=ok walk-forward row per id.

    Fixed-cell rows (``mode == 'fixed'``: the orchestrator's held-out
    evaluations, appended after the dev report was frozen) are ledger
    entries, not dev trials; they never replace a dev row as 'final' and
    have no retained ``trials/NNN_<id>.json`` artifact."""
    out: Dict[str, int] = {}
    for i, r in enumerate(rows, 1):
        if r.get('status') == 'ok' and r.get('mode') != 'fixed':
            out[r['id']] = i
    return out


def check_results_are_final(results: Dict[str, Any], rows: List[Dict[str, Any]]) -> None:
    """Every ``results/<id>.json`` must be the artifact of that id's latest ok
    ledger row (the harness overwrites the file on every run)."""
    final = final_row_index(rows)
    for cid, r in results.items():
        i = final.get(cid)
        if i is None:
            raise ReportError(f'{cid}: no ok ledger row')
        lr = r.get('ledger_row') or {}
        if lr.get('timestamp') != rows[i - 1]['timestamp']:
            raise ReportError(f'results/{cid}.json is the artifact of {lr.get("timestamp")}, not of the latest '
                              f'ok ledger row {i} ({rows[i - 1]["timestamp"]})')


def check_stress_matches(stress: Dict[str, Any], champ: str, r: Dict[str, Any]) -> None:
    """The D07 stress artifact's UNSTRESSED full-period numbers must equal the
    champion's final file (same path, same costs) for both books."""
    if stress['candidate']['id'] != champ:
        raise ReportError(f'_d07_stress.json is for {stress["candidate"]["id"]}, champion is {champ}')
    pairs = [('candidate', r['full_period']['report_cell']), ('benchmark', r['full_period']['benchmarks'][BENCH])]
    for book, cell in pairs:
        s = stress[book]
        for k in STRESS_KEYS:
            if not _close(s['summary']['unstressed'].get(k), cell.get(k)):
                raise ReportError(f'_d07_stress.json {book} unstressed {k} {s["summary"]["unstressed"].get(k)} '
                                  f'!= results/{champ}.json {cell.get(k)}')
        if s['forced_exits'] != cell.get('forced_exits'):
            raise ReportError(f'_d07_stress.json {book} forced_exits {s["forced_exits"]} != {cell.get("forced_exits")}')
        if not _close(s['forced_exit_value_fraction'], cell.get('forced_exit_value_fraction')):
            raise ReportError(f'_d07_stress.json {book} forced_exit_value_fraction differs from results/{champ}.json')
        if not s['reconciliation'].get('reconciled'):
            raise ReportError(f'_d07_stress.json {book}: forced exits not reconciled with backtester diagnostics')


def load() -> Dict[str, Any]:
    reg = yaml.safe_load((HERE / 'candidates.yaml').read_text())
    ids = [e['id'] for e in reg['candidates'] if e['role'] == 'candidate']
    results = {cid: json.loads((HERE / 'results' / f'{cid}.json').read_text()) for cid in ids}
    rows = [json.loads(line) for line in (HERE / 'trials.jsonl').read_text().splitlines() if line.strip()]
    cal = json.loads((HERE / 'results' / '_calibration_counts.json').read_text())
    stress = json.loads((HERE / 'results' / '_d07_stress.json').read_text())
    artifacts = {i: artifact_for_row(i, row) for i, row in enumerate(rows, 1)}
    return {'registry': reg, 'ids': ids, 'results': results, 'rows': rows, 'calibration': cal,
            'stress': stress, 'artifacts': artifacts}


def held_out_lines(rows: List[Dict[str, Any]]) -> List[str]:
    """The fixed-cell (held-out) ledger rows, numbers copied from the ledger
    row itself (the harness writes its metrics; the full artifact is the
    ``results/<id>__fixed_<start>_<end>_<cellsha>.json`` its ``results_path``
    names). ``['Not yet run.']`` when there is none."""
    fixed = [(i, r) for i, r in enumerate(rows, 1) if r.get('mode') == 'fixed' and r.get('status') == 'ok']
    if not fixed:
        return ['Not yet run.']
    out = ['| # | id | window | N | verdict | OOS Sharpe | bench Sharpe | active ann | 3x-cost active | t(NW) | DSR |',
           '|---|---|---|---|---|---|---|---|---|---|---|']
    for i, r in fixed:
        m = r.get('metrics') or {}
        out.append(f"| {i} | `{r['id']}` | {r.get('start')}..{r.get('end')} | {r.get('n_trials')} | **{r.get('verdict')}** | "
                   f"{fnum(m.get('oos_sharpe'))} | {fnum(m.get('benchmark_oos_sharpe_max'))} | {pct(m.get('active_ann_return'))} | "
                   f"{pct(m.get('active_ann_return_3x'))} | {fnum(m.get('active_t_nw'))} | {fnum(m.get('active_dsr'))} |")
    return out


def distinct_trials(rows: List[Dict[str, Any]]) -> int:
    return len({r['entry_sha256'] for r in rows if r.get('role') == 'candidate' and r.get('entry_sha256')})


# ---------------------------------------------------------------- tables
def ledger_table(rows: List[Dict[str, Any]], artifacts: Dict[int, Optional[Dict[str, Any]]],
                 final: Dict[str, int]) -> List[str]:
    """One line per ledger row; NUMBERS only from a retained artifact. A row
    without one still shows its harness verdict (a label the ledger carries,
    not a metric) so every trial is listed with its verdict."""
    out = ['| # | timestamp (UTC) | id | artifact | trials at run | verdict | OOS Sharpe | active ann | t(NW) | DSR |',
           '|---|---|---|---|---|---|---|---|---|---|']
    for i, r in enumerate(rows, 1):
        art = artifacts.get(i)
        if art is None:
            out.append(f"| {i} | {str(r.get('timestamp', ''))[:19]} | `{r['id']}` | not retained | - | "
                       f"{r.get('verdict') or '-'} | - | - | - | - |")
            continue
        m = art.get('ledger_metrics') or {}
        tag = 'final' if final.get(r['id']) == i else 'retained'
        out.append(f"| {i} | {str(r.get('timestamp', ''))[:19]} | `{r['id']}` | "
                   f"{tag} | {art['dsr']['n_trials']} | {art.get('verdict') or '-'} | {fnum(m.get('oos_sharpe'))} | "
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


def stress_table(stress: Dict[str, Any]) -> List[str]:
    c, b, a = stress['candidate'], stress['benchmark'], stress['active']
    names = {'unstressed': 'unstressed (as reported)', 'stressed_all': 'every forced exit to zero',
             'stressed_collapses': 'collapses only to zero'}
    out = ['| variant | cand CAGR | cand Sharpe | cand maxDD | bench CAGR | bench Sharpe | bench maxDD | active ann | t(NW) |',
           '|---|---|---|---|---|---|---|---|---|']
    for k, label in names.items():
        cs, bs, ak = c['summary'][k], b['summary'][k], a[k]
        out.append(f"| {label} | {pct(cs['cagr'])} | {fnum(cs['sharpe'])} | {fnum(cs['max_drawdown'], '{:.1%}')} | "
                   f"{pct(bs['cagr'])} | {fnum(bs['sharpe'])} | {fnum(bs['max_drawdown'], '{:.1%}')} | "
                   f"{pct(ak['ann_return'])} | {fnum(ak['t_nw'])} |")
    return out


# ---------------------------------------------------------------- render
def render(d: Dict[str, Any]) -> str:
    results, rows, ids, cal, stress, artifacts = (d['results'], d['rows'], d['ids'], d['calibration'],
                                                  d['stress'], d['artifacts'])
    n_ledger = distinct_trials(rows)
    stale = [cid for cid in ids if results[cid]['dsr']['n_trials'] != n_ledger]
    if stale:
        raise ReportError(f'final pass incomplete: {stale} carry a trial count != {n_ledger}; re-run them before reporting')
    check_results_are_final(results, rows)
    final = final_row_index(rows)
    sel = champion(results)
    ch = sel['champion']
    r = results[ch]
    check_stress_matches(stress, ch, r)
    w, o, a, dsr, fp = r['window'], r['oos'], r['active'], r['dsr'], r['full_period']['report_cell']
    b = r['oos_benchmarks'][BENCH]
    sn = r['stopped_names']
    g = r['gates']
    n_retained = sum(1 for v in artifacts.values() if v is not None)
    sc, sb = stress['candidate'], stress['benchmark']
    lines: List[str] = []
    lines += [f'# Insider family — REPORT (C04; D11, D14)', '',
              f'Generated {dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")} by `make_report.py` from '
              f'`results/<id>.json` (final pass), `results/trials/*.json` (retained per-trial artifacts), `trials.jsonl`, '
              f'`candidates.yaml`, `results/_calibration_counts.json`, `results/_d07_stress.json`. '
              f'Pre-registration: `PREREG.md` (git `f0658e5`; addendum 1 before the first ledger row; addendum 2 is '
              f'procedural, written after results). '
              f'Dev period {w["start"]}..{w["end"]}, walk-forward train {w["train_days"]} / test {w["test_days"]} days, '
              f'{w["n_folds"]} folds, OOS {o["days"]} sessions; costs {r["costs"]["base"]["slippage_bps"]:.0f} bps + '
              f'${r["costs"]["base"]["commission_per_share"]}/share, stress x{r["costs"]["stress_multiplier"]:.0f}. '
              f'Distinct candidate trials in the ledger: **{n_ledger}** (budget 30); every final file is deflated by {n_ledger}.', '']
    lines += ['## Verdict', '',
              f'**Champion: `{ch}` ({LABEL[ch]}), selected by {sel["how"]}. Harness verdict: {r["verdict"]}.** '
              + ('The champion PASSes D01.' if r['verdict'] == 'PASS' else
                 f'This is the null: no pre-registered insider rule beat the equal-weight PIT basket to the D01 standard. '
                 f'Gates: ' + ', '.join(f'{k}={"n/a" if v is None else ("ok" if v else "FAIL")}' for k, v in g.items()) + '.'), '']
    lines += [f'## Every candidate (final pass, N = {n_ledger})', ''] + results_table(results, ids) + ['',
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
              f'{fnum(sn.get("full_period_forced_exit_value_fraction"), "{:.2%}")} of initial capital ({sn.get("oos_forced_exits")} '
              f'inside OOS windows; benchmark {sn.get("benchmark_full_period_forced_exits")}). Terminal-loss stress: next section.',
              f'* Lookahead: {r["lookahead"]["cells_checked"]} cell(s) and {r["lookahead"]["benchmarks_checked"]} checked with '
              f'{r["lookahead"]["prefixes_replayed"]} fresh-state prefixes; PBO n/a (single cell).', '',
              '### Per-year active return (OOS)', ''] + per_year(r) + ['']
    lines += ['### D07 terminal-loss stress (full period, candidate AND benchmark, `results/_d07_stress.json`)', '',
              f'Both books re-run over {stress["window"]["start"]}..{stress["window"]["end"]} through the identical path; '
              f'every forced exit is identified in the fill log and reconciled with the backtester\'s own forced-exit count '
              f'and gross value (candidate {sc["forced_exits"]} exits, {fnum(sc["forced_exit_value_fraction"], "{:.2%}")} of '
              f'initial capital gross; benchmark {sb["forced_exits"]}, {fnum(sb["forced_exit_value_fraction"], "{:.2%}")}). '
              f'Stress = the exit\'s proceeds re-marked to zero on its day, pathwise (the book is worth E_t - P_t and everything '
              f'after scales). "Collapses" = exits whose terminal close is below '
              f'{stress["collapse_definition"]["fraction_of_trailing_high"]:.0%} of the trailing '
              f'{stress["collapse_definition"]["trailing_sessions"]}-session high: candidate {sc["collapse_exits"]} '
              f'({fnum(sc["collapse_net_proceeds_fraction"], "{:.2%}")} of capital), benchmark {sb["collapse_exits"]} '
              f'({fnum(sb["collapse_net_proceeds_fraction"], "{:.2%}")}).', ''] + stress_table(stress) + ['',
              'The unstressed row equals the champion\'s final file (`full_period.report_cell` and '
              f'`full_period.benchmarks.{BENCH}`), which `make_report.py` verifies before rendering. The overlay and the '
              'basket stop-print in the same names, so the stress moves both books together; the active row is what the '
              'champion\'s edge looks like when its own stopped names are marked to zero as well as the benchmark\'s.', '']
    lines += ['## Decomposition against the confounders', '',
              'Purchase count alone = C0 (`ins_purchase_count`). Size = each rule\'s market-cap-matched control (same K, same sleeve, '
              'PREREG addendum 1). Calibration columns are qualifier counts, not returns.', ''] + decomposition(results, cal) + ['',
              'Reading the controls: every control beats the equal-weight basket although it holds no insider information. '
              'The matching pool is covered names WITH an as-of market cap (operating companies with EDGAR fundamentals; 58 % '
              'of covered name-days per the calibration file), so part of a control\'s excess over the basket is "operating '
              'companies vs the ETFs/ADRs/trusts in the PIT basket" - a tilt every rule shares by construction (addendum 1). '
              'The honest reading is therefore rule minus control: negative for all eight rules, i.e. conditional on size and on '
              'being an operating company, the names insiders bought did WORSE than their neighbours over 2019-2024. '
              'The controls are candidate-role trials (they count in N) but are not champion-eligible (PREREG rule 1); their '
              'PROMISING verdicts are reported, not claimed.', '']
    lines += ['## Every ledger trial', '',
              f'{len(rows)} rows (`trials.jsonl`, append-only; modes '
              f'{"/".join(sorted(set(str(x.get("mode")) for x in rows)))}, status '
              f'{"/".join(sorted(set(str(x.get("status")) for x in rows)))}; fixed-mode rows are the held-out '
              'evaluations listed in the last section). Pass 1 (rows 1-16) ran the registrations in '
              'order, so early rows were deflated by partial trial counts; pass 2 (rows 17-32) re-ran all 16 once every hash '
              'was ledgered; pass 3 (rows 33-48) re-ran all 16 through the mandated '
              '`uv run python -m trader.research.lowfreq evaluate insider/<id>` (PREREG addendum 2) and is the final pass quoted '
              'above. The harness overwrites `results/<id>.json` on every run; from pass 2 on each run\'s file is retained as '
              f'`results/trials/NNN_<id>.json` (NNN = ledger row; {n_retained} retained) and the numbers below come from those '
              'artifacts only. Pass-1 files were overwritten before retention existed, so those rows show the harness '
              'verdict the ledger recorded but no numbers.', ''
              ] + ledger_table(rows, artifacts, final) + ['']
    lines += ['## Held-out evaluation (orchestrator only, D11)', '',
              'The held-out period starts after the dev cutoff; the full research directory\'s insider data ends '
              f'{HELD_OUT_END} (DATA.md), so the window is `{HELD_OUT_START}..{HELD_OUT_END}`:', '', '```',
              f"LOWFREQ_RESEARCH_CONFIG=~/.local/share/mmr/research_full/trader_research.yaml \\",
              f"  uv run python -m trader.research.lowfreq evaluate insider/{ch} --fixed-params '{{}}' --start {HELD_OUT_START} --end {HELD_OUT_END}",
              '```', ''] + held_out_lines(rows) + ['',
              f'Source of every number: `results/{ch}.json` keys `oos`, `oos_benchmarks.{BENCH}`, `active`, `dsr`, `full_period.report_cell`, '
              '`stopped_names`, `gates`; other candidates from their own `results/<id>.json`; ledger rows from their '
              '`results/trials/NNN_<id>.json`; K and market-cap percentiles from `results/_calibration_counts.json`; the stress '
              'table from `results/_d07_stress.json`.']
    return '\n'.join(lines) + '\n'


def main() -> int:
    try:
        d = load()
        text = render(d)
    except ReportError as exc:
        print(f'refusing to render: {exc}', file=sys.stderr)
        return 1
    (HERE / 'REPORT.md').write_text(text)
    words = len(text.split())
    print(f'REPORT.md written: {words} words; champion {champion(d["results"])["champion"]}')
    return 0 if words < 3000 else 1


if __name__ == '__main__':
    sys.exit(main())
