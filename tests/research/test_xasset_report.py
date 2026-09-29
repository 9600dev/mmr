"""research/lowfreq/xasset/make_report.py: the PREREG champion rule, the modal
final-cell rule with its A1.3 tie-break, distinct-hash trial counting under
refresh rows, pass-1/pass-2 determinism comparison, immutable snapshots,
rendered values matching their JSON source (fold, per-year and dated-holdings
tables included) - and, at the artefact level, that the checked-in REPORT.md
contains exactly what the renderer produces from the CURRENT ledger, results
and diagnostics, names the rule's champion with its JSON verdict, and stays
under the 3000-word limit."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('xasset_make_report', REPO / 'research' / 'lowfreq' / 'xasset' / 'make_report.py')
assert SPEC is not None and SPEC.loader is not None
mr = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mr)


def result(cid, verdict, t, sharpe, chosen=None, dsr=0.5, n_trials=7):
    chosen = chosen or [{'A': 1}] * 3
    return {
        'id': cid, 'verdict': verdict, 'entry_sha256': 'e' * 64, 'strategy_sha256': 's' * 64,
        'oos': {'sharpe': sharpe, 'cagr': 0.05, 'max_drawdown': -0.2, 'days': 3015, 'ann_mean': 0.05},
        'oos_benchmarks': {'etf_ew': {'sharpe': 0.59, 'cagr': 0.059, 'max_drawdown': -0.255, 'days': 3015},
                           'etf_6040': {'sharpe': 0.95, 'cagr': 0.091, 'max_drawdown': -0.212, 'days': 3015}},
        'active': {'ann_return': 0.01, 'sharpe': 0.1, 't_nw': t, 'nw_lags': 8,
                   'per_year': {'2013': {'active_return': 0.02, 'counted': True}, '2024': {'active_return': -0.01, 'counted': False}},
                   'fraction_positive_years': 0.6, 'ann_return_3x_cost': 0.005, 'max_drawdown_benchmark': -0.212},
        'dsr': {'value': dsr, 'n_trials': n_trials, 'trial_sharpes': {}},
        'gate_inputs': {'benchmark_oos_sharpe_max': 0.95, 'benchmark_max_drawdown': -0.212},
        'gates': {'a_sharpe_vs_benchmark': True, 'b_active_t_ge_2': False, 'b_active_t_ge_1': True, 'c_dsr_ge_0_90': False,
                  'd_drawdown_within_5pp': True, 'e_positive_years_ge_60pct': True, 'f_active_positive_at_3x_cost': True,
                  'g_pbo_and_stability': None},
        'grid': {'cells': 3, 'registered_cells': 3, 'report_cell': {'A': 1}},
        'folds': [{'fold': i, 'chosen': c, 'oos': {}, 'benchmarks': {}, 'decisive': 'etf_6040'} for i, c in enumerate(chosen)],
        'decisive_per_fold': ['etf_6040'] * len(chosen), 'pbo': None, 'selection_stability': 1.0,
        'full_period': {'report_cell': {'sharpe': 0.7, 'cagr': 0.06, 'max_drawdown': -0.3, 'turnover_per_year': 0.5,
                                        'median_names_held': 10.0, 'min_cash_fraction': 0.0, 'scaled_buy_fills': 0,
                                        'forced_exits': 0}, 'benchmarks': {}, 'cells': {}},
        'stopped_names': {}, 'lookahead': {},
    }


# ------------------------------------------------------------------ champion rule
def test_champion_verdict_class_beats_t_stat():
    order = ['a', 'b']
    res = {'a': result('a', 'FAIL', t=5.0, sharpe=2.0), 'b': result('b', 'PROMISING', t=1.1, sharpe=0.5)}
    assert mr.champion(order, res)[0] == 'b'


def test_champion_within_class_higher_t_then_sharpe_then_registry_order():
    order = ['a', 'b', 'c', 'd']
    res = {'a': result('a', 'FAIL', t=0.5, sharpe=0.9), 'b': result('b', 'FAIL', t=1.5, sharpe=0.4),
           'c': result('c', 'FAIL', t=1.5, sharpe=0.7), 'd': result('d', 'FAIL', t=1.5, sharpe=0.7)}
    assert mr.champion(order, res)[0] == 'c'          # t tie with b -> Sharpe; tie with d -> registry order
    res['a']['active']['t_nw'] = None                 # an uncomputable t never wins
    assert mr.champion(order, res)[0] == 'c'
    assert mr.champion(order, {}) is None


def test_champion_skips_unevaluated_ids():
    order = ['zzz', 'a']
    res = {'a': result('a', 'FAIL', t=0.1, sharpe=0.1)}
    assert mr.champion(order, res)[0] == 'a'


# ------------------------------------------------------------------ final cell (A1.3)
def test_final_cell_modal():
    r = result('a', 'FAIL', 0.0, 0.0, chosen=[{'L': 63}, {'L': 252}, {'L': 252}, {'L': 126}])
    cell, counts = mr.final_cell(r)
    assert cell == {'L': 252} and counts[json.dumps({'L': 252}, sort_keys=True)] == 2


def test_final_cell_tie_goes_to_latest_fold_within_tied_set_not_last_fold():
    # {L:63} and {L:252} tie at 2; the LAST fold chose the non-modal {L:126}
    # (the round-1 bug picked it). The latest fold in the tied set is fold 3 -> {L:63}.
    r = result('a', 'FAIL', 0.0, 0.0, chosen=[{'L': 252}, {'L': 63}, {'L': 252}, {'L': 63}, {'L': 126}])
    cell, _ = mr.final_cell(r)
    assert cell == {'L': 63}


def test_final_cell_key_order_does_not_split_a_cell():
    r = result('a', 'FAIL', 0.0, 0.0, chosen=[{'A': 1, 'B': 2}, {'B': 2, 'A': 1}, {'A': 9, 'B': 9}])
    cell, counts = mr.final_cell(r)
    assert cell == {'A': 1, 'B': 2} and max(counts.values()) == 2


# ------------------------------------------------------------------ ledger + passes
def _row(cid, sha, status='ok', role='candidate', sharpe=0.3):
    return {'id': cid, 'role': role, 'status': status, 'entry_sha256': sha, 'verdict': 'FAIL', 'n_trials': 7,
            'timestamp': '2026-09-29T09:00:00+00:00',
            'metrics': {'oos_sharpe': sharpe, 'active_ann_return': 0.01, 'active_t_nw': 0.5, 'active_dsr': 0.4}}


def test_ledger_counts_distinct_hashes_and_numbers_passes():
    rows = [_row('etf_ew', 'b' * 64, role='benchmark'), _row('a', 'a' * 64), _row('b', 'c' * 64),
            _row('a', 'a' * 64), _row('b', 'c' * 64), _row('a', 'a' * 64, status='error')]
    assert mr.distinct_candidate_hashes(rows) == 2
    assert mr.pass_numbers(rows) == [None, 1, 1, 2, 2, None]
    table = mr.ledger_table(['a', 'b'], rows)
    assert 'distinct entry hashes): **2**' in table and '| aaaaaaaaaaaa | 2 | 7 |' in table   # (id, pass) -> snapshot file
    assert '5 candidate rows in total' in table


def test_compare_passes_ignores_dsr_but_flags_metric_drift():
    a = result('a', 'FAIL', 1.0, 0.5, dsr=0.2, n_trials=1)
    b = result('a', 'FAIL', 1.0, 0.5, dsr=0.9, n_trials=7)
    b['verdict'] = 'PASS'
    assert mr.compare_passes(a, b) == []
    b['oos']['sharpe'] = 0.6
    b['active']['t_nw'] = 1.1
    assert mr.compare_passes(a, b) == ['oos', 'active.t_nw']


def test_snapshot_is_immutable_and_check_reads_it(tmp_path):
    here = tmp_path
    (here / 'results').mkdir()
    (here / 'candidates.yaml').write_text(
        'family: xasset\nuniverse: etf\ncandidates:\n  - {id: a, role: candidate}\n  - {id: e, role: benchmark}\n')
    (here / 'trials.jsonl').write_text(json.dumps(_row('a', 'a' * 64)) + '\n')
    r1 = result('a', 'FAIL', 1.0, 0.5, dsr=0.2, n_trials=1)
    (here / 'results' / 'a.json').write_text(json.dumps(r1))
    paths = mr.snapshot('pass1', here)
    assert [p.name for p in paths] == ['a__pass1.json']
    with pytest.raises(FileExistsError):
        mr.snapshot('pass1', here)
    r2 = dict(r1, dsr={'value': 0.95, 'n_trials': 7, 'trial_sharpes': {}}, verdict='PROMISING')
    (here / 'results' / 'a.json').write_text(json.dumps(r2))
    mr.snapshot('pass2', here)
    table = mr.check_passes(here)
    assert '| a | 0.20 (1) | 0.95 (7) | FAIL | PROMISING | none |' in table


# ------------------------------------------------------------------ rendered == JSON
def test_rendered_numbers_match_json_source():
    r = result('a', 'PROMISING', t=1.234, sharpe=1.0567, chosen=[{'A': 1}, {'A': 2}, {'A': 2}])
    r['oos']['cagr'] = 0.0912
    table = mr.candidate_table(['a'], {'a': r})
    assert '| +1.057 |' in table and '| +9.12% |' in table and '| 1.23 |' in table and '**PROMISING**' in table
    section = mr.champion_section('a', r)
    assert '| a | +1.057 | +9.12% | -20.0% | 3015 |' in section
    assert '| 2013 | +2.00% | yes |' in section and '| 2024 | -1.00% | no (<60 days)' in section
    assert 'Final parameter cell' in section and '`{"A": 2}`' in section
    assert 'DSR 0.50 with N = 7 trials' in section
    assert 'Gates (D01): a_sharpe_vs_benchmark ok; b_active_t_ge_2 FAIL; b_active_t_ge_1 ok;' in section
    assert 'g_pbo_and_stability n/a.' in section
    assert '| a | **PROMISING** | 3 | +1.057 | +9.12% | -20.0% | -21.2% | +1.00% | 1.23 | 0.50 (7) | +0.50% | 60% | n/a | 1.00 | bc |' in table


# ------------------------------------------------------------------ fold / per-year / holdings tables
WINDOWS = [('2012-06-30', '2013-06-30'), ('2013-06-30', '2014-06-30'), ('2014-06-30', '2015-06-30')]


def _fold(k, cand, b6040, bew, chosen=None):
    start, end = WINDOWS[k]
    return {'fold': k, 'test_start': start, 'test_end': end, 'chosen': chosen or {'A': 1},
            'oos': {'sharpe': cand, 'cagr': 0.0}, 'decisive': 'etf_6040' if b6040 >= bew else 'etf_ew',
            'benchmarks': {'etf_6040': {'sharpe': b6040}, 'etf_ew': {'sharpe': bew}}}


def test_fold_table_marks_wins_only_beyond_visible_margin():
    a, b = result('a', 'FAIL', 0.0, 0.0), result('b', 'FAIL', 0.0, 0.0)
    a['folds'] = [_fold(0, 1.0, 0.5, 0.2), _fold(1, 0.504, 0.5, 0.9), _fold(2, 0.553, 0.5, 0.55)]
    b['folds'] = [_fold(0, 0.1, 0.5, 0.2), _fold(1, 1.0, 0.5, 0.9), _fold(2, 0.2, 0.5, 0.55)]
    table = mr.fold_table(['a', 'b'], {'a': a, 'b': b})
    assert '| series | 0:12-13 | 1:13-14 | 2:14-15 | >60/40 | >both |' in table
    assert '| 60/40 | +0.50 | +0.50 | +0.50 | - | - |' in table
    assert '| EW | +0.20 | +0.90 | +0.55 | - | - |' in table
    # fold 1: +0.004 above 60/40 is a tie (below the table's precision); fold 2: +0.053 above 60/40
    # earns `*` but +0.003 above EW does not earn `!`
    assert '| a | +1.00*! | +0.50 | +0.55* | 2/3 | 1/3 |' in table
    assert '| b | +0.10 | +1.00*! | +0.20 | 1/3 | 1/3 |' in table
    assert mr.fold_table(['a'], {}) == 'No candidate evaluated yet.'


def test_fold_table_refuses_inconsistent_benchmark_folds():
    a, b = result('a', 'FAIL', 0.0, 0.0), result('b', 'FAIL', 0.0, 0.0)
    a['folds'] = [_fold(0, 1.0, 0.5, 0.2)]
    b['folds'] = [_fold(0, 1.0, 0.7, 0.2)]                    # same window, different benchmark Sharpe
    with pytest.raises(ValueError):
        mr.fold_table(['a', 'b'], {'a': a, 'b': b})
    b['folds'] = [dict(_fold(0, 1.0, 0.5, 0.2), test_end='2013-07-30')]   # different window
    with pytest.raises(ValueError):
        mr.fold_table(['a', 'b'], {'a': a, 'b': b})


def test_positive_years_table_counts_strictly_positive_like_gate_e():
    a, b = result('a', 'FAIL', 0.0, 0.0), result('b', 'FAIL', 0.0, 0.0)
    a['active']['per_year'] = {'2013': {'active_return': 0.021, 'counted': True}, '2014': {'active_return': 0.0, 'counted': True}}
    b['active']['per_year'] = {'2013': {'active_return': -0.05, 'counted': True}, '2014': {'active_return': 1e-6, 'counted': True}}
    table = mr.positive_years_table(['a', 'b'], {'a': a, 'b': b})
    assert '| year | a | b | positive |' in table
    assert '| 2013 | +2.1% | -5.0% | 1/2 |' in table
    assert '| 2014 | +0.0% | +0.0% | 1/2 |' in table      # exactly 0 is not positive; 1e-6 is (the harness's gate (e) rule)


def test_holdings_table_renders_from_json_with_picks_and_cap(tmp_path):
    (tmp_path / 'results').mkdir()
    data = {'top': 3, 'candidates': {'xa_x': {'folds': [
        {'fold': 0, 'test_start': '2012-06-30', 'test_end': '2013-06-30', 'chosen': {'B': 2, 'A': 1},
         'top_holdings': [{'ticker': 'XLE', 'mean_weight': 0.18}, {'ticker': 'USO', 'mean_weight': 0.13},
                          {'ticker': 'BIL', 'mean_weight': 0.05}]},
        {'fold': 1, 'test_start': '2013-06-30', 'test_end': '2014-06-30', 'chosen': {'A': 1, 'B': 2},
         'top_holdings': [{'ticker': 'SPY', 'mean_weight': 0.6}]}]}}}
    (tmp_path / 'results' / 'holdings_diagnostics.json').write_text(json.dumps(data))
    full = mr.holdings_table(tmp_path)
    assert '| xa_x | 0 | 2012-06-30..2013-06-30 | `{"A": 1, "B": 2}` | XLE 18%, USO 13%, BIL 5% |' in full
    assert '| xa_x | 1 | 2013-06-30..2014-06-30 | `{"A": 1, "B": 2}` | SPY 60% |' in full
    picked = mr.holdings_table(tmp_path, picks=[('xa_x', 0)], max_names=2)
    assert '| xa_x | 1 |' not in picked
    assert '| XLE 18%, USO 13% +1 more |' in picked


# ------------------------------------------------------------------ the checked-in artefacts
XASSET = REPO / 'research' / 'lowfreq' / 'xasset'


def _is_reproduction(row, earlier, results):
    """A ledger row that re-runs an already-reported candidate entry unchanged (same id and
    entry hash as an earlier ok row). A fixed-cell window carries its own verdict; a
    walk-forward re-run must reproduce the verdict recorded in results/<id>.json."""
    if row.get('status') != 'ok' or row.get('role') != 'candidate':
        return False
    if not any(e.get('status') == 'ok' and e.get('id') == row.get('id')
               and e.get('entry_sha256') == row.get('entry_sha256') for e in earlier):
        return False
    if row.get('mode') == 'fixed':
        return True
    return row.get('id') in results and results[row['id']].get('verdict') == row.get('verdict')


def test_check_passes_accepts_a_reproduction_rewrite_but_flags_drift(tmp_path):
    """Integration: re-running `evaluate xasset/<id>` after the passes rewrites results/<id>.json
    with new run provenance; check_passes must still call it the pass-2 result, and must still
    flag a changed verdict or deterministic field."""
    here = tmp_path
    (here / 'results').mkdir()
    (here / 'candidates.yaml').write_text(
        'family: xasset\nuniverse: etf\ncandidates:\n  - {id: a, role: candidate}\n  - {id: e, role: benchmark}\n')
    (here / 'trials.jsonl').write_text(json.dumps(_row('a', 'a' * 64)) + '\n')
    r = result('a', 'FAIL', 1.0, 0.5, dsr=0.2, n_trials=7)
    (here / 'results' / 'a.json').write_text(json.dumps(r))
    mr.snapshot('pass1', here)
    mr.snapshot('pass2', here)
    (here / 'results' / 'a.json').write_text(json.dumps(dict(r, elapsed_s=99.0, ledger_row={'timestamp': 'later', 'git_sha': 'x'})))
    assert '| a | 0.20 (7) | 0.20 (7) | FAIL | FAIL | none |' in mr.check_passes(here)
    (here / 'results' / 'a.json').write_text(json.dumps(dict(r, verdict='PROMISING')))
    assert '(results/<id>.json != pass2 snapshot!)' in mr.check_passes(here)
    (here / 'results' / 'a.json').write_text(json.dumps(result('a', 'FAIL', 1.0, 0.6, dsr=0.2, n_trials=7)))
    assert '(results/<id>.json != pass2 snapshot!)' in mr.check_passes(here)


def test_report_tables_match_current_artifacts():
    """REPORT.md must contain, verbatim, every line the renderer produces from
    the CURRENT ledger, results JSONs and holdings diagnostic; its champion
    heading must name the rule's champion with the verdict of that champion's
    results JSON; and it must stay under the 3000-word limit."""
    _, order, rows, results = mr.load(XASSET)
    report = (XASSET / 'REPORT.md').read_text()
    assert len(order) >= 4 and set(order) <= set(results)           # >= 4 distinct hypotheses, all evaluated
    assert 4 <= mr.distinct_candidate_hashes(rows) <= 40             # D04 trial budget
    for table in (mr.candidate_table(order, results), mr.fold_table(order, results)):
        for line in table.splitlines():
            assert (not line.strip()) or line in report, line
    # Ledger table: every row the report was written from appears verbatim, as a prefix. Rows
    # appended AFTER the report - the integration/held-out reproductions (`evaluate xasset/<id>`
    # re-runs and `--fixed-params` runs of an already-reported entry) - need not, but each must
    # repeat an earlier ok row's (id, entry hash), never a new or changed trial, and a
    # walk-forward re-run must reproduce the verdict of results/<id>.json.
    ledger_lines = mr.ledger_table(order, rows).splitlines()
    header, body = ledger_lines[:2], ledger_lines[2:2 + len(rows)]
    assert all(line in report for line in header)
    documented = 0
    for k, (line, row) in enumerate(zip(body, rows)):
        if line in report:
            assert documented == k, f'row {k + 1} is in the report but an earlier row is not: {line}'
            documented += 1
        else:
            assert _is_reproduction(row, rows[:k], results), line
    assert documented >= 1
    assert f'Candidate-role trial count (distinct entry hashes): **{mr.distinct_candidate_hashes(rows)}**' in report
    cid, r = mr.champion(order, results)
    assert f"## Champion: `{cid}` — verdict **{r['verdict']}**" in report
    for line in mr.champion_section(cid, r).splitlines()[1:]:
        assert (not line.strip()) or line in report, line
    assert json.load(open(XASSET / 'results' / f'{cid}.json'))['verdict'] == r['verdict']
    assert f"verdict **{r['verdict']}** in\n`results/{cid}.json`" in report or \
        f"verdict **{r['verdict']}** in `results/{cid}.json`" in report
    # every dated-holdings row quoted in the report is a row of the diagnostic (at some display cap)
    data = json.load(open(XASSET / 'results' / 'holdings_diagnostics.json'))
    quoted = [l for l in report.splitlines() if l.startswith('| xa_') and '`{' in l]
    assert quoted, 'the report quotes no dated-holdings row'
    for line in quoted:
        cells = [c.strip() for c in line.strip().strip('|').split('|')]
        cid_q, fold_q = cells[0], int(cells[1])
        assert any(line in mr.holdings_table(XASSET, picks=[(cid_q, fold_q)], max_names=m)
                   for m in [None] + list(range(1, int(data['top']) + 1))), line
    assert len(report.split()) < 3000


def test_pass_snapshots_agree_and_final_json_is_pass2():
    """PREREG A1.2 as executed: for every candidate no deterministic field
    differs between the pass-1 and pass-2 snapshots, results/<id>.json IS the
    pass-2 snapshot, and its DSR is deflated by the FINAL trial count."""
    _, order, rows, results = mr.load(XASSET)
    table = mr.check_passes(XASSET)
    for cid in order:
        row = next(l for l in table.splitlines() if l.startswith(f'| {cid} |'))
        assert row.endswith('| none |'), row
    n = mr.distinct_candidate_hashes(rows)
    for cid in order:
        assert results[cid]['dsr']['n_trials'] == n
        assert results[cid]['role'] == 'candidate' and results[cid]['family'] == 'xasset'
