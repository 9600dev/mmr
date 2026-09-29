"""D02: registry refusal, ledger append-before-print including errors, trial count."""
from __future__ import annotations

import json

import pytest

from trader.research.lowfreq import ledger
from trader.research.lowfreq.evaluate import evaluate
from trader.research.lowfreq.registry import RegistryError, entry_sha256, expand_grid, load_family
from research.conftest import entry, write_family


def _family(root, extra=()):
    cands = [entry('bench_ew', 'benchmark', 'EtfEqualWeight', module='trader.research.lowfreq.reference_strategies'),
             entry('hold_spy', 'candidate', 'HoldOne', benchmarks=['bench_ew'])] + list(extra)
    return write_family(root, 'fam', cands)


def test_refuses_unregistered_id_and_still_ledgers(research_env):
    d = _family(research_env['root'])
    with pytest.raises(RegistryError, match='not registered'):
        evaluate('fam/nope')
    rows = ledger.read(d / 'trials.jsonl')
    assert len(rows) == 1 and rows[0]['status'] == 'refused' and rows[0]['entry_sha256'] is None
    assert not (d / 'results').exists()


def test_every_call_appends_one_line_including_errors(research_env):
    d = _family(research_env['root'], extra=[entry('broken', 'candidate', 'DoesNotExist', benchmarks=['bench_ew'])])
    evaluate('fam/hold_spy')
    with pytest.raises(Exception):
        evaluate('fam/broken')
    evaluate('fam/hold_spy')
    rows = ledger.read(d / 'trials.jsonl')
    assert [r['status'] for r in rows] == ['ok', 'error', 'ok']
    ok = rows[0]
    for k in ('entry_sha256', 'grid', 'start', 'end', 'metrics', 'verdict', 'git_sha', 'timestamp'):
        assert k in ok, k
    assert ok['verdict'] in ('PASS', 'PROMISING', 'FAIL')
    assert rows[1]['verdict'] is None and 'DoesNotExist' in rows[1]['error']
    assert rows[1]['entry_sha256'] == entry_sha256(load_family('fam').get('broken'))
    # the ledger line is written before the results JSON is read back by callers
    assert (d / 'results' / 'hold_spy.json').exists()


def test_benchmark_rows_are_excluded_from_trial_count(research_env):
    d = _family(research_env['root'], extra=[entry('broken', 'candidate', 'DoesNotExist', benchmarks=['bench_ew'])])
    evaluate('fam/bench_ew')
    r1 = evaluate('fam/hold_spy')
    assert r1['dsr']['n_trials'] == 1
    with pytest.raises(Exception):
        evaluate('fam/broken')             # an errored candidate is still a trial
    r2 = evaluate('fam/hold_spy')          # same hash: not a new trial
    assert r2['dsr']['n_trials'] == 2
    rows = ledger.read(d / 'trials.jsonl')
    assert ledger.trial_count(rows) == 2
    assert sum(1 for r in rows if r['role'] == 'benchmark') == 1


def test_entry_hash_is_order_independent_but_content_sensitive():
    a = {'id': 'x', 'params': {'A': [1, 2]}, 'role': 'candidate'}
    b = {'role': 'candidate', 'params': {'A': [1, 2]}, 'id': 'x'}
    assert entry_sha256(a) == entry_sha256(b)
    assert entry_sha256(dict(a, params={'A': [1, 3]})) != entry_sha256(a)


def test_registry_validation(research_env):
    root = research_env['root']
    write_family(root, 'bad1', [entry('c', 'candidate', 'HoldOne', benchmarks=['missing'])])
    with pytest.raises(RegistryError, match='benchmark'):
        load_family('bad1')
    write_family(root, 'bad2', [entry('c', 'weird', 'HoldOne')])
    with pytest.raises(RegistryError, match='role'):
        load_family('bad2')
    write_family(root, 'bad3', [entry('c', 'candidate', 'HoldOne', params={'X': 5})])
    with pytest.raises(RegistryError, match='params'):
        load_family('bad3')
    assert expand_grid({'params': {'B': [1, 2], 'A': [3]}}) == [{'A': 3, 'B': 1}, {'A': 3, 'B': 2}]
    assert expand_grid({'params': {}}) == [{}]


def test_ledger_file_is_valid_jsonl(research_env):
    d = _family(research_env['root'])
    evaluate('fam/hold_spy')
    for line in (d / 'trials.jsonl').read_text().splitlines():
        json.loads(line)


# ---- round-2 hardening: one schema-complete row per call, on every path -------

def _rows(path):
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def test_every_row_is_schema_complete_on_ok_error_and_refused_paths(research_env):
    d = _family(research_env['root'], extra=[entry('broken', 'candidate', 'DoesNotExist', benchmarks=['bench_ew'])])
    evaluate('fam/hold_spy')
    with pytest.raises(Exception):
        evaluate('fam/broken')
    with pytest.raises(RegistryError):
        evaluate('fam/nope')
    rows = _rows(d / 'trials.jsonl')
    assert [r['status'] for r in rows] == ['ok', 'error', 'refused']
    for r in rows:
        assert set(r) == set(ledger.LEDGER_FIELDS), sorted(set(r) ^ set(ledger.LEDGER_FIELDS))
        assert set(r['metrics']) == set(ledger.METRIC_FIELDS)
        assert r['timestamp'] and r['harness_version'] == ledger.HARNESS_VERSION and r['elapsed_s'] is not None
    ok, err, ref = rows
    assert ok['verdict'] in ('PASS', 'PROMISING', 'FAIL') and ok['results_path'].endswith('hold_spy.json')
    assert ok['start'] == '2015-06-01' and ok['end'] == '2017-08-01' and ok['n_trials'] == 1
    assert ok['metrics']['oos_sharpe'] is not None and ok['strategy_sha256']
    assert err['verdict'] is None and 'DoesNotExist' in err['error'] and err['start'] == '2015-06-01'
    assert err['n_trials'] == 2 and err['entry_sha256'] and err['role'] == 'candidate'
    assert all(v is None for v in err['metrics'].values())
    assert ref['entry_sha256'] is None and ref['role'] is None and ref['n_trials'] == 2 and ref['verdict'] is None


def test_a_missing_or_malformed_family_is_refused_and_still_ledgered(research_env):
    root = research_env['root']
    with pytest.raises(RegistryError, match='no registry'):
        evaluate('ghost/anything')
    rows = _rows(root / 'ghost' / 'trials.jsonl')
    assert len(rows) == 1 and rows[0]['status'] == 'refused' and rows[0]['id'] == 'anything'
    assert set(rows[0]) == set(ledger.LEDGER_FIELDS)
    (root / 'mangled').mkdir()
    (root / 'mangled' / 'candidates.yaml').write_text('universe: etf\ncandidates: [not a mapping]\n')
    with pytest.raises(RegistryError):
        evaluate('mangled/x')
    assert _rows(root / 'mangled' / 'trials.jsonl')[-1]['status'] == 'refused'


def test_a_results_write_failure_is_recorded_as_error_not_ok(research_env):
    d = _family(research_env['root'])
    (d / 'results').write_text('a file where the results directory must be')   # mkdir will fail
    with pytest.raises(FileExistsError):
        evaluate('fam/hold_spy')
    rows = _rows(d / 'trials.jsonl')
    assert len(rows) == 1 and rows[0]['status'] == 'error' and 'FileExistsError' in rows[0]['error']
    assert rows[0]['verdict'] is None


def test_ok_row_is_written_after_the_results_file_and_matches_it(research_env):
    d = _family(research_env['root'])
    r = evaluate('fam/hold_spy')
    on_disk = json.loads((d / 'results' / 'hold_spy.json').read_text())
    row = _rows(d / 'trials.jsonl')[-1]
    assert on_disk['ledger_row'] == row == r['ledger_row']
    assert row['results_path'] == on_disk['results_path']


def test_compose_refuses_rows_outside_the_schema():
    with pytest.raises(ValueError, match='outside the schema'):
        ledger.compose({'status': 'ok', 'bogus': 1})
    with pytest.raises(ValueError, match='status'):
        ledger.compose({'family': 'f', 'status': 'done'})
