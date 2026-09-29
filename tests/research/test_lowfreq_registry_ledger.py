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
