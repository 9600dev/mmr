"""The mutation gate must fail closed, and must not false-alarm on renumbering.

The mutation score was a SHELL COMMENT for its whole life — nothing detected
drift, and the recorded numbers were stale within a day. This gate makes it a
machine-checked artifact like the ty baselines.

Two properties matter, and both are lessons paid for tonight:

  * FAIL CLOSED. The ty gate reported "OK" with exit 0 whenever `ty` had not
    actually run, because a broken toolchain parses to zero diagnostics and zero
    diagnostics looks like success. A missing baseline, absent mutation data, or
    a module that was generated but never exercised must all FAIL here.

  * Baseline the SCORE, not the survivor keys. Mutant identifiers renumber
    whenever a mutated function changes — check_leverage's two documented
    equivalents moved from 12/14 to 13/15 when the function grew a tri-state
    record, with no change in what survived. A key-based baseline would cry wolf
    on every edit until nobody read it.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent


def _mod():
    spec = importlib.util.spec_from_file_location(
        'mutation_score', REPO / 'scripts' / 'mutation_score.py')
    assert spec and spec.loader
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _write_baseline(tmp_path, modules):
    path = tmp_path / 'mutation_baseline.json'
    path.write_text(json.dumps({'modules': modules}))
    return path


@pytest.fixture
def gate(tmp_path, monkeypatch):
    m = _mod()
    monkeypatch.setattr(m, 'BASELINE', tmp_path / 'mutation_baseline.json')
    return m


class TestFailsClosed:
    def test_missing_baseline_fails(self, gate):
        assert gate.check_against_baseline({'a.py': {'killed': 10, 'survived': 0}}) == 1

    def test_baselined_module_absent_from_the_run_fails(self, gate, tmp_path):
        """A partial run must not satisfy the gate — `run_mutation.sh cores`
        exercises two modules and would otherwise 'pass' the whole kernel."""
        _write_baseline(tmp_path, {'kernel.py': {'killed': 90, 'survived': 10, 'score': 0.9}})
        assert gate.check_against_baseline({'other.py': {'killed': 5, 'survived': 0}}) == 1

    def test_baselined_module_generated_but_not_exercised_fails(self, gate, tmp_path):
        """0 killed + 0 survived means the mutants exist but never ran."""
        _write_baseline(tmp_path, {'kernel.py': {'killed': 90, 'survived': 10, 'score': 0.9}})
        assert gate.check_against_baseline({'kernel.py': {'killed': 0, 'survived': 0}}) == 1


class TestRegressionDetection:
    def test_score_drop_fails(self, gate, tmp_path):
        _write_baseline(tmp_path, {'kernel.py': {'killed': 90, 'survived': 10, 'score': 0.9}})
        # One mutant moved from killed to survived.
        assert gate.check_against_baseline({'kernel.py': {'killed': 89, 'survived': 11}}) == 1

    def test_equal_score_passes(self, gate, tmp_path):
        _write_baseline(tmp_path, {'kernel.py': {'killed': 90, 'survived': 10, 'score': 0.9}})
        assert gate.check_against_baseline({'kernel.py': {'killed': 90, 'survived': 10}}) == 0

    def test_improvement_passes(self, gate, tmp_path):
        _write_baseline(tmp_path, {'kernel.py': {'killed': 90, 'survived': 10, 'score': 0.9}})
        assert gate.check_against_baseline({'kernel.py': {'killed': 95, 'survived': 5}}) == 0

    def test_growing_a_module_with_well_tested_code_passes(self, gate, tmp_path):
        """Adding code adds mutants. The gate must judge the RATIO, not the raw
        survivor count — otherwise every feature that adds a branch looks like a
        regression and the gate gets re-baselined reflexively."""
        _write_baseline(tmp_path, {'kernel.py': {'killed': 90, 'survived': 10, 'score': 0.9}})
        # +60 mutants, 57 of them killed: more survivors (10 -> 13) but a better ratio.
        assert gate.check_against_baseline({'kernel.py': {'killed': 147, 'survived': 13}}) == 0

    def test_growing_a_module_with_untested_code_fails(self, gate, tmp_path):
        """The converse: added code that nothing covers drops the ratio."""
        _write_baseline(tmp_path, {'kernel.py': {'killed': 90, 'survived': 10, 'score': 0.9}})
        assert gate.check_against_baseline({'kernel.py': {'killed': 92, 'survived': 28}}) == 1


class TestRealBaselineIsPresentAndSane:
    def test_repo_baseline_exists_and_covers_the_kernel(self):
        """Guard the guard: an empty or missing baseline makes every check
        vacuous, which is exactly the failure mode being fixed."""
        m = _mod()
        assert m.BASELINE.exists(), 'no recorded mutation baseline in the repo'
        modules = json.loads(m.BASELINE.read_text())['modules']
        assert modules, 'baseline records no modules — every check would be vacuous'
        for expected in ('trader/trading/order_math.py',
                         'trader/trading/exit_class.py',
                         'trader/trading/risk_gate.py',
                         'trader/data/proposal_transitions.py'):
            assert expected in modules, f'{expected} missing from the mutation baseline'
        for name, entry in modules.items():
            assert 0.0 <= entry['score'] <= 1.0, f'{name} has a nonsense score'
