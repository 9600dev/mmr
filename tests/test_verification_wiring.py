"""The verification toolchain must not silently lose coverage.

Every layer here is configured by a hand-maintained list, each carrying a
"keep in sync" comment and nothing to enforce it. When such a list drifts the
failure is silent and flattering: the tool still runs, still passes, and simply
stops looking at something. These tests make the drift loud instead.

Concretely they would have caught:
  * a new ``@deal``-contracted function never added to crosshair's TARGETS
    (contract written, never symbolically checked);
  * the contracted safety kernel drifting out of mutmut's ``only_mutate``
    (mutation score still printed, that module no longer measured).
"""

from __future__ import annotations

import ast
import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parent.parent


def _deal_contracted_functions() -> set[str]:
    """Dotted names of every function carrying a ``@deal.*`` decorator."""
    found: set[str] = set()
    for path in sorted((REPO / 'trader').rglob('*.py')):
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:  # pragma: no cover
            continue
        module = str(path.relative_to(REPO)).removesuffix('.py').replace('/', '.')
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for dec in node.decorator_list:
                expr = dec.func if isinstance(dec, ast.Call) else dec
                root = expr
                while isinstance(root, ast.Attribute):
                    root = root.value
                if isinstance(root, ast.Name) and root.id == 'deal':
                    found.add(f'{module}.{node.name}')
                    break
    return found


def _crosshair_targets() -> set[str]:
    src = (REPO / 'scripts' / 'crosshair_check.py').read_text()
    block = re.search(r'TARGETS\s*=\s*\[(.*?)\]', src, re.S)
    assert block, 'could not locate TARGETS in scripts/crosshair_check.py'
    return set(re.findall(r'"([\w.]+)"', block.group(1)))


def _mutation_scope() -> set[str]:
    src = (REPO / 'pyproject.toml').read_text()
    block = re.search(r'only_mutate\s*=\s*\[(.*?)\]', src, re.S)
    assert block, 'could not locate only_mutate in pyproject.toml'
    return set(re.findall(r'"([\w/.]+\.py)"', block.group(1)))


def _mutation_oracle() -> set[str]:
    src = (REPO / 'pyproject.toml').read_text()
    block = re.search(r'pytest_add_cli_args_test_selection\s*=\s*\[(.*?)\]', src, re.S)
    assert block, 'could not locate the mutation oracle selection in pyproject.toml'
    return set(re.findall(r'"([\w/.]+\.py)"', block.group(1)))


# Spec files deliberately absent from the mutation oracle, with the reason. Any
# OTHER omission is drift and fails the test below.
_ORACLE_EXCLUSIONS = {
    # Shells out to `ty`/uv to assert the type gate rejects gate-skipping calls:
    # slow, environment-dependent, and unrelated to the mutated pure kernels.
    'tests/invariants/test_approved_order.py',
    # Static assertions about the strategy manifest, not kernel behaviour.
    'tests/invariants/test_manifest.py',
}


# Contracted functions CrossHair cannot analyse, with the reason. The contract
# still runs at runtime and is still pinned by Hypothesis — what is excluded is
# only the SYMBOLIC pass. Any other omission is drift and fails the test below.
#
# The bar for entry here is "CrossHair would report success without having
# checked anything", which is worse than an honest gap: it executes symbolically
# and cannot see through numpy's C implementations, so a function whose entire
# body is array work yields a vacuous green tick.
_CROSSHAIR_EXCLUSIONS = {
    # Takes an ndarray of returns and an arbitrary-length trial vector; every
    # branch runs through numpy reductions and scipy's compiled norm.cdf.
    'trader.simulation.selection_bias.deflated_sharpe',
    # Enumerates C(S, S/2) numpy slicings — symbolically intractable, and the
    # properties that matter (noise -> 0.5, real edge -> low) are statistical
    # rather than per-input, so they are pinned by generated data instead.
    'trader.simulation.selection_bias.pbo_cscv',
    # Tried and rejected by the tool, not by us: CrossHair reaches
    # scipy.stats.norm.ppf and dies inside the compiled ndtri ufunc with
    # "not supported for the input types ... calling expected_max_sharpe(2, 0.5)".
    # That input is fine concretely (it returns 0.367, postcondition holds) — the
    # failure is symbolic values meeting C code, which no rewrite of this
    # function fixes short of reimplementing the inverse normal CDF.
    'trader.simulation.selection_bias.expected_max_sharpe',
}


class TestCrossHairTargetsInSync:
    def test_every_contracted_function_is_symbolically_checked(self):
        """A ``@deal`` contract that CrossHair never runs is decoration."""
        missing = (_deal_contracted_functions() - _crosshair_targets()
                   - _CROSSHAIR_EXCLUSIONS)
        assert not missing, (
            'these @deal-contracted functions are absent from crosshair TARGETS '
            '(add them to scripts/crosshair_check.py, or to _CROSSHAIR_EXCLUSIONS '
            f'here with a reason): {sorted(missing)}'
        )

    def test_no_stale_targets(self):
        """A renamed/removed target makes CrossHair error rather than check —
        it fails closed, but the list should still be truthful."""
        stale = _crosshair_targets() - _deal_contracted_functions()
        assert not stale, (
            'these crosshair TARGETS are no longer @deal-contracted functions: '
            f'{sorted(stale)}'
        )


class TestMutationScopeCoversTheContractedKernel:
    def test_every_contracted_module_is_mutated(self):
        """The contracts are the spec; mutation is what proves the TESTS enforce
        them. A contracted module outside only_mutate is unmeasured."""
        contracted_modules = {
            name.rsplit('.', 1)[0].replace('.', '/') + '.py'
            for name in _deal_contracted_functions()
        }
        missing = contracted_modules - _mutation_scope()
        assert not missing, (
            'these modules hold @deal contracts but are outside mutmut only_mutate '
            f'in pyproject.toml: {sorted(missing)}'
        )

    def test_exit_class_decision_is_in_scope(self):
        """Pinned explicitly: this is the predicate whose absence from the
        mutation scope let a size-triggered backdoor survive the entire suite."""
        assert 'trader/trading/exit_class.py' in _mutation_scope()


class TestMutationOracleIncludesTheSpec:
    """A spec file absent from the oracle does not constrain any mutant.

    This is not hypothetical: adding tests/invariants/test_exit_class.py left the
    exit_class mutation score unchanged at 61.3%, because mutmut only runs the
    hand-listed oracle. Adding the file to `pytest_add_cli_args_test_selection`
    took it to 93.5%. The file existed, passed, and measured nothing.
    """

    def test_every_invariants_file_is_in_the_oracle_or_documented_as_excluded(self):
        spec_files = {
            str(p.relative_to(REPO))
            for p in (REPO / 'tests' / 'invariants').glob('test_*.py')
        }
        missing = spec_files - _mutation_oracle() - _ORACLE_EXCLUSIONS
        assert not missing, (
            'these invariants files are not in the mutmut oracle selection, so no '
            'mutant is constrained by them — add them to '
            'pytest_add_cli_args_test_selection in pyproject.toml, or to '
            f'_ORACLE_EXCLUSIONS here with a reason: {sorted(missing)}'
        )

    def test_oracle_has_no_dangling_entries(self):
        missing = {f for f in _mutation_oracle() if not (REPO / f).exists()}
        assert not missing, f'oracle references nonexistent test files: {sorted(missing)}'


def _ty_scopes() -> dict[str, list[str]]:
    """SCOPES read from the real gate module, not re-parsed by hand."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'ty_gate', REPO / 'scripts' / 'ty_gate.py')
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return {name: list(scope['dirs']) for name, scope in mod.SCOPES.items()}


# Modules that place orders, run strategies, or front the trading API. Every one
# must sit in SOME ty scope. Listed as directories so a NEW file appearing in one
# fails this test until someone assigns it a scope — the alternative is a module
# quietly joining the trading path with zero type coverage, which is exactly how
# auto_executor.py (1052 lines, places the live orders), strategy_runtime.py and
# sdk.py sat unchecked until 2026-07-24.
_MUST_BE_TYPE_CHECKED = ['trader/trading', 'trader/strategy']


class TestTradingPathIsTypeChecked:
    def test_every_trading_path_module_is_in_a_ty_scope(self):
        covered: list[str] = [d for dirs in _ty_scopes().values() for d in dirs]

        def is_covered(rel: str) -> bool:
            return any(rel == c or rel.startswith(c.rstrip('/') + '/') for c in covered)

        uncovered = sorted(
            str(p.relative_to(REPO))
            for root in _MUST_BE_TYPE_CHECKED
            for p in (REPO / root).rglob('*.py')
            if not p.name.startswith('__')
            and not is_covered(str(p.relative_to(REPO)))
        )
        assert not uncovered, (
            'these trading-path modules are in NO ty scope — add them to SCOPES in '
            f'scripts/ty_gate.py (kernel if clean, advisory otherwise): {uncovered}'
        )

    def test_auto_executor_is_in_the_kernel_scope_at_zero(self):
        """It places the live orders and is type-clean; it must be held at zero,
        not baselined, so a new diagnostic there fails the gate."""
        assert 'trader/strategy/auto_executor.py' in _ty_scopes()['kernel']
