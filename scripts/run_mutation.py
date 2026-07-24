#!/usr/bin/env python3
"""Driver for the kernel mutation pass — mutmut + a deal-aware un-skip patch.

WHY THIS EXISTS
    mutmut 3.x refuses to mutate *any* decorated function (to avoid breaking its
    trampoline on decorators with definition-time side effects, e.g. @property or
    @app.post). But the MMR safety kernel's most critical functions —
    ``whole_shares_for_notional``, ``_floor_shares_for_notional``,
    ``is_known_status``, ``is_valid_transition``, ``_confidence_scale``,
    ``_volatility_multiplier``, ``compute_atr`` — are all ``@deal.*``-contracted.
    Unpatched, mutmut generates ZERO mutants for them, silently reporting a
    perfect score for functions it never actually tested.

WHAT THE PATCH DOES
    It un-skips a function decorated *solely* by ``deal.*`` so mutmut mutates its
    BODY, while still skipping the ``deal.*`` decorator nodes themselves (so the
    contract lambdas in ``@deal.ensure(lambda _: ...)`` are never mutated — those
    execute at definition time and are the spec, not the code under test). Any
    other decorator kind (@property, mixed) keeps mutmut's default skip, since
    those genuinely break the trampoline. mutmut's trampoline copies the
    (unmutated) ``deal`` decorators onto every generated variant, so the runtime
    contract stays live per-mutant and helps kill contract-violating mutants —
    exactly the intended defense-in-depth.

USAGE (always via the canonical env interpreter, which has deal/duckdb/hypothesis
+ mutmut; mutmut 3.x runs pytest in-process so this IS the test interpreter):

    ~/miniforge3/envs/mmr/bin/python3 scripts/run_mutation.py run                       # all 4 kernel files
    ~/miniforge3/envs/mmr/bin/python3 scripts/run_mutation.py run 'trader.trading.order_math.*'
    ~/miniforge3/envs/mmr/bin/python3 scripts/run_mutation.py results

Config (scope + oracle test selection) lives in ``[tool.mutmut]`` in
pyproject.toml. See scripts/run_mutation.sh for the standard staged invocation
and scripts/mutation_score.py for the per-module readout.
"""
from __future__ import annotations

import sys

import libcst as cst
# Importing mutmut.__main__ runs its module-level set_start_method("fork"), so the
# generation Pool forks and inherits the monkeypatch applied below.
from mutmut import __main__ as mm  # noqa: F401
from mutmut.mutation import file_mutation as _fm


_ORIG_SKIP = _fm.MutationVisitor._skip_node_and_children


def _root_name(expr: cst.BaseExpression) -> str | None:
    """Leftmost dotted-name of a decorator expression: deal.ensure(...) -> 'deal'."""
    node: cst.CSTNode = expr
    while True:
        if isinstance(node, cst.Call):
            node = node.func
        elif isinstance(node, cst.Attribute):
            node = node.value
        elif isinstance(node, cst.Name):
            return node.value
        else:
            return None


def _all_deal(func: cst.FunctionDef) -> bool:
    return bool(func.decorators) and all(
        _root_name(d.decorator) == "deal" for d in func.decorators
    )


def _patched_skip(self, node: cst.CSTNode) -> bool:
    # Never mutate a deal.* contract expression (the lambda in @deal.ensure/@deal.pre
    # is the spec and runs at definition time — mutating it breaks import, not logic).
    if isinstance(node, cst.Decorator) and _root_name(node.decorator) == "deal":
        return True
    # Un-skip a purely deal-decorated function so its body is mutated.
    if isinstance(node, cst.FunctionDef) and _all_deal(node):
        if node.name.value in _fm.NEVER_MUTATE_FUNCTION_NAMES:
            return True
        return False
    return _ORIG_SKIP(self, node)


_fm.MutationVisitor._skip_node_and_children = _patched_skip


if __name__ == "__main__":
    mm.cli(args=sys.argv[1:])
