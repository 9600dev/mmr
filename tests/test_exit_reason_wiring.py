"""Every exit exemption must name the rule that justifies it.

``is_exit=True`` on an ApprovedOrder skips the chokepoint's gate-record
requirement, so it is the one field whose being wrong puts an ungated order on
the wire. It cannot be verified wholesale (PROTECTIVE_CHILD legs are exit-class
by construction and have no position yet), so the compensating control is
attribution: every mint site states an ``ExitReason``.

That is only a control if it is enforced mechanically — a convention nobody
checks is how the bare bool got trusted for eight call sites in the first place.
This walks the AST rather than grepping, so a reformatted or multi-line call
cannot slip past.
"""

from __future__ import annotations

import ast
import pathlib

REPO = pathlib.Path(__file__).resolve().parent.parent


def _mint_calls() -> list[tuple[str, int, set[str], ast.Call]]:
    """(relpath, lineno, kwarg names, node) for every mint_approved_order call."""
    calls: list[tuple[str, int, set[str], ast.Call]] = []
    for path in sorted((REPO / 'trader').rglob('*.py')):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, 'id', None)
            if name == 'mint_approved_order':
                kwargs = {kw.arg for kw in node.keywords if kw.arg}
                calls.append((str(path.relative_to(REPO)), node.lineno, kwargs, node))
    return calls


def _is_literal_false(node: ast.Call, arg: str) -> bool:
    for kw in node.keywords:
        if kw.arg == arg:
            return isinstance(kw.value, ast.Constant) and kw.value.value is False
    return False


def test_mint_sites_exist_and_are_found():
    """Guard the guard: if the AST walk finds nothing, every test below passes
    vacuously — the same fail-open shape as the old ty gate."""
    assert len(_mint_calls()) >= 6, (
        f'expected the known mint sites, found {len(_mint_calls())} — has the '
        'function been renamed? These tests would silently pass on zero.'
    )


def test_every_mint_site_states_an_exit_reason():
    """A site whose is_exit can be True at runtime must name the justifying rule.

    Exempt: a site passing the literal is_exit=False, where a reason is
    meaningless. Everything else — True, or a variable that could be True — must
    carry exit_reason.
    """
    missing = [
        f'{rel}:{line}'
        for rel, line, kwargs, node in _mint_calls()
        if 'exit_reason' not in kwargs and not _is_literal_false(node, 'is_exit')
    ]
    assert not missing, (
        'these mint sites can claim exit-class without stating an ExitReason, so '
        'they would skip the chokepoint gate-record check unattributed: '
        f'{missing}. Pass exit_reason=ExitReason.<rule>.'
    )


def test_no_mint_site_passes_a_bare_positional_is_exit():
    """is_exit is keyword-only in the signature; assert callers keep it that way
    so the AST check above cannot be evaded with positional args."""
    positional = [
        f'{rel}:{line}'
        for rel, line, _kwargs, node in _mint_calls()
        if len(node.args) > 2
    ]
    assert not positional, f'mint called with extra positional args: {positional}'
