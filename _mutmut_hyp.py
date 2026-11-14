"""Pytest plugin (mutation-run only) — neutralize Hypothesis cross-session state.

Loaded ONLY by the mutation pass, via ``-p _mutmut_hyp`` wired into
``[tool.mutmut] pytest_add_cli_args`` in ``pyproject.toml`` (and copied into the
``mutants/`` sandbox via ``also_copy``). It is inert for the normal test suite:
it is not a conftest, does not match ``test_*.py`` collection, and is never
imported unless mutmut explicitly asks for it.

Why it exists: mutmut 3.x runs *two* in-process pytest sessions per invocation
(a stats/coverage pass, then the clean + per-mutant test runs). Class-based
``@given`` tests (e.g. ``TestOrderMathContract`` in ``tests/test_deal_contracts.py``)
then trip Hypothesis's ``differing_executors`` health check, because each pytest
session instantiates a fresh test-class ``self`` while the check's state lives on
the (process-persistent) decorated function object. That check guards against
flaky cross-executor example-DB replay — a concern that does not apply to a
mutation harness. We therefore, *for the mutation oracle only*:

  * disable the example database (``database=None``) so nothing persists across
    the two sessions, and
  * suppress exactly the health checks that mutmut's re-run model provokes
    (``differing_executors``) plus the performance checks + deadline, which
    would otherwise convert an incidentally-slow mutant run into a *false* kill
    (a real survivor must be a genuine assertion gap, not a timing artifact).

None of this weakens the tests themselves — the assertions and the invariants
are unchanged; only Hypothesis's harness-level flakiness guards are relaxed for
the duration of the mutation run.
"""

from hypothesis import HealthCheck, settings


settings.register_profile(
    "mutmut",
    database=None,
    deadline=None,
    suppress_health_check=[
        HealthCheck.differing_executors,
        HealthCheck.too_slow,
        HealthCheck.function_scoped_fixture,
        HealthCheck.data_too_large,
        HealthCheck.filter_too_much,
    ],
)
settings.load_profile("mutmut")


# --- pytest >= 9.2 compatibility for mutmut 3.6.0 ---------------------------
#
# mutmut's stats plugin reads the PRIVATE ``item._nodeid`` to map each mutated
# function to the tests that cover it. pytest 9.2 replaced that attribute with
# a structured ``_id`` behind the public ``nodeid`` property, so the stats pass
# died on its first test ("'Function' object has no attribute '_nodeid'") and
# the whole mutation gate reported every module as "not checked" — found the
# day the libraries were upgraded (2026-11-14). mutmut is pinned to exactly
# 3.6.0 because scripts/run_mutation.py patches its internals, so the alias
# lives here, in the one module that is only ever loaded for a mutation run.
import _pytest.nodes as _nodes

if not hasattr(_nodes.Node, "_nodeid"):
    _nodes.Node._nodeid = property(lambda self: self.nodeid)  # type: ignore[attr-defined]
