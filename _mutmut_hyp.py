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
