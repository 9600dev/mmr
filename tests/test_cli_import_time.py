"""Cold-start budget for one-shot `mmr` invocations.

Every `mmr ...` command is a fresh interpreter, so module-level imports on the
`import trader.mmr_cli` path are paid on EVERY call. On 2026-11-05 that was
~0.85-1.1s, almost all of it scipy.stats (~400ms, pulled in by
trader/common/helpers.py for two rarely-used distribution helpers) and
exchange_calendars (~200ms, needed only for annotations / data back-fill).
Those are now imported inside the functions that use them; this test keeps
them off the CLI import path. It checks sys.modules (deterministic) rather
than wall time (machine-dependent and flaky).
"""
import json
import os
import subprocess
import sys

import pytest

# Heavy modules that must NOT be imported by `import trader.mmr_cli`.
_FORBIDDEN = ('scipy', 'exchange_calendars', 'bs4', 'plotille', 'pypager')


def _modules_after_import(module: str) -> set[str]:
    code = (
        'import json, sys\n'
        f'import {module}\n'
        'print(json.dumps(sorted(sys.modules)))\n'
    )
    proc = subprocess.run(
        [sys.executable, '-c', code],
        capture_output=True, text=True, timeout=120, env=dict(os.environ),
    )
    assert proc.returncode == 0, proc.stderr
    # last stdout line is the JSON (logging setup may print before it)
    return set(json.loads(proc.stdout.strip().splitlines()[-1]))


@pytest.fixture(scope='module')
def cli_modules() -> set[str]:
    return _modules_after_import('trader.mmr_cli')


@pytest.mark.parametrize('heavy', _FORBIDDEN)
def test_mmr_cli_import_does_not_pull_heavy_module(cli_modules, heavy):
    leaked = sorted(m for m in cli_modules if m == heavy or m.startswith(heavy + '.'))
    assert not leaked, (
        f'`import trader.mmr_cli` imported {heavy} ({leaked[:5]}...). '
        f'Move the import inside the function that needs it — every one-shot '
        f'CLI call pays for module-level imports.'
    )


def test_lazy_helpers_still_work():
    """The lazily-imported helpers must still resolve their dependency."""
    from ib_async.contract import Contract
    from trader.common import helpers

    dist = helpers.get_truncated_normal(mean=5.0, sd=1.0, low=0.0, upp=10.0)
    assert 0.0 <= float(dist.rvs(random_state=1)) <= 10.0

    cal = helpers.get_exchange_calendar(Contract(exchange='NYSE'))
    assert cal.name == 'XNYS'


def test_strategy_runtime_calendar_lookup_still_resolves():
    from trader.strategy.strategy_runtime import StrategyRuntime

    class _Sec:
        primaryExchange = 'NASDAQ'
        exchange = 'SMART'

    import exchange_calendars

    cal = StrategyRuntime._try_get_exchange_calendar(_Sec())  # ty: ignore[invalid-argument-type]  # duck-typed stand-in
    assert cal is not None
    assert cal.name == exchange_calendars.get_calendar('NASDAQ').name
    assert StrategyRuntime._try_get_exchange_calendar(None) is None
