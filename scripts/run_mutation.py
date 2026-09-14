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
import logging
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import tempfile
import time
from functools import wraps

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


def _plain_exception_tracebacks() -> None:
    """Keep exception logs without highlighting the large generated sources.

    Rich tokenizes an entire source file to render a caught exception. Generated
    mutation modules can be tens of megabytes, making that presentation dominate
    a failing test. Only this driver's process (and its forked workers) changes:
    records, levels, exception details, and pytest's native traceback stay live.
    """
    from rich.logging import RichHandler

    original_init = RichHandler.__init__

    @wraps(original_init)
    def plain_init(self, *args, **kwargs):
        kwargs['rich_tracebacks'] = False
        original_init(self, *args, **kwargs)

    RichHandler.__init__ = plain_init
    # Cover handlers created by driver imports as well as later dictConfig
    # calls, which construct their handlers through the wrapper above.
    loggers = [logging.getLogger(), *logging.Logger.manager.loggerDict.values()]
    for logger in loggers:
        if isinstance(logger, logging.Logger):
            for handler in logger.handlers:
                if isinstance(handler, RichHandler):
                    handler.rich_tracebacks = False


class _MutationTempDirectories:
    """Reclaim a mutation worker's files only after the parent has reaped it.

    Mutmut exits workers with os._exit, so TemporaryDirectory finalizers and
    pytest's atexit lock cleanup never run. Isolate each worker before pytest
    starts and reclaim its namespace at mutmut's post-wait result hook. The
    coordinator directory is deliberately retained: baseline tests can leave
    daemon threads holding its journals until this entire driver exits.
    """

    _TEMP_ENV = ('TMPDIR', 'TMP', 'TEMP', 'PYTEST_DEBUG_TEMPROOT')
    _TERMINATE_GRACE_SECONDS = 2.0
    _KILL_GRACE_SECONDS = 2.0

    def __init__(self):
        self.owner_pid = os.getpid()
        self.root = None
        self._root_identity = None
        self._active_pid = None
        self._namespace = None
        self._children = set()

    def __enter__(self):
        self._previous_env = {name: os.environ.get(name) for name in self._TEMP_ENV}
        self._previous_tempdir = tempfile.tempdir
        self._execute_pytest = mm.PytestRunner.execute_pytest
        self._register_timeout = mm.register_timeout
        self._register_result = mm.SourceFileMutationData.register_result

        @wraps(self._execute_pytest)
        def execute_pytest(runner, params, **kwargs):
            self._activate_namespace()
            return self._execute_pytest(runner, params, **kwargs)

        @wraps(self._register_timeout)
        def register_timeout(pid, timeout_s):
            # This is mutmut's first parent callback after the mutation fork.
            # Record ownership before timeout registration itself can fail.
            if os.getpid() != self.owner_pid or pid <= 0 or pid == self.owner_pid:
                raise RuntimeError('invalid mutation worker ownership')
            self._children.add(pid)
            return self._register_timeout(pid=pid, timeout_s=timeout_s)

        @wraps(self._register_result)
        def register_result(data, *, pid, exit_code):
            if os.getpid() != self.owner_pid or pid not in self._children:
                raise RuntimeError('result for an untracked mutation worker')
            # Installed mutmut calls this only after os.wait has reaped pid.
            # Remove it before save(), which can raise (including ENOSPC).
            self._children.remove(pid)
            try:
                return self._register_result(data, pid=pid, exit_code=exit_code)
            finally:
                original_failure = sys.exc_info()[0] is not None
                try:
                    self._remove_reaped_worker(pid)
                except Exception:
                    if not original_failure:
                        raise
                    logging.exception('Mutation temp cleanup also failed for pid %s', pid)

        mm.PytestRunner.execute_pytest = execute_pytest
        mm.register_timeout = register_timeout
        mm.SourceFileMutationData.register_result = register_result
        return self

    def _activate_namespace(self):
        pid = os.getpid()
        if self.root is None:
            if pid != self.owner_pid:
                raise RuntimeError('mutation worker started before coordinator namespace')
            self.root = Path(tempfile.mkdtemp(prefix='mmr-mutmut-')).resolve()
            info = self.root.lstat()
            self._root_identity = (info.st_dev, info.st_ino, info.st_uid)
            (self.root / 'owner.json').write_text(json.dumps({
                'kind': 'mmr-mutation-temporary-files',
                'driver_pid': self.owner_pid,
                'uid': os.getuid(),
                'coordinator_cleanup': 'only after driver and all workers exit',
            }) + '\n')
            print(f'Mutation temporary files: {self.root}', file=sys.stderr, flush=True)
        self._check_root()
        if self._active_pid != pid:
            name = 'coordinator' if pid == self.owner_pid else f'worker-{pid}'
            self._namespace = self.root / name
            self._namespace.mkdir(mode=0o700)
            self._active_pid = pid
        for name in self._TEMP_ENV:
            os.environ[name] = str(self._namespace)
        # tempfile caches its directory; changing TMPDIR alone after fork does
        # not change TemporaryDirectory/mkdtemp calls in the imported test code.
        tempfile.tempdir = str(self._namespace)

    def _check_root(self):
        if self.root is None:
            raise RuntimeError('missing mutation temporary root')
        info = self.root.lstat()
        if (not stat.S_ISDIR(info.st_mode)
                or (info.st_dev, info.st_ino, info.st_uid) != self._root_identity
                or info.st_uid != os.getuid() or info.st_mode & 0o077):
            raise RuntimeError('mutation temporary root identity changed')

    def _remove_reaped_worker(self, pid):
        self._check_root()
        path = self.root / f'worker-{pid}'
        try:
            info = path.lstat()
        except FileNotFoundError:
            # A child can die before reaching execute_pytest.
            return
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
            raise RuntimeError(f'unsafe mutation worker directory: {path}')
        shutil.rmtree(path)

    def _poll_owned_child(self, pid, errors):
        # A positive waitpid is the only proof used to permit deletion. If
        # somebody else reaped it, do not signal a potentially recycled PID.
        try:
            waited, _ = os.waitpid(pid, os.WNOHANG)
        except OSError as error:
            # Retain this namespace and do not signal an unconfirmed PID, but
            # still shut down the other children whose ownership we can prove.
            self._children.remove(pid)
            errors.append(error)
            return True
        if waited:
            self._children.remove(pid)
            try:
                self._remove_reaped_worker(pid)
            except Exception as error:
                errors.append(error)
            return True
        return False

    def _stop_unreported_children(self):
        if not self._children:
            return
        errors = []
        for sig, grace in ((signal.SIGTERM, self._TERMINATE_GRACE_SECONDS),
                           (signal.SIGKILL, self._KILL_GRACE_SECONDS)):
            for pid in tuple(self._children):
                if not self._poll_owned_child(pid, errors):
                    try:
                        os.kill(pid, sig)
                    except ProcessLookupError:
                        pass  # The next waitpid must still confirm its exit.
            deadline = time.monotonic() + grace
            while self._children and time.monotonic() < deadline:
                for pid in tuple(self._children):
                    self._poll_owned_child(pid, errors)
                if self._children:
                    time.sleep(0.01)
        if self._children:
            raise RuntimeError(f'Mutation workers still alive; retaining temp files: {sorted(self._children)}')
        if errors:
            raise RuntimeError('Mutation worker shutdown could not safely reclaim every namespace') from errors[0]
        # These workers never passed mutmut's result hook. Do not turn an
        # interrupted/aborted run into a success or manufacture their verdicts.
        raise RuntimeError('Mutation run ended with unreported workers; their verdicts remain incomplete')

    def __exit__(self, exc_type, exc, traceback):
        if os.getpid() != self.owner_pid:
            return False  # A child must never clean inherited sibling state.
        completed_normally = exc_type is None or (
            isinstance(exc, SystemExit) and (
                exc.code is None or isinstance(exc.code, int) and exc.code == 0))
        try:
            try:
                self._stop_unreported_children()
            except Exception:
                if completed_normally:
                    raise
                logging.exception('Mutation worker shutdown also failed')
        finally:
            mm.PytestRunner.execute_pytest = self._execute_pytest
            mm.register_timeout = self._register_timeout
            mm.SourceFileMutationData.register_result = self._register_result
            tempfile.tempdir = self._previous_tempdir
            for name, value in self._previous_env.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            if self.root is not None:
                # Also retain on a post-fork/pre-registration interruption:
                # no visible directory is not proof that no child exists yet.
                print(f'Mutation coordinator temp retained until driver exit: {self.root}',
                      file=sys.stderr, flush=True)
        return False


if __name__ == "__main__":
    _plain_exception_tracebacks()
    with _MutationTempDirectories():
        mm.cli(args=sys.argv[1:])
