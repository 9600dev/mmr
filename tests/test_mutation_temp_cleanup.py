"""The mutation driver reclaims only positively reaped worker namespaces.

Every control runs in a separate, bounded process group. It uses the installed
mutmut result writer and real fork/wait semantics without generating mutations
or collecting the application oracle.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest


DRIVER = Path(__file__).resolve().parents[1] / 'scripts/run_mutation.py'

CONTROL = r"""
import errno
import json
import os
from pathlib import Path
import runpy
import resource
import select
import signal
import sqlite3
import sys
import tempfile

driver, scenario, argument = sys.argv[1:]
# Mutmut reads configuration during import. Keep that read inside this tiny
# control project rather than importing the application's mutation oracle.
(Path.cwd() / 'probe_source.py').write_text('def value():\n    return 1\n')
(Path.cwd() / 'pyproject.toml').write_text(
    '[tool.mutmut]\nsource_paths = ["probe_source.py"]\nuse_setproctitle = false\n')
namespace = runpy.run_path(driver)
mm = namespace['mm']
Manager = namespace['_MutationTempDirectories']
manager = Manager()
manager._TERMINATE_GRACE_SECONDS = 0.1
manager._KILL_GRACE_SECONDS = 1.0
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
original_env = {key: os.environ.get(key) for key in manager._TEMP_ENV}
original_tempdir = tempfile.tempdir
timeout_calls = []
mm.register_timeout = lambda **kwargs: timeout_calls.append(kwargs)
if scenario == 'timeout_registration':
    def fail_timeout(**kwargs):
        timeout_calls.append(kwargs)
        raise LookupError('timeout registration failed')
    mm.register_timeout = fail_timeout
held = []


def exercise(runner, params, **kwargs):
    base = Path(tempfile.gettempdir())
    assert all(os.environ[key] == str(base) for key in manager._TEMP_ENV)
    temporary = tempfile.TemporaryDirectory(prefix='mmr-lifecycle-')
    connection = sqlite3.connect(Path(temporary.name) / 'events.execution.sqlite3')
    connection.execute('CREATE TABLE state(value INTEGER)')
    connection.execute('INSERT INTO state VALUES (7)')
    connection.commit()
    held.append((temporary, connection))  # Deliberately defeat early GC cleanup.
    (base / 'ready').write_text('live')
    return 0


if scenario != 'pytest':
    mm.PytestRunner.execute_pytest = exercise

runner = mm.PytestRunner()
data = mm.SourceFileMutationData(path='probe.py')
data.meta_path = Path.cwd() / 'probe.meta'
data.exit_code_by_key = {}


def spawn(*, wait=False, ignore_term=False, track=True, exit_code=7):
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        if ignore_term:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            runner.execute_pytest([])
            os.write(write_fd, b'R')
            os.close(write_fd)
            if wait:
                while True:
                    signal.pause()
            os._exit(exit_code)
        except BaseException:
            os._exit(91)
    os.close(write_fd)
    key = f'probe-{pid}'
    data.exit_code_by_key[key] = None
    data.register_pid(pid=pid, key=key)
    if track:
        mm.register_timeout(pid=pid, timeout_s=123.0)
    assert select.select([read_fd], [], [], 5)[0], 'worker did not become ready'
    assert os.read(read_fd, 1) == b'R'
    os.close(read_fd)
    return pid, key


def reap(pid):
    waited, status = os.waitpid(pid, 0)
    assert waited == pid
    return os.waitstatus_to_exitcode(status)


def worker_path(pid):
    return manager.root / f'worker-{pid}'


result = {}
if scenario == 'pytest':
    probe = Path.cwd() / 'test_probe.py'
    probe.write_text('''
import os
from pathlib import Path
import tempfile
held = []
def test_real_temporary_files(tmp_path, request):
    base = Path(tempfile.gettempdir())
    assert tmp_path.is_relative_to(base)
    assert all(os.environ[k] == str(base) for k in ('TMPDIR', 'TMP', 'TEMP', 'PYTEST_DEBUG_TEMPROOT'))
    held.append(tempfile.TemporaryDirectory(prefix='mmr-lifecycle-'))
    (Path(held[-1].name) / 'retained').write_text('data')
    (base / 'nodeid').write_text(request.node.nodeid)
    assert os.environ.get('CONTROL_FAILURE') != '1'
''')
    params = ['-q', '--confcutdir=' + str(Path.cwd()), str(probe)]
    with manager:
        assert runner.execute_pytest(params) == 0
        coordinator = Path(tempfile.gettempdir())
        coordinator_node = (coordinator / 'nodeid').read_text()
        pid = os.fork()
        if pid == 0:
            os.environ['CONTROL_FAILURE'] = argument
            os._exit(runner.execute_pytest(params))
        key = f'probe-{pid}'
        data.exit_code_by_key[key] = None
        data.register_pid(pid=pid, key=key)
        mm.register_timeout(pid=pid, timeout_s=123.0)
        code = reap(pid)
        child = worker_path(pid)
        child_node = (child / 'nodeid').read_text()
        assert list(child.glob('mmr-lifecycle-*/retained'))
        assert list(child.glob('pytest-of-*/pytest-*/test_real_temporary_files*'))
        data.register_result(pid=pid, exit_code=code)
        assert not child.exists()
        assert coordinator.is_dir()
        assert child_node == coordinator_node
        assert data.exit_code_by_key[key] == int(argument)
        result.update(code=code, same_selected_node=True, child_removed=True)
    assert coordinator.is_dir()
elif scenario == 'siblings':
    with manager:
        runner.execute_pytest([])
        coordinator = Path(tempfile.gettempdir())
        first, first_key = spawn(wait=True)
        second, second_key = spawn(wait=True)
        assert worker_path(first) != worker_path(second) != coordinator
        os.kill(first, int(argument))
        code = reap(first)
        data.register_result(pid=first, exit_code=code)
        assert not worker_path(first).exists()
        assert (worker_path(second) / 'ready').read_text() == 'live'
        assert (coordinator / 'ready').read_text() == 'live'
        os.kill(second, signal.SIGTERM)
        data.register_result(pid=second, exit_code=reap(second))
        result.update(code=code, first_removed=True, live_sibling_preserved=True)
    # An open baseline journal is still usable after the context exits.
    assert held[0][1].execute('SELECT value FROM state').fetchone() == (7,)
    assert coordinator.is_dir()
elif scenario == 'metadata_failure':
    with manager:
        runner.execute_pytest([])
        pid, key = spawn()
        code = reap(pid)
        if argument == 'symlink':
            worker_path(pid).rename(manager.root / 'retained-real-worker')
            neighbor = Path.cwd() / 'neighbor'
            neighbor.mkdir()
            (neighbor / 'keep').write_text('foreign')
            worker_path(pid).symlink_to(neighbor, target_is_directory=True)
        def fail_save():
            raise OSError(errno.ENOSPC, 'metadata storage failed')
        data.save = fail_save
        try:
            data.register_result(pid=pid, exit_code=code)
        except OSError as error:
            assert error.errno == errno.ENOSPC
        else:
            raise AssertionError('metadata exception was swallowed')
        assert data.exit_code_by_key[key] == 7
        if argument == 'symlink':
            assert worker_path(pid).is_symlink()
            assert (neighbor / 'keep').read_text() == 'foreign'
        else:
            assert not worker_path(pid).exists()
        result.update(original_error_preserved=True, verdict=7)
elif scenario == 'abort':
    try:
        with manager:
            runner.execute_pytest([])
            pid, key = spawn(wait=True, ignore_term=True)
            if argument == 'exit':
                raise SystemExit(7)
            raise ValueError('original coordinating failure')
    except (ValueError, SystemExit) as error:
        if argument == 'exit':
            assert isinstance(error, SystemExit) and error.code == 7
        else:
            assert str(error) == 'original coordinating failure'
    else:
        raise AssertionError('original failure was swallowed')
    try:
        os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        pass
    else:
        raise AssertionError('aborted worker was not reaped')
    assert not worker_path(pid).exists()
    assert data.exit_code_by_key[key] is None
    result.update(worker_reaped=True, original_error_preserved=True, pending_preserved=True)
elif scenario == 'unreported':
    try:
        with manager:
            runner.execute_pytest([])
            pid, key = spawn()
            # Deliberately return without mutmut's wait/result callback.
            if argument == 'click':
                raise SystemExit(0)  # Click's standalone CLI success path.
            if argument == 'click_none':
                raise SystemExit(None)
    except RuntimeError as error:
        assert 'unreported workers' in str(error)
    else:
        raise AssertionError('incomplete mutation run appeared successful')
    assert data.exit_code_by_key[key] is None
    assert not worker_path(pid).exists()
    result.update(pending_preserved=True, run_refused=True)
elif scenario == 'timeout_registration':
    try:
        with manager:
            runner.execute_pytest([])
            spawn(wait=True)
    except LookupError as error:
        assert str(error) == 'timeout registration failed'
    else:
        raise AssertionError('timeout registration failure was swallowed')
    pid = timeout_calls[0]['pid']
    try:
        os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        pass
    else:
        raise AssertionError('worker escaped a failed timeout registration')
    assert not worker_path(pid).exists()
    assert list(data.exit_code_by_key.values()) == [None]
    result.update(worker_reaped=True, pending_preserved=True)
elif scenario == 'unconfirmable':
    try:
        with manager:
            runner.execute_pytest([])
            first, first_key = spawn(wait=True)
            second, second_key = spawn(wait=True)
            os.kill(first, signal.SIGTERM)
            reap(first)  # Simulate another consumer taking the wait status.
    except RuntimeError as error:
        assert 'could not safely reclaim' in str(error)
    else:
        raise AssertionError('unconfirmed ownership was accepted')
    assert worker_path(first).is_dir()
    assert not worker_path(second).exists()
    assert data.exit_code_by_key[first_key] is None
    assert data.exit_code_by_key[second_key] is None
    result.update(unconfirmed_retained=True, other_worker_reaped=True)
elif scenario == 'unsafe_namespace':
    with manager:
        runner.execute_pytest([])
        pid, key = spawn(track=argument != 'untracked')
        code = reap(pid)
        neighbor = Path.cwd() / 'neighbor'
        neighbor.mkdir()
        (neighbor / 'keep').write_text('foreign')
        if argument == 'symlink':
            worker_path(pid).rename(manager.root / 'retained-real-worker')
            worker_path(pid).symlink_to(neighbor, target_is_directory=True)
        try:
            data.register_result(pid=pid, exit_code=code)
        except RuntimeError:
            pass
        else:
            raise AssertionError('unsafe cleanup was accepted')
        assert worker_path(pid).exists()
        assert (neighbor / 'keep').read_text() == 'foreign'
        result.update(namespace_retained=True, neighbor_untouched=True)
else:
    raise AssertionError(scenario)

assert tempfile.tempdir == original_tempdir
assert {key: os.environ.get(key) for key in manager._TEMP_ENV} == original_env
assert manager.root.is_dir(), 'coordinator root must outlive the live driver'
result.update(coordinator_retained=True, environment_restored=True,
              timeout_arguments=[call['timeout_s'] for call in timeout_calls])
print('CONTROL_RESULT=' + json.dumps(result, sort_keys=True), flush=True)
"""


def control(tmp_path, scenario, argument=''):
    script = tmp_path / 'control.py'
    script.write_text(CONTROL)
    environment = dict(os.environ, TMPDIR=str(tmp_path), TMP=str(tmp_path),
                       TEMP=str(tmp_path), PYTEST_DEBUG_TEMPROOT=str(tmp_path),
                       PYTEST_DISABLE_PLUGIN_AUTOLOAD='1')
    process = subprocess.Popen(
        [sys.executable, str(script), str(DRIVER), scenario, str(argument)],
        cwd=tmp_path, env=environment, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True, start_new_session=True,
    )
    try:
        output, _ = process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        output, _ = process.communicate(timeout=5)
        pytest.fail('mutation temp control exceeded its process-group deadline:\n' + output)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate(timeout=5)
        raise
    finally:
        # A failing control can leave a descendant that closed its stdout.
        # Reclaim only this control's new session, even after a normal return.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    assert process.returncode == 0, output
    records = [line.removeprefix('CONTROL_RESULT=') for line in output.splitlines()
               if line.startswith('CONTROL_RESULT=')]
    assert len(records) == 1, output
    return json.loads(records[0])


@pytest.mark.parametrize('failure', [0, 1])
def test_real_pytest_verdict_and_selection_survive_temp_isolation(tmp_path, failure):
    result = control(tmp_path, 'pytest', failure)
    assert result['code'] == failure
    assert result['same_selected_node'] and result['child_removed']
    assert result['coordinator_retained'] and result['environment_restored']
    assert result['timeout_arguments'] == [123.0]


@pytest.mark.parametrize('death_signal', [signal.SIGTERM, signal.SIGXCPU, signal.SIGKILL])
def test_reaped_signal_worker_is_removed_without_touching_live_sibling(tmp_path, death_signal):
    result = control(tmp_path, 'siblings', death_signal)
    assert result['code'] == -death_signal
    assert result['first_removed'] and result['live_sibling_preserved']


@pytest.mark.parametrize('cleanup_failure', ['', 'symlink'])
def test_metadata_save_error_remains_original_even_if_cleanup_fails(tmp_path, cleanup_failure):
    result = control(tmp_path, 'metadata_failure', cleanup_failure)
    assert result['original_error_preserved'] and result['verdict'] == 7


@pytest.mark.parametrize('failure', ['exception', 'exit'])
def test_parent_abort_kills_and_reaps_only_its_worker_without_inventing_verdict(tmp_path, failure):
    result = control(tmp_path, 'abort', failure)
    assert result['worker_reaped'] and result['pending_preserved']
    assert result['original_error_preserved']


@pytest.mark.parametrize('return_path', ['return', 'click', 'click_none'])
def test_normal_return_with_unreported_child_cannot_claim_complete_run(tmp_path, return_path):
    result = control(tmp_path, 'unreported', return_path)
    assert result['pending_preserved'] and result['run_refused']


def test_timeout_registration_failure_cannot_leave_its_child_running(tmp_path):
    result = control(tmp_path, 'timeout_registration')
    assert result['worker_reaped'] and result['pending_preserved']


def test_unconfirmable_pid_retains_its_files_but_other_owned_children_are_reaped(tmp_path):
    result = control(tmp_path, 'unconfirmable')
    assert result['unconfirmed_retained'] and result['other_worker_reaped']


@pytest.mark.parametrize('unsafe', ['symlink', 'untracked'])
def test_unknown_worker_or_substituted_namespace_is_retained(tmp_path, unsafe):
    result = control(tmp_path, 'unsafe_namespace', unsafe)
    assert result['namespace_retained'] and result['neighbor_untouched']
