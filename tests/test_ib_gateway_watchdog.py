"""Exercise the actual gateway-local guard with deterministic subprocesses.

Only sleep, date and probe results are substituted through PATH/files. There
is no Docker connection, host scheduler, live API probe or wall-clock wait.
"""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
WATCHDOG = ROOT / 'scripts/ib_gateway_watchdog.sh'
HEALTHCHECK = ROOT / 'scripts/ib_gateway_healthcheck.sh'
NOW = 1_800_000_000


class LocalGuard:
    def __init__(self, root):
        self.root = root
        self.bin = root / 'bin'
        self.bin.mkdir()
        self.state = root / 'persistent settings' / '.mmr-recovery'
        self.stamp = self.state / 'last_restart'
        self.calls = root / 'calls.jsonl'
        self.control = root / 'control.json'
        self.probe = root / 'native probe.sh'
        common = f'''from pathlib import Path
import json, os, signal, sys
root = Path({str(root)!r})
control = json.loads((root / 'control.json').read_text())
calls = root / 'calls.jsonl'
history = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
def record(kind, value=None):
    with calls.open('a') as out:
        out.write(json.dumps([kind, value]) + '\\n')
'''
        self._executable(self.bin / 'sleep', common + '''
assert sys.argv[1:] == ['300'], sys.argv
checks = sum(row[0] == 'sleep' for row in history)
record('sleep', 300)
if control.get('sleep_failure'):
    raise SystemExit(control['sleep_failure'])
if checks >= control['checks']:
    record('signal', 'TERM')
    os.kill(os.getppid(), signal.SIGTERM)
''')
        self._executable(self.bin / 'date', common + '''
assert sys.argv[1:] == ['+%s'], sys.argv
print(control['now'])
''')
        probe_driver = root / 'probe_driver.py'
        probe_driver.write_text(common + '''
index = sum(row[0] == 'probe' for row in history)
codes = control['codes']
code = codes[min(index, len(codes) - 1)]
record('probe', code)
raise SystemExit(code)
''')
        # The guard deliberately invokes a shell probe; keep paths with spaces
        # so shell argument boundaries are exercised rather than assumed.
        import shlex
        self.probe.write_text('#!/bin/bash\nexec ' + shlex.quote(sys.executable)
                              + ' ' + shlex.quote(str(probe_driver)) + '\n')
        self.probe.chmod(0o755)
        for name in ('docker', 'launchctl', 'crontab'):
            self._executable(self.bin / name, common + f'''
record('forbidden', {name!r})
raise SystemExit(99)
''')

    @staticmethod
    def _executable(path, body):
        path.write_text(f'#!{sys.executable}\n' + body)
        path.chmod(0o755)

    def seed(self, contents):
        self.state.mkdir(parents=True, exist_ok=True)
        self.stamp.write_text(contents)

    def run(self, codes, *, checks=None, now=NOW, probe=None, extra_env=None, sleep_failure=0):
        self.control.write_text(json.dumps({
            'codes': codes,
            'checks': len(codes) if checks is None else checks,
            'now': now,
            'sleep_failure': sleep_failure,
        }))
        self.calls.unlink(missing_ok=True)
        env = {**os.environ, **(extra_env or {}),
               'PATH': str(self.bin) + os.pathsep + os.environ['PATH']}
        command = ['/bin/bash', str(WATCHDOG), str(probe or self.probe), str(self.state)]
        process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            stdout, stderr = process.communicate(timeout=10)
        except BaseException:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            raise
        events = [json.loads(line) for line in self.calls.read_text().splitlines()] if self.calls.exists() else []
        assert not any(row[0] == 'forbidden' for row in events), events
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr), events


@pytest.fixture
def guard(tmp_path):
    return LocalGuard(tmp_path)


def _probes(events):
    return [value for kind, value in events if kind == 'probe']


def test_three_confirmed_failures_reserve_cooldown_before_restart_exit(guard):
    result, events = guard.run([1, 1, 1])
    assert result.returncode == 42, result.stdout + result.stderr
    assert events == [['sleep', 300], ['probe', 1]] * 3
    assert guard.stamp.read_text().strip() == str(NOW)


def test_healthy_gateway_never_requests_restart(guard):
    result, events = guard.run([0] * 6)
    assert result.returncode == 0, result.stdout + result.stderr
    assert events[0] == ['sleep', 300]
    assert _probes(events) == [0] * 6
    assert not guard.stamp.exists()


@pytest.mark.parametrize('third_failure', [False, True])
def test_healthy_probe_clears_consecutive_failure_streak(guard, third_failure):
    codes = [1, 1, 0, 1, 1] + ([1] if third_failure else [])
    result, events = guard.run(codes)
    assert result.returncode == (42 if third_failure else 0), result.stdout + result.stderr
    assert _probes(events) == codes
    assert guard.stamp.exists() is third_failure
    assert 'recovered' in (result.stdout + result.stderr).lower()


@pytest.mark.parametrize('unavailable', [2, 42, 126, 127, 137])
def test_unassessable_probe_breaks_failure_streak(guard, unavailable):
    codes = [1, 1, unavailable, 1, 1]
    result, events = guard.run(codes)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _probes(events) == codes
    assert 'probe unavailable' in result.stdout + result.stderr
    assert not guard.stamp.exists()


def test_restart_can_follow_three_new_failures_after_unassessable_probe(guard):
    codes = [1, 1, 2, 1, 1, 1]
    result, events = guard.run(codes)
    assert result.returncode == 42, result.stdout + result.stderr
    assert _probes(events) == codes
    assert guard.stamp.read_text().strip() == str(NOW)


def test_missing_probe_is_unassessable_without_requesting_restart(guard):
    result, events = guard.run([1], checks=4, probe=guard.root / 'missing probe.sh')
    assert result.returncode == 0, result.stdout + result.stderr
    assert _probes(events) == []
    assert 'probe unavailable' in result.stdout + result.stderr
    assert not guard.stamp.exists()


@pytest.mark.parametrize('mode', ['', 'invalid'])
def test_real_probe_unknown_mode_never_counts_as_gateway_failure(guard, mode):
    result, events = guard.run([1], checks=4, probe=HEALTHCHECK,
                               extra_env={'TRADING_MODE': mode, 'DUAL_MODE': 'yes'})
    assert result.returncode == 0, result.stdout + result.stderr
    assert events[0] == ['sleep', 300]
    assert 'probe unavailable' in result.stdout + result.stderr
    assert not guard.stamp.exists()


def test_cooldown_survives_a_new_guard_process_until_full_1800_seconds(guard):
    first, _ = guard.run([1, 1, 1])
    assert first.returncode == 42
    saved = guard.stamp.read_bytes()

    suppressed, events = guard.run([1, 1, 1, 1], now=NOW + 1799)
    assert suppressed.returncode == 0, suppressed.stdout + suppressed.stderr
    assert _probes(events) == [1] * 4
    assert guard.stamp.read_bytes() == saved
    assert 'restart suppressed' in suppressed.stdout + suppressed.stderr

    eligible, events = guard.run([1, 1, 1], now=NOW + 1800)
    assert eligible.returncode == 42, eligible.stdout + eligible.stderr
    assert _probes(events) == [1] * 3
    assert guard.stamp.read_text().strip() == str(NOW + 1800)


def test_future_restart_timestamp_conservatively_suppresses_restart(guard):
    contents = f'{NOW + 3600}\n'
    guard.seed(contents)
    result, _ = guard.run([1] * 4)
    assert result.returncode == 0, result.stdout + result.stderr
    assert guard.stamp.read_text() == contents
    assert 'restart suppressed' in result.stdout + result.stderr


def test_restart_timestamp_is_decimal_even_with_leading_zero(guard):
    guard.seed('0999999999\n')
    result, _ = guard.run([1, 1, 1], now=1_000_000_000)
    assert result.returncode == 0, result.stdout + result.stderr
    assert guard.stamp.read_text() == '0999999999\n'
    assert 'restart suppressed' in result.stdout + result.stderr


@pytest.mark.parametrize('contents', ['', 'invalid\n', '-1\n', '1.5\n',
                                      '1\n2\n', '10000000000\n'])
def test_invalid_restart_state_is_preserved_and_loudly_skipped(guard, contents):
    guard.seed(contents)
    result, _ = guard.run([1, 1, 1])
    assert result.returncode == 0, result.stdout + result.stderr
    assert guard.stamp.read_text() == contents
    assert 'invalid restart state; not intervening' in result.stdout + result.stderr


def test_sigterm_after_two_failures_exits_without_restart_request(guard):
    result, events = guard.run([1, 1])
    assert result.returncode == 0, result.stdout + result.stderr
    assert _probes(events) == [1, 1]
    assert events[-1] == ['signal', 'TERM']
    assert not guard.stamp.exists()


def test_failed_sleep_cannot_skip_grace_or_create_a_busy_restart_loop(guard):
    result, events = guard.run([1, 1, 1], sleep_failure=1)
    assert result.returncode == 2, result.stdout + result.stderr
    assert events == [['sleep', 300]]
    assert not guard.stamp.exists()


def test_uncreatable_state_directory_cannot_authorize_restart(guard):
    guard.state.parent.write_text('existing regular file\n')
    result, events = guard.run([1, 1, 1])
    assert result.returncode == 0, result.stdout + result.stderr
    assert _probes(events) == [1, 1, 1]
    assert guard.state.parent.read_text() == 'existing regular file\n'
    assert not guard.stamp.exists()
    assert 'restart state unavailable; not intervening' in result.stdout + result.stderr


@pytest.mark.parametrize('prior_timestamp', [None, NOW - 1801])
def test_failed_atomic_rename_preserves_cooldown_and_cannot_authorize_restart(guard, prior_timestamp):
    prior = None
    if prior_timestamp is not None:
        guard.seed(f'{prior_timestamp}\n')
        prior = guard.stamp.read_bytes()
    attempted = guard.root / 'rename attempted'
    guard._executable(guard.bin / 'mv',
                      f'from pathlib import Path\nPath({str(attempted)!r}).write_text("attempted")\n'
                      'raise SystemExit(1)\n')

    result, events = guard.run([1, 1, 1])

    assert result.returncode == 0, result.stdout + result.stderr
    assert _probes(events) == [1, 1, 1]
    assert attempted.read_text() == 'attempted'
    if prior is None:
        assert not guard.stamp.exists()
    else:
        assert guard.stamp.read_bytes() == prior
    assert list(guard.state.glob('.last_restart.*')) == []
    assert 'cannot persist restart cooldown; not intervening' in result.stdout + result.stderr
