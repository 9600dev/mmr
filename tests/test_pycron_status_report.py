"""The readers of pycron's status file: the status page, scripts/pycron_status.py
and scripts/monitor_health.sh.

``data_audit`` failed 46 nights in ``pycron_cron.log``, which nothing reads.
The status file only helps if something reports it, so each reader is
exercised against a file produced by the real scheduler.
"""
import asyncio
import datetime as dt
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application, url

import pycron.pycron as mod
from pycron.pycron import Job, JobScheduler, MainHandler

ROOT = Path(__file__).resolve().parents[1]
REPORTER = ROOT / 'scripts' / 'pycron_status.py'
MONITOR = ROOT / 'scripts' / 'monitor_health.sh'
UTC = dt.timezone.utc


def _load_reporter():
    spec = importlib.util.spec_from_file_location('pycron_status_script', REPORTER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


reporter = _load_reporter()


class _Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


def _run_job(scheduler, job, monkeypatch, returncode, out=b''):
    """One run of ``job`` through the real start_job with a scripted process."""
    class Process:
        pid = 4242

        def __init__(self):
            self.returncode = None
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.stdout.feed_data(out)
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            self.returncode = returncode
            return returncode

    async def spawn(command, **kwargs):
        return Process()

    monkeypatch.setattr(mod.asyncio, 'create_subprocess_shell', spawn)
    asyncio.run(scheduler.start_job(job))


@pytest.fixture
def status(tmp_path, monkeypatch):
    """A status file written by a scheduler whose audit job fails and whose backup succeeds."""
    path = tmp_path / 'logs' / 'pycron_status.json'
    clock = _Clock(dt.datetime(2026, 11, 14, 12, 0, tzinfo=UTC))
    jobs = [Job({'name': name, 'command': 'true', 'arguments': name, 'start': start,
                 'timezone': 'America/New_York'})
            for name, start in (('data_audit', '0 7 * * 1-5'), ('db_backup', '17 22 * * *'))]
    scheduler = JobScheduler(jobs, status_path=str(path), clock=clock)
    scheduler.first_start = False
    _run_job(scheduler, jobs[1], monkeypatch, 0, b'snapshot written\n')
    for _ in range(3):
        _run_job(scheduler, jobs[0], monkeypatch, 1, b'grading\nREGRESSED 1 min|empty_bar 534 vs 278\n')
    scheduler.last_poll = clock.now
    scheduler.save_status()
    return path, scheduler, clock


def _report(path, now, run_dir=None, **kwargs):
    return reporter.report(str(path), str(run_dir or path.parent / 'run'),
                           kwargs.pop('stale_minutes', 5.0), kwargs.pop('show_all', False), now=now)


# ── scripts/pycron_status.py ─────────────────────────────────────────────────

def test_reports_only_the_job_whose_last_run_failed(status):
    path, _, clock = status
    lines, code = _report(path, clock.now)
    assert code == 1
    assert len(lines) == 1
    line = lines[0]
    assert line.startswith('pycron: data_audit FAILED rc=1 at ')
    assert '3 in a row' in line and 'last success never' in line
    assert line.endswith('| REGRESSED 1 min|empty_bar 534 vs 278')


def test_silent_and_zero_when_everything_passed(status, monkeypatch):
    path, scheduler, clock = status
    _run_job(scheduler, scheduler.jobs[0], monkeypatch, 0)
    assert _report(path, clock.now) == ([], 0)


def test_all_lists_healthy_jobs_too(status):
    path, _, clock = status
    lines, code = _report(path, clock.now, show_all=True)
    assert code == 1 and len(lines) == 2
    assert any(line.startswith('pycron: db_backup ok, last run ') and 'rc=0' in line for line in lines)


def test_missed_run_is_reported_until_the_job_runs_again(status, monkeypatch):
    path, scheduler, clock = status
    backup = scheduler.jobs[1]
    backup.state['last_missed'] = '2026-11-15T03:17:00+00:00'      # after its last start
    backup.state['missed_count'] = 1
    scheduler.save_status()
    lines, code = _report(path, clock.now)
    assert code == 1
    assert any(line.startswith('pycron: db_backup MISSED its ') for line in lines)

    clock.now = dt.datetime(2026, 11, 16, 3, 17, tzinfo=UTC)
    _run_job(scheduler, backup, monkeypatch, 0)
    scheduler.last_poll = clock.now
    scheduler.save_status()
    lines, _ = _report(path, clock.now)
    assert not any('MISSED' in line for line in lines)


@pytest.mark.parametrize('outcome,word', [('timeout', 'TIMED OUT'), ('spawn_failed', 'COULD NOT START'),
                                          ('interrupted', 'INTERRUPTED'), ('unknown', 'OUTCOME UNKNOWN')])
def test_every_non_success_outcome_is_a_problem(status, outcome, word):
    path, scheduler, clock = status
    backup = scheduler.jobs[1]
    backup.state.update(last_outcome=outcome, last_return_code=None,
                        last_error='exceeded timeout_minutes=60; process group terminated')
    scheduler.save_status()
    lines, code = _report(path, clock.now)
    assert code == 1
    assert any(line.startswith(f'pycron: db_backup {word} exceeded timeout_minutes=60') for line in lines)


def test_stopped_by_the_scheduler_is_not_a_problem(status):
    path, scheduler, clock = status
    scheduler.jobs[0].state.update(last_outcome='stopped', last_return_code=-9, consecutive_failures=0)
    scheduler.save_status()
    assert _report(path, clock.now) == ([], 0)


def test_scheduler_that_stopped_polling_is_reported(status):
    path, _, clock = status
    lines, code = _report(path, clock.now + dt.timedelta(minutes=4))
    assert not any('has not polled' in line for line in lines)

    later = clock.now + dt.timedelta(minutes=30)
    lines, code = _report(path, later)
    assert code == 1
    stale = [line for line in lines if 'has not polled since' in line]
    assert len(stale) == 1 and 'NOT running' in stale[0]
    # The text does not change as the outage goes on, so a monitor prints it once.
    assert _report(path, later + dt.timedelta(hours=2))[0] == lines
    assert not any('has not polled' in line
                   for line in _report(path, later, stale_minutes=0)[0])


def test_circuit_breaker_marker_is_reported(status, tmp_path):
    path, _, clock = status
    run_dir = tmp_path / 'run'
    run_dir.mkdir()
    (run_dir / 'pycron.breaker').write_text('rc=1 restarts=5\n')
    (run_dir / 'trader_service.breaker').write_text('rc=1 restarts=5\n')   # not a side service
    lines, code = _report(path, clock.now, run_dir=run_dir)
    assert code == 1
    breaker = [line for line in lines if 'circuit-breaker' in line]
    assert breaker == ['side service pycron was stopped by the restart circuit-breaker (rc=1 restarts=5); '
                       'it stays down until the stack is restarted']


def test_missing_or_corrupt_status_file_is_exit_2(tmp_path):
    now = dt.datetime(2026, 11, 14, 12, 0, tzinfo=UTC)
    lines, code = _report(tmp_path / 'pycron_status.json', now)
    assert code == 2 and 'no status file' in lines[0]
    bad = tmp_path / 'bad.json'
    bad.write_text('{"jobs": ')
    lines, code = _report(bad, now)
    assert code == 2 and 'unreadable' in lines[0]


def test_command_line_runs_on_a_bare_interpreter(status):
    """No project imports: the monitor runs it with whatever python3 the host has."""
    path, _, _ = status
    result = subprocess.run(
        [sys.executable, '-I', str(REPORTER), '--file', str(path), '--run-dir', str(path.parent),
         '--stale-minutes', '0'],
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 1, result.stderr
    assert result.stdout.startswith('pycron: data_audit FAILED rc=1')
    assert result.stderr == ''


# ── scripts/monitor_health.sh ────────────────────────────────────────────────

def _monitor_output(log_dir, run_dir, capture, until, settle=1.0, limit=20.0):
    """Run the (never-ending) monitor until ``until`` shows up, then ``settle`` longer."""
    env = {**os.environ, 'MMR_LOG_DIR': str(log_dir), 'MMR_RUN_DIR': str(run_dir)}
    with open(capture, 'w') as sink:
        process = subprocess.Popen(['bash', str(MONITOR)], env=env, stdout=sink,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + limit
            while time.monotonic() < deadline and until not in Path(capture).read_text():
                time.sleep(0.05)
            time.sleep(settle)
        finally:
            # The script is a pipeline that never ends; stop the whole group.
            os.killpg(process.pid, 15)
            process.wait(timeout=10)
    return Path(capture).read_text()


@pytest.mark.skipif(sys.platform == 'win32', reason='bash monitor')
def test_monitor_health_reports_a_failed_job(status, tmp_path):
    path, _, _ = status
    # Keep the heartbeat fresh relative to the real clock the script uses.
    payload = json.loads(path.read_text())
    payload['scheduler']['last_poll'] = dt.datetime.now(UTC).replace(microsecond=0).isoformat()
    path.write_text(json.dumps(payload))
    output = _monitor_output(path.parent, tmp_path / 'run', tmp_path / 'monitor.out', until='FAILED')
    assert f'[monitor_health] scheduled jobs: {path}' in output
    failed = [line for line in output.splitlines() if 'data_audit FAILED' in line]
    assert len(failed) == 1
    assert failed[0].startswith('[monitor_health] pycron: data_audit FAILED rc=1')
    assert 'db_backup' not in output


@pytest.mark.skipif(sys.platform == 'win32', reason='bash monitor')
def test_monitor_health_is_quiet_about_healthy_jobs(status, tmp_path, monkeypatch):
    path, scheduler, _ = status
    _run_job(scheduler, scheduler.jobs[0], monkeypatch, 0)
    scheduler.last_poll = dt.datetime.now(UTC)
    scheduler.save_status()
    output = _monitor_output(path.parent, tmp_path / 'run', tmp_path / 'monitor.out',
                             until='scheduled jobs:', settle=2.0)
    assert '[monitor_health] scheduled jobs:' in output
    assert 'pycron:' not in output


# ── the status page ──────────────────────────────────────────────────────────

class TestStatusPage(AsyncHTTPTestCase):
    def get_app(self):
        self.job = Job({'name': 'data_audit', 'command': 'true', 'start': '0 7 * * 1-5',
                        'timezone': 'America/New_York', 'catch_up_minutes': 240, 'timeout_minutes': 60})
        self.scheduler = JobScheduler([self.job])
        self.job.state.update(last_start='2026-11-13T12:00:00+00:00', last_end='2026-11-13T12:00:41+00:00',
                              last_return_code=1, last_outcome='failed', last_success='2026-09-29T11:00:12+00:00',
                              consecutive_failures=46, last_output_tail='REGRESSED 1 min|empty_bar 534 vs 278')
        return Application([url(r'/', MainHandler, dict(job_scheduler=self.scheduler))])

    def test_page_exposes_the_run_history(self):
        response = self.fetch('/')
        assert response.code == 200
        (page,) = json.loads(response.body)
        assert page['name'] == 'data_audit'
        assert page['last_start'] == '2026-11-13T12:00:00+00:00'
        assert page['last_end'] == '2026-11-13T12:00:41+00:00'
        assert page['last_return_code'] == 1
        assert page['last_success'] == '2026-09-29T11:00:12+00:00'
        assert page['consecutive_failures'] == 46
        assert page['last_output_tail'] == 'REGRESSED 1 min|empty_bar 534 vs 278'
        assert page['catch_up_minutes'] == 240 and page['timeout_minutes'] == 60
        assert set(mod.JOB_STATE_DEFAULTS) <= set(page)

    def test_restart_over_http_is_still_refused(self):
        assert self.fetch('/?restart=data_audit').code == 405
