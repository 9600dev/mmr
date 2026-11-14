"""pycron: failures are visible, missed runs are caught up or reported, hung jobs
are killed, and a failed spawn does not disable a job.

The 2026-11-14 audit (B5.3) found each of these the hard way: ``data_audit``
exited non-zero 46 nights running in a log no monitor reads; three jobs were
silently skipped on 2026-11-10 while the stack was down; a hung job counted as
running for ever; one failed ``create_subprocess_shell`` left
``trying_to_start`` set, so every later firing returned at the guard.

No real clock and (bar two process-group tests) no real subprocess: the
scheduler takes an injected clock and ``asyncio.create_subprocess_shell`` is
replaced by a scripted fake.
"""
import asyncio
import datetime as dt
import json
import logging
import os
import signal
import sys
import time
from zoneinfo import ZoneInfo

import psutil
import pytest
import yaml

import pycron.pycron as mod
from pycron.pycron import Job, JobScheduler, default_status_path, redact_secrets, status_tail

UTC = dt.timezone.utc
NY = ZoneInfo('America/New_York')


def ny(year, month, day, hour, minute, second=0):
    """A New York wall time as a UTC instant."""
    return dt.datetime(year, month, day, hour, minute, second, tzinfo=NY).astimezone(UTC)


class FakeClock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


class FakeProcess:
    """Stands in for asyncio.subprocess.Process."""

    def __init__(self, pid):
        self.pid = pid
        self.returncode = None
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self._exited = asyncio.Event()

    def finish(self, returncode=0, out=b'', err=b''):
        if out:
            self.stdout.feed_data(out)
        if err:
            self.stderr.feed_data(err)
        self.stdout.feed_eof()
        self.stderr.feed_eof()
        self.returncode = returncode
        self._exited.set()

    async def wait(self):
        await self._exited.wait()
        return self.returncode


class FakeSpawner:
    """Scripted ``asyncio.create_subprocess_shell``.

    Each script entry is an exception to raise, a callable applied to the new
    FakeProcess (e.g. to finish it), or None for a process that keeps running.
    """

    def __init__(self, script):
        self.script = list(script)
        self.calls = []
        self.processes = []

    async def __call__(self, command, **kwargs):
        self.calls.append((command, kwargs))
        behaviour = self.script.pop(0) if self.script else None
        if isinstance(behaviour, BaseException):
            raise behaviour
        process = FakeProcess(pid=40000 + len(self.processes))
        self.processes.append(process)
        if behaviour is not None:
            behaviour(process)
        return process


def exits(returncode=0, out=b'', err=b''):
    return lambda process: process.finish(returncode, out, err)


@pytest.fixture
def spawn(monkeypatch):
    def install(*script):
        spawner = FakeSpawner(script)
        monkeypatch.setattr(mod.asyncio, 'create_subprocess_shell', spawner)
        return spawner
    return install


def make_job(name='audit', start='0 7 * * 1-5', **extra):
    spec = {'name': name, 'command': 'true', 'arguments': name, 'start': start,
            'timezone': 'America/New_York'}
    spec.update(extra)
    return Job(spec)


def make_scheduler(jobs, clock=None, status_path=None, first_start=False):
    scheduler = JobScheduler(jobs, status_path=status_path, clock=clock)
    # first_start=True scans the process table for a leftover process of each
    # job; only the restart tests want that pass (with the scan stubbed out).
    scheduler.first_start = first_start
    scheduler.catch_up_settle_s = 0.0
    scheduler.spawn_retry_delays = (0.0, 0.0)
    return scheduler


def record_starts(scheduler, clock=None):
    """Replace start_job with a recorder; returns the list of (job name, time)."""
    started = []

    async def fake_start(job):
        started.append((job.name, clock.now if clock else None))

    scheduler.start_job = fake_start
    return started


def poll_at(scheduler, clock, times):
    async def go():
        for instant in times:
            clock.now = instant
            scheduler.polling_loop()
            for _ in range(5):
                await asyncio.sleep(0)
    asyncio.run(go())


def every_minute(first, last, offset_seconds=20):
    instant = first + dt.timedelta(seconds=offset_seconds)
    while instant <= last:
        yield instant
        instant += dt.timedelta(seconds=60)


def read_status(path):
    with open(path) as fh:
        return json.load(fh)


# ── 1. failures are visible ──────────────────────────────────────────────────

def test_nonzero_exit_is_logged_at_error_and_recorded(spawn, tmp_path, caplog):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 13, 7, 0))
    job = make_job('data_audit')
    scheduler = make_scheduler([job], clock, status_path)
    spawn(exits(1, b'grading...\nREGRESSED 1 min|empty_bar 534 vs 278\n', b'Traceback: boom\n'))

    with caplog.at_level(logging.INFO, logger='pycron'):
        asyncio.run(scheduler.start_job(job))

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert 'data_audit' in errors[0].getMessage() and 'ret: [1]' in errors[0].getMessage()

    entry = read_status(status_path)['jobs']['data_audit']
    assert entry['last_return_code'] == 1
    assert entry['last_outcome'] == 'failed'
    assert entry['consecutive_failures'] == 1
    assert entry['last_success'] is None
    assert entry['last_start'] == entry['last_end'] == '2026-11-13T12:00:00+00:00'
    assert 'REGRESSED 1 min|empty_bar 534 vs 278' in entry['last_output_tail']
    assert 'Traceback: boom' in entry['last_output_tail']
    assert entry['running'] is False


def test_success_is_info_and_resets_the_failure_streak(spawn, tmp_path, caplog):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 12, 7, 0))
    job = make_job('data_audit')
    scheduler = make_scheduler([job], clock, status_path)
    spawn(exits(1), exits(2), exits(0, b'clean\n'))

    asyncio.run(scheduler.start_job(job))
    clock.now = ny(2026, 11, 13, 7, 0)
    asyncio.run(scheduler.start_job(job))
    assert read_status(status_path)['jobs']['data_audit']['consecutive_failures'] == 2

    clock.now = ny(2026, 11, 16, 7, 0)
    caplog.clear()
    with caplog.at_level(logging.INFO, logger='pycron'):
        asyncio.run(scheduler.start_job(job))
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any('job finished ret: [0]' in r.getMessage() for r in caplog.records)

    entry = read_status(status_path)['jobs']['data_audit']
    assert entry['consecutive_failures'] == 0
    assert entry['last_outcome'] == 'success' and entry['last_error'] is None
    assert entry['last_success'] == '2026-11-16T12:00:00+00:00'
    assert entry['run_count'] == 3


def test_failure_streak_survives_a_scheduler_restart(spawn, tmp_path):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 12, 7, 0))
    first = make_scheduler([make_job('data_audit')], clock, status_path)
    spawn(exits(1), exits(1), exits(1))
    asyncio.run(first.start_job(first.jobs[0]))
    asyncio.run(first.start_job(first.jobs[0]))

    second = make_scheduler([make_job('data_audit')], clock, status_path)
    assert second.jobs[0].state['consecutive_failures'] == 2
    asyncio.run(second.start_job(second.jobs[0]))
    assert read_status(status_path)['jobs']['data_audit']['consecutive_failures'] == 3


def test_status_file_is_replaced_atomically(spawn, tmp_path, monkeypatch):
    status_path = str(tmp_path / 'pycron_status.json')
    scheduler = make_scheduler([make_job()], FakeClock(ny(2026, 11, 13, 7, 0)), status_path)
    replaced = []
    real_replace = os.replace

    def spy(src, dst):
        # The complete document is already on disk under the temporary name.
        with open(src) as fh:
            replaced.append((json.load(fh), dst))
        real_replace(src, dst)

    monkeypatch.setattr(mod.os, 'replace', spy)
    scheduler.save_status()
    assert [dst for _, dst in replaced] == [status_path]
    assert replaced[0][0]['version'] == mod.STATUS_VERSION
    assert os.listdir(tmp_path) == ['pycron_status.json']  # no temporary file left behind


def test_unwritable_status_file_is_reported_once_and_never_stops_a_job(spawn, tmp_path, caplog):
    blocker = tmp_path / 'not_a_directory'
    blocker.write_text('x')
    job = make_job()
    scheduler = make_scheduler([job], FakeClock(ny(2026, 11, 13, 7, 0)),
                               str(blocker / 'pycron_status.json'))
    spawn(exits(0), exits(0))
    with caplog.at_level(logging.INFO, logger='pycron'):
        assert asyncio.run(scheduler.start_job(job)) == ''
        asyncio.run(scheduler.start_job(job))
    complaints = [r for r in caplog.records if 'cannot write status file' in r.getMessage()]
    assert len(complaints) == 1 and complaints[0].levelno == logging.ERROR
    assert job.state['run_count'] == 2 and job.state['last_outcome'] == 'success'


def test_corrupt_status_file_starts_clean(tmp_path, caplog):
    status_path = tmp_path / 'pycron_status.json'
    status_path.write_text('{"jobs": {"audit": {"last_sched')
    with caplog.at_level(logging.WARNING, logger='pycron'):
        scheduler = make_scheduler([make_job()], FakeClock(ny(2026, 11, 13, 7, 0)), str(status_path))
    assert any('unreadable' in r.getMessage() for r in caplog.records)
    assert scheduler.jobs[0].state == mod.JOB_STATE_DEFAULTS


def test_hand_edited_status_values_fall_back_to_defaults(tmp_path):
    status_path = tmp_path / 'pycron_status.json'
    status_path.write_text(json.dumps({'jobs': {'audit': {
        'start': '0 7 * * 1-5', 'timezone': 'America/New_York',
        'consecutive_failures': 'many', 'last_return_code': True, 'last_scheduled': 12,
        'last_output_tail': None, 'missed_count': -3}}}))
    state = make_scheduler([make_job()], FakeClock(ny(2026, 11, 13, 7, 0)), str(status_path)).jobs[0].state
    assert state['consecutive_failures'] == 0 and state['missed_count'] == 0
    assert state['last_return_code'] is None and state['last_scheduled'] is None
    assert state['last_output_tail'] == ''


def test_default_status_path_follows_mmr_log_dir(monkeypatch, tmp_path):
    monkeypatch.setenv('MMR_LOG_DIR', str(tmp_path / 'elsewhere'))
    assert default_status_path() == str(tmp_path / 'elsewhere' / 'pycron_status.json')
    monkeypatch.delenv('MMR_LOG_DIR')
    assert default_status_path() == os.path.expanduser('~/.local/share/mmr/logs/pycron_status.json')


def test_scheduler_without_a_status_path_writes_nothing(spawn, tmp_path, monkeypatch):
    monkeypatch.setenv('MMR_LOG_DIR', str(tmp_path))
    job = make_job()
    scheduler = JobScheduler([job])
    spawn(exits(0))
    asyncio.run(scheduler.start_job(job))
    assert os.listdir(tmp_path) == []


def test_status_page_carries_the_same_fields_as_the_file(spawn, tmp_path):
    status_path = str(tmp_path / 'pycron_status.json')
    job = make_job('data_audit', catch_up_minutes=240, timeout_minutes=60)
    scheduler = make_scheduler([job], FakeClock(ny(2026, 11, 13, 7, 0)), status_path)
    spawn(exits(3, b'bad\n'))
    asyncio.run(scheduler.start_job(job))

    page = json.loads(json.dumps([j.__dict__() for j in scheduler.jobs]))[0]
    entry = read_status(status_path)['jobs']['data_audit']
    for field in mod.JOB_STATE_DEFAULTS:
        assert page[field] == entry[field], field
    assert page['last_return_code'] == 3 and page['consecutive_failures'] == 1
    assert page['catch_up_minutes'] == 240 and page['timeout_minutes'] == 60
    # the keys the page always had are still there
    assert page['name'] == 'data_audit' and page['running'] is False and 'start_count' in page


def test_status_tail_is_bounded_and_blanks_credentials():
    stdout = '\n'.join(f'line {i}' for i in range(500))
    stderr = 'GET https://api.example.com/v1?symbol=AAPL&apikey=abc123&interval=1min failed'
    tail = status_tail(stdout, stderr)
    assert 'line 499' in tail and 'line 100' not in tail
    assert 'abc123' not in tail and 'apikey=<redacted>&interval=1min' in tail
    assert len(status_tail('x' * 100000, '')) <= mod.STATUS_TAIL_CHARS + 10
    assert redact_secrets('TWELVEDATA_API_KEY=s3cr3t ok') == 'TWELVEDATA_API_KEY=<redacted> ok'
    assert redact_secrets('{"token": "abc"}') == '{"token": "<redacted>"}'
    assert redact_secrets('no credentials here: 42') == 'no credentials here: 42'


# ── 2. missed runs ───────────────────────────────────────────────────────────

def test_instant_missed_while_down_is_caught_up_once(tmp_path, caplog):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 9, 6, 59, 30))  # Monday
    job = make_job('data_audit', catch_up_minutes=240)
    scheduler = make_scheduler([job], clock, status_path)
    started = record_starts(scheduler, clock)
    poll_at(scheduler, clock, [ny(2026, 11, 9, 6, 59, 30)])   # Monday's run fires on time
    assert [name for name, _ in started] == ['data_audit']
    assert read_status(status_path)['jobs']['data_audit']['last_scheduled'] == '2026-11-09T12:00:00+00:00'

    # Stack down across Tuesday 07:00; back at 09:10, 130 minutes late.
    clock.now = ny(2026, 11, 10, 9, 10)
    restarted = make_scheduler([make_job('data_audit', catch_up_minutes=240)], clock, status_path)
    restarted_starts = record_starts(restarted, clock)
    with caplog.at_level(logging.WARNING, logger='pycron'):
        poll_at(restarted, clock, [ny(2026, 11, 10, 9, 10), ny(2026, 11, 10, 9, 11),
                                   ny(2026, 11, 10, 9, 12)])
    assert restarted_starts == [('data_audit', ny(2026, 11, 10, 9, 10))]
    catch_up = [r for r in caplog.records if 'CATCH-UP' in r.getMessage()]
    assert len(catch_up) == 1 and 'data_audit' in catch_up[0].getMessage()
    entry = read_status(status_path)['jobs']['data_audit']
    assert entry['last_scheduled'] == '2026-11-10T12:00:00+00:00'
    assert entry['last_scheduled_action'] == 'catch_up'

    # A second restart inside the window does not run it again.
    clock.now = ny(2026, 11, 10, 9, 30)
    again = make_scheduler([make_job('data_audit', catch_up_minutes=240)], clock, status_path)
    again_starts = record_starts(again, clock)
    poll_at(again, clock, [ny(2026, 11, 10, 9, 30), ny(2026, 11, 10, 9, 31)])
    assert again_starts == []


def test_instant_outside_the_window_is_reported_missed_once(tmp_path, caplog):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 9, 6, 59, 30))
    scheduler = make_scheduler([make_job('data_audit', catch_up_minutes=240)], clock, status_path)
    record_starts(scheduler, clock)
    poll_at(scheduler, clock, [clock.now])

    clock.now = ny(2026, 11, 10, 11, 30)  # 4.5 h after Tuesday's 07:00
    restarted = make_scheduler([make_job('data_audit', catch_up_minutes=240)], clock, status_path)
    started = record_starts(restarted, clock)
    with caplog.at_level(logging.WARNING, logger='pycron'):
        poll_at(restarted, clock, [ny(2026, 11, 10, 11, 30), ny(2026, 11, 10, 11, 31)])
    assert started == []
    missed = [r for r in caplog.records if 'MISSED' in r.getMessage()]
    assert len(missed) == 1 and missed[0].levelno == logging.WARNING
    entry = read_status(status_path)['jobs']['data_audit']
    assert entry['last_missed'] == '2026-11-10T12:00:00+00:00'
    assert entry['missed_count'] == 1
    assert entry['last_scheduled_action'] == 'missed'

    # Not reported again by the next scheduler instance either.
    caplog.clear()
    clock.now = ny(2026, 11, 10, 12, 0)
    third = make_scheduler([make_job('data_audit', catch_up_minutes=240)], clock, status_path)
    record_starts(third, clock)
    with caplog.at_level(logging.WARNING, logger='pycron'):
        poll_at(third, clock, [clock.now])
    assert not [r for r in caplog.records if 'MISSED' in r.getMessage()]
    assert read_status(status_path)['jobs']['data_audit']['missed_count'] == 1


def test_default_is_no_catch_up_but_the_miss_is_recorded(tmp_path, caplog):
    """catch_up_minutes unset: a late job is never run (as before), and now says so."""
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 9, 8, 59, 30))
    scheduler = make_scheduler([make_job('preflight_us', '0 9 * * 1-5')], clock, status_path)
    record_starts(scheduler, clock)
    poll_at(scheduler, clock, [clock.now])

    clock.now = ny(2026, 11, 10, 9, 0, 45)  # 45 s after the instant
    restarted = make_scheduler([make_job('preflight_us', '0 9 * * 1-5')], clock, status_path)
    started = record_starts(restarted, clock)
    with caplog.at_level(logging.WARNING, logger='pycron'):
        poll_at(restarted, clock, [clock.now])
    assert started == []
    assert [r for r in caplog.records if 'MISSED job preflight_us' in r.getMessage()]


def test_first_ever_start_runs_nothing_and_reports_nothing(tmp_path, caplog):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 10, 9, 10))
    scheduler = make_scheduler([make_job('data_audit', catch_up_minutes=240)], clock, status_path)
    started = record_starts(scheduler, clock)
    with caplog.at_level(logging.WARNING, logger='pycron'):
        poll_at(scheduler, clock, [clock.now, ny(2026, 11, 10, 9, 11)])
    assert started == []
    assert not [r for r in caplog.records if r.name == 'pycron' and r.levelno >= logging.WARNING]
    entry = read_status(status_path)['jobs']['data_audit']
    assert entry['last_scheduled'] == '2026-11-10T12:00:00+00:00'
    assert entry['last_scheduled_action'] == 'baseline'


def test_edited_schedule_is_not_caught_up(tmp_path):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 9, 22, 16, 30))
    scheduler = make_scheduler([make_job('db_backup', '17 22 * * *', catch_up_minutes=240)],
                               clock, status_path)
    record_starts(scheduler, clock)
    poll_at(scheduler, clock, [clock.now])

    # Moved to 21:00; pycron restarted at 21:30 the next day. 21:00 "today" is
    # later than the stored instant, but it was never on the old schedule.
    clock.now = ny(2026, 11, 10, 21, 30)
    restarted = make_scheduler([make_job('db_backup', '0 21 * * *', catch_up_minutes=240)],
                               clock, status_path)
    started = record_starts(restarted, clock)
    poll_at(restarted, clock, [clock.now])
    assert started == []
    assert restarted.jobs[0].state['last_scheduled_action'] == 'baseline'


def test_on_time_run_is_not_caught_up_again_after_the_instant():
    clock = FakeClock(ny(2026, 11, 10, 6, 58, 20))
    scheduler = make_scheduler([make_job('data_audit', catch_up_minutes=240)], clock)
    started = record_starts(scheduler, clock)
    poll_at(scheduler, clock, every_minute(ny(2026, 11, 10, 6, 58), ny(2026, 11, 10, 7, 30)))
    assert started == [('data_audit', ny(2026, 11, 10, 6, 59, 20))]


def test_catch_up_waits_for_the_settle_period(tmp_path):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 9, 6, 59, 30))
    scheduler = make_scheduler([make_job('data_audit', catch_up_minutes=240)], clock, status_path)
    record_starts(scheduler, clock)
    poll_at(scheduler, clock, [clock.now])

    clock.now = ny(2026, 11, 10, 9, 10)
    restarted = make_scheduler([make_job('data_audit', catch_up_minutes=240)], clock, status_path)
    restarted.catch_up_settle_s = 300.0
    started = record_starts(restarted, clock)
    poll_at(restarted, clock, every_minute(ny(2026, 11, 10, 9, 10), ny(2026, 11, 10, 9, 20), 0))
    # Nothing in the first five minutes after the start, then exactly once.
    assert started == [('data_audit', ny(2026, 11, 10, 9, 15))]


def test_gap_in_polling_is_treated_like_a_restart(caplog):
    """Host asleep with pycron alive: the instant passes with no pass to fire it."""
    clock = FakeClock(ny(2026, 11, 9, 22, 0, 20))
    scheduler = make_scheduler([make_job('data_refresh_us', '0 23 * * 1-5', catch_up_minutes=240)], clock)
    scheduler.catch_up_settle_s = 120.0
    started = record_starts(scheduler, clock)
    with caplog.at_level(logging.WARNING, logger='pycron'):
        poll_at(scheduler, clock, list(every_minute(ny(2026, 11, 9, 22, 0), ny(2026, 11, 9, 22, 5)))
                + list(every_minute(ny(2026, 11, 10, 1, 0), ny(2026, 11, 10, 1, 5))))
    assert any('no scheduling pass ran' in r.getMessage() for r in caplog.records)
    assert started == [('data_refresh_us', ny(2026, 11, 10, 1, 2, 20))]


def test_catch_ups_run_one_at_a_time_oldest_instant_first(spawn, tmp_path):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 9, 12, 0))
    specs = [('data_audit', '0 7 * * 1-5'), ('data_refresh_asx', '0 6 * * 1-5'),
             ('db_backup', '17 22 * * *'), ('data_refresh_us', '0 23 * * 1-5')]

    def jobs():
        return [make_job(name, start, catch_up_minutes=720) for name, start in specs]

    first = make_scheduler(jobs(), clock, status_path)
    record_starts(first, clock)
    poll_at(first, clock, [clock.now])                     # baseline on Monday noon

    clock.now = ny(2026, 11, 10, 8, 0)                     # down all night
    scheduler = make_scheduler(jobs(), clock, status_path)
    spawner = spawn()                                      # every process keeps running

    async def go():
        scheduler.polling_loop()
        order = []
        for _ in range(len(specs)):
            for _ in range(20):
                await asyncio.sleep(0)
            assert len(spawner.processes) == len(order) + 1  # exactly one running
            order.append(spawner.calls[-1][0].split()[-1])
            scheduler.polling_loop()                         # a pass while it runs: no extra start
            for _ in range(20):
                await asyncio.sleep(0)
            assert len(spawner.processes) == len(order)
            spawner.processes[-1].finish(0)
        for _ in range(20):
            await asyncio.sleep(0)
        return order

    assert asyncio.run(go()) == ['db_backup', 'data_refresh_us', 'data_refresh_asx', 'data_audit']
    assert not any(job.catch_up_pending for job in scheduler.jobs)


def test_running_job_at_its_instant_is_skipped_not_missed(spawn, caplog):
    clock = FakeClock(ny(2026, 11, 10, 6, 58, 20))
    job = make_job('data_audit', '*/5 * * * *', catch_up_minutes=30)
    scheduler = make_scheduler([job], clock)
    spawner = spawn()   # the first run never finishes within the test window

    async def go():
        for instant in every_minute(ny(2026, 11, 10, 6, 58), ny(2026, 11, 10, 7, 12)):
            clock.now = instant
            scheduler.polling_loop()
            for _ in range(10):
                await asyncio.sleep(0)
        spawner.processes[0].finish(0)
        for _ in range(10):
            await asyncio.sleep(0)
        # Idle again between instants: the two it sat through are not owed.
        clock.now = ny(2026, 11, 10, 7, 13, 20)
        scheduler.polling_loop()
        for _ in range(10):
            await asyncio.sleep(0)

    with caplog.at_level(logging.WARNING, logger='pycron'):
        asyncio.run(go())
    assert len(spawner.calls) == 1
    skipped = [r.getMessage() for r in caplog.records if 'SKIPPED' in r.getMessage()]
    assert len(skipped) == 2    # 07:05 and 07:10
    assert not [r for r in caplog.records if 'MISSED' in r.getMessage() or 'CATCH-UP' in r.getMessage()]


def test_service_jobs_make_no_missed_noise(spawn, tmp_path, caplog):
    """A `* * * * *` keep-alive job passes an instant every minute by design."""
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 10, 7, 0, 20))
    spec = dict(start='* * * * *', start_on_pycron_start=True, restart_if_finished=True)
    scheduler = make_scheduler([make_job('trader_service', **spec)], clock, status_path)
    spawner = spawn()

    async def go():
        for instant in every_minute(ny(2026, 11, 10, 7, 0), ny(2026, 11, 10, 7, 20)):
            clock.now = instant
            scheduler.polling_loop()
            for _ in range(10):
                await asyncio.sleep(0)

    with caplog.at_level(logging.WARNING, logger='pycron'):
        asyncio.run(go())
        # Down for three hours, then a new instance.
        clock.now = ny(2026, 11, 10, 10, 30)
        restarted = make_scheduler([make_job('trader_service', **spec)], clock, status_path)
        record_starts(restarted, clock)
        poll_at(restarted, clock, [clock.now, ny(2026, 11, 10, 10, 31)])
    assert len(spawner.calls) == 1
    noise = [r.getMessage() for r in caplog.records
             if any(word in r.getMessage() for word in ('MISSED', 'CATCH-UP', 'SKIPPED'))]
    assert noise == []


def test_job_with_a_stop_schedule_is_never_caught_up(tmp_path, caplog):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 9, 12, 0))
    spec = dict(start='0 9 * * 1-5', stop='0 17 * * 1-5', catch_up_minutes=600)
    first = make_scheduler([make_job('windowed', **spec)], clock, status_path)
    record_starts(first, clock)
    poll_at(first, clock, [clock.now])

    clock.now = ny(2026, 11, 10, 18, 0)   # after Tuesday's stop
    restarted = make_scheduler([make_job('windowed', **spec)], clock, status_path)
    started = record_starts(restarted, clock)
    with caplog.at_level(logging.WARNING, logger='pycron'):
        poll_at(restarted, clock, [clock.now])
    assert started == []
    assert [r for r in caplog.records if 'MISSED job windowed' in r.getMessage()]


def test_one_job_failing_to_evaluate_does_not_stop_the_others(caplog):
    """An exception escaping a pass disposes reactivex's periodic timer for good."""
    clock = FakeClock(ny(2026, 11, 10, 6, 59, 30))
    broken = make_job('broken', '0 7 * * 1-5', eval=['1 / 0'])
    scheduler = make_scheduler([broken, make_job('data_audit')], clock)
    started = record_starts(scheduler, clock)
    with caplog.at_level(logging.ERROR, logger='pycron'):
        poll_at(scheduler, clock, [clock.now])
    assert [name for name, _ in started] == ['data_audit']
    assert any('scheduling pass failed for job broken' in r.getMessage() for r in caplog.records)
    assert scheduler.last_poll == clock.now


# ── 2b. daylight saving ──────────────────────────────────────────────────────

def _runs_over(job, first, last, offset_seconds, status_path=None):
    clock = FakeClock(first)
    scheduler = make_scheduler([job], clock, status_path)
    started = record_starts(scheduler, clock)
    poll_at(scheduler, clock, every_minute(first, last, offset_seconds))
    return started, job


@pytest.mark.parametrize('offset_seconds', [0, 20, 59])
@pytest.mark.parametrize('start', ['30 1 * * *', '0 1 * * *', '59 1 * * *', '0 2 * * *',
                                   '30 0 * * *', '0 3 * * *'])
def test_repeated_hour_at_the_end_of_dst_fires_once(start, offset_seconds):
    # 2026-11-01: 02:00 EDT -> 01:00 EST, so 01:00-01:59 happens twice.
    # ('59 1' with a pass landing exactly on the second 01:00:00 is the case the
    # old "within one polling period" rule alone would have fired twice.)
    first = dt.datetime(2026, 11, 1, 4, 15, tzinfo=UTC)     # 00:15 EDT
    last = dt.datetime(2026, 11, 1, 8, 15, tzinfo=UTC)      # 03:15 EST
    started, job = _runs_over(make_job('nightly', start, catch_up_minutes=180),
                              first, last, offset_seconds)
    assert len(started) == 1, started
    assert job.state['missed_count'] == 0


@pytest.mark.parametrize('offset_seconds', [0, 20, 59])
def test_restart_inside_the_repeated_hour_does_not_fire_again(tmp_path, offset_seconds):
    status_path = str(tmp_path / 'pycron_status.json')
    first = dt.datetime(2026, 11, 1, 5, 0, tzinfo=UTC)      # 01:00 EDT, first pass
    started, _ = _runs_over(make_job('nightly', '30 1 * * *', catch_up_minutes=180),
                            first, dt.datetime(2026, 11, 1, 5, 45, tzinfo=UTC),
                            offset_seconds, status_path)
    assert len(started) == 1
    # New instance at 01:10 EST (second pass), running through 03:00 EST.
    started, _ = _runs_over(make_job('nightly', '30 1 * * *', catch_up_minutes=180),
                            dt.datetime(2026, 11, 1, 6, 10, tzinfo=UTC),
                            dt.datetime(2026, 11, 1, 8, 0, tzinfo=UTC),
                            offset_seconds, status_path)
    assert started == []


def test_run_spanning_the_repeated_hour_is_not_followed_by_a_second_one(spawn):
    """While the job is busy the schedule's "latest instant" steps BACK during the
    second pass of the hour; the accounting must not follow it backwards."""
    clock = FakeClock(dt.datetime(2026, 11, 1, 5, 0, 20, tzinfo=UTC))       # 01:00 EDT
    job = make_job('nightly', '30 1 * * *', catch_up_minutes=180)
    scheduler = make_scheduler([job], clock)
    spawner = spawn(None)

    async def go():
        # Fires at 01:29 EDT and is still running at 01:10 EST...
        for instant in every_minute(dt.datetime(2026, 11, 1, 5, 0, tzinfo=UTC),
                                    dt.datetime(2026, 11, 1, 6, 12, tzinfo=UTC)):
            clock.now = instant
            scheduler.polling_loop()
            for _ in range(10):
                await asyncio.sleep(0)
        spawner.processes[0].finish(0)
        # ...then idle through the second 01:30 and beyond.
        for instant in every_minute(dt.datetime(2026, 11, 1, 6, 13, tzinfo=UTC),
                                    dt.datetime(2026, 11, 1, 8, 0, tzinfo=UTC)):
            clock.now = instant
            scheduler.polling_loop()
            for _ in range(10):
                await asyncio.sleep(0)

    asyncio.run(go())
    assert len(spawner.calls) == 1
    assert job.state['last_scheduled'] == '2026-11-01T05:30:00+00:00'


@pytest.mark.parametrize('offset_seconds', [0, 20, 59])
def test_skipped_hour_at_the_start_of_dst(offset_seconds):
    # 2026-03-08: 02:00 EST -> 03:00 EDT, so 02:30 never appears on the clock.
    first = dt.datetime(2026, 3, 8, 6, 0, tzinfo=UTC)       # 01:00 EST
    last = dt.datetime(2026, 3, 8, 8, 30, tzinfo=UTC)       # 04:30 EDT
    started, job = _runs_over(make_job('nightly', '30 2 * * *', catch_up_minutes=120),
                              first, last, offset_seconds)
    assert len(started) == 1 and job.state['missed_count'] == 0
    # ...at the instant the skipped wall time maps to (03:30 EDT), 24 h after
    # the previous day's run, not the moment the clock jumps.
    mapped = dt.datetime(2026, 3, 8, 7, 30, tzinfo=UTC)
    assert mapped <= started[0][1] < mapped + dt.timedelta(seconds=60)
    # Without catch-up it is not run that day (it never was) — and is reported.
    started, job = _runs_over(make_job('nightly', '30 2 * * *'), first, last, offset_seconds)
    assert started == [] and job.state['missed_count'] == 1
    # A job outside the skipped hour is unaffected.
    for start in ('0 2 * * *', '30 1 * * *', '0 3 * * *'):
        started, job = _runs_over(make_job('nightly', start, catch_up_minutes=120),
                                  first, last, offset_seconds)
        assert len(started) == 1 and job.state['missed_count'] == 0, start


@pytest.mark.parametrize('transition', ['2026-03-08', '2026-11-01'])
def test_whole_transition_day_runs_each_daily_job_once(transition):
    """The 23- and 25-hour days, midnight to midnight New York time."""
    day = dt.date.fromisoformat(transition)
    first = dt.datetime.combine(day, dt.time(0, 0), tzinfo=NY).astimezone(UTC)
    last = dt.datetime.combine(day + dt.timedelta(days=1), dt.time(0, 0), tzinfo=NY).astimezone(UTC)
    assert (last - first) in (dt.timedelta(hours=23), dt.timedelta(hours=25))
    clock = FakeClock(first)
    jobs = [make_job(name, start, catch_up_minutes=240)
            for name, start in (('refresh', '0 23 * * *'), ('audit', '0 7 * * *'), ('backup', '17 22 * * *'))]
    scheduler = make_scheduler(jobs, clock)
    started = record_starts(scheduler, clock)
    poll_at(scheduler, clock, every_minute(first, last, 37))
    assert sorted(name for name, _ in started) == ['audit', 'backup', 'refresh']
    assert all(job.state['missed_count'] == 0 for job in jobs)


# ── 3. timeout ───────────────────────────────────────────────────────────────

def test_timeout_terminates_the_group_and_records_a_failure(spawn, tmp_path, monkeypatch, caplog):
    status_path = str(tmp_path / 'pycron_status.json')
    job = make_job('data_refresh_us', '0 23 * * 1-5', timeout_minutes=0.001)   # 60 ms
    scheduler = make_scheduler([job], FakeClock(ny(2026, 11, 10, 23, 0)), status_path)
    scheduler.timeout_kill_grace = 0.05
    spawner = spawn(lambda process: process.stdout.feed_data(b'fetching AAPL\n'))
    signals = []

    def fake_killpg(pgid, sig):
        signals.append((pgid, sig))
        if sig == signal.SIGKILL:           # this job ignores SIGTERM
            spawner.processes[0].finish(-signal.SIGKILL)

    monkeypatch.setattr(mod.os, 'killpg', fake_killpg)
    with caplog.at_level(logging.INFO, logger='pycron'):
        asyncio.run(asyncio.wait_for(scheduler.start_job(job), timeout=5))

    pid = spawner.processes[0].pid
    assert signals == [(pid, signal.SIGTERM), (pid, signal.SIGKILL)]
    assert spawner.calls[0][1].get('start_new_session') is True
    assert not scheduler.is_running(job) and job.process is None and job.finished
    entry = read_status(status_path)['jobs']['data_refresh_us']
    assert entry['last_outcome'] == 'timeout'
    assert entry['consecutive_failures'] == 1
    assert entry['last_return_code'] == -signal.SIGKILL
    assert 'timeout_minutes' in entry['last_error']
    assert 'fetching AAPL' in entry['last_output_tail']     # what it printed before hanging
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert any('exceeded timeout_minutes' in message for message in errors)
    assert any('FAILED' in message for message in errors)


def test_timed_out_job_fires_again_at_its_next_instant(spawn, monkeypatch):
    clock = FakeClock(ny(2026, 11, 10, 22, 59, 30))
    job = make_job('data_refresh_us', '0 23 * * 1-5', timeout_minutes=0.001)
    scheduler = make_scheduler([job], clock)
    scheduler.timeout_kill_grace = 0.05
    spawner = spawn(None, exits(0))
    monkeypatch.setattr(mod.os, 'killpg',
                        lambda pgid, sig: spawner.processes[0].finish(-sig))

    async def go():
        scheduler.polling_loop()
        await asyncio.sleep(0.3)
        assert not scheduler.is_running(job)
        clock.now = ny(2026, 11, 11, 22, 59, 30)
        scheduler.polling_loop()
        await asyncio.sleep(0.05)

    asyncio.run(go())
    assert len(spawner.calls) == 2
    assert job.state['last_outcome'] == 'success' and job.state['consecutive_failures'] == 0


def test_job_that_keeps_a_pipe_open_after_the_kill_still_ends(spawn, monkeypatch):
    """A descendant outside the group holding stdout must not keep the job 'running'."""
    job = make_job('data_refresh_us', timeout_minutes=0.001)
    scheduler = make_scheduler([job], FakeClock(ny(2026, 11, 10, 23, 0)))
    scheduler.timeout_kill_grace = 0.05
    spawner = spawn(None)

    def fake_killpg(pgid, sig):
        process = spawner.processes[0]
        process.returncode = -sig          # the process dies; its pipes never reach EOF
        process._exited.set()

    monkeypatch.setattr(mod.os, 'killpg', fake_killpg)
    asyncio.run(asyncio.wait_for(scheduler.start_job(job), timeout=5))
    assert not scheduler.is_running(job) and job.state['last_outcome'] == 'timeout'


def test_job_without_a_timeout_is_spawned_as_before(spawn, monkeypatch):
    job = make_job('trader_service', '* * * * *')
    scheduler = make_scheduler([job], FakeClock(ny(2026, 11, 10, 23, 0)))
    spawner = spawn(exits(0))
    monkeypatch.setattr(mod.os, 'killpg', lambda *a: pytest.fail('no timeout configured'))
    asyncio.run(scheduler.start_job(job))
    command, kwargs = spawner.calls[0]
    assert command == 'true trader_service'
    assert set(kwargs) == {'stdout', 'stderr', 'cwd'}


@pytest.mark.skipif(sys.platform == 'win32', reason='process groups')
def test_timeout_kills_the_whole_process_group_for_real(tmp_path):
    pid_file = tmp_path / 'child.pid'
    # A shell that starts a background child, then both sleep far past the limit.
    job = Job({'name': 'hung', 'command': 'sleep 300 & echo $! >',
               'arguments': f'{pid_file}; sleep 300', 'start': '0 7 * * *',
               'timeout_minutes': 0.01})                                   # 0.6 s
    scheduler = JobScheduler([job])
    scheduler.timeout_kill_grace = 2.0
    began = time.monotonic()
    asyncio.run(asyncio.wait_for(scheduler.start_job(job), timeout=20))
    assert time.monotonic() - began < 10
    assert job.state['last_outcome'] == 'timeout' and job.return_code == -signal.SIGTERM
    assert not scheduler.is_running(job)

    child = int(pid_file.read_text())
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            if psutil.Process(child).status() == psutil.STATUS_ZOMBIE:
                break
        except psutil.NoSuchProcess:
            break
        time.sleep(0.05)
    else:
        pytest.fail(f'background child {child} survived the timeout')


@pytest.mark.skipif(sys.platform == 'win32', reason='process groups')
def test_job_ignoring_sigterm_is_sigkilled_for_real():
    # The ignored signal is inherited across exec, so there is no window in
    # which SIGTERM could still kill it.
    job = Job({'name': 'stubborn', 'command': "trap '' TERM; echo up;",
               'arguments': 'exec sleep 300',
               'start': '0 7 * * *', 'timeout_minutes': 0.01})             # 0.6 s
    scheduler = JobScheduler([job])
    scheduler.timeout_kill_grace = 0.5
    asyncio.run(asyncio.wait_for(scheduler.start_job(job), timeout=20))
    assert job.state['last_outcome'] == 'timeout'
    assert job.return_code == -signal.SIGKILL
    assert job.state['last_output_tail'] == 'up'


# ── 4. a failed spawn does not disable the job ───────────────────────────────

def test_failed_spawn_is_retried_within_the_firing(spawn, caplog):
    job = make_job()
    scheduler = make_scheduler([job], FakeClock(ny(2026, 11, 10, 7, 0)))
    spawner = spawn(OSError('fork: Resource temporarily unavailable'), exits(0, b'ok\n'))
    with caplog.at_level(logging.WARNING, logger='pycron'):
        assert asyncio.run(scheduler.start_job(job)) == 'ok'
    assert len(spawner.calls) == 2 and job.trying_to_start is False
    assert job.state['last_outcome'] == 'success'
    assert any('launch attempt 1/3 failed' in r.getMessage() for r in caplog.records)


def test_spawn_that_keeps_failing_is_recorded_and_the_next_firing_still_runs(spawn, tmp_path, caplog):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 10, 7, 0))
    job = make_job('data_audit')
    scheduler = make_scheduler([job], clock, status_path)
    spawner = spawn(OSError('no'), OSError('no'), OSError('no'), exits(0))

    with caplog.at_level(logging.ERROR, logger='pycron'):
        assert asyncio.run(scheduler.start_job(job)) == ''
    assert len(spawner.calls) == mod.SPAWN_TRIES
    assert job.trying_to_start is False          # the flag that used to stick
    assert not scheduler.is_running(job)
    assert any('could not be started' in r.getMessage() for r in caplog.records)
    entry = read_status(status_path)['jobs']['data_audit']
    assert entry['last_outcome'] == 'spawn_failed' and entry['consecutive_failures'] == 1
    assert entry['last_return_code'] is None and 'OSError' in entry['last_error']

    clock.now = ny(2026, 11, 11, 7, 0)
    asyncio.run(scheduler.start_job(job))
    assert len(spawner.calls) == mod.SPAWN_TRIES + 1
    assert read_status(status_path)['jobs']['data_audit']['last_outcome'] == 'success'


def test_failed_spawn_does_not_block_the_next_scheduled_instant(spawn):
    clock = FakeClock(ny(2026, 11, 10, 6, 59, 30))
    job = make_job('data_audit')
    scheduler = make_scheduler([job], clock)
    spawner = spawn(OSError('no'), OSError('no'), OSError('no'), exits(0))

    async def go():
        scheduler.polling_loop()
        await asyncio.sleep(0.05)
        clock.now = ny(2026, 11, 11, 6, 59, 30)
        scheduler.polling_loop()
        await asyncio.sleep(0.05)

    asyncio.run(go())
    assert len(spawner.calls) == 4 and job.state['last_outcome'] == 'success'


def test_second_start_while_launching_is_still_refused(spawn):
    """The guard keeps its purpose: no double launch while one is in flight."""
    job = make_job('strategy_service', delay=0.05)
    scheduler = make_scheduler([job], FakeClock(ny(2026, 11, 10, 7, 0)))
    spawner = spawn(exits(0), exits(0))

    async def go():
        first = asyncio.ensure_future(scheduler.start_job(job))
        await asyncio.sleep(0.01)
        assert job.trying_to_start is True
        assert await scheduler.start_job(job) is None
        await first

    asyncio.run(go())
    assert len(spawner.calls) == 1


def test_missing_dependency_does_not_leave_the_flag_set(spawn):
    job = make_job('strategy_service', depends_on=['no_such_job'])
    scheduler = make_scheduler([job], FakeClock(ny(2026, 11, 10, 7, 0)))
    spawner = spawn(exits(0))
    assert asyncio.run(scheduler.start_job(job)) == ''
    assert job.trying_to_start is False and spawner.calls == []


# ── scheduler-initiated stops, restarts, adopted processes ───────────────────

def test_stop_by_the_scheduler_is_not_a_failure(spawn, monkeypatch, caplog):
    job = make_job('windowed', '0 9 * * 1-5')
    scheduler = make_scheduler([job], FakeClock(ny(2026, 11, 10, 9, 0)))
    spawner = spawn(None)

    class Killable:
        def __init__(self, pid):
            self.pid = pid

        def children(self, recursive=False):
            return []

        def kill(self):
            spawner.processes[0].finish(-signal.SIGKILL)

    monkeypatch.setattr(mod.psutil, 'Process', Killable)

    async def go():
        run = asyncio.ensure_future(scheduler.start_job(job))
        await asyncio.sleep(0.01)
        scheduler.stop_job(job)
        await run

    with caplog.at_level(logging.ERROR, logger='pycron'):
        asyncio.run(go())
    assert job.state['last_outcome'] == 'stopped' and job.state['consecutive_failures'] == 0
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_run_in_progress_when_the_scheduler_died_is_recorded_interrupted(spawn, tmp_path, monkeypatch, caplog):
    status_path = str(tmp_path / 'pycron_status.json')
    clock = FakeClock(ny(2026, 11, 10, 22, 59, 30))
    first = make_scheduler([make_job('data_refresh_us', '0 23 * * 1-5', catch_up_minutes=240)],
                           clock, status_path)
    spawn(None)

    async def go():
        first.polling_loop()
        await asyncio.sleep(0.01)

    asyncio.run(go())
    assert read_status(status_path)['jobs']['data_refresh_us']['running'] is True

    monkeypatch.setattr(mod.psutil, 'process_iter', lambda: [])   # the process is gone
    clock.now = ny(2026, 11, 10, 23, 20)
    restarted = make_scheduler([make_job('data_refresh_us', '0 23 * * 1-5', catch_up_minutes=240)],
                               clock, status_path, first_start=True)
    started = record_starts(restarted, clock)
    with caplog.at_level(logging.WARNING, logger='pycron'):
        poll_at(restarted, clock, [clock.now])
    assert started == []          # that instant already ran: never twice
    entry = read_status(status_path)['jobs']['data_refresh_us']
    assert entry['last_outcome'] == 'interrupted' and entry['consecutive_failures'] == 1
    assert any('interrupted' in r.getMessage() for r in caplog.records)


@pytest.fixture
def sleeper():
    import subprocess
    process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    yield process
    if process.poll() is None:
        process.kill()
        process.wait()


def _adopting_scheduler(job, clock, monkeypatch, process, status_path=None):
    """A scheduler whose first pass finds ``process`` and takes it for ``job``'s."""
    scheduler = make_scheduler([job], clock, status_path, first_start=True)
    found = psutil.Process(process.pid)
    monkeypatch.setattr(mod.psutil, 'process_iter', lambda: [found])
    monkeypatch.setattr(scheduler, 'compare_process_name', lambda proc, name, args: True)
    return scheduler


def _status_naming(path, name, pid, **entry):
    """A status file in which the previous instance left ``name`` running as ``pid``."""
    entry = {'start': '0 7 * * 1-5', 'timezone': 'America/New_York', 'running': True, 'pid': pid,
             'last_start': '2026-11-10T10:58:00+00:00', **entry}
    path.write_text(json.dumps({'version': 1, 'jobs': {name: entry}}))
    return str(path)


def test_adopted_process_is_noticed_when_it_exits(tmp_path, monkeypatch, sleeper):
    """A process found at startup used to count as running for ever."""
    clock = FakeClock(ny(2026, 11, 10, 6, 0))
    job = make_job('data_audit')
    scheduler = _adopting_scheduler(job, clock, monkeypatch, sleeper,
                                    _status_naming(tmp_path / 's.json', 'data_audit', sleeper.pid))
    started = record_starts(scheduler, clock)

    poll_at(scheduler, clock, [clock.now])
    assert scheduler.is_running(job) and job.pid == sleeper.pid
    assert job.state['last_outcome'] is None          # adopted, so not "interrupted"

    sleeper.kill()
    sleeper.wait()
    poll_at(scheduler, clock, [ny(2026, 11, 10, 6, 1)])
    assert not scheduler.is_running(job) and job.process is None
    assert job.state['last_outcome'] == 'unknown' and job.state['consecutive_failures'] == 0

    poll_at(scheduler, clock, [ny(2026, 11, 10, 6, 59, 30)])
    assert [name for name, _ in started] == ['data_audit']    # and it fires again


def test_recorded_run_that_outlives_its_timeout_is_killed_after_a_restart(tmp_path, monkeypatch, sleeper):
    clock = FakeClock(dt.datetime.now(UTC) + dt.timedelta(minutes=10))     # 10 min after it started
    job = make_job('data_audit', timeout_minutes=5)
    scheduler = _adopting_scheduler(job, clock, monkeypatch, sleeper,
                                    _status_naming(tmp_path / 's.json', 'data_audit', sleeper.pid))
    record_starts(scheduler, clock)
    poll_at(scheduler, clock, [clock.now])
    assert sleeper.wait(timeout=5) == -signal.SIGKILL
    assert not scheduler.is_running(job)
    assert job.state['last_outcome'] == 'timeout' and job.state['consecutive_failures'] == 1


def test_look_alike_process_is_never_killed(tmp_path, monkeypatch, sleeper, caplog):
    """Adoption matches on the command line alone. A process this scheduler did
    not start (the same command run by hand) is reported, not killed."""
    clock = FakeClock(dt.datetime.now(UTC) + dt.timedelta(minutes=10))
    job = make_job('data_audit', timeout_minutes=5)
    # The status file names a different pid (or there is no status file at all).
    scheduler = _adopting_scheduler(job, clock, monkeypatch, sleeper,
                                    _status_naming(tmp_path / 's.json', 'data_audit', sleeper.pid + 1))
    record_starts(scheduler, clock)
    with caplog.at_level(logging.ERROR, logger='pycron'):
        poll_at(scheduler, clock, [clock.now, clock.now + dt.timedelta(minutes=1),
                                   clock.now + dt.timedelta(minutes=2)])
    assert sleeper.poll() is None                      # still alive
    assert scheduler.is_running(job)
    reports = [r for r in caplog.records if 'NOT killing it' in r.getMessage()]
    assert len(reports) == 1

    sleeper.kill()
    sleeper.wait()
    poll_at(scheduler, clock, [clock.now + dt.timedelta(minutes=3)])
    assert not scheduler.is_running(job)
    assert job.state['last_outcome'] != 'unknown'      # nothing is claimed about a stranger's run


def test_process_matching_reads_cmdline_once_and_survives_inaccessible_processes():
    scheduler = JobScheduler()

    class Proc:
        def __init__(self, cmdline):
            self._cmdline = cmdline

        def name(self):
            return 'python3'

        def cmdline(self):
            if self._cmdline is None:
                raise psutil.AccessDenied()
            return self._cmdline

        def as_dict(self):
            raise AssertionError('as_dict() collects every attribute of every process')

    wanted = '-m trader.mmr_cli data audit'
    assert scheduler.compare_process_name(Proc(['python3'] + wanted.split(' ')), 'python3', wanted)
    assert scheduler.compare_process_name(Proc(['/bin/sh', '-c', 'python3 ' + wanted]), 'python3', wanted)
    assert not scheduler.compare_process_name(Proc(['python3', '-m', 'web.app']), 'python3', wanted)
    assert not scheduler.compare_process_name(Proc(None), 'python3', wanted)


# ── configuration ────────────────────────────────────────────────────────────

@pytest.mark.parametrize('key', ['catch_up_minutes', 'timeout_minutes'])
@pytest.mark.parametrize('bad', [-1, 'soon', True, float('nan'), float('inf'), [5]])
def test_malformed_minutes_refuse_to_load(key, bad):
    with pytest.raises(ValueError, match=key):
        make_job(**{key: bad})


def test_new_keys_are_optional_and_default_to_the_old_behaviour():
    job = Job({'name': 'j', 'command': 'true', 'start': '0 9 * * 1-5'})
    assert job.catch_up_minutes == 0 and job.timeout_minutes == 0
    assert job.catch_up_seconds == 0 and job.timeout_seconds == 0
    assert make_job(catch_up_minutes=None, timeout_minutes='').timeout_minutes == 0


def test_template_opts_the_right_jobs_in():
    with open('config_defaults/pycron.yaml') as fh:
        config = yaml.safe_load(fh)
    jobs = {spec['name']: Job(spec, default_timezone=config['timezone']) for spec in config['jobs']}

    for name in ('data_refresh_us', 'data_refresh_asx', 'data_audit', 'db_backup'):
        assert 60 <= jobs[name].catch_up_minutes <= 12 * 60, name
        assert jobs[name].timeout_minutes >= 60, name
    for name in ('preflight_us', 'preflight_asx'):
        # A preflight run hours late says nothing about readiness at the open.
        assert jobs[name].catch_up_minutes == 0, name
        assert 0 < jobs[name].timeout_minutes <= 15, name
    for name in ('data_service', 'trader_service', 'strategy_service', 'web_dashboard'):
        assert jobs[name].timeout_minutes == 0, name
        assert jobs[name].catch_up_minutes == 0, name
    # A timeout shorter than the gap between runs, so a hung run cannot swallow the next one.
    for name, job in jobs.items():
        assert job.timeout_minutes < 24 * 60, name
