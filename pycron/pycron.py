from asyncio import Task
from asyncio.subprocess import Process
from crontab import CronTab
from reactivex.scheduler.eventloop.asynciothreadsafescheduler import AsyncIOThreadSafeScheduler
from rich.logging import RichHandler
from tornado.platform.asyncio import AsyncIOMainLoop
from tornado.web import Application, RequestHandler, url
from typing import Any, Callable, cast, Dict, List, Optional, Tuple

import asyncio
import click
import collections
import datetime as dt
import json
import logging
import os
import psutil
import re
import signal
import socket
import sys
import yaml


log = logging.getLogger('pycron')


TAIL_LINES = 200
TAIL_LINE_BYTES = 4096

# ── run status ───────────────────────────────────────────────────────────────
# The scheduler's own log (pycron_cron.log) is read by no monitor: the nightly
# data_audit job exited non-zero 46 nights running and nothing reported it.
# Every job's last run is therefore also kept in one small JSON file, rewritten
# atomically on each poll, that scripts/monitor_health.sh (via
# scripts/pycron_status.py) and the status page read.
STATUS_FILE_NAME = 'pycron_status.json'
STATUS_VERSION = 1
# Bound on the output kept per job in the status file / status page (the full
# 200-line tail still goes to the log).
STATUS_TAIL_LINES = 20
STATUS_TAIL_CHARS = 4000

# Launch attempts for one firing (the first plus two retries), and the pause
# before each retry.
SPAWN_TRIES = 3
SPAWN_RETRY_DELAYS = (1.0, 2.0)

# After a job exceeds `timeout_minutes` its process group gets SIGTERM, then
# SIGKILL this many seconds later.
TIMEOUT_KILL_GRACE_S = 10.0

# Catch-up runs are not started until the scheduler has been polling this long
# after a start or a gap in polling (host sleep). A catch-up fired on the first
# poll would run a data refresh or a database backup while the trading services
# launched alongside pycron are still connecting and priming history.
CATCH_UP_SETTLE_S = 300.0

# Per-job fields that survive a scheduler restart (persisted in the status file).
JOB_STATE_DEFAULTS: Dict[str, Any] = {
    # Most recent scheduled instant that has been accounted for — fired, caught
    # up, reported missed, or skipped because the job was busy. An instant at or
    # before this one is never run (again).
    'last_scheduled': None,
    'last_scheduled_action': None,
    'last_start': None,
    'last_end': None,
    'last_duration_s': None,
    'last_return_code': None,
    # success | failed | timeout | spawn_failed | stopped | interrupted | unknown
    'last_outcome': None,
    'last_error': None,
    'last_success': None,
    'consecutive_failures': 0,
    'last_output_tail': '',
    'last_missed': None,
    'missed_count': 0,
    'run_count': 0,
}

_SECRET_RE = re.compile(
    r'(?i)((?:api[_-]?key|token|secret|passw(?:or)?d)\w*["\']?\s*[=:]\s*["\']?)([^\s&"\',;]+)')


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _iso(value: Optional[dt.datetime]) -> Optional[str]:
    """Aware datetime -> ISO-8601 UTC string at whole seconds (None passes through)."""
    if value is None:
        return None
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()


def _parse_iso(value: Any) -> Optional[dt.datetime]:
    """ISO-8601 string with an offset -> aware datetime; anything else -> None."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def default_status_path() -> str:
    """``$MMR_LOG_DIR/pycron_status.json`` (default ``~/.local/share/mmr/logs``).

    Same override rule as ``trader/common/logging_helper.py``; not imported
    from there because that module configures logging on import.
    """
    log_dir = os.path.abspath(
        os.path.expanduser(os.environ.get('MMR_LOG_DIR') or '~/.local/share/mmr/logs'))
    return os.path.join(log_dir, STATUS_FILE_NAME)


def redact_secrets(text: str) -> str:
    """Blank values that look like credentials (``apikey=…``, ``token: …``).

    Best effort: job output can quote a vendor URL carrying an API key, and the
    status page is published on every host interface.
    """
    return _SECRET_RE.sub(r'\1<redacted>', text)


def status_tail(stdout_text: str, stderr_text: str,
                max_lines: int = STATUS_TAIL_LINES, max_chars: int = STATUS_TAIL_CHARS) -> str:
    """The last few lines of a run's output, bounded and with secrets blanked."""
    parts: List[str] = []
    if stdout_text:
        parts.append('\n'.join(stdout_text.splitlines()[-max_lines:]))
    if stderr_text:
        parts.append('[stderr]\n' + '\n'.join(stderr_text.splitlines()[-max_lines:]))
    text = redact_secrets('\n'.join(parts))
    if len(text) > max_chars:
        text = '[...]' + text[-max_chars:]
    return text


class OutputTail():
    """Bounded tail of one output stream.

    Kept on an object (not returned by the coroutine) so the lines read so far
    survive when the drain is abandoned, as it is when a job times out.
    """

    def __init__(self, max_lines: int = TAIL_LINES, max_line_bytes: int = TAIL_LINE_BYTES):
        self.max_lines = max_lines
        self.max_line_bytes = max_line_bytes
        self.lines: collections.deque = collections.deque(maxlen=max_lines)
        self.dropped = 0

    async def drain(self, stream: Optional[asyncio.StreamReader]) -> None:
        if stream is None:
            return
        while True:
            try:
                line = await stream.readline()
            except ValueError:
                # Line longer than the reader's limit: consume what's buffered.
                line = await stream.read(self.max_line_bytes)
            if not line:
                break
            if len(self.lines) == self.max_lines:
                self.dropped += 1
            self.lines.append(line[:self.max_line_bytes].decode(errors='replace').rstrip('\n'))

    def text(self) -> str:
        prefix = f'[... {self.dropped} earlier lines dropped ...]\n' if self.dropped else ''
        return prefix + '\n'.join(self.lines)


async def drain_tail(stream: Optional[asyncio.StreamReader],
                     max_lines: int = TAIL_LINES, max_line_bytes: int = TAIL_LINE_BYTES) -> str:
    """Read ``stream`` to EOF keeping only the last ``max_lines`` lines.

    Memory is bounded by ``max_lines * max_line_bytes`` no matter how much the
    child writes; over-long lines are truncated (the pipe is still drained so
    the child never blocks on a full pipe).
    """
    tail = OutputTail(max_lines, max_line_bytes)
    await tail.drain(stream)
    return tail.text()


class ZonedCronTab(CronTab):
    """CronTab whose schedule is evaluated in a fixed IANA timezone.

    Plain CronTab evaluates against the host's local clock, so a schedule
    written in ET silently shifts when the container runs in PT. With a zone
    set, ``next()`` (called without ``now``) measures from the current time in
    that zone, so "0 9 * * 1-5" means 09:00 in that zone, DST included.
    ``tz=None`` keeps the legacy local-time behaviour.
    """

    def __init__(self, spec: str, tz: Optional[str] = None):
        super().__init__(spec)
        self.tz_name = tz or None
        self.tz = resolve_timezone(tz)

    def next(self, now=None, *args, **kwargs):  # type: ignore[override]
        if now is None and self.tz is not None:
            now = dt.datetime.now(self.tz)
        return super().next(now, *args, **kwargs)


def resolve_timezone(tz: Optional[str]):
    """Return a ZoneInfo for ``tz`` (None/'' -> None). Unknown zones raise ValueError."""
    if not tz:
        return None
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        return ZoneInfo(str(tz))
    except (ZoneInfoNotFoundError, ValueError) as ex:
        raise ValueError(f'pycron: unknown timezone {tz!r}') from ex


class Job():
    def __init__(self, dictionary: Optional[Dict[str, str]], default_timezone: Optional[str] = None):
        self.name: str = ''
        self.description: str = ''
        self.command: str = ''
        self.arguments: str = ''
        self.working_directory: str = ''
        self.start: str = ''
        self.stop: str = ''
        self.trying_to_start = False
        self.finished: bool = False
        self.start_on_pycron_start: bool = False
        self.restart_if_found: bool = False
        self.restart_if_finished: bool = False
        self.start_count: int = 0
        self.running: bool = False
        self.pid: int = 0
        self.return_code: Optional[int] = None
        self.process: Optional[Process] = None
        self.eval: Optional[List[str]] = None
        self.eval_result: Optional[List[bool]] = None
        self.last_started: Optional[dt.datetime] = None
        self.depends_on: Optional[List[str]] = None
        self.delay: int = 0
        self.eval_running: Optional[List[str]] = None
        self.eval_running_result: Optional[List[bool]] = None
        # IANA zone the start/stop expressions are written in. None = the
        # process's local time (legacy behaviour).
        self.timezone: Optional[str] = default_timezone or None
        # Optional. A scheduled instant that passed without the job being run
        # (stack down, host asleep) is run once if it is at most this many
        # minutes old; older than that it is reported MISSED. 0 = never run late.
        self.catch_up_minutes: float = 0
        # Optional. Terminate the job's process group after this many minutes
        # and record a failure. 0 = no limit.
        self.timeout_minutes: float = 0

        if dictionary:
            for k, v in dictionary.items():
                setattr(self, k, v)

        self.catch_up_minutes = self._minutes('catch_up_minutes')
        self.timeout_minutes = self._minutes('timeout_minutes')

        self.crontab_start = ZonedCronTab(self.start, self.timezone)
        self.crontab_stop: Optional[CronTab] = None
        if self.stop:
            self.crontab_stop = ZonedCronTab(self.stop, self.timezone)

        # Run history (see JOB_STATE_DEFAULTS); restored from the status file.
        self.state: Dict[str, Any] = dict(JOB_STATE_DEFAULTS)
        # stop_job() was called for the current run: its exit is not a failure.
        self.stop_requested: bool = False
        # A catch-up run is queued or in progress.
        self.catch_up_pending: bool = False
        # The status file says the previous scheduler instance left it running,
        # as this pid.
        self.was_running: bool = False
        self.saved_pid: int = 0
        # The process adopted at startup is that recorded run (not merely a
        # process with the same command line).
        self.adopted_recorded: bool = False
        self.adopted_overdue_reported: bool = False

    def _minutes(self, key: str) -> float:
        value = getattr(self, key)
        if value is None or value == '':
            return 0.0
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or value != value or value < 0 or value == float('inf')):
            raise ValueError(
                f'pycron: job {self.name!r}: {key} must be a non-negative number of minutes, '
                f'got {value!r}')
        return float(value)

    @property
    def timeout_seconds(self) -> float:
        return self.timeout_minutes * 60.0

    @property
    def catch_up_seconds(self) -> float:
        # A job with a `stop` schedule runs for a window: starting it late could
        # start it after the window closed. Reported missed, never caught up.
        return 0.0 if self.crontab_stop else self.catch_up_minutes * 60.0

    def is_service(self) -> bool:
        """Kept running by the scheduler, as opposed to run at scheduled instants."""
        return bool(self.start_on_pycron_start or self.restart_if_finished)

    def restore_state(self, saved: Dict[str, Any]) -> None:
        """Adopt the run history a previous scheduler instance persisted.

        A value of the wrong type (a hand-edited file) falls back to its default.
        """
        def is_int(value: Any) -> bool:
            return isinstance(value, int) and not isinstance(value, bool)

        for key, default in JOB_STATE_DEFAULTS.items():
            value = saved.get(key, default)
            if key in ('consecutive_failures', 'missed_count', 'run_count'):
                valid = is_int(value) and value >= 0
            elif key == 'last_return_code':
                valid = value is None or is_int(value)
            elif key == 'last_duration_s':
                valid = value is None or is_int(value) or isinstance(value, float)
            elif key == 'last_output_tail':
                valid = isinstance(value, str)
            else:
                valid = value is None or isinstance(value, str)
            self.state[key] = value if valid else default
        if saved.get('start') != self.start or (saved.get('timezone') or None) != self.timezone:
            # The schedule was edited. The stored instant belongs to the old
            # expression; comparing it with the new one would "catch up" a run
            # that was never scheduled.
            self.state['last_scheduled'] = None
            self.state['last_scheduled_action'] = None
        self.was_running = bool(saved.get('running'))
        pid = saved.get('pid')
        self.saved_pid = pid if self.was_running and is_int(pid) and pid > 0 else 0

    def status_dict(self) -> Dict[str, Any]:
        """This job's entry in the status file."""
        status: Dict[str, Any] = {
            'start': self.start,
            'stop': self.stop,
            'timezone': self.timezone,
            'catch_up_minutes': self.catch_up_minutes,
            'timeout_minutes': self.timeout_minutes,
            'running': bool(self.running),
            'pid': self.pid,
        }
        status.update(self.state)
        return status

    def __str__(self):
        return '{} {} {} {} {}: {} {}'.format(
            self.name,
            self.command,
            self.arguments,
            self.pid,
            self.start_count,
            self.start,
            self.stop
        )

    def __dict__(self):
        result = {
            'name': self.name,
            'description': self.description,
            'command': self.command,
            'arguments': self.arguments,
            'working_directory': self.working_directory,
            'start': self.start,
            'stop': self.stop,
            'timezone': self.timezone,
            'trying_to_start': self.trying_to_start,
            'start_on_pycron_start': self.start_on_pycron_start,
            'running': self.running,
            'restart_if_found': self.restart_if_found,
            'restart_if_finished': self.restart_if_finished,
            'start_count': self.start_count,
            'finished': self.finished,
            'pid': self.pid,
            'eval': self.eval,
            'eval_result': self.eval_result,
            'depends_on': self.depends_on,
            'delay': self.delay,
            'eval_running': self.eval_running,
            'eval_running_result': self.eval_running_result,
            'catch_up_minutes': self.catch_up_minutes,
            'timeout_minutes': self.timeout_minutes,
        }
        # The run history, under the same names as in the status file.
        result.update(self.state)
        return result


class StatusStore():
    """Atomic JSON persistence for the status file. ``path=None`` keeps nothing on disk."""

    def __init__(self, path: Optional[str]):
        self.path = path
        self._write_failed = False

    def load(self) -> Dict[str, Any]:
        if not self.path or not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, 'r') as fh:
                data = json.load(fh)
            if not isinstance(data, dict):
                raise ValueError('top level is not an object')
            return data
        except (OSError, ValueError) as ex:
            # No history means no instant can be shown to have been missed, so
            # nothing is caught up on this start — the conservative direction.
            log.warning('status file {} is unreadable ({}: {}); starting with no run history, '
                        'so nothing is caught up on this start'.format(self.path, type(ex).__name__, ex))
            return {}

    def save(self, payload: Dict[str, Any]) -> bool:
        if not self.path:
            return True
        tmp = '{}.{}.tmp'.format(self.path, os.getpid())
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(tmp, 'w') as fh:
                json.dump(payload, fh, indent=2, sort_keys=True)
                fh.write('\n')
                fh.flush()
                os.fsync(fh.fileno())
            # rename is atomic: a reader sees the old file or the new one,
            # never a half-written one.
            os.replace(tmp, self.path)
        except (OSError, TypeError, ValueError) as ex:
            if not self._write_failed:
                log.error('cannot write status file {} ({}: {}); job failures will not reach '
                          'the monitors until this is fixed'.format(self.path, type(ex).__name__, ex))
            self._write_failed = True
            try:
                os.unlink(tmp)
            except OSError:
                pass
            return False
        if self._write_failed:
            log.info('status file {} is writable again'.format(self.path))
            self._write_failed = False
        return True


class JobScheduler():
    def __init__(self, jobs_list: Optional[List[Job]] = None, health_check_eval: Optional[List[str]] = None,
                 status_path: Optional[str] = None,
                 clock: Optional[Callable[[], dt.datetime]] = None):
        self.jobs: List[Job] = []
        self.scheduler: Optional[AsyncIOThreadSafeScheduler] = None
        self.first_start = True
        self.polling_period = 60.0
        self.health_check_eval = health_check_eval
        # Aware UTC "now". Injected by tests; every schedule decision in a poll
        # is taken against one reading of it.
        self.clock: Callable[[], dt.datetime] = clock or utc_now
        self.spawn_retry_delays: Tuple[float, ...] = SPAWN_RETRY_DELAYS
        self.timeout_kill_grace = TIMEOUT_KILL_GRACE_S
        self.catch_up_settle_s = CATCH_UP_SETTLE_S
        # status_path=None keeps the run history in memory only (library use,
        # tests); main() passes the real file.
        self.status_store = StatusStore(status_path)
        previous = self.status_store.load()
        saved_jobs = previous.get('jobs')
        self._saved_jobs: Dict[str, Any] = saved_jobs if isinstance(saved_jobs, dict) else {}
        saved_scheduler = previous.get('scheduler')
        self.previous_last_poll: Optional[dt.datetime] = _parse_iso(
            saved_scheduler.get('last_poll') if isinstance(saved_scheduler, dict) else None)
        self.started_at: dt.datetime = self.clock()
        self.last_poll: Optional[dt.datetime] = None
        # When polling (re)started: scheduler start, or the first poll after a gap.
        self.resumed_at: Optional[dt.datetime] = None
        # One catch-up run at a time, oldest scheduled instant first.
        self._catch_up_lock = asyncio.Lock()
        if jobs_list:
            self.schedule_jobs(jobs_list)

    def health_check(self):
        eval_list = []
        # single element
        if type(self.health_check_eval) is str:
            self.health_check_eval = [self.health_check_eval]  # type: ignore

        # multiple lines
        if self.health_check_eval:
            eval_results = []
            for e in self.health_check_eval:
                eval_results.append(eval(e))
            return all(eval_results)
        # nothing to do
        else:
            return True

    def get_job(self, name: str) -> Optional[Job]:
        job = None
        for j in self.jobs:
            if j.name == name:
                job = j
        return job

    def get_jobs(self, names: List[str]) -> List[Job]:
        jobs = []
        for j in names:
            if self.get_job(j):
                jobs.append(cast(Job, self.get_job(j)))
        return jobs

    def is_running(self, job: Job) -> bool:
        if job.eval_running:
            if type(job.eval_running) is str:
                job.eval_running = [job.eval_running]  # type: ignore
            if job.eval_running:
                eval_running_results = []
                for e in job.eval_running:
                    eval_running_results.append(eval(e))
                job.eval_running_result = eval_running_results
                return all(job.eval_running_result)
            else:
                return True
        else:
            return job.running and not job.finished

    # ── status file ──────────────────────────────────────────────────────────

    def status_payload(self) -> Dict[str, Any]:
        return {
            'version': STATUS_VERSION,
            'scheduler': {
                'pid': os.getpid(),
                'started': _iso(self.started_at),
                # Heartbeat: a value that stops advancing means nothing is being
                # scheduled, whatever the process table says.
                'last_poll': _iso(self.last_poll),
                'polling_period_s': self.polling_period,
            },
            'jobs': {job.name: job.status_dict() for job in self.jobs},
        }

    def save_status(self) -> None:
        """Persist the run history. Never raises: bookkeeping must not stop a job."""
        try:
            self.status_store.save(self.status_payload())
        except Exception:
            log.exception('writing the status file failed')

    def _record_start(self, job: Job) -> None:
        job.state['last_start'] = _iso(self.clock())
        job.state['run_count'] = int(job.state.get('run_count') or 0) + 1
        self.save_status()

    def _record_end(self, job: Job, outcome: str, return_code: Optional[int],
                    error: Optional[str], output_tail: Optional[str] = None) -> None:
        """Record how a run ended. ``stopped`` and ``unknown`` are neither a
        success nor a failure and leave the failure streak alone."""
        end = self.clock()
        state = job.state
        state['last_end'] = _iso(end)
        started = _parse_iso(state.get('last_start'))
        # Only a run this scheduler watched end has a meaningful duration.
        state['last_duration_s'] = (
            round((end - started).total_seconds(), 1)
            if started is not None and outcome in ('success', 'failed', 'timeout', 'stopped')
            else None)
        state['last_return_code'] = return_code
        state['last_outcome'] = outcome
        state['last_error'] = error
        if output_tail is not None:
            state['last_output_tail'] = output_tail
        if outcome == 'success':
            state['last_success'] = state['last_end']
            state['consecutive_failures'] = 0
        elif outcome in ('failed', 'timeout', 'spawn_failed', 'interrupted'):
            state['consecutive_failures'] = int(state.get('consecutive_failures') or 0) + 1
        self.save_status()

    # ── running a job ────────────────────────────────────────────────────────

    async def start_job(self, job: Job):
        if job.trying_to_start:
            return

        job.trying_to_start = True
        try:
            process = await self._launch(job)
        except Exception as ex:
            log.error('job {} could not be started: {}: {}'.format(job.name, type(ex).__name__, ex),
                      exc_info=True)
            try:
                job.state['last_start'] = _iso(self.clock())
                self._record_end(job, 'spawn_failed', None,
                                 'could not be started: {}: {}'.format(type(ex).__name__, ex), '')
            except Exception:
                log.exception('recording the failed start of {} failed'.format(job.name))
            return ''
        finally:
            # Whatever happened, the next firing must be able to try again. This
            # used to be reset only after a successful spawn, so ONE failed
            # spawn left it True and every later firing returned at the guard
            # above: the job was disabled until pycron restarted.
            job.trying_to_start = False

        if process is None:
            try:
                job.state['last_start'] = _iso(self.clock())
                self._record_end(job, 'spawn_failed', None,
                                 'a job it depends on is not configured: {}'.format(job.depends_on), '')
            except Exception:
                log.exception('recording the failed start of {} failed'.format(job.name))
            return ''
        return await self._run_to_completion(job, process)

    async def _launch(self, job: Job) -> Optional[Process]:
        """Wait for dependencies and the start delay, then spawn. None = not started."""
        if job.depends_on:
            log.info('trying to start job: {} that depends on: {}'.format(job, str(job.depends_on)))
        else:
            log.info('trying to start job: {}'.format(job))

        cwd = job.working_directory or os.getcwd()
        command = '{} {}'.format(job.command, job.arguments)

        if job.depends_on and not len(job.depends_on) == len(self.get_jobs(job.depends_on)):
            log.error('cannot find all dependent jobs {} for {}, aborting'.format(job.depends_on, job))
            return None

        while job.depends_on and [j for j in self.get_jobs(job.depends_on) if not self.is_running(j)]:  # type: ignore
            # wait until it's running
            # todo do some backoff/exception stuff here
            await asyncio.sleep(5)

        # delay start (default is 0)
        await asyncio.sleep(job.delay)

        process = await self._spawn(job, command, cwd)
        job.process = process
        job.running = True
        job.finished = False
        job.pid = process.pid
        job.last_started = dt.datetime.now()
        job.stop_requested = False

        stopping_debug = ''
        if job.crontab_stop:
            stopping_debug = 'stopping job in {} secs'.format(
                self._seconds_until(job.crontab_stop, self.clock()))
        log.info('started job: {} {} {} {}'.format(job.command, job.arguments, str(process.pid), stopping_debug))
        try:
            self._record_start(job)
        except Exception:
            log.exception('recording the start of {} failed'.format(job.name))
        return process

    async def _spawn(self, job: Job, command: str, cwd: str) -> Process:
        kwargs: Dict[str, Any] = {}
        if job.timeout_seconds > 0:
            # Own session (hence own process group), so a timeout can signal
            # the job AND everything it started, and nothing else. Jobs without
            # a timeout are spawned exactly as before.
            kwargs['start_new_session'] = True
        attempt = 1
        while True:
            try:
                return await asyncio.create_subprocess_shell(
                    command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                    cwd=cwd, **kwargs
                )
            except Exception as ex:
                if attempt >= SPAWN_TRIES:
                    raise
                delay = self.spawn_retry_delays[min(attempt - 1, len(self.spawn_retry_delays) - 1)] \
                    if self.spawn_retry_delays else 0.0
                log.warning('job {}: launch attempt {}/{} failed ({}: {}); retrying in {}s'.format(
                    job.name, attempt, SPAWN_TRIES, type(ex).__name__, ex, delay))
                attempt += 1
                await asyncio.sleep(delay)

    async def _run_to_completion(self, job: Job, process: Process):
        # Stream both pipes into bounded tails instead of communicate():
        # communicate() buffers a long-running service's ENTIRE output in
        # memory until it exits (days, for trader_service), so pycron's RSS
        # grew without bound. Only the tail is ever reported.
        out_tail, err_tail = OutputTail(), OutputTail()

        async def wait_for_exit():
            await asyncio.gather(out_tail.drain(process.stdout), err_tail.drain(process.stderr))
            await process.wait()

        timed_out = False
        timeout = job.timeout_seconds
        if timeout > 0:
            waiter = asyncio.ensure_future(wait_for_exit())
            done, _ = await asyncio.wait({waiter}, timeout=timeout)
            if done:
                waiter.result()
            else:
                # A hung job used to stay "running" for ever, and a running job
                # is never started again.
                timed_out = True
                log.error('job {} exceeded timeout_minutes={:g} (pid {}); terminating its process group'.format(
                    job.name, job.timeout_minutes, process.pid))
                await self._terminate_group(job, process)
                # The pipes reach EOF once every holder is dead. A process that
                # left the group and kept a pipe open must not hold the job
                # "running": stop waiting after the same grace.
                done, _ = await asyncio.wait({waiter}, timeout=self.timeout_kill_grace)
                if done:
                    waiter.result()
                else:
                    waiter.cancel()
                    await asyncio.gather(waiter, return_exceptions=True)
        else:
            await wait_for_exit()

        result = out_tail.text().strip()
        stderr_text = err_tail.text().strip()
        stopped = job.stop_requested

        job.return_code = process.returncode
        job.running = False
        job.process = None
        job.pid = 0
        job.finished = True
        job.start_count += 1
        job.stop_requested = False

        if timed_out:
            outcome = 'timeout'
            error: Optional[str] = 'exceeded timeout_minutes={:g}; process group terminated'.format(
                job.timeout_minutes)
        elif stopped:
            outcome, error = 'stopped', 'stopped by the scheduler'
        elif job.return_code == 0:
            outcome, error = 'success', None
        else:
            outcome, error = 'failed', 'exit code {}'.format(job.return_code)

        if outcome in ('success', 'stopped'):
            log.info('job finished ret: [{}] {} stdout: {} stderr: {}'.format(job.return_code,
                                                                              job.command,
                                                                              result,
                                                                              stderr_text))
        else:
            # A non-zero exit was logged at INFO, in a file no monitor reads.
            log.error('job {} FAILED ({}) ret: [{}] {} {} stdout: {} stderr: {}'.format(
                job.name, error, job.return_code, job.command, job.arguments, result, stderr_text))
        try:
            self._record_end(job, outcome, job.return_code, error, status_tail(result, stderr_text))
        except Exception:
            log.exception('recording the result of {} failed'.format(job.name))
        return result

    def _signal_group(self, pgid: int, sig: int, quiet: bool = False) -> bool:
        """Signal a process group. False = nothing was signalled."""
        try:
            os.killpg(pgid, sig)
            return True
        except ProcessLookupError:
            return False
        except PermissionError as ex:
            # macOS answers EPERM, not ESRCH, for a group holding only zombies.
            if not quiet:
                log.error('cannot signal process group {}: {}'.format(pgid, ex))
            return False

    async def _terminate_group(self, job: Job, process: Process) -> None:
        # The job was started in its own session, so its pid is its group id.
        pgid = process.pid
        self._signal_group(pgid, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=self.timeout_kill_grace)
        except asyncio.TimeoutError:
            log.error('job {} (pid {}) still running {:g}s after SIGTERM; sending SIGKILL'.format(
                job.name, pgid, self.timeout_kill_grace))
        # Always: a child of the job can outlive it by ignoring SIGTERM. Once
        # the job itself is gone an empty group is the expected answer.
        self._signal_group(pgid, signal.SIGKILL, quiet=process.returncode is not None)

    # todo make this better
    def restart_job(self, job: Job):
        self.stop_job(job)
        task = [asyncio.create_task(self.start_job(job))]
        asyncio.gather(*task)

    def compare_process_name(self, process: psutil.Process, name: str, arguments: str):
        def compare_list(a: List[str], b: List[str]):
            return [s.lower() for s in a] == [s.lower() for s in b]

        try:
            test_process_name = process.name()
            test_process_cmdline = process.cmdline()
        except (psutil.ZombieProcess, psutil.NoSuchProcess, psutil.AccessDenied):
            return False

        process_names: List[str] = []
        process_names.append(name.lower())

        # strip command from arguments. (This read process.as_dict()['cmdline'],
        # which collects EVERY attribute of every process on the first pass and
        # yields None — a TypeError here — for one it may not inspect.)
        test_process_arguments = test_process_cmdline[1:]
        args = arguments.split(' ')

        if os.path.exists(name):
            process_names.append(os.path.basename(name).lower())

        if '/bin/sh' in test_process_cmdline and '-c' in test_process_cmdline:
            # chew the /bin/sh -c
            test_process_name = test_process_cmdline[2]
            if ' ' in test_process_name:
                test_process_arguments = test_process_name.split(' ')[1:]
                test_process_name = test_process_name.split(' ')[0]

        for process_name in process_names:
            if process_name in test_process_name.lower() and compare_list(args, test_process_arguments):
                return True

        return False

    def stop_job(self, job: Job):
        def kill(proc_pid):
            try:
                process = psutil.Process(proc_pid)
                for proc in process.children(recursive=True):
                    proc.kill()
                process.kill()
                return True
            except Exception as ex:
                logging.error('kill() for {} failed with {}'.format(job, ex))
                return False

        def ps_kill(job: Job):
            try:
                for proc in psutil.process_iter():
                    if self.compare_process_name(proc, job.command, job.arguments):
                        log.debug('stop_job found process {}, kill()'.format(proc.name()))
                        return kill(proc.pid)
                return False
            except Exception as ex:
                logging.error('ps_kill() for {} failed with {}'.format(job, ex))

        # The exit that follows is the scheduler's doing, not a job failure.
        job.stop_requested = True

        if job.process:
            log.debug('stop_job process kill() {}'.format(job))
            if kill(job.process.pid):
                job.process = None
                job.running = False
                job.finished = True
                return

        log.debug('stop_job no process, trying to find using ps')
        if ps_kill(job):
            job.process = None
            job.running = False
            job.finished = True
            return
        log.error('was not able to kill {}'.format(job))

    # ── schedule arithmetic ──────────────────────────────────────────────────

    def _cron_now(self, cron: CronTab, now_utc: dt.datetime) -> dt.datetime:
        """``now_utc`` on the wall clock ``cron`` is written in."""
        tz = getattr(cron, 'tz', None)
        if tz is not None:
            return now_utc.astimezone(tz)
        # No zone configured: the process's local time, naive (legacy behaviour).
        return now_utc.astimezone().replace(tzinfo=None)

    def _seconds_until(self, cron: CronTab, now_utc: dt.datetime):
        """Seconds until ``cron`` next matches (what ``cron.next()`` returned before
        the clock was injectable)."""
        return cron.next(self._cron_now(cron, now_utc), default_utc=False)

    def _within_polling_period(self, cron: CronTab, now_utc: dt.datetime) -> bool:
        seconds = self._seconds_until(cron, now_utc)
        return seconds is not None and -self.polling_period <= seconds <= self.polling_period

    def _upcoming_instant(self, cron: CronTab, now_utc: dt.datetime) -> Optional[dt.datetime]:
        """The instant ``_seconds_until`` counts down to."""
        wall = cron.next(self._cron_now(cron, now_utc), default_utc=False, return_datetime=True)
        return None if wall is None else wall.astimezone(dt.timezone.utc)

    def _due_instant(self, cron: CronTab, now_utc: dt.datetime) -> Optional[dt.datetime]:
        """The most recent scheduled instant at or before ``now_utc``.

        The schedule is matched on the wall clock and the match converted to an
        instant with fold=0, so an hour repeated at the end of daylight saving
        yields ONE instant (its first pass) and during its second pass the
        answer steps back, never forward: comparing against the last accounted
        instant cannot fire twice. A wall time skipped at the start of daylight
        saving maps to an instant one hour later, which may still lie ahead of
        ``now_utc``; callers treat that as not yet due.
        """
        local = self._cron_now(cron, now_utc)
        # previous() excludes its argument when it carries no microseconds;
        # probing one second later makes "now" itself count.
        probe = local.replace(microsecond=0) + dt.timedelta(seconds=1)
        wall = cron.previous(probe, default_utc=False, return_datetime=True)
        return None if wall is None else wall.astimezone(dt.timezone.utc)

    def _last_scheduled(self, job: Job) -> Optional[dt.datetime]:
        return _parse_iso(job.state.get('last_scheduled'))

    def _consume(self, job: Job, instant: Optional[dt.datetime], action: str) -> bool:
        """Mark ``instant`` accounted for. Only ever moves forward."""
        if instant is None:
            return False
        last = self._last_scheduled(job)
        if last is not None and instant <= last:
            return False
        job.state['last_scheduled'] = _iso(instant)
        job.state['last_scheduled_action'] = action
        return True

    # ── the poll ─────────────────────────────────────────────────────────────

    def polling_loop(self):
        """One scheduler pass.

        Must not raise: reactivex's periodic timer disposes itself when its
        action throws, after which nothing is ever scheduled again while the
        process — and its status port — stay up. One job's failure to evaluate
        is logged and the other jobs carry on.
        """
        tasks: List[Task] = []
        try:
            now_utc = self.clock()
            self._note_poll(now_utc)
            catch_ups: List[Tuple[dt.datetime, Job]] = []
            for job in self.jobs:
                try:
                    self._poll_job(job, now_utc, tasks, catch_ups)
                except Exception:
                    log.exception('scheduling pass failed for job {}; other jobs are unaffected'.format(job.name))
            self.first_start = False
            # Oldest missed instant first: a restart after a night down replays
            # the night in order (refresh, then the audit that grades it)
            # instead of starting everything at once.
            for _, job in sorted(catch_ups, key=lambda item: item[0]):
                tasks.append(asyncio.create_task(self._run_catch_up(job)))
        except Exception:
            log.exception('scheduling pass failed')
        # wait on process start tasks
        if tasks:
            asyncio.gather(*tasks)
        self.save_status()

    def _note_poll(self, now_utc: dt.datetime) -> None:
        previous = self.last_poll
        self.last_poll = now_utc
        if previous is None:
            self.resumed_at = now_utc
            if self.previous_last_poll is not None:
                log.info('previous scheduler instance last polled at {} ({:.0f} min ago)'.format(
                    _iso(self.previous_last_poll),
                    (now_utc - self.previous_last_poll).total_seconds() / 60.0))
            return
        gap = (now_utc - previous).total_seconds()
        if gap > 3 * self.polling_period:
            log.warning('no scheduling pass ran for {:.0f} min ({} to {}): host asleep or event loop '
                        'blocked'.format(gap / 60.0, _iso(previous), _iso(now_utc)))
            self.resumed_at = now_utc

    def _recently_started_with_error(self, job: Job):
        return (
            job.return_code
            and job.return_code > 0
            and job.last_started
            # total_seconds(), not .seconds — the latter drops the days
            # component (it is 0..86399), so after 24h the restart-suppression
            # window wrapped and misbehaved.
            and (dt.datetime.now() - job.last_started).total_seconds() < self.polling_period * (job.start_count + 1)
        )

    def _test_eval(self, job: Job) -> bool:
        if type(job.eval) is str:
            job.eval = [job.eval]  # type: ignore
        if job.eval:
            eval_results = []
            for e in job.eval:
                eval_results.append(eval(e))
            job.eval_result = eval_results
            return all(job.eval_result)
        else:
            return True

    def _adopt_running_process(self, job: Job) -> bool:
        """First pass only: hook a job up to a process a previous instance started."""
        adopted = False
        for proc in psutil.process_iter():
            if self.compare_process_name(proc, job.command, job.arguments):
                log.debug('found existing process {}, for job {}'.format(proc.name(), job))
                job.process = proc  # type: ignore
                job.pid = proc.pid
                job.running = True
                adopted = True
                job.adopted_recorded = job.adopted_recorded or self._is_recorded_run(job, proc)

                if job.restart_if_found:
                    log.debug('restart_if_found is true, restarting {}'.format(job))
                    self.stop_job(job)
        return adopted

    def _is_recorded_run(self, job: Job, proc: psutil.Process) -> bool:
        """Is ``proc`` the run the status file says the previous instance started?

        Adoption matches on the command line alone, so it also picks up a
        look-alike this scheduler never started (the same command run by hand).
        The recorded pid is the shell's; the match may be the shell or its child.
        """
        if not job.saved_pid:
            return False
        if proc.pid == job.saved_pid:
            return True
        try:
            return proc.ppid() == job.saved_pid
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    def _check_adopted(self, job: Job, now_utc: dt.datetime) -> None:
        """Notice when an adopted process has gone, or has outlived its timeout.

        Nothing awaits an adopted process, so it used to count as running for
        ever once found, and its job was never started again. Only a process
        that is provably the recorded run is killed on timeout; a look-alike is
        reported and left alone.
        """
        proc = job.process
        if not isinstance(proc, psutil.Process):
            return
        alive = proc.is_running()
        if alive:
            try:
                alive = proc.status() != psutil.STATUS_ZOMBIE
            except (psutil.NoSuchProcess, psutil.ZombieProcess):
                alive = False
            except psutil.AccessDenied:
                pass
        if alive:
            if job.timeout_seconds > 0:
                try:
                    age = now_utc.timestamp() - proc.create_time()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    return
                if age > job.timeout_seconds and job.adopted_recorded:
                    log.error('job {} (pid {}, started by the previous scheduler instance) exceeded '
                              'timeout_minutes={:g}; killing it'.format(job.name, proc.pid, job.timeout_minutes))
                    self.stop_job(job)
                    job.stop_requested = False
                    if job.process is None:
                        job.pid = 0
                        self._record_end(job, 'timeout', None,
                                         'exceeded timeout_minutes={:g}; killed'.format(job.timeout_minutes))
                elif age > job.timeout_seconds and not job.adopted_overdue_reported:
                    job.adopted_overdue_reported = True
                    log.error('job {}: pid {} has the job\'s command line and has run longer than '
                              'timeout_minutes={:g}, but this scheduler did not start it; NOT killing it. '
                              'The job will not be started while it runs.'.format(
                                  job.name, proc.pid, job.timeout_minutes))
            return
        job.process = None
        job.running = False
        job.finished = True
        job.pid = 0
        if job.adopted_recorded:
            log.warning('job {}: process {} started by the previous scheduler instance has exited; '
                        'its exit status could not be observed'.format(job.name, proc.pid))
            self._record_end(job, 'unknown', None,
                             'ran under a previous scheduler instance; exit status not observed')
        else:
            log.info('job {}: look-alike process {} has exited'.format(job.name, proc.pid))

    def _poll_job(self, job: Job, now_utc: dt.datetime, tasks: List[Task],
                  catch_ups: List[Tuple[dt.datetime, Job]]) -> None:
        # check to see if the process is already running
        # and if so, hook it up to the job
        adopted = False
        if self.first_start and not job.running and not job.process:
            try:
                adopted = self._adopt_running_process(job)
            except Exception:
                # Not finding a leftover process must not cost the job its
                # first pass (which is the only one that starts
                # start_on_pycron_start jobs).
                log.exception('looking for an existing process of job {} failed'.format(job.name))
        self._check_adopted(job, now_utc)

        if self.first_start and job.was_running:
            job.was_running = False
            if not adopted and not job.running:
                log.warning('job {} was running (started {}) when the previous scheduler instance '
                            'stopped; outcome unknown, recorded as interrupted'.format(
                                job.name, job.state.get('last_start')))
                self._record_end(job, 'interrupted', None,
                                 'the scheduler stopped while this run was in progress; outcome unknown')

        due = self._due_instant(job.crontab_start, now_utc)
        if self._last_scheduled(job) is None and due is not None:
            # No history for this schedule (first run of this scheduler, a new
            # job, an edited expression): nothing can be shown to have been
            # missed, so start counting from here rather than run something.
            self._consume(job, due, 'baseline')

        # The existing rule: start a job at the pass that falls within one
        # polling period BEFORE its scheduled instant.
        early = self._within_polling_period(job.crontab_start, now_utc)
        upcoming = self._upcoming_instant(job.crontab_start, now_utc) if early else None
        # Target of this pass: the instant about to fire, else the latest past one.
        target = upcoming if upcoming is not None else due

        # check to see if there is an eval, and if so, evaluate it
        # if False, kill job if it exists, or skip
        if not self._test_eval(job):
            log.debug('eval for job {}, eval: {} was False'.format(job, job.eval))
            # check to see if the job is running, and if so, kill it
            if self.is_running(job):
                log.debug('eval for job {} failed, stopping job'.format(job))
                self.stop_job(job)
            self._consume(job, target, 'skipped_eval')
            return

        started = False
        stopped = False
        last = self._last_scheduled(job)
        # if we're firing up for the first time, schedule these jobs if
        # the job stop time is positive, or doesn't exist
        if (
            self.first_start and not self.is_running(job) and job.start_on_pycron_start
            and (not job.crontab_stop or (job.crontab_stop and self._seconds_until(job.crontab_stop, now_utc) > 0))  # type: ignore
        ):
            tasks.append(asyncio.create_task(self.start_job(job)))
            started = True
        # try stop the job, irregardless of if we know if it's started or not
        elif (
            self.is_running(job) and job.crontab_stop and self._within_polling_period(job.crontab_stop, now_utc)
        ):
            self.stop_job(job)
            stopped = True
        # restart finished job
        elif (
            not self.is_running(job)
            and job.restart_if_finished
            and job.finished
            and not self._recently_started_with_error(job)
            and job.crontab_stop
            and self._seconds_until(job.crontab_start, now_utc) < self._seconds_until(job.crontab_stop, now_utc)  # type: ignore
        ):
            logging.debug('restarting finished job')
            tasks.append(asyncio.create_task(self.start_job(job)))
            started = True
        # restart finished job
        elif (
            not self.is_running(job)
            and job.restart_if_finished
            and job.finished
            and not self._recently_started_with_error(job)
            and not job.crontab_stop
        ):
            logging.debug('restarting finished job, as no cron stop configured')
            tasks.append(asyncio.create_task(self.start_job(job)))
            started = True
        # start the job
        elif (
            not self.is_running(job) and early
            # ...unless that instant is already accounted for: never twice for
            # one scheduled instant.
            and not (upcoming is not None and last is not None and upcoming <= last)
            and not job.catch_up_pending
        ):
            tasks.append(asyncio.create_task(self.start_job(job)))
            started = True

        if started:
            self._consume(job, target, 'fired')
            return
        if stopped:
            self._consume(job, target, 'stopped')
            return
        if self.is_running(job) or job.trying_to_start or job.catch_up_pending:
            # Busy at its scheduled instant: that instant is skipped, as it
            # always was. Worth saying so for a job that runs on a schedule.
            if self._consume(job, target, 'skipped_running') and early and not job.is_service():
                log.warning('SKIPPED job {}: scheduled {} but its previous run (started {}) is still '
                            'in progress'.format(job.name, _iso(upcoming), job.state.get('last_start')))
            return
        self._account_missed(job, now_utc, due, catch_ups)

    def _account_missed(self, job: Job, now_utc: dt.datetime, due: Optional[dt.datetime],
                        catch_ups: List[Tuple[dt.datetime, Job]]) -> None:
        """The job is idle and nothing started it this pass: was an instant missed?"""
        last = self._last_scheduled(job)
        if due is None or last is None or due <= last:
            return
        late = (now_utc - due).total_seconds()
        if late < 0:
            # A wall time skipped by daylight saving: its instant is still ahead.
            return
        if self.resumed_at is not None and (now_utc - self.resumed_at).total_seconds() < self.catch_up_settle_s:
            # Too soon after a start or a wake to decide; see CATCH_UP_SETTLE_S.
            return
        if job.catch_up_seconds > 0 and late <= job.catch_up_seconds:
            log.warning('CATCH-UP job {}: scheduled {} did not run (last accounted {}); {:.0f} min late, '
                        'within catch_up_minutes={:g}; running it now'.format(
                            job.name, _iso(due), _iso(last), late / 60.0, job.catch_up_minutes))
            self._consume(job, due, 'catch_up')
            job.catch_up_pending = True
            catch_ups.append((due, job))
        else:
            log.warning('MISSED job {}: scheduled {} did not run (last accounted {}); {:.0f} min late, '
                        'catch_up_minutes={:g}; not running it'.format(
                            job.name, _iso(due), _iso(last), late / 60.0, job.catch_up_minutes))
            self._consume(job, due, 'missed')
            job.state['last_missed'] = _iso(due)
            job.state['missed_count'] = int(job.state.get('missed_count') or 0) + 1

    async def _run_catch_up(self, job: Job) -> None:
        try:
            async with self._catch_up_lock:
                # It may have waited here behind another catch-up for a long
                # time; start_job itself does not look at job.running.
                if self.is_running(job) or job.trying_to_start:
                    log.info('catch-up of job {} dropped: it is already running'.format(job.name))
                    return
                await self.start_job(job)
        finally:
            job.catch_up_pending = False

    def schedule_job(self, job: Job):
        log.debug('scheduling job "{}"'.format(job))
        saved = self._saved_jobs.get(job.name)
        if isinstance(saved, dict):
            job.restore_state(saved)
        if job.timeout_minutes > 0 and job.is_service():
            log.warning('job {}: timeout_minutes={:g} on a job the scheduler keeps running will kill '
                        'and restart it at that interval'.format(job.name, job.timeout_minutes))
        if job.catch_up_minutes > 0 and job.crontab_stop:
            log.warning('job {}: catch_up_minutes is ignored for a job with a stop schedule'.format(job.name))
        self.jobs.append(job)

    def schedule_jobs(self, jobs: List[Job]):
        for j in jobs:
            self.schedule_job(j)

    def start(self):
        log.debug('starting job scheduler with {} jobs'.format(len(self.jobs)))
        if self.scheduler is None:
            self.scheduler = AsyncIOThreadSafeScheduler(asyncio.get_event_loop())
        # start now
        self.scheduler.schedule(lambda x, y: self.polling_loop())
        # then schedule every 'n' periods
        self.scheduler.schedule_periodic(self.polling_period, lambda x: self.polling_loop())
        # schedule health check
        if self.health_check_eval:
            self.scheduler.schedule_periodic(self.polling_period * 10,
                                             lambda x: logging.info('health check: {}'.format(self.health_check())))


class MainHandler(RequestHandler):
    def initialize(self, job_scheduler: JobScheduler):
        self.job_scheduler = job_scheduler

    def prepare(self):
        header = "Content-Type"
        body = "application/json"
        self.set_header(header, body)

    def restart(self):
        restart_argument = self.get_query_arguments('restart')[0]
        job = self.job_scheduler.get_job(restart_argument)
        if job:
            self.write(json.dumps({'result': '{} {}'.format('restarting', restart_argument)}))
            self.job_scheduler.restart_job(job)
        else:
            self.write(json.dumps({'result': 'failed'}))

    def get(self):
        # READ-ONLY. `?restart=<job>` used to SIGKILL and relaunch the job on a
        # plain GET, and this port is published on every host interface: any
        # LAN host — or an <img> tag in a page the operator opened — could
        # restart db_backup until its --keep window rotated every snapshot out.
        # Restarting a job is an operator act: `docker exec` and re-run it.
        if len(self.get_query_arguments('restart')) > 0:
            self.set_status(405)
            self.write(json.dumps({'result': 'refused',
                                   'reason': 'job restart over HTTP is disabled; this page is read-only'}))
            return
        self.write(json.dumps([ob.__dict__() for ob in self.job_scheduler.jobs]))


def main(config_file: str, only_list: List[str], except_list: List[str], no_health_check: bool = False):
    def get_network_ip() -> str:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip

    log.debug('network address: {}'.format(get_network_ip()))
    log.debug('loading config file {}'.format(config_file))
    conf_file = open(config_file, 'r')
    config = yaml.load(conf_file, Loader=yaml.FullLoader)

    # change the working directory
    root_directory = config['root_directory']
    if root_directory and not os.path.exists(root_directory):
        log.error('root_directory {} not found'.format(root_directory))
    elif root_directory and os.path.exists(root_directory):
        os.chdir(root_directory)

    port = config['port']
    # The health_check_eval imports trader modules and opens an IB connection —
    # that's wanted when pycron supervises the full service stack, but a
    # cron-only instance (started by start_mmr.sh alongside directly-launched
    # services) must not: it runs inside pycron's own event loop and just spams
    # connection errors every 10 minutes.
    health_check_eval = None if no_health_check else config['health_check_eval']
    env = config['env']
    if env:
        for env_line in env.split(' '):
            key, val = env_line.split('=')
            os.environ[key] = val

    conf_file.close()

    # Optional file-level default zone for every job's start/stop expressions;
    # a job's own `timezone:` key overrides it. Validated up front so a typo
    # fails at load rather than silently running on local time.
    default_timezone = config.get('timezone') or None
    resolve_timezone(default_timezone)

    jobs: List[Job] = []
    for j in config['jobs']:
        job = Job(j, default_timezone=default_timezone)
        if only_list and job.name not in only_list:
            continue
        if except_list and job.name in except_list:
            continue
        jobs.append(job)
        log.debug('adding job: {}'.format(job))

    # required for nested asyncio calls and avoids RuntimeError: This event loop is already running
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    status_path = default_status_path()
    log.info('job status file: {}'.format(status_path))
    job_scheduler = JobScheduler(jobs, health_check_eval=health_check_eval, status_path=status_path)
    # Optional top-level key: seconds of polling after a start or a wake before
    # any catch-up run is started (default CATCH_UP_SETTLE_S).
    settle = config.get('catch_up_settle_seconds')
    if settle is not None:
        if isinstance(settle, bool) or not isinstance(settle, (int, float)) or settle != settle or settle < 0:
            raise ValueError(
                'pycron: catch_up_settle_seconds must be a non-negative number, got {!r}'.format(settle))
        job_scheduler.catch_up_settle_s = float(settle)

    app = Application([
        url(r"/", MainHandler, dict(job_scheduler=job_scheduler))
    ])

    log.debug('starting pycron at port {}'.format(port))
    app.listen(port)

    job_scheduler.start()
    log.debug('run_forever()')
    loop.run_forever()


@click.command()
@click.option('--config', required=True, help='yaml configuration file')
@click.option('--start', '-s', multiple=True, required=False, help='process name(s) to start [repeat option for more]')
@click.option('--butnot', '-n', multiple=True, required=False, help='process names(s) not to start [repeat option for more]')
@click.option('--no-health-check', is_flag=True, required=False, help='skip the periodic health_check_eval (use for cron-only instances)')
@click.option('--debug', is_flag=True, required=False, help='show debug output')
def bootstrap(
    config: str,
    start,
    butnot,
    no_health_check,
    debug
):
    debug_level = 'DEBUG' if debug else 'INFO'
    logging.basicConfig(
        level=debug_level, format='%(message)s', datefmt='[%X]', handlers=[RichHandler()]
    )

    if not os.path.exists(config):
        log.error('config_file at {} does not exist'.format(config))
        sys.exit(1)

    main(config, start, butnot, no_health_check)


if __name__ == '__main__':
    bootstrap()  # type: ignore
