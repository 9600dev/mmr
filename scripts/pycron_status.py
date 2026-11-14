#!/usr/bin/env python3
"""Report pycron's scheduled jobs: which ones failed, were missed, or stopped running.

pycron writes ``pycron_status.json`` next to the service logs (see
``pycron/pycron.py``). This prints one line per problem and nothing when all is
well, so a monitor can show its output verbatim:

    pycron: data_audit FAILED rc=1 at 2026-11-14 07:00:41 (46 in a row; last success 2026-09-29 07:00:12) | <last output line>
    pycron: db_backup MISSED its 2026-11-10 22:17:00 run (not run since)
    pycron: scheduler has not polled since 2026-11-14 03:12:10; scheduled jobs are NOT running
    side service pycron was stopped by the restart circuit-breaker (rc=1 restarts=5); it stays down until the stack is restarted

Used by ``scripts/monitor_health.sh``; also an on-demand read:

    scripts/pycron_status.py          # problems only
    scripts/pycron_status.py --all    # every job, one line each

Exit status: 0 no problems, 1 problems, 2 the status file is missing or unreadable.

Standard library only, so it runs on a host with no project environment. Times
are shown in the local zone of the machine running it (the schedules themselves
are written in the zone pycron.yaml names).
"""
import argparse
import datetime as dt
import json
import os
import sys

DEFAULT_STALE_MINUTES = 5.0

# last_outcome values that mean the last run did not do its job.
PROBLEM_OUTCOMES = {
    'failed': 'FAILED',
    'timeout': 'TIMED OUT',
    'spawn_failed': 'COULD NOT START',
    'interrupted': 'INTERRUPTED',
    'unknown': 'OUTCOME UNKNOWN',
}

# Side services start_mmr.sh stops restarting (instead of tearing the stack
# down) when they crash-loop. Their breaker file is the only trace.
SIDE_SERVICES = ('pycron', 'web_dashboard')


def default_status_file():
    log_dir = os.environ.get('MMR_LOG_DIR') or os.path.expanduser('~/.local/share/mmr/logs')
    return os.path.join(log_dir, 'pycron_status.json')


def default_run_dir():
    return os.environ.get('MMR_RUN_DIR') or os.path.expanduser('~/.local/share/mmr/run')


def parse_time(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def show_time(value):
    parsed = parse_time(value)
    if parsed is None:
        return 'never'
    return parsed.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z').rstrip()


def last_output_line(job):
    tail = job.get('last_output_tail')
    if not isinstance(tail, str):
        return ''
    lines = [line.strip() for line in tail.splitlines() if line.strip() and line.strip() != '[stderr]']
    return lines[-1][:200] if lines else ''


def job_problems(name, job):
    problems = []
    outcome = job.get('last_outcome')
    if outcome in PROBLEM_OUTCOMES:
        code = job.get('last_return_code')
        detail = 'rc={}'.format(code) if code is not None else (job.get('last_error') or 'no exit status')
        streak = job.get('consecutive_failures') or 0
        line = 'pycron: {} {} {} at {} ({} in a row; last success {})'.format(
            name, PROBLEM_OUTCOMES[outcome], detail, show_time(job.get('last_end')),
            streak, show_time(job.get('last_success')))
        if job.get('running'):
            line += ' [running again now]'
        last_line = last_output_line(job)
        if last_line:
            line += ' | ' + last_line
        problems.append(line)
    missed = parse_time(job.get('last_missed'))
    started = parse_time(job.get('last_start'))
    if missed is not None and (started is None or started < missed):
        problems.append('pycron: {} MISSED its {} run (not run since)'.format(
            name, show_time(job.get('last_missed'))))
    return problems


def job_summary(name, job):
    if job.get('running'):
        return 'pycron: {} running since {}'.format(name, show_time(job.get('last_start')))
    if not job.get('last_start'):
        return 'pycron: {} ok (not run yet; schedule {} {})'.format(
            name, job.get('start'), job.get('timezone') or 'local time')
    duration = job.get('last_duration_s')
    return 'pycron: {} ok, last run {} rc={}{}'.format(
        name, show_time(job.get('last_start')), job.get('last_return_code'),
        ' ({}s)'.format(duration) if duration is not None else '')


def scheduler_problems(payload, now, stale_minutes):
    if stale_minutes <= 0:
        return []
    scheduler = payload.get('scheduler')
    last_poll = parse_time(scheduler.get('last_poll')) if isinstance(scheduler, dict) else None
    if last_poll is None:
        return []
    if (now - last_poll).total_seconds() > stale_minutes * 60.0:
        # No age in the text: a monitor repeats a line only when it changes.
        return ['pycron: scheduler has not polled since {}; scheduled jobs are NOT running'.format(
            show_time(scheduler.get('last_poll')))]
    return []


def breaker_problems(run_dir):
    problems = []
    for name in SIDE_SERVICES:
        path = os.path.join(run_dir, name + '.breaker')
        try:
            with open(path, 'r') as fh:
                detail = fh.read().strip()
        except OSError:
            continue
        problems.append('side service {} was stopped by the restart circuit-breaker ({}); '
                        'it stays down until the stack is restarted'.format(name, detail or 'no detail'))
    return problems


def report(status_file, run_dir, stale_minutes, show_all, now=None):
    """Return (lines, exit_status)."""
    now = now or dt.datetime.now(dt.timezone.utc)
    lines = breaker_problems(run_dir)
    try:
        with open(status_file, 'r') as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict):
            raise ValueError('top level is not an object')
    except FileNotFoundError:
        return lines + ['pycron: no status file at {} (scheduler not running here, or not started yet)'.format(
            status_file)], 2
    except (OSError, ValueError) as ex:
        return lines + ['pycron: status file {} is unreadable ({})'.format(status_file, ex)], 2

    lines += scheduler_problems(payload, now, stale_minutes)
    jobs = payload.get('jobs')
    jobs = jobs if isinstance(jobs, dict) else {}
    problem_count = len(lines)
    for name in sorted(jobs):
        job = jobs[name]
        if not isinstance(job, dict):
            continue
        problems = job_problems(name, job)
        problem_count += len(problems)
        lines += problems
        if show_all and not problems:
            lines.append(job_summary(name, job))
    return lines, (1 if problem_count else 0)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--file', default=None, help='status file (default: $MMR_LOG_DIR/pycron_status.json)')
    parser.add_argument('--run-dir', default=None,
                        help='start_mmr.sh run directory holding *.breaker files '
                             '(default: $MMR_RUN_DIR or ~/.local/share/mmr/run)')
    parser.add_argument('--stale-minutes', type=float, default=DEFAULT_STALE_MINUTES,
                        help='report the scheduler as stopped when its last poll is older than this '
                             '(default %(default)s; 0 disables)')
    parser.add_argument('--all', action='store_true', help='also list jobs with no problem')
    args = parser.parse_args(argv)
    lines, status = report(args.file or default_status_file(), args.run_dir or default_run_dir(),
                           args.stale_minutes, args.all)
    for line in lines:
        print(line)
    return status


if __name__ == '__main__':
    sys.exit(main())
