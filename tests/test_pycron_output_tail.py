"""pycron keeps only a bounded tail of a job's output (no unbounded buffering)."""
import asyncio
import sys

from pycron.pycron import Job, JobScheduler, drain_tail


async def _run(code, **kw):
    p = await asyncio.create_subprocess_exec(sys.executable, '-c', code,
                                             stdout=asyncio.subprocess.PIPE)
    out = await drain_tail(p.stdout, **kw)
    await p.wait()
    return out


def test_tail_is_bounded_and_keeps_last_lines():
    out = asyncio.run(_run("import sys\nfor i in range(50000): print(f'line {i}')", max_lines=100))
    lines = out.splitlines()
    assert lines[0].startswith('[... 49900 earlier lines dropped')
    assert lines[-1] == 'line 49999' and len(lines) == 101


def test_overlong_line_does_not_break_draining():
    out = asyncio.run(_run("print('x' * 200000); print('done')", max_line_bytes=1000))
    assert out.splitlines()[-1] == 'done'
    assert max(len(l) for l in out.splitlines()) <= 1000


def test_start_job_returns_stdout_tail():
    job = Job({'name': 'echo', 'command': sys.executable,
               'arguments': "-c \"print('hello'); import sys; print('oops', file=sys.stderr)\"",
               'start': '* * * * *'})
    sched = JobScheduler()
    assert asyncio.run(sched.start_job(job)) == 'hello'
    assert job.return_code == 0 and job.finished
