"""The non-TTY container proxy relays termination to the in-container mmr.

`docker exec` without -t does not forward signals, so killing the host CLI
used to orphan the in-container process (still holding the DuckDB lock). A
fake `docker` runs the "container" side locally so the relay is exercised
end to end without Docker.
"""
import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest

FAKE_DOCKER = textwrap.dedent("""\
    #!/usr/bin/env python3
    import os, sys
    a = sys.argv[1:]
    assert a[0] == 'exec'; a = a[1:]
    while a and a[0].startswith('-'):
        flag = a.pop(0)
        if flag in ('-u', '-w', '-e'):
            a.pop(0)
    a.pop(0)                      # container name
    os.execvp(a[0], a)
""")

FAKE_MMR = textwrap.dedent("""\
    #!/bin/sh
    echo $$ > "$MMR_FAKE_PIDFILE"
    exec sleep 60
""")

RUNNER = textwrap.dedent("""\
    import sys
    from trader.mmr_cli import _run_proxied
    sys.exit(_run_proxied(sys.argv[1], 'fake-ctr', ['data', 'summary']))
""")


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.mark.skipif(sys.platform == 'win32', reason='posix signals')
def test_sigterm_reaches_in_container_process(tmp_path):
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    docker = bindir / 'docker'
    docker.write_text(FAKE_DOCKER)
    mmr = bindir / 'mmr'
    mmr.write_text(FAKE_MMR)
    for p in (docker, mmr):
        p.chmod(0o755)
    inner_pidfile = tmp_path / 'inner.pid'
    env = dict(os.environ, PATH=f'{bindir}:{os.environ["PATH"]}',
               MMR_FAKE_PIDFILE=str(inner_pidfile))
    runner = tmp_path / 'runner.py'
    runner.write_text(RUNNER)
    proc = subprocess.Popen([sys.executable, str(runner), str(docker)], env=env,
                            cwd=os.getcwd(), stdin=subprocess.DEVNULL)
    try:
        deadline = time.time() + 20
        while not inner_pidfile.exists() and time.time() < deadline:
            time.sleep(0.1)
        assert inner_pidfile.exists(), 'fake in-container mmr never started'
        time.sleep(0.2)
        inner = int(inner_pidfile.read_text().strip())
        assert _alive(inner)
        proc.send_signal(signal.SIGTERM)
        rc = proc.wait(timeout=20)
        deadline = time.time() + 5
        while _alive(inner) and time.time() < deadline:
            time.sleep(0.1)
        assert not _alive(inner), 'in-container process orphaned after client SIGTERM'
        assert rc != 0
    finally:
        if proc.poll() is None:
            proc.kill()
