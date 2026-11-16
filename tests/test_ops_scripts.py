"""docker.sh and start_mmr.sh: the operational paths that touch a running stack.

From the 2026-11-14 audit (B5.3-B5.5):

* ``docker.sh -u`` — documented, and suggested by the CLI, as "start the
  containers" — ``rm -f``'d (SIGKILL) every container on the project network
  first, so running it against a live stack killed the trading services with no
  shutdown at all. ``-r`` did the same to the MMR container.
* ``docker.sh -a`` rsynced ``.env`` (the IB login) and other local-only files
  into the container.
* A crash-looping side service (pycron, the dashboard) tripped the restart
  circuit-breaker and took the whole stack down with it, over and over.

None of this runs docker, podman or a real service. The functions under test are
lifted verbatim out of the scripts and run with ``$RUNTIME`` / ``$COMPOSE``
replaced by shell functions that keep their "containers" in a text file (and,
for the sync test, with real ``rsync`` between two temporary directories).
"""
import os
import re
import shutil
import signal
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCKER_SH = ROOT / 'docker.sh'
START_MMR = ROOT / 'start_mmr.sh'

pytestmark = pytest.mark.skipif(sys.platform == 'win32' or shutil.which('bash') is None,
                                reason='bash scripts')


def _run(script, *args, timeout=60, env=None):
    """Run a harness in its own process group and leave nothing behind."""
    process = subprocess.Popen(['bash', str(script), *map(str, args)], stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, start_new_session=True,
                               env={**os.environ, **(env or {})})
    try:
        output, _ = process.communicate(timeout=timeout)
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    return process.returncode, output


@pytest.mark.parametrize('script', ['docker.sh', 'start_mmr.sh', 'scripts/monitor_health.sh'])
def test_scripts_parse(script):
    result = subprocess.run(['bash', '-n', str(ROOT / script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


# ── docker.sh against a fake container runtime ───────────────────────────────

DOCKER_HARNESS = r'''
set -o errexit -o pipefail -o noclobber -o nounset
SRC="$1"; SCENARIO="$2"; ACTION="$3"; SCRATCH="$4"
export HOME="$SCRATCH/home"; mkdir -p "$HOME"
STATE="$SCRATCH/containers"     # lines: <cid> <name> <status> <health>
LOG="$SCRATCH/calls"
: > "$STATE"; : > "$LOG"

case "$SCENARIO" in
    healthy)   printf 'g1 mmr-ib-gateway-1 running healthy\nm1 mmr-mmr-1 running healthy\n' >> "$STATE" ;;
    starting)  printf 'g1 mmr-ib-gateway-1 running starting\nm1 mmr-mmr-1 running starting\n' >> "$STATE" ;;
    nohealth)  printf 'g1 mmr-ib-gateway-1 running none\nm1 mmr-mmr-1 running none\n' >> "$STATE" ;;
    unhealthy) printf 'g1 mmr-ib-gateway-1 running healthy\nm1 mmr-mmr-1 running unhealthy\n' >> "$STATE" ;;
    gw_down)   printf 'g1 mmr-ib-gateway-1 exited none\nm1 mmr-mmr-1 running healthy\n' >> "$STATE" ;;
    stale_mmr) printf 'g1 mmr-ib-gateway-1 running healthy\nm1 mmr-mmr-1 exited none\n' >> "$STATE" ;;
    leftover)  printf 'g1 mmr-ib-gateway-1 running healthy\nm1 mmr-mmr-1 running healthy\nm0 mmr_mmr_1 exited none\n' >> "$STATE" ;;
    absent)    ;;
    *) echo "unknown scenario $SCENARIO"; exit 2 ;;
esac

_rewrite() {
    awk "$1" "$STATE" > "$STATE.new" || true
    mv -f "$STATE.new" "$STATE"
}

fake_runtime() {
    echo "runtime $*" >> "$LOG"
    case "$1" in
        ps)
            local all=0 filter="" a cid name status health
            for a in "$@"; do
                case "$a" in
                    -a|-aq) all=1 ;;
                    name=*|network=*) filter="$a" ;;
                esac
            done
            while read -r cid name status health; do
                [ -n "$cid" ] || continue
                if [ "$all" = 0 ] && [ "$status" != running ]; then continue; fi
                case "$filter" in
                    name=*) if echo "$name" | grep -Eq "${filter#name=}"; then echo "$cid"; fi ;;
                    *)      echo "$cid" ;;      # every project container is on mmr_default
                esac
            done < "$STATE"
            ;;
        inspect)
            local want="${!#}" found=0 cid name status health
            while read -r cid name status health; do
                if [ "$cid" = "$want" ]; then echo "$status/$health"; found=1; fi
            done < "$STATE"
            [ "$found" = 1 ] || return 1
            ;;
        stop)
            local cid="${!#}"
            _rewrite "{ if (\$1 == \"$cid\") { \$3 = \"exited\"; \$4 = \"none\" } print }"
            ;;
        rm)
            local cid="${!#}"
            if grep -q "^$cid [^ ]* running" "$STATE"; then
                echo "SIGKILL $cid removed while running" >> "$LOG"
            fi
            _rewrite "\$1 != \"$cid\""
            ;;
        *) ;;
    esac
}

fake_compose() {
    echo "compose $*" >> "$LOG"
    shift 2     # -f <file>
    case "$1" in
        up)
            local want="ib-gateway mmr" a svc
            for a in "$@"; do
                case "$a" in ib-gateway|mmr) want="$a" ;; esac
            done
            for svc in $want; do
                if ! grep -q " mmr-$svc-1 " "$STATE"; then
                    echo "new-$svc mmr-$svc-1 running starting" >> "$STATE"
                fi
            done
            ;;
        down)
            # An older compose that does not know `down -t`: usage error, nothing done.
            if [ "${FAKE_COMPOSE_REJECTS_DOWN_TIMEOUT:-0}" = 1 ] && echo " $* " | grep -q -- " -t "; then
                echo "unknown option -t" >&2
                return 2
            fi
            : >| "$STATE"
            ;;
        *) ;;
    esac
}

RUNTIME=fake_runtime
COMPOSE=fake_compose
BUILDDIR="$SCRATCH/build"; mkdir -p "$BUILDDIR"
MMR_DATA_DIR="$HOME/.local/share/mmr"
MMR_DB_VOLUME="mmr_mmr_db_data"

check_env() { :; }
print_api_keys() { :; }
print_vnc_url() { :; }
seed_db_if_empty() { :; }
build() { echo "build" >> "$LOG"; }

extract() { sed -n "/^$1() {\$/,/^}\$/p" "$SRC"; }
grace="$(grep -E '^STOP_GRACE_SECONDS=' "$SRC")"
eval "$grace"
for fn in _stop_and_remove _stack_is_up up _remove_mmr_dependents restart_ib ib_only down; do
    body="$(extract "$fn")"
    if [ -z "$body" ]; then echo "HARNESS: $fn() not found in $SRC" >&2; exit 3; fi
    eval "$body"
done
# ib_only sources the real .env for the port hint; give it a harmless one.
echo "TRADING_MODE=paper" > "$BUILDDIR/.env"

case "$ACTION" in
    up|down|restart_ib|ib_only) "$ACTION" ;;
    go) build; down 2>/dev/null || true; up ;;       # what `-g` runs
    *) echo "unknown action $ACTION"; exit 2 ;;
esac
'''


@pytest.fixture
def docker_sh(tmp_path):
    harness = tmp_path / 'docker_harness.sh'
    harness.write_text(DOCKER_HARNESS)
    counter = {'n': 0}

    def run(scenario, action, env=None):
        counter['n'] += 1
        scratch = tmp_path / f'run{counter["n"]}'
        scratch.mkdir()
        code, output = _run(harness, DOCKER_SH, scenario, action, scratch, env=env)
        assert code == 0, output
        calls = (scratch / 'calls').read_text().splitlines()
        containers = (scratch / 'containers').read_text().split('\n')
        return calls, [line for line in containers if line], output
    return run


def _verb(call):
    """'runtime stop -t 30 m1' -> 'stop'; 'compose -f x up -d' -> '-f'; 'build' -> ''."""
    words = call.split()
    return words[1] if len(words) > 1 else ''


def _stops_before_removals(calls):
    """Every removal of a container is preceded by a graceful stop of that container."""
    stopped = set()
    for call in calls:
        words = call.split()
        if words[:2] == ['runtime', 'stop']:
            assert words[2:4] == ['-t', '30'], call
            stopped.add(words[-1])
        elif words[:2] == ['runtime', 'rm']:
            assert words[-1] in stopped, f'{call!r} with no stop first: {calls}'
    assert not [call for call in calls if call.startswith('SIGKILL')], calls


@pytest.mark.parametrize('scenario', ['healthy', 'starting', 'nohealth'])
def test_up_leaves_a_running_stack_alone(docker_sh, scenario):
    calls, containers, output = docker_sh(scenario, 'up')
    touched = [call for call in calls if _verb(call) in ('stop', 'rm', 'network', 'kill', 'restart')]
    assert touched == [], calls
    assert not [call for call in calls if ' pull ' in call], 'a new gateway image would recreate the gateway'
    assert [call for call in calls if call.startswith('compose') and call.endswith(' up -d')]
    assert [line.split()[0] for line in containers] == ['g1', 'm1']      # the same containers
    assert 'already running' in output


@pytest.mark.parametrize('scenario', ['unhealthy', 'gw_down', 'stale_mmr', 'leftover'])
def test_up_recovery_stops_before_it_removes(docker_sh, scenario):
    calls, containers, _ = docker_sh(scenario, 'up')
    _stops_before_removals(calls)
    removed = [call.split()[-1] for call in calls if call.startswith('runtime rm')]
    assert 'm1' in removed and 'g1' in removed
    assert removed.index('m1') < removed.index('g1'), 'MMR goes down before the gateway it is using'
    assert any('network rm mmr_default' in call for call in calls)
    assert [call for call in calls if ' pull ' in call]
    assert sorted(line.split()[1] for line in containers) == ['mmr-ib-gateway-1', 'mmr-mmr-1']


def test_up_on_a_fresh_machine_just_starts(docker_sh):
    calls, containers, output = docker_sh('absent', 'up')
    assert not [call for call in calls if _verb(call) in ('stop', 'rm')]
    assert [call for call in calls if call.endswith(' up -d')]
    assert len(containers) == 2 and 'Containers started' in output


@pytest.mark.parametrize('action', ['restart_ib', 'ib_only'])
def test_gateway_restart_stops_mmr_gracefully(docker_sh, action):
    calls, _, _ = docker_sh('healthy', action)
    _stops_before_removals(calls)
    assert 'runtime stop -t 30 m1' in calls and 'runtime rm -f m1' in calls
    assert any('--force-recreate ib-gateway' in call for call in calls)
    if action == 'restart_ib':
        assert calls[-1].endswith(' up -d mmr')      # and MMR is brought back


def test_down_gives_the_services_time_to_stop(docker_sh):
    calls, containers, _ = docker_sh('healthy', 'down')
    assert any(call.startswith('compose') and call.endswith(' down -t 30') for call in calls)
    _stops_before_removals(calls)
    assert containers == []


def test_down_falls_back_to_a_graceful_sweep_when_compose_rejects_the_timeout(docker_sh):
    """`compose down -t` is not universal; whatever it leaves is stopped, then removed."""
    calls, containers, _ = docker_sh('healthy', 'down', env={'FAKE_COMPOSE_REJECTS_DOWN_TIMEOUT': '1'})
    _stops_before_removals(calls)
    assert 'runtime stop -t 30 m1' in calls and 'runtime stop -t 30 g1' in calls
    assert any('network rm mmr_default' in call for call in calls)
    assert containers == []


def test_go_still_rebuilds_and_recreates(docker_sh):
    """`-g` keeps its meaning: build, take the stack down, bring a new one up."""
    calls, containers, _ = docker_sh('healthy', 'go')
    assert calls[0] == 'build'
    order = [i for i, call in enumerate(calls)
             if call.endswith(' down -t 30') or ' pull ' in call or call.endswith(' up -d')]
    assert len(order) == 3 and order == sorted(order)
    assert sorted(line.split()[0] for line in containers) == ['new-ib-gateway', 'new-mmr']
    _stops_before_removals(calls)


# ── docker.sh -a: what may be copied into the container ──────────────────────

SYNC_HARNESS = r'''
set -o errexit -o pipefail -o noclobber -o nounset
SRC="$1"; BUILDDIR="$2"; DEST="$3"
RUNTIME=fake_runtime
fake_runtime() { if [ "$1" = ps ]; then echo "cid123"; fi; }
# Real rsync, local destination: drop the `-e <transport>` pair and the two
# positional arguments, pass every filter through exactly as docker.sh built it.
rsync() {
    local filters=() a skip=0
    for a in "$@"; do
        if [ "$skip" = 1 ]; then skip=0; continue; fi
        case "$a" in
            -e) skip=1 ;;
            --exclude=*|--include=*|--filter=*) filters+=("$a") ;;
        esac
    done
    ( cd "$BUILDDIR" && command rsync -a --delete ./ "$DEST/" "${filters[@]}" )
}
extract() { sed -n "/^$1() {\$/,/^}\$/p" "$SRC"; }
filters_src="$(sed -n '/^SYNC_ALL_FILTERS=($/,/^)$/p' "$SRC")"
if [ -z "$filters_src" ]; then echo "HARNESS: SYNC_ALL_FILTERS not found" >&2; exit 3; fi
eval "$filters_src"
body="$(extract sync_all)"
if [ -z "$body" ]; then echo "HARNESS: sync_all() not found" >&2; exit 3; fi
eval "$body"
sync_all
'''


@pytest.mark.skipif(shutil.which('rsync') is None, reason='rsync not installed')
def test_sync_all_never_copies_secrets_or_local_state(tmp_path):
    repo, container = tmp_path / 'repo', tmp_path / 'container'
    container.mkdir()
    must_not_copy = ['.env', '.env.local', 'secrets.env', 'prod.secrets', 'sub/.env', 'sub/.env.production',
                     'sub/prod.secrets', 'docs/OPERATIONAL_STATE.md', '.venv/bin/python', '.git/config',
                     '.cache/x', '.mypy_cache/x', '.pytest_cache/x', 'mutants/trader/x.py',
                     '.claude/worktrees/a/.env', '.claude/worktrees/a/trader/x.py', '.vscode/settings.json']
    must_copy = ['.env.example', 'docs/OPERATIONAL_STATE.README.md', 'docs/MONITORING.md',
                 'trader/data/duckdb_store.py', 'trader/mutants/keep.py', 'data/local.csv',
                 'scripts/tool.sh', 'untracked_notes.txt', 'strategies/wip.py']
    for name in must_not_copy + must_copy:
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f'host copy of {name}\n')
    # Already in the container: a .env from an earlier sync, and a file since deleted upstream.
    (container / '.env').write_text('ALREADY-IN-CONTAINER\n')
    (container / 'removed_upstream.py').write_text('stale\n')

    harness = tmp_path / 'sync_harness.sh'
    harness.write_text(SYNC_HARNESS)
    code, output = _run(harness, DOCKER_SH, repo, container)
    assert code == 0, output

    present = {str(path.relative_to(container)) for path in container.rglob('*') if path.is_file()}
    assert present == set(must_copy) | {'.env'}, sorted(present ^ (set(must_copy) | {'.env'}))
    # Excluded means untouched: neither overwritten with the host's nor deleted.
    assert (container / '.env').read_text() == 'ALREADY-IN-CONTAINER\n'
    assert not (container / 'removed_upstream.py').exists()        # --delete still works


def test_sync_all_filters_cover_the_dockerignore_secrets():
    """.dockerignore keeps these out of the IMAGE; -a must keep them out of the container."""
    script = DOCKER_SH.read_text()
    block = script[script.index('SYNC_ALL_FILTERS=('):]
    block = block[:block.index('\n)\n')]
    for pattern in ('.env', '.env.*', 'secrets.env', '*.secrets', 'docs/OPERATIONAL_STATE.md', '.venv', '.git'):
        assert f"--exclude='{pattern}'" in block, pattern
        assert pattern in (ROOT / '.dockerignore').read_text().split(), pattern
    # First match wins in rsync: the template must be let through before `.env.*`.
    assert block.index("--include='.env.example'") < block.index("--exclude='.env.*'")
    assert '"${SYNC_ALL_FILTERS[@]}"' in script[script.index('sync_all() {'):]


# ── start_mmr.sh: which scheduled jobs the scheduler is told to load ─────────

def _scheduled_jobs_started_by_the_script():
    """The names after ``-s`` in start_mmr.sh's pycron command line."""
    text = START_MMR.read_text()
    body = text[text.index('run_pycron() {'):]
    body = body[:body.index('\n}')]
    return re.findall(r'-s\s+([A-Za-z0-9_]+)', body)


def _template_jobs():
    import yaml
    doc = yaml.safe_load((ROOT / 'config_defaults' / 'pycron.yaml').read_text())
    return doc['jobs']


def test_every_scheduled_job_in_the_template_is_loaded_by_the_start_script():
    """pycron only schedules the jobs it is started with (``-s NAME``). A job
    defined in pycron.yaml and missing from that list never runs, and nothing
    reports it: ``sleeve_upkeep`` was in the config, documented as nightly, and
    had never been scheduled (found 2026-11-16; it had only been run by hand)."""
    scheduled = {job['name'] for job in _template_jobs() if not job.get('start_on_pycron_start')}
    assert scheduled == {'data_refresh_us', 'data_audit', 'data_refresh_asx', 'db_backup', 'sleeve_upkeep',
                         'preflight_us', 'preflight_asx'}
    started = _scheduled_jobs_started_by_the_script()
    assert set(started) == scheduled
    assert len(started) == len(set(started))


def test_the_start_script_does_not_hand_the_services_to_the_scheduler():
    """The long-running services are launched (and supervised) by the script
    itself; a second copy from pycron would collide on the ZMQ ports."""
    services = {job['name'] for job in _template_jobs() if job.get('start_on_pycron_start')}
    assert services == {'data_service', 'trader_service', 'strategy_service', 'web_dashboard'}
    assert not services & set(_scheduled_jobs_started_by_the_script())


# ── start_mmr.sh: the restart circuit-breaker ────────────────────────────────

SUPERVISE_HARNESS = r'''
set -e
SRC="$1"; SCENARIO="$2"; export HOME="$3"
mkdir -p "$HOME/.local/share/mmr/logs"
fail() { printf 'FAIL: %s\n' "$1"; }
RUN_DIR="$HOME/.local/share/mmr/run"
SHUTDOWN_FLAG="$RUN_DIR/start_mmr.shutdown"
CLEANUP_EXIT_CODE=0
mkdir -p "$RUN_DIR"

# The backoff and poll sleeps are shortened; the stub services really run.
sleep() { command sleep 0.01; }
service_ok()  { exec sleep 30; }
service_bad() { return 1; }

stop_everything() {
    touch "$SHUTDOWN_FLAG"
    for f in "$RUN_DIR"/*.pid; do
        if [ -f "$f" ]; then kill "$(cat "$f")" 2>/dev/null || true; fi
    done
    kill $(jobs -p) 2>/dev/null || true
}
cleanup() {      # what start_mmr.sh's own cleanup() ends with
    echo "STACK TORN DOWN exit=$CLEANUP_EXIT_CODE"
    stop_everything
    exit "$CLEANUP_EXIT_CODE"
}

extract() { sed -n "/^$1() {\$/,/^}\$/p" "$SRC"; }
body="$(extract supervise)";  [ -n "$body" ] || { echo "HARNESS: supervise() not found"; exit 3; }
eval "$body"
body="$(extract _svc_pid)";   [ -n "$body" ] || { echo "HARNESS: _svc_pid() not found"; exit 3; }
eval "$body"
loop="$(sed -n '/^# ─── Wait for Supervisors/,$p' "$SRC")"
[ -n "$loop" ] || { echo "HARNESS: wait loop not found"; exit 3; }

DATA=service_ok; TRADER=service_ok; STRATEGY=service_ok; WEB=service_ok; CRON=service_ok
case "$SCENARIO" in
    pycron)  CRON=service_bad ;;
    web)     WEB=service_bad ;;
    both)    CRON=service_bad; WEB=service_bad ;;
    trader)  TRADER=service_bad ;;
    data)    DATA=service_bad ;;
    strategy) STRATEGY=service_bad ;;
esac
supervise data_service $DATA &          DATA_SUP_PID=$!
supervise trader_service $TRADER &      TRADER_SUP_PID=$!
supervise strategy_service $STRATEGY &  STRATEGY_SUP_PID=$!
supervise web_dashboard $WEB &          WEB_SUP_PID=$!
supervise pycron $CRON &                PYCRON_SUP_PID=$!

# The real loop never returns. Once the expected breaker(s) have tripped and
# the loop has had time to react, report which supervisors are still there and
# end the run.
(
    expect=""
    case "$SCENARIO" in
        pycron) expect="pycron" ;;
        web)    expect="web_dashboard" ;;
        both)   expect="pycron web_dashboard" ;;
    esac
    tries=0
    while [ "$tries" -lt 400 ]; do
        missing=0
        for name in $expect; do
            if [ ! -f "$RUN_DIR/$name.breaker" ]; then missing=1; fi
        done
        if [ "$missing" = 0 ]; then break; fi
        command sleep 0.05
        tries=$((tries + 1))
    done
    command sleep 0.5
    for pair in "data_service:$DATA_SUP_PID" "trader_service:$TRADER_SUP_PID" \
                "strategy_service:$STRATEGY_SUP_PID" "web_dashboard:$WEB_SUP_PID" "pycron:$PYCRON_SUP_PID"; do
        if kill -0 "${pair##*:}" 2>/dev/null; then echo "ALIVE ${pair%%:*}"; else echo "GONE ${pair%%:*}"; fi
    done
    kill -TERM $$
) &
trap 'echo "STACK STILL UP"; stop_everything; exit 0' TERM

eval "$loop"
'''


@pytest.fixture
def supervise(tmp_path):
    harness = tmp_path / 'supervise_harness.sh'
    harness.write_text(SUPERVISE_HARNESS)

    def run(scenario):
        home = tmp_path / f'home_{scenario}'
        home.mkdir()
        code, output = _run(harness, START_MMR, scenario, home)
        run_dir = home / '.local/share/mmr/run'
        breakers = sorted(path.name for path in run_dir.glob('*.breaker'))
        cron_log = home / '.local/share/mmr/logs/pycron_cron.log'
        return code, output, breakers, cron_log.read_text() if cron_log.exists() else ''
    return run


@pytest.mark.parametrize('scenario,down', [('pycron', ['pycron']), ('web', ['web_dashboard']),
                                           ('both', ['pycron', 'web_dashboard'])])
def test_side_service_crash_loop_leaves_trading_up(supervise, scenario, down):
    code, output, breakers, cron_log = supervise(scenario)
    assert 'STACK TORN DOWN' not in output, output
    assert 'STACK STILL UP' in output and code == 0, output
    for name in ('data_service', 'trader_service', 'strategy_service'):
        assert f'ALIVE {name}' in output, output
    for name in down:
        assert f'GONE {name}' in output
        assert f'FAIL: {name} hit the restart circuit-breaker' in output      # said loudly
        assert f'FAIL: {name} is DOWN and stays down' in output
    # The marker scripts/monitor_health.sh reports stays in place.
    assert breakers == [f'{name}.breaker' for name in down]
    if 'pycron' in down:
        assert 'no scheduled job will run' in output
        assert 'pycron stopped by the restart circuit-breaker' in cron_log
    else:
        assert 'ALIVE pycron' in output and cron_log == ''


@pytest.mark.parametrize('scenario', ['trader', 'data', 'strategy'])
def test_trading_service_crash_loop_still_exits_42(supervise, scenario):
    code, output, breakers, _ = supervise(scenario)
    assert code == 42, output
    assert 'STACK TORN DOWN exit=42' in output
    assert f'FAIL: {scenario}_service hit the restart circuit-breaker' in output
    assert 'tearing the stack down with exit 42' in output
    assert breakers == [f'{scenario}_service.breaker']
