#!/usr/bin/env bash
#
# Health monitor — infrastructure signals from the trader_service log:
#
#   - IB upstream connectivity transitions (1100/1102/2110/2157 + the
#     ib_upstream flag lines), reconnect/relogin activity
#   - farm-status transitions ("ibrx farm status:" — INFO since the G2 fix)
#   - any ERROR / CRITICAL / Traceback
#
# ...and, from pycron's status file (pycron_status.json in the same directory,
# read by scripts/pycron_status.py), the scheduled jobs:
#
#   - a job whose last run failed, timed out or could not be started
#   - a scheduled run that was missed and not caught up
#   - a scheduler that has stopped polling, or that the restart
#     circuit-breaker stopped: nothing scheduled is running at all
#
# Those lines are printed when they CHANGE, not repeated, and do not pass
# through the include/exclude filters below. pycron's own log
# (pycron_cron.log) is read by nothing: data_audit failed 46 nights in it.
#
# Noise policy (mute at the monitor, never at the log):
#   - DEBUG lines excluded wholesale
#   - the 30s "pulse ib_connected=..." heartbeat excluded — it would
#     false-match 'ib_upstream'. Read it with scripts/last_pulse.sh; a
#     pulse that STOPS APPEARING for >2 intervals means the service is
#     wedged (that absence, not an error line, was the G3 failure mode).
#   - G5's ib_async decoder KeyError traceback never lands in this file
#     (ib_async loggers route to console + logs/debug.log only) — no exclusion
#     needed here; if you monitor docker logs instead, exclude 'KeyError: 81'.
#
# Works from host or container (bind-mounted log dir). Re-arm after any
# container recreation.
#
set -u

LOG_DIR="${MMR_LOG_DIR:-$HOME/.local/share/mmr/logs}"
SERVICE="trader_service"
BACKLOG="${MMR_MONITOR_BACKLOG:-50}"

INCLUDE='ib_upstream|upstream|1100|1102|2110|2157|farm status|SUBSCRIPTION|reconnect|relogin|disconnect|AccountNotPinned|::ERROR::|::CRITICAL::|Traceback'
EXCLUDE='::DEBUG::|::INFO::pulse '

# Newest NON-EMPTY session log. find+stat instead of a shell glob: the logs
# dir accumulates session-stamped files from every CLI/pytest run (66k found
# 2026-07-19 — enough to blow ARG_MAX and silently break glob expansion).
# stat -f is macOS, -c is Linux — try both for host/container portability.
newest() {
    find "$LOG_DIR" -maxdepth 1 -name "${SERVICE}_*.log" -size +0c \
        \( -exec stat -f '%m %N' {} \; -o -exec stat -c '%Y %n' {} \; \) 2>/dev/null \
        | sort -rn | head -1 | cut -d' ' -f2-
}

# Scheduled-job report. Needs python3 (standard library only); without it the
# job checks are skipped, and that is said once.
PYCRON_STATUS_FILE="${MMR_PYCRON_STATUS:-$LOG_DIR/pycron_status.json}"
PYCRON_REPORTER="$(cd "$(dirname "$0")" && pwd)/pycron_status.py"
PYCRON_LAST=""
pycron_report() {
    local out
    if ! command -v python3 >/dev/null 2>&1 || [ ! -f "$PYCRON_REPORTER" ]; then
        out="pycron: scheduled-job checks skipped (need python3 and $PYCRON_REPORTER)"
    else
        out=$(python3 "$PYCRON_REPORTER" --file "$PYCRON_STATUS_FILE" 2>&1)
    fi
    if [ "$out" != "$PYCRON_LAST" ]; then
        if [ -n "$out" ]; then
            printf '%s\n' "$out" | sed 's/^/[monitor_health] /' >&3
        else
            echo "[monitor_health] pycron: scheduled jobs OK" >&3
        fi
        PYCRON_LAST="$out"
    fi
}

echo "[monitor_health] watching $LOG_DIR/${SERVICE}_*.log"
echo "[monitor_health] include: $INCLUDE"
echo "[monitor_health] scheduled jobs: $PYCRON_STATUS_FILE"

# fd 3 = this script's stdout, for lines that must bypass the grep filters.
exec 3>&1
pycron_report

{
    while true; do
        f=$(newest)
        if [ -z "$f" ]; then
            echo "[monitor_health] no ${SERVICE} log yet — waiting..."
            sleep 10
            pycron_report
            continue
        fi
        echo "[monitor_health] following $f"
        tail -n "$BACKLOG" -F "$f" &
        TAIL_PID=$!
        while sleep 10; do
            pycron_report
            latest=$(newest)
            if [ -n "$latest" ] && [ "$latest" != "$f" ]; then
                echo "[monitor_health] new session log detected: $latest"
                break
            fi
            kill -0 "$TAIL_PID" 2>/dev/null || break
        done
        kill "$TAIL_PID" 2>/dev/null
        wait "$TAIL_PID" 2>/dev/null
    done
} | grep --line-buffered -E "$INCLUDE" | grep --line-buffered -vE "$EXCLUDE"
