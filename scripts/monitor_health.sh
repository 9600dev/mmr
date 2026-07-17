#!/usr/bin/env bash
#
# Health monitor — infrastructure signals from the trader_service log:
#
#   - IB upstream connectivity transitions (1100/1102/2110/2157 + the
#     ib_upstream flag lines), reconnect/relogin activity
#   - farm-status transitions ("ibrx farm status:" — INFO since the G2 fix)
#   - any ERROR / CRITICAL / Traceback
#
# Noise policy (mute at the monitor, never at the log):
#   - DEBUG lines excluded wholesale
#   - the 30s "pulse ib_connected=..." heartbeat excluded — it would
#     false-match 'ib_upstream'. Read it with scripts/last_pulse.sh; a
#     pulse that STOPS APPEARING for >2 intervals means the service is
#     wedged (that absence, not an error line, was the G3 failure mode).
#   - G5's ib_async decoder KeyError traceback never lands in this file
#     (ib_async loggers route to console/debug.log only) — no exclusion
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

# Newest NON-EMPTY session log. Every process that loads the logging config
# (each mmr CLI run, pytest) creates its own empty session-stamped file set —
# only the real service ever writes to its file, so size>0 disambiguates.
newest() {
    ls -t "$LOG_DIR/${SERVICE}_"*.log 2>/dev/null | while read -r f; do
        if [ -s "$f" ]; then
            echo "$f"
            break
        fi
    done
}

echo "[monitor_health] watching $LOG_DIR/${SERVICE}_*.log"
echo "[monitor_health] include: $INCLUDE"

{
    while true; do
        f=$(newest)
        if [ -z "$f" ]; then
            echo "[monitor_health] no ${SERVICE} log yet — waiting..."
            sleep 10
            continue
        fi
        echo "[monitor_health] following $f"
        tail -n "$BACKLOG" -F "$f" &
        TAIL_PID=$!
        while sleep 10; do
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
