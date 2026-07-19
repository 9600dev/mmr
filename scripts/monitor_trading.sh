#!/usr/bin/env bash
#
# Trading monitor — the curated tail an operator (or Claude) keeps open all
# session. Watches the strategy_service log for signal/execution events:
#
#   - auto-executor: OPENING/OPENED/CLOSING/CLOSED/CLOSE FAILED/refused lines
#   - BUY/SELL signal emissions
#   - strategy crashes ("raised on_prices ... disabling it")
#   - any ERROR / CRITICAL / Traceback
#
# Noise policy (horserank rule: mute at the monitor, never at the log):
#   - DEBUG lines excluded wholesale
#   - the 30s "pulse ..." heartbeat excluded (read it on demand with
#     scripts/last_pulse.sh; its ABSENCE or ticks_60s=0 during market hours
#     is the escalation signal — see docs/MONITORING.md)
#
# Log files are per-process session-stamped (strategy_service_<ts>.log), so
# this script re-resolves the newest file and follows across service
# restarts. Works from the host or inside the container (same bind-mounted
# path). Re-arm after container recreation kills an exec-based session.
#
set -u

LOG_DIR="${MMR_LOG_DIR:-$HOME/.local/share/mmr/logs}"
SERVICE="strategy_service"
BACKLOG="${MMR_MONITOR_BACKLOG:-50}"

INCLUDE='auto-executor:|BUY signal|SELL signal|raised on_prices|disabling|auto-execute submission failed|open_failed|reconciliation error|::ERROR::|::CRITICAL::|Traceback'
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

echo "[monitor_trading] watching $LOG_DIR/${SERVICE}_*.log"
echo "[monitor_trading] include: $INCLUDE"

{
    while true; do
        f=$(newest)
        if [ -z "$f" ]; then
            echo "[monitor_trading] no ${SERVICE} log yet — waiting..."
            sleep 10
            continue
        fi
        echo "[monitor_trading] following $f"
        tail -n "$BACKLOG" -F "$f" &
        TAIL_PID=$!
        # Switch to a newer session file when the service restarts.
        while sleep 10; do
            latest=$(newest)
            if [ -n "$latest" ] && [ "$latest" != "$f" ]; then
                echo "[monitor_trading] new session log detected: $latest"
                break
            fi
            kill -0 "$TAIL_PID" 2>/dev/null || break
        done
        kill "$TAIL_PID" 2>/dev/null
        wait "$TAIL_PID" 2>/dev/null
    done
} | grep --line-buffered -E "$INCLUDE" | grep --line-buffered -vE "$EXCLUDE"
