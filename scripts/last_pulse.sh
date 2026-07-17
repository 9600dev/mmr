#!/usr/bin/env bash
#
# Print each service's most recent heartbeat pulse — the on-demand health
# read the monitors deliberately exclude from their streams.
#
#   trader_service pulse:   pulse ib_connected=True ib_upstream=True open_orders=0
#   strategy_service pulse: pulse strategies=5/5 ticks_60s=[conid:n,...]
#                           bar_age_s=[conid:n,...] auto_exec_open=1
#
# Escalation reads (docs/MONITORING.md):
#   - a pulse older than ~2 minutes  -> that service's loop is wedged
#   - ticks_60s all :0 while a traded market is open -> feed is dead (G3 class)
#   - ib_connected/ib_upstream False -> gateway/upstream trouble
#
set -u

LOG_DIR="${MMR_LOG_DIR:-$HOME/.local/share/mmr/logs}"
STALE_AFTER="${MMR_PULSE_STALE_SECONDS:-120}"
rc=0

for svc in trader_service strategy_service; do
    # Newest NON-EMPTY session log (every CLI/pytest run creates empty
    # session-stamped files for all services; only the service writes).
    f=$(ls -t "$LOG_DIR/${svc}_"*.log 2>/dev/null | while read -r c; do
            [ -s "$c" ] && { echo "$c"; break; }
        done)
    if [ -z "$f" ]; then
        echo "$svc: NO LOG FILE in $LOG_DIR"
        rc=1
        continue
    fi
    line=$(grep '::INFO::pulse ' "$f" | tail -1)
    if [ -z "$line" ]; then
        echo "$svc: NO PULSE in $(basename "$f") (service started before pulses shipped, or wedged before first pulse)"
        rc=1
        continue
    fi
    echo "$svc: $line"
    # Staleness check: pulse timestamps are the log line's leading asctime
    # ("YYYY-MM-DD HH:MM:SS,mmm::..." in the service's local time).
    ts=$(printf '%s' "$line" | cut -d, -f1)
    if [ -n "$ts" ]; then
        now_epoch=$(date +%s)
        then_epoch=$(date -j -f '%Y-%m-%d %H:%M:%S' "$ts" +%s 2>/dev/null \
                     || date -d "$ts" +%s 2>/dev/null || echo "")
        if [ -n "$then_epoch" ]; then
            age=$((now_epoch - then_epoch))
            if [ "$age" -gt "$STALE_AFTER" ]; then
                echo "$svc: *** PULSE STALE (${age}s old > ${STALE_AFTER}s) — service loop may be wedged ***"
                rc=1
            fi
        fi
    fi
done

exit $rc
