#!/usr/bin/env bash
# Runs INSIDE the gateway, supervised by run.sh. No host scheduler or Docker
# socket is needed. Exit 42 requests a container restart through the existing
# restart policy; other errors keep monitoring without restarting the gateway.
set -u

PROBE=${1:-/home/ibgateway/scripts/mmr-healthcheck.sh}
STATE_DIR=${2:-${TWS_SETTINGS_PATH:-/home/ibgateway/tws_settings}/.mmr-recovery}
STATE_FILE=$STATE_DIR/last_restart
FAIL_THRESHOLD=3
CHECK_INTERVAL=300
RESTART_COOLDOWN=1800
failures=0

log() { printf '[gateway recovery] %s\n' "$*"; }
trap 'exit 0' TERM INT
log "monitor started: native API check every ${CHECK_INTERVAL}s; ${FAIL_THRESHOLD} failures before recovery"

while true; do
    # Also gives a fresh login five minutes before the first observation.
    if ! sleep "$CHECK_INTERVAL"; then
        log "monitor timer failed; not intervening"
        exit 2
    fi
    result=0
    bash "$PROBE" >/dev/null 2>&1 || result=$?
    case "$result" in
        0)
            if [ "$failures" -gt 0 ]; then log "native API listener recovered"; fi
            failures=0
            continue
            ;;
        1) failures=$((failures + 1)) ;;
        *)
            failures=0
            log "probe unavailable (exit $result); failure streak cleared"
            continue
            ;;
    esac
    if [ "$failures" -gt "$FAIL_THRESHOLD" ]; then failures=$FAIL_THRESHOLD; fi
    log "native API listener unavailable ($failures/$FAIL_THRESHOLD checks)"
    if [ "$failures" -lt "$FAIL_THRESHOLD" ]; then continue; fi

    # Never evaluate unchecked file contents as Bash arithmetic. A bad state
    # file, missing clock, or failed persistence cannot authorize a restart.
    last_restart=0
    if [ -e "$STATE_FILE" ]; then
        if ! last_restart=$(cat "$STATE_FILE") || [[ ! "$last_restart" =~ ^[0-9]{1,10}$ ]]; then
            log "invalid restart state; not intervening"
            continue
        fi
        last_restart=$((10#$last_restart))
    fi
    if ! now=$(date +%s) || [[ ! "$now" =~ ^[0-9]{1,10}$ ]]; then
        log "clock unavailable; not intervening"
        continue
    fi
    now=$((10#$now))
    # A backwards wall-clock jump conservatively retains the cooldown.
    if [ "$last_restart" -gt 0 ] && [ $((now - last_restart)) -lt "$RESTART_COOLDOWN" ]; then
        log "restart suppressed by ${RESTART_COOLDOWN}s cooldown"
        continue
    fi

    if ! mkdir -p "$STATE_DIR" || ! temporary=$(mktemp "$STATE_DIR/.last_restart.XXXXXX"); then
        log "restart state unavailable; not intervening"
        continue
    fi
    if ! printf '%s\n' "$now" > "$temporary" || ! mv -f "$temporary" "$STATE_FILE"; then
        rm -f "$temporary"
        log "cannot persist restart cooldown; not intervening"
        continue
    fi
    log "native API unavailable across three checks; requesting gateway container restart"
    exit 42
done
