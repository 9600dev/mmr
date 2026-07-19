#!/bin/bash
#
# ib_gateway_watchdog.sh — host-side watchdog for the IB Gateway container.
#
# Why: IB Gateway auto-restarts itself nightly at 23:59 (IBC AutoRestartTime).
# That restart occasionally hangs at a login/message dialog — the container
# stays "Up" but the API port never opens, and every strategy is dead until
# someone notices (2026-07-04 → 10h outage). The in-container services can't
# fix this: they have no docker socket, so only the host can bounce the
# gateway container.
#
# What it does (designed to run from cron every 5 minutes):
#   * If the gateway container isn't running at all → log, do nothing
#     (restart policy is unless-stopped; a stopped container was stopped on
#     purpose and the watchdog must not fight that).
#   * If the container is running but the API port has been closed for
#     FAIL_THRESHOLD consecutive checks (~15 min — comfortably longer than a
#     healthy auto-restart) → docker restart it, at most once per
#     RESTART_COOLDOWN_SECS so a genuinely broken gateway doesn't get
#     bounce-looped.
#
# Install (idempotent):
#   (crontab -l 2>/dev/null | grep -v ib_gateway_watchdog; \
#    echo '*/5 * * * * /Users/joelpob/dev/mmr/scripts/ib_gateway_watchdog.sh') | crontab -

set -u
PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:$PATH"

CONTAINER="mmr-ib-gateway-1"
# INTERNAL Java API port (paper 4002, live 4001). We probe this via docker
# exec instead of the host-mapped port: socat keeps the host port (7497)
# accepting even when the Java process is dead, so a host-port check reads
# "healthy" through a hung gateway — proven 2026-07-19, when the Saturday
# 23:59 auto-restart hung against IB weekend maintenance and this watchdog
# stayed satisfied for 8 hours while trader_service failed 226 reconnects.
WATCH_INTERNAL_PORT="${WATCH_INTERNAL_PORT:-4002}"
FAIL_THRESHOLD=3
RESTART_COOLDOWN_SECS=1800

STATE_DIR="$HOME/.local/share/mmr"
LOG_FILE="$STATE_DIR/logs/ib_gateway_watchdog.log"
FAIL_FILE="$STATE_DIR/ib_gateway_watchdog.failcount"
LAST_RESTART_FILE="$STATE_DIR/ib_gateway_watchdog.lastrestart"

mkdir -p "$STATE_DIR/logs"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG_FILE"; }

running=$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null || echo "absent")
if [ "$running" != "true" ]; then
    log "container $CONTAINER not running ($running) — not intervening"
    rm -f "$FAIL_FILE"
    exit 0
fi

if docker exec "$CONTAINER" bash -c "exec 3<>/dev/tcp/127.0.0.1/$WATCH_INTERNAL_PORT" 2>/dev/null; then
    # Healthy — the Java API itself is listening. Clear any failure streak;
    # log only the recovery edge.
    if [ -f "$FAIL_FILE" ]; then
        log "internal port $WATCH_INTERNAL_PORT reachable again (was down $(cat "$FAIL_FILE") checks)"
        rm -f "$FAIL_FILE"
    fi
    exit 0
fi

fails=$(( $(cat "$FAIL_FILE" 2>/dev/null || echo 0) + 1 ))
echo "$fails" > "$FAIL_FILE"
log "internal port $WATCH_INTERNAL_PORT closed (consecutive failures: $fails/$FAIL_THRESHOLD)"

if [ "$fails" -lt "$FAIL_THRESHOLD" ]; then
    exit 0
fi

now=$(date +%s)
last_restart=$(cat "$LAST_RESTART_FILE" 2>/dev/null || echo 0)
if [ $(( now - last_restart )) -lt "$RESTART_COOLDOWN_SECS" ]; then
    log "restart suppressed — last restart $(( now - last_restart ))s ago (< ${RESTART_COOLDOWN_SECS}s cooldown)"
    exit 0
fi

log "RESTARTING $CONTAINER (port down ${fails} consecutive checks)"
if docker restart "$CONTAINER" >> "$LOG_FILE" 2>&1; then
    echo "$now" > "$LAST_RESTART_FILE"
    rm -f "$FAIL_FILE"
    log "restart issued OK"
else
    log "restart FAILED (docker error) — will retry after cooldown"
    echo "$now" > "$LAST_RESTART_FILE"
fi
