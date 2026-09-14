#!/usr/bin/env bash
# Check Java's actual IB API listeners, not socat's always-open forwarding ports.
# No broker request, credentials or account data are needed for this probe.
set -eu

case "${TRADING_MODE:-}" in
    paper) ports=(4002) ;;
    live) ports=(4001) ;;
    both) ports=(4001 4002) ;;
    *) echo "unhealthy: unknown TRADING_MODE" >&2; exit 2 ;;
esac
if [ "${DUAL_MODE:-no}" = yes ]; then
    ports=(4001 4002)
fi

for port in "${ports[@]}"; do
    result=0
    timeout -k 1 3 bash -c 'exec 3<>/dev/tcp/127.0.0.1/"$1"' -- "$port" 2>/dev/null || result=$?
    case "$result" in
        0) ;;
        1|124|137)
            echo "unhealthy: IB API port $port unavailable" >&2
            exit 1
            ;;
        *) echo "unassessable: API probe failed (exit $result)" >&2; exit 2 ;;
    esac
done

echo "healthy: IB API listening"
