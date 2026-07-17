#!/usr/bin/env bash
#
# Docker HEALTHCHECK probe for the mmr container.
#
# Health here means "the trading services answer RPC" — trader_service
# (42001 get_status) and strategy_service (42005 runtime_status) both reply
# within their deadlines. Deliberately NOT gated on IB connectivity: the
# gateway container has its own healthcheck, and `mmr verify` / the host
# watchdog own the IB-liveness question — a healthy mmr container with a
# dead gateway should read as gateway-unhealthy, not mmr-unhealthy.
#
# Note: Docker only REPORTS unhealthy (docker ps / inspect); with
# restart: unless-stopped it does not auto-restart on health alone. The
# value is visibility plus a hook for the host watchdog.
#
set -u

PY="${HOME}/.venv/bin/python3"
[ -x "$PY" ] || PY=python3

exec timeout 15 "$PY" - <<'EOF'
import asyncio
import sys

from trader.messaging.clientserver import RPCClient


def probe(port: int, method: str):
    client = RPCClient(
        zmq_server_address='tcp://127.0.0.1',
        zmq_server_port=port,
        timeout=5,
    )
    asyncio.run(client.connect())
    return getattr(client.rpc(return_type=dict), method)()


try:
    probe(42001, 'get_status')
    probe(42005, 'runtime_status')
except Exception as ex:
    print(f'unhealthy: {type(ex).__name__}: {ex}', file=sys.stderr)
    sys.exit(1)

print('healthy')
EOF
