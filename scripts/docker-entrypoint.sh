#!/bin/bash
# MMR container entrypoint
# Sets IB Gateway connection env vars based on TRADING_MODE, then idles

# Default IB Gateway connection (overridable via env vars)
IB_SERVER_ADDRESS="${IB_SERVER_ADDRESS:-ib-gateway}"

# Set the API port based on trading mode
# IB Gateway internal ports: 4003 = live, 4004 = paper
if [ "${TRADING_MODE:-paper}" = "paper" ]; then
    IB_SERVER_PORT="${IB_SERVER_PORT:-4004}"
else
    IB_SERVER_PORT="${IB_SERVER_PORT:-4003}"
fi

echo "MMR starting: IB_SERVER_ADDRESS=$IB_SERVER_ADDRESS IB_SERVER_PORT=$IB_SERVER_PORT TRADING_MODE=${TRADING_MODE:-paper}"

# Write env vars to a file that .bash_profile and pycron can source.
# This ensures SSH sessions and child processes see them.
cat > /home/trader/.mmr_env <<EOF
export IB_SERVER_ADDRESS="$IB_SERVER_ADDRESS"
export IB_SERVER_PORT="$IB_SERVER_PORT"
export TRADING_MODE="${TRADING_MODE:-paper}"
export IB_ACCOUNT="${IB_ACCOUNT:-}"
export TRADER_CONFIG="${TRADER_CONFIG:-/home/trader/.config/mmr/trader.yaml}"
export ZMQ_RPC_SERVER_ADDRESS="${ZMQ_RPC_SERVER_ADDRESS:-tcp://127.0.0.1}"
export ZMQ_PUBSUB_SERVER_ADDRESS="${ZMQ_PUBSUB_SERVER_ADDRESS:-tcp://127.0.0.1}"
export ZMQ_STRATEGY_RPC_SERVER_ADDRESS="${ZMQ_STRATEGY_RPC_SERVER_ADDRESS:-tcp://127.0.0.1}"
export ZMQ_MESSAGEBUS_SERVER_ADDRESS="${ZMQ_MESSAGEBUS_SERVER_ADDRESS:-tcp://127.0.0.1}"
export ZMQ_DATA_RPC_SERVER_ADDRESS="${ZMQ_DATA_RPC_SERVER_ADDRESS:-tcp://127.0.0.1}"
EOF
# The entrypoint runs as trader (Dockerfile `USER trader`), so these chowns
# are normally no-ops that succeed trivially. They are kept — and made
# non-fatal — so the script still works if the container is run as root
# (e.g. `docker run -u 0`), where they do real work. A chown of a file this
# uid does not own fails; that is never a reason to abort startup.
chown trader:trader /home/trader/.mmr_env 2>/dev/null || true

# Ensure config dir exists with defaults
mkdir -p /home/trader/.config/mmr
cp -n /home/trader/mmr/config_defaults/*.yaml /home/trader/.config/mmr/ 2>/dev/null || true
chown -R trader:trader /home/trader/.config/mmr 2>/dev/null || true

# Ensure data and log directories exist (bind-mounted from host ~/.local/share/mmr/)
mkdir -p /home/trader/.local/share/mmr/data
mkdir -p /home/trader/.local/share/mmr/logs

# Fix permissions — use chmod to avoid chown failures in podman rootless.
# Both arms are best-effort: running as trader, any file left behind by an
# earlier root-era container (there are pre-existing root-owned log files)
# can be neither chowned nor chmodded by us, and that must not abort
# startup — new files are created by this uid and are fine.
chown -R trader:trader /home/trader/.local/share/mmr 2>/dev/null \
    || chmod -R 777 /home/trader/.local/share/mmr 2>/dev/null \
    || true

# Source the env file we just wrote and launch services. start_mmr.sh
# launches data/trader/strategy as children and waits in a monitor loop —
# becomes PID 1, so the container exits if all services die (and Docker's
# restart policy can react).
. /home/trader/.mmr_env
export TRADER_CONFIG="${TRADER_CONFIG:-/home/trader/.config/mmr/trader.yaml}"
exec /home/trader/mmr/start_mmr.sh
