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
chown trader:trader /home/trader/.mmr_env

# Ensure config dir exists with defaults
mkdir -p /home/trader/.config/mmr
cp -n /home/trader/mmr/configs/*.yaml /home/trader/.config/mmr/ 2>/dev/null || true
chown -R trader:trader /home/trader/.config/mmr

# Ensure data and log directories exist (bind-mounted from host ~/.local/share/mmr/)
mkdir -p /home/trader/.local/share/mmr/data
mkdir -p /home/trader/.local/share/mmr/logs

# Fix permissions — use chmod to avoid chown failures in podman rootless
chown -R trader:trader /home/trader/.local/share/mmr 2>/dev/null || chmod -R 777 /home/trader/.local/share/mmr

# Keep container running
exec tail -f /dev/null
