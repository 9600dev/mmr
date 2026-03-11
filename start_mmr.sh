#!/bin/bash
#
# start_mmr.sh — start MMR services as foreground child processes
#
# Normal mode: starts IB Gateway, waits for connectivity, launches all services
# CLI mode:    verifies services are running, then launches mmr_cli
#

set -e

MMR_DIR="$(cd "$(dirname "$0")"; pwd)"
TRADER_CONFIG="${TRADER_CONFIG:-$HOME/.config/mmr/trader.yaml}"
TRADING_MODE=""
CLI_MODE=false

# PIDs of child service processes
DATA_PID=""
TRADER_PID=""
STRATEGY_PID=""

# ─── Argument Parsing ────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
  case $1 in
    --paper)
        TRADING_MODE="paper"
        shift
        ;;
    --live)
        TRADING_MODE="live"
        shift
        ;;
    --config)
        TRADER_CONFIG="$2"
        shift; shift
        ;;
    --cli)
        CLI_MODE=true
        shift
        ;;
    -h|--help)
        echo "usage: start_mmr.sh [options]"
        echo ""
        echo "  --paper      Paper trading mode (default)"
        echo "  --live       Live trading mode"
        echo "  --config     Path to trader.yaml config file"
        echo "  --cli        CLI-only mode: verify services are running, launch mmr_cli"
        echo "  -h, --help   Show this help"
        echo ""
        echo "Normal mode starts IB Gateway container, waits for connectivity,"
        echo "then launches data_service, trader_service, and strategy_service"
        echo "as child processes with interleaved output."
        echo ""
        echo "CLI mode checks that services are already running, then launches"
        echo "the interactive CLI."
        echo ""
        exit 0
        ;;
    *)
        echo "Unknown option: $1"
        echo "Run with --help for usage."
        exit 1
        ;;
  esac
done

# Default to paper if no mode specified
if [ -z "$TRADING_MODE" ]; then
    TRADING_MODE="paper"
fi

export TRADING_MODE

# ─── Config Setup ────────────────────────────────────────────────────────────

if [ ! -d "$HOME/.config/mmr" ]; then
    mkdir -p "$HOME/.config/mmr"
    if [ -d "$MMR_DIR/configs" ]; then
        cp -n "$MMR_DIR"/configs/*.yaml "$HOME/.config/mmr/" 2>/dev/null || true
        echo "Copied default configs to ~/.config/mmr/"
    fi
elif [ ! -f "$TRADER_CONFIG" ] && [ -d "$MMR_DIR/configs" ]; then
    cp -n "$MMR_DIR"/configs/*.yaml "$HOME/.config/mmr/" 2>/dev/null || true
fi

if [ ! -f "$TRADER_CONFIG" ]; then
    echo "Error: config file not found: $TRADER_CONFIG"
    exit 1
fi

# ─── Docker Detection ──────────────────────────────────────────────────────

# Source Docker env vars if running inside the MMR container
if [ -f "$HOME/.mmr_env" ]; then
    source "$HOME/.mmr_env"
fi

IN_DOCKER=false
if [ -f "$HOME/.mmr_env" ]; then
    # .mmr_env is written by docker-entrypoint.sh — its presence means we're in the container
    IN_DOCKER=true
fi

# Use 'uv run python' locally, 'python3' in Docker (no uv installed)
if [ "$IN_DOCKER" = true ]; then
    PY="python3"
else
    PY="uv run python"
fi

# ─── Apply Trading Mode ─────────────────────────────────────────────────────

# IB_SERVER_ADDRESS and IB_SERVER_PORT may already be set by .mmr_env (Docker)
# Fall back to localhost defaults for non-Docker
IB_HOST="${IB_SERVER_ADDRESS:-127.0.0.1}"

if [ -n "$IB_SERVER_PORT" ]; then
    IB_PORT="$IB_SERVER_PORT"
elif [ "$TRADING_MODE" = "live" ]; then
    IB_PORT=7496
else
    IB_PORT=7497
fi

# Update trader.yaml so Python config picks up the mode
sed -i.bak "s/^trading_mode:.*/trading_mode: ${TRADING_MODE}/" "$TRADER_CONFIG"
rm -f "${TRADER_CONFIG}.bak"

# ─── Helper Functions ────────────────────────────────────────────────────────

check_tcp_port() {
    local host=$1 port=$2
    timeout 1 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null
}

# ─── Signal Handling ─────────────────────────────────────────────────────────

cleanup() {
    echo ""
    echo "Shutting down services..."
    # Send SIGINT first (services handle it gracefully)
    for pid in $STRATEGY_PID $TRADER_PID $DATA_PID; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill -INT "$pid" 2>/dev/null || true
        fi
    done
    # Give them up to 10 seconds to exit gracefully
    WAIT=0
    while [ $WAIT -lt 10 ]; do
        ALL_DONE=true
        for pid in $STRATEGY_PID $TRADER_PID $DATA_PID; do
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                ALL_DONE=false
            fi
        done
        if [ "$ALL_DONE" = true ]; then
            break
        fi
        sleep 1
        WAIT=$((WAIT + 1))
    done
    # Force kill anything still alive
    for pid in $STRATEGY_PID $TRADER_PID $DATA_PID; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "  Force killing PID $pid..."
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
    echo "All services stopped."
    exit 0
}

trap cleanup SIGINT SIGTERM

# ─── CLI Mode ────────────────────────────────────────────────────────────────

if [ "$CLI_MODE" = true ]; then
    echo ""
    echo "--------------------------------------------"
    echo "  MMR CLI Mode"
    echo "--------------------------------------------"
    echo ""

    # Check IB Gateway container is running
    if command -v docker &>/dev/null && docker compose -f "$MMR_DIR/docker-compose.yml" ps ib-gateway 2>/dev/null | grep -q "running"; then
        echo "  IB Gateway: running"
    else
        echo "  IB Gateway: not detected (container may not be running)"
    fi

    # Check services are reachable via TCP
    SERVICES_OK=true
    for port_info in "42001:trader_service" "42003:data_service" "42005:strategy_service"; do
        port="${port_info%%:*}"
        name="${port_info##*:}"
        if check_tcp_port 127.0.0.1 "$port"; then
            echo "  $name (port $port): reachable"
        else
            echo "  $name (port $port): NOT reachable"
            SERVICES_OK=false
        fi
    done

    if [ "$SERVICES_OK" = false ]; then
        echo ""
        echo "Error: one or more services are not running."
        echo "Start services first with: ./start_mmr.sh"
        exit 1
    fi

    echo ""
    echo "All services reachable. Starting CLI..."
    echo ""
    cd "$MMR_DIR"
    exec $PY -m trader.mmr_cli
fi

# ─── Normal Mode ─────────────────────────────────────────────────────────────

echo ""
echo "--------------------------------------------"
echo "  MMR — Starting Services"
echo "--------------------------------------------"
echo ""
echo "  Config:       $TRADER_CONFIG"
echo "  Trading mode: $TRADING_MODE"
echo "  IB host:      $IB_HOST"
echo "  IB port:      $IB_PORT"
if [ "$IN_DOCKER" = true ]; then
    echo "  Environment:  Docker container"
fi
echo ""

cd "$MMR_DIR"

# ─── Start IB Gateway ───────────────────────────────────────────────────────

if [ "$IN_DOCKER" = true ]; then
    echo "Running inside Docker — IB Gateway is a sibling container, skipping local start."
else
    echo "Starting IB Gateway container..."
    ./docker.sh -i
fi

# ─── Wait for IB Gateway Readiness ──────────────────────────────────────────

# Phase 1: TCP port check
echo ""
echo "Waiting for IB Gateway TCP port ($IB_HOST:$IB_PORT)..."
ELAPSED=0
TIMEOUT=120
while [ $ELAPSED -lt $TIMEOUT ]; do
    if check_tcp_port "$IB_HOST" "$IB_PORT"; then
        echo "  $IB_HOST:$IB_PORT is accepting connections (${ELAPSED}s)"
        break
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
    if [ $((ELAPSED % 10)) -eq 0 ]; then
        echo "  Still waiting... (${ELAPSED}s)"
    fi
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "Error: IB Gateway did not open port $IB_HOST:$IB_PORT within ${TIMEOUT}s"
    echo "Check IB Gateway logs: docker compose logs ib-gateway"
    exit 1
fi

# Phase 2: IB API handshake check (connect + managedAccounts)
echo ""
echo "Verifying IB Gateway API connectivity..."
ELAPSED=0
TIMEOUT=120
while [ $ELAPSED -lt $TIMEOUT ]; do
    HC_OUTPUT=$($PY -c "
from ib_async import IB
ib = IB()
try:
    ib.connect('$IB_HOST', $IB_PORT, clientId=199, timeout=5, readonly=True)
    print('CONNECTED' if ib.isConnected() else 'FAILED')
    ib.disconnect()
except Exception as e:
    print(f'FAILED: {e}')
" 2>&1) || true
    if echo "$HC_OUTPUT" | grep -q "CONNECTED"; then
        echo "  IB Gateway API connection verified (${ELAPSED}s)"
        break
    fi
    if [ $ELAPSED -eq 0 ]; then
        echo "  First attempt: $HC_OUTPUT"
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    if [ $((ELAPSED % 15)) -eq 0 ]; then
        echo "  Still waiting... (${ELAPSED}s)"
    fi
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "Error: IB Gateway API not reachable after ${TIMEOUT}s"
    echo "Port is open but IB may not be fully authenticated."
    echo "Last output: $HC_OUTPUT"
    exit 1
fi

# ─── Launch Services ─────────────────────────────────────────────────────────

echo ""
echo "Launching services..."
echo ""

# data_service
echo "  Starting data_service..."
$PY -m trader.data_service &
DATA_PID=$!

sleep 3

# trader_service
echo "  Starting trader_service..."
$PY -m trader.trader_service &
TRADER_PID=$!

sleep 5

# strategy_service
echo "  Starting strategy_service..."
$PY -m trader.strategy_service &
STRATEGY_PID=$!

echo ""
echo "--------------------------------------------"
echo "  All services running"
echo "--------------------------------------------"
echo "  data_service     PID: $DATA_PID"
echo "  trader_service   PID: $TRADER_PID"
echo "  strategy_service PID: $STRATEGY_PID"
echo ""
echo "  Press Ctrl-C to stop all services"
echo "  Run './start_mmr.sh --cli' in another terminal for the CLI"
echo ""

# ─── Wait for Children ───────────────────────────────────────────────────────

# Monitor child processes — report if any die unexpectedly
while true; do
    for pid_info in "$DATA_PID:data_service" "$TRADER_PID:trader_service" "$STRATEGY_PID:strategy_service"; do
        pid="${pid_info%%:*}"
        name="${pid_info##*:}"
        if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
            wait "$pid" 2>/dev/null || true
            EXIT_CODE=$?
            echo ""
            echo "WARNING: $name (PID $pid) exited with code $EXIT_CODE"
            # Clear the PID so we don't report it again
            case "$name" in
                data_service)     DATA_PID="" ;;
                trader_service)   TRADER_PID="" ;;
                strategy_service) STRATEGY_PID="" ;;
            esac
            # If all services are dead, exit
            if [ -z "$DATA_PID" ] && [ -z "$TRADER_PID" ] && [ -z "$STRATEGY_PID" ]; then
                echo "All services have exited."
                exit 1
            fi
        fi
    done
    sleep 2
done
