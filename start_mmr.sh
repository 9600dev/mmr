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
SETUP_MODE=false

# PIDs of child service processes
DATA_PID=""
TRADER_PID=""
STRATEGY_PID=""

# ─── Colors ──────────────────────────────────────────────────────────────────
# Enable colors on a TTY, or when FORCE_COLOR=1; disable when NO_COLOR is set.
if [ -n "$NO_COLOR" ]; then
    C_RESET="" C_BOLD="" C_DIM=""
    C_RED="" C_GREEN="" C_YELLOW="" C_BLUE="" C_MAGENTA="" C_CYAN=""
elif [ -t 1 ] || [ "${FORCE_COLOR:-0}" = "1" ]; then
    C_RESET=$'\033[0m'    C_BOLD=$'\033[1m'    C_DIM=$'\033[2m'
    C_RED=$'\033[31m'     C_GREEN=$'\033[32m'  C_YELLOW=$'\033[33m'
    C_BLUE=$'\033[34m'    C_MAGENTA=$'\033[35m' C_CYAN=$'\033[36m'
else
    C_RESET="" C_BOLD="" C_DIM=""
    C_RED="" C_GREEN="" C_YELLOW="" C_BLUE="" C_MAGENTA="" C_CYAN=""
fi

# Output helpers
hdr()   { printf '\n%s━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n'   "$C_CYAN$C_BOLD" "$C_RESET"
          printf '%s  %s%s\n' "$C_CYAN$C_BOLD" "$1" "$C_RESET"
          printf '%s━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━%s\n\n' "$C_CYAN$C_BOLD" "$C_RESET"; }
info()  { printf '%s%s%s\n' "$C_DIM" "$1" "$C_RESET"; }
step()  { printf '%s›%s %s\n' "$C_CYAN" "$C_RESET" "$1"; }
ok()    { printf '  %s✓%s %s\n' "$C_GREEN" "$C_RESET" "$1"; }
warn()  { printf '  %s!%s %s\n' "$C_YELLOW" "$C_RESET" "$1"; }
fail()  { printf '  %s✗%s %s\n' "$C_RED" "$C_RESET" "$1" >&2; }
kv()    { printf '  %-18s%s%s%s\n' "$1" "$C_BOLD" "$2" "$C_RESET"; }

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
    --setup)
        SETUP_MODE=true
        shift
        ;;
    -h|--help)
        echo "usage: start_mmr.sh [options]"
        echo ""
        echo "  --paper      Paper trading mode (default)"
        echo "  --live       Live trading mode"
        echo "  --config     Path to trader.yaml config file"
        echo "  --cli        CLI-only mode: verify services are running, launch mmr_cli"
        echo "  --setup      Run interactive setup wizard"
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

FIRST_RUN=false
if [ ! -d "$HOME/.config/mmr" ]; then
    mkdir -p "$HOME/.config/mmr"
    FIRST_RUN=true
    if [ -d "$MMR_DIR/config_defaults" ]; then
        cp -n "$MMR_DIR"/config_defaults/*.yaml "$HOME/.config/mmr/" 2>/dev/null || true
        echo "Copied default configs to ~/.config/mmr/"
    fi
elif [ ! -f "$TRADER_CONFIG" ] && [ -d "$MMR_DIR/config_defaults" ]; then
    cp -n "$MMR_DIR"/config_defaults/*.yaml "$HOME/.config/mmr/" 2>/dev/null || true
fi

if [ ! -f "$TRADER_CONFIG" ]; then
    fail "config file not found: $TRADER_CONFIG"
    exit 1
fi

# ─── Setup Wizard ─────────────────────────────────────────────────────────────

run_setup() {
    hdr "MMR Setup Wizard"
    kv "Config file:" "$TRADER_CONFIG"
    echo ""

    # ── Trading Mode ──
    local current_mode
    current_mode=$(grep '^trading_mode:' "$TRADER_CONFIG" | awk '{print $2}')
    echo "Trading mode determines which IB account and port are used."
    read -p "Trading mode (paper/live) [${current_mode:-paper}]: " mode
    mode="${mode:-${current_mode:-paper}}"
    sed -i.bak "s/^trading_mode:.*/trading_mode: ${mode}/" "$TRADER_CONFIG"

    # ── IB Gateway Connection ──
    echo ""
    echo "IB Gateway connection settings."
    echo "If running IB Gateway locally, the defaults are usually correct."
    echo ""

    local current_address
    current_address=$(grep '^ib_server_address:' "$TRADER_CONFIG" | awk '{print $2}')
    read -p "IB Gateway host [${current_address:-127.0.0.1}]: " ib_host
    ib_host="${ib_host:-${current_address:-127.0.0.1}}"
    sed -i.bak "s/^ib_server_address:.*/ib_server_address: ${ib_host}/" "$TRADER_CONFIG"

    # ── IB Accounts ──
    echo ""
    echo "IB account numbers. Paper accounts start with 'DU', live with 'U'."
    echo "You can find these in TWS/Gateway under Account > Account Number."
    echo ""

    local current_paper
    current_paper=$(grep '^ib_paper_account:' "$TRADER_CONFIG" | awk '{print $2}')
    read -p "Paper trading account [${current_paper}]: " paper_acct
    paper_acct="${paper_acct:-${current_paper}}"
    sed -i.bak "s/^ib_paper_account:.*/ib_paper_account: ${paper_acct}/" "$TRADER_CONFIG"

    local current_live
    current_live=$(grep '^ib_live_account:' "$TRADER_CONFIG" | awk '{print $2}')
    read -p "Live trading account (leave empty to skip) [${current_live}]: " live_acct
    live_acct="${live_acct:-${current_live}}"
    if [ -n "$live_acct" ] && [ "$live_acct" != "''" ]; then
        sed -i.bak "s/^ib_live_account:.*/ib_live_account: ${live_acct}/" "$TRADER_CONFIG"
    fi

    # ── IB Ports ──
    echo ""
    echo "IB Gateway API ports. Defaults: 7497 (paper), 7496 (live)."
    echo "Docker maps 4003→7497 and 4004→7496 — use the host-side ports."
    echo ""

    local current_paper_port current_live_port
    current_paper_port=$(grep '^ib_paper_port:' "$TRADER_CONFIG" | awk '{print $2}')
    current_live_port=$(grep '^ib_live_port:' "$TRADER_CONFIG" | awk '{print $2}')
    read -p "Paper trading port [${current_paper_port:-7497}]: " paper_port
    paper_port="${paper_port:-${current_paper_port:-7497}}"
    sed -i.bak "s/^ib_paper_port:.*/ib_paper_port: ${paper_port}/" "$TRADER_CONFIG"

    read -p "Live trading port [${current_live_port:-7496}]: " live_port
    live_port="${live_port:-${current_live_port:-7496}}"
    sed -i.bak "s/^ib_live_port:.*/ib_live_port: ${live_port}/" "$TRADER_CONFIG"

    # ── Massive API Key ──
    echo ""
    echo "Massive.com (Polygon.io) API key for US market data."
    echo "Required for: ideas scanner, historical data downloads, news, fundamentals."
    echo "Get one at: https://massive.com or https://polygon.io"
    echo ""

    local current_key
    current_key=$(grep '^massive_api_key:' "$TRADER_CONFIG" | awk '{print $2}' | tr -d "'\"")
    if [ -n "$current_key" ]; then
        # Mask all but last 4 chars
        local masked="${current_key:0:4}...${current_key: -4}"
        read -p "Massive/Polygon API key [${masked}]: " api_key
    else
        read -p "Massive/Polygon API key (leave empty to skip): " api_key
    fi
    if [ -n "$api_key" ]; then
        sed -i.bak "s/^massive_api_key:.*/massive_api_key: '${api_key}'/" "$TRADER_CONFIG"
    fi

    # ── DuckDB Path ──
    echo ""
    local current_db
    current_db=$(grep '^duckdb_path:' "$TRADER_CONFIG" | awk '{print $2}')
    read -p "DuckDB storage path [${current_db:-~/.local/share/mmr/data/mmr.duckdb}]: " db_path
    db_path="${db_path:-${current_db:-~/.local/share/mmr/data/mmr.duckdb}}"
    sed -i.bak "s|^duckdb_path:.*|duckdb_path: ${db_path}|" "$TRADER_CONFIG"

    # ── Cleanup sed backups ──
    rm -f "${TRADER_CONFIG}.bak"

    hdr "Setup complete"
    kv "Config saved to:" "$TRADER_CONFIG"
    kv "Trading mode:"    "$mode"
    kv "IB Gateway:"      "$ib_host (paper:$paper_port / live:$live_port)"
    kv "Paper account:"   "$paper_acct"
    if [ -n "$live_acct" ] && [ "$live_acct" != "''" ]; then
        kv "Live account:" "$live_acct"
    fi
    if [ -n "$api_key" ] || [ -n "$current_key" ]; then
        kv "Massive API key:" "configured"
    else
        kv "Massive API key:" "not set (US scanning/data disabled)"
    fi
    kv "DuckDB:"          "$db_path"
    echo ""
    info "Run again anytime with: ./start_mmr.sh --setup"
    echo ""
}

# Auto-trigger setup on first run (non-Docker), or if --setup flag
if [ "$SETUP_MODE" = true ]; then
    run_setup
    exit 0
fi

if [ "$FIRST_RUN" = true ] && [ ! -f "$HOME/.mmr_env" ]; then
    echo ""
    echo "First run detected. Running setup wizard..."
    echo "(You can re-run this later with: ./start_mmr.sh --setup)"
    run_setup
    read -p "Start services now? [Y/n]: " start_now
    case "${start_now:-y}" in
        n|N|no) echo "Exiting. Start services later with: ./start_mmr.sh"; exit 0 ;;
    esac
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
    step "Shutting down services..."
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
            warn "force killing PID $pid"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
    ok "all services stopped"
    exit 0
}

trap cleanup SIGINT SIGTERM

# ─── CLI Mode ────────────────────────────────────────────────────────────────

if [ "$CLI_MODE" = true ]; then
    hdr "MMR CLI Mode"

    # Check IB Gateway container is running
    if command -v docker &>/dev/null && docker compose -f "$MMR_DIR/docker-compose.yml" ps ib-gateway 2>/dev/null | grep -q "running"; then
        ok "IB Gateway: running"
    else
        warn "IB Gateway: not detected (container may not be running)"
    fi

    # Check services are reachable via TCP
    SERVICES_OK=true
    for port_info in "42001:trader_service" "42003:data_service" "42005:strategy_service"; do
        port="${port_info%%:*}"
        name="${port_info##*:}"
        if check_tcp_port 127.0.0.1 "$port"; then
            ok "$name (port $port): reachable"
        else
            fail "$name (port $port): NOT reachable"
            SERVICES_OK=false
        fi
    done

    if [ "$SERVICES_OK" = false ]; then
        echo ""
        fail "one or more services are not running"
        info "Start services first with: ./start_mmr.sh"
        exit 1
    fi

    echo ""
    step "all services reachable — starting CLI..."
    echo ""
    cd "$MMR_DIR"
    exec $PY -m trader.mmr_cli
fi

# ─── Normal Mode ─────────────────────────────────────────────────────────────

hdr "MMR — Starting Services"
kv "Config:"       "$TRADER_CONFIG"
kv "Trading mode:" "$TRADING_MODE"
kv "IB host:"      "$IB_HOST"
kv "IB port:"      "$IB_PORT"
if [ "$IN_DOCKER" = true ]; then
    kv "Environment:" "Docker container"
fi
echo ""

cd "$MMR_DIR"

# ─── Start IB Gateway ───────────────────────────────────────────────────────

if [ "$IN_DOCKER" = true ]; then
    info "Running inside Docker — IB Gateway is a sibling container, skipping local start."
else
    step "Starting IB Gateway container..."
    ./docker.sh -i
fi

# ─── Wait for IB Gateway Readiness ──────────────────────────────────────────

# Phase 1: TCP port check
echo ""
step "Waiting for IB Gateway TCP port (${C_BOLD}${IB_HOST}:${IB_PORT}${C_RESET})..."
ELAPSED=0
TIMEOUT=120
while [ $ELAPSED -lt $TIMEOUT ]; do
    if check_tcp_port "$IB_HOST" "$IB_PORT"; then
        ok "$IB_HOST:$IB_PORT is accepting connections (${ELAPSED}s)"
        break
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
    if [ $((ELAPSED % 10)) -eq 0 ]; then
        info "  still waiting... (${ELAPSED}s)"
    fi
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    fail "IB Gateway did not open port $IB_HOST:$IB_PORT within ${TIMEOUT}s"
    info "Check IB Gateway logs: docker compose logs ib-gateway"
    exit 1
fi

# Phase 2: IB API handshake check (connect + managedAccounts).
#
# IBC takes a few seconds after the TCP port opens to accept the paper-
# trading disclaimer and finish the API auth handshake, so the first try
# often fails with error 10141 or "clientId X already in use" from a
# lingering previous connection. We give IB a warm-up window, rotate the
# clientId on every attempt, and only surface errors if we ultimately
# time out — otherwise the expected transient failure looks alarming.
echo ""
step "Verifying IB Gateway API connectivity..."
info "  giving IB ${C_BOLD}8s${C_RESET}${C_DIM} to finish authentication / disclaimer handling..."
sleep 8

ELAPSED=8
TIMEOUT=180
LAST_ERROR=""
while [ $ELAPSED -lt $TIMEOUT ]; do
    # Random clientId per attempt so a lingering prior connection doesn't
    # bounce us with "clientId X already in use".
    CID=$(( (RANDOM % 40000) + 2000 ))
    HC_OUTPUT=$($PY -c "
from ib_async import IB
ib = IB()
try:
    ib.connect('$IB_HOST', $IB_PORT, clientId=$CID, timeout=8, readonly=True)
    if ib.isConnected():
        print('CONNECTED')
    else:
        print('NOT_CONNECTED')
    ib.disconnect()
except Exception as e:
    print(f'FAILED: {e}')
" 2>&1) || true
    if echo "$HC_OUTPUT" | grep -q "CONNECTED"; then
        ok "IB Gateway API connection verified (${ELAPSED}s)"
        break
    fi
    LAST_ERROR="$HC_OUTPUT"
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    if [ $((ELAPSED % 15)) -eq 0 ]; then
        info "  still waiting... (${ELAPSED}s)"
    fi
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    fail "IB Gateway API not reachable after ${TIMEOUT}s"
    info "Port is open but IB may not be fully authenticated."
    info "Last output: $LAST_ERROR"
    exit 1
fi

# ─── Launch Services ─────────────────────────────────────────────────────────

# Ensure data and log directories exist
mkdir -p ~/.local/share/mmr/data ~/.local/share/mmr/logs

echo ""
step "Launching services..."
echo ""

# data_service
step "Starting data_service..."
$PY -m trader.data_service &
DATA_PID=$!

sleep 3

# trader_service
step "Starting trader_service..."
$PY -m trader.trader_service &
TRADER_PID=$!

sleep 5

# strategy_service
step "Starting strategy_service..."
$PY -m trader.strategy_service &
STRATEGY_PID=$!

echo ""
hdr "All services running"
kv "data_service"     "PID $DATA_PID"
kv "trader_service"   "PID $TRADER_PID"
kv "strategy_service" "PID $STRATEGY_PID"
echo ""
info "Press Ctrl-C to stop all services"
info "Run './start_mmr.sh --cli' in another terminal for the CLI"
info "IB Gateway VNC: vnc://localhost:5901"
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
            fail "$name (PID $pid) exited with code $EXIT_CODE"
            # Clear the PID so we don't report it again
            case "$name" in
                data_service)     DATA_PID="" ;;
                trader_service)   TRADER_PID="" ;;
                strategy_service) STRATEGY_PID="" ;;
            esac
            # If all services are dead, exit
            if [ -z "$DATA_PID" ] && [ -z "$TRADER_PID" ] && [ -z "$STRATEGY_PID" ]; then
                fail "all services have exited"
                exit 1
            fi
        fi
    done
    sleep 2
done
