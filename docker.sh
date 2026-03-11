#!/bin/bash

set -o errexit -o pipefail -o noclobber -o nounset

# usually /home/trader/mmr
BUILDDIR=$(cd $(dirname "$0"); pwd)

# Detect container runtime.
# 1. Use whichever is already running.
# 2. If neither is running, try to start whichever is installed.
_setup_podman_compose() {
    if podman compose version &> /dev/null; then
        COMPOSE="podman compose"
    elif command -v podman-compose &> /dev/null; then
        COMPOSE="podman-compose"
    else
        echo "Error: podman found but no compose support."
        echo "Install it with: pip3 install podman-compose"
        exit 1
    fi
}

_try_start_runtime() {
    # Try Docker
    if command -v docker &> /dev/null; then
        echo "Docker found but not running. Attempting to start..."
        if [[ "$(uname)" == "Darwin" ]]; then
            open -a Docker 2>/dev/null || true
        elif command -v systemctl &> /dev/null; then
            sudo systemctl start docker 2>/dev/null || true
        fi
        for i in $(seq 1 30); do
            if docker info &> /dev/null; then
                echo "Docker started successfully."
                RUNTIME=docker; COMPOSE="docker compose"; return 0
            fi
            sleep 1
        done
    fi
    # Try Podman
    if command -v podman &> /dev/null; then
        echo "Podman found but not running. Attempting to start machine..."
        podman machine start 2>/dev/null || { podman machine init 2>/dev/null && podman machine start 2>/dev/null; } || true
        if podman info &> /dev/null; then
            echo "Podman machine started successfully."
            RUNTIME=podman; _setup_podman_compose; return 0
        fi
    fi
    return 1
}

# Check what's already running first
if command -v docker &> /dev/null && docker info &> /dev/null; then
    RUNTIME=docker
    COMPOSE="docker compose"
elif command -v podman &> /dev/null && podman info &> /dev/null; then
    RUNTIME=podman
    _setup_podman_compose
elif ! _try_start_runtime; then
    echo "Error: no working container runtime found."
    echo "  - docker: $(command -v docker &> /dev/null && echo 'installed' || echo 'not installed')"
    echo "  - podman: $(command -v podman &> /dev/null && echo 'installed' || echo 'not installed')"
    echo ""
    echo "Install Docker Desktop: https://docker.com/products/docker-desktop"
    echo "Or install podman: brew install podman"
    exit 1
fi

echo "Using container runtime: $RUNTIME"

echo_usage() {
    echo "usage: docker.sh -- helper script to manage MMR + IB Gateway containers"
    echo
    echo "  Uses docker-compose with IB Gateway sidecar container."
    echo "  Supports both docker and podman (auto-detected)."
    echo
    echo "  -b (build MMR image)"
    echo "  -u (up: start all containers — IB Gateway + MMR)"
    echo "  -i (ib-only: start only IB Gateway for local development)"
    echo "  -d (down: stop and remove all containers)"
    echo "  -c (clean: remove all images and containers)"
    echo "  -f (force clean: also remove build cache)"
    echo "  -s (sync code to running MMR container)"
    echo "  -a (sync all files to running MMR container)"
    echo "  -g (go: build, then start all containers and SSH in)"
    echo "  -l (logs: tail logs from all containers)"
    echo "  -e (exec: SSH into running MMR container)"
    echo ""
}

b=n c=n f=n u=n d=n s=n a=n g=n l=n e=n i=n

while [[ $# -gt 0 ]]; do
  case $1 in
    -b|--build)
      b=y
      shift
      ;;
    -u|--up)
      u=y
      shift
      ;;
    -d|--down)
      d=y
      shift
      ;;
    -c|--clean)
      c=y
      shift
      ;;
    -f|--force)
      f=y
      shift
      ;;
    -g|--go)
      g=y
      shift
      ;;
    -s|--sync)
      s=y
      shift
      ;;
    -a|--sync_all)
      a=y
      shift
      ;;
    -l|--logs)
      l=y
      shift
      ;;
    -e|--exec)
      e=y
      shift
      ;;
    -i|--ib-only)
      i=y
      shift
      ;;
    -*|--*)
      echo "Unknown option $1"
      echo_usage
      exit 1
      ;;
    *)
      shift
      ;;
  esac
done

# show usage if no action flags were set
if [[ $b == "n" && $c == "n" && $f == "n" && $u == "n" && $d == "n" && $s == "n" && $a == "n" && $g == "n" && $l == "n" && $e == "n" && $i == "n" ]]; then
    echo_usage
    exit 0
fi

# Prompt for IB credentials and write .env file
setup_credentials() {
    echo ""
    echo "============================================================"
    echo "  MMR First-Time Setup"
    echo "============================================================"
    echo ""

    local userid password account trading_mode vnc_password timezone

    read -p "Interactive Brokers username: " userid
    read -s -p "Interactive Brokers password: " password
    echo ""
    read -p "IB account number (U... for live, DU... for paper): " account

    echo ""
    echo "Trading mode:"
    echo "  1) paper (default)"
    echo "  2) live"
    read -p "Select [1]: " mode_choice
    case "${mode_choice:-1}" in
        2|live) trading_mode="live" ;;
        *) trading_mode="paper" ;;
    esac

    read -p "Timezone [America/New_York]: " timezone
    timezone="${timezone:-America/New_York}"

    read -p "VNC password for IB Gateway GUI (leave empty to disable): " vnc_password

    cat > "$BUILDDIR/.env" <<EOF
# MMR Configuration — generated by docker.sh
# Edit this file to change credentials or settings.

# IB Gateway credentials
TWS_USERID=${userid}
TWS_PASSWORD=${password}

# Trading mode: paper or live
TRADING_MODE=${trading_mode}

# IB account number
IB_ACCOUNT=${account}

# Timezone
TIME_ZONE=${timezone}

# VNC password for IB Gateway GUI (empty = disabled)
VNC_SERVER_PASSWORD=${vnc_password}

# IB Gateway settings
TWS_ACCEPT_INCOMING=accept
READ_ONLY_API=no
TWOFA_TIMEOUT_ACTION=restart
RELOGIN_AFTER_TWOFA_TIMEOUT=yes
EXISTING_SESSION_DETECTED_ACTION=primaryoverride
AUTO_RESTART_TIME="11:59 PM"
ALLOW_BLIND_TRADING=no
EOF

    echo ""
    echo "Credentials saved to $BUILDDIR/.env"
    echo "You can edit this file later to change settings."
    echo ""
}

# check for .env file, prompt if missing or incomplete
check_env() {
    if [ ! -f "$BUILDDIR/.env" ]; then
        setup_credentials
    fi

    # Check if credentials are filled in
    source "$BUILDDIR/.env"
    if [ -z "${TWS_USERID:-}" ] || [ -z "${TWS_PASSWORD:-}" ]; then
        echo "IB credentials are not set in .env"
        read -p "Set up credentials now? [Y/n]: " answer
        case "${answer:-y}" in
            n|N|no) echo "Skipping — IB Gateway will not be able to log in." ;;
            *) setup_credentials ;;
        esac
    fi
}

build() {
    echo "Building MMR image..."
    echo ""
    $COMPOSE -f "$BUILDDIR/docker-compose.yml" build mmr
}

up() {
    check_env
    echo "Starting IB Gateway + MMR containers..."
    echo ""
    # Ensure bind-mount directories exist on host
    mkdir -p "$BUILDDIR/data" "$BUILDDIR/logs"
    # Remove stale network to avoid label mismatch errors (podman/docker-compose compat).
    # Must remove containers using the network first, then the network itself.
    for cid in $($RUNTIME ps -a -q --filter network=mmr_default 2>/dev/null); do
        $RUNTIME rm -f "$cid" 2>/dev/null || true
    done
    $RUNTIME network rm mmr_default 2>/dev/null || true
    $COMPOSE -f "$BUILDDIR/docker-compose.yml" up -d
    echo ""
    echo "Containers started. Use './docker.sh -e' to SSH in, or './docker.sh -l' for logs."
    echo "IB Gateway VNC available at localhost:5901 (if VNC_SERVER_PASSWORD is set in .env)"
}

ib_only() {
    check_env
    echo "Starting IB Gateway only (for local development)..."
    echo ""
    # Force recreate so .env changes are always picked up
    $COMPOSE -f "$BUILDDIR/docker-compose.yml" up -d --force-recreate ib-gateway
    echo ""
    source "$BUILDDIR/.env"
    local ib_port
    if [ "${TRADING_MODE:-paper}" = "paper" ]; then
        ib_port=4002
    else
        ib_port=4001
    fi
    echo "IB Gateway running."
    echo "  API:  localhost:$ib_port (${TRADING_MODE:-paper})"
    echo "  VNC:  localhost:5901 (if VNC_SERVER_PASSWORD is set)"
    echo ""
    echo "Configure your local trader.yaml:"
    echo "  ib_server_address: 127.0.0.1"
    echo "  ib_server_port: $ib_port"
    echo ""
    echo "Or run services directly:"
    echo "  python3 -m trader.trader_service"
    echo "  python3 -m trader.strategy_service"
    echo "  python3 -m trader.mmr_cli"
}

down() {
    echo "Stopping all containers..."
    $COMPOSE -f "$BUILDDIR/docker-compose.yml" down 2>/dev/null || true
    # Remove stale network to avoid label mismatch errors (podman/docker-compose compat)
    for cid in $($RUNTIME ps -a -q --filter network=mmr_default 2>/dev/null); do
        $RUNTIME rm -f "$cid" 2>/dev/null || true
    done
    $RUNTIME network rm mmr_default 2>/dev/null || true
}

clean() {
    echo "Stopping and removing all containers and images..."
    $COMPOSE -f "$BUILDDIR/docker-compose.yml" down --rmi all --volumes 2>/dev/null || true
}

force_clean() {
    clean
    echo "Cleaning build cache..."
    if [[ "$RUNTIME" == "docker" ]]; then
        docker builder prune --force
    else
        podman system prune --force
    fi
}

logs() {
    $COMPOSE -f "$BUILDDIR/docker-compose.yml" logs -f
}

ssh_in() {
    echo "SSH'ing into MMR container (password: 'trader')..."
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null trader@localhost -p 2222
}

sync() {
    echo "Syncing code directory to MMR container..."
    echo ""
    CONTID="$($RUNTIME ps -aqf name=mmr-mmr)"
    if [[ -z "$CONTID" ]]; then
      # try alternate name patterns
      CONTID="$($RUNTIME ps -aqf name=mmr_mmr || true)"
    fi
    if [[ -z "$CONTID" ]]; then
      echo "Can't find running MMR container"
      exit 1
    fi
    echo "container id: $CONTID"

    RSYNC_RSH="$RUNTIME exec -i"
    rsync -e "$RSYNC_RSH" -av --delete $BUILDDIR/ $CONTID:/home/trader/mmr/ --exclude='.git' --filter="dir-merge,- .gitignore"
}

sync_all() {
    echo "Syncing entire mmr directory to MMR container..."
    echo ""
    CONTID="$($RUNTIME ps -aqf name=mmr-mmr)"
    if [[ -z "$CONTID" ]]; then
      CONTID="$($RUNTIME ps -aqf name=mmr_mmr || true)"
    fi
    if [[ -z "$CONTID" ]]; then
      echo "Can't find running MMR container"
      exit 1
    fi
    echo "container id: $CONTID"

    RSYNC_RSH="$RUNTIME exec -i"
    rsync -e "$RSYNC_RSH" -av --delete $BUILDDIR/ $CONTID:/home/trader/mmr/ --exclude='.git'
}

echo "action: build=$b clean=$c force=$f up=$u down=$d sync=$s sync_all=$a go=$g logs=$l exec=$e ib-only=$i | runtime: $RUNTIME"

if [[ $b == "y" ]]; then
    build
fi
if [[ $d == "y" ]]; then
    down
fi
if [[ $c == "y" ]]; then
    clean
fi
if [[ $f == "y" ]]; then
    force_clean
fi
if [[ $u == "y" ]]; then
    up
fi
if [[ $i == "y" ]]; then
    ib_only
fi
if [[ $s == "y" ]]; then
    sync
fi
if [[ $a == "y" ]]; then
    sync_all
fi
if [[ $l == "y" ]]; then
    logs
fi
if [[ $e == "y" ]]; then
    ssh_in
fi
if [[ $g == "y" ]]; then
    build
    down 2>/dev/null || true
    up
    echo ""
    echo "Waiting for containers to start..."
    sleep 3
    ssh_in
fi
