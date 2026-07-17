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
RESTART_ALL=false
DOCKER_MODE=false
NEWS_MODE=false
NEWS_DIR="${NEWS_DIR:-$HOME/dev/news}"

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

# Render an API-key status row. Renders one of:
#   ✓ green "set (...XXXX)"     — value present and non-empty
#   ✓ green "set (env override)" — env var set, masking yaml value
#   ✗ red   "not set"           — empty / unset / placeholder
#
# $1 = display label ("Massive (Polygon)")
# $2 = trader.yaml key ("massive_api_key")
# $3 = matching env var name ("MASSIVE_API_KEY")
# $4 = path to trader.yaml (so we can pass $TRADER_CONFIG)
api_key_status() {
    local label="$1" yaml_key="$2" env_var="$3" yaml_file="$4"
    local env_val="${!env_var:-}"
    local yaml_val=""
    if [ -f "$yaml_file" ]; then
        # Find the first matching `key: value` line, strip the key/colon
        # prefix, strip surrounding single or double quotes, strip trailing
        # whitespace. Empty / unset / placeholder ("") values come out as
        # the empty string and render as "not set" below.
        yaml_val=$(
            grep -E "^[[:space:]]*${yaml_key}[[:space:]]*:" "$yaml_file" 2>/dev/null \
                | head -n1 \
                | sed -E "s/^[[:space:]]*${yaml_key}[[:space:]]*:[[:space:]]*//" \
                | sed -E 's/^"//;  s/"$//'                                          \
                | sed -E "s/^'//;  s/'$//"                                          \
                | sed -E 's/[[:space:]]+$//'
        )
    fi

    local status_color status_glyph status_text
    if [ -n "$env_val" ]; then
        status_color="$C_GREEN"
        status_glyph="✓"
        status_text="set via \$$env_var (...${env_val: -4})"
    elif [ -n "$yaml_val" ]; then
        status_color="$C_GREEN"
        status_glyph="✓"
        # Show last 4 chars only — full key in scrollback is a leak hazard.
        if [ ${#yaml_val} -gt 4 ]; then
            status_text="set (...${yaml_val: -4})"
        else
            status_text="set"
        fi
    else
        status_color="$C_RED"
        status_glyph="✗"
        # Make the "not set" diagnostic actionable: the most common gotcha
        # is `MY_KEY=foo` in the user's interactive shell rc instead of
        # `export MY_KEY=foo`. Without `export`, child processes (this
        # script) don't inherit the variable.
        status_text="not set (export \$$env_var or set ${yaml_key} in trader.yaml)"
    fi

    printf "  %-18s%s%s %s%s\n" "$label" "$status_color" "$status_glyph" "$status_text" "$C_RESET"
}

# Print all the data-feed API keys in one block.
#
# Sources, in priority order:
#   1. Exported env var (MASSIVE_API_KEY, TWELVEDATA_API_KEY)
#   2. ~/.config/mmr/secrets.env if it exists (sourced for KEY=VALUE lines).
#      This is a convenient way to keep keys out of trader.yaml (which
#      gets copied around) and out of shell history.
#   3. The matching key in trader.yaml.
print_api_keys() {
    local yaml_file="${1:-$TRADER_CONFIG}"

    # Optionally pull in keys from a sidecar secrets file. We `set -a`
    # so plain KEY=VALUE lines auto-export; this only persists for the
    # life of the function (subshell scoping via the local block above
    # would defeat the purpose, so we accept the export side-effect —
    # children launched after print_api_keys will inherit them too).
    local secrets_file="$HOME/.config/mmr/secrets.env"
    if [ -f "$secrets_file" ]; then
        set -a
        # shellcheck disable=SC1090
        source "$secrets_file"
        set +a
    fi

    printf '  %sData feed API keys%s\n' "$C_DIM" "$C_RESET"
    api_key_status "Massive/Polygon"  "massive_api_key"     "MASSIVE_API_KEY"     "$yaml_file"
    api_key_status "TwelveData"       "twelvedata_api_key"  "TWELVEDATA_API_KEY"  "$yaml_file"
}

# Bring up the sibling news scraper docker stack (~/dev/news by default,
# override with NEWS_DIR=...). Used by the `news` skill which talks to
# http://127.0.0.1:8089. Idempotent: news/docker.sh -u reuses the
# existing container; `--news -g` does a build → down → up cycle.
#
# We deliberately never invoke `news/docker.sh -g` directly from here:
# that command ends with `compose logs -f` and would block start_mmr.sh
# forever, preventing the rest of the MMR boot from running. Build +
# down + up gives the same end-state without tailing.
start_news_stack() {
    if [ "$NEWS_MODE" != true ]; then
        return 0
    fi
    if [ -f "$HOME/.mmr_env" ]; then
        # The news stack lives on the host. If we're inside the MMR
        # container the host's docker socket isn't reachable here.
        warn "--news ignored: running inside MMR container (start news from host)"
        return 0
    fi
    if [ ! -d "$NEWS_DIR" ]; then
        fail "--news: NEWS_DIR=$NEWS_DIR not found"
        info "  clone the news repo to ~/dev/news, or set NEWS_DIR=/path/to/news"
        exit 1
    fi
    if [ ! -x "$NEWS_DIR/docker.sh" ]; then
        fail "--news: $NEWS_DIR/docker.sh not found or not executable"
        exit 1
    fi

    hdr "Starting news scraper stack"
    kv "News dir:" "$NEWS_DIR"

    local rc=0
    if [ "$RESTART_ALL" = true ]; then
        step "Clean rebuild + restart of news stack (-g)..."
        ( cd "$NEWS_DIR" && ./docker.sh -b && ./docker.sh -d && ./docker.sh -u )
        rc=$?
    else
        step "Bringing news stack up (idempotent)..."
        ( cd "$NEWS_DIR" && ./docker.sh -u )
        rc=$?
    fi

    if [ $rc -ne 0 ]; then
        fail "news startup failed (rc=$rc)"
        info "  see $NEWS_DIR/docker.sh, or run there directly:"
        info "    cd $NEWS_DIR && ./docker.sh -g"
        exit $rc
    fi

    ok "news service up — health check: curl http://127.0.0.1:8089/v1/health"
    info "  logs:   (cd $NEWS_DIR && ./docker.sh -l)"
    info "  shell:  (cd $NEWS_DIR && ./docker.sh -e)"
    info "  stop:   (cd $NEWS_DIR && ./docker.sh -d)"
    echo ""
}

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
    --docker)
        DOCKER_MODE=true
        shift
        ;;
    --news)
        # Bring up the news scraper docker stack at $NEWS_DIR. Additive:
        # combines with any other mode (hybrid / --docker / --cli).
        NEWS_MODE=true
        shift
        ;;
    --setup)
        SETUP_MODE=true
        shift
        ;;
    -g|--restart-all)
        # Tear down the entire MMR stack (Python services + IB Gateway
        # container) before starting fresh. Use this when something is
        # wedged and you want a clean slate.
        RESTART_ALL=true
        shift
        ;;
    -h|--help)
        cat <<'USAGE'
usage: start_mmr.sh [options]

Modes (pick at most one):
  (default)            Hybrid mode — IB Gateway runs in a container, the
                       three Python services (data, trader, strategy) run
                       as child processes on the host via `uv run python`.
                       Best for day-to-day development.

  --docker             All-in-Docker — bring up the full containerized
                       stack via `./docker.sh -u` (IB Gateway + MMR
                       container + services). After it's up, prints how
                       to exec in or tail logs. Best for "just run it"
                       or production-like setups.

  --cli                CLI-only — verify services are running (locally on
                       127.0.0.1:42001/42003/42005) and launch the
                       interactive `trader.mmr_cli`. Run this in a second
                       terminal once a normal `./start_mmr.sh` is up.

  --setup              Run the interactive setup wizard, write
                       ~/.config/mmr/trader.yaml, then exit. Auto-runs on
                       first launch.

Add-ons (combine with any mode above):
  --news               Also bring up the news scraper container stack
                       (sibling repo, default $HOME/dev/news; override
                       with NEWS_DIR=...). Idempotent: reuses the
                       container if it's already running. Combine with
                       `-g` to do a clean rebuild + restart of news.
                       Used by the `news` skill — see skills/news/.

Trading mode (host services only):
  --paper              Paper trading (default)
  --live               Live trading
  --config <path>      Override trader.yaml location
                       (default: $HOME/.config/mmr/trader.yaml)

Lifecycle:
  -g, --restart-all    Clean restart. Behavior depends on mode:
                         hybrid:    SIGINT/KILL leftover host Python
                                    services, `./docker.sh -d` to drop
                                    the IB Gateway container, then bring
                                    everything back up fresh.
                         --docker:  invokes `./docker.sh -g` — rebuild
                                    MMR image, compose down, compose up,
                                    exec into the container.
                       Use when something is wedged or after a code/
                       Dockerfile change.
  -h, --help           Show this help

Notes:
  Hybrid mode is idempotent: if IB Gateway is already listening on its
  port, the existing container is reused; otherwise it's started via
  `./docker.sh -i` (auto-detects docker vs podman).

  --docker mode is a thin wrapper around `./docker.sh -u`. For build +
  exec, use `./docker.sh -g` directly.
USAGE
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

# ─── News Add-On (--news) ────────────────────────────────────────────────────
#
# Brought up before the main mode branches so it works in --docker, --cli,
# and hybrid flows alike. No-op when --news wasn't passed.
start_news_stack

# ─── Docker Mode ─────────────────────────────────────────────────────────────
#
# `--docker` is a thin wrapper around `./docker.sh -u`: it brings up the
# full containerized stack (IB Gateway + MMR container + services) and
# leaves the host shell free. We delegate everything — credential prompts,
# runtime detection (docker vs podman), VM RAM check, network cleanup —
# to docker.sh, which already does all of it. After compose returns we
# print a short "what now" footer so the user knows how to drive the
# stack from here.

if [ "$DOCKER_MODE" = true ]; then
    if [ -f "$HOME/.mmr_env" ]; then
        # If somebody runs `start_mmr.sh --docker` *inside* the MMR
        # container, that's almost certainly a mistake (you'd be asking
        # the container to start its own host-side container stack).
        # Refuse loudly rather than silently doing nothing useful.
        echo "Error: --docker was passed but we're already running inside" >&2
        echo "       the MMR container. Run --docker from the host shell." >&2
        exit 1
    fi
    cd "$MMR_DIR"
    if [ "$RESTART_ALL" = true ]; then
        # `--docker -g` = clean rebuild + restart + exec in. Maps to
        # `./docker.sh -g`, which does: build MMR image -> compose down
        # -> compose up -> exec into the MMR container. After exec the
        # user is inside the container, so we don't print a footer.
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "  MMR — Clean rebuild + restart of full container stack"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        print_api_keys "$TRADER_CONFIG"
        echo ""
        exec ./docker.sh -g
    fi
    # Plain `--docker` = idempotent up. Reuses existing containers when
    # they're healthy. Use `--docker -g` for a forced clean rebuild.
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  MMR — Starting full stack in containers"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    print_api_keys "$TRADER_CONFIG"
    echo ""
    ./docker.sh -u
    rc=$?
    if [ $rc -ne 0 ]; then
        echo ""
        echo "docker.sh -u exited with code $rc" >&2
        exit $rc
    fi
    echo ""
    echo "Next steps:"
    echo "  ./start_mmr.sh --docker -g   # clean rebuild + restart + exec in"
    echo "  ./docker.sh -e               # shell into the MMR container"
    echo "  ./docker.sh -l               # tail logs from all containers"
    echo "  ./docker.sh -d               # stop and remove all containers"
    echo ""
    exit 0
fi

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
    echo "Docker maps container 4003→host 7496 (live) and 4004→host 7497 (paper)."
    echo "Use the host-side ports (7496 / 7497) here."
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

# ─── Container Runtime Detection ────────────────────────────────────────────
#
# We delegate all container actions to ./docker.sh, which already auto-detects
# docker vs podman. But for the lightweight "is the container running?" probe
# in CLI mode and for the log-hint message, we need to know which compose
# command exists on this machine. Mirror docker.sh's selection logic but stay
# silent — if neither runtime is available we just leave RUNTIME / COMPOSE
# empty and the probe falls back to "not detected".
RUNTIME=""
COMPOSE=""
if [ "$IN_DOCKER" != true ]; then
    if command -v docker &>/dev/null && docker info &>/dev/null; then
        RUNTIME=docker
        COMPOSE="docker compose"
    elif command -v podman &>/dev/null && podman info &>/dev/null; then
        RUNTIME=podman
        if podman compose version &>/dev/null; then
            COMPOSE="podman compose"
        elif command -v podman-compose &>/dev/null; then
            COMPOSE="podman-compose"
        fi
    fi
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

# Portable TCP-port probe with a ~1s budget.
#
# The previous version used `timeout 1 bash -c '...'` which silently broke on
# stock macOS (GNU `timeout` isn't shipped, so the call returns 127 / "command
# not found", `check_tcp_port` always reports "closed", and the IB-Gateway
# readiness wait times out even when the gateway is happily listening). Now:
#   1. Prefer `nc -z -w 1` (BSD/GNU netcat both support `-w`; ships with macOS
#      and most Linux distros).
#   2. Fall back to bash's `/dev/tcp` builtin. For a localhost target the
#      kernel returns ECONNREFUSED instantly, so the lack of an explicit
#      timeout is fine; for a firewalled remote host the call may block for
#      the kernel default (~75s) but that's still better than always-false.
check_tcp_port() {
    local host=$1 port=$2
    if command -v nc &>/dev/null; then
        nc -z -w 1 "$host" "$port" &>/dev/null
    else
        (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null
    fi
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

    # Check IB Gateway container is running. We try two independent signals:
    #   1. Native runtime probe (`docker ps` / `podman ps` filtered by name).
    #      Avoids `compose ps <service>` because podman-compose doesn't accept
    #      a service argument, and we don't want to special-case it.
    #   2. TCP port probe — covers the case where the gateway was started by
    #      something other than this stack (or both runtimes are absent).
    # Either signal alone is enough to call it "running".
    IB_GW_RUNNING=false
    if [ -n "$RUNTIME" ] && $RUNTIME ps --filter "name=mmr[-_]ib-gateway" --format '{{.Names}}' 2>/dev/null | grep -q '.'; then
        IB_GW_RUNNING=true
    elif check_tcp_port "$IB_HOST" "$IB_PORT"; then
        IB_GW_RUNNING=true
    fi
    if [ "$IB_GW_RUNNING" = true ]; then
        ok "IB Gateway: running ($IB_HOST:$IB_PORT)"
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
print_api_keys "$TRADER_CONFIG"
echo ""

cd "$MMR_DIR"

# ─── Restart-All (-g) Teardown ───────────────────────────────────────────────
#
# `start_mmr.sh -g` is the "burn it down and start over" button. Useful when
# something is wedged: a stale Python service holding a port, a half-loaded
# IB Gateway, or you just want a clean slate. We:
#   1. SIGINT any leftover trader.{data,trader,strategy}_service processes
#      from a previous run (they may be orphaned if the parent terminal
#      was closed without Ctrl-C).
#   2. SIGKILL any survivors after a grace period.
#   3. Bring the IB Gateway container down via ./docker.sh -d (which is
#      already runtime-agnostic — works under docker or podman).
# After this, the "Start IB Gateway" block below sees no listening port
# and starts the container fresh.

if [ "$RESTART_ALL" = true ] && [ "$IN_DOCKER" != true ]; then
    hdr "Tearing down existing MMR stack (-g)"
    step "Stopping leftover MMR python services..."
    LEFTOVER_FOUND=false
    for svc in strategy_service trader_service data_service; do
        # Match `python -m trader.<svc>` or any path containing trader.<svc>.
        # Use pgrep -f to scan the whole command line; -lf prints PID+cmd.
        pids=$(pgrep -f "trader\.${svc}" 2>/dev/null || true)
        for p in $pids; do
            # Skip ourselves / our subshells (pgrep -f matches this script
            # too if we have the service name in our command line; we
            # don't, but be defensive).
            if [ "$p" = "$$" ] || [ "$p" = "$PPID" ]; then
                continue
            fi
            if kill -0 "$p" 2>/dev/null; then
                LEFTOVER_FOUND=true
                info "  SIGINT $svc (PID $p)"
                kill -INT "$p" 2>/dev/null || true
            fi
        done
    done
    if [ "$LEFTOVER_FOUND" = true ]; then
        # Grace period for clean shutdown.
        WAIT=0
        while [ $WAIT -lt 8 ]; do
            STILL_ALIVE=false
            for svc in strategy_service trader_service data_service; do
                if pgrep -f "trader\.${svc}" >/dev/null 2>&1; then
                    STILL_ALIVE=true
                fi
            done
            [ "$STILL_ALIVE" = false ] && break
            sleep 1
            WAIT=$((WAIT + 1))
        done
        # Force-kill survivors.
        for svc in strategy_service trader_service data_service; do
            for p in $(pgrep -f "trader\.${svc}" 2>/dev/null || true); do
                if kill -0 "$p" 2>/dev/null; then
                    warn "  SIGKILL $svc (PID $p)"
                    kill -9 "$p" 2>/dev/null || true
                fi
            done
        done
        ok "leftover services stopped"
    else
        ok "no leftover services to stop"
    fi

    step "Stopping IB Gateway container..."
    if ./docker.sh -d 2>&1 | sed 's/^/  /'; then
        ok "IB Gateway container stopped"
    else
        warn "./docker.sh -d returned non-zero (continuing anyway)"
    fi
    echo ""
fi

# ─── Start IB Gateway ───────────────────────────────────────────────────────
#
# Idempotent: if the gateway port is already open, trust the existing
# container (the phase-2 API handshake below will catch any half-loaded
# state and fail loudly). Otherwise delegate to ./docker.sh -i, which
# auto-detects docker vs podman and force-recreates the container with
# the latest image + .env values.

if [ "$IN_DOCKER" = true ]; then
    info "Running inside Docker — IB Gateway is a sibling container, skipping local start."
elif check_tcp_port "$IB_HOST" "$IB_PORT"; then
    ok "IB Gateway already listening on $IB_HOST:$IB_PORT — reusing existing container"
    info "  (use './start_mmr.sh -g' to force a fresh restart of everything)"
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
    if [ -n "$COMPOSE" ]; then
        info "Check IB Gateway logs: $COMPOSE -f $MMR_DIR/docker-compose.yml logs ib-gateway"
    else
        info "Check IB Gateway logs: docker compose logs ib-gateway  (or podman compose ...)"
    fi
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
#
# DNS workaround for podman rootless + aardvark-dns:
# Python's asyncio getaddrinfo can intermittently return EAI_NONAME for a
# sibling container's compose service name even when `nc` and `getent
# hosts` both resolve it fine (observed surfacing as
# `gaierror(-2, 'Name or service not known')`). We pre-resolve via
# `getent hosts` (libc NSS, same path nc uses) and feed the literal IP
# to ib_async so the asyncio resolver is never invoked.
IB_HOST_FOR_PY="$IB_HOST"
if [ "$IN_DOCKER" = true ] && [[ ! "$IB_HOST" =~ ^[0-9.]+$ ]] && [[ ! "$IB_HOST" =~ : ]]; then
    RESOLVED_IP=$(getent hosts "$IB_HOST" 2>/dev/null | awk '{print $1}' | head -n1)
    if [ -n "$RESOLVED_IP" ]; then
        IB_HOST_FOR_PY="$RESOLVED_IP"
        info "  resolved $IB_HOST -> $RESOLVED_IP (sidestepping asyncio DNS issue)"
    fi
fi

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
    ib.connect('$IB_HOST_FOR_PY', $IB_PORT, clientId=$CID, timeout=8, readonly=True)
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

# ─── Phase 3: IB Data-Farm Health ───────────────────────────────────────────
#
# The API handshake passing tells us nothing about whether IB Gateway can
# actually return market data. The data subsystem is a separate session
# (the "data farms") that connects post-login and can be in any of:
#   * OK        — usfarm, ushmds, secdefil reported 2104/2106/2158
#   * BROKEN    — 2103/2105 ("Market data farm connection is broken:..."),
#                 usually downstream of a session conflict (error 162)
#   * LAZY-OK   — 2107/2108 ("inactive but available upon demand"), normal
#                 idle state for farms not currently in use
#   * CONNECTING — 2119, transient; should resolve to OK within ~30 s
#
# Without this probe, a broken farm state surfaces only as opaque snapshot
# timeouts 30 minutes later — exactly the failure mode that bit us at
# 17:14 today (usfarm broken, ushmds broken, snapshot('AAPL') timed out).
#
# We don't fail startup on a degraded farm — false-positive risk is too
# high (LAZY state can look like absence-of-OK in a short window) and
# the operator may want to start services anyway and watch logs. We just
# warn loudly so the failure mode is named and pinned to the right cause.

step "Probing IB data-farm health..."
FARM_OUTPUT=$($PY -m trader.tools.ib_health \
    --host "$IB_HOST_FOR_PY" --port "$IB_PORT" --timeout 10 2>&1) || FARM_RC=$?
FARM_RC=${FARM_RC:-0}
echo "$FARM_OUTPUT" | sed "s/^/  /"
if [ "$FARM_RC" -eq 0 ]; then
    ok "data farms healthy"
elif [ "$FARM_RC" -eq 1 ]; then
    warn "data farms DEGRADED — services will start but snapshots / history may fail"
    info "  See diagnosis above. Most common fix: log out of IBKR Mobile / TWS"
    info "  on other devices, then './docker.sh -r' to restart the Gateway."
else
    warn "data-farm probe could not connect (rc=$FARM_RC) — continuing anyway"
fi
echo ""

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

sleep 3

# web_dashboard — read-only view (currencies, positions, strategies, proposals)
# on port 7424. PYTHONPATH so `web` resolves whether or not it's pip-installed.
step "Starting web_dashboard..."
PYTHONPATH="$MMR_DIR${PYTHONPATH:+:$PYTHONPATH}" WEB_PORT="${WEB_PORT:-7424}" $PY -m web.app &
WEB_PID=$!

# pycron (cron-only) — the services above are launched directly, so load ONLY
# the scheduled jobs from pycron.yaml (nightly db_backup, data_refresh_*).
# Without this nothing ever fires them. Its web port (8081) doubles as a
# double-start guard: a second instance fails the bind and exits before
# scheduling anything.
PYCRON_PID=""
if [ "$IN_DOCKER" = true ]; then
    step "Starting pycron (cron jobs: db_backup, data_refresh_us, data_refresh_asx)..."
    $PY -m pycron.pycron --config "$HOME/.config/mmr/pycron.yaml" \
        -s db_backup -s data_refresh_us -s data_refresh_asx \
        --no-health-check \
        >> "$HOME/.local/share/mmr/logs/pycron_cron.log" 2>&1 &
    PYCRON_PID=$!
fi

echo ""
hdr "All services running"
kv "data_service"     "PID $DATA_PID"
kv "trader_service"   "PID $TRADER_PID"
kv "strategy_service" "PID $STRATEGY_PID"
kv "web_dashboard"    "PID $WEB_PID (http://localhost:${WEB_PORT:-7424})"
if [ -n "$PYCRON_PID" ]; then
    kv "pycron (cron)" "PID $PYCRON_PID"
fi
echo ""
info "Press Ctrl-C to stop all services"
info "Run './start_mmr.sh --cli' in another terminal for the CLI"
info "IB Gateway VNC: vnc://localhost:5901"
echo ""

# ─── Wait for Children ───────────────────────────────────────────────────────

# Monitor child processes — report if any die unexpectedly
while true; do
    for pid_info in "$DATA_PID:data_service" "$TRADER_PID:trader_service" "$STRATEGY_PID:strategy_service" "$WEB_PID:web_dashboard" "$PYCRON_PID:pycron"; do
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
                web_dashboard)    WEB_PID="" ;;
                pycron)           PYCRON_PID="" ;;
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
