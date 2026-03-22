<p align="center">
  <img src="docs/logo-1.png" alt="MMR - Make Me Rich" width="700">
</p>

<h1 align="center">MMR — Make Me Rich</h1>

<p align="center">
  <strong>An LLM-native algorithmic trading platform for Interactive Brokers</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-%3E%3D3.12-blue" alt="Python">
  <img src="https://img.shields.io/badge/IB-Gateway%20%2F%20TWS-red" alt="Interactive Brokers">
  <img src="https://img.shields.io/badge/storage-DuckDB-yellow" alt="DuckDB">
  <img src="https://img.shields.io/badge/messaging-ZeroMQ-green" alt="ZeroMQ">
  <img src="https://img.shields.io/badge/license-Apache%202.0%20%2B%20Commons%20Clause-lightgrey" alt="License">
</p>

---

MMR is a Python trading platform built to be operated by both humans and LLMs. It connects to Interactive Brokers via [ib_async](https://github.com/ib-api-reloaded/ib_async), uses ZeroMQ for inter-service messaging, DuckDB for storage, and exposes every operation as a JSON-returning CLI command — making it a natural fit for LLM agents that trade autonomously.

## Why LLM-Native?

Most trading platforms are built for humans staring at charts. MMR is built for an LLM sitting in a loop:

- **Propose → Review → Approve pipeline** — the LLM never places a trade directly. It creates proposals with confidence scores and reasoning, reviews the auto-computed position sizing, checks portfolio risk, then approves or rejects. A human can review pending proposals at any time.
- **Every command returns JSON** (`--json` flag) — structured data the LLM can parse without scraping Rich tables.
- **ATR-inverse position sizing** — the LLM doesn't need to reason about how much to buy. Volatile stocks automatically get smaller positions; stable stocks get larger ones. The sizing pipeline: `base × risk_multiplier × confidence × ATR_volatility_adjustment`.
- **Portfolio snapshots & diffs** — compact (~500 token) state summaries designed for context-window efficiency. The LLM calls `portfolio-snapshot` every cycle and `portfolio-diff` to see what changed — if nothing moved, it skips analysis entirely.
- **Risk reports before and after** — `portfolio-risk` returns concentration (HHI), group budget compliance, correlation clusters, and plain-English warnings. Run it before proposing to catch existing issues; run it after to catch issues the new trade would create.
- **Position groups with budgets** — tag trades into named groups (e.g. "mining" at 20% max allocation) so the LLM can manage sector exposure without manual tracking.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Claude Code / LLM Agent                        │
│  CLAUDE.md (context) + mmr CLI --json (tools)                       │
│  MONITOR → ANALYZE → PROPOSE → DIGEST → sleep → repeat             │
└────────────────────────────┬────────────────────────────────────────┘
                             │ Bash: mmr --json <command>
┌────────────────────────────▼────────────────────────────────────────┐
│                         mmr_cli / sdk.py                            │
│              80+ commands, JSON output, prompt_toolkit REPL         │
└──────┬──────────────────┬──────────────────┬───────────────────────┘
       │ ZMQ RPC          │ ZMQ RPC          │ ZMQ PubSub
       ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐
│trader_service│  │ data_service │  │  strategy_service    │
│  port 42001  │  │  port 42003  │  │  port 42005          │
│              │  │              │  │                      │
│ IBAIORx ─────┼──┼──► IB Gateway│  │ Strategy runtime     │
│ Executioner  │  │  Massive.com │  │ RxPY pipelines       │
│ Portfolio    │  │  DuckDB      │  │ MessageBus (42006)   │
│ Risk Gate    │  │              │  │                      │
│ Book Subject │  │              │  │                      │
└──────────────┘  └──────────────┘  └──────────────────────┘
       │                                      │
       │          ┌──────────────┐             │
       └──────────┤  IB Gateway  ├─────────────┘
                  │  (ib_async)  │
                  └──────────────┘
```

### Services

| Service | Port | Role |
|---------|------|------|
| **trader_service** | 42001 (RPC), 42002 (PubSub) | Trading runtime — order execution, portfolio, market data streaming, risk gate |
| **data_service** | 42003 (RPC) | Historical data downloads (Massive.com + IB), DuckDB storage |
| **strategy_service** | 42005 (RPC), 42006 (MessageBus) | Loads and runs strategies, receives tick streams, emits signals |
| **mmr_cli** | — | Interactive REPL or one-shot commands, connects to services via ZMQ |

### Messaging (ZeroMQ)

All inter-service communication uses ZeroMQ with msgpack serialization — no HTTP, no web frameworks. Three patterns:

- **RPC** (DEALER/ROUTER) — synchronous request/reply. CLI calls `trader_service` methods via `@rpcmethod` decorated handlers. `RPCClient[T]` uses `__getattr__` chaining so calls look like `client.rpc().place_order(...)`.
- **PubSub** (PUB/SUB) — one-way broadcast of live ticker data. Topic filtering at the socket level, zero server-side bookkeeping. Ideal for high-frequency market data.
- **MessageBus** (DEALER/ROUTER with subscription tracking) — targeted routing for strategy signals. The server knows who subscribes to which topics and routes accordingly.

### Storage (DuckDB)

Short-lived connections (connect → execute → close) so multiple processes can share the same database file:

- **tick_data** — time-series OHLCV bars
- **object_store** — dill-serialized Python objects
- **event_store** — trading event audit trail (signals, orders, fills, rejections)
- **proposal_store** — trade proposals with status tracking
- **position_groups** — named groups with allocation budgets

### Market Data

| Source | Coverage | Speed | Use Case |
|--------|----------|-------|----------|
| **Massive.com** (Polygon.io) | US equities | ~4s full scan | Primary for US — server-side indicators, batch snapshots, fundamentals, news with sentiment |
| **IB APIs** | International (ASX, TSE, SEHK, EU, etc.) | ~30-90s | Scanner, snapshots, reqHistoricalData. Falls back here for non-US markets |

Yahoo Finance is explicitly not used.

## Claude Code Integration

MMR is designed to be operated by [Claude Code](https://docs.anthropic.com/en/docs/claude-code) as an autonomous trading agent. The `CLAUDE.md` file provides Claude with complete platform context — architecture, all 80+ CLI commands, the propose/approve pipeline, risk management, and the LLM trading loop workflow. Claude interacts with MMR entirely through the `mmr` CLI with `--json` output via Bash.

### How It Works

Claude Code reads `CLAUDE.md` on startup, giving it full knowledge of the platform. It operates MMR by running CLI commands:

```bash
# Claude runs these via Bash tool
mmr --json portfolio-snapshot     # ~500 tokens — compact state
mmr --json portfolio-diff         # what changed since last check
mmr --json ideas momentum         # scan for opportunities
mmr --json propose AAPL BUY --market --confidence 0.7 --group tech \
  --reasoning "Strong momentum, RSI 65"
mmr --json portfolio-risk         # check risk before approving
mmr approve 42                    # execute the trade
```

No special SDK or helper library needed — Claude Code's Bash tool is the entire integration surface.

### Autonomous Trading Loop

The recommended workflow for autonomous trading follows a phased cycle:

```
MONITOR → ANALYZE → PROPOSE → DIGEST → (sleep) → repeat
```

**MONITOR** — `portfolio-snapshot` + `portfolio-diff`. If nothing moved (all positions unchanged), skip to DIGEST. ~500 tokens per cycle.

**ANALYZE** — `portfolio-risk` for warnings, `ideas` with rotating presets (momentum → mean-reversion → breakout → volatile → gap-down), `news` for held positions.

**PROPOSE** — `propose` with auto-sizing, group tagging, confidence scores, and reasoning. Never auto-executes — proposals wait for review.

**DIGEST** — save observations to Claude Code memory, sleep until next cycle.

The loop can be run using Claude Code's `/loop` command or by asking Claude to monitor on an interval. Claude can be interrupted at any time to approve/reject proposals, answer questions, or adjust strategy.

### Context Efficiency

Every command in the loop is designed for minimal token usage:

| Command | Tokens | Purpose |
|---------|--------|---------|
| `portfolio-snapshot` | ~500 | Total value, P&L, top movers |
| `portfolio-diff` | ~200-500 | Only what changed since last cycle |
| `portfolio-risk` | ~800 | HHI, warnings, group budgets |
| `session` | ~300 | Remaining capacity, sizing config |
| `ideas momentum` | ~1000 | Top 10 ranked opportunities |

Compare to `portfolio` which returns ~2000+ tokens of full position detail. The snapshot/diff commands exist specifically so the LLM can monitor efficiently without burning context.

## The Propose → Approve Pipeline

The LLM (or human) never places trades directly. Instead:

```bash
# 1. Create a proposal with reasoning
mmr propose AAPL BUY --market --confidence 0.7 --group tech \
  --reasoning "Strong momentum, RSI 65, above 200-day MA"

# 2. Review — check auto-computed sizing
mmr proposals show 42
# Shows: base $5K × 1.0 risk × 0.7 confidence × 0.85 ATR_adj = $2,975

# 3. Check risk impact
mmr portfolio-risk

# 4. Approve or reject
mmr approve 42
mmr reject 42 --reason "Group over budget"
```

Position sizing is automatic: `base_position × risk_multiplier × confidence_scale × ATR_volatility_adjustment`. Volatile stocks (high ATR%) get smaller positions; stable stocks get larger ones. Configured in `configs/position_sizing.yaml`.

## Installation

### Requirements

- Python >= 3.12
- Interactive Brokers account with [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php) or TWS
- Market data subscriptions for target exchanges

### From Source

```bash
git clone https://github.com/9600dev/mmr.git
cd mmr
pip install -e ".[test]"
```

### Docker (Recommended)

Two-container setup: IB Gateway sidecar + MMR application.

```bash
# First run — prompts for IB credentials, writes .env
./docker.sh -g

# Individual commands
./docker.sh -b    # Build image
./docker.sh -u    # Start containers
./docker.sh -d    # Stop containers
./docker.sh -s    # Sync code to running container
./docker.sh -e    # SSH into container
./docker.sh -l    # Tail logs
./docker.sh -c    # Clean images/volumes
./docker.sh -i    # IB Gateway only (for local dev)
```

### Configuration

On first run, `start_mmr.sh` automatically launches an interactive setup wizard that walks you through:

- **Trading mode** — paper or live
- **IB Gateway connection** — host and port
- **IB account numbers** — paper (DU...) and live (U...)
- **Massive.com API key** — for US market data, scanning, and fundamentals
- **DuckDB storage path**

Settings are saved to `~/.config/mmr/trader.yaml`. Re-run the wizard anytime:

```bash
./start_mmr.sh --setup
```

Docker users get a separate credential prompt via `./docker.sh -g` that writes IB Gateway login credentials to `.env` (gitignored).

### Starting Services

```bash
# All services (auto-starts IB Gateway container)
./start_mmr.sh              # Paper trading (default)
./start_mmr.sh --live        # Live trading
./start_mmr.sh --cli         # CLI only, no services

# Individual services
trader-service               # or: python -m trader.trader_service
strategy-service             # or: python -m trader.strategy_service
data-service                 # or: python -m trader.data_service
mmr                          # or: python -m trader.mmr_cli
```

## CLI Reference

The `mmr` CLI is an interactive REPL (via prompt_toolkit) or accepts one-shot commands. All commands support `--json` for machine-readable output.

### Portfolio & Account

```bash
portfolio                    # Positions with P&L
portfolio-snapshot           # Compact JSON (~500 tokens) — alias: psnap
portfolio-diff               # Delta since last snapshot — alias: pdiff
portfolio-risk               # Concentration, correlation, group budgets — alias: prisk
orders                       # Open orders
status                       # Service health + PnL
session                      # Sizing config, remaining capacity
```

### Trading

```bash
buy AMD --market --amount 100.0
buy EUR --sectype CASH --market --quantity 20000
sell AMD --market --quantity 10
cancel 123                   # Cancel order by ID
cancel-all                   # Cancel all orders
close 1                      # Close position by row number
```

### Proposals

```bash
propose AMD BUY --market --quantity 100 --bracket 180 150
propose AAPL BUY --limit 165 --amount 5000 --trailing-stop-pct 2.0
propose BHP BUY --market --confidence 0.7 --group mining --exchange ASX --currency AUD
proposals                    # List pending
proposals --all              # All statuses
proposals show 3             # Full detail
approve 3                    # Execute
reject 3 --reason "Thesis changed"
```

### Position Groups

```bash
group list                   # Groups with members + allocation %
group create mining --budget 20
group add mining BHP RIO FMG
group set mining --budget 25
group show mining
group remove mining BHP
group delete mining
```

### Scanning & Ideas

```bash
# US via Massive.com (~4s)
ideas                        # Momentum (default)
ideas gap-up                 # Gap-up preset
ideas mean-reversion         # Mean-reversion preset
ideas breakout               # Breakout preset
ideas volatile               # Volatile/scalping
ideas momentum --tickers AAPL MSFT AMD NVDA
ideas momentum --universe sp500
ideas momentum --fundamentals --news --detail

# International via IB (~30-90s)
ideas momentum --location STK.AU.ASX --tickers BHP CBA CSL
ideas gap-up --location STK.HK.SEHK --tickers 0700 0005

# IB scanner
scan                         # Top gainers (default)
scan losers / scan active / scan hot-volume
scan --instrument ETF --location STK.US

# Market movers
movers                       # Stock gainers
movers --market crypto --losers
```

### Market Data

```bash
resolve AMD                  # Symbol → conId
resolve EURUSD --sectype CASH
snapshot AMD                 # Price snapshot
depth AAPL                   # Level 2 + PNG chart
depth BHP --exchange ASX --currency AUD
depth AAPL --rows 10 --smart
listen AMD                   # Stream live ticks
watch                        # Live portfolio monitor
stream AAPL MSFT AMD         # Massive.com stream
stream EURUSD --feed forex --quotes
```

### Historical Data

```bash
history list                 # Downloaded data inventory
history massive --symbol AAPL --bar_size "1 day" --prev_days 30
history ib --symbol AAPL --bar_size "1 min" --prev_days 5
```

### Options

```bash
options expirations AAPL
options chain AAPL --expiration 2026-03-20 --type call
options chain AAPL -e 2026-03-20 --strike-min 200 --strike-max 250
options snapshot O:AAPL260320C00250000
options implied AAPL -e 2026-03-20
options buy AAPL -e 2026-03-20 -s 250 -r C -q 5 --market
```

### News & Fundamentals

```bash
news AAPL --detail           # Headlines + sentiment
movers                       # Market movers
```

### Forex

```bash
forex snapshot EURUSD
forex quote EUR USD
forex snapshot-all
forex movers / forex movers --losers
forex convert EUR USD 1000
```

### Universes

```bash
universe list                # All universes with counts
universe show sp500
universe create my_universe
universe add my_universe AAPL MSFT AMD
universe import my_universe symbols.csv
universe remove my_universe MSFT
universe delete my_universe
```

### Strategies

```bash
strategies                   # List all strategies
strategies enable my_strat
strategies disable my_strat
```

### Portfolio Resizing

```bash
resize-positions --max-bound 500000          # Trim to $500K
resize-positions --min-bound 300000          # Grow to $300K
resize-positions --max-bound 500000 --dry-run
```

## Writing a Strategy

Subclass `Strategy` and implement `on_prices()`:

```python
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class MyStrategy(Strategy):
    def on_prices(self, prices):
        # prices: DataFrame with open, high, low, close, volume, vwap (DatetimeIndex)
        if some_buy_condition(prices):
            return Signal(source_name=self.name, action=Action.BUY, probability=0.8)
        return None
```

Register in `configs/strategy_runtime.yaml`:

```yaml
strategies:
  - name: my_strategy
    module: strategies.my_strategy
    class_name: MyStrategy
    bar_size: "1 min"
    conids: [265598]  # Verify with: mmr resolve AAPL
    historical_days_prior: 5
```

## Risk Management

MMR has multiple layers of risk controls:

- **Risk Gate** (`risk_gate.py`) — pre-trade enforcement of max position size, daily loss limit, open order count, and signal rate limits via event store lookback
- **Position Sizing** (`position_sizing.py`) — ATR-inverse volatility adjustment, confidence scaling, liquidity checks (max 2% of ADV, spread penalty)
- **Portfolio Risk** (`portfolio_risk.py`) — HHI concentration index, position weight warnings (>10% warning, >15% critical), group budget compliance, return correlation cluster detection (>30% correlated exposure)
- **Trading Filter** (`trading_filter.py`) — symbol/exchange denylist and allowlist enforcement
- **Position Groups** (`position_groups.py`) — named groups with allocation budgets (e.g. "mining" at 20% max)

## Configuration

User configs live in `~/.config/mmr/` (auto-copied from `configs/` on first run):

| File | Purpose |
|------|---------|
| `trader.yaml` | IB connection, DuckDB path, ZMQ ports |
| `position_sizing.yaml` | Base size, risk level, ATR params, hard limits |
| `trading_filters.yaml` | Symbol/exchange denylist and allowlist |
| `strategy_runtime.yaml` | Strategy definitions (module, class, conids, bar_size) |
| `pycron.yaml` | Service scheduling and auto-restart |
| `logging.yaml` | Rich console + rotating file handlers |

Environment variables override config values (uppercased parameter name). The `TRADER_CONFIG` env var overrides the config file path.

## Project Structure

```
mmr/
├── trader/                        # Core library
│   ├── trader_service.py          # Trading runtime entry point
│   ├── strategy_service.py        # Strategy runtime entry point
│   ├── data_service.py            # Data service entry point
│   ├── mmr_cli.py                 # CLI REPL (80+ commands)
│   ├── sdk.py                     # ZMQ RPC client wrapper
│   ├── container.py               # DI container
│   ├── config.py                  # Typed config dataclasses
│   ├── objects.py                 # Domain enums (Action, BarSize, etc.)
│   ├── common/                    # Logging, helpers, RxPY utilities
│   ├── data/                      # DuckDB stores, universe, market data
│   ├── listeners/                 # IB + Massive.com data adapters
│   ├── messaging/                 # ZMQ RPC, PubSub, MessageBus
│   ├── trading/                   # Runtime, executioner, risk, sizing, proposals
│   ├── strategy/                  # Strategy runtime
│   ├── simulation/                # Backtester
│   └── tools/                     # Idea scanner, depth chart, options chain
├── strategies/                    # User strategy implementations
├── configs/                       # Bundled defaults
├── CLAUDE.md                      # Claude Code context (architecture, commands, workflows)
├── tests/                         # 483 tests (pytest, no IB required)
├── docker-compose.yml             # IB Gateway + MMR containers
├── Dockerfile                     # Debian bookworm + Python venv
├── docker.sh                      # Docker/Podman build helper
├── start_mmr.sh                   # Service startup (tmux + health checks)
└── pyproject.toml                 # Package config + 58 dependencies
```

## Testing

All tests are unit tests using temporary DuckDB databases — no IB connection required.

```bash
pytest tests/ -v                   # Run all tests
pytest tests/test_book.py          # Single file
pytest tests/test_idea_scanner.py  # 123 tests covering scanner presets, indicators, IB path
```

483 tests across 19 files covering position sizing, risk gates, portfolio risk analysis, trade proposals, idea scanning, order book, serialization, backtesting, and more.

## Dependencies

Core: `ib_async`, `duckdb`, `pyzmq`, `msgpack`, `reactivex`, `pandas`, `numpy`, `pyarrow`, `rich`, `massive` (Polygon.io), `scikit-learn`, `vectorbt`, `exchange-calendars`, `matplotlib`

Full list in `pyproject.toml`. Install with `pip install -e .`

## License

Fair-code: Apache 2.0 with Commons Clause.
