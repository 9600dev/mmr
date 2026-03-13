# CLAUDE.md

## Project Overview

MMR (Make Me Rich) is a Python-based algorithmic trading platform for Interactive Brokers. It supports automated strategy execution, interactive CLI trading, historical data collection, real-time market data streaming, and idea scanning across US and international markets.

## Design Principles

**Precision over convenience**: This is a trading system — wrong data is worse than no data. Contract identifiers (conIds) must resolve exactly or fail. Never fall back to fuzzy matching, string coercion, or "close enough" lookups. If a conId isn't found, return an error. If a symbol resolves to the wrong exchange, that's a bug. Integer conIds passed to `resolve_symbol` must not be converted to string symbol lookups (e.g. conId `4391` must not become ticker `"4391"` which matches a Japanese stock on TSEJ).

**Fail loudly, not silently**: When an IB API call fails (scanner error 162, market data not subscribed, contract not found), surface the error to the caller. Don't swallow exceptions and return empty results — the user needs to know *why* something failed so they can fix it (subscribe to market data, use a different location code, etc.).

**Massive first, IB fallback**: For US markets, Massive.com (Polygon.io) is the primary data source — it's fast (~4s for a full scan), has server-side indicators, and doesn't require trader_service. For international markets (ASX, TSE, SEHK, etc.), fall back to IB's APIs (scanner, snapshots, reqHistoricalData). Don't use Yahoo Finance.

**No sentiment analysis on IB path**: IB's news API doesn't provide sentiment scoring. On the Massive path, sentiment comes from Polygon's insights. On the IB path, we only show the headline — no fake or estimated sentiment.

## Architecture

```
trader.trader_service ──► Trader (trading_runtime.py)
                           ├── IBAIORx (ibreactive.py) ──► IB Gateway (ib_async)
                           ├── TradeExecutioner, BookSubject, Portfolio
                           ├── ZMQ RPC Server (port 42001)
                           ├── ZMQ PubSub Publisher (port 42002)
                           └── ZMQ MessageBus (port 42006)

trader.strategy_service ──► StrategyRuntime (strategy_runtime.py)
                              ├── Loads strategies from strategy_runtime.yaml
                              ├── ZMQ RPC Client → trader_service
                              ├── ZMQ PubSub Subscriber ← tickers
                              └── ZMQ RPC Server (port 42005)

trader.data_service ──► DataService (data_service.py)
                          ├── Concurrent history downloads (asyncio + to_thread)
                          ├── MassiveHistoryWorker, IBHistoryWorker
                          └── ZMQ RPC Server (port 42003)

trader.mmr_cli ──► ZMQ RPC Client → trader_service / strategy_service
pycron/pycron.py ──► Process manager for all services
```

### Key Patterns

**Dependency injection**: `Container.resolve(Type)` introspects `__init__` parameter names, matches them against env vars (uppercased) then config YAML values, and constructs the instance. Constructor param names must match config keys.

**Messaging (pyzmq)**: All inter-process communication uses ZeroMQ (not HTTP). This was chosen over HTTP/REST for lower latency (important for trading), native support for pub/sub patterns, and no web framework dependency. Three socket patterns in `trader/messaging/clientserver.py`, each serving a distinct communication need:

- **RPC** (ports 42001, 42003, 42005): DEALER/ROUTER sockets with msgpack serialization. Synchronous request/reply — a client calls a method name with arguments and blocks for the result. Used by the CLI to call `trader_service` methods (place trades, query portfolio, etc.) and by `strategy_service` to submit orders to `trader_service`. Methods are marked with the `@rpcmethod` decorator on the service API classes (`trader_service_api.py`, `strategy_service_api.py`). The `RPCClient[T]` generic uses `__getattr__` chaining so calls look like `client.rpc().place_order(...)` — the method name is serialized as a string and dispatched on the server side.

- **PubSub** (port 42002): ZMQ PUB/SUB sockets for one-way broadcast of live ticker data. `trader_service` publishes; `strategy_service`, CLI, and TUI subscribe. Uses ZMQ's native topic filtering at the socket level — the publisher doesn't track subscribers and subscribers filter by topic prefix. This is the most efficient pattern for "blast market data to everyone" since there's no per-client routing overhead. Implemented as `MultithreadedTopicPubSub` which runs the PUB socket on a dedicated thread with an async queue for thread-safe writes from the main event loop.

- **MessageBus** (port 42006): DEALER/ROUTER sockets with explicit subscription tracking and topic-based routing. Unlike PubSub, the server knows who is subscribed to which topics and routes messages only to matching clients. Used for strategy signals — a strategy publishes a signal to a topic, and only clients subscribed to that topic receive it. This enables targeted communication between specific services rather than broadcast. The server maintains a `clients: Dict[(client_id, topic), bool]` registry and forwards messages accordingly.

The reason PubSub and MessageBus are separate (despite overlap) is efficiency: ZMQ's native PUB/SUB does topic filtering in the kernel/socket layer with zero server-side bookkeeping, which is ideal for high-frequency ticker data. The MessageBus trades that efficiency for per-client routing control, which strategy signals need but tickers don't.

**Serialization**: All ZMQ messages use msgpack with custom ExtType handlers for datetime, date, time, timedelta, pandas DataFrames (via PyArrow IPC), and a dill fallback for arbitrary Python objects. Defined in `clientserver.py` (`ext_pack`/`ext_unpack`).

**Storage (DuckDB)**: `trader/data/duckdb_store.py` uses short-lived connections (connect, execute, close) to allow multiple processes to share the same database file. Two tables: `tick_data` (time-series OHLCV) and `object_store` (dill-serialized blobs).

**Event store**: `trader/data/event_store.py` records trading events (signals, orders, fills, rejections) in DuckDB for audit trail and risk gate lookback.

**Risk gate**: `trader/trading/risk_gate.py` enforces pre-trade risk limits (max position size, daily loss, open orders, signal rate) by querying the event store.

**Position sizing**: `trader/trading/position_sizing.py` computes position sizes based on confidence, risk level, portfolio state, and liquidity (ADV, spread). Configured via `configs/position_sizing.yaml`. Used automatically by `propose` when no quantity/amount is specified.

**Trading filter**: `trader/trading/trading_filter.py` enforces symbol/exchange denylist/allowlist rules. Checked by the executioner and risk gate before any order placement.

**Portfolio resizing**: `trader/sdk.py` provides `compute_resize_deltas()` (pure function) and `compute_resize_plan()`/`execute_resize_plan()` (SDK methods) for proportionally scaling all positions to fit within a target portfolio value. The resize workflow: (1) compute scale factor from max/min bounds, (2) find associated protective orders (stops, trailing stops, take-profits) for each position, (3) cancel protective orders, (4) place market orders for position deltas, (5) re-create protective orders at new quantities preserving original prices. Exposed via `resize-positions` CLI command. The `place_standalone_order()` RPC method on `trading_runtime.py` supports placing standalone STP/TRAIL/LMT orders for existing positions (used to re-create protectives after resizing).

**Reactive streams (RxPY)**: `IBAIORx` converts IB events into RxPY Subjects/Observables. Strategies receive accumulated DataFrames via reactive pipelines.

## Project Structure

```
mmr/
├── pycron/pycron.py           # Process manager / scheduler
│
├── configs/                       # Bundled defaults (copied to ~/.config/mmr/ on first run)
│   ├── trader.yaml            # IB connection, ZMQ ports, DuckDB path
│   ├── pycron.yaml            # Service job definitions (Docker mode)
│   ├── no_docker_pycron.yaml  # Service job definitions (non-Docker)
│   ├── strategy_runtime.yaml  # Strategy definitions
│   ├── position_sizing.yaml   # Position sizing defaults (base size, risk level, limits)
│   ├── trading_filters.yaml   # Trading filter config (denylist, allowlist, exchanges)
│   └── logging.yaml           # Python logging config
│
├── trader/                    # Core library + entry points
│   ├── trader_service.py      # Entry point: trading runtime
│   ├── strategy_service.py    # Entry point: strategy runtime
│   ├── data_service.py        # Entry point: data download service (RPC + direct)
│   ├── mmr_cli.py             # Entry point: CLI REPL (argparse + prompt_toolkit)
│   ├── sdk.py                 # SDK used by mmr_cli (ZMQ RPC client wrapper)
│   ├── __main__.py            # python -m trader support
│   ├── container.py           # DI container (Singleton, YAML + env var resolution)
│   ├── config.py              # Typed config dataclasses (IBConfig, StorageConfig, ZMQConfig)
│   ├── objects.py             # Domain enums/dataclasses (Action, BarSize, etc.)
│   ├── common/
│   │   ├── singleton.py       # Singleton metaclass
│   │   ├── helpers.py         # Utility functions (dateify, rich_table, etc.)
│   │   ├── logging_helper.py  # Per-module logger setup from logging.yaml
│   │   ├── reactivex.py       # RxPY helpers (AnonymousObserver, EventSubject)
│   │   ├── listener_helpers.py
│   │   ├── exceptions.py      # TraderException, TraderConnectionException
│   │   ├── contract_sink.py
│   │   └── dataclass_cache.py # Cache with reactive update notifications
│   ├── data/
│   │   ├── store.py           # Abstract DataStore/ObjectStore + DateRange
│   │   ├── duckdb_store.py    # DuckDB implementations (DuckDBDataStore, DuckDBObjectStore)
│   │   ├── data_access.py     # TickData, DictData, TickStorage, SecurityDefinition
│   │   ├── event_store.py     # EventStore — trading event audit trail (DuckDB)
│   │   ├── proposal_store.py  # ProposalStore — trade proposal storage (DuckDB)
│   │   ├── universe.py        # Universe, UniverseAccessor
│   │   └── market_data.py     # MarketData, SecurityDataStream
│   ├── listeners/
│   │   ├── ibreactive.py      # IBAIORx: RxPY wrapper around ib_async
│   │   ├── ib_history_worker.py
│   │   ├── massive_history.py # Massive.com REST history worker
│   │   └── massive_reactive.py # Massive.com WebSocket streaming
│   ├── messaging/
│   │   ├── clientserver.py    # ZMQ RPC, PubSub, MessageBus
│   │   ├── trader_service_api.py
│   │   ├── strategy_service_api.py
│   │   └── data_service_api.py
│   ├── trading/
│   │   ├── trading_runtime.py # Trader class (main runtime, Singleton)
│   │   ├── executioner.py     # TradeExecutioner
│   │   ├── book.py            # BookSubject (reactive order book)
│   │   ├── portfolio.py       # Portfolio positions
│   │   ├── order_validator.py
│   │   ├── risk_gate.py       # RiskGate — pre-trade risk limit enforcement
│   │   ├── position_sizing.py # PositionSizer — confidence/risk/liquidity-aware sizing
│   │   ├── trading_filter.py  # TradingFilter — denylist/allowlist/exchange filtering
│   │   ├── strategy.py        # Strategy ABC, Signal, StrategyConfig, StrategyContext
│   │   └── proposal.py        # TradeProposal, ExecutionSpec — propose/review/approve pipeline
│   ├── strategy/
│   │   └── strategy_runtime.py # StrategyRuntime (loads/runs strategies)
│   ├── simulation/
│   │   ├── historical_simulator.py
│   │   └── backtester.py      # Backtester — replay historical data through strategies
│   └── tools/                 # Importable scripts (moved from scripts/)
│       ├── idea_scanner.py    # IdeaScanner (Massive) + IBIdeaScanner (IB international)
│       ├── chain.py           # Options chain analysis
│       ├── trader_check.py    # Service health check
│       ├── zmq_pub_listener.py # ZMQ PubSub pretty printer
│       ├── ib_instrument_scraper.py # IB product scraper
│       └── ib_resolve.py      # IB symbol resolver
│
├── strategies/                # User strategy implementations
│   ├── global.py              # Global (no-op) strategy
│   └── smi_crossover.py       # SMI crossover example
│
├── scripts/                   # Standalone operational scripts (not imported by library)
│   ├── docker-entrypoint.sh   # Container entrypoint
│   ├── ib-gateway-run.sh      # IB Gateway startup script
│   ├── ib_status.py           # IB Gateway health check
│   ├── reporting.py           # Trade reporting
│   ├── test_pycron.py         # Pycron integration test
│   └── test_pycron_sync.py    # Pycron sync test
│
├── tests/                     # Test suite (pytest)
│   ├── conftest.py            # Shared fixtures (DuckDB, strategies, OHLCV data)
│   ├── test_backtester.py
│   ├── test_book.py
│   ├── test_config.py
│   ├── test_container.py
│   ├── test_duckdb_store.py
│   ├── test_event_store.py
│   ├── test_idea_scanner.py   # 123 tests: presets, indicators, IB scanner, tickers path, fundamentals, news
│   ├── test_portfolio.py
│   ├── test_position_sizing.py # 72 tests: sizing, confidence, risk, liquidity, resize deltas
│   ├── test_proposal.py
│   ├── test_proposal_store.py
│   ├── test_risk_gate.py
│   ├── test_sdk.py
│   ├── test_serialization.py
│   ├── test_strategy.py
│   └── test_trading_filter.py
│
├── Dockerfile                 # Debian bookworm + Python venv
├── docker-compose.yml         # IB Gateway sidecar + MMR container
├── docker.sh                  # Docker/Podman build/deploy helper
├── start_mmr.sh               # Startup script (tmux + pycron)
└── pyproject.toml             # Package config, dependencies, entry points
```

## Docker Setup

Two-container model via docker-compose:
- **ib-gateway**: `ghcr.io/gnzsnz/ib-gateway:latest` — runs IB Gateway. Configured via `.env` file.
- **mmr**: Built from Dockerfile. Connects to ib-gateway via Docker DNS. SSH on port 2222.

IB Gateway ports: 4003 (live), 4004 (paper), mapped to host as 4001/4002.

Data and logs are bind-mounted from `./data` and `./logs` on the host.

## Build & Run

```bash
# Docker (first-time prompts for IB credentials, writes .env)
./docker.sh -g              # Build + start + SSH in
./docker.sh -b              # Build image only
./docker.sh -u              # Start containers
./docker.sh -d              # Stop containers
./docker.sh -s              # Sync code to running container
./docker.sh -e              # SSH into container
./docker.sh -l              # Tail logs
./docker.sh -c              # Clean all images/volumes

# Inside container (or non-Docker)
./start_mmr.sh              # Start tmux session with all services
./start_mmr.sh --paper      # Paper trading mode
./start_mmr.sh --no-tmux    # Run pycron directly

# Individual services
python3 -m trader.trader_service
python3 -m trader.strategy_service
python3 -m trader.data_service   # Persistent data download RPC server
python3 -m trader.mmr_cli    # Interactive REPL
```

## Package Installation

The project is a proper Python package installable via pip:

```bash
pip install -e .             # Editable install

# Console entry points after install:
trader-service               # trader.trader_service:main
strategy-service             # trader.strategy_service:main
data-service                 # trader.data_service:main
mmr                          # trader.mmr_cli:main
```

## CLI Commands

```
status                       # Service connectivity check
resolve AMD                  # Resolve symbol to conId/universe
resolve EURUSD --sectype CASH # Resolve forex pair via IB (IDEALPRO)
portfolio                    # Current portfolio
orders                       # Open orders
buy AMD --market --amount 100.0
buy EUR --sectype CASH --market --quantity 20000  # Forex buy
sell AMD --market --quantity 10
sell EUR --sectype CASH --market --quantity 20000  # Forex sell
cancel 123                   # Cancel order by ID
cancel-all                   # Cancel all orders
close 1                      # Close position by row number
strategies                   # List strategies
strategies enable my_strat   # Enable a strategy
snapshot AMD                 # Price snapshot
listen AMD                   # Stream live ticks via ZMQ
watch                        # Live portfolio monitor
history list                     # List all downloaded history
history list --symbol AAPL       # Filter by symbol
history list --bar_size "1 day"  # Filter by bar size
history massive --symbol AAPL --bar_size "1 day" --prev_days 30
history massive --universe portfolio --bar_size "1 day" --prev_days 30
history ib --symbol AAPL --universe portfolio --bar_size "1 min" --prev_days 5
stream AAPL MSFT AMD         # Stream from Massive.com
stream AAPL --trades         # Stream trades instead of aggs
stream EURUSD GBPUSD --feed forex           # Forex 1-min aggs
stream EURUSD --feed forex --quotes         # Forex bid/ask quotes
stream EURUSD --feed forex --trades         # Forex per-second aggs
universe list                # List all universes with symbol counts
universe show sp500          # Show symbols in a universe
universe create my_universe  # Create an empty universe
universe delete my_universe  # Delete a universe (with confirmation)
universe add my_universe AAPL MSFT AMD  # Resolve via IB and add symbols
universe remove my_universe MSFT        # Remove a symbol from a universe
universe import my_universe symbols.csv # Bulk import from CSV file
options expirations AAPL                              # List expiry dates + DTE
options chain AAPL                                    # Full chain snapshot (nearest exp)
options chain AAPL --expiration 2026-03-20            # Specific expiration
options chain AAPL -e 2026-03-20 --type call          # Calls only
options chain AAPL -e 2026-03-20 --strike-min 200 --strike-max 250
options snapshot O:AAPL260320C00250000                # Single contract detail
options implied AAPL -e 2026-03-20                    # Probability distribution
options buy AAPL -e 2026-03-20 -s 250 -r C -q 5 --market
options sell AAPL -e 2026-03-20 -s 250 -r C -q 5 --limit 3.50
news                                         # General market news
news AAPL                                    # News for a ticker
news AAPL --limit 20                         # More articles
news AAPL --source benzinga                  # Benzinga source
news AAPL --detail                           # Full details + sentiment
movers                           # Top stock gainers (default)
movers --market crypto           # Crypto gainers
movers --market indices          # Index gainers
movers --losers                  # Stock losers
movers --market crypto --losers  # Crypto losers
scan                             # Top gainers (default preset)
scan losers                      # Top losers
scan active                      # Most active by volume
scan hot-volume                  # Hot by volume change
scan --scan-code HIGH_OPT_VOLUME # Raw IB scanner code
scan gainers --above-price 10 --num 30  # Filtered
scan --instrument ETF --location STK.US  # ETFs
ideas                                        # Momentum scan (default, US/Massive)
ideas gap-up                                 # Gap-up preset
ideas mean-reversion                         # Mean-reversion preset
ideas breakout                               # Breakout preset
ideas gap-down                               # Gap-down preset
ideas volatile                               # Volatile/scalping preset
ideas momentum --tickers AAPL MSFT AMD NVDA  # Scan specific tickers
ideas momentum --universe sp500              # Scan a universe
ideas gap-up --min-price 10                  # Override preset filter
ideas volatile --num 25                      # Top 25 results
ideas --presets                              # List all presets
ideas momentum --detail                      # Show all columns incl. indicators
ideas momentum --fundamentals                # Enrich with financial ratios (PE, D/E, ROE, etc.)
ideas momentum --news                        # Enrich with latest news headline + sentiment
ideas mean-reversion --news --fundamentals   # Full picture: technicals + fundamentals + news
ideas gap-up -t AAPL MSFT --fundamentals     # Specific tickers with fundamentals
ideas momentum --location STK.AU.ASX --tickers BHP CBA CSL  # ASX via IB
ideas mean-reversion --location STK.AU.ASX --tickers BHP CBA --detail  # ASX with enrichment
ideas gap-up --location STK.HK.SEHK --tickers 0700 0005     # Hong Kong via IB
propose AMD BUY --market --quantity 100 --bracket 180 150 --reasoning "Breakout above resistance"
propose AMD BUY --limit 165 --amount 5000 --trailing-stop-pct 2.0 --tif GTC
propose AAPL SELL --market --quantity 50 --stop-loss 140
proposals                                    # List pending proposals
proposals --all                              # All statuses
proposals --status EXECUTED                  # Filter by status
proposals show 3                             # Full detail for proposal #3
approve 3                                    # Execute proposal #3 (requires trader_service)
reject 3 --reason "Changed thesis"           # Reject proposal #3
resize-positions --max-bound 500000          # Trim portfolio to $500k
resize-positions --min-bound 300000          # Grow portfolio to $300k
resize-positions --max-bound 500000 --min-bound 300000  # Both bounds
resize-positions --max-bound 500000 --dry-run  # Preview without executing
forex snapshot EURUSD                        # Forex snapshot via IB (default)
forex snapshot EURUSD --source massive       # Forex snapshot via Massive
forex quote EUR USD                          # Last bid/ask via IB (default)
forex quote EUR USD --source massive         # Last bid/ask via Massive
forex snapshot-all                           # All forex snapshots (Massive only)
forex movers                                 # Top forex gainers (Massive only)
forex movers --losers                        # Top forex losers (Massive only)
forex convert EUR USD 1000                   # Currency conversion (Massive only)
```

## Ideas Scanner Architecture

The ideas scanner has two backends that share scoring/filtering logic:

```
ideas momentum                                  → IdeaScanner (Massive.com, US only, ~4s)
ideas momentum --location STK.AU.ASX --tickers   → IBIdeaScanner (IB API, international, ~30-90s)
```

### IdeaScanner (Massive path — default)

```
Massive movers API  →  snapshots  →  filter  →  Massive indicator API (parallel)  →  score  →  rank
```

- Fast (~4s for full scan) — server-side indicators, batch snapshots
- US markets only (Polygon.io coverage)
- Fundamentals from `list_financials_ratios`, news from `list_ticker_news` with sentiment

### IBIdeaScanner (IB path — `--location`)

```
IB scanner / resolve_contract  →  get_snapshot (sequential)  →  reqHistoricalData  →  local RSI/EMA/SMA  →  score  →  rank
```

- Slower (~30-90s) — sequential IB snapshot requests, IB pacing limits
- Works for any IB-supported market: ASX, TSE, SEHK, EU exchanges, etc.
- Discovery: IB scanner API (when available) or explicit `--tickers`/`--universe`
- The IB scanner API may not support all location codes (error 162). When this happens, use `--tickers` to specify symbols explicitly
- Indicators computed locally from history bars (pure pandas) — `compute_rsi()`, `compute_ema()`, `compute_sma()`
- Fundamentals from `reqFundamentalData` (ReportSnapshot XML), news from `reqHistoricalNews` (no sentiment)
- IB news headlines include metadata prefixes like `{A:800015:L:en}...` which are stripped before display

### Shared module-level functions (used by both scanners)

- `PRESETS` dict, `ScanFilter`/`ScanPreset` dataclasses
- Scoring functions: `_score_momentum`, `_score_gap_up`, `_score_gap_down`, `_score_mean_reversion`, `_score_breakout`, `_score_volatile`
- `apply_filters()`, `to_dataframe()`, `merge_filters()`
- `PRESET_SCAN_CODES` maps preset names to IB scan codes

### IB Scanner Location Codes

Common codes: `STK.US.MAJOR` (US), `STK.AU.ASX` (Australia), `STK.CA` (Canada), `STK.HK.SEHK` (Hong Kong), `STK.JP.TSE` (Japan), `STK.EU` (Europe).

The scanner API requires market data subscriptions for the target exchange. If the scanner returns error 162 ("Market Scanner is not configured for one of the chosen locations"), use `--tickers` to provide symbols explicitly — they'll be resolved via `resolve_contract` with the exchange extracted from the location code.

When explicit tickers are provided with `--location`, the `min_change_pct`/`max_change_pct` preset defaults are relaxed (the user chose these symbols specifically and shouldn't have them filtered out).

## Contract Resolution

`resolve_symbol()` in `trading_runtime.py` handles symbol/conId lookups:

1. First checks the local DuckDB universe database
2. If not found:
   - **Integer (conId)**: Returns empty — no fallback. ConIds are exact identifiers; if a conId isn't in the local DB, it's stale or wrong. Fuzzy matching would be dangerous (e.g. conId `4391` as a string matches Japanese ticker "4391" on TSEJ instead of AMD).
   - **String (symbol)**: Falls back to IB `reqContractDetailsAsync` with the exchange hint if provided
   - **CASH sec_type**: Constructs forex pair on IDEALPRO

The `IBIdeaScanner` uses `resolve_contract` (not `resolve_symbol`) to resolve tickers for international markets. This takes a partial `Contract` object with the exchange set from the location code (e.g. `STK.AU.ASX` → `exchange=ASX`), ensuring resolution to the local listing rather than a US ADR.

## Configuration

User configs live in `~/.config/mmr/`. On first run, bundled defaults from `configs/` are copied there automatically (`container.ensure_config_dir()`). The `TRADER_CONFIG` env var overrides the config file path.

**`~/.config/mmr/trader.yaml`**: IB connection (address, port, client IDs, account), DuckDB path, ZMQ port assignments. Env vars override config values (uppercased param name).

**`~/.config/mmr/pycron.yaml`**: Service definitions with cron scheduling, auto-restart, dependency ordering.

**`~/.config/mmr/strategy_runtime.yaml`**: Strategy name, Python module path, class name, bar_size, conids/universe, historical_days_prior.

**`~/.config/mmr/logging.yaml`**: Python logging config (Rich console handler + rotating file handlers).

**`.env`** (gitignored): IB Gateway credentials (TWS_USERID, TWS_PASSWORD, TRADING_MODE, IB_ACCOUNT).

## Logging

Logs are written to `~/.local/share/mmr/logs/` with per-session timestamps (e.g. `trader_service_2026-02-19_18-38-06.log`). The directory is created automatically. Console output uses Rich for colored log levels and timestamps. Configured in `~/.config/mmr/logging.yaml`.

## Key ZMQ Ports

| Port  | Protocol | Service |
|-------|----------|---------|
| 42001 | RPC      | trader_service |
| 42002 | PubSub   | ticker broadcast |
| 42003 | RPC      | data_service |
| 42005 | RPC      | strategy_service |
| 42006 | MessageBus | strategy signals |

## Dependencies

Key packages: `ib_async`, `duckdb`, `pyzmq`, `msgpack`, `reactivex`, `pandas`, `numpy`, `pyarrow`, `dill`, `rich`, `backoff`, `psutil`, `exchange-calendars`, `massive`.

Python >= 3.12. Install: `pip install -e .` or `pip install -r requirements.txt`

## Testing

Tests use pytest with shared fixtures in `tests/conftest.py`. All tests are unit tests that use temporary DuckDB databases (no IB connection required).

```bash
pytest tests/ -v             # Run all tests
pytest tests/test_book.py    # Run a specific test file
```

The working test suite (384 tests across 16 files):
```bash
pytest tests/test_idea_scanner.py tests/test_config.py tests/test_risk_gate.py \
  tests/test_proposal.py tests/test_proposal_store.py tests/test_book.py \
  tests/test_backtester.py tests/test_container.py tests/test_duckdb_store.py \
  tests/test_event_store.py tests/test_portfolio.py tests/test_serialization.py \
  tests/test_strategy.py tests/test_position_sizing.py tests/test_sdk.py \
  tests/test_trading_filter.py -v
```

Some test files have import errors due to missing optional dependencies (`aioreactive`) — these are pre-existing and can be ignored: `test_aiorx.py`, `test_aiozmq_simple.py`, `test_disposable.py`, `test_mmr_client.py`, `test_mmr_server.py`, `test_perf2.py`, `test_performance.py`.

## Writing a Strategy

Subclass `trader.trading.strategy.Strategy` and implement `on_prices()`:

```python
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class MyStrategy(Strategy):
    def on_prices(self, prices):
        # prices is a DataFrame of accumulated OHLCV data
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
    conids: [265598]  # AAPL — use current conIds, verify with `mmr resolve AAPL`
    historical_days_prior: 5
```

**Important**: ConIds can change. Always verify with `mmr resolve SYMBOL` before hardcoding. If a conId is stale, the strategy will log an error and be disabled — it will NOT silently subscribe to a different instrument.

## Claude Code Agent Workflow

### JSON Output

All CLI commands support `--json` for machine-readable output:

```bash
mmr --json portfolio
mmr --json resolve AAPL
mmr --json data summary
mmr --json backtest -s strategies/my_strategy.py --class MyStrategy --conids 265598
```

JSON output always follows the structure `{"data": ..., "title": ...}` for data commands and `{"success": bool, "message": ...}` for status messages.

### Explore → Write → Backtest → Iterate → Deploy

**Step 1: Explore available data** (no service needed)
```bash
mmr --json data summary                                    # What data is in local DuckDB
mmr --json data query AAPL --bar-size "1 day" --days 30    # Read OHLCV from local store
```

**Step 2: Download historical data** (no service needed, requires massive_api_key)
```bash
mmr data download AAPL MSFT --bar-size "1 day" --days 365
```

**Step 3: Create a strategy**
```bash
mmr strategies create my_strategy                          # Creates strategies/my_strategy.py
# Edit strategies/my_strategy.py with your logic
```

**Step 4: Backtest** (no service needed)
```bash
mmr --json backtest -s strategies/my_strategy.py --class MyStrategy --conids 265598 --days 365
```

**Step 5: Deploy to paper trading** (no service needed — writes to config)
```bash
mmr strategies deploy my_strategy --conids 265598 --paper
```

**Step 6: Monitor signals** (no service needed)
```bash
mmr --json strategies signals my_strategy
mmr --json portfolio                                       # Requires trader_service
```

### Command Service Requirements

**No service needed** (fully local):
- `data summary`, `data query`, `data download`
- `backtest` / `bt`
- `strategies create`, `strategies deploy`, `strategies undeploy`
- `strategies signals`, `strategies backtest`
- `universe list`, `universe show`, `universe create`, `universe delete`, `universe remove`, `universe import`
- `propose`, `proposals`, `reject`

**Requires trader_service**:
- `portfolio`, `positions`, `orders`, `trades`, `account`, `status`
- `buy`, `sell`, `cancel`, `cancel-all`, `close`
- `resize-positions` (reads portfolio + orders, places market orders + protective orders)
- `resolve`, `snapshot`
- `approve`
- `strategies enable`, `strategies disable`
- `listen`, `watch`
- `ideas --location` (IB path for international markets)
- `scan` (IB scanner)

**Requires massive_api_key only** (no service):
- `data download`
- `financials`, `options`, `news`, `movers`
- `ideas` (without `--location` — default Massive path)

### ConId Lookup

Symbols in DuckDB are stored by conId (IB contract ID). To find a conId:
```bash
mmr --json resolve AAPL    # Returns conId in JSON data
```

Common conIds: AAPL=265598, MSFT=272093, NVDA=4815747. Note: conIds can become stale — always verify before use.

### Valid Bar Sizes

`1 secs`, `5 secs`, `10 secs`, `15 secs`, `30 secs`, `1 min`, `2 mins`, `3 mins`, `5 mins`, `10 mins`, `15 mins`, `20 mins`, `30 mins`, `1 hour`, `2 hours`, `3 hours`, `4 hours`, `8 hours`, `1 day`, `1 week`, `1 month`

### Strategy File Conventions

- Strategy files live in `strategies/` directory
- File name: `snake_case.py` (e.g. `my_strategy.py`)
- Class name: `CamelCase` (e.g. `MyStrategy`)
- Must subclass `trader.trading.strategy.Strategy`
- Must implement `on_prices(self, prices: pd.DataFrame) -> Optional[Signal]`
- DataFrame columns: `open`, `high`, `low`, `close`, `volume`, `vwap`, `bar_count`, `bid`, `ask`, `last`, `last_size` (DatetimeIndex named `date`)

### Command Latency Reference

All timings measured on local macOS. Network commands (download) depend on Massive.com API latency. Set appropriate timeouts — in particular, 1-min backtests on 16K+ bars take 10-15s.

#### Data Download (`mmr data download`)
Downloads historical data from Massive.com REST API to local DuckDB. Sequential per symbol (not parallelized).

| Operation | Time | Notes |
|-----------|------|-------|
| 1 symbol, daily, 30 days (~21 bars) | ~3s | Includes API round-trip + DuckDB write |
| 1 symbol, daily, 365 days (~252 bars) | ~3s | Same API call, slightly more data |
| 1 symbol, 1-min, 30 days (~15K bars) | ~4s | Larger payload, single API call |
| 3 symbols, daily, 365 days | ~4s | ~1s per symbol after first |
| 5 symbols, daily, 365 days | ~6s | Downloads are sequential |

**Massive API behavior**: Returns up to 50,000 bars per call. For 1-min data, 30 days is ~15-20K bars (within single call). The API may return NaN/null rows for future dates or market holidays — the backtester drops these automatically.

#### Data Query (`mmr data query`)
Reads from local DuckDB. Very fast.

| Operation | Time |
|-----------|------|
| `data summary` (list all symbols/bar sizes) | ~1.5s |
| `data query SYMBOL --days 30` | ~2s |

#### Backtesting (`mmr backtest`)
CPU-bound bar-by-bar replay. Time scales with number of bars × strategy complexity.

| Operation | Time | Notes |
|-----------|------|-------|
| Daily, 1 symbol, 252 bars (simple strategy) | ~1.5s | MA crossover, RSI, mean reversion |
| Daily, 1 symbol, 252 bars (vectorbt strategy) | ~4s | First run includes numba JIT compilation |
| Daily, 3 symbols, 252 bars each | ~2.5s | Multi-conid merges timelines |
| 1-min, 1 symbol, 16K bars (simple strategy) | ~14s | Scales linearly with bar count |

**Vectorbt note**: Strategies using `vectorbt` indicators (e.g. `VbtMacdBB`) incur a one-time ~2s numba JIT compilation penalty on first run. Subsequent runs in the same process are fast. The JIT also produces verbose DEBUG logs from numba — these are harmless.

#### Ideas Scanner (`mmr ideas`)

| Operation | Time | Notes |
|-----------|------|-------|
| `ideas` (Massive, movers, momentum preset) | ~4s | 2 snapshot + ~40 indicator calls (parallelized) |
| `ideas momentum --tickers AAPL MSFT AMD` | ~2s | 1 batch snapshot + ~6 indicator calls |
| `ideas --presets` | ~1s | No API calls, prints preset table |
| `ideas volatile --num 25` | ~4s | Same as movers, just more output rows |
| `ideas momentum --location STK.AU.ASX --tickers BHP CBA CSL` | ~30-90s | IB path: sequential snapshots + history |

#### Strategy Management
All local file/YAML operations.

| Operation | Time |
|-----------|------|
| `strategies create name` | ~2s |
| `strategies deploy name` | ~2s |
| `strategies list` | ~2s |

#### Test Suite
| Operation | Time |
|-----------|------|
| Full working suite (384 tests) | ~4s |
| Single test file | ~1-2s |

#### Python Import Overhead
All commands have ~1s baseline overhead for Python startup + importing trader modules (pandas, numpy, duckdb, etc.). This is unavoidable and included in all timings above.
