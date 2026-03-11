# CLAUDE.md

## Project Overview

MMR (Make Me Rich) is a Python-based algorithmic trading platform for Interactive Brokers. It supports automated strategy execution, interactive CLI trading, historical data collection, and real-time market data streaming.

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
│   │   └── strategy.py        # Strategy ABC, Signal, StrategyConfig, StrategyContext
│   ├── strategy/
│   │   └── strategy_runtime.py # StrategyRuntime (loads/runs strategies)
│   ├── simulation/
│   │   ├── historical_simulator.py
│   │   └── backtester.py      # Backtester — replay historical data through strategies
│   └── tools/                 # Importable scripts (moved from scripts/)
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
│   ├── test_portfolio.py
│   ├── test_risk_gate.py
│   ├── test_serialization.py
│   └── test_strategy.py
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
forex snapshot EURUSD                        # Forex snapshot via IB (default)
forex snapshot EURUSD --source massive       # Forex snapshot via Massive
forex quote EUR USD                          # Last bid/ask via IB (default)
forex quote EUR USD --source massive         # Last bid/ask via Massive
forex snapshot-all                           # All forex snapshots (Massive only)
forex movers                                 # Top forex gainers (Massive only)
forex movers --losers                        # Top forex losers (Massive only)
forex convert EUR USD 1000                   # Currency conversion (Massive only)
```

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
    conids: [4391]  # AMD
    historical_days_prior: 5
```

## Claude Code Agent Workflow

### JSON Output

All CLI commands support `--json` for machine-readable output:

```bash
mmr --json portfolio
mmr --json resolve AAPL
mmr --json data summary
mmr --json backtest -s strategies/my_strategy.py --class MyStrategy --conids 4391
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
mmr --json backtest -s strategies/my_strategy.py --class MyStrategy --conids 4391 --days 365
```

**Step 5: Deploy to paper trading** (no service needed — writes to config)
```bash
mmr strategies deploy my_strategy --conids 4391 --paper
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

**Requires trader_service**:
- `portfolio`, `positions`, `orders`, `trades`, `account`, `status`
- `buy`, `sell`, `cancel`, `cancel-all`, `close`
- `resolve`, `snapshot`
- `strategies enable`, `strategies disable`
- `listen`, `watch`

**Requires massive_api_key only** (no service):
- `data download`
- `financials`, `options`, `news`, `movers`

### ConId Lookup

Symbols in DuckDB are stored by conId (IB contract ID). To find a conId:
```bash
mmr --json resolve AAPL    # Returns conId in JSON data
```

Common conIds: AAPL=265598, AMD=4391, MSFT=272093, NVDA=4815747.

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
| Full suite (359 tests) | ~8s |
| Single test file | ~1-2s |

#### Python Import Overhead
All commands have ~1s baseline overhead for Python startup + importing trader modules (pandas, numpy, duckdb, etc.). This is unavoidable and included in all timings above.
