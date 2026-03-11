---
name: mmr
description: Operate the MMR algorithmic trading platform on Interactive Brokers. Trade stocks and options, manage portfolios, resolve symbols, manage universes, download historical data, analyze options chains, and control strategies. All operations are available via async Python helper methods.
metadata:
  author: mmr
  version: "1.0"
---

# MMR Trading Skill

**LLMVM**: Use `<helpers>...</helpers>` blocks to call MMRHelpers methods. All methods are `async` and return strings.

## Service Requirements

- **trader_service required**: portfolio, positions, orders, trades, account, resolve, snapshot, buy, sell, cancel, strategies (list/enable/disable), `universe_add`, `buy_option`, `sell_option`
- **data_service required**: history_massive, history_ib
- **massive_api_key only** (no service needed): balance_sheet, income_statement, cash_flow, ratios, filing_section, `options_expirations`, `options_chain`, `options_snapshot`, `options_implied`, `data_download`
- **No service needed**: universe_list, universe_show, universe_create, universe_delete, universe_remove, universe_import, status, `data_summary`, `data_query`, `backtest`, `strategy_create`, `strategy_deploy`, `strategy_undeploy`, `strategy_signals`, `strategy_backtest`

## MMRHelpers

### Portfolio & Account

| Method | Description |
|--------|-------------|
| `MMRHelpers.portfolio()` | Portfolio with P&L (symbol, position, mktPrice, avgCost, unrealizedPNL, dailyPNL) |
| `MMRHelpers.positions()` | Raw positions (symbol, secType, position, avgCost, currency) |
| `MMRHelpers.orders()` | Open orders (orderId, action, orderType, lmtPrice, totalQuantity) |
| `MMRHelpers.trades()` | Active trades with fill status |
| `MMRHelpers.account()` | IB account ID |
| `MMRHelpers.status()` | Service health check |

### Symbol Resolution & Market Data

| Method | Description |
|--------|-------------|
| `MMRHelpers.resolve(symbol)` | Resolve ticker to IB contract (conId, exchange, secType, longName) |
| `MMRHelpers.snapshot(symbol, delayed=False)` | Price snapshot (bid, ask, last, OHLC) |

### Financial Statements

| Method | Description |
|--------|-------------|
| `MMRHelpers.balance_sheet(symbol, limit=4, timeframe="quarterly")` | Balance sheet (assets, liabilities, equity) |
| `MMRHelpers.income_statement(symbol, limit=4, timeframe="quarterly")` | Income statement (revenue, earnings, margins) |
| `MMRHelpers.cash_flow(symbol, limit=4, timeframe="quarterly")` | Cash flow statement (operating, investing, financing) |
| `MMRHelpers.ratios(symbol)` | Financial ratios TTM (P/E, P/B, ROE, EV/EBITDA, etc.) |
| `MMRHelpers.filing_section(symbol, section="business", limit=1)` | 10-K filing section text (business or risk_factors) |

Requires `massive_api_key` in config. No trader_service needed. `timeframe`: `"quarterly"` or `"annual"` (not applicable to ratios/filing).

### Trading

| Method | Description |
|--------|-------------|
| `MMRHelpers.buy(symbol, quantity=, amount=, limit_price=, market=)` | Place buy order |
| `MMRHelpers.sell(symbol, quantity=, amount=, limit_price=, market=)` | Place sell order |
| `MMRHelpers.cancel(order_id)` | Cancel order by ID |
| `MMRHelpers.cancel_all()` | Cancel all open orders |

Buy/sell require: `market=True` or `limit_price=X`. Plus: `quantity=N` (shares) or `amount=N` (dollars).

### Options

| Method | Service? | Description |
|--------|----------|-------------|
| `MMRHelpers.options_expirations(symbol)` | No* | List expiration dates with DTE |
| `MMRHelpers.options_chain(symbol, expiration=, contract_type=, strike_min=, strike_max=)` | No* | Full chain snapshot (strike, bid/ask, greeks, IV, OI) |
| `MMRHelpers.options_snapshot(option_ticker)` | No* | Single contract detail |
| `MMRHelpers.options_implied(symbol, expiration, risk_free_rate=0.05)` | No* | Market-implied vs constant-vol probability distribution |
| `MMRHelpers.buy_option(symbol, expiration, strike, right, quantity, limit_price=, market=)` | **Yes** | Buy option contracts |
| `MMRHelpers.sell_option(symbol, expiration, strike, right, quantity, limit_price=, market=)` | **Yes** | Sell option contracts |

*Requires `massive_api_key` in config.

`right`: `"C"` for call, `"P"` for put. `option_ticker` format: `O:AAPL260320C00250000` (symbol + YYMMDD + C/P + strike*1000 zero-padded to 8 digits).

### Strategies

| Method | Service? | Description |
|--------|----------|-------------|
| `MMRHelpers.strategies()` | **Yes** | List strategies (name, state, paper, bar_size) |
| `MMRHelpers.enable_strategy(name)` | **Yes** | Enable a strategy |
| `MMRHelpers.disable_strategy(name)` | **Yes** | Disable a strategy |

### Strategy Development (No Service Required)

| Method | Description |
|--------|-------------|
| `MMRHelpers.strategy_create(name)` | Create template strategy file in `strategies/` |
| `MMRHelpers.backtest(path, class_name, conids, ...)` | Backtest a strategy against historical data |
| `MMRHelpers.strategy_deploy(name, conids, paper=True)` | Deploy to `strategy_runtime.yaml` |
| `MMRHelpers.strategy_undeploy(name)` | Remove from config |
| `MMRHelpers.strategy_signals(name, limit=20)` | View recent signals from event store |
| `MMRHelpers.strategy_backtest(name, days=365)` | Backtest a deployed strategy by name |

### Data Exploration (No Service Required)

| Method | Description |
|--------|-------------|
| `MMRHelpers.data_summary()` | What historical data is available locally |
| `MMRHelpers.data_query(symbol, bar_size, days)` | Read OHLCV data from local DuckDB |
| `MMRHelpers.data_download(symbols, bar_size, days)` | Download from Massive.com to local DuckDB |

`data_download` requires `massive_api_key` in config. All three methods return JSON.

### Universe Management

Universes are named collections of stock definitions stored in DuckDB.

| Method | Service? | Description |
|--------|----------|-------------|
| `MMRHelpers.universe_list()` | No | List all universes with symbol counts |
| `MMRHelpers.universe_show(name)` | No | Show symbols in a universe |
| `MMRHelpers.universe_create(name)` | No | Create empty universe |
| `MMRHelpers.universe_delete(name)` | No | Delete universe (auto-confirms) |
| `MMRHelpers.universe_add(name, symbols)` | **Yes** | Resolve via IB and add symbols |
| `MMRHelpers.universe_remove(name, symbol)` | No | Remove a symbol |
| `MMRHelpers.universe_import(name, csv_file)` | No | Bulk import from CSV |

`universe_add` takes a list of ticker strings: `["AAPL", "MSFT", "AMD"]`.

### Historical Data

| Method | Description |
|--------|-------------|
| `MMRHelpers.history_massive(symbol=, universe=, bar_size="1 day", prev_days=30)` | Download from Massive.com (via data_service RPC) |
| `MMRHelpers.history_ib(symbol=, universe=, bar_size="1 min", prev_days=5)` | Download from IB (via data_service RPC) |

Must specify either `symbol` or `universe`. Requires `data_service` to be running.

**Bar sizes**: `1 secs`, `5 secs`, `10 secs`, `15 secs`, `30 secs`, `1 min`, `2 mins`, `3 mins`, `5 mins`, `10 mins`, `15 mins`, `20 mins`, `30 mins`, `1 hour`, `2 hours`, `3 hours`, `4 hours`, `8 hours`, `1 day`, `1 week`, `1 month`

**prev_days**: number of calendar days back from today to download (e.g. `prev_days=365` for one year).

### Escape Hatch

| Method | Description |
|--------|-------------|
| `MMRHelpers.cli(command)` | Run any CLI command directly (e.g. `MMRHelpers.cli("resolve AAPL")`) |

## Patterns

### Pattern 1: Create a Universe from a List of Symbols

```python
# Create and populate a universe
result = await MMRHelpers.universe_create("tech_stocks")
emit(result)

# Add symbols (requires trader_service for IB resolution)
result = await MMRHelpers.universe_add("tech_stocks", ["AAPL", "MSFT", "NVDA", "AMD", "GOOGL", "META"])
emit(result)

# Verify
result = await MMRHelpers.universe_show("tech_stocks")
emit(result)
```

### Pattern 2: Check Portfolio and Close a Position

```python
# View portfolio
portfolio = await MMRHelpers.portfolio()
emit(portfolio)

# Sell a specific stock at market
result = await MMRHelpers.sell("AMD", market=True, quantity=10)
emit(result)
```

### Pattern 3: Research a Symbol

```python
# Resolve to get IB contract details
info = await MMRHelpers.resolve("AAPL")
emit(info)

# Get current price snapshot
snap = await MMRHelpers.snapshot("AAPL")
emit(snap)
```

### Pattern 4: Download Historical Data

```python
# Download 30 days of daily bars for a symbol
result = await MMRHelpers.history_massive(symbol="AAPL", bar_size="1 day", prev_days=30)
emit(result)

# Download for an entire universe
result = await MMRHelpers.history_massive(universe="tech_stocks", bar_size="1 day", prev_days=60)
emit(result)

# Download 1-minute bars from IB (last 5 days)
result = await MMRHelpers.history_ib(symbol="AAPL", bar_size="1 min", prev_days=5)
emit(result)

# Download hourly bars for a year
result = await MMRHelpers.history_massive(symbol="NVDA", bar_size="1 hour", prev_days=365)
emit(result)

# Download daily bars for an entire universe
result = await MMRHelpers.history_massive(universe="nasdaq_top25", bar_size="1 day", prev_days=365)
emit(result)
```

### Pattern 5: Analyze Company Financials

```python
# Get quarterly balance sheet (last 4 quarters)
bs = await MMRHelpers.balance_sheet("AAPL")
emit(bs)

# Get annual income statement (last 8 years)
income = await MMRHelpers.income_statement("AAPL", limit=8, timeframe="annual")
emit(income)

# Get quarterly cash flow
cf = await MMRHelpers.cash_flow("AAPL")
emit(cf)

# Get current financial ratios (P/E, ROE, etc.)
ratios = await MMRHelpers.ratios("AAPL")
emit(ratios)

# Get 10-K business description
biz = await MMRHelpers.filing_section("AAPL", section="business")
emit(biz)

# Get 10-K risk factors
risks = await MMRHelpers.filing_section("AAPL", section="risk_factors")
emit(risks)
```

### Pattern 6: Place a Limit Order

```python
# Buy 10 shares of AMD at $150 limit
result = await MMRHelpers.buy("AMD", limit_price=150.00, quantity=10)
emit(result)

# Buy $500 worth at market
result = await MMRHelpers.buy("AMD", market=True, amount=500.0)
emit(result)
```

### Pattern 7: Manage Strategies

```python
# List strategies
strats = await MMRHelpers.strategies()
emit(strats)

# Enable a strategy
result = await MMRHelpers.enable_strategy("smi_crossover")
emit(result)
```

### Pattern 8: Analyze Options Chain

```python
# Get available expirations
exps = await MMRHelpers.options_expirations("AAPL")
emit(exps)

# Get call chain for nearest expiration, strikes 200-250
chain = await MMRHelpers.options_chain("AAPL", expiration="2026-03-20", contract_type="call", strike_min=200, strike_max=250)
emit(chain)

# Get single option contract detail
snap = await MMRHelpers.options_snapshot("O:AAPL260320C00250000")
emit(snap)

# Get probability distribution
implied = await MMRHelpers.options_implied("AAPL", "2026-03-20")
emit(implied)
```

### Pattern 9: Trade Options

```python
# Buy 5 AAPL call contracts at market
result = await MMRHelpers.buy_option("AAPL", "2026-03-20", 250.0, "C", 5, market=True)
emit(result)

# Sell 3 AAPL put contracts at $2.00 limit
result = await MMRHelpers.sell_option("AAPL", "2026-03-20", 240.0, "P", 3, limit_price=2.00)
emit(result)
```

### Pattern 10: Build and Test a Strategy (Full Loop)

```python
# Step 1: Check what data we have
summary = await MMRHelpers.data_summary()
emit(summary)

# Step 2: Download historical data if needed
result = await MMRHelpers.data_download(["AAPL", "MSFT"], bar_size="1 day", days=365)
emit(result)

# Step 3: Explore the data
data = await MMRHelpers.data_query("AAPL", bar_size="1 day", days=30)
emit(data)

# Step 4: Create a strategy template
result = await MMRHelpers.strategy_create("momentum_breakout")
emit(result)
# Now edit strategies/momentum_breakout.py with your logic

# Step 5: Backtest it
result = await MMRHelpers.backtest(
    "strategies/momentum_breakout.py", "MomentumBreakout",
    conids=[265598], days=365, capital=100000
)
emit(result)

# Step 6: If results look good, deploy to paper trading
result = await MMRHelpers.strategy_deploy(
    "momentum_breakout", conids=[265598], paper=True
)
emit(result)

# Step 7: Monitor signals
signals = await MMRHelpers.strategy_signals("momentum_breakout")
emit(signals)
```

### Pattern 11: Iterative Strategy Optimization

```python
# Backtest with different parameters
for fast_period in [5, 10, 15]:
    for slow_period in [20, 30, 50]:
        # Write strategy with these parameters
        # (use file write to update the strategy code)
        result = await MMRHelpers.backtest(
            "strategies/ma_crossover.py", "MACrossover",
            conids=[265598], days=365
        )
        emit(f"fast={fast_period} slow={slow_period}: {result}")
```

## References

For detailed documentation on specific topics, see:
- **`references/STRATEGIES.md`**: Strategy ABC, Signal fields, on_prices() contract, common patterns (MA crossover, RSI, Bollinger, EMA)
- **`references/DATA.md`**: DuckDB schema, BarSize values, data source details, TickStorage patterns, common conIds

## Python SDK Reference

For advanced scripting beyond the helpers, you can use the Python SDK directly.
The SDK lives at `trader/sdk.py` in the MMR project and must run inside the MMR venv.

```python
from trader.sdk import MMR

with MMR() as mmr:
    df = mmr.portfolio()          # pd.DataFrame
    defs = mmr.resolve("AMD")     # List[SecurityDefinition]
    snap = mmr.snapshot("AMD")    # dict with bid/ask/last/OHLC
    result = mmr.buy("AMD", market=True, quantity=10)  # SuccessFail[Trade]
    result.is_success()           # bool
    result.obj                    # Trade object on success
    result.error                  # error string on failure

    # Historical data (via data_service RPC)
    result = mmr.pull_massive(symbols=["AAPL"], bar_size="1 day", prev_days=30)
    result = mmr.pull_massive(universe="nasdaq_top25", bar_size="1 hour", prev_days=60)
    result = mmr.pull_ib(symbols=["AAPL"], bar_size="1 min", prev_days=5)
    # result: {"enqueued": N, "completed": N, "failed": N, "errors": [...]}
```

Key SDK methods: `portfolio()`, `positions()`, `orders()`, `trades()`, `resolve()`, `snapshot()`, `buy()`, `sell()`, `cancel()`, `cancel_all()`, `close_position()`, `strategies()`, `enable_strategy()`, `disable_strategy()`, `account()`, `status()`, `subscribe_ticks()`, `pull_massive()`, `pull_ib()`, `data_service_status()`, `balance_sheet()`, `income_statement()`, `cash_flow()`, `ratios()`, `filing_sections()`, `options_expirations()`, `options_chain()`, `options_snapshot()`, `options_implied()`, `buy_option()`, `sell_option()`.
