---
name: mmr-skill
description: Operate the MMR algorithmic trading platform on Interactive Brokers. Trade stocks and options, manage portfolios, scan for ideas, create trade proposals with auto position sizing, manage universes, download historical data, analyze options chains, and control strategies. All operations are available via async Python helper methods.
metadata:
  author: mmr
  version: "2.0"
---

# MMR Trading Skill

**LLMVM**: Use `<helpers>...</helpers>` blocks to call MMRHelpers methods. All methods are `async`. Most return JSON dicts (access data via `result["data"]`). Trading actions (`buy`, `sell`, `cancel`) return strings.

## Service Requirements

- **trader_service required**: portfolio, positions, orders, trades, account, resolve, snapshot, depth, buy, sell, cancel, cancel_all, close_all_positions, resize_positions, approve, strategies (list/enable/disable), `universe_add`, `buy_option`, `sell_option`, `risk`, `scan`, `ideas` (with `--location` for international markets), `listen`, `watch`
- **data_service required**: history_massive, history_ib
- **massive_api_key only** (no service needed): balance_sheet, income_statement, cash_flow, ratios, filing_section, `options_expirations`, `options_chain`, `options_snapshot`, `options_implied`, `data_download`, `ideas` (default US path), `news`, `movers`, `forex_snapshot` (massive source), `forex_movers`, `stream`
- **No service needed**: universe_list, universe_show, universe_create, universe_delete, universe_remove, universe_import, status, market_hours, `data_summary`, `data_query`, `backtest`, `strategy_create`, `strategy_deploy`, `strategy_undeploy`, `strategy_signals`, `strategy_backtest`, `propose`, `proposals`, `reject`, `session_limits`, `session_status`, `group_list`, `group_create`, `group_delete`, `group_show`, `group_add`, `group_remove`, `group_set`, `logs`

## International Stocks (ASX, TSE, SEHK, etc.)

Many commands accept `exchange` and `currency` hints for international stocks. Without these hints, resolution defaults to US markets (USD/SMART). For example:

```python
# US stock (default)
result = await MMRHelpers.resolve("AMD")

# Australian stock — must specify exchange/currency
result = await MMRHelpers.resolve("BHP", exchange="ASX", currency="AUD")
result = await MMRHelpers.buy("BHP", market=True, quantity=100, exchange="ASX", currency="AUD")
result = await MMRHelpers.snapshot("BHP", exchange="ASX", currency="AUD")
result = await MMRHelpers.ideas("momentum", location="STK.AU.ASX", tickers=["BHP", "RIO", "FMG"])
```

Common location codes: `STK.US.MAJOR` (US), `STK.AU.ASX` (Australia), `STK.CA` (Canada), `STK.HK.SEHK` (Hong Kong), `STK.JP.TSE` (Japan), `STK.EU` (Europe).

## MMRHelpers

### Portfolio & Account

| Method | Description |
|--------|-------------|
| `MMRHelpers.portfolio()` | **JSON** dict — positions with P&L (symbol, position, mktPrice, avgCost, unrealizedPNL, dailyPNL) |
| `MMRHelpers.portfolio_snapshot()` | **JSON** dict — compact: total_value, daily_pnl, position_count, top movers by % change |
| `MMRHelpers.portfolio_diff()` | **JSON** dict — delta since last call: changed, new, removed positions (auto-stores snapshot) |
| `MMRHelpers.positions()` | Raw positions (symbol, secType, position, avgCost, currency) |
| `MMRHelpers.orders()` | **JSON** dict — open orders (orderId, action, orderType, lmtPrice, totalQuantity) |
| `MMRHelpers.trades()` | **JSON** dict — active trades with fill status |
| `MMRHelpers.account()` | IB account ID |
| `MMRHelpers.status()` | **JSON** dict — service health with PnL (DailyPnL, UnrealizedPnL, RealizedPnL, TotalPnL) |
| `MMRHelpers.market_hours()` | **JSON** dict — market open/close status for major exchanges |

### Symbol Resolution & Market Data

| Method | Description |
|--------|-------------|
| `MMRHelpers.resolve(symbol, sectype="STK", exchange="", currency="")` | **JSON** dict — resolve ticker to IB contract (conId, exchange, secType, longName) |
| `MMRHelpers.snapshot(symbol, delayed=False, exchange="", currency="")` | **JSON** dict — price snapshot (bid, ask, last, OHLC). Access: `result["data"]["last"]` |
| `MMRHelpers.snapshots_batch(symbols, exchange="", currency="")` | **JSON** dict — batch snapshots for multiple symbols in one call (~4s total). Much faster than calling snapshot() in a loop |
| `MMRHelpers.depth(symbol, rows=5, exchange="", currency="", smart=False, no_chart=False)` | Level 2 order book table + PNG depth chart |
| `MMRHelpers.depth_json(symbol, rows=5, exchange="", currency="", smart=False)` | **JSON** dict — order book with `chart_path` to PNG image for visual analysis |

### Trading

| Method | Description |
|--------|-------------|
| `MMRHelpers.buy(symbol, quantity=, amount=, limit_price=, market=, exchange=, currency=)` | Place buy order |
| `MMRHelpers.sell(symbol, quantity=, amount=, limit_price=, market=, exchange=, currency=)` | Place sell order |
| `MMRHelpers.cancel(order_id)` | Cancel order by ID |
| `MMRHelpers.cancel_all()` | Cancel all open orders |
| `MMRHelpers.close_all_positions()` | Close all positions at market (cancels orders first, bypasses risk gate) |
| `MMRHelpers.resize_positions(max_bound=, min_bound=, dry_run=)` | Proportionally resize all positions |

Buy/sell require: `market=True` or `limit_price=X`. Plus: `quantity=N` (shares) or `amount=N` (dollars).

### Trade Proposals & Position Sizing

Trade proposals are stored locally and auto-sized based on confidence, ATR volatility, and `position_sizing.yaml` config. Create proposals without trader_service; approve requires trader_service.

| Method | Service? | Description |
|--------|----------|-------------|
| `MMRHelpers.propose(symbol, action, confidence=, reasoning=, group=, ...)` | No* | Create proposal with auto position sizing |
| `MMRHelpers.proposals(status=, all_statuses=)` | No | List proposals |
| `MMRHelpers.approve(proposal_id)` | **Yes** | Execute a proposal |
| `MMRHelpers.reject(proposal_id, reason=)` | No | Reject a proposal |
| `MMRHelpers.session_status()` | No | Full sizing config + portfolio state + capacity (JSON) |
| `MMRHelpers.session_limits()` | No | View position sizing hard limits |

*Auto-sizing requires trader_service for snapshot/ATR data; gracefully degrades without it.

**Sizing pipeline**: `base_position × risk_multiplier × confidence_scale × volatility_adjustment`. With `base_position_pct=0.02` and a $1M account, base is $20K. Volatile stocks (high ATR%) get smaller positions; stable stocks get larger ones. The `sizing_result` in proposal metadata shows the full reasoning chain.

### Position Groups

Named groups with allocation budgets for organizing thematic trades. All group operations are local (no trader_service needed).

| Method | Description |
|--------|-------------|
| `MMRHelpers.group_list()` | List all groups with members and budgets (JSON) |
| `MMRHelpers.group_create(name, budget=, description=)` | Create group (budget in %, e.g. 20 = 20%) |
| `MMRHelpers.group_delete(name)` | Delete group and members |
| `MMRHelpers.group_show(name)` | Group detail with members (JSON) |
| `MMRHelpers.group_add(name, symbols)` | Add symbols to group |
| `MMRHelpers.group_remove(name, symbol)` | Remove symbol from group |
| `MMRHelpers.group_set(name, budget=, description=)` | Update group settings |

Using `group=` in `propose()` auto-adds the symbol to the group.

### Portfolio Risk Analysis

| Method | Service? | Description |
|--------|----------|-------------|
| `MMRHelpers.portfolio_risk()` | **Yes** | Concentration (HHI), group budgets, correlation clusters, warnings (JSON) |

The risk report includes: `hhi` (0=diversified, 1=concentrated), `top_positions` by weight, `group_allocations` with over-budget flags, `correlation_clusters` (symbols with >0.7 correlation), `warnings` (critical >15%, warning >10% concentration, group over budget, correlated clusters >30%), and a `summary` paragraph.

### Market Scanning & Ideas

| Method | Service? | Description |
|--------|----------|-------------|
| `MMRHelpers.ideas(preset, tickers=, universe=, num=, location=, ...)` | No*/Yes** | **JSON** dict — scan for trading ideas with technical scoring |
| `MMRHelpers.news(ticker="", limit=10, detail=False)` | No* | Market news with optional sentiment |
| `MMRHelpers.movers(market="stocks", losers=False, num=20)` | No* | Top market movers |

*Requires `massive_api_key`. **Requires trader_service when using `location=` for international markets.

Presets: `momentum`, `gap-up`, `gap-down`, `mean-reversion`, `breakout`, `volatile`.

### Financial Statements

| Method | Description |
|--------|-------------|
| `MMRHelpers.balance_sheet(symbol, limit=4, timeframe="quarterly")` | Balance sheet (assets, liabilities, equity) |
| `MMRHelpers.income_statement(symbol, limit=4, timeframe="quarterly")` | Income statement (revenue, earnings, margins) |
| `MMRHelpers.cash_flow(symbol, limit=4, timeframe="quarterly")` | Cash flow statement (operating, investing, financing) |
| `MMRHelpers.ratios(symbol)` | Financial ratios TTM (P/E, P/B, ROE, EV/EBITDA, etc.) |
| `MMRHelpers.filing_section(symbol, section="business", limit=1)` | 10-K filing section text (business or risk_factors) |

Requires `massive_api_key` in config. No trader_service needed. `timeframe`: `"quarterly"` or `"annual"` (not applicable to ratios/filing).

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

### Forex

| Method | Service? | Description |
|--------|----------|-------------|
| `MMRHelpers.forex_snapshot(pair, source="ib")` | Yes/No* | Forex pair snapshot |
| `MMRHelpers.forex_movers(losers=False)` | No* | Top forex movers |

*Requires `massive_api_key` for massive source.

### Risk & Session

| Method | Service? | Description |
|--------|----------|-------------|
| `MMRHelpers.risk()` | **Yes** | View risk gate limits |
| `MMRHelpers.session_status()` | No | Full sizing config + portfolio state + capacity (JSON) |
| `MMRHelpers.session_limits()` | No | View position sizing hard limits |
| `MMRHelpers.portfolio_risk()` | **Yes** | Portfolio risk analysis (JSON) |

### Escape Hatch

| Method | Description |
|--------|-------------|
| `MMRHelpers.cli(command)` | Run any CLI command directly (e.g. `MMRHelpers.cli("resolve AAPL")`) |

## Patterns

### Pattern 1: Scan for Ideas and Create Proposals

```python
# Scan US market for momentum ideas (returns JSON dict)
result = await MMRHelpers.ideas("momentum", num=10)
for idea in result["data"]:
    print(f'{idea["ticker"]}: score={idea["score"]}, signal={idea["signal"]}, price={idea["price"]}')

# Scan ASX stocks via IB (use tickers with location for international)
result = await MMRHelpers.ideas("momentum", location="STK.AU.ASX", tickers=["BHP", "RIO"])
for idea in result["data"]:
    print(f'{idea["ticker"]}: {idea["change_pct"]:+.2f}% score={idea["score"]}')

# Create a proposal with auto position sizing
result = await MMRHelpers.propose("AAPL", "BUY", confidence=0.7,
    reasoning="Breakout above 200-day MA on high volume")
emit(result)

# Approve a proposal (executes the trade)
result = await MMRHelpers.approve(42)
emit(result)
```

### Pattern 2: Risk-Aware Trading Loop (LLM)

```python
# 1. MONITOR — quick check, minimal context
snap = await MMRHelpers.portfolio_snapshot()  # compact JSON
diff = await MMRHelpers.portfolio_diff()       # what changed since last cycle
# If nothing moved (all unchanged), skip to sleep
if diff["data"]["unchanged_count"] == snap["data"]["position_count"]:
    emit("Portfolio flat — skipping analysis")
else:
    emit(f"Changes: {len(diff['data']['changed'])} moved, {len(diff['data']['new'])} new")

# 2. ANALYZE — only if something interesting
risk = await MMRHelpers.portfolio_risk()       # HHI, group budgets, warnings
session = await MMRHelpers.session_status()    # remaining capacity
ideas = await MMRHelpers.ideas("momentum", num=10)  # JSON dict

# 3. PROPOSE — auto-sized, group-tagged, never auto-executes
result = await MMRHelpers.propose("AAPL", "BUY", confidence=0.8,
    reasoning="Strong momentum, RSI 65, above 200-day MA",
    group="tech", source="llm")
# result["data"]["sizing_result"]["reasoning"] shows full ATR pipeline
# result["data"]["proposal_id"] → 42

# 4. REVIEW — check risk before approving
risk = await MMRHelpers.portfolio_risk()
if not risk["data"]["warnings"]:
    result = await MMRHelpers.approve(42)
```

Key for loop efficiency: `portfolio_snapshot()` and `portfolio_diff()` return small JSON (~500 tokens) vs `portfolio()` (~1000 tokens JSON). Use snapshot/diff for every cycle, full portfolio only when investigating.

### Pattern 3: Manage Position Groups

```python
# Create thematic groups with allocation budgets
await MMRHelpers.group_create("mining", budget=20, description="Mining & resources")
await MMRHelpers.group_create("banks", budget=25, description="Big 4 banks")
await MMRHelpers.group_add("mining", ["BHP", "RIO", "FMG", "WDS"])
await MMRHelpers.group_add("banks", ["CBA", "NAB", "ANZ", "WBC"])

# Check allocations against budgets
risk = await MMRHelpers.portfolio_risk()
for g in risk["data"]["group_allocations"]:
    status = "OVER" if g["over_budget"] else "OK"
    emit(f'{g["name"]}: {g["pct"]:.1%} / {g["budget_pct"]:.0%} budget [{status}]')

# Proposals auto-register group membership
await MMRHelpers.propose("STO", "BUY", confidence=0.6, group="mining",
    exchange="ASX", currency="AUD")
```

### Pattern 4: Portfolio Management

```python
# View portfolio with P&L
portfolio = await MMRHelpers.portfolio()
emit(portfolio)

# Check service status including PnL summary
status = await MMRHelpers.status()
emit(status)

# Close all positions (emergency liquidation)
result = await MMRHelpers.close_all_positions()
emit(result)

# Resize portfolio to fit within bounds
result = await MMRHelpers.resize_positions(max_bound=500000, dry_run=True)
emit(result)
```

### Pattern 5: Research a Symbol (Including International)

```python
# US stock — all return JSON dicts
info = await MMRHelpers.resolve("AAPL")
snap = await MMRHelpers.snapshot("AAPL")
print(f'AAPL conId: {info["data"][0]["conId"]}')
print(f'AAPL last: {snap["data"]["last"]}, bid: {snap["data"]["bid"]}, ask: {snap["data"]["ask"]}')

# Australian stock — must specify exchange/currency
snap = await MMRHelpers.snapshot("BHP", exchange="ASX", currency="AUD")
print(f'BHP: ${snap["data"]["last"]}, open: ${snap["data"]["open"]}, high: ${snap["data"]["high"]}')

# Batch snapshots — one call for multiple symbols
result = await MMRHelpers.snapshots_batch(["BHP", "CBA", "NAB"], exchange="ASX", currency="AUD")
for s in result["data"]:
    chg = ((s["last"] - s["close"]) / s["close"] * 100) if s["close"] else 0
    print(f'{s["symbol"]}: ${s["last"]:.2f} ({chg:+.2f}%)')
```

### Pattern 6: Market Depth Analysis

```python
# Get order book for a US stock (renders table + saves PNG chart)
result = await MMRHelpers.depth("AAPL")
emit(result)

# Get depth as JSON with chart path for visual analysis
data = await MMRHelpers.depth_json("AAPL")
# data["data"]["bids"] → [{price, size, marketMaker}, ...]
# data["data"]["asks"] → [{price, size, marketMaker}, ...]
# data["chart_path"] → path to PNG depth chart image
emit(str(data))

# International stock depth
result = await MMRHelpers.depth("BHP", exchange="ASX", currency="AUD")
emit(result)

# Depth with more price levels
result = await MMRHelpers.depth("AAPL", rows=10)
emit(result)

# Data only, no chart
result = await MMRHelpers.depth("AAPL", no_chart=True)
emit(result)
```

The depth chart PNG is saved to `~/.local/share/mmr/depth/{SYMBOL}_{timestamp}.png` and can be read as an image file for visual analysis. The chart shows a diverging horizontal bar chart: green bid bars extend left, red ask bars extend right, with spread highlighted in the center.

### Pattern 7: Trade International Stocks

```python
# Buy ASX stock
result = await MMRHelpers.buy("BHP", market=True, quantity=100, exchange="ASX", currency="AUD")
emit(result)

# Create proposal for ASX stock with position sizing
result = await MMRHelpers.propose("WDS", "BUY", confidence=0.75,
    reasoning="LNG producer, rising fuel prices", exchange="ASX", currency="AUD")
emit(result)
```

### Pattern 8: Download Historical Data

```python
# Download daily bars from Massive.com
result = await MMRHelpers.data_download(["AAPL", "MSFT"], bar_size="1 day", days=365)
emit(result)

# Download for an entire universe
result = await MMRHelpers.history_massive(universe="tech_stocks", bar_size="1 day", prev_days=60)
emit(result)
```

### Pattern 9: Analyze Company Financials

```python
bs = await MMRHelpers.balance_sheet("AAPL")
income = await MMRHelpers.income_statement("AAPL", limit=8, timeframe="annual")
cf = await MMRHelpers.cash_flow("AAPL")
ratios = await MMRHelpers.ratios("AAPL")
biz = await MMRHelpers.filing_section("AAPL", section="business")
emit(bs + income + cf + ratios + biz)
```

### Pattern 10: Options Analysis and Trading

```python
exps = await MMRHelpers.options_expirations("AAPL")
chain = await MMRHelpers.options_chain("AAPL", expiration="2026-03-20", contract_type="call", strike_min=200, strike_max=250)
implied = await MMRHelpers.options_implied("AAPL", "2026-03-20")
result = await MMRHelpers.buy_option("AAPL", "2026-03-20", 250.0, "C", 5, market=True)
emit(exps + chain + implied + result)
```

### Pattern 11: Build and Test a Strategy (Full Loop)

```python
# 1. Check/download data
summary = await MMRHelpers.data_summary()
result = await MMRHelpers.data_download(["AAPL", "MSFT"], bar_size="1 day", days=365)

# 2. Create strategy template
result = await MMRHelpers.strategy_create("momentum_breakout")
# Edit strategies/momentum_breakout.py with your logic

# 3. Backtest
result = await MMRHelpers.backtest("strategies/momentum_breakout.py", "MomentumBreakout", conids=[265598], days=365)
emit(result)

# 4. Deploy to paper trading
result = await MMRHelpers.strategy_deploy("momentum_breakout", conids=[265598], paper=True)

# 5. Monitor
signals = await MMRHelpers.strategy_signals("momentum_breakout")
emit(signals)
```

### Pattern 12: Market Scanning Workflow

```python
# Check market hours
hours = await MMRHelpers.market_hours()
emit(hours)

# Get top movers
movers = await MMRHelpers.movers()
emit(movers)

# Get news for a ticker
news = await MMRHelpers.news("AAPL", detail=True)
emit(news)

# Run sector scan with specific tickers
ideas = await MMRHelpers.ideas("momentum", tickers=["XOM", "CVX", "COP", "EOG"])
emit(ideas)
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
    book = mmr.depth("AMD")       # dict with bids/asks/last — Level 2 order book
    result = mmr.buy("AMD", market=True, quantity=10)  # SuccessFail[Trade]
    result.is_success()           # bool
    result.obj                    # Trade object on success
    result.error                  # error string on failure

    # International stocks — pass exchange/currency
    snap = mmr.snapshot("BHP", exchange="ASX", currency="AUD")
    result = mmr.buy("BHP", market=True, quantity=100, exchange="ASX", currency="AUD")

    # Close all positions (bypasses risk gate)
    mmr.close_position("BHP", skip_risk_gate=True)

    # Position sizing via proposals (volatility-aware)
    pid, leverage, snapshot = mmr.propose("AAPL", "BUY", confidence=0.7,
        reasoning="Breakout", group="tech")

    # Portfolio risk analysis
    report = mmr.risk_report()  # dict with hhi, warnings, group_allocations, summary

    # Historical data (via data_service RPC)
    result = mmr.pull_massive(symbols=["AAPL"], bar_size="1 day", prev_days=30)
    result = mmr.pull_ib(symbols=["AAPL"], bar_size="1 min", prev_days=5)
```

Key SDK methods: `portfolio()`, `positions()`, `orders()`, `trades()`, `resolve()`, `snapshot()`, `depth()`, `buy()`, `sell()`, `cancel()`, `cancel_all()`, `close_position()`, `close_all_positions()`, `resize_positions()`, `propose()`, `proposals()`, `approve()`, `reject()`, `risk_report()`, `session_status()`, `strategies()`, `enable_strategy()`, `disable_strategy()`, `account()`, `status()`, `market_hours()`, `subscribe_ticks()`, `pull_massive()`, `pull_ib()`, `data_service_status()`, `balance_sheet()`, `income_statement()`, `cash_flow()`, `ratios()`, `filing_sections()`, `options_expirations()`, `options_chain()`, `options_snapshot()`, `options_implied()`, `buy_option()`, `sell_option()`, `news()`, `movers()`, `ideas()`, `forex_snapshot()`, `forex_movers()`.
