import asyncio
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


# Resolve paths once at import time
_MMR_ROOT = str(Path(__file__).resolve().parent.parent.parent)

# Use uv run --project to resolve dependencies automatically.
# This avoids maintaining a .venv and matches the shell function pattern.
_UV_PREFIX = ["uv", "run", "--project", _MMR_ROOT]

# Serialize all CLI calls to avoid DuckDB single-writer lock contention.
# Each _run_cli/_run_cli_json/_run_sdk_script spawns a subprocess that opens
# its own DuckDB connection; concurrent writes will fail with a lock error.
_CLI_LOCK = asyncio.Lock()


def _run_cli_sync(*args: str, timeout: int = 30) -> str:
    """Run an mmr CLI command and return combined stdout+stderr as a string."""
    cmd = _UV_PREFIX + ["python", "-m", "trader.mmr_cli"] + list(args)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=_MMR_ROOT,
            timeout=timeout,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "NO_COLOR": "1"},
        )
    except subprocess.TimeoutExpired:
        return f"ERROR: Command timed out after {timeout}s — trader_service may be unresponsive (check IB Gateway connectivity)"
    output = (result.stdout + result.stderr).strip()
    # Strip any remaining ANSI escape sequences (covers all CSI sequences)
    output = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', output)
    return output


def _run_cli_json_sync(*args: str, timeout: int = 30) -> dict:
    """Run mmr CLI with --json and return parsed JSON.

    Uses only stdout for JSON parsing (stderr may contain uv warnings,
    Python logging, etc. that would break json.loads).
    """
    cmd = _UV_PREFIX + ["python", "-m", "trader.mmr_cli", "--json"] + list(args)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=_MMR_ROOT,
            timeout=timeout,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "NO_COLOR": "1"},
        )
    except subprocess.TimeoutExpired:
        return {"data": None, "error": f"timed out after {timeout}s", "timed_out": True}
    stdout = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', result.stdout.strip())
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        stderr = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', result.stderr.strip())
        return {"data": stdout or stderr, "title": None, "error": "Failed to parse JSON"}


def _run_sdk_script_sync(script: str, timeout: int = 30) -> str:
    """Run a Python script in the mmr venv and return output."""
    cmd = _UV_PREFIX + ["python", "-c", script]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=_MMR_ROOT,
            timeout=timeout,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "NO_COLOR": "1"},
        )
    except subprocess.TimeoutExpired:
        return f"ERROR: Script timed out after {timeout}s — trader_service may be unresponsive"
    output = (result.stdout + result.stderr).strip()
    output = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', output)
    return output


async def _run_cli(*args: str, timeout: int = 30) -> str:
    """Async wrapper: acquires lock, runs CLI in a thread."""
    async with _CLI_LOCK:
        return await asyncio.to_thread(_run_cli_sync, *args, timeout=timeout)


async def _run_cli_json(*args: str, timeout: int = 30) -> dict:
    """Async wrapper: acquires lock, runs CLI JSON in a thread."""
    async with _CLI_LOCK:
        return await asyncio.to_thread(_run_cli_json_sync, *args, timeout=timeout)


async def _run_sdk_script(script: str, timeout: int = 30) -> str:
    """Async wrapper: acquires lock, runs SDK script in a thread."""
    async with _CLI_LOCK:
        return await asyncio.to_thread(_run_sdk_script_sync, script, timeout=timeout)


class MMRHelpers:
    """MMR trading platform helpers. All methods are async and return strings or dicts."""

    # ------------------------------------------------------------------
    # Portfolio & Account
    # ------------------------------------------------------------------

    @staticmethod
    async def portfolio() -> dict:
        """
        Get current portfolio with P&L for all positions.
        Returns JSON dict with list of positions: symbol, position, mktPrice,
        avgCost, marketValue, unrealizedPNL, realizedPNL, dailyPNL.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.portfolio()
        # result["data"] → [{"symbol": "AAPL", "position": 100, ...}, ...]
        """
        return await _run_cli_json("portfolio")

    @staticmethod
    async def positions() -> str:
        """
        Get raw positions (no P&L).
        Returns a table with: account, conId, symbol, secType, position, avgCost, currency, total.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.positions()
        """
        return await _run_cli("positions")

    @staticmethod
    async def orders() -> dict:
        """
        Get all open orders.
        Returns JSON dict with list of orders: orderId, action, orderType, lmtPrice, totalQuantity.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.orders()
        # result["data"] → [{"orderId": 123, "action": "BUY", ...}, ...]
        """
        return await _run_cli_json("orders")

    @staticmethod
    async def trades() -> dict:
        """
        Get active trades.
        Returns JSON dict with list of trades: conId, symbol, orderId, action, status, filled, orderType, lmtPrice, totalQuantity.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.trades()
        # result["data"] → [{"symbol": "AAPL", "orderId": 123, ...}, ...]
        """
        return await _run_cli_json("trades")

    @staticmethod
    async def account() -> str:
        """
        Get the IB account ID.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.account()
        """
        return await _run_cli("account")

    @staticmethod
    async def status() -> dict:
        """
        Check service health / connectivity to trader_service.
        Returns JSON dict with: connected, account, ib_upstream_connected, NetLiquidation, positions, DailyPnL, etc.

        IMPORTANT: Always check ib_upstream_connected before trading. If False, IB Gateway
        cannot reach IBKR servers — resolve, snapshot, buy, sell will all timeout.

        Example:
        result = await MMRHelpers.status()
        # result["data"]["ib_upstream_connected"] → True (or False if Gateway can't reach IBKR)
        # result["data"]["ib_upstream_error"] → "..." (only present when ib_upstream_connected is False)
        # result["data"]["connected"] → True
        # result["data"]["DailyPnL"] → "-$241.78"
        """
        return await _run_cli_json("status")

    # ------------------------------------------------------------------
    # Symbol Resolution & Market Data
    # ------------------------------------------------------------------

    @staticmethod
    async def resolve(symbol: str, sectype: str = "STK",
                      exchange: str = "", currency: str = "") -> dict:
        """
        Resolve a symbol to IB contract details (conId, exchange, secType, etc).
        Returns JSON dict. Requires trader_service to be running.

        :param symbol: Stock ticker or conId (e.g. "AMD", "265598")
        :param sectype: Security type (STK, CASH, OPT, FUT, etc.)
        :param exchange: Exchange hint for international stocks (e.g. "ASX", "TSE")
        :param currency: Currency hint (e.g. "AUD", "JPY")
        :return: Dict with conId, symbol, secType, exchange, primaryExchange, currency, longName

        Example:
        result = await MMRHelpers.resolve("AMD")
        # result["data"] → [{"conId": 4391, "symbol": "AMD", ...}]
        result = await MMRHelpers.resolve("BHP", exchange="ASX", currency="AUD")
        """
        args = ["resolve", symbol, "--sectype", sectype]
        if exchange:
            args.extend(["--exchange", exchange])
        if currency:
            args.extend(["--currency", currency])
        return await _run_cli_json(*args)

    @staticmethod
    async def snapshot(symbol: str, delayed: bool = False,
                       exchange: str = "", currency: str = "") -> dict:
        """
        Get a price snapshot for a symbol (bid, ask, last, OHLC, volume).
        Returns JSON dict with price data. Requires trader_service to be running.

        :param symbol: Stock ticker (e.g. "AMD")
        :param delayed: Use delayed market data (default False)
        :param exchange: Exchange hint for international stocks (e.g. "ASX")
        :param currency: Currency hint (e.g. "AUD")
        :return: Dict with bid, ask, last, open, high, low, close, volume, etc.

        Example:
        result = await MMRHelpers.snapshot("AAPL")
        # result["data"]["last"] → 150.25
        # result["data"]["bid"] → 150.20
        result = await MMRHelpers.snapshot("BHP", exchange="ASX", currency="AUD")
        """
        args = ["snapshot", symbol]
        if delayed:
            args.append("--delayed")
        if exchange:
            args.extend(["--exchange", exchange])
        if currency:
            args.extend(["--currency", currency])
        return await _run_cli_json(*args)

    @staticmethod
    async def snapshots_batch(
        symbols: List[str],
        exchange: str = "",
        currency: str = "",
    ) -> dict:
        """
        Get price snapshots for multiple symbols in one call.
        Much faster than calling snapshot() in a loop (~4s vs ~4s per symbol).
        Returns JSON dict with list of snapshot dicts.
        Requires trader_service to be running.

        :param symbols: List of ticker strings (e.g. ["BHP", "CBA", "NAB"])
        :param exchange: Exchange hint for all symbols (e.g. "ASX")
        :param currency: Currency hint for all symbols (e.g. "AUD")
        :return: Dict with list of snapshots, each containing bid, ask, last, open, high, low, close

        Example:
        result = await MMRHelpers.snapshots_batch(["BHP", "CBA", "NAB"], exchange="ASX", currency="AUD")
        # result["data"] → [{"symbol": "BHP", "last": 50.15, "bid": 50.14, ...}, ...]
        """
        args = ["snapshot-batch"] + symbols
        if exchange:
            args.extend(["--exchange", exchange])
        if currency:
            args.extend(["--currency", currency])
        return await _run_cli_json(*args, timeout=120)

    @staticmethod
    async def depth(symbol: str, rows: int = 5,
                    exchange: str = "", currency: str = "",
                    smart: bool = False, no_chart: bool = False) -> str:
        """
        Get Level 2 market depth (order book) for a symbol.
        Returns a Rich table + PNG chart saved to ~/.local/share/mmr/depth/.
        Requires trader_service to be running.

        :param symbol: Stock ticker (e.g. "AAPL")
        :param rows: Number of price levels per side (default 5, max 5)
        :param exchange: Exchange hint for international stocks (e.g. "ASX")
        :param currency: Currency hint (e.g. "AUD")
        :param smart: Use SMART depth aggregation
        :param no_chart: Skip PNG chart generation
        :return: Order book table + chart path. JSON mode returns {data, chart_path}

        Example:
        result = await MMRHelpers.depth("AAPL")
        result = await MMRHelpers.depth("BHP", exchange="ASX", currency="AUD")
        """
        args = ["depth", symbol, "--rows", str(rows)]
        if exchange:
            args.extend(["--exchange", exchange])
        if currency:
            args.extend(["--currency", currency])
        if smart:
            args.append("--smart")
        if no_chart:
            args.append("--no-chart")
        args.append("--no-open")  # never open Preview from LLM context
        return await _run_cli(*args)

    @staticmethod
    async def depth_json(symbol: str, rows: int = 5,
                         exchange: str = "", currency: str = "",
                         smart: bool = False) -> dict:
        """
        Get Level 2 market depth as JSON with a chart PNG path.
        Returns {"data": {symbol, conId, bids, asks, bid, ask, last, ...}, "chart_path": "/path/to/png"}.
        The chart_path points to a PNG image that can be read as a file for visual analysis.
        Requires trader_service to be running.

        :param symbol: Stock ticker (e.g. "AAPL")
        :param rows: Number of price levels per side (default 5)
        :param exchange: Exchange hint for international stocks (e.g. "ASX")
        :param currency: Currency hint (e.g. "AUD")
        :param smart: Use SMART depth aggregation
        :return: Dict with order book data and chart_path to PNG file

        Example:
        result = await MMRHelpers.depth_json("AAPL")
        # result["data"]["bids"] → [{price, size, marketMaker}, ...]
        # result["chart_path"] → "/Users/.../.local/share/mmr/depth/AAPL_20260315_161253.png"
        """
        args = ["depth", symbol, "--rows", str(rows), "--no-open"]
        if exchange:
            args.extend(["--exchange", exchange])
        if currency:
            args.extend(["--currency", currency])
        if smart:
            args.append("--smart")
        return await _run_cli_json(*args)

    # ------------------------------------------------------------------
    # Trading
    # ------------------------------------------------------------------

    @staticmethod
    async def buy(
        symbol: str,
        quantity: Optional[float] = None,
        amount: Optional[float] = None,
        limit_price: Optional[float] = None,
        market: bool = False,
        sectype: str = "STK",
        exchange: str = "",
        currency: str = "",
    ) -> str:
        """
        Place a buy order. Must specify either market=True or limit_price.
        Must specify either quantity (shares) or amount (dollar value).
        Requires trader_service to be running.

        :param symbol: Stock ticker (e.g. "AMD")
        :param quantity: Number of shares to buy
        :param amount: Dollar amount to buy
        :param limit_price: Limit price (omit for market order)
        :param market: True for market order
        :param sectype: Security type (STK, CASH, etc.)
        :param exchange: Exchange hint for international stocks (e.g. "ASX")
        :param currency: Currency hint (e.g. "AUD")

        Example:
        result = await MMRHelpers.buy("AMD", market=True, quantity=10)
        result = await MMRHelpers.buy("AMD", market=True, amount=500.0)
        result = await MMRHelpers.buy("BHP", market=True, quantity=100, exchange="ASX", currency="AUD")
        """
        args = ["buy", symbol]
        if market:
            args.append("--market")
        if limit_price is not None:
            args.extend(["--limit", str(limit_price)])
        if quantity is not None:
            args.extend(["--quantity", str(quantity)])
        if amount is not None:
            args.extend(["--amount", str(amount)])
        if sectype != "STK":
            args.extend(["--sectype", sectype])
        if exchange:
            args.extend(["--exchange", exchange])
        if currency:
            args.extend(["--currency", currency])
        return await _run_cli(*args, timeout=30)

    @staticmethod
    async def sell(
        symbol: str,
        quantity: Optional[float] = None,
        amount: Optional[float] = None,
        limit_price: Optional[float] = None,
        market: bool = False,
        sectype: str = "STK",
        exchange: str = "",
        currency: str = "",
    ) -> str:
        """
        Place a sell order. Must specify either market=True or limit_price.
        Must specify either quantity (shares) or amount (dollar value).
        Requires trader_service to be running.

        :param symbol: Stock ticker (e.g. "AMD")
        :param quantity: Number of shares to sell
        :param amount: Dollar amount to sell
        :param limit_price: Limit price (omit for market order)
        :param market: True for market order
        :param sectype: Security type (STK, CASH, etc.)
        :param exchange: Exchange hint for international stocks (e.g. "ASX")
        :param currency: Currency hint (e.g. "AUD")

        Example:
        result = await MMRHelpers.sell("AMD", market=True, quantity=10)
        result = await MMRHelpers.sell("BHP", market=True, quantity=50, exchange="ASX", currency="AUD")
        """
        args = ["sell", symbol]
        if market:
            args.append("--market")
        if limit_price is not None:
            args.extend(["--limit", str(limit_price)])
        if quantity is not None:
            args.extend(["--quantity", str(quantity)])
        if amount is not None:
            args.extend(["--amount", str(amount)])
        if sectype != "STK":
            args.extend(["--sectype", sectype])
        if exchange:
            args.extend(["--exchange", exchange])
        if currency:
            args.extend(["--currency", currency])
        return await _run_cli(*args, timeout=30)

    @staticmethod
    async def cancel(order_id: int) -> str:
        """
        Cancel a single order by its order ID.
        Requires trader_service to be running.

        :param order_id: The order ID to cancel

        Example:
        result = await MMRHelpers.cancel(12345)
        """
        return await _run_cli("cancel", str(order_id))

    @staticmethod
    async def cancel_all() -> str:
        """
        Cancel all open orders.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.cancel_all()
        """
        return await _run_cli("cancel-all")

    # ------------------------------------------------------------------
    # Strategies
    # ------------------------------------------------------------------

    @staticmethod
    async def strategies() -> str:
        """
        List all configured strategies with their state, bar_size, conids.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.strategies()
        """
        return await _run_cli("strategies")

    @staticmethod
    async def enable_strategy(name: str) -> str:
        """
        Enable a strategy by name.
        Requires trader_service to be running.

        :param name: Strategy name

        Example:
        result = await MMRHelpers.enable_strategy("smi_crossover")
        """
        return await _run_cli("strategies", "enable", name)

    @staticmethod
    async def disable_strategy(name: str) -> str:
        """
        Disable a strategy by name.
        Requires trader_service to be running.

        :param name: Strategy name

        Example:
        result = await MMRHelpers.disable_strategy("smi_crossover")
        """
        return await _run_cli("strategies", "disable", name)

    @staticmethod
    async def reload_strategies() -> str:
        """
        Reload strategies from YAML config and re-subscribe to instruments.
        Triggers immediate reconciliation without waiting for the 30-second cycle.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.reload_strategies()
        """
        return await _run_cli("strategies", "reload")

    # ------------------------------------------------------------------
    # Universe Management (most commands work without trader_service)
    # ------------------------------------------------------------------

    @staticmethod
    async def universe_list() -> str:
        """
        List all universes with their symbol counts.
        Does NOT require trader_service.

        Example:
        result = await MMRHelpers.universe_list()
        """
        return await _run_cli("universe", "list")

    @staticmethod
    async def universe_show(name: str) -> str:
        """
        Show all symbols in a universe with conId, symbol, secType, exchange, etc.
        Does NOT require trader_service.

        :param name: Universe name (e.g. "nasdaq_top25", "portfolio")

        Example:
        result = await MMRHelpers.universe_show("portfolio")
        """
        return await _run_cli("universe", "show", name)

    @staticmethod
    async def universe_create(name: str) -> str:
        """
        Create a new empty universe.
        Does NOT require trader_service.

        :param name: Name for the new universe

        Example:
        result = await MMRHelpers.universe_create("my_watchlist")
        """
        return await _run_cli("universe", "create", name)

    @staticmethod
    async def universe_delete(name: str) -> str:
        """
        Delete a universe. Automatically confirms deletion (no interactive prompt).
        Does NOT require trader_service.

        :param name: Universe name to delete

        Example:
        result = await MMRHelpers.universe_delete("old_universe")
        """
        # Pipe "y" to auto-confirm the deletion prompt
        cmd = _UV_PREFIX + ["python", "-m", "trader.mmr_cli", "universe", "delete", name]
        result = subprocess.run(
            cmd,
            input="y\n",
            capture_output=True,
            text=True,
            cwd=_MMR_ROOT,
            timeout=15,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "NO_COLOR": "1"},
        )
        output = (result.stdout + result.stderr).strip()
        output = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', output)
        return output

    @staticmethod
    async def universe_add(name: str, symbols: List[str]) -> str:
        """
        Resolve symbols via IB and add them to a universe.
        REQUIRES trader_service to be running (uses IB for symbol resolution).
        Creates the universe if it doesn't exist.

        :param name: Universe name
        :param symbols: List of ticker symbols to add (e.g. ["AAPL", "MSFT", "AMD"])

        Example:
        result = await MMRHelpers.universe_add("tech_stocks", ["AAPL", "MSFT", "NVDA", "AMD"])
        """
        args = ["universe", "add", name] + symbols
        return await _run_cli(*args, timeout=120)

    @staticmethod
    async def universe_remove(name: str, symbol: str) -> str:
        """
        Remove a symbol from a universe.
        Does NOT require trader_service.

        :param name: Universe name
        :param symbol: Symbol to remove (e.g. "MSFT" or conId as string)

        Example:
        result = await MMRHelpers.universe_remove("tech_stocks", "INTC")
        """
        return await _run_cli("universe", "remove", name, symbol)

    @staticmethod
    async def universe_import(name: str, csv_file: str) -> str:
        """
        Bulk import symbols into a universe from a CSV file.
        CSV must have a header with SecurityDefinition fields.
        At minimum: symbol,exchange,conId,secType,primaryExchange,currency.
        Does NOT require trader_service.

        :param name: Universe name
        :param csv_file: Path to CSV file

        Example:
        result = await MMRHelpers.universe_import("my_universe", "/path/to/symbols.csv")
        """
        return await _run_cli("universe", "import", name, csv_file)

    # ------------------------------------------------------------------
    # Financial Statements (no trader_service needed, uses Massive.com API)
    # ------------------------------------------------------------------

    @staticmethod
    async def balance_sheet(
        symbol: str,
        limit: int = 4,
        timeframe: str = "quarterly",
    ) -> str:
        """
        Get balance sheet data for a company from Massive.com.
        Does NOT require trader_service. Requires massive_api_key in config.

        :param symbol: Stock ticker (e.g. "AAPL")
        :param limit: Number of periods to return (default 4)
        :param timeframe: "quarterly" or "annual" (default "quarterly")

        Example:
        result = await MMRHelpers.balance_sheet("AAPL")
        result = await MMRHelpers.balance_sheet("NVDA", limit=8, timeframe="annual")
        """
        args = ["financials", "balance", symbol, "--limit", str(limit), "--timeframe", timeframe]
        return await _run_cli(*args)

    @staticmethod
    async def income_statement(
        symbol: str,
        limit: int = 4,
        timeframe: str = "quarterly",
    ) -> str:
        """
        Get income statement data for a company from Massive.com.
        Does NOT require trader_service. Requires massive_api_key in config.

        :param symbol: Stock ticker (e.g. "AAPL")
        :param limit: Number of periods to return (default 4)
        :param timeframe: "quarterly" or "annual" (default "quarterly")

        Example:
        result = await MMRHelpers.income_statement("AAPL")
        result = await MMRHelpers.income_statement("NVDA", limit=8, timeframe="annual")
        """
        args = ["financials", "income", symbol, "--limit", str(limit), "--timeframe", timeframe]
        return await _run_cli(*args)

    @staticmethod
    async def cash_flow(
        symbol: str,
        limit: int = 4,
        timeframe: str = "quarterly",
    ) -> str:
        """
        Get cash flow statement data for a company from Massive.com.
        Does NOT require trader_service. Requires massive_api_key in config.

        :param symbol: Stock ticker (e.g. "AAPL")
        :param limit: Number of periods to return (default 4)
        :param timeframe: "quarterly" or "annual" (default "quarterly")

        Example:
        result = await MMRHelpers.cash_flow("MSFT")
        result = await MMRHelpers.cash_flow("AAPL", limit=8, timeframe="annual")
        """
        args = ["financials", "cashflow", symbol, "--limit", str(limit), "--timeframe", timeframe]
        return await _run_cli(*args)

    @staticmethod
    async def filing_section(
        symbol: str,
        section: str = "business",
        limit: int = 1,
    ) -> str:
        """
        Get 10-K filing section text from Massive.com.
        Returns the full text of the specified section from SEC 10-K filings.
        Does NOT require trader_service. Requires massive_api_key in config.

        :param symbol: Stock ticker (e.g. "AAPL")
        :param section: "business" or "risk_factors" (default "business")
        :param limit: Number of filings to return (default 1, most recent)

        Example:
        result = await MMRHelpers.filing_section("AAPL", section="business")
        result = await MMRHelpers.filing_section("NVDA", section="risk_factors")
        """
        args = ["financials", "filing", symbol, "--section", section, "--limit", str(limit)]
        return await _run_cli(*args, timeout=60)

    @staticmethod
    async def ratios(symbol: str) -> str:
        """
        Get financial ratios (TTM) for a company from Massive.com.
        Includes P/E, P/B, P/S, EV/EBITDA, ROA, ROE, current ratio,
        debt-to-equity, dividend yield, EPS, market cap, and more.
        Does NOT require trader_service. Requires massive_api_key in config.

        :param symbol: Stock ticker (e.g. "AAPL")

        Example:
        result = await MMRHelpers.ratios("AAPL")
        """
        return await _run_cli("financials", "ratios", symbol)

    # ------------------------------------------------------------------
    # Historical Data
    # ------------------------------------------------------------------

    @staticmethod
    async def history_massive(
        symbol: Optional[str] = None,
        universe: Optional[str] = None,
        bar_size: str = "1 day",
        prev_days: int = 30,
        timeout: int = 300,
    ) -> str:
        """
        Download historical data from Massive.com.
        Must specify either symbol or universe.
        Does NOT require trader_service.

        :param symbol: Single symbol (e.g. "AAPL")
        :param universe: Universe name (e.g. "portfolio")
        :param bar_size: Bar size (default "1 day")
        :param prev_days: Days of history to download (default 30)
        :param timeout: Seconds before the CLI subprocess is killed. Bump
            this for universes with many symbols or 1-min pulls.

        Example:
        result = await MMRHelpers.history_massive(symbol="AAPL", bar_size="1 day", prev_days=30)
        result = await MMRHelpers.history_massive(universe="portfolio", prev_days=60)
        """
        args = ["history", "massive", "--bar_size", bar_size, "--prev_days", str(prev_days)]
        if symbol:
            args.extend(["--symbol", symbol])
        if universe:
            args.extend(["--universe", universe])
        return await _run_cli(*args, timeout=timeout)

    @staticmethod
    async def history_ib(
        symbol: Optional[str] = None,
        universe: Optional[str] = None,
        bar_size: str = "1 min",
        prev_days: int = 5,
        timeout: int = 300,
    ) -> str:
        """
        Download historical data from Interactive Brokers.
        Must specify either symbol or universe.
        Does NOT require trader_service but does need IB Gateway running.

        :param symbol: Single symbol (e.g. "AAPL")
        :param universe: Universe name (e.g. "portfolio")
        :param bar_size: Bar size (default "1 min")
        :param prev_days: Days of history to download (default 5)
        :param timeout: Seconds before the CLI subprocess is killed. IB pacing
            limits apply; bump well past 300s for universe-scale pulls.

        Example:
        result = await MMRHelpers.history_ib(symbol="AAPL", bar_size="1 min", prev_days=5)
        """
        args = ["history", "ib", "--bar_size", bar_size, "--prev_days", str(prev_days)]
        if symbol:
            args.extend(["--symbol", symbol])
        if universe:
            args.extend(["--universe", universe])
        return await _run_cli(*args, timeout=timeout)

    # ------------------------------------------------------------------
    # Options
    # ------------------------------------------------------------------

    @staticmethod
    async def options_expirations(symbol: str) -> str:
        """
        Get available expiration dates for a symbol's options.
        Shows dates with days-to-expiration (DTE).
        Does NOT require trader_service. Requires massive_api_key in config.

        :param symbol: Stock ticker (e.g. "AAPL")

        Example:
        result = await MMRHelpers.options_expirations("AAPL")
        """
        return await _run_cli("options", "expirations", symbol)

    @staticmethod
    async def options_chain(
        symbol: str,
        expiration: Optional[str] = None,
        contract_type: Optional[str] = None,
        strike_min: Optional[float] = None,
        strike_max: Optional[float] = None,
    ) -> str:
        """
        Get options chain snapshot for a symbol.
        Shows strike, bid, ask, mid, last, volume, open interest, IV, greeks, break-even.
        Does NOT require trader_service. Requires massive_api_key in config.

        :param symbol: Stock ticker (e.g. "AAPL")
        :param expiration: Filter by expiration date (YYYY-MM-DD). Default: nearest.
        :param contract_type: Filter by "call" or "put"
        :param strike_min: Minimum strike price
        :param strike_max: Maximum strike price

        Example:
        result = await MMRHelpers.options_chain("AAPL", expiration="2026-03-20", contract_type="call")
        result = await MMRHelpers.options_chain("AAPL", strike_min=200, strike_max=250)
        """
        args = ["options", "chain", symbol]
        if expiration:
            args.extend(["-e", expiration])
        if contract_type:
            args.extend(["--type", contract_type])
        if strike_min is not None:
            args.extend(["--strike-min", str(strike_min)])
        if strike_max is not None:
            args.extend(["--strike-max", str(strike_max)])
        return await _run_cli(*args)

    @staticmethod
    async def options_snapshot(option_ticker: str) -> str:
        """
        Get detailed snapshot for a single option contract.
        Shows greeks, bid/ask, IV, open interest, break-even, underlying price.
        Does NOT require trader_service. Requires massive_api_key in config.

        :param option_ticker: Massive option ticker (e.g. "O:AAPL260320C00250000")

        Example:
        result = await MMRHelpers.options_snapshot("O:AAPL260320C00250000")
        """
        return await _run_cli("options", "snapshot", option_ticker)

    @staticmethod
    async def options_implied(
        symbol: str,
        expiration: str,
        risk_free_rate: float = 0.05,
    ) -> str:
        """
        Get implied probability distribution for an options expiration.
        Shows market-implied vs constant-vol probability chart.
        Does NOT require trader_service. Requires massive_api_key in config.

        :param symbol: Stock ticker (e.g. "AAPL")
        :param expiration: Expiration date (YYYY-MM-DD)
        :param risk_free_rate: Risk-free rate (default 0.05)

        Example:
        result = await MMRHelpers.options_implied("AAPL", "2026-03-20")
        """
        args = ["options", "implied", symbol, "-e", expiration,
                "--risk-free-rate", str(risk_free_rate)]
        return await _run_cli(*args)

    @staticmethod
    async def buy_option(
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        quantity: float,
        limit_price: Optional[float] = None,
        market: bool = False,
    ) -> str:
        """
        Buy option contracts. Resolves option contract via IB and places order.
        REQUIRES trader_service to be running.

        :param symbol: Underlying ticker (e.g. "AAPL")
        :param expiration: Expiration date (YYYY-MM-DD)
        :param strike: Strike price
        :param right: "C" for call, "P" for put
        :param quantity: Number of contracts
        :param limit_price: Limit price per contract (omit for market order)
        :param market: True for market order

        Example:
        result = await MMRHelpers.buy_option("AAPL", "2026-03-20", 250.0, "C", 5, market=True)
        result = await MMRHelpers.buy_option("AAPL", "2026-03-20", 250.0, "C", 5, limit_price=3.50)
        """
        args = ["options", "buy", symbol, "-e", expiration, "-s", str(strike),
                "-r", right, "-q", str(quantity)]
        if market:
            args.append("--market")
        if limit_price is not None:
            args.extend(["--limit", str(limit_price)])
        return await _run_cli(*args, timeout=30)

    @staticmethod
    async def sell_option(
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        quantity: float,
        limit_price: Optional[float] = None,
        market: bool = False,
    ) -> str:
        """
        Sell option contracts. Resolves option contract via IB and places order.
        REQUIRES trader_service to be running.

        :param symbol: Underlying ticker (e.g. "AAPL")
        :param expiration: Expiration date (YYYY-MM-DD)
        :param strike: Strike price
        :param right: "C" for call, "P" for put
        :param quantity: Number of contracts
        :param limit_price: Limit price per contract (omit for market order)
        :param market: True for market order

        Example:
        result = await MMRHelpers.sell_option("AAPL", "2026-03-20", 250.0, "C", 5, market=True)
        result = await MMRHelpers.sell_option("AAPL", "2026-03-20", 250.0, "P", 3, limit_price=2.00)
        """
        args = ["options", "sell", symbol, "-e", expiration, "-s", str(strike),
                "-r", right, "-q", str(quantity)]
        if market:
            args.append("--market")
        if limit_price is not None:
            args.extend(["--limit", str(limit_price)])
        return await _run_cli(*args, timeout=30)

    # ------------------------------------------------------------------
    # Data Exploration (no service needed)
    # ------------------------------------------------------------------

    @staticmethod
    async def data_summary() -> str:
        """
        Show summary of all local historical data in DuckDB.
        Returns JSON with conId, symbol, bar_size, start/end dates.
        Does NOT require any service.

        Example:
        result = await MMRHelpers.data_summary()
        """
        result = await _run_cli_json("data", "summary")
        return json.dumps(result, indent=2)

    @staticmethod
    async def data_query(
        symbol: str,
        bar_size: str = "1 day",
        days: int = 30,
        tail: Optional[int] = None,
    ) -> str:
        """
        Read OHLCV data from local DuckDB.
        Does NOT require any service.

        :param symbol: Stock ticker or conId (e.g. "AAPL", "265598")
        :param bar_size: Bar size (default "1 day")
        :param days: Days of history to query (default 30)
        :param tail: Show only last N rows

        Example:
        result = await MMRHelpers.data_query("AAPL", bar_size="1 day", days=30)
        """
        args = ["data", "query", symbol, "--bar-size", bar_size, "--days", str(days)]
        if tail is not None:
            args.extend(["--tail", str(tail)])
        result = await _run_cli_json(*args)
        return json.dumps(result, indent=2)

    @staticmethod
    async def data_download(
        symbols: List[str],
        bar_size: str = "1 day",
        days: int = 365,
        timeout: int = 300,
        progress: bool = False,
    ) -> str:
        """
        Download data from Massive.com to local DuckDB.
        Does NOT require trader_service. Requires massive_api_key in config.

        :param symbols: List of tickers to download (e.g. ["AAPL", "MSFT"])
        :param bar_size: Bar size (default "1 day")
        :param days: Days of history to download (default 365)
        :param timeout: Seconds before the CLI subprocess is killed. In the
            default (batch) mode this is the total budget for all symbols.
            With progress=True it's the per-symbol budget.
        :param progress: If True, download one symbol per subprocess and
            print a live status line to stdout between each. Trades a little
            subprocess overhead (~1s per symbol) for visible progress on
            long pulls. Also makes partial progress easy to reason about
            if a symbol fails midway — prior symbols are already persisted.

        NOTE: Do NOT try to run this with ``asyncio.create_task()`` or
        ``asyncio.run_coroutine_threadsafe()`` — the helper cell has no
        running event loop to schedule a background task against. Just
        ``await`` this directly; the real work already runs in a worker
        thread via ``asyncio.to_thread``, so long-running pulls don't block
        the helper runtime.

        Example:
        # Fast single-subprocess batch download
        result = await MMRHelpers.data_download(["AAPL", "MSFT"], bar_size="1 day", days=365)

        # Big 1-min pull with a live per-symbol progress line
        result = await MMRHelpers.data_download(
            big_list, bar_size="1 min", days=730, progress=True, timeout=120,
        )
        """
        if not progress:
            args = ["data", "download"] + symbols + ["--bar-size", bar_size, "--days", str(days)]
            result = await _run_cli_json(*args, timeout=timeout)
            return json.dumps(result, indent=2)

        # progress=True: one subprocess per symbol so we can stream progress.
        import sys as _sys
        total = len(symbols)
        completed = 0
        failed = 0
        per_symbol = []
        print(
            f"Downloading {total} symbols ({bar_size}, {days}d)...",
            flush=True, file=_sys.stdout,
        )
        for i, symbol in enumerate(symbols, 1):
            print(f"  [{i}/{total}] {symbol}...", flush=True, file=_sys.stdout)
            args = ["data", "download", symbol, "--bar-size", bar_size, "--days", str(days)]
            r = await _run_cli_json(*args, timeout=timeout)
            success = bool(r.get("success") or r.get("completed", 0) > 0) and not r.get("error")
            per_symbol.append({
                "symbol": symbol,
                "success": success,
                "message": r.get("message") or r.get("error") or "",
            })
            if success:
                completed += 1
                print(
                    f"    ✓ {symbol}: {r.get('message', 'ok')}",
                    flush=True, file=_sys.stdout,
                )
            else:
                failed += 1
                print(
                    f"    ✗ {symbol}: {r.get('error') or r.get('message', 'unknown error')}",
                    flush=True, file=_sys.stdout,
                )

        print(
            f"Done: {completed}/{total} succeeded, {failed} failed",
            flush=True, file=_sys.stdout,
        )
        return json.dumps({
            "success": failed == 0,
            "completed": completed,
            "failed": failed,
            "total": total,
            "per_symbol": per_symbol,
        }, indent=2)

    # ------------------------------------------------------------------
    # Backtesting (no service needed)
    # ------------------------------------------------------------------

    @staticmethod
    async def backtest(
        strategy_path: str,
        class_name: str,
        conids: Optional[List[int]] = None,
        universe: Optional[str] = None,
        days: int = 365,
        capital: float = 100000,
        bar_size: str = "1 min",
        params: Optional[Dict[str, Any]] = None,
        summary_only: bool = True,
        timeout: int = 300,
    ) -> str:
        """
        Backtest a strategy against historical data.
        Does NOT require any service. Uses local DuckDB data.

        Path gotcha: the CLI is spawned from the skill directory, not your
        project root — so a relative path like ``strategies/foo.py`` will
        resolve wrong. Pass an **absolute path** (or just the filename, if
        the strategy is under the project's configured strategies_directory).

        Timeout gotcha: strategies that only override ``on_prices`` are
        O(N²) on a backtest — a 1-year × 1-min run hangs past the 300s
        default. Call ``strategies_inspect()`` first and filter by
        ``mode == "precompute"``, or drop to ``days=30`` for a calibration
        run on slow strategies before committing to the full window.

        :param strategy_path: **Absolute** path to strategy .py file
            (e.g. ``"/Users/you/dev/mmr/strategies/my_strategy.py"``)
        :param class_name: Strategy class name (e.g. "MyStrategy")
        :param conids: List of contract IDs to backtest
        :param universe: Universe name (alternative to conids)
        :param days: Days of history (default 365). Use 30 for O(N²)
            strategies to avoid timeout.
        :param capital: Initial capital (default 100000)
        :param bar_size: Bar size (default "1 min")
        :param params: Optional parameter overrides, e.g.
            ``{"EMA_PERIOD": 15, "BAND_MULT": 1.5}``. Upper-case keys are
            applied to class attributes; lower-case to ``self.params``.
            Typos raise ``ValueError`` rather than silently no-op.
        :param summary_only: Default True — omit the per-trade array from
            the JSON response (trades still persist to the history store
            for ``backtests_show`` unless ``--no-save-trades`` is passed).
            Pass False only when you actually need the fills client-side.
        :param timeout: Seconds before the CLI subprocess is killed.
            Default 300s matches the CLI's internal cap.

        Example:
        result = await MMRHelpers.backtest(
            "/Users/you/dev/mmr/strategies/keltner_breakout.py",
            "KeltnerBreakout", conids=[265598],
            params={"EMA_PERIOD": 15, "BAND_MULT": 1.5})
        """
        args = ["backtest", "-s", strategy_path, "--class", class_name,
                "--days", str(days), "--capital", str(capital), "--bar-size", bar_size]
        if conids:
            args.extend(["--conids"] + [str(c) for c in conids])
        if universe:
            args.extend(["--universe", universe])
        if params:
            args.extend(["--params", json.dumps(params)])
        if summary_only:
            args.append("--summary-only")
        result = await _run_cli_json(*args, timeout=timeout)
        return json.dumps(result, indent=2)

    @staticmethod
    async def backtest_sweep(
        strategy_path: str,
        class_name: str,
        param_grid: Dict[str, List[Any]],
        conids: Optional[List[int]] = None,
        universe: Optional[str] = None,
        days: int = 180,
        capital: float = 100000,
        bar_size: str = "1 min",
        top: int = 10,
        note: str = "",
        timeout: int = 1800,
    ) -> str:
        """
        Cartesian-product parameter sweep — runs one backtest per grid
        combination, persists each to the history store, and returns a
        composite-score leaderboard.

        Runs in-process (data is loaded once, reused across combos) so
        this is the right path when the grid is small-to-medium
        (≤ a few dozen combos). For many-combo or multi-strategy sweeps
        prefer ``backtest_batch`` which parallelises at the subprocess
        level.

        Does NOT require any service.

        :param strategy_path: Absolute path to the strategy .py file
        :param class_name: Strategy class name
        :param param_grid: ``{"EMA_PERIOD": [10, 20, 30], "BAND_MULT": [1.5, 2.0]}``
            — cartesian product expands to 6 runs.
        :param conids: Contract IDs (or use universe)
        :param universe: Universe name (alternative to conids)
        :param days: Backtest window (default 180 — shorter than single
            backtest default because you're running many of them)
        :param capital: Initial capital
        :param bar_size: Bar size (default "1 min")
        :param top: Show top-N in the leaderboard (default 10)
        :param note: Free-text note stamped on every sweep run
        :param timeout: Overall subprocess timeout. Default 30 minutes
            covers most sweeps; bump it for 100+ combos.

        Example:
        result = await MMRHelpers.backtest_sweep(
            "/path/to/strategies/keltner_breakout.py", "KeltnerBreakout",
            param_grid={"EMA_PERIOD": [10, 15, 20], "BAND_MULT": [1.5, 2.0]},
            conids=[756733], days=180)
        """
        args = ["bt-sweep", "-s", strategy_path, "--class", class_name,
                "--days", str(days), "--capital", str(capital),
                "--bar-size", bar_size, "--grid", json.dumps(param_grid),
                "--top", str(top)]
        if conids:
            args.extend(["--conids"] + [str(c) for c in conids])
        if universe:
            args.extend(["--universe", universe])
        if note:
            args.extend(["--note", note])
        result = await _run_cli_json(*args, timeout=timeout)
        return json.dumps(result, indent=2)

    @staticmethod
    async def backtest_batch(
        jobs: List[Dict[str, Any]],
        concurrency: int = 4,
        summary_only: bool = True,
        per_job_timeout: int = 300,
    ) -> str:
        """
        Run several backtests in parallel via subprocess concurrency.

        Use this when you have heterogeneous jobs (different strategies,
        different symbols) that don't fit ``backtest_sweep``'s single-
        strategy / cartesian-grid shape. Each job spawns its own CLI
        subprocess, so the GIL doesn't serialise them.

        Returns a list in the same order as ``jobs``, with a ``status``
        field per entry: ``"ok"`` (summary attached), ``"timeout"``,
        or ``"error"`` (details in ``error``).

        Does NOT require any service.

        :param jobs: List of dicts, each with keys:
            ``strategy_path`` (required, absolute),
            ``class_name`` (required),
            ``conids`` or ``universe`` (required),
            plus any of ``days``, ``capital``, ``bar_size``, ``params``,
            ``note``.
        :param concurrency: Max concurrent subprocesses (default 4)
        :param summary_only: Omit per-trade arrays from each response
            (recommended — saves context)
        :param per_job_timeout: Seconds before a single job is killed

        Example:
        result = await MMRHelpers.backtest_batch([
            {"strategy_path": "/abs/path/keltner.py", "class_name": "KeltnerBreakout",
             "conids": [756733], "days": 180},
            {"strategy_path": "/abs/path/vwap.py", "class_name": "VwapReversion",
             "conids": [756733], "days": 180},
        ], concurrency=4)
        """
        async def _one(job: Dict[str, Any]) -> Dict[str, Any]:
            try:
                raw = await MMRHelpers.backtest(
                    strategy_path=job["strategy_path"],
                    class_name=job["class_name"],
                    conids=job.get("conids"),
                    universe=job.get("universe"),
                    days=job.get("days", 180),
                    capital=job.get("capital", 100000),
                    bar_size=job.get("bar_size", "1 min"),
                    params=job.get("params"),
                    summary_only=summary_only,
                    timeout=per_job_timeout,
                )
                parsed = json.loads(raw)
                return {"status": "ok", "input": job, "result": parsed}
            except asyncio.TimeoutError:
                return {"status": "timeout", "input": job}
            except Exception as ex:
                return {"status": "error", "input": job,
                        "error": f"{type(ex).__name__}: {ex}"}

        sem = asyncio.Semaphore(max(1, concurrency))

        async def _guarded(j: Dict[str, Any]) -> Dict[str, Any]:
            async with sem:
                return await _one(j)

        results = await asyncio.gather(*[_guarded(j) for j in jobs])
        return json.dumps({
            "data": results,
            "title": f"Backtest batch ({len(jobs)} jobs, concurrency={concurrency})",
        }, indent=2)

    @staticmethod
    async def strategies_inspect(
        directory: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> str:
        """
        Discover tunable params + dispatch mode for every Strategy subclass
        in ``strategies/``. AST-based — does NOT execute the strategy file.

        Returns ``[{file, class, mode, tunables, docstring}]`` where:
          * ``mode`` is ``"precompute"`` (fast, O(N)), ``"on_prices"``
            (slow, O(N²) on a backtest — use days=30 or skip),
            ``"precompute+on_prices"``, or ``"inherited"``.
          * ``tunables`` is ``{KEY: default_value}`` combining class-level
            constants (``EMA_PERIOD = 20``) and ``self.params.get('key', default)``
            lookups.

        Use this **before** planning a sweep so you know:
          (a) which strategies are fast enough to run on 1-min 1-year data,
          (b) what knobs exist to sweep, and
          (c) their default values (so your grid brackets them sensibly).

        Does NOT require any service.

        :param directory: Override strategies directory
        :param strategy: Inspect a single file (path relative to
            project root or absolute)

        Example:
        result = await MMRHelpers.strategies_inspect()
        # → [{"class": "KeltnerBreakout", "mode": "precompute",
        #     "tunables": {"EMA_PERIOD": 20, "BAND_MULT": 2.0, ...}}, ...]
        """
        args = ["strategies", "inspect"]
        if directory:
            args.extend(["--directory", directory])
        if strategy:
            args.extend(["--strategy", strategy])
        result = await _run_cli_json(*args)
        return json.dumps(result, indent=2)

    # ------------------------------------------------------------------
    # Strategy Lifecycle (no service needed)
    # ------------------------------------------------------------------

    @staticmethod
    async def strategy_create(name: str, directory: str = "strategies") -> str:
        """
        Create a strategy template file.
        Does NOT require any service.

        :param name: Strategy name in snake_case (e.g. "my_strategy")
        :param directory: Directory for strategy file (default "strategies")

        Example:
        result = await MMRHelpers.strategy_create("momentum_breakout")
        """
        args = ["strategies", "create", name, "--directory", directory]
        result = await _run_cli_json(*args)
        return json.dumps(result, indent=2)

    @staticmethod
    async def strategy_deploy(
        name: str,
        conids: Optional[List[int]] = None,
        universe: Optional[str] = None,
        bar_size: str = "1 min",
        days: int = 90,
        paper: bool = True,
    ) -> str:
        """
        Deploy a strategy to strategy_runtime.yaml.
        Does NOT require any service.

        :param name: Strategy name
        :param conids: Contract IDs
        :param universe: Universe name (alternative to conids)
        :param bar_size: Bar size (default "1 min")
        :param days: Historical days prior (default 90)
        :param paper: Paper trading mode (default True)

        Example:
        result = await MMRHelpers.strategy_deploy("my_strategy", conids=[265598], paper=True)
        """
        args = ["strategies", "deploy", name, "--bar-size", bar_size, "--days", str(days)]
        if conids:
            args.extend(["--conids"] + [str(c) for c in conids])
        if universe:
            args.extend(["--universe", universe])
        if paper:
            args.append("--paper")
        result = await _run_cli_json(*args)
        return json.dumps(result, indent=2)

    @staticmethod
    async def strategy_undeploy(name: str) -> str:
        """
        Remove a strategy from strategy_runtime.yaml.
        Does NOT require any service.

        :param name: Strategy name to remove

        Example:
        result = await MMRHelpers.strategy_undeploy("my_strategy")
        """
        result = await _run_cli_json("strategies", "undeploy", name)
        return json.dumps(result, indent=2)

    @staticmethod
    async def strategy_signals(name: str, limit: int = 20) -> str:
        """
        View recent signals from a strategy (from event store).
        Does NOT require any service.

        :param name: Strategy name
        :param limit: Number of signals to show (default 20)

        Example:
        result = await MMRHelpers.strategy_signals("my_strategy")
        """
        result = await _run_cli_json("strategies", "signals", name,
                                         "--limit", str(limit))
        return json.dumps(result, indent=2)

    @staticmethod
    async def strategy_backtest(
        name: str,
        days: int = 365,
        capital: float = 100000,
    ) -> str:
        """
        Backtest a deployed strategy by its name (looks up config).
        Does NOT require any service.

        :param name: Strategy name (must be in strategy_runtime.yaml)
        :param days: Days of history (default 365)
        :param capital: Initial capital (default 100000)

        Example:
        result = await MMRHelpers.strategy_backtest("smi_crossover_amd", days=365)
        """
        args = ["strategies", "backtest", name, "--days", str(days), "--capital", str(capital)]
        result = await _run_cli_json(*args, timeout=300)
        return json.dumps(result, indent=2)

    @staticmethod
    async def backtests_list(
        sort_by: str = "score",
        limit: int = 25,
        strategy: Optional[str] = None,
        descending: bool = True,
        include_archived: bool = False,
        archived_only: bool = False,
    ) -> str:
        """
        List past backtest runs from the local history store.
        Does NOT require any service.

        The default ``sort_by="score"`` ranks by a composite quality score
        (weighted blend of sortino, profit_factor, expectancy_bps, return,
        and drawdown, gated by trade count). Use ``"time"`` for most-recent
        first, or any metric column: ``return``, ``sharpe``, ``sortino``,
        ``calmar``, ``pf``, ``expectancy``, ``max_dd``, ``trades``, ``tim``.

        Archived runs are hidden by default. Pass ``include_archived=True``
        to show everything, or ``archived_only=True`` to list only hidden
        runs (useful for reviewing before purging or bulk unarchiving).

        :param sort_by: Sort column (default "score")
        :param limit: Max rows (default 25)
        :param strategy: Filter by class name (optional)
        :param descending: Descending order (default True — "best first")
        :param include_archived: Show archived runs alongside active (default False)
        :param archived_only: Show only archived runs (default False)

        Example:
        result = await MMRHelpers.backtests_list(sort_by="score", limit=10)
        result = await MMRHelpers.backtests_list(strategy="KeltnerBreakout")
        result = await MMRHelpers.backtests_list(include_archived=True)
        """
        args = ["backtests", "--sort-by", sort_by, "--limit", str(limit)]
        if not descending:
            args.append("--asc")
        if strategy:
            args.extend(["--strategy", strategy])
        if archived_only:
            args.append("--archived")
        elif include_archived:
            args.append("--all")
        result = await _run_cli_json(*args)
        return json.dumps(result, indent=2)

    @staticmethod
    async def backtests_archive(run_ids: List[int]) -> str:
        """
        Archive one or more backtest runs — hides them from the default
        ``backtests_list`` but keeps the data for later analysis. Reversible
        via ``backtests_unarchive``. Prefer this over ``delete`` when tidying
        up the history view: archived runs remain queryable with
        ``include_archived=True`` or ``archived_only=True``.

        Does NOT require any service.

        :param run_ids: List of run ids to archive

        Example:
        result = await MMRHelpers.backtests_archive([72, 73, 74])
        """
        args = ["backtests", "archive"] + [str(i) for i in run_ids]
        result = await _run_cli_json(*args)
        return json.dumps(result, indent=2)

    @staticmethod
    async def backtests_unarchive(run_ids: List[int]) -> str:
        """
        Restore previously-archived runs to the default ``backtests_list``.

        Does NOT require any service.

        :param run_ids: List of run ids to unarchive

        Example:
        result = await MMRHelpers.backtests_unarchive([72])
        """
        args = ["backtests", "unarchive"] + [str(i) for i in run_ids]
        result = await _run_cli_json(*args)
        return json.dumps(result, indent=2)

    @staticmethod
    async def backtests_show(run_id: int) -> str:
        """
        Show full detail for a past backtest run, including statistical-
        confidence tests (PSR, t-stat, bootstrap CIs, P&L distribution,
        losing-streak MC) when the run was saved with trade/equity data.

        Does NOT require any service.

        The ``statistical_confidence`` block in the response tells you
        whether the run's edge is statistically distinguishable from noise.
        Key fields:
          - ``probabilistic_sharpe``: P(true Sharpe > 0) in [0, 1]; > 0.95
            is strong evidence of a real edge.
          - ``p_value``: two-sided t-test of mean per-trade P&L == 0.
            < 0.05 is the conventional significance threshold.
          - ``return_ci_lo/hi``: bootstrap 95% CI on mean per-trade $ P&L.
            If it straddles zero, the edge isn't statistically reliable.
          - ``sharpe_ci_lo/hi``: bootstrap 95% CI on annualised Sharpe.
          - ``pnl_skew`` + ``pnl_excess_kurtosis``: negative skew + high
            kurtosis (> 5) is the "blow-up risk" signature Sharpe misses.
          - ``losing_streak_actual`` vs ``losing_streak_mc_95``: if actual
            exceeds MC 95th pct, losses cluster (auto-correlated risk).

        :param run_id: Backtest run ID (from backtests_list)

        Example:
        result = await MMRHelpers.backtests_show(42)
        """
        result = await _run_cli_json("backtests", "show", str(run_id))
        return json.dumps(result, indent=2)

    # ------------------------------------------------------------------
    # Position Management
    # ------------------------------------------------------------------

    @staticmethod
    async def close_all_positions() -> str:
        """
        Close all positions at market. Cancels all open orders first, then
        submits market sell/buy-to-cover for every position. Bypasses risk gate.
        Requires trader_service. WARNING: This is a liquidation command.

        Example:
        result = await MMRHelpers.close_all_positions()
        """
        # Pipe "y" to auto-confirm
        cmd = _UV_PREFIX + ["python", "-m", "trader.mmr_cli", "close-all-positions"]
        result = subprocess.run(
            cmd, input="y\n", capture_output=True, text=True,
            cwd=_MMR_ROOT, timeout=120,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "NO_COLOR": "1"},
        )
        output = (result.stdout + result.stderr).strip()
        output = re.sub(r'\x1b\[[0-9;]*[a-zA-Z]', '', output)
        return output

    @staticmethod
    async def resize_positions(
        max_bound: Optional[float] = None,
        min_bound: Optional[float] = None,
        dry_run: bool = False,
    ) -> str:
        """
        Proportionally resize all positions to fit within a target portfolio value.
        Requires trader_service.

        :param max_bound: Maximum portfolio value (trim positions if above)
        :param min_bound: Minimum portfolio value (grow positions if below)
        :param dry_run: Preview without executing (default False)

        Example:
        result = await MMRHelpers.resize_positions(max_bound=500000, dry_run=True)
        """
        args = ["resize-positions"]
        if max_bound is not None:
            args.extend(["--max-bound", str(max_bound)])
        if min_bound is not None:
            args.extend(["--min-bound", str(min_bound)])
        if dry_run:
            args.append("--dry-run")
        return await _run_cli(*args, timeout=120)

    # ------------------------------------------------------------------
    # Trade Proposals (no service needed to create; approve needs trader_service)
    # ------------------------------------------------------------------

    @staticmethod
    async def propose(
        symbol: str,
        action: str,
        quantity: Optional[float] = None,
        amount: Optional[float] = None,
        market: bool = True,
        limit_price: Optional[float] = None,
        confidence: float = 0.0,
        reasoning: str = "",
        thesis: str = "",
        source: str = "",
        group: str = "",
        exchange: str = "",
        currency: str = "",
    ) -> dict:
        """
        Create a trade proposal. Stored locally — not executed until approved.
        Auto-sizes position based on confidence, ATR volatility, and position_sizing.yaml
        config when neither quantity nor amount is specified.
        Does NOT require trader_service to create (but ATR/snapshot enrichment needs it).

        Position sizing pipeline: base_position * risk_multiplier * confidence_scale * volatility_adjustment.
        Volatile stocks (high ATR%) get smaller positions; stable stocks get larger ones.

        :param symbol: Stock ticker
        :param action: "BUY" or "SELL"
        :param quantity: Number of shares (optional — auto-sized if omitted)
        :param amount: Dollar amount (optional — auto-sized if omitted)
        :param market: Market order (default True)
        :param limit_price: Limit price
        :param confidence: Confidence 0.0-1.0 (affects position sizing)
        :param reasoning: Trade reasoning text
        :param thesis: Short thesis label
        :param source: Source label (manual, llm, scanner)
        :param group: Position group name (e.g. "mining", "tech") — auto-adds symbol to group
        :param exchange: Exchange hint (e.g. "ASX")
        :param currency: Currency hint (e.g. "AUD")

        Example:
        result = await MMRHelpers.propose("AAPL", "BUY", confidence=0.7, reasoning="Breakout above resistance")
        result = await MMRHelpers.propose("BHP", "BUY", confidence=0.6, group="mining", exchange="ASX", currency="AUD")
        """
        args = ["propose", symbol, action]
        if market:
            args.append("--market")
        if limit_price is not None:
            args.extend(["--limit", str(limit_price)])
        if quantity is not None:
            args.extend(["--quantity", str(quantity)])
        if amount is not None:
            args.extend(["--amount", str(amount)])
        if confidence:
            args.extend(["--confidence", str(confidence)])
        if reasoning:
            args.extend(["--reasoning", reasoning])
        if thesis:
            args.extend(["--thesis", thesis])
        if source:
            args.extend(["--source", source])
        if group:
            args.extend(["--group", group])
        if exchange:
            args.extend(["--exchange", exchange])
        if currency:
            args.extend(["--currency", currency])
        return await _run_cli_json(*args, timeout=30)

    @staticmethod
    async def proposals(status: Optional[str] = None, all_statuses: bool = False) -> str:
        """
        List trade proposals. Does NOT require trader_service.

        :param status: Filter by status (PENDING, EXECUTED, REJECTED, etc.)
        :param all_statuses: Show all statuses (default: PENDING only)

        Example:
        result = await MMRHelpers.proposals()
        result = await MMRHelpers.proposals(status="EXECUTED")
        """
        args = ["proposals"]
        if status:
            args.extend(["--status", status])
        if all_statuses:
            args.append("--all")
        return await _run_cli(*args)

    @staticmethod
    async def approve(proposal_id: int) -> str:
        """
        Approve and execute a trade proposal. Requires trader_service.

        :param proposal_id: Proposal ID to approve

        Example:
        result = await MMRHelpers.approve(42)
        """
        return await _run_cli("approve", str(proposal_id), timeout=30)

    @staticmethod
    async def reject(proposal_id: int, reason: str = "") -> str:
        """
        Reject a trade proposal. Does NOT require trader_service.

        :param proposal_id: Proposal ID to reject
        :param reason: Optional rejection reason

        Example:
        result = await MMRHelpers.reject(42, reason="Changed thesis")
        """
        args = ["reject", str(proposal_id)]
        if reason:
            args.extend(["--reason", reason])
        return await _run_cli(*args)

    # ------------------------------------------------------------------
    # Market Scanning & Ideas
    # ------------------------------------------------------------------

    @staticmethod
    async def ideas(
        preset: str = "momentum",
        tickers: Optional[List[str]] = None,
        universe: Optional[str] = None,
        num: int = 15,
        location: str = "",
        detail: bool = False,
        fundamentals: bool = False,
        news: bool = False,
    ) -> dict:
        """
        Scan for trading ideas using technical indicators and scoring.
        Returns JSON dict with list of scored candidates.
        Default path uses Massive.com (US only, ~4s). Use --location for
        international markets via IB (slower, ~30-90s).
        Requires massive_api_key (default) or trader_service (with --location).

        :param preset: momentum, gap-up, gap-down, mean-reversion, breakout, volatile
        :param tickers: Scan specific tickers instead of movers
        :param universe: Scan a universe instead of movers
        :param num: Number of results (default 15)
        :param location: IB location code for international markets (e.g. "STK.AU.ASX")
        :param detail: Show all columns including indicators
        :param fundamentals: Enrich with financial ratios (slower)
        :param news: Enrich with latest news + sentiment (slower)
        :return: Dict with list of ideas: ticker, price, change_pct, volume, score, signal, etc.

        Example:
        result = await MMRHelpers.ideas()
        # result["data"] → [{"ticker": "AAPL", "price": 150, "score": 8.2, "signal": "BUY", ...}]
        result = await MMRHelpers.ideas("momentum", tickers=["AAPL", "MSFT", "AMD"])
        result = await MMRHelpers.ideas("momentum", location="STK.AU.ASX", tickers=["BHP", "RIO"])
        """
        args = ["ideas", preset, "--num", str(num)]
        if tickers:
            args.extend(["--tickers"] + tickers)
        if universe:
            args.extend(["--universe", universe])
        if location:
            args.extend(["--location", location])
        if detail:
            args.append("--detail")
        if fundamentals:
            args.append("--fundamentals")
        if news:
            args.append("--news")
        timeout = 120 if location else 30
        return await _run_cli_json(*args, timeout=timeout)

    @staticmethod
    async def news(ticker: str = "", limit: int = 10, detail: bool = False) -> str:
        """
        Get market news, optionally for a specific ticker with sentiment.
        Requires massive_api_key. Does NOT require trader_service.

        :param ticker: Stock ticker (omit for general market news)
        :param limit: Number of articles (default 10)
        :param detail: Show full details + sentiment

        Example:
        result = await MMRHelpers.news()
        result = await MMRHelpers.news("AAPL", limit=5, detail=True)
        """
        args = ["news"]
        if ticker:
            args.append(ticker)
        args.extend(["--limit", str(limit)])
        if detail:
            args.append("--detail")
        return await _run_cli(*args)

    @staticmethod
    async def movers(market: str = "stocks", losers: bool = False, num: int = 20) -> str:
        """
        Get top market movers from Massive.com.
        Requires massive_api_key. Does NOT require trader_service.

        :param market: stocks, crypto, indices, options, futures
        :param losers: Show losers instead of gainers
        :param num: Number of results (default 20)

        Example:
        result = await MMRHelpers.movers()
        result = await MMRHelpers.movers(market="crypto", losers=True)
        """
        args = ["movers", "--market", market, "--num", str(num)]
        if losers:
            args.append("--losers")
        return await _run_cli(*args)

    @staticmethod
    async def market_hours() -> dict:
        """
        Show market open/close status for major exchanges.
        Returns JSON dict with exchange statuses. Does NOT require any service.

        Example:
        result = await MMRHelpers.market_hours()
        # result["data"] → [{"exchange": "ASX", "status": "OPEN", ...}, ...]
        """
        return await _run_cli_json("market-hours")

    # ------------------------------------------------------------------
    # Forex
    # ------------------------------------------------------------------

    @staticmethod
    async def forex_snapshot(pair: str, source: str = "ib") -> str:
        """
        Get forex pair snapshot. Requires trader_service (IB source) or massive_api_key (massive source).

        :param pair: Currency pair (e.g. "EURUSD")
        :param source: "ib" or "massive"

        Example:
        result = await MMRHelpers.forex_snapshot("EURUSD")
        result = await MMRHelpers.forex_snapshot("GBPUSD", source="massive")
        """
        return await _run_cli("forex", "snapshot", pair, "--source", source)

    @staticmethod
    async def forex_movers(losers: bool = False) -> str:
        """
        Get top forex movers. Requires massive_api_key.

        :param losers: Show losers instead of gainers

        Example:
        result = await MMRHelpers.forex_movers()
        """
        args = ["forex", "movers"]
        if losers:
            args.append("--losers")
        return await _run_cli(*args)

    # ------------------------------------------------------------------
    # Risk & Session Management
    # ------------------------------------------------------------------

    @staticmethod
    async def risk() -> dict:
        """
        View current risk gate limits (max open orders, daily loss, position size, etc).
        Returns JSON dict. Requires trader_service.

        Example:
        result = await MMRHelpers.risk()
        # result["data"] → [{"limit": "max_daily_loss", "value": 1000, ...}, ...]
        """
        return await _run_cli_json("risk")

    @staticmethod
    async def session_limits() -> str:
        """
        View current position sizing limits and session settings.
        Does NOT require trader_service.

        Example:
        result = await MMRHelpers.session_limits()
        """
        return await _run_cli("session", "limits")

    @staticmethod
    async def portfolio_snapshot() -> dict:
        """
        Compact portfolio snapshot with key metrics for loop monitoring.
        Returns JSON with: total_value, daily_pnl, position_count, exposure_pct,
        and top 10 movers (sorted by absolute daily % change).
        Requires trader_service.

        Much smaller than portfolio() — designed to be called every cycle
        without bloating the context window.

        Example:
        snap = await MMRHelpers.portfolio_snapshot()
        # snap["data"]["daily_pnl"] → -51.23
        # snap["data"]["movers"][0] → {"symbol": "XRO", "change_pct": -0.0215, ...}
        """
        return await _run_cli_json("portfolio-snapshot")

    @staticmethod
    async def portfolio_diff() -> dict:
        """
        Portfolio changes since last snapshot. Returns JSON with:
        {changed, new, removed, unchanged_count, prev_timestamp}.
        Stores current snapshot automatically — next call diffs against it.
        Requires trader_service.

        On first call (no previous snapshot), all positions appear as 'new'.
        'changed' only includes positions that moved >0.5% in value.

        Example:
        diff = await MMRHelpers.portfolio_diff()
        # diff["data"]["changed"] → [{"symbol": "XRO", "value_change": -105.23, ...}]
        # diff["data"]["new"] → []
        # diff["data"]["removed"] → []
        # diff["data"]["unchanged_count"] → 15
        """
        return await _run_cli_json("portfolio-diff")

    @staticmethod
    async def session_status() -> dict:
        """
        Full session status: position sizing config (including volatility settings),
        portfolio state, remaining capacity, and recommended position sizes at
        different confidence levels. Returns JSON.

        Example:
        status = await MMRHelpers.session_status()
        # status["data"]["config"]["volatility_adjustment"] → True
        # status["data"]["recommended_sizes"]["high_confidence"]["reasoning"]
        """
        return await _run_cli_json("session")

    @staticmethod
    async def portfolio_risk() -> dict:
        """
        Portfolio risk analysis: concentration (HHI), position weights, group
        allocation vs budget, correlation clusters, warnings, and a plain-English
        summary. Returns JSON. Requires trader_service for portfolio data.

        Example:
        report = await MMRHelpers.portfolio_risk()
        # report["data"]["hhi"] → 0.064
        # report["data"]["warnings"] → [{level, message, symbols}]
        # report["data"]["group_allocations"] → [{name, pct, budget_pct, over_budget}]
        # report["data"]["summary"] → "Portfolio has 17 positions..."
        """
        return await _run_cli_json("portfolio-risk")

    # ------------------------------------------------------------------
    # Position Groups
    # ------------------------------------------------------------------

    @staticmethod
    async def group_list() -> dict:
        """
        List all position groups with members and budget allocations. Returns JSON.
        Does NOT require trader_service.

        Example:
        groups = await MMRHelpers.group_list()
        # groups["data"]["groups"] → [{name, members, max_allocation_pct, ...}]
        """
        return await _run_cli_json("group", "list")

    @staticmethod
    async def group_create(name: str, budget: float = 0.0, description: str = "") -> str:
        """
        Create a position group with optional allocation budget.
        Does NOT require trader_service.

        :param name: Group name (e.g. "mining", "tech", "defensive")
        :param budget: Max allocation as percentage (e.g. 20 = 20% of portfolio)
        :param description: Group description

        Example:
        result = await MMRHelpers.group_create("mining", budget=20, description="Mining stocks")
        """
        args = ["group", "create", name]
        if budget:
            args.extend(["--budget", str(budget)])
        if description:
            args.extend(["--description", description])
        return await _run_cli(*args)

    @staticmethod
    async def group_delete(name: str) -> str:
        """
        Delete a position group and its members.
        Does NOT require trader_service.

        Example:
        result = await MMRHelpers.group_delete("mining")
        """
        return await _run_cli("group", "delete", name)

    @staticmethod
    async def group_show(name: str) -> dict:
        """
        Show group details including members and budget. Returns JSON.
        Does NOT require trader_service.

        Example:
        group = await MMRHelpers.group_show("mining")
        # group["data"]["members"] → ["BHP", "RIO", "FMG"]
        """
        return await _run_cli_json("group", "show", name)

    @staticmethod
    async def group_add(name: str, symbols: List[str]) -> str:
        """
        Add symbols to a position group.
        Does NOT require trader_service.

        :param name: Group name
        :param symbols: List of ticker symbols to add

        Example:
        result = await MMRHelpers.group_add("mining", ["BHP", "RIO", "FMG"])
        """
        return await _run_cli("group", "add", name, *symbols)

    @staticmethod
    async def group_remove(name: str, symbol: str) -> str:
        """
        Remove a symbol from a position group.
        Does NOT require trader_service.

        Example:
        result = await MMRHelpers.group_remove("mining", "BHP")
        """
        return await _run_cli("group", "remove", name, symbol)

    @staticmethod
    async def group_set(name: str, budget: Optional[float] = None, description: Optional[str] = None) -> str:
        """
        Update group settings (budget, description).
        Does NOT require trader_service.

        :param name: Group name
        :param budget: New max allocation percentage (e.g. 25 = 25%)
        :param description: New description

        Example:
        result = await MMRHelpers.group_set("mining", budget=25)
        """
        args = ["group", "set", name]
        if budget is not None:
            args.extend(["--budget", str(budget)])
        if description is not None:
            args.extend(["--description", description])
        return await _run_cli(*args)

    # ------------------------------------------------------------------
    # Direct CLI (escape hatch)
    # ------------------------------------------------------------------

    @staticmethod
    async def cli(command: str) -> str:
        """
        Run any mmr CLI command directly. Use this as an escape hatch
        when no specific helper exists. The command string is split by spaces.

        :param command: Full CLI command (e.g. "universe show portfolio")

        Example:
        result = await MMRHelpers.cli("universe list")
        result = await MMRHelpers.cli("resolve AAPL")
        """
        args = command.split()
        return await _run_cli(*args)
