import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


# Resolve paths once at import time
_MMR_ROOT = str(Path(__file__).resolve().parent.parent.parent)
_VENV_PYTHON = os.path.join(_MMR_ROOT, ".venv", "bin", "python")

# Fall back to sys.executable if venv doesn't exist
if not os.path.exists(_VENV_PYTHON):
    _VENV_PYTHON = sys.executable


def _run_cli(*args: str, timeout: int = 30) -> str:
    """Run an mmr CLI command and return combined stdout+stderr as a string."""
    cmd = [_VENV_PYTHON, "-m", "trader.mmr_cli"] + list(args)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=_MMR_ROOT,
        timeout=timeout,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    output = (result.stdout + result.stderr).strip()
    # Strip ANSI escape sequences for cleaner output
    import re
    output = re.sub(r'\x1b\[[0-9;]*m', '', output)
    return output


def _run_cli_json(*args: str, timeout: int = 30) -> dict:
    """Run mmr CLI with --json and return parsed JSON."""
    raw = _run_cli("--json", *args, timeout=timeout)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"data": raw, "title": None, "error": "Failed to parse JSON"}


def _run_sdk_script(script: str, timeout: int = 30) -> str:
    """Run a Python script in the mmr venv and return output."""
    result = subprocess.run(
        [_VENV_PYTHON, "-c", script],
        capture_output=True,
        text=True,
        cwd=_MMR_ROOT,
        timeout=timeout,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    output = (result.stdout + result.stderr).strip()
    import re
    output = re.sub(r'\x1b\[[0-9;]*m', '', output)
    return output


class MMRHelpers:
    """MMR trading platform helpers. All methods are async and return strings."""

    # ------------------------------------------------------------------
    # Portfolio & Account
    # ------------------------------------------------------------------

    @staticmethod
    async def portfolio() -> str:
        """
        Get current portfolio with P&L for all positions.
        Returns a table with: account, conId, symbol, position, mktPrice,
        avgCost, marketValue, unrealizedPNL, realizedPNL, dailyPNL.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.portfolio()
        """
        return await asyncio.to_thread(_run_cli, "portfolio")

    @staticmethod
    async def positions() -> str:
        """
        Get raw positions (no P&L).
        Returns a table with: account, conId, symbol, secType, position, avgCost, currency, total.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.positions()
        """
        return await asyncio.to_thread(_run_cli, "positions")

    @staticmethod
    async def orders() -> str:
        """
        Get all open orders.
        Returns a table with: orderId, action, orderType, lmtPrice, totalQuantity.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.orders()
        """
        return await asyncio.to_thread(_run_cli, "orders")

    @staticmethod
    async def trades() -> str:
        """
        Get active trades.
        Returns a table with: conId, symbol, orderId, action, status, filled, orderType, lmtPrice, totalQuantity.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.trades()
        """
        return await asyncio.to_thread(_run_cli, "trades")

    @staticmethod
    async def account() -> str:
        """
        Get the IB account ID.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.account()
        """
        return await asyncio.to_thread(_run_cli, "account")

    @staticmethod
    async def status() -> str:
        """
        Check service health / connectivity to trader_service.

        Example:
        result = await MMRHelpers.status()
        """
        return await asyncio.to_thread(_run_cli, "status")

    # ------------------------------------------------------------------
    # Symbol Resolution & Market Data
    # ------------------------------------------------------------------

    @staticmethod
    async def resolve(symbol: str) -> str:
        """
        Resolve a symbol to IB contract details (conId, exchange, secType, etc).
        Requires trader_service to be running.

        :param symbol: Stock ticker or conId (e.g. "AMD", "265598")
        :return: Table with conId, symbol, secType, exchange, primaryExchange, currency, longName

        Example:
        result = await MMRHelpers.resolve("AMD")
        """
        return await asyncio.to_thread(_run_cli, "resolve", symbol)

    @staticmethod
    async def snapshot(symbol: str, delayed: bool = False) -> str:
        """
        Get a price snapshot for a symbol (bid, ask, last, OHLC, volume).
        Requires trader_service to be running.

        :param symbol: Stock ticker (e.g. "AMD")
        :param delayed: Use delayed market data (default False)

        Example:
        result = await MMRHelpers.snapshot("AAPL")
        """
        args = ["snapshot", symbol]
        if delayed:
            args.append("--delayed")
        return await asyncio.to_thread(_run_cli, *args)

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

        Example:
        result = await MMRHelpers.buy("AMD", market=True, quantity=10)
        result = await MMRHelpers.buy("AMD", market=True, amount=500.0)
        result = await MMRHelpers.buy("AMD", limit_price=150.00, quantity=10)
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
        return await asyncio.to_thread(_run_cli, *args, timeout=30)

    @staticmethod
    async def sell(
        symbol: str,
        quantity: Optional[float] = None,
        amount: Optional[float] = None,
        limit_price: Optional[float] = None,
        market: bool = False,
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

        Example:
        result = await MMRHelpers.sell("AMD", market=True, quantity=10)
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
        return await asyncio.to_thread(_run_cli, *args, timeout=30)

    @staticmethod
    async def cancel(order_id: int) -> str:
        """
        Cancel a single order by its order ID.
        Requires trader_service to be running.

        :param order_id: The order ID to cancel

        Example:
        result = await MMRHelpers.cancel(12345)
        """
        return await asyncio.to_thread(_run_cli, "cancel", str(order_id))

    @staticmethod
    async def cancel_all() -> str:
        """
        Cancel all open orders.
        Requires trader_service to be running.

        Example:
        result = await MMRHelpers.cancel_all()
        """
        return await asyncio.to_thread(_run_cli, "cancel-all")

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
        return await asyncio.to_thread(_run_cli, "strategies")

    @staticmethod
    async def enable_strategy(name: str) -> str:
        """
        Enable a strategy by name.
        Requires trader_service to be running.

        :param name: Strategy name

        Example:
        result = await MMRHelpers.enable_strategy("smi_crossover")
        """
        return await asyncio.to_thread(_run_cli, "strategies", "enable", name)

    @staticmethod
    async def disable_strategy(name: str) -> str:
        """
        Disable a strategy by name.
        Requires trader_service to be running.

        :param name: Strategy name

        Example:
        result = await MMRHelpers.disable_strategy("smi_crossover")
        """
        return await asyncio.to_thread(_run_cli, "strategies", "disable", name)

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
        return await asyncio.to_thread(_run_cli, "universe", "list")

    @staticmethod
    async def universe_show(name: str) -> str:
        """
        Show all symbols in a universe with conId, symbol, secType, exchange, etc.
        Does NOT require trader_service.

        :param name: Universe name (e.g. "nasdaq_top25", "portfolio")

        Example:
        result = await MMRHelpers.universe_show("portfolio")
        """
        return await asyncio.to_thread(_run_cli, "universe", "show", name)

    @staticmethod
    async def universe_create(name: str) -> str:
        """
        Create a new empty universe.
        Does NOT require trader_service.

        :param name: Name for the new universe

        Example:
        result = await MMRHelpers.universe_create("my_watchlist")
        """
        return await asyncio.to_thread(_run_cli, "universe", "create", name)

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
        cmd = [_VENV_PYTHON, "-m", "trader.mmr_cli", "universe", "delete", name]
        result = subprocess.run(
            cmd,
            input="y\n",
            capture_output=True,
            text=True,
            cwd=_MMR_ROOT,
            timeout=15,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        output = (result.stdout + result.stderr).strip()
        import re
        return re.sub(r'\x1b\[[0-9;]*m', '', output)

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
        return await asyncio.to_thread(_run_cli, *args, timeout=120)

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
        return await asyncio.to_thread(_run_cli, "universe", "remove", name, symbol)

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
        return await asyncio.to_thread(_run_cli, "universe", "import", name, csv_file)

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
        return await asyncio.to_thread(_run_cli, *args)

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
        return await asyncio.to_thread(_run_cli, *args)

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
        return await asyncio.to_thread(_run_cli, *args)

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
        return await asyncio.to_thread(_run_cli, *args, timeout=60)

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
        return await asyncio.to_thread(_run_cli, "financials", "ratios", symbol)

    # ------------------------------------------------------------------
    # Historical Data
    # ------------------------------------------------------------------

    @staticmethod
    async def history_massive(
        symbol: Optional[str] = None,
        universe: Optional[str] = None,
        bar_size: str = "1 day",
        prev_days: int = 30,
    ) -> str:
        """
        Download historical data from Massive.com.
        Must specify either symbol or universe.
        Does NOT require trader_service.

        :param symbol: Single symbol (e.g. "AAPL")
        :param universe: Universe name (e.g. "portfolio")
        :param bar_size: Bar size (default "1 day")
        :param prev_days: Days of history to download (default 30)

        Example:
        result = await MMRHelpers.history_massive(symbol="AAPL", bar_size="1 day", prev_days=30)
        result = await MMRHelpers.history_massive(universe="portfolio", prev_days=60)
        """
        args = ["history", "massive", "--bar_size", bar_size, "--prev_days", str(prev_days)]
        if symbol:
            args.extend(["--symbol", symbol])
        if universe:
            args.extend(["--universe", universe])
        return await asyncio.to_thread(_run_cli, *args, timeout=300)

    @staticmethod
    async def history_ib(
        symbol: Optional[str] = None,
        universe: Optional[str] = None,
        bar_size: str = "1 min",
        prev_days: int = 5,
    ) -> str:
        """
        Download historical data from Interactive Brokers.
        Must specify either symbol or universe.
        Does NOT require trader_service but does need IB Gateway running.

        :param symbol: Single symbol (e.g. "AAPL")
        :param universe: Universe name (e.g. "portfolio")
        :param bar_size: Bar size (default "1 min")
        :param prev_days: Days of history to download (default 5)

        Example:
        result = await MMRHelpers.history_ib(symbol="AAPL", bar_size="1 min", prev_days=5)
        """
        args = ["history", "ib", "--bar_size", bar_size, "--prev_days", str(prev_days)]
        if symbol:
            args.extend(["--symbol", symbol])
        if universe:
            args.extend(["--universe", universe])
        return await asyncio.to_thread(_run_cli, *args, timeout=300)

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
        return await asyncio.to_thread(_run_cli, "options", "expirations", symbol)

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
        return await asyncio.to_thread(_run_cli, *args)

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
        return await asyncio.to_thread(_run_cli, "options", "snapshot", option_ticker)

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
        return await asyncio.to_thread(_run_cli, *args)

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
        return await asyncio.to_thread(_run_cli, *args, timeout=30)

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
        return await asyncio.to_thread(_run_cli, *args, timeout=30)

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
        result = await asyncio.to_thread(_run_cli_json, "data", "summary")
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
        result = await asyncio.to_thread(_run_cli_json, *args)
        return json.dumps(result, indent=2)

    @staticmethod
    async def data_download(
        symbols: List[str],
        bar_size: str = "1 day",
        days: int = 365,
    ) -> str:
        """
        Download data from Massive.com to local DuckDB.
        Does NOT require trader_service. Requires massive_api_key in config.

        :param symbols: List of tickers to download (e.g. ["AAPL", "MSFT"])
        :param bar_size: Bar size (default "1 day")
        :param days: Days of history to download (default 365)

        Example:
        result = await MMRHelpers.data_download(["AAPL", "MSFT"], bar_size="1 day", days=365)
        """
        args = ["data", "download"] + symbols + ["--bar-size", bar_size, "--days", str(days)]
        result = await asyncio.to_thread(_run_cli_json, *args, timeout=300)
        return json.dumps(result, indent=2)

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
    ) -> str:
        """
        Backtest a strategy against historical data.
        Does NOT require any service. Uses local DuckDB data.

        :param strategy_path: Path to strategy .py file (e.g. "strategies/my_strategy.py")
        :param class_name: Strategy class name (e.g. "MyStrategy")
        :param conids: List of contract IDs to backtest
        :param universe: Universe name (alternative to conids)
        :param days: Days of history (default 365)
        :param capital: Initial capital (default 100000)
        :param bar_size: Bar size (default "1 min")

        Example:
        result = await MMRHelpers.backtest("strategies/my_strategy.py", "MyStrategy", conids=[265598])
        """
        args = ["backtest", "-s", strategy_path, "--class", class_name,
                "--days", str(days), "--capital", str(capital), "--bar-size", bar_size]
        if conids:
            args.extend(["--conids"] + [str(c) for c in conids])
        if universe:
            args.extend(["--universe", universe])
        result = await asyncio.to_thread(_run_cli_json, *args, timeout=300)
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
        result = await asyncio.to_thread(_run_cli_json, *args)
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
        result = await asyncio.to_thread(_run_cli_json, *args)
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
        result = await asyncio.to_thread(_run_cli_json, "strategies", "undeploy", name)
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
        result = await asyncio.to_thread(_run_cli_json, "strategies", "signals", name,
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
        result = await asyncio.to_thread(_run_cli_json, *args, timeout=300)
        return json.dumps(result, indent=2)

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
        return await asyncio.to_thread(_run_cli, *args)
