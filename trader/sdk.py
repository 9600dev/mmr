"""MMR SDK — programmatic access to the trader_service.

Usage::

    from trader.sdk import MMR

    with MMR() as mmr:
        print(mmr.portfolio())
        mmr.buy("AMD", market=True, quantity=10)
"""

from expression import pipe
from expression.collections import seq
from ib_async.contract import Contract
from ib_async.objects import PortfolioItem, Position
from ib_async.order import Order, Trade
from ib_async.ticker import Ticker
from trader.common.reactivex import SuccessFail
from trader.data.data_access import PortfolioSummary, SecurityDefinition
from trader.data.universe import Universe
from trader.messaging.clientserver import consume, RPCClient, TopicPubSub, pack, unpack
from trader.messaging.data_service_api import DataServiceApi
from trader.messaging.trader_service_api import TraderServiceApi
from trader.trading.strategy import StrategyConfig, StrategyState
from typing import Any, Callable, Dict, List, Optional, Union

import asyncio
import dataclasses
import pandas as pd
import threading
import zmq


class Subscription:
    """Handle returned by :meth:`MMR.subscribe_ticks`.  Call :meth:`stop` to unsubscribe."""

    def __init__(self):
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def is_active(self) -> bool:
        return self._thread is not None and self._thread.is_alive()


class MMR:
    """Synchronous Python SDK for the MMR trader_service.

    Parameters
    ----------
    config_file : str
        Path to trader.yaml.  Defaults to ``~/.config/mmr/trader.yaml``.
    rpc_address : str, optional
        Override ZMQ RPC server address (e.g. ``tcp://127.0.0.1``).
    rpc_port : int, optional
        Override ZMQ RPC server port (e.g. ``42001``).
    pubsub_address : str, optional
        Override ZMQ PubSub server address.
    pubsub_port : int, optional
        Override ZMQ PubSub server port.
    timeout : int
        RPC timeout in seconds.
    """

    def __init__(
        self,
        config_file: str = '',
        rpc_address: Optional[str] = None,
        rpc_port: Optional[int] = None,
        pubsub_address: Optional[str] = None,
        pubsub_port: Optional[int] = None,
        timeout: int = 10,
    ):
        from trader.container import Container

        self._container = Container(config_file) if config_file else Container.instance()
        cfg = self._container.config()

        self._rpc_address = rpc_address or cfg['zmq_rpc_server_address']
        self._rpc_port = rpc_port or cfg['zmq_rpc_server_port']
        self._pubsub_address = pubsub_address or cfg['zmq_pubsub_server_address']
        self._pubsub_port = pubsub_port or cfg['zmq_pubsub_server_port']
        self._data_rpc_address = cfg.get('zmq_data_rpc_server_address', 'tcp://127.0.0.1')
        self._data_rpc_port = cfg.get('zmq_data_rpc_server_port', 42003)
        self._timeout = timeout

        self._client: Optional[RPCClient[TraderServiceApi]] = None
        self._data_client: Optional[RPCClient[DataServiceApi]] = None
        self._massive_rest_client = None
        self._subscriptions: List[Subscription] = []

        # position_map: row_number -> symbol string (set by portfolio/positions)
        self._position_map: Dict[int, str] = {}

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> 'MMR':
        """Connect to trader_service.  Returns *self* for chaining."""
        self._client = RPCClient[TraderServiceApi](
            zmq_server_address=self._rpc_address,
            zmq_server_port=self._rpc_port,
            timeout=self._timeout,
        )
        asyncio.run(self._client.connect())
        return self

    def _connect_data_service(self) -> None:
        """Connect to data_service RPC. Called lazily on first data request."""
        if self._data_client is None:
            self._data_client = RPCClient[DataServiceApi](
                zmq_server_address=self._data_rpc_address,
                zmq_server_port=self._data_rpc_port,
                timeout=120,
            )
            asyncio.run(self._data_client.connect())

    @property
    def _data_rpc(self) -> RPCClient[DataServiceApi]:
        self._connect_data_service()
        if self._data_client is None or not self._data_client.is_setup:
            raise ConnectionError("Not connected to data_service.")
        return self._data_client

    def close(self) -> None:
        """Disconnect and clean up resources."""
        for sub in self._subscriptions:
            sub.stop()
        self._subscriptions.clear()
        if self._client:
            self._client.close()
            self._client = None
        if self._data_client:
            self._data_client.close()
            self._data_client = None

    def __enter__(self) -> 'MMR':
        return self.connect()

    def __exit__(self, *exc) -> None:
        self.close()

    @property
    def _rpc(self) -> RPCClient[TraderServiceApi]:
        if self._client is None or not self._client.is_setup:
            raise ConnectionError("Not connected. Call .connect() first.")
        return self._client

    # ------------------------------------------------------------------
    # Symbol resolution (internal helper)
    # ------------------------------------------------------------------

    def resolve(
        self,
        symbol: Union[str, int],
        sec_type: str = 'STK',
        exchange: str = '',
        universe: str = '',
    ) -> List[SecurityDefinition]:
        """Resolve a symbol string or conId to SecurityDefinition(s) via trader_service."""
        return consume(
            self._rpc.rpc(return_type=list[SecurityDefinition]).resolve_symbol(
                symbol, exchange, universe, sec_type
            )
        )

    def _resolve_contract(self, symbol: Union[str, int], sec_type: str = 'STK') -> Contract:
        """Resolve *symbol* to a single Contract, raising on ambiguity."""
        definitions = self.resolve(symbol, sec_type=sec_type)
        if not definitions:
            raise ValueError(f"Could not resolve symbol: {symbol}")
        sec = definitions[0]
        # Build Contract from attributes directly (avoid isinstance checks
        # that can fail on deserialized objects).
        return Contract(
            conId=sec.conId,
            symbol=sec.symbol,
            secType=sec.secType,
            exchange=sec.exchange,
            primaryExchange=getattr(sec, 'primaryExchange', ''),
            currency=sec.currency,
        )

    # ------------------------------------------------------------------
    # Portfolio & Positions
    # ------------------------------------------------------------------

    def portfolio(self) -> pd.DataFrame:
        """Portfolio with P&L (matches the old ``portfolio`` CLI command)."""
        summaries: list[PortfolioSummary] = self._rpc.rpc(
            return_type=list[PortfolioSummary]
        ).get_portfolio_summary()

        rows = []
        for p in summaries:
            rows.append({
                'account': p.account,
                'conId': p.contract.conId,
                'symbol': p.contract.localSymbol or p.contract.symbol,
                'position': p.position,
                'mktPrice': p.marketPrice,
                'avgCost': p.averageCost,
                'marketValue': p.marketValue,
                'unrealizedPNL': p.unrealizedPNL,
                'realizedPNL': p.realizedPNL,
                'dailyPNL': p.dailyPNL,
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(by='dailyPNL', ascending=False).reset_index(drop=True)
            # Update position map for close-by-number
            self._position_map = {
                i + 1: row['symbol'] for i, row in df.iterrows()
            }
        return df

    def positions(self) -> pd.DataFrame:
        """Raw positions (no P&L)."""
        pos_list: list[Position] = self._rpc.rpc(
            return_type=list[Position]
        ).get_positions()

        rows = []
        for p in pos_list:
            rows.append({
                'account': p.account,
                'conId': p.contract.conId,
                'symbol': p.contract.localSymbol or p.contract.symbol,
                'secType': p.contract.secType,
                'position': p.position,
                'avgCost': p.avgCost,
                'currency': p.contract.currency,
                'total': p.position * p.avgCost,
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(by='currency').reset_index(drop=True)
            self._position_map = {
                i + 1: row['symbol'] for i, row in df.iterrows()
            }
        return df

    # ------------------------------------------------------------------
    # Order Book
    # ------------------------------------------------------------------

    def orders(self) -> pd.DataFrame:
        """Open orders."""
        orders_raw: dict[int, list[Order]] = self._rpc.rpc(
            return_type=dict[int, list[Order]]
        ).get_orders()

        rows = []
        for order_id, order_list in orders_raw.items():
            o = order_list[0]
            rows.append({
                'orderId': o.orderId,
                'action': o.action,
                'orderType': o.orderType,
                'lmtPrice': o.lmtPrice,
                'totalQuantity': o.totalQuantity,
            })
        return pd.DataFrame(rows)

    def trades(self) -> pd.DataFrame:
        """Active trades in the book."""
        trades_raw: dict[int, list[Trade]] = self._rpc.rpc(
            return_type=dict[int, list[Trade]]
        ).get_trades()

        rows = []
        for trade_id, trade_list in trades_raw.items():
            t = trade_list[0]
            rows.append({
                'conId': t.contract.conId,
                'symbol': t.contract.symbol,
                'orderId': t.order.orderId,
                'action': t.order.action,
                'status': t.orderStatus.status,
                'filled': t.orderStatus.filled,
                'orderType': t.order.orderType,
                'lmtPrice': t.order.lmtPrice,
                'totalQuantity': t.order.totalQuantity,
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(by='orderId', ascending=True)
        return df

    # ------------------------------------------------------------------
    # Trading
    # ------------------------------------------------------------------

    def _place_order(
        self,
        symbol: Union[str, int],
        action: str,
        amount: Optional[float] = None,
        quantity: Optional[float] = None,
        limit_price: Optional[float] = None,
        market: bool = False,
        stop_loss_percentage: float = 0.0,
        debug: bool = False,
        sec_type: str = 'STK',
    ) -> SuccessFail:
        if not market and limit_price is None:
            raise ValueError("Specify market=True or provide a limit_price")
        if amount is None and quantity is None:
            raise ValueError("Specify amount (dollar value) or quantity")

        contract = self._resolve_contract(symbol, sec_type=sec_type)

        return consume(
            self._rpc.rpc(return_type=SuccessFail[Trade]).place_order_simple(
                contract=contract,
                action=action,
                equity_amount=amount,
                quantity=quantity,
                limit_price=limit_price,
                market_order=market,
                stop_loss_percentage=stop_loss_percentage,
                debug=debug,
            )
        )

    def buy(
        self,
        symbol: Union[str, int],
        amount: Optional[float] = None,
        quantity: Optional[float] = None,
        limit_price: Optional[float] = None,
        market: bool = False,
        stop_loss_percentage: float = 0.0,
        debug: bool = False,
        sec_type: str = 'STK',
    ) -> SuccessFail:
        """Place a buy order."""
        return self._place_order(
            symbol, 'BUY', amount, quantity, limit_price, market,
            stop_loss_percentage, debug, sec_type=sec_type,
        )

    def sell(
        self,
        symbol: Union[str, int],
        amount: Optional[float] = None,
        quantity: Optional[float] = None,
        limit_price: Optional[float] = None,
        market: bool = False,
        stop_loss_percentage: float = 0.0,
        debug: bool = False,
        sec_type: str = 'STK',
    ) -> SuccessFail:
        """Place a sell order."""
        return self._place_order(
            symbol, 'SELL', amount, quantity, limit_price, market,
            stop_loss_percentage, debug, sec_type=sec_type,
        )

    def cancel(self, order_id: int) -> SuccessFail:
        """Cancel a single order by ID."""
        return self._rpc.rpc(return_type=SuccessFail[Trade]).cancel_order(order_id)

    def cancel_all(self) -> SuccessFail:
        """Cancel all open orders."""
        return self._rpc.rpc(return_type=SuccessFail[list[int]]).cancel_all()

    # ------------------------------------------------------------------
    # Position closing
    # ------------------------------------------------------------------

    def close_position(self, symbol: str, quantity: Optional[float] = None) -> SuccessFail:
        """Close (or reduce) a position.

        If *quantity* is None, sells the entire position at market.
        """
        portfolio_df = self.portfolio()
        if portfolio_df.empty:
            return SuccessFail.fail(error=f"No positions found")

        match = portfolio_df[portfolio_df['symbol'] == symbol]
        if match.empty:
            return SuccessFail.fail(error=f"No position for {symbol}")

        pos_size = float(match.iloc[0]['position'])
        if pos_size == 0:
            return SuccessFail.fail(error=f"Position for {symbol} is zero")

        close_qty = abs(quantity) if quantity is not None else abs(pos_size)
        action = 'SELL' if pos_size > 0 else 'BUY'

        contract = self._resolve_contract(symbol)
        return consume(
            self._rpc.rpc(return_type=SuccessFail[Trade]).place_order_simple(
                contract=contract,
                action=action,
                equity_amount=None,
                quantity=close_qty,
                limit_price=None,
                market_order=True,
                stop_loss_percentage=0.0,
                debug=False,
            )
        )

    # ------------------------------------------------------------------
    # Market Data
    # ------------------------------------------------------------------

    def snapshot(self, symbol: Union[str, int], delayed: bool = False) -> dict:
        """Get a price snapshot for *symbol*."""
        contract = self._resolve_contract(symbol)
        ticker: Ticker = consume(
            self._rpc.rpc(return_type=Ticker).get_snapshot(contract, delayed)
        )
        return {
            'symbol': ticker.contract.symbol if ticker.contract else '',
            'conId': ticker.contract.conId if ticker.contract else '',
            'time': ticker.time,
            'bid': ticker.bid,
            'bidSize': ticker.bidSize,
            'ask': ticker.ask,
            'askSize': ticker.askSize,
            'last': ticker.last,
            'lastSize': ticker.lastSize,
            'open': ticker.open,
            'high': ticker.high,
            'low': ticker.low,
            'close': ticker.close,
            'halted': ticker.halted,
        }

    def subscribe_ticks(
        self,
        symbol: Union[str, int],
        callback: Callable[[Any], None],
        topic: str = 'ticker',
        delayed: bool = False,
    ) -> Subscription:
        """Subscribe to live tick data for *symbol*.

        *callback* fires on a background thread each time a tick arrives.
        Call ``subscription.stop()`` to unsubscribe.
        """
        contract = self._resolve_contract(symbol)
        # Tell trader_service to publish ticks for this contract
        self._rpc.rpc().publish_contract(contract, delayed)

        sub = Subscription()

        def _listener():
            ctx = zmq.Context()
            socket = ctx.socket(zmq.SUB)
            socket.connect(f'{self._pubsub_address}:{self._pubsub_port}')
            socket.setsockopt_string(zmq.SUBSCRIBE, topic)
            socket.setsockopt(zmq.RCVTIMEO, 1000)  # 1s poll

            try:
                while not sub._stop_event.is_set():
                    try:
                        frames = socket.recv_multipart()
                        if len(frames) >= 2:
                            obj = unpack(frames[1])
                            callback(obj)
                    except zmq.Again:
                        continue
                    except zmq.ZMQError:
                        break
            finally:
                socket.close()
                ctx.term()

        sub._thread = threading.Thread(target=_listener, daemon=True)
        sub._thread.start()
        self._subscriptions.append(sub)
        return sub

    # ------------------------------------------------------------------
    # Strategies
    # ------------------------------------------------------------------

    def strategies(self) -> pd.DataFrame:
        """List configured strategies."""
        result: SuccessFail = consume(
            self._rpc.rpc(return_type=SuccessFail[list[StrategyConfig]]).get_strategies()
        )
        if result.is_success() and result.obj:
            rows = []
            for s in result.obj:
                rows.append({
                    'name': s.name,
                    'state': str(s.state),
                    'paper': s.paper,
                    'bar_size': str(s.bar_size),
                    'conids': s.conids or [],
                    'hist_days_prior': s.historical_days_prior,
                })
            return pd.DataFrame(rows)
        return pd.DataFrame()

    def enable_strategy(self, name: str, paper: bool = True) -> SuccessFail:
        """Enable a strategy by name."""
        return consume(self._rpc.rpc().enable_strategy(name, paper))

    def disable_strategy(self, name: str) -> SuccessFail:
        """Disable a strategy by name."""
        return consume(self._rpc.rpc().disable_strategy(name))

    # ------------------------------------------------------------------
    # Historical Data (via data_service RPC)
    # ------------------------------------------------------------------

    def pull_massive(
        self,
        symbols: Optional[List[str]] = None,
        universe: Optional[str] = None,
        bar_size: str = '1 day',
        prev_days: int = 30,
    ) -> dict:
        """Download historical data from Massive.com via the data_service."""
        return consume(
            self._data_rpc.rpc(return_type=dict).pull_massive(
                symbols=symbols, universe=universe, bar_size=bar_size, prev_days=prev_days,
            )
        )

    def pull_ib(
        self,
        symbols: Optional[List[str]] = None,
        universe: Optional[str] = None,
        bar_size: str = '1 min',
        prev_days: int = 5,
        ib_client_id: int = 10,
    ) -> dict:
        """Download historical data from IB via the data_service."""
        return consume(
            self._data_rpc.rpc(return_type=dict).pull_ib(
                symbols=symbols, universe=universe, bar_size=bar_size,
                prev_days=prev_days, ib_client_id=ib_client_id,
            )
        )

    def data_service_status(self) -> dict:
        """Check data_service status (running jobs, etc.)."""
        return consume(self._data_rpc.rpc(return_type=dict).status())

    def history_list(self, symbol: Optional[str] = None, bar_size: Optional[str] = None) -> pd.DataFrame:
        """List downloaded history in DuckDB, grouped by symbol and bar_size.

        Returns a DataFrame with columns: symbol, name, bar_size, start, end, rows.
        Resolves conIds to ticker names via local universes (no service needed).
        """
        from trader.data.duckdb_store import DuckDBDataStore
        from trader.data.universe import UniverseAccessor

        cfg = self._container.config()
        db_path = cfg.get('duckdb_path', '')
        store = DuckDBDataStore(db_path)

        # Build the query with optional filters
        conditions = []
        params: list = []
        if symbol:
            # Could be a ticker name or conId — we'll filter after resolving
            pass
        if bar_size:
            conditions.append("bar_size = ?")
            params.append(bar_size)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        def _query(conn):
            result = conn.execute(f"""
                SELECT symbol, bar_size, MIN(date) as start, MAX(date) as end, COUNT(*) as rows
                FROM {store.TABLE_NAME}
                {where}
                GROUP BY symbol, bar_size
                ORDER BY symbol, bar_size
            """, params or None)
            return result.fetchdf()

        df = store._db.execute_atomic(_query)
        if df.empty:
            return pd.DataFrame(columns=['symbol', 'name', 'bar_size', 'start', 'end', 'rows'])

        # Build a conId → ticker name mapping from universes
        universe_lib = cfg.get('universe_library', 'Universes')
        accessor = UniverseAccessor(db_path, universe_lib)
        conid_map: Dict[str, str] = {}
        try:
            for u in accessor.get_all():
                for d in u.security_definitions:
                    conid_map[str(d.conId)] = d.symbol
        except Exception:
            pass

        df['name'] = df['symbol'].map(lambda s: conid_map.get(s, ''))

        # If filtering by ticker name, apply now
        if symbol:
            # Try matching by conId string or by resolved name
            mask = (df['symbol'] == symbol) | (df['name'].str.upper() == symbol.upper())
            df = df[mask]

        # Reorder columns
        df = df[['symbol', 'name', 'bar_size', 'start', 'end', 'rows']]
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Account / Status
    # ------------------------------------------------------------------

    def account(self) -> str:
        """Return the IB account ID."""
        return consume(self._rpc.rpc(return_type=str).get_ib_account())

    def status(self) -> dict:
        """Check connectivity to trader_service."""
        from trader.tools.trader_check import health_check
        return {'status': health_check(self._container.config_file)}

    # ------------------------------------------------------------------
    # Financial Statements (via Massive.com REST API)
    # ------------------------------------------------------------------

    @property
    def _massive_client(self):
        """Lazy-init Massive.com REST client."""
        if self._massive_rest_client is None:
            from massive import RESTClient
            cfg = self._container.config()
            api_key = cfg.get('massive_api_key', '')
            if not api_key:
                raise ValueError("massive_api_key not configured in trader.yaml")
            self._massive_rest_client = RESTClient(api_key=api_key)
        return self._massive_rest_client

    def _financials_to_df(self, results, limit: int = 0) -> pd.DataFrame:
        """Convert an iterator of financial dataclass objects to a DataFrame."""
        rows = []
        for item in results:
            row = dataclasses.asdict(item)
            rows.append(row)
            if limit and len(rows) >= limit:
                break
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        # Drop columns that are all None
        df = df.dropna(axis=1, how='all')
        # Sort by period descending if available
        if 'period_end' in df.columns:
            df = df.sort_values('period_end', ascending=False).reset_index(drop=True)
        return df

    def balance_sheet(self, symbol: str, limit: int = 4, timeframe: str = 'quarterly') -> pd.DataFrame:
        """Get balance sheet data from Massive.com.

        Parameters
        ----------
        symbol : str
            Stock ticker (e.g. "AAPL").
        limit : int
            Number of periods to return (default 4).
        timeframe : str
            "quarterly" or "annual" (default "quarterly").
        """
        results = self._massive_client.list_financials_balance_sheets(
            tickers=symbol, timeframe=timeframe, limit=limit,
        )
        return self._financials_to_df(results, limit=limit)

    def income_statement(self, symbol: str, limit: int = 4, timeframe: str = 'quarterly') -> pd.DataFrame:
        """Get income statement data from Massive.com.

        Parameters
        ----------
        symbol : str
            Stock ticker (e.g. "AAPL").
        limit : int
            Number of periods to return (default 4).
        timeframe : str
            "quarterly" or "annual" (default "quarterly").
        """
        results = self._massive_client.list_financials_income_statements(
            tickers=symbol, timeframe=timeframe, limit=limit,
        )
        return self._financials_to_df(results, limit=limit)

    def cash_flow(self, symbol: str, limit: int = 4, timeframe: str = 'quarterly') -> pd.DataFrame:
        """Get cash flow statement data from Massive.com.

        Parameters
        ----------
        symbol : str
            Stock ticker (e.g. "AAPL").
        limit : int
            Number of periods to return (default 4).
        timeframe : str
            "quarterly" or "annual" (default "quarterly").
        """
        results = self._massive_client.list_financials_cash_flow_statements(
            tickers=symbol, timeframe=timeframe, limit=limit,
        )
        return self._financials_to_df(results, limit=limit)

    def ratios(self, symbol: str) -> pd.DataFrame:
        """Get financial ratios (TTM) from Massive.com.

        Parameters
        ----------
        symbol : str
            Stock ticker (e.g. "AAPL").
        """
        results = self._massive_client.list_financials_ratios(
            ticker=symbol, limit=1,
        )
        return self._financials_to_df(results, limit=1)

    def filing_sections(
        self,
        symbol: str,
        section: str = 'business',
        limit: int = 1,
    ) -> List[dict]:
        """Get 10-K filing sections from Massive.com.

        Parameters
        ----------
        symbol : str
            Stock ticker (e.g. "AAPL").
        section : str
            Section name: "business" or "risk_factors" (default "business").
        limit : int
            Number of filings to return (default 1, i.e. most recent).

        Returns
        -------
        List[dict]
            Each dict has: ticker, cik, filing_date, period_end, section,
            text, filing_url.
        """
        params = {
            'ticker': symbol,
            'section': section,
            'limit': limit,
        }
        resp = self._massive_client._get(
            '/stocks/filings/10-K/v1/sections',
            params=params,
            result_key='results',
            raw=False,
        )
        if isinstance(resp, list):
            return resp
        return list(resp) if resp else []

    # ------------------------------------------------------------------
    # Options — Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_massive_option_ticker(ticker: str) -> dict:
        """Parse a Massive option ticker like ``O:AAPL260320C00250000`` into components.

        Returns dict with keys: symbol, expiration, right, strike.
        """
        t = ticker
        if t.startswith('O:'):
            t = t[2:]

        # Format: SYMBOL YYMMDD C/P STRIKE*1000 (strike is 8 digits, 3 implied decimals)
        # Find where the date starts — first digit run after the symbol
        i = 0
        while i < len(t) and t[i].isalpha():
            i += 1
        symbol = t[:i]
        rest = t[i:]  # e.g. 260320C00250000

        if len(rest) < 9:
            raise ValueError(f"Cannot parse option ticker: {ticker}")

        date_str = rest[:6]  # YYMMDD
        right = rest[6]      # C or P
        strike_str = rest[7:]

        expiration = f'20{date_str[:2]}-{date_str[2:4]}-{date_str[4:6]}'
        strike = float(strike_str) / 1000.0

        return {
            'symbol': symbol,
            'expiration': expiration,
            'right': right,
            'strike': strike,
        }

    @staticmethod
    def _build_massive_option_ticker(symbol: str, expiration: str, strike: float, right: str) -> str:
        """Build a Massive option ticker from components.

        Parameters
        ----------
        symbol : str
            Underlying symbol (e.g. "AAPL").
        expiration : str
            Expiration date as YYYY-MM-DD.
        strike : float
            Strike price.
        right : str
            "C" for call, "P" for put.

        Returns
        -------
        str
            Ticker like ``O:AAPL260320C00250000``.
        """
        from datetime import datetime
        dt_obj = datetime.strptime(expiration, '%Y-%m-%d')
        date_str = dt_obj.strftime('%y%m%d')
        strike_int = int(strike * 1000)
        return f'O:{symbol}{date_str}{right.upper()}{strike_int:08d}'

    def _resolve_option_contract(
        self,
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
    ) -> Contract:
        """Build a partial option Contract and resolve it via IB to get the conId.

        Parameters
        ----------
        symbol : str
            Underlying symbol.
        expiration : str
            Expiration as YYYY-MM-DD.
        strike : float
            Strike price.
        right : str
            "C" for call, "P" for put.

        Returns
        -------
        Contract
            Fully resolved IB Contract with conId.
        """
        # IB wants lastTradeDateOrContractMonth as YYYYMMDD
        last_trade_date = expiration.replace('-', '')

        partial = Contract(
            symbol=symbol,
            secType='OPT',
            exchange='SMART',
            currency='USD',
            lastTradeDateOrContractMonth=last_trade_date,
            strike=strike,
            right=right.upper(),
            multiplier='100',
        )

        defs: List[SecurityDefinition] = consume(
            self._rpc.rpc(return_type=list[SecurityDefinition]).resolve_contract(partial)
        )
        if not defs:
            raise ValueError(
                f"Could not resolve option contract: {symbol} {expiration} {strike} {right}"
            )
        d = defs[0]
        return Contract(
            conId=d.conId,
            symbol=d.symbol,
            secType=d.secType,
            exchange=d.exchange,
            primaryExchange=getattr(d, 'primaryExchange', ''),
            currency=d.currency,
            lastTradeDateOrContractMonth=last_trade_date,
            strike=strike,
            right=right.upper(),
            multiplier='100',
        )

    # ------------------------------------------------------------------
    # Options — Data (Massive API, no trader_service needed)
    # ------------------------------------------------------------------

    def options_expirations(self, symbol: str) -> List[str]:
        """Get available expiration dates for a symbol's options.

        Parameters
        ----------
        symbol : str
            Underlying symbol (e.g. "AAPL").

        Returns
        -------
        List[str]
            Sorted list of expiration dates as YYYY-MM-DD strings.
        """
        from trader.tools.chain import get_option_dates
        cfg = self._container.config()
        api_key = cfg.get('massive_api_key', '')
        return get_option_dates(symbol, api_key=api_key)

    def options_chain(
        self,
        symbol: str,
        expiration: Optional[str] = None,
        contract_type: Optional[str] = None,
        strike_min: Optional[float] = None,
        strike_max: Optional[float] = None,
    ) -> pd.DataFrame:
        """Get options chain snapshot from Massive.com.

        Parameters
        ----------
        symbol : str
            Underlying symbol (e.g. "AAPL").
        expiration : str, optional
            Filter to specific expiration (YYYY-MM-DD). If None, uses nearest.
        contract_type : str, optional
            Filter to "call" or "put".
        strike_min : float, optional
            Minimum strike price.
        strike_max : float, optional
            Maximum strike price.

        Returns
        -------
        pd.DataFrame
            Chain with columns: ticker, type, strike, expiration, bid, ask, mid,
            last, volume, open_interest, iv, delta, gamma, theta, vega,
            break_even, underlying_price.
        """
        import datetime as dt_mod
        cfg = self._container.config()
        api_key = cfg.get('massive_api_key', '')
        if not api_key:
            raise ValueError("massive_api_key not configured in trader.yaml")

        if not expiration:
            from trader.tools.chain import get_option_dates
            dates = get_option_dates(symbol, api_key=api_key)
            if not dates:
                return pd.DataFrame()
            expiration = dates[0]

        from massive import RESTClient
        client = RESTClient(api_key=api_key)

        rows = []
        for snap in client.list_snapshot_options_chain(
            underlying_asset=symbol,
            params={'expiration_date': expiration},
        ):
            details = snap.details
            if not details or not details.strike_price:
                continue

            ct = (details.contract_type or '').lower()
            if contract_type and ct != contract_type.lower():
                continue

            strike = details.strike_price
            if strike_min is not None and strike < strike_min:
                continue
            if strike_max is not None and strike > strike_max:
                continue

            bid = ask = last = volume = 0.0
            if snap.last_quote:
                bid = snap.last_quote.bid or 0.0
                ask = snap.last_quote.ask or 0.0
            if snap.last_trade:
                last = getattr(snap.last_trade, 'price', 0.0) or 0.0
            if snap.day:
                volume = getattr(snap.day, 'volume', 0.0) or 0.0

            mid = (bid + ask) / 2.0 if (bid and ask) else 0.0

            greeks = snap.greeks
            delta = gamma = theta = vega = 0.0
            if greeks:
                delta = greeks.delta or 0.0
                gamma = greeks.gamma or 0.0
                theta = greeks.theta or 0.0
                vega = greeks.vega or 0.0

            underlying_price = 0.0
            if snap.underlying_asset:
                underlying_price = snap.underlying_asset.price or 0.0

            rows.append({
                'ticker': details.ticker or '',
                'type': ct,
                'strike': strike,
                'expiration': details.expiration_date or expiration,
                'bid': bid,
                'ask': ask,
                'mid': mid,
                'last': last,
                'volume': volume,
                'open_interest': snap.open_interest or 0.0,
                'iv': (snap.implied_volatility or 0.0) * 100.0,  # as percentage
                'delta': delta,
                'gamma': gamma,
                'theta': theta,
                'vega': vega,
                'break_even': snap.break_even_price or 0.0,
                'underlying_price': underlying_price,
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(by=['type', 'strike']).reset_index(drop=True)
        return df

    def options_snapshot(self, option_ticker: str) -> dict:
        """Get detailed snapshot for a single option contract.

        Parameters
        ----------
        option_ticker : str
            Massive option ticker (e.g. ``O:AAPL260320C00250000``).

        Returns
        -------
        dict
            Snapshot details including greeks, quote, underlying price.
        """
        cfg = self._container.config()
        api_key = cfg.get('massive_api_key', '')
        if not api_key:
            raise ValueError("massive_api_key not configured in trader.yaml")

        parsed = self._parse_massive_option_ticker(option_ticker)
        # The Massive API wants the ticker without the O: prefix
        ticker_clean = option_ticker
        if ticker_clean.startswith('O:'):
            ticker_clean = ticker_clean[2:]

        from massive import RESTClient
        client = RESTClient(api_key=api_key)
        snap = client.get_snapshot_option(
            underlying_asset=parsed['symbol'],
            option_contract=ticker_clean,
        )

        result = {
            'ticker': option_ticker,
            'symbol': parsed['symbol'],
            'expiration': parsed['expiration'],
            'strike': parsed['strike'],
            'right': parsed['right'],
            'break_even': snap.break_even_price or 0.0,
            'implied_volatility': f'{(snap.implied_volatility or 0.0) * 100.0:.2f}%',
            'open_interest': snap.open_interest or 0.0,
        }

        if snap.last_quote:
            result['bid'] = snap.last_quote.bid or 0.0
            result['ask'] = snap.last_quote.ask or 0.0
            result['mid'] = ((snap.last_quote.bid or 0.0) + (snap.last_quote.ask or 0.0)) / 2.0

        if snap.last_trade:
            result['last'] = getattr(snap.last_trade, 'price', 0.0) or 0.0

        if snap.greeks:
            result['delta'] = snap.greeks.delta or 0.0
            result['gamma'] = snap.greeks.gamma or 0.0
            result['theta'] = snap.greeks.theta or 0.0
            result['vega'] = snap.greeks.vega or 0.0

        if snap.underlying_asset:
            result['underlying_price'] = snap.underlying_asset.price or 0.0

        if snap.day:
            result['volume'] = getattr(snap.day, 'volume', 0.0) or 0.0

        return result

    def options_implied(
        self,
        symbol: str,
        expiration: str,
        risk_free_rate: float = 0.05,
    ) -> Dict:
        """Get implied probability distribution for an options expiration.

        Parameters
        ----------
        symbol : str
            Underlying symbol.
        expiration : str
            Expiration date as YYYY-MM-DD.
        risk_free_rate : float
            Risk-free rate (default 0.05).

        Returns
        -------
        dict
            Keys: x (strikes), market_implied (probabilities), constant (probabilities).
        """
        from trader.tools.chain import implied_constant
        cfg = self._container.config()
        api_key = cfg.get('massive_api_key', '')
        return implied_constant(symbol, expiration, risk_free_rate, api_key=api_key)

    # ------------------------------------------------------------------
    # Options — Trading (IB via trader_service)
    # ------------------------------------------------------------------

    def buy_option(
        self,
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        quantity: float,
        limit_price: Optional[float] = None,
        market: bool = False,
    ) -> SuccessFail:
        """Place a buy order for an option contract.

        Parameters
        ----------
        symbol : str
            Underlying symbol (e.g. "AAPL").
        expiration : str
            Expiration date as YYYY-MM-DD.
        strike : float
            Strike price.
        right : str
            "C" for call, "P" for put.
        quantity : float
            Number of contracts.
        limit_price : float, optional
            Limit price per contract. Required if market=False.
        market : bool
            True for market order.
        """
        if not market and limit_price is None:
            raise ValueError("Specify market=True or provide a limit_price")

        contract = self._resolve_option_contract(symbol, expiration, strike, right)

        return consume(
            self._rpc.rpc(return_type=SuccessFail[Trade]).place_order_simple(
                contract=contract,
                action='BUY',
                equity_amount=None,
                quantity=quantity,
                limit_price=limit_price,
                market_order=market,
                stop_loss_percentage=0.0,
                debug=False,
            )
        )

    def sell_option(
        self,
        symbol: str,
        expiration: str,
        strike: float,
        right: str,
        quantity: float,
        limit_price: Optional[float] = None,
        market: bool = False,
    ) -> SuccessFail:
        """Place a sell order for an option contract.

        Parameters
        ----------
        symbol : str
            Underlying symbol (e.g. "AAPL").
        expiration : str
            Expiration date as YYYY-MM-DD.
        strike : float
            Strike price.
        right : str
            "C" for call, "P" for put.
        quantity : float
            Number of contracts.
        limit_price : float, optional
            Limit price per contract. Required if market=False.
        market : bool
            True for market order.
        """
        if not market and limit_price is None:
            raise ValueError("Specify market=True or provide a limit_price")

        contract = self._resolve_option_contract(symbol, expiration, strike, right)

        return consume(
            self._rpc.rpc(return_type=SuccessFail[Trade]).place_order_simple(
                contract=contract,
                action='SELL',
                equity_amount=None,
                quantity=quantity,
                limit_price=limit_price,
                market_order=market,
                stop_loss_percentage=0.0,
                debug=False,
            )
        )

    # ------------------------------------------------------------------
    # Forex
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_forex_pair(pair: str) -> tuple[str, str]:
        """Parse 'EURUSD', 'EUR/USD', or 'C:EURUSD' into ('EUR', 'USD')."""
        pair = pair.replace('/', '').replace('C:', '').upper()
        if len(pair) == 6:
            return pair[:3], pair[3:]
        return pair, 'USD'

    def forex_snapshot(self, pair: str, source: str = 'ib') -> dict:
        """Get a snapshot for a forex pair.

        Parameters
        ----------
        pair : str
            Currency pair (e.g. 'EURUSD', 'EUR/USD', 'C:EURUSD').
        source : str
            'ib' (default) uses Interactive Brokers via trader_service.
            'massive' uses Massive.com REST API (requires forex plan).
        """
        if source == 'massive':
            ticker = pair.upper()
            if not ticker.startswith('C:'):
                ticker = f'C:{ticker}'
            snap = self._massive_client.get_snapshot_ticker(market_type='forex', ticker=ticker)
            result = {'ticker': ticker}
            if snap.day:
                result['open'] = getattr(snap.day, 'open', None)
                result['high'] = getattr(snap.day, 'high', None)
                result['low'] = getattr(snap.day, 'low', None)
                result['close'] = getattr(snap.day, 'close', None)
                result['volume'] = getattr(snap.day, 'volume', None)
                result['vwap'] = getattr(snap.day, 'vwap', None)
            if snap.last_quote:
                result['bid'] = getattr(snap.last_quote, 'bid', None) or getattr(snap.last_quote, 'P', None)
                result['ask'] = getattr(snap.last_quote, 'ask', None) or getattr(snap.last_quote, 'P', None)
            if snap.todays_change is not None:
                result['change'] = snap.todays_change
            if snap.todays_change_percent is not None:
                result['change_pct'] = snap.todays_change_percent
            return result
        else:
            # IB source
            base, quote_ccy = self._parse_forex_pair(pair)
            contract = self._resolve_contract(base, sec_type='CASH')
            ticker_data: Ticker = consume(
                self._rpc.rpc(return_type=Ticker).get_snapshot(contract, False)
            )
            return {
                'pair': f'{base}/{quote_ccy}',
                'bid': ticker_data.bid,
                'bidSize': ticker_data.bidSize,
                'ask': ticker_data.ask,
                'askSize': ticker_data.askSize,
                'last': ticker_data.last,
                'open': ticker_data.open,
                'high': ticker_data.high,
                'low': ticker_data.low,
                'close': ticker_data.close,
                'time': ticker_data.time,
            }

    def forex_quote(self, from_currency: str, to_currency: str, source: str = 'ib') -> dict:
        """Get the last forex quote for a currency pair.

        Parameters
        ----------
        from_currency : str
            Base currency (e.g. 'EUR').
        to_currency : str
            Quote currency (e.g. 'USD').
        source : str
            'ib' (default) or 'massive'.
        """
        if source == 'massive':
            result = self._massive_client.get_last_forex_quote(from_currency, to_currency)
            out = {'symbol': result.symbol}
            if result.last:
                out['bid'] = result.last.bid
                out['ask'] = result.last.ask
                out['exchange'] = result.last.exchange
                out['timestamp'] = result.last.timestamp
            return out
        else:
            # IB source — use snapshot on CASH contract
            contract = self._resolve_contract(from_currency.upper(), sec_type='CASH')
            ticker_data: Ticker = consume(
                self._rpc.rpc(return_type=Ticker).get_snapshot(contract, False)
            )
            return {
                'pair': f'{from_currency.upper()}/{to_currency.upper()}',
                'bid': ticker_data.bid,
                'ask': ticker_data.ask,
                'last': ticker_data.last,
                'time': ticker_data.time,
            }

    def forex_snapshot_all(self, tickers: Optional[List[str]] = None) -> pd.DataFrame:
        """Get snapshots for all forex pairs (Massive.com only)."""
        ticker_arg = None
        if tickers:
            ticker_arg = [t if t.startswith('C:') else f'C:{t}' for t in tickers]
        snaps = self._massive_client.get_snapshot_all(market_type='forex', tickers=ticker_arg)
        rows = []
        for snap in snaps:
            row = {'ticker': snap.ticker or ''}
            if snap.day:
                row['open'] = getattr(snap.day, 'open', None)
                row['high'] = getattr(snap.day, 'high', None)
                row['low'] = getattr(snap.day, 'low', None)
                row['close'] = getattr(snap.day, 'close', None)
                row['volume'] = getattr(snap.day, 'volume', None)
            if snap.todays_change is not None:
                row['change'] = snap.todays_change
            if snap.todays_change_percent is not None:
                row['change_pct'] = snap.todays_change_percent
            rows.append(row)
        df = pd.DataFrame(rows)
        if not df.empty and 'change_pct' in df.columns:
            df = df.sort_values('change_pct', ascending=False).reset_index(drop=True)
        return df

    def forex_movers(self, direction: str = 'gainers') -> pd.DataFrame:
        """Get top forex movers (Massive.com only)."""
        snaps = self._massive_client.get_snapshot_direction(
            market_type='forex', direction=direction,
        )
        rows = []
        for snap in snaps:
            row = {'ticker': snap.ticker or ''}
            if snap.day:
                row['close'] = getattr(snap.day, 'close', None)
                row['volume'] = getattr(snap.day, 'volume', None)
            if snap.todays_change is not None:
                row['change'] = snap.todays_change
            if snap.todays_change_percent is not None:
                row['change_pct'] = snap.todays_change_percent
            rows.append(row)
        return pd.DataFrame(rows)

    def movers(self, market: str = 'stocks', direction: str = 'gainers') -> pd.DataFrame:
        """Get top movers from Massive.com.

        market: stocks, crypto, indices, options, futures (forex already has its own command)
        direction: gainers or losers
        """
        snaps = self._massive_client.get_snapshot_direction(
            market_type=market, direction=direction,
        )
        rows = []
        for snap in snaps:
            row = {'ticker': snap.ticker or ''}
            if snap.day:
                row['close'] = getattr(snap.day, 'close', None)
                row['volume'] = getattr(snap.day, 'volume', None)
            if snap.todays_change is not None:
                row['change'] = snap.todays_change
            if snap.todays_change_percent is not None:
                row['change_pct'] = snap.todays_change_percent
            rows.append(row)
        return pd.DataFrame(rows)

    def scan(
        self,
        scan_code: str = 'TOP_PERC_GAIN',
        instrument: str = 'STK',
        location_code: str = 'STK.US.MAJOR',
        num_rows: int = 20,
        above_price: float = 0.0,
        above_volume: int = 0,
        market_cap_above: float = 0.0,
    ) -> pd.DataFrame:
        """Run an IB market scanner."""
        results = consume(
            self._rpc.rpc(return_type=list[dict]).scanner_data(
                scan_code=scan_code,
                instrument=instrument,
                location_code=location_code,
                num_rows=num_rows,
                above_price=above_price,
                above_volume=above_volume,
                market_cap_above=market_cap_above,
            )
        )
        return pd.DataFrame(results) if results else pd.DataFrame()

    def forex_convert(self, from_currency: str, to_currency: str, amount: float) -> dict:
        """Convert an amount between currencies (Massive.com only)."""
        result = self._massive_client.get_real_time_currency_conversion(
            from_currency, to_currency, amount=amount,
        )
        out = {
            'from': result.from_,
            'to': result.to,
            'amount': result.initial_amount,
            'converted': result.converted,
        }
        if result.last:
            out['bid'] = result.last.bid
            out['ask'] = result.last.ask
        return out

    # ------------------------------------------------------------------
    # News (Massive.com REST API)
    # ------------------------------------------------------------------

    def news(
        self,
        ticker: Optional[str] = None,
        limit: int = 10,
        source: str = 'polygon',
    ) -> pd.DataFrame:
        """Get news articles, optionally filtered by ticker.

        Parameters
        ----------
        ticker : str, optional
            Stock ticker to filter (e.g. "AAPL"). None for general news.
        limit : int
            Max articles to return (default 10).
        source : str
            "polygon" (default) or "benzinga".
        """
        rows = []
        if source == 'benzinga':
            articles = self._massive_client.list_benzinga_news(
                tickers=ticker, limit=limit,
            )
            for a in articles:
                rows.append({
                    'published': a.published or '',
                    'title': a.title or '',
                    'tickers': ', '.join(a.tickers) if a.tickers else '',
                    'author': a.author or '',
                    'url': a.url or '',
                    'teaser': a.teaser or '',
                })
                if len(rows) >= limit:
                    break
        else:
            articles = self._massive_client.list_ticker_news(
                ticker=ticker, limit=limit,
            )
            for a in articles:
                sentiment = ''
                if a.insights:
                    sentiments = [i.sentiment for i in a.insights if i.sentiment]
                    sentiment = ', '.join(sentiments)
                rows.append({
                    'published': (a.published_utc or '')[:19],
                    'title': a.title or '',
                    'tickers': ', '.join(a.tickers) if a.tickers else '',
                    'sentiment': sentiment,
                    'author': a.author or '',
                    'url': a.article_url or '',
                })
                if len(rows) >= limit:
                    break
        return pd.DataFrame(rows)

    def news_detail(
        self,
        ticker: Optional[str] = None,
        limit: int = 5,
        source: str = 'polygon',
    ) -> List[dict]:
        """Get news articles with full descriptions/teasers.

        Parameters
        ----------
        ticker : str, optional
            Stock ticker to filter.
        limit : int
            Max articles (default 5).
        source : str
            "polygon" or "benzinga".
        """
        results = []
        if source == 'benzinga':
            articles = self._massive_client.list_benzinga_news(
                tickers=ticker, limit=limit,
            )
            for a in articles:
                results.append({
                    'title': a.title or '',
                    'published': a.published or '',
                    'author': a.author or '',
                    'tickers': a.tickers or [],
                    'tags': a.tags or [],
                    'url': a.url or '',
                    'teaser': a.teaser or '',
                })
                if len(results) >= limit:
                    break
        else:
            articles = self._massive_client.list_ticker_news(
                ticker=ticker, limit=limit,
            )
            for a in articles:
                insights = []
                if a.insights:
                    for i in a.insights:
                        insights.append({
                            'ticker': i.ticker,
                            'sentiment': i.sentiment,
                            'reasoning': i.sentiment_reasoning,
                        })
                results.append({
                    'title': a.title or '',
                    'published': (a.published_utc or '')[:19],
                    'author': a.author or '',
                    'tickers': a.tickers or [],
                    'description': a.description or '',
                    'insights': insights,
                    'url': a.article_url or '',
                })
                if len(results) >= limit:
                    break
        return results
