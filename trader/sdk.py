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


def compute_resize_deltas(
    positions: list[dict],
    max_bound: Optional[float],
    min_bound: Optional[float],
) -> tuple[float, list[dict]]:
    """Compute proportional resize plan from portfolio positions.

    Returns (scale_factor, adjustments).

    Each adjustment: {symbol, conId, current_qty, target_qty, delta_qty, action,
                      current_value, target_value, mkt_price}
    """
    import math

    total_value = sum(abs(p.get('marketValue', 0) or 0) for p in positions)

    if total_value == 0:
        return 1.0, []

    scale_factor = 1.0
    if max_bound is not None and total_value > max_bound:
        scale_factor = max_bound / total_value
    elif min_bound is not None and total_value < min_bound:
        scale_factor = min_bound / total_value

    if scale_factor == 1.0:
        return 1.0, []

    adjustments = []
    for p in positions:
        current_qty = p.get('position', 0)
        if current_qty == 0:
            continue

        mkt_price = p.get('mktPrice', 0) or 0
        current_value = p.get('marketValue', 0) or 0

        target_qty = int(current_qty * scale_factor)
        delta_qty = target_qty - current_qty

        if delta_qty == 0:
            continue

        # For long positions: selling reduces, buying increases
        # For short positions: buying reduces (covers), selling increases
        if current_qty > 0:
            action = 'BUY' if delta_qty > 0 else 'SELL'
        else:
            action = 'SELL' if delta_qty < 0 else 'BUY'

        target_value = target_qty * mkt_price if mkt_price else current_value * scale_factor

        adjustments.append({
            'symbol': p.get('symbol', ''),
            'conId': p.get('conId', 0),
            'current_qty': current_qty,
            'target_qty': target_qty,
            'delta_qty': delta_qty,
            'action': action,
            'current_value': current_value,
            'target_value': target_value,
            'mkt_price': mkt_price,
        })

    return scale_factor, adjustments


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
        timeout: int = 30,
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
        currency: str = '',
    ) -> List[SecurityDefinition]:
        """Resolve a symbol string or conId to SecurityDefinition(s) via trader_service.

        First checks the local universe DB.  If not found and the symbol is a
        string, falls back to explicit IB discovery via ``resolve_contract``
        with a fully-specified Contract (currency='USD' for STK, IDEALPRO for
        CASH).  Integer conIds never fall back — they must exist locally.
        """
        result = consume(
            self._rpc.rpc(return_type=list[SecurityDefinition]).resolve_symbol(
                symbol, exchange, universe, sec_type
            )
        )
        if result:
            return result

        # No IB fallback for integer conIds — must be exact.
        if type(symbol) is int:
            return []

        # Explicit IB discovery with a fully-specified Contract.
        if sec_type == 'CASH':
            pair = str(symbol).replace('/', '').replace('C:', '').upper()
            base = pair[:3] if len(pair) == 6 else pair
            quote_ccy = pair[3:] if len(pair) == 6 else 'USD'
            contract = Contract(symbol=base, secType='CASH', exchange='IDEALPRO', currency=quote_ccy)
        else:
            contract = Contract(
                symbol=str(symbol),
                exchange=exchange or 'SMART',
                secType=sec_type or 'STK',
                currency=currency or 'USD',
            )

        return consume(
            self._rpc.rpc(return_type=list[SecurityDefinition]).resolve_contract(contract)
        )

    def _resolve_contract(self, symbol: Union[str, int], sec_type: str = 'STK',
                          exchange: str = '', currency: str = '') -> Contract:
        """Resolve *symbol* to a single Contract, raising on ambiguity.

        When *exchange* or *currency* are provided, prefer definitions matching
        those hints (e.g. exchange='ASX', currency='AUD' for Australian stocks).
        Otherwise, prefers USD contracts on US exchanges.
        """
        definitions = self.resolve(symbol, sec_type=sec_type, exchange=exchange, currency=currency)
        if not definitions:
            raise ValueError(f"Could not resolve symbol: {symbol}")

        sec = definitions[0]
        if exchange or currency:
            # Prefer definition matching the exchange/currency hint
            for d in definitions:
                d_exchange = getattr(d, 'exchange', '') or ''
                d_primary = getattr(d, 'primaryExchange', '') or ''
                d_currency = getattr(d, 'currency', '') or ''
                if exchange and exchange.upper() not in (d_exchange.upper(), d_primary.upper()):
                    continue
                if currency and d_currency.upper() != currency.upper():
                    continue
                sec = d
                break
            else:
                # No definition matched the hints — re-resolve via IB directly
                if isinstance(symbol, str):
                    direct = consume(
                        self._rpc.rpc(return_type=list[SecurityDefinition]).resolve_contract(
                            Contract(
                                symbol=symbol,
                                exchange=exchange or 'SMART',
                                secType=sec_type or 'STK',
                                currency=currency or 'USD',
                            )
                        )
                    )
                    if direct:
                        sec = direct[0]
        elif len(definitions) > 1:
            # Default: prefer USD on a US exchange
            us_exchanges = {'SMART', 'NYSE', 'NASDAQ', 'AMEX', 'ARCA', 'BATS', 'IEX', 'ISLAND'}
            for d in definitions:
                d_currency = getattr(d, 'currency', '')
                d_exchange = getattr(d, 'exchange', '')
                d_primary = getattr(d, 'primaryExchange', '')
                if d_currency == 'USD' and (d_exchange in us_exchanges or d_primary in us_exchanges):
                    sec = d
                    break
            else:
                # Fallback: prefer USD even if exchange isn't explicitly US
                for d in definitions:
                    if getattr(d, 'currency', '') == 'USD':
                        sec = d
                        break

        # Use SMART routing for non-US exchanges to avoid IB Error 10311
        # ("direct routed orders may result in higher trade fees").
        # Keep the real exchange in primaryExchange so IB routes correctly.
        sec_exchange = sec.exchange or ''
        sec_primary = getattr(sec, 'primaryExchange', '') or ''
        us_smart = {'SMART', 'NYSE', 'NASDAQ', 'AMEX', 'ARCA', 'BATS', 'IEX', 'ISLAND'}
        if sec_exchange.upper() in us_smart:
            order_exchange = sec_exchange
            primary_exchange = sec_primary
        else:
            # Non-US exchange (ASX, TSE, SEHK, etc.) — use SMART routing
            order_exchange = 'SMART'
            primary_exchange = sec_primary or sec_exchange

        return Contract(
            conId=sec.conId,
            symbol=sec.symbol,
            secType=sec.secType,
            exchange=order_exchange,
            primaryExchange=primary_exchange,
            currency=sec.currency,
        )

    # ------------------------------------------------------------------
    # Portfolio & Positions
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_contract(contract) -> dict:
        """Extract contract fields whether it's a Contract object or a dict."""
        if isinstance(contract, dict):
            return {
                'conId': contract.get('conId', ''),
                'symbol': contract.get('localSymbol') or contract.get('symbol', ''),
                'secType': contract.get('secType', ''),
                'currency': contract.get('currency', ''),
            }
        return {
            'conId': contract.conId,
            'symbol': contract.localSymbol or contract.symbol,
            'secType': contract.secType,
            'currency': contract.currency,
        }

    def portfolio(self) -> pd.DataFrame:
        """Portfolio with P&L (matches the old ``portfolio`` CLI command)."""
        summaries = self._rpc.rpc(
            return_type=list[PortfolioSummary]
        ).get_portfolio_summary()

        rows = []
        for p in summaries:
            # PortfolioSummary is a NamedTuple; msgpack may deserialize as a plain list
            if isinstance(p, (list, tuple)) and not hasattr(p, 'account'):
                contract, position, mkt_price, mkt_value, avg_cost, unrealized, realized, account, daily = p
            else:
                contract, position, mkt_price, mkt_value = p.contract, p.position, p.marketPrice, p.marketValue
                avg_cost, unrealized, realized, account, daily = p.averageCost, p.unrealizedPNL, p.realizedPNL, p.account, p.dailyPNL

            con = self._extract_contract(contract)
            rows.append({
                'account': account,
                'conId': con['conId'],
                'symbol': con['symbol'],
                'position': position,
                'mktPrice': mkt_price,
                'avgCost': avg_cost,
                'marketValue': mkt_value,
                'unrealizedPNL': unrealized,
                'realizedPNL': realized,
                'dailyPNL': daily,
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
        pos_list = self._rpc.rpc(
            return_type=list[Position]
        ).get_positions()

        rows = []
        for p in pos_list:
            # Position is a NamedTuple; msgpack may deserialize as a plain list
            if isinstance(p, (list, tuple)) and not hasattr(p, 'account'):
                account, contract, position, avg_cost = p[0], p[1], p[2], p[3]
            else:
                account, contract, position, avg_cost = p.account, p.contract, p.position, p.avgCost

            con = self._extract_contract(contract)
            rows.append({
                'account': account,
                'conId': con['conId'],
                'symbol': con['symbol'],
                'secType': con['secType'],
                'position': position,
                'avgCost': avg_cost,
                'currency': con['currency'],
                'total': position * avg_cost,
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
        """Open orders with full detail (symbol, name, prices, account %)."""
        trades_raw: dict[int, list[Trade]] = self._rpc.rpc(
            return_type=dict[int, list[Trade]]
        ).get_trades()

        if not trades_raw:
            return pd.DataFrame()

        # Filter to active (non-terminal) orders only
        _terminal = {'Cancelled', 'Filled', 'Inactive', 'ApiCancelled'}
        trades_raw = {
            tid: tl for tid, tl in trades_raw.items()
            if tl[0].orderStatus.status not in _terminal
        }
        if not trades_raw:
            return pd.DataFrame()

        # Get net liquidation for account % calculation
        net_liq = 0.0
        try:
            acct_vals = consume(self._rpc.rpc(return_type=dict).get_account_values())
            net_liq = float(acct_vals.get('NetLiquidation', {}).get('value', 0))
        except Exception:
            pass

        # Resolve company names + snapshots for unique symbols
        name_cache: dict[str, str] = {}
        snap_cache: dict[str, dict] = {}
        unique_symbols: set[str] = set()
        for trade_list in trades_raw.values():
            unique_symbols.add(trade_list[0].contract.symbol)

        for sym in unique_symbols:
            # Company name from local universe DB
            try:
                defs = consume(
                    self._rpc.rpc(return_type=list[SecurityDefinition]).resolve_symbol(sym)
                )
                name_cache[sym] = defs[0].longName if defs and defs[0].longName else ''
            except Exception:
                name_cache[sym] = ''
            # Live snapshot for bid/ask context
            try:
                snap_cache[sym] = self.snapshot(sym, delayed=True)
            except Exception:
                snap_cache[sym] = {}

        rows = []
        for trade_id, trade_list in trades_raw.items():
            t = trade_list[0]
            o = t.order
            snap = snap_cache.get(t.contract.symbol, {})
            bid = snap.get('bid')
            ask = snap.get('ask')
            last = snap.get('last')
            # Estimate order value from the best available price
            # IB uses 1.7976931348623157e+308 as sentinel for "no price"
            _lmt = o.lmtPrice if o.lmtPrice and o.lmtPrice < 1e300 else 0
            _aux = o.auxPrice if o.auxPrice and o.auxPrice < 1e300 else 0
            price = _lmt or _aux or 0
            order_value = price * (o.totalQuantity or 0) if price else None
            acct_pct = (order_value / net_liq * 100) if order_value and net_liq > 0 else None

            rows.append({
                'orderId': o.orderId,
                'symbol': t.contract.symbol,
                'name': name_cache.get(t.contract.symbol, ''),
                'action': o.action,
                'orderType': o.orderType,
                'quantity': o.totalQuantity,
                'lmtPrice': o.lmtPrice if o.lmtPrice and o.lmtPrice < 1e300 else None,
                'auxPrice': o.auxPrice if o.auxPrice and o.auxPrice < 1e300 else None,
                'orderValue': round(order_value, 2) if order_value else None,
                'acctPct': round(acct_pct, 1) if acct_pct else None,
                'status': t.orderStatus.status,
                'filled': t.orderStatus.filled,
                'remaining': t.orderStatus.remaining,
                'avgFillPrice': t.orderStatus.avgFillPrice if t.orderStatus.avgFillPrice else None,
                'tif': o.tif,
                'parentId': o.parentId if o.parentId else None,
                'bid': bid,
                'ask': ask,
                'last': last,
            })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(by='orderId', ascending=True)
        return df

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

    def to_market(self, order_id: int) -> SuccessFail:
        """Cancel an open limit order and re-place as market, preserving stop-loss children."""
        trades_raw: dict[int, list[Trade]] = self._rpc.rpc(
            return_type=dict[int, list[Trade]]
        ).get_trades()

        if not trades_raw or order_id not in trades_raw:
            return SuccessFail.fail(error=f'Order #{order_id} not found')

        trade = trades_raw[order_id][0]
        contract = trade.contract
        order = trade.order
        action = order.action
        quantity = float(order.totalQuantity)

        if order.orderType == 'MKT':
            return SuccessFail.fail(error=f'Order #{order_id} is already a market order')

        # Find stop-loss child (parentId == this order)
        stop_price = None
        for tid, tlist in trades_raw.items():
            child = tlist[0]
            if child.order.parentId == order_id and child.order.orderType in ('STP', 'STP LMT'):
                stop_price = child.order.auxPrice
                break

        # Cancel parent (IB auto-cancels bracket children)
        self.cancel(order_id)

        # Build execution spec and re-place as market
        from trader.trading.proposal import ExecutionSpec
        if stop_price:
            spec = ExecutionSpec(order_type='MARKET', exit_type='STOP_LOSS', stop_loss_price=stop_price)
        else:
            spec = ExecutionSpec(order_type='MARKET')

        return consume(
            self._rpc.rpc(return_type=SuccessFail[list[Trade]]).place_expressive_order(
                contract=contract,
                action=action,
                quantity=quantity,
                execution_spec=spec.to_dict(),
                algo_name='to_market',
            )
        )

    # ------------------------------------------------------------------
    # Trade Proposals
    # ------------------------------------------------------------------

    def _proposal_store(self):
        """Lazy-init ProposalStore from config duckdb_path."""
        if not hasattr(self, '_prop_store'):
            cfg = self._container.config()
            duckdb_path = cfg.get('duckdb_path', '')
            if not duckdb_path:
                raise ValueError("duckdb_path not configured")
            from trader.data.proposal_store import ProposalStore
            self._prop_store = ProposalStore(duckdb_path)
        return self._prop_store

    def _get_portfolio_state(self):
        """Build a PortfolioState snapshot. Gracefully degrades if trader_service unavailable."""
        from trader.trading.position_sizing import PortfolioState
        state = PortfolioState()
        try:
            acct_vals = consume(self._rpc.rpc(return_type=dict).get_account_values())
            if acct_vals:
                state.net_liquidation = float(acct_vals.get('NetLiquidation', {}).get('value', 0))
                state.gross_position_value = float(acct_vals.get('GrossPositionValue', {}).get('value', 0))
                state.available_funds = float(acct_vals.get('AvailableFunds', {}).get('value', 0))
        except Exception:
            pass

        try:
            portfolio_df = self.portfolio()
            if portfolio_df is not None and not portfolio_df.empty:
                state.position_count = len(portfolio_df)
                if 'dailyPNL' in portfolio_df.columns:
                    state.daily_pnl = portfolio_df['dailyPNL'].sum()
        except Exception:
            pass

        try:
            store = self._proposal_store()
            pending = store.query(status='PENDING')
            if pending:
                state.pending_proposal_value = sum(
                    p.amount for p in pending if p.amount is not None and p.amount == p.amount
                )
        except Exception:
            pass

        return state

    def session_status(self) -> dict:
        """Return position sizing config, portfolio state, capacity, and recommended sizes."""
        from trader.trading.position_sizing import PositionSizingConfig, PositionSizer
        config = PositionSizingConfig.load()
        state = self._get_portfolio_state()
        return PositionSizer(config).session_summary(state)

    def propose(
        self,
        symbol: str,
        action: str,
        quantity: Optional[float] = None,
        amount: Optional[float] = None,
        execution=None,
        reasoning: str = '',
        confidence: float = 0.0,
        thesis: str = '',
        source: str = 'manual',
        metadata: Optional[dict] = None,
        sec_type: str = 'STK',
        exchange: str = '',
        currency: str = '',
    ) -> tuple[int, Optional[dict], Optional[dict]]:
        """Create a trade proposal. Returns (proposal_id, leverage_info, snapshot_info). No trader_service needed."""
        from trader.trading.proposal import ExecutionSpec, TradeProposal
        spec = execution if isinstance(execution, ExecutionSpec) else ExecutionSpec()
        validation_errors = spec.validate()
        if validation_errors:
            raise ValueError(f'Invalid execution spec: {"; ".join(validation_errors)}')

        # Auto-size when neither quantity nor amount is provided
        if metadata is None:
            metadata = {}
        if quantity is None and amount is None:
            try:
                from trader.trading.position_sizing import (
                    LiquidityInfo, PositionSizingConfig, PositionSizer,
                )
                config = PositionSizingConfig.load()
                state = self._get_portfolio_state()
                sizer = PositionSizer(config)

                # Build liquidity info from snapshot (gracefully degrades)
                liq = None
                try:
                    snap = self.snapshot(symbol, exchange=exchange, currency=currency)
                    if snap:
                        import math
                        _bid = snap.get('bid', 0) or 0
                        _ask = snap.get('ask', 0) or 0
                        _last = snap.get('last', 0) or 0
                        _bid_sz = snap.get('bidSize', 0) or 0
                        _ask_sz = snap.get('askSize', 0) or 0
                        # NaN guard
                        if isinstance(_bid, float) and math.isnan(_bid): _bid = 0
                        if isinstance(_ask, float) and math.isnan(_ask): _ask = 0
                        if isinstance(_last, float) and math.isnan(_last): _last = 0
                        if isinstance(_bid_sz, float) and math.isnan(_bid_sz): _bid_sz = 0
                        if isinstance(_ask_sz, float) and math.isnan(_ask_sz): _ask_sz = 0
                        liq = LiquidityInfo(
                            bid=_bid, ask=_ask, last=_last,
                            bid_size=_bid_sz, ask_size=_ask_sz,
                        )
                        # Try to get ADV from local historical data
                        try:
                            from trader.data.duckdb_store import DuckDBDataStore
                            cfg = self._container.config()
                            duckdb_path = cfg.get('duckdb_path', '')
                            if duckdb_path:
                                ds = DuckDBDataStore(duckdb_path)
                                # Look up conId for the symbol
                                defs = self.resolve(symbol, sec_type=sec_type, exchange=exchange)
                                if defs:
                                    con_id = defs[0].conId
                                    hist = ds.read(con_id, '1 day', count=20)
                                    if hist is not None and not hist.empty and 'volume' in hist.columns:
                                        liq.avg_daily_volume = float(hist['volume'].mean())
                        except Exception:
                            pass
                except Exception:
                    pass

                result = sizer.compute(
                    confidence=confidence, portfolio_state=state, liquidity=liq,
                )
                if result.amount_usd > 0:
                    amount = result.amount_usd
                    metadata['auto_sized'] = True
                    metadata['sizing_result'] = {
                        'amount': result.amount_usd,
                        'reasoning': result.reasoning,
                        'capped_by': result.capped_by,
                        'warnings': result.warnings,
                    }
            except Exception:
                pass  # Fall through — proposal will have no size, same as before

        proposal = TradeProposal(
            symbol=symbol,
            action=action,
            quantity=quantity,
            amount=amount,
            execution=spec,
            reasoning=reasoning,
            confidence=confidence,
            thesis=thesis,
            source=source,
            metadata=metadata or {},
            sec_type=sec_type,
            exchange=exchange,
            currency=currency,
        )
        proposal_id = self._proposal_store().add(proposal)

        # Attempt to capture snapshot + leverage estimate (requires trader_service, gracefully degrades)
        leverage_info = None
        snapshot_info = None
        try:
            # Capture bid/ask/last at proposal time
            snap = self.snapshot(symbol, exchange=exchange, currency=currency)
            if snap:
                snapshot_info = {
                    'bid': snap.get('bid'),
                    'ask': snap.get('ask'),
                    'last': snap.get('last'),
                    'time': str(snap.get('time', '')),
                }
                self._proposal_store().update_metadata(proposal_id, {'snapshot': snapshot_info})

            acct_vals = consume(self._rpc.rpc(return_type=dict).get_account_values())
            if acct_vals:
                net_liq = float(acct_vals.get('NetLiquidation', {}).get('value', 0))
                gross_pos = float(acct_vals.get('GrossPositionValue', {}).get('value', 0))
                buying_power = float(acct_vals.get('BuyingPower', {}).get('value', 0))

                # Estimate order value using snapshot price if no limit
                snap_price = snap.get('last') or snap.get('ask') or 0 if snap else 0
                price = spec.limit_price or snap_price
                order_value = quantity * price if quantity and price else amount or 0

                if net_liq > 0:
                    current_leverage = gross_pos / net_liq
                    estimated_leverage = (gross_pos + order_value) / net_liq
                    leverage_info = {
                        'current_leverage': round(current_leverage, 2),
                        'estimated_leverage': round(estimated_leverage, 2),
                        'net_liquidation': net_liq,
                        'buying_power': buying_power,
                        'uses_margin': estimated_leverage > 1.0,
                    }
                    self._proposal_store().update_metadata(proposal_id, {'leverage_estimate': leverage_info})
        except Exception:
            pass  # No trader_service connection, skip enrichment

        return proposal_id, leverage_info, snapshot_info

    def proposals(self, status: Optional[str] = None, limit: int = 50) -> pd.DataFrame:
        """List proposals. No trader_service needed."""
        props = self._proposal_store().query(status=status, limit=limit)
        if not props:
            return pd.DataFrame()

        rows = []
        for p in props:
            # Build concise size column: "100 sh" or "$5,000"
            if p.quantity is not None and p.quantity == p.quantity:  # not NaN
                size = f'{p.quantity:g} sh'
            elif p.amount is not None and p.amount == p.amount:
                size = f'${p.amount:,.0f}'
            else:
                size = '-'

            # Build concise order description: "MKT" or "LMT @165"
            order = 'MKT' if p.execution.order_type == 'MARKET' else f'LMT @{p.execution.limit_price:g}'

            # Compact exit type
            exit_abbrev = {'NONE': '-', 'BRACKET': 'BKT', 'TRAILING_STOP': 'TSL', 'STOP_LOSS': 'SL'}
            exit_label = exit_abbrev.get(p.execution.exit_type, p.execution.exit_type[:3])

            # Compact timestamp — full detail in `proposals show N`
            created = p.created_at.strftime('%m/%d %H:%M') if p.created_at else ''

            row = {
                'id': p.id,
                'symbol': p.symbol,
                'action': p.action,
                'size': size,
                'order': order,
                'exit': exit_label,
                'conf': f'{p.confidence:.0%}' if p.confidence else '-',
                'created': created,
                'reasoning': p.reasoning or '',
            }
            # Include status column only when showing mixed statuses (--all)
            if status is None:
                row['status'] = p.status
            # Include leverage warning if available
            leverage = p.metadata.get('leverage_estimate')
            if leverage and leverage.get('uses_margin'):
                row['margin'] = f"{leverage['estimated_leverage']:.1f}x"
            rows.append(row)
        return pd.DataFrame(rows)

    def proposal_detail(self, proposal_id: int) -> Optional[dict]:
        """Get full proposal detail as dict. No trader_service needed."""
        p = self._proposal_store().get(proposal_id)
        if not p:
            return None
        detail = {
            'id': p.id,
            'symbol': p.symbol,
            'action': p.action,
            'quantity': p.quantity,
            'amount': p.amount,
            'sec_type': p.sec_type,
            'order_type': p.execution.order_type,
            'limit_price': p.execution.limit_price,
            'exit_type': p.execution.exit_type,
            'take_profit_price': p.execution.take_profit_price,
            'stop_loss_price': p.execution.stop_loss_price,
            'trailing_stop_percent': p.execution.trailing_stop_percent,
            'trailing_stop_amount': p.execution.trailing_stop_amount,
            'tif': p.execution.tif,
            'outside_rth': p.execution.outside_rth,
            'good_till_date': p.execution.good_till_date,
            'reasoning': p.reasoning,
            'confidence': p.confidence,
            'thesis': p.thesis,
            'source': p.source,
            'metadata': p.metadata,
            'status': p.status,
            'created_at': p.created_at,
            'updated_at': p.updated_at,
            'order_ids': p.order_ids,
            'rejection_reason': p.rejection_reason,
        }

        # Include snapshot from metadata if present
        snapshot = p.metadata.get('snapshot')
        if snapshot:
            detail['snapshot_bid'] = snapshot.get('bid')
            detail['snapshot_ask'] = snapshot.get('ask')
            detail['snapshot_last'] = snapshot.get('last')
            detail['snapshot_time'] = snapshot.get('time')

        # Include leverage estimate from metadata if present
        leverage = p.metadata.get('leverage_estimate')
        if leverage:
            detail['leverage_current'] = leverage.get('current_leverage')
            detail['leverage_estimated'] = leverage.get('estimated_leverage')
            detail['uses_margin'] = leverage.get('uses_margin', False)

        return detail

    def reject(self, proposal_id: int, reason: str = '') -> bool:
        """Reject a proposal. No trader_service needed."""
        p = self._proposal_store().get(proposal_id)
        if not p:
            return False
        if p.status != 'PENDING':
            return False
        self._proposal_store().update_status(proposal_id, 'REJECTED', rejection_reason=reason)
        return True

    def approve(self, proposal_id: int) -> SuccessFail:
        """Approve and execute a proposal. REQUIRES trader_service."""
        from trader.trading.proposal import ProposalStatus

        store = self._proposal_store()
        proposal = store.get(proposal_id)
        if not proposal:
            return SuccessFail.fail(error=f'Proposal #{proposal_id} not found')
        if proposal.status != 'PENDING':
            return SuccessFail.fail(error=f'Proposal #{proposal_id} is {proposal.status}, not PENDING')

        # Mark as approved
        store.update_status(proposal_id, 'APPROVED')

        try:
            contract = self._resolve_contract(
                proposal.symbol, sec_type=proposal.sec_type,
                exchange=proposal.exchange, currency=proposal.currency,
            )

            # Determine quantity
            qty = proposal.quantity
            if qty is None and proposal.amount is not None:
                import math
                ticker = consume(
                    self._rpc.rpc(return_type=Ticker).get_snapshot(contract, False)
                )
                snap = {
                    'last': getattr(ticker, 'last', float('nan')),
                    'ask': getattr(ticker, 'ask', float('nan')),
                    'bid': getattr(ticker, 'bid', float('nan')),
                }
                # Pick the first valid (non-NaN, positive) price
                price = None
                for key in ('last', 'ask', 'bid'):
                    val = snap.get(key)
                    if val and isinstance(val, (int, float)) and not math.isnan(val) and val > 0:
                        price = val
                        break
                if not price:
                    store.update_status(proposal_id, 'FAILED')
                    return SuccessFail.fail(error='Could not determine price for quantity calculation')
                qty = round(proposal.amount / price)
                if qty < 1:
                    qty = 1

            if qty is None or qty <= 0:
                store.update_status(proposal_id, 'FAILED')
                return SuccessFail.fail(error='No valid quantity or amount specified')

            result = consume(
                self._rpc.rpc(return_type=SuccessFail[list[Trade]]).place_expressive_order(
                    contract=contract,
                    action=proposal.action,
                    quantity=float(qty),
                    execution_spec=proposal.execution.to_dict(),
                    algo_name='proposal',
                )
            )

            if result.is_success():
                order_ids = []
                if result.obj:
                    for t in result.obj:
                        oid = getattr(t, 'orderId', None) or getattr(getattr(t, 'order', None), 'orderId', None)
                        if oid:
                            order_ids.append(oid)
                store.update_status(proposal_id, 'EXECUTED', order_ids=order_ids)
                return SuccessFail.success(obj=order_ids)
            else:
                store.update_status(proposal_id, 'FAILED')
                return SuccessFail.fail(error=result.error, exception=result.exception)

        except Exception as ex:
            store.update_status(proposal_id, 'FAILED')
            return SuccessFail.fail(error=str(ex))

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
    # Portfolio Resizing
    # ------------------------------------------------------------------

    def compute_resize_plan(
        self,
        max_bound: Optional[float] = None,
        min_bound: Optional[float] = None,
    ) -> dict:
        """Compute a resize plan for the portfolio.

        Returns a dict with scale_factor, current_total, target_total,
        and adjustments (each with associated_orders info).
        """
        portfolio_df = self.portfolio()
        if portfolio_df.empty:
            return {'scale_factor': 1.0, 'current_total': 0, 'target_total': 0, 'adjustments': []}

        positions = portfolio_df.to_dict('records')
        total_value = sum(abs(p.get('marketValue', 0) or 0) for p in positions)

        scale_factor, adjustments = compute_resize_deltas(positions, max_bound, min_bound)

        if scale_factor == 1.0:
            return {
                'scale_factor': 1.0,
                'current_total': total_value,
                'target_total': total_value,
                'adjustments': [],
            }

        target_total = total_value * scale_factor

        # Find associated protective orders for each position
        trades_raw: dict = {}
        try:
            trades_raw = self._rpc.rpc(
                return_type=dict[int, list[Trade]]
            ).get_trades()
        except Exception:
            pass

        if trades_raw:
            _terminal = {'Cancelled', 'Filled', 'Inactive', 'ApiCancelled'}
            trades_raw = {
                tid: tl for tid, tl in trades_raw.items()
                if tl[0].orderStatus.status not in _terminal
            }

        for adj in adjustments:
            associated = []
            if trades_raw:
                con_id = adj['conId']
                pos_direction = 'LONG' if adj['current_qty'] > 0 else 'SHORT'
                for tid, tl in trades_raw.items():
                    t = tl[0]
                    t_con_id = t.contract.conId if hasattr(t.contract, 'conId') else (t.contract.get('conId') if isinstance(t.contract, dict) else 0)
                    if t_con_id != con_id:
                        continue
                    o = t.order
                    order_type = o.orderType or ''
                    order_action = o.action or ''

                    # Protective orders are opposite direction to position
                    is_protective = (
                        (pos_direction == 'LONG' and order_action == 'SELL') or
                        (pos_direction == 'SHORT' and order_action == 'BUY')
                    )
                    is_stop_type = order_type in ('STP', 'STP LMT', 'TRAIL')
                    is_tp = order_type == 'LMT' and (o.parentId or 0) > 0

                    if is_protective and (is_stop_type or is_tp):
                        aux = o.auxPrice if hasattr(o, 'auxPrice') and o.auxPrice and o.auxPrice < 1e300 else 0
                        lmt = o.lmtPrice if hasattr(o, 'lmtPrice') and o.lmtPrice and o.lmtPrice < 1e300 else 0
                        trail_pct = getattr(o, 'trailingPercent', 0) or 0

                        associated.append({
                            'orderId': o.orderId,
                            'orderType': order_type,
                            'action': order_action,
                            'quantity': float(o.totalQuantity or 0),
                            'auxPrice': float(aux),
                            'lmtPrice': float(lmt),
                            'trailingPercent': float(trail_pct),
                            'tif': o.tif or 'GTC',
                        })

            adj['associated_orders'] = associated

        return {
            'scale_factor': scale_factor,
            'current_total': total_value,
            'target_total': target_total,
            'adjustments': adjustments,
        }

    def execute_resize_plan(self, plan: dict) -> dict:
        """Execute a resize plan: cancel protective orders, place deltas, re-create protectives.

        Returns a summary dict with successes, failures, and warnings.
        """
        results = {
            'successes': [],
            'failures': [],
            'warnings': [],
        }

        for adj in plan.get('adjustments', []):
            symbol = adj['symbol']
            delta_qty = adj['delta_qty']
            action = adj['action']
            target_qty = adj['target_qty']
            associated = adj.get('associated_orders', [])

            # 1. Cancel associated protective orders
            for order_info in associated:
                try:
                    cancel_result = self.cancel(order_info['orderId'])
                    if not cancel_result.is_success():
                        results['warnings'].append(
                            f'{symbol}: failed to cancel order #{order_info["orderId"]}: {cancel_result.error}'
                        )
                except Exception as ex:
                    results['warnings'].append(
                        f'{symbol}: error cancelling order #{order_info["orderId"]}: {ex}'
                    )

            # 2. Place market order for delta shares
            try:
                order_result = self._place_order(
                    symbol=symbol,
                    action=action,
                    quantity=abs(delta_qty),
                    market=True,
                )
                if order_result.is_success():
                    results['successes'].append(
                        f'{symbol}: {action} {abs(delta_qty)} shares'
                    )
                else:
                    results['failures'].append(
                        f'{symbol}: {action} {abs(delta_qty)} failed — {order_result.error}'
                    )
                    continue  # Skip re-creating protectives if delta order failed
            except Exception as ex:
                results['failures'].append(f'{symbol}: {action} {abs(delta_qty)} error — {ex}')
                continue

            # 3. Re-create protective orders with new quantity
            for order_info in associated:
                try:
                    contract = self._resolve_contract(symbol)
                    new_qty = abs(target_qty)

                    consume(
                        self._rpc.rpc(return_type=SuccessFail[Trade]).place_standalone_order(
                            contract=contract,
                            action=order_info['action'],
                            quantity=new_qty,
                            order_type=order_info['orderType'],
                            aux_price=order_info['auxPrice'],
                            limit_price=order_info['lmtPrice'],
                            trailing_percent=order_info['trailingPercent'],
                            tif=order_info['tif'],
                        )
                    )
                    results['successes'].append(
                        f'{symbol}: re-created {order_info["orderType"]} {order_info["action"]} {new_qty}'
                    )
                except Exception as ex:
                    results['warnings'].append(
                        f'{symbol}: failed to re-create {order_info["orderType"]}: {ex}'
                    )

        return results

    # ------------------------------------------------------------------
    # Market Data
    # ------------------------------------------------------------------

    def snapshot(self, symbol: Union[str, int], delayed: bool = False,
                 exchange: str = '', currency: str = '') -> dict:
        """Get a price snapshot for *symbol*."""
        contract = self._resolve_contract(symbol, exchange=exchange, currency=currency)
        ticker = consume(
            self._rpc.rpc(return_type=Ticker).get_snapshot(contract, delayed)
        )

        # Handle deserialized ticker — contract may be dict/list after msgpack round-trip
        sym = ''
        con_id = ''
        tc = getattr(ticker, 'contract', None)
        if tc:
            if isinstance(tc, dict):
                sym = tc.get('symbol', '')
                con_id = tc.get('conId', '')
            elif hasattr(tc, 'symbol'):
                sym = tc.symbol
                con_id = tc.conId

        return {
            'symbol': sym,
            'conId': con_id,
            'time': getattr(ticker, 'time', None),
            'bid': getattr(ticker, 'bid', float('nan')),
            'bidSize': getattr(ticker, 'bidSize', float('nan')),
            'ask': getattr(ticker, 'ask', float('nan')),
            'askSize': getattr(ticker, 'askSize', float('nan')),
            'last': getattr(ticker, 'last', float('nan')),
            'lastSize': getattr(ticker, 'lastSize', float('nan')),
            'open': getattr(ticker, 'open', float('nan')),
            'high': getattr(ticker, 'high', float('nan')),
            'low': getattr(ticker, 'low', float('nan')),
            'close': getattr(ticker, 'close', float('nan')),
            'halted': getattr(ticker, 'halted', float('nan')),
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

    def get_risk_limits(self) -> dict:
        """Get current risk gate limits from trader_service."""
        return consume(self._rpc.rpc(return_type=dict).get_risk_limits())

    def set_risk_limits(self, **kwargs) -> dict:
        """Update risk gate limits on trader_service. Returns updated limits."""
        return consume(self._rpc.rpc(return_type=dict).set_risk_limits(**kwargs))

    # ------------------------------------------------------------------
    # Trading filters (local YAML, no RPC needed)
    # ------------------------------------------------------------------

    def get_filters(self) -> dict:
        """Load current trading filters from YAML config."""
        from trader.trading.trading_filter import TradingFilter
        return TradingFilter.load().to_dict()

    def set_filters(self, **kwargs) -> dict:
        """Update specific filter fields and save."""
        from trader.trading.trading_filter import TradingFilter
        tf = TradingFilter.load()
        for key, value in kwargs.items():
            if hasattr(tf, key):
                setattr(tf, key, value)
        tf.save()
        return tf.to_dict()

    def add_to_filter_list(self, list_name: str, symbols: list[str]) -> dict:
        """Add symbols to a named list (denylist, allowlist, etc.)."""
        from trader.trading.trading_filter import TradingFilter
        tf = TradingFilter.load()
        current = getattr(tf, list_name, [])
        for sym in symbols:
            s = sym.upper()
            if s not in current:
                current.append(s)
        setattr(tf, list_name, current)
        tf.save()
        return tf.to_dict()

    def remove_from_filter_list(self, list_name: str, symbols: list[str]) -> dict:
        """Remove symbols from a named list."""
        from trader.trading.trading_filter import TradingFilter
        tf = TradingFilter.load()
        current = getattr(tf, list_name, [])
        upper_syms = {s.upper() for s in symbols}
        setattr(tf, list_name, [s for s in current if s.upper() not in upper_syms])
        tf.save()
        return tf.to_dict()

    def reset_filters(self) -> dict:
        """Reset all filters to defaults."""
        from trader.trading.trading_filter import TradingFilter
        tf = TradingFilter.default()
        tf.save()
        return tf.to_dict()

    def status(self) -> dict:
        """Check connectivity to trader_service via ZMQ RPC with diagnostics."""
        try:
            acct = consume(self._rpc.rpc(return_type=str).get_ib_account())
        except Exception:
            return {'connected': False}

        result = {'connected': True, 'account': acct}

        try:
            acct_vals = consume(
                self._rpc.rpc(return_type=dict).get_account_values()
            )
            if acct_vals:
                for key in ('NetLiquidation', 'TotalCashValue', 'AvailableFunds', 'BuyingPower'):
                    entry = acct_vals.get(key)
                    if entry:
                        result[key] = f"${float(entry['value']):,.2f} {entry['currency']}"

                # Leverage / margin info
                net_liq_entry = acct_vals.get('NetLiquidation')
                gross_pos_entry = acct_vals.get('GrossPositionValue')
                init_margin_entry = acct_vals.get('InitMarginReq')
                cushion_entry = acct_vals.get('Cushion')

                if net_liq_entry and gross_pos_entry:
                    net_liq = float(net_liq_entry['value'])
                    gross_pos = float(gross_pos_entry['value'])
                    if net_liq > 0:
                        result['Leverage'] = f'{gross_pos / net_liq:.2f}x'

                if init_margin_entry and net_liq_entry:
                    init_margin = float(init_margin_entry['value'])
                    net_liq = float(net_liq_entry['value'])
                    result['MarginUsed'] = f'${init_margin:,.2f} / ${net_liq:,.2f}'

                if cushion_entry:
                    result['Cushion'] = cushion_entry['value']
        except Exception:
            pass

        try:
            portfolio = consume(
                self._rpc.rpc(return_type=list[PortfolioItem]).get_portfolio()
            )
            positions = [p for p in (portfolio or []) if abs(p.position) > 0]
            result['positions'] = len(positions)
            result['unrealized_pnl'] = sum(p.unrealizedPNL for p in positions)
            result['realized_pnl'] = sum(p.realizedPNL for p in positions)
        except Exception:
            result['positions'] = '?'

        try:
            orders = consume(
                self._rpc.rpc(return_type=dict[int, list[Order]]).get_orders()
            )
            result['open_orders'] = sum(len(v) for v in (orders or {}).values())
        except Exception:
            result['open_orders'] = '?'

        try:
            pubs = consume(
                self._rpc.rpc(return_type=list[int]).get_published_contracts()
            )
            result['streaming'] = len(pubs or [])
        except Exception:
            pass

        try:
            store = self._proposal_store()
            pending = store.query(status='PENDING')
            result['pending_proposals'] = len(pending)
        except Exception:
            pass

        return result

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

    def movers_detail(
        self,
        market: str = 'stocks',
        direction: str = 'gainers',
        num: int = 20,
    ) -> list[dict]:
        """Get movers enriched with company name, ratios, and news."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        snaps = self._massive_client.get_snapshot_direction(
            market_type=market, direction=direction,
        )

        # Build base data from snapshots
        movers = []
        for snap in snaps[:num]:
            ticker = snap.ticker or ''
            if not ticker:
                continue
            row = {
                'ticker': ticker,
                'open': getattr(snap.day, 'open', None) if snap.day else None,
                'close': getattr(snap.day, 'close', None) if snap.day else None,
                'volume': getattr(snap.day, 'volume', None) if snap.day else None,
                'change': snap.todays_change,
                'change_pct': snap.todays_change_percent,
            }
            movers.append(row)

        if not movers:
            return []

        tickers = [m['ticker'] for m in movers]

        # Parallel fetch: ticker details, ratios, news
        details_map = {}
        ratios_map = {}
        news_map = {}

        def fetch_details(t):
            try:
                d = self._massive_client.get_ticker_details(t)
                return (t, {'name': d.name, 'market_cap': d.market_cap, 'description': d.description})
            except Exception:
                return (t, {})

        def fetch_ratios(t):
            try:
                results = list(self._massive_client.list_financials_ratios(ticker=t, limit=1))
                if not results:
                    return (t, {})
                r = results[0]
                data = {}
                for attr, label in [
                    ('price_to_earnings', 'pe'), ('debt_to_equity', 'de'),
                    ('return_on_equity', 'roe'), ('earnings_per_share', 'eps'),
                    ('dividend_yield', 'div_yield'),
                ]:
                    val = getattr(r, attr, None)
                    if val is not None:
                        data[label] = round(float(val), 2)
                return (t, data)
            except Exception:
                return (t, {})

        def fetch_news(t):
            try:
                articles = list(self._massive_client.list_ticker_news(ticker=t, limit=1))
                if not articles:
                    return (t, {})
                a = articles[0]
                sentiment = ''
                if a.insights:
                    sentiments = [i.sentiment for i in a.insights if i.sentiment]
                    sentiment = ', '.join(sentiments)
                return (t, {'headline': a.title, 'sentiment': sentiment})
            except Exception:
                return (t, {})

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = []
            for t in tickers:
                futures.append(pool.submit(fetch_details, t))
                futures.append(pool.submit(fetch_ratios, t))
                futures.append(pool.submit(fetch_news, t))

            for future in as_completed(futures):
                ticker, data = future.result()
                fn = future._args[0] if hasattr(future, '_args') else ''
                # Determine which map to update based on keys
                if 'name' in data:
                    details_map[ticker] = data
                elif 'headline' in data:
                    news_map[ticker] = data
                elif data and 'name' not in data and 'headline' not in data:
                    ratios_map[ticker] = data

        # Merge into results
        for m in movers:
            t = m['ticker']
            m['details'] = details_map.get(t, {})
            m['ratios'] = ratios_map.get(t, {})
            m['news'] = news_map.get(t, {})

        return movers

    def scan_ideas(
        self,
        preset: str = 'momentum',
        source: str = 'movers',
        tickers: Optional[List[str]] = None,
        universe: Optional[str] = None,
        top_n: int = 15,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        min_volume: Optional[int] = None,
        min_change_pct: Optional[float] = None,
        max_change_pct: Optional[float] = None,
        fundamentals: bool = False,
        news: bool = False,
        names: bool = False,
        location: Optional[str] = None,
    ) -> pd.DataFrame:
        """Scan for trading ideas using preset-based scoring.

        Parameters
        ----------
        preset : str
            Scoring preset (momentum, gap-up, gap-down, mean-reversion, breakout, volatile).
        source : str
            'movers' (default), 'tickers', or 'universe'.
        tickers : list of str, optional
            Explicit ticker list (when source='tickers').
        universe : str, optional
            Universe name to scan (when source='universe').
        top_n : int
            Max results to return.
        min_price, max_price, min_volume, min_change_pct, max_change_pct
            Override preset filter defaults.
        fundamentals : bool
            If True, enrich results with financial ratios (PE, D/E, ROE, etc.).
        news : bool
            If True, enrich results with latest news headline and sentiment.
        location : str, optional
            IB market location code (e.g. STK.AU.ASX, STK.CA). When set, uses
            IB scanner API instead of Massive.com (for international markets).
        """
        # Build custom filter overrides
        custom_filters = {}
        if min_price is not None:
            custom_filters['min_price'] = min_price
        if max_price is not None:
            custom_filters['max_price'] = max_price
        if min_volume is not None:
            custom_filters['min_volume'] = min_volume
        if min_change_pct is not None:
            custom_filters['min_change_pct'] = min_change_pct
        if max_change_pct is not None:
            custom_filters['max_change_pct'] = max_change_pct

        # IB path: use IBIdeaScanner for international markets
        if location:
            from trader.tools.idea_scanner import IBIdeaScanner

            # Resolve universe to symbol list for IB path
            ib_universe_symbols = None
            if source == 'universe' and universe:
                from trader.data.universe import UniverseAccessor
                cfg = self._container.config()
                accessor = UniverseAccessor(
                    cfg.get('duckdb_path', ''),
                    cfg.get('universe_library', 'Universes'),
                )
                u = accessor.get(universe)
                if u.security_definitions:
                    ib_universe_symbols = [d.symbol for d in u.security_definitions]
                else:
                    return pd.DataFrame()

            scanner = IBIdeaScanner(self._rpc)
            return scanner.scan(
                preset=preset,
                location=location,
                top_n=top_n,
                custom_filters=custom_filters or None,
                fundamentals=fundamentals,
                news=news,
                tickers=tickers if source == 'tickers' else None,
                universe_symbols=ib_universe_symbols,
            )

        # Massive path (default): US markets
        from trader.tools.idea_scanner import IdeaScanner

        # Resolve universe to symbol list
        universe_symbols = None
        if source == 'universe' and universe:
            from trader.data.universe import UniverseAccessor
            cfg = self._container.config()
            accessor = UniverseAccessor(
                cfg.get('duckdb_path', ''),
                cfg.get('universe_library', 'Universes'),
            )
            u = accessor.get(universe)
            if u.security_definitions:
                universe_symbols = [d.symbol for d in u.security_definitions]
            else:
                return pd.DataFrame()

        scanner = IdeaScanner(self._massive_client)
        return scanner.scan(
            preset=preset,
            source=source,
            tickers=tickers,
            universe_symbols=universe_symbols,
            top_n=top_n,
            custom_filters=custom_filters or None,
            fundamentals=fundamentals,
            news=news,
            names=names,
        )

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
        # Check location against trading filters
        from trader.trading.trading_filter import TradingFilter
        tf = TradingFilter.load()
        if not tf.is_empty():
            allowed, reason = tf.is_allowed('', location=location_code)
            if not allowed:
                raise ValueError(f'Trading filter blocked location {location_code}: {reason}')
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

    # ------------------------------------------------------------------
    # Market hours (local-only, no service needed)
    # ------------------------------------------------------------------

    _MARKET_CALENDARS = [
        ('XNYS',  'NYSE',       'US'),
        ('XNAS',  'NASDAQ',     'US'),
        ('XASX',  'ASX',        'Australia'),
        ('XTSE',  'TSX',        'Canada'),
        ('XLON',  'LSE',        'UK'),
        ('XHKG',  'HKEX',       'Hong Kong'),
        ('XTKS',  'TSE',        'Japan'),
        ('XFRA',  'Frankfurt',  'Germany'),
        ('XPAR',  'Euronext',   'France'),
    ]

    def market_hours(self) -> List[Dict]:
        """Return open/close status for major exchanges. No RPC needed."""
        import exchange_calendars as xcals

        now = pd.Timestamp.now(tz='UTC')
        today = pd.Timestamp(now.date())  # tz-naive date for session lookups
        results = []

        for cal_code, name, region in self._MARKET_CALENDARS:
            cal = xcals.get_calendar(cal_code)
            is_open = cal.is_open_on_minute(now)
            status = 'OPEN' if is_open else 'CLOSED'

            if is_open:
                # Next event is market close
                session = cal.minute_to_session(now)
                next_event_time = cal.session_close(session)
                next_event = 'closes'
            else:
                # Next event is market open — search upcoming sessions
                next_event_time = None
                next_event = 'opens'
                try:
                    for offset in range(5):
                        candidate = today + pd.Timedelta(days=offset)
                        if cal.is_session(candidate):
                            open_time = cal.session_open(candidate)
                            if open_time > now:
                                next_event_time = open_time
                                break
                except Exception:
                    pass

            if next_event_time is not None:
                delta = next_event_time - now
                total_minutes = int(delta.total_seconds() // 60)
                hours, minutes = divmod(total_minutes, 60)
                if hours > 0:
                    relative = f'in {hours}h {minutes}m'
                else:
                    relative = f'in {minutes}m'
            else:
                relative = ''

            results.append({
                'exchange': name,
                'region': region,
                'status': status,
                'next_event': next_event,
                'next_event_time': str(next_event_time)[:19] if next_event_time else '',
                'relative': relative,
            })

        return results
