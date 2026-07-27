from ib_async.contract import Contract
from ib_async.objects import PnLSingle, PortfolioItem, Position
from ib_async.order import LimitOrder, MarketOrder, Order, StopLimitOrder, StopOrder, Trade
from ib_async.ticker import Ticker
from reactivex import pipe
from reactivex.abc import DisposableBase, ObserverBase
from reactivex.disposable import Disposable
from reactivex.observable import Observable
from reactivex.observer import AutoDetachObserver, Observer
from reactivex.scheduler.eventloop.asynciothreadsafescheduler import AsyncIOThreadSafeScheduler
from reactivex.subject import Subject
from trader.common.contract_sink import ContractSink
from trader.common.dataclass_cache import DataClassCache, DataClassEvent, UpdateEvent
from trader.common.exceptions import trader_exception, TraderConnectionException, TraderException
from trader.common.helpers import ListHelper
from trader.common.logging_helper import get_callstack, log_method, setup_logging
from trader.common.reactivex import AnonymousObserver, SuccessFail

from trader.data.data_access import PortfolioSummary, SecurityDefinition, TickStorage
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.data.market_data import SecurityDataStream
from trader.data.universe import Universe, UniverseAccessor
from trader.trading.approved_order import ExitReason, mint_approved_order
from trader.trading.exit_class import reduces_exposure
from trader.trading.order_split import split_order
from trader.trading.order_structure import rejection_for_order
from trader.trading.risk_gate import RiskGate, RiskInputs, RiskLimits
from trader.listeners.ibreactive import IBAIORx, IBAIORxError
from trader.messaging.clientserver import MessageBusServer, MultithreadedTopicPubSub, RPCClient, RPCServer
from trader.objects import Action, ContractOrderPair, ExecutorCondition
from trader.trading.book import BookSubject
from trader.trading.executioner import TradeExecutioner
from trader.trading.portfolio import Portfolio
from trader.trading.strategy import Strategy, StrategyConfig, StrategyState
from typing import Any, cast, Dict, List, NamedTuple, Optional, Tuple, Union

import asyncio
import backoff
import datetime as dt
import hmac
import math
import os
import reactivex as rx
import reactivex.operators as ops
import threading
import time
import trader.messaging.strategy_service_api as strategy_bus
import trader.messaging.trader_service_api as bus


logging = setup_logging(module_name='trading_runtime')

# notes
# https://groups.io/g/insync/topic/using_reqallopenorders/27261173?p=,,,20,0,0,0::recentpostdate%2Fsticky,,,20,2,0,27261173
# talks about trades/orders being tied to clientId, which means we'll need to always have a consistent clientid


class AccountNotPinnedError(Exception):
    """The trader is not pinned to a valid, mode-matched IB account.

    Raised at connect time to refuse trading when ``ib_account`` is blank,
    not among IB's ``managedAccounts()``, or mismatched with the trading
    mode — any of which could let orders route to the wrong account on a
    multi-account login. A hard, fatal refusal (not retried).
    """


class Trader():
    def __init__(self,
                 ib_server_address: str,
                 ib_server_port: int,
                 trading_runtime_ib_client_id: int,
                 ib_account: str,
                 duckdb_path: str,
                 universe_library: str,
                 zmq_pubsub_server_address: str,
                 zmq_pubsub_server_port: int,
                 zmq_rpc_server_address: str,
                 zmq_rpc_server_port: int,
                 zmq_strategy_rpc_server_address: str,
                 zmq_strategy_rpc_server_port: int,
                 zmq_messagebus_server_address: str,
                 zmq_messagebus_server_port: int,
                 history_duckdb_path: str = '',
                 paper_trading: bool = False,
                 simulation: bool = False,
                 require_proposal_approval: bool = False,
                 approver_required_above_usd: float = 0.0,
                 approver_key: str = ''):
        self.ib_server_address = ib_server_address
        self.ib_server_port = ib_server_port
        self.trading_runtime_ib_client_id = trading_runtime_ib_client_id
        self.ib_account = ib_account
        self.duckdb_path = duckdb_path
        self.history_duckdb_path = history_duckdb_path or duckdb_path
        self.universe_library = universe_library
        self.simulation: bool = simulation
        self.paper_trading = paper_trading
        # When True, `place_order_simple` (the direct buy/sell RPC path) is
        # rejected unless the order is exit-class (it reduces the live broker
        # position — see order_reduces_exposure). All actionable new trades
        # must come in through `place_expressive_order`, which the approve()
        # CLI / helper use after a proposal is reviewed. Defensive gate
        # against LLM loops drifting off-plan and firing direct orders.
        self.require_proposal_approval: bool = require_proposal_approval
        # Server-side notional tier (Phase 2 proposer/approver split). When
        # approver_required_above_usd > 0, an exposure-INCREASING order whose
        # SERVER-RECOMPUTED notional exceeds the threshold requires a matching
        # approver_key (constant-time compared). 0 => feature OFF (default,
        # byte-identical to prior behaviour). The auto-executor is kept BELOW
        # the threshold by position sizing, so it carries no key and is
        # unaffected. Exit-class orders NEVER hit this gate — a close must
        # never need a key. The secret's canonical source is the process env
        # (MMR_APPROVER_KEY) which wins over YAML — YAML / .config / compose
        # env leak to a same-container proposer, so the operator delivers the
        # key out of band (a typed --approver-key or a services-only
        # secrets.env), never on a surface the proposer can read.
        self.approver_required_above_usd: float = approver_required_above_usd
        _env_key = os.environ.get('MMR_APPROVER_KEY')
        self.approver_key: str = _env_key if _env_key is not None else (approver_key or '')
        self.zmq_pubsub_server_address = zmq_pubsub_server_address
        self.zmq_pubsub_server_port = zmq_pubsub_server_port
        self.zmq_rpc_server_address = zmq_rpc_server_address
        self.zmq_rpc_server_port = zmq_rpc_server_port
        self.zmq_strategy_rpc_server_address = zmq_strategy_rpc_server_address
        self.zmq_strategy_rpc_server_port = zmq_strategy_rpc_server_port
        self.zmq_messagebus_server_address = zmq_messagebus_server_address
        self.zmq_messagebus_server_port = zmq_messagebus_server_port

        # todo you can have up to 24 connections to IB Gateway
        # so we need to take this from single client, to multiple client
        self.client: IBAIORx
        self.data: TickStorage
        self.universe_accessor: UniverseAccessor

        # the live ticker data streams we have
        self.contract_subscriptions: Dict[Contract, ContractSink] = {}
        # the minute-by-minute MarketData stream's we're subscribed to
        self.market_data_subscriptions: Dict[SecurityDefinition, SecurityDataStream] = {}

        # current order book (outstanding orders, trades etc)
        self.book: BookSubject = BookSubject()
        # portfolio (current and past positions)
        self.portfolio: Portfolio = Portfolio()
        # In-memory set of conIds already present in the 'portfolio' universe.
        # Populated lazily on the first update_portfolio_universe() call
        # per session. Lets a burst of N positionEvent emissions (49 on
        # initial connect) short-circuit without 49× DuckDB reads when
        # nothing's actually changing. Huge win: the old path did one
        # `universe_accessor.get + dill.dumps + DuckDB write` per event
        # on the main loop.
        self._known_portfolio_conids: set = set()
        # pnl for current portfolio
        self.pnl: DataClassCache = DataClassCache[PnLSingle](lambda pnl: str((pnl.account, pnl.conId)))
        self.pnl_subscriptions: Dict[Tuple[str, int], bool] = {}
        self._pnl_subscriptions_lock: threading.Lock = threading.Lock()
        # The main event loop, captured on connect(). Used when IB callbacks
        # fire on threads other than the loop thread.
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        # Risk gate — constructed in connect() before the RPC server serves.
        # Declared None here so gate consumers use hard attribute access and a
        # missing gate fails CLOSED (non-exit-class orders refused), never open.
        self.risk_gate: Optional[RiskGate] = None
        # takes care of execution of orders
        self.executioner: TradeExecutioner
        # a list of all the universes of stocks we have registered
        self.market_data = 3
        self.zmq_rpc_server: RPCServer[bus.TraderServiceApi]
        self.zmq_pubsub_server: MultithreadedTopicPubSub
        self.zmq_pubsub_contracts: Dict[int, Observable[IBAIORxError]] = {}
        self.zmq_pubsub_contract_filters: Dict[int, bool] = {}
        self.zmq_pubsub_contract_subscription: DisposableBase = Disposable()
        # conId -> (Contract, delayed). Remembered so live ticker subscriptions
        # can be re-established after an IB reconnect (the old market-data lines
        # die with the previous session). Survives reconnects; reset on a fresh
        # connect(). Without this the strategy tick feed silently dies on reconnect.
        self.zmq_pubsub_published_contracts: Dict[int, Tuple[Contract, bool]] = {}
        # Coalesces the connected_event double-fire (eventkit emit + explicit call
        # on reconnect) so re-subscription doesn't run twice concurrently.
        self._in_connected_event: bool = False
        # Single sink for IB order-status truth (fill/cancel/reject events +
        # acceptance queries). Built here so it exists before the first
        # connected_event/setup_subscriptions runs; its event store is wired in
        # connect() and it's attached to orderStatusEvent in setup_subscriptions.
        from trader.trading.order_lifecycle import OrderLifecycleTracker
        self.order_tracker = OrderLifecycleTracker(None)

        self.zmq_strategy_client: RPCClient[strategy_bus.StrategyServiceApi]
        self.zmq_messagebus: MessageBusServer

        self.startup_time: dt.datetime = dt.datetime.now()
        self.last_connect_time: dt.datetime
        self.load_test: bool = False
        self.tws_client_ids: List[int] = [self.trading_runtime_ib_client_id, self.trading_runtime_ib_client_id + 1]
        self.scheduler: Optional[AsyncIOThreadSafeScheduler] = None

        # IB upstream connectivity tracking
        # These error codes indicate Gateway is connected locally but lost upstream IBKR connection
        self._ib_upstream_connected: bool = True
        self._ib_upstream_error: str = ''

        self.disposables: List[DisposableBase] = []

    def _assert_account_pinned(self, managed: list) -> str:
        """Verify the trader is pinned to exactly one configured account that
        IB actually manages, matching the trading mode. Returns the active
        account or raises ``AccountNotPinnedError``.

        This is the startup half of the account-safety story (the per-order
        half is the guard in ``TradeExecutioner.subscribe_place_order_direct``).
        The IB login can manage multiple accounts (e.g. a client sub-account
        plus a master/aggregate). If ``ib_account`` were blank, every order
        would be built with ``account=''`` and IB would route it to the
        *default* account — potentially the wrong one. So we refuse to start
        unless ``ib_account`` is non-empty AND present in ``managedAccounts()``.
        Paper accounts start with "D" (DU.../DF...); live accounts don't.
        """
        mode = 'paper' if self.paper_trading else 'live'
        if not self.ib_account:
            raise AccountNotPinnedError(
                f'SAFETY: no ib_account configured (trading_mode={mode}, managed={managed}). '
                'Set ib_paper_account / ib_live_account in trader.yaml or the IB_ACCOUNT env var. '
                'Refusing to continue.'
            )
        if not managed:
            raise AccountNotPinnedError(
                f'SAFETY: IB returned no managed accounts; cannot verify ib_account '
                f'"{self.ib_account}". Refusing to continue.'
            )
        if self.ib_account not in managed:
            raise AccountNotPinnedError(
                f'SAFETY: configured ib_account "{self.ib_account}" is not among IB managed '
                f'accounts {managed}. Refusing to continue.'
            )
        is_paper_account = self.ib_account.startswith('D')
        if self.paper_trading and not is_paper_account:
            raise AccountNotPinnedError(
                f'SAFETY: trading_mode is "paper" but ib_account "{self.ib_account}" looks live. '
                f'Managed accounts: {managed}. Refusing to continue.'
            )
        if not self.paper_trading and is_paper_account:
            raise AccountNotPinnedError(
                f'SAFETY: trading_mode is "live" but ib_account "{self.ib_account}" looks like a '
                f'paper account. Managed accounts: {managed}. Check your config.'
            )
        return self.ib_account

    @backoff.on_exception(backoff.expo, (ConnectionRefusedError, TimeoutError), max_tries=10, max_time=120)
    def connect(self):
        logging.debug('trading_runtime.connect() connecting to services: %s:%s' % (self.ib_server_address, self.ib_server_port))
        try:
            self.client = IBAIORx(
                ib_server_address=self.ib_server_address,
                ib_server_port=self.ib_server_port,
                ib_client_id=self.trading_runtime_ib_client_id,
                ib_account=self.ib_account,
            )
            self.data = TickStorage(self.history_duckdb_path)
            self.universe_accessor = UniverseAccessor(self.duckdb_path, self.universe_library)
            self.clear_portfolio_universe()
            self.contract_subscriptions = {}
            self.market_data_subscriptions = {}
            self.client.ib.connectedEvent += self.connected_event
            self.client.ib.disconnectedEvent += self.disconnected_event
            self.client.connect()

            # Track IB upstream connectivity via error codes
            self.client.error_subject.subscribe(AnonymousObserver(
                on_next=self._on_ib_error,
                on_error=lambda e: None,
            ))

            # Hard safety gate: refuse to run unless we're pinned to exactly
            # one configured account that IB actually manages (and it matches
            # the trading mode). See _assert_account_pinned for the rationale.
            managed = self.client.ib.managedAccounts()
            try:
                active_account = self._assert_account_pinned(managed)
            except AccountNotPinnedError:
                self.client.ib.disconnect()
                raise
            logging.info('trading mode verified: %s, account: %s', 'paper' if self.paper_trading else 'live', active_account)

            self.last_connect_time = dt.datetime.now()
            self.zmq_rpc_server = RPCServer[bus.TraderServiceApi](
                instance=bus.TraderServiceApi(self),
                zmq_rpc_server_address=self.zmq_rpc_server_address,
                zmq_rpc_server_port=self.zmq_rpc_server_port
            )
            self.zmq_pubsub_server = MultithreadedTopicPubSub(
                zmq_pubsub_server_address=self.zmq_pubsub_server_address,
                zmq_pubsub_server_port=self.zmq_pubsub_server_port
            )
            self.zmq_pubsub_server.start()

            self.zmq_messagebus = MessageBusServer(self.zmq_messagebus_server_address, self.zmq_messagebus_server_port)
            self.run(self.zmq_messagebus.start())

            self.zmq_pubsub_contracts = {}
            self.zmq_pubsub_contract_filters = {}
            self.zmq_pubsub_contract_subscription = Disposable()

            # connect to the strategy server
            self.zmq_strategy_client = RPCClient[strategy_bus.StrategyServiceApi](
                self.zmq_strategy_rpc_server_address,
                self.zmq_strategy_rpc_server_port,
                timeout=6,
            )

            # initialize event store and risk gate
            self.event_store = EventStore(self.duckdb_path)
            self.risk_gate = RiskGate(RiskLimits.load(), self.event_store)

            # The order-lifecycle tracker itself is built in __init__ (so it
            # exists before the first connected_event runs); wire its event store
            # now that it's available.
            self.order_tracker.set_event_store(self.event_store)

            # load trading filters (allowlist/denylist)
            from trader.trading.trading_filter import TradingFilter
            self.risk_gate.trading_filter = TradingFilter.load()

            # Server-side notional-tier startup diagnostics (Phase 2). Announce
            # the active threshold and flag the operational footgun where the
            # position sizer's own cap could size an auto-executor trade ABOVE
            # the threshold (which would then be refused for lack of a key).
            if self.approver_required_above_usd > 0:
                logging.info(
                    'approver notional tier ACTIVE: opens above $%.2f require an '
                    'approver key (key source: %s; exits always exempt)',
                    self.approver_required_above_usd,
                    'env MMR_APPROVER_KEY' if os.environ.get('MMR_APPROVER_KEY') is not None
                    else ('yaml' if self.approver_key else 'UNSET — all above-threshold opens will be refused'))
                # The tier now gates ALL exposure-increasing server paths
                # (approve AND direct buy/sell). Flag the split when the
                # proposer/approver separation isn't also enforced: a single
                # context can then both size the open and supply the key, which
                # the notional tier alone can't prevent.
                if not self.require_proposal_approval:
                    logging.warning(
                        'approver notional tier is ACTIVE but require_proposal_approval '
                        'is OFF — the direct buy/sell order path is tier-gated, yet the '
                        'proposer/approver split is NOT enforced. A large open is refused '
                        'without a key on every path, but nothing stops one context from '
                        'both proposing and supplying the key. Set require_proposal_approval: '
                        'true to enforce the split.')
                try:
                    from trader.trading.position_sizing import PositionSizingConfig
                    _sizing_max = PositionSizingConfig.load().max_position_usd
                    if _sizing_max > self.approver_required_above_usd:
                        logging.warning(
                            'position sizing max_position_usd ($%.2f) EXCEEDS the approver '
                            'threshold ($%.2f) — an auto-executor trade sized above the '
                            'threshold would be refused (no key). Set the threshold ABOVE '
                            'the auto-executor max sized notional.',
                            _sizing_max, self.approver_required_above_usd)
                except Exception as ex:
                    logging.warning('could not load position sizing config for approver-tier check: %s', ex)

            # fire up the executioner
            self.executioner = TradeExecutioner()
            self.executioner.connect(self)

            self.run(self.zmq_strategy_client.connect())
            self.run(self.zmq_rpc_server.serve())

        except KeyboardInterrupt:
            logging.info('connect() interrupted, shutting down')
            raise
        except (ConnectionRefusedError, TimeoutError):
            # Propagate un-wrapped so the @backoff.on_exception decorator on
            # connect() actually retries them. The old blanket `except Exception`
            # rewrapped these as TraderConnectionException, which backoff doesn't
            # match — so the retry decorator was dead code.
            raise
        except AccountNotPinnedError:
            # Fatal safety refusal — must never be retried or wrapped.
            raise
        except Exception as ex:
            raise trader_exception(self, TraderConnectionException, message='trading_runtime connect() exception', inner=ex)

    @log_method
    async def shutdown(self):
        self.client.ib.connectedEvent -= self.connected_event
        self.client.ib.disconnectedEvent -= self.disconnected_event
        self.client.ib.disconnect()

        for contract, sink in self.contract_subscriptions.items():
            sink.dispose()

        self.zmq_pubsub_contract_subscription.dispose()

        # for security_definition, security_datastream in self.market_data_subscriptions.items():
        #   security_datastream.dispose()

        for disposable in self.disposables:
            disposable.dispose()

        self.book.dispose()
        await self.client.shutdown()

    @log_method
    def reconnect(self):
        # this will force a reconnect through the disconnected event
        self.client.ib.disconnect()

    def __update_positions(self, positions: Union[List[Position], Position]):
        logging.debug('__update_positions')
        if type(positions) is Position:
            self.portfolio.add_position(positions)
        elif type(positions) is list:
            for position in positions:
                self.portfolio.add_position(position)

    def __update_portfolio(self, portfolio_item: PortfolioItem):
        logging.debug('__update_portfolio')
        self.portfolio.add_portfolio_item(portfolio_item=portfolio_item)
        # Schedule the async universe update onto the trader's main loop even
        # when this callback fires on an IB/eventkit thread. The old code fell
        # back to a *synchronous* disk-IO path in that case, which blocked
        # every other IB event on the callback thread.
        coro = self.update_portfolio_universe(portfolio_item)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coro)
            return
        except RuntimeError:
            pass

        main_loop = self._main_loop
        if main_loop is not None and main_loop.is_running():
            asyncio.run_coroutine_threadsafe(coro, main_loop)
        else:
            # Truly no loop available (shutdown path, tests). Fall back to the
            # sync version but close the coroutine to avoid "never awaited".
            coro.close()
            self._update_portfolio_universe_sync(portfolio_item)

    def __dataclass_server_put(self, message: DataClassEvent):
        # logging.debug('__dataclass_server_put: {}'.format(message))
        self.zmq_pubsub_server.put(('dataclass', message))

    @log_method
    async def setup_subscriptions(self):
        if not self.is_ib_connected():
            raise ConnectionError('not connected to interactive brokers')

        def handle_subscription_exception(ex):
            exception = trader_exception(self, TraderException, message='setup_subscriptions()', inner=ex)
            raise exception

        def handle_completed():
            logging.debug('handle_completed()')

        # have the book subscribe to all relevant trade events
        await self.book.subscribe_to_eventkit_event(
            [
                self.client.ib.orderStatusEvent,
                self.client.ib.orderModifyEvent,
                self.client.ib.newOrderEvent,
                self.client.ib.cancelOrderEvent,
                self.client.ib.openOrderEvent,
            ]
        )

        # Feed the order-lifecycle tracker directly from the CURRENT ib's
        # orderStatusEvent. Use connect(keep_ref=True) — eventkit defaults to a
        # WEAK reference, which silently drops a freshly-bound method handler; a
        # strong ref guarantees delivery. disconnect-then-connect keeps exactly
        # one registration across reconnects (the ib instance is fresh each time).
        if getattr(self, 'order_tracker', None) is not None:
            _ev = self.client.ib.orderStatusEvent
            try:
                _ev.disconnect(self.order_tracker.on_trade)
            except Exception:
                pass
            _ev.connect(self.order_tracker.on_trade, keep_ref=True)
            logging.info('order-lifecycle tracker attached to orderStatusEvent')

        positions_observer = Observer(
            on_next=self.__update_positions,
            on_error=handle_subscription_exception,
            on_completed=handle_completed
        )

        positions_disposable = (await self.client.subscribe_positions()).subscribe(positions_observer)
        self.disposables.append(positions_disposable)

        portfolio_disposable = (await self.client.subscribe_portfolio()).subscribe(AnonymousObserver(
            on_next=self.__update_portfolio,
            on_error=handle_subscription_exception,
        ))
        self.disposables.append(portfolio_disposable)

        # subscribe to all portfolio changes, then make sure we're subscribing to the pnl for each
        def __subscribe_pnl(portfolio_item: PortfolioItem):
            async def __async_subscribe_pnl(portfolio_item: PortfolioItem):
                if not portfolio_item.contract:
                    return
                key = (portfolio_item.account, portfolio_item.contract.conId)
                # Atomic "first claim wins" — prevents two concurrent portfolio
                # events from both crossing the earlier check-then-act gap and
                # leaking duplicate PnL subscriptions on reconnect.
                with self._pnl_subscriptions_lock:
                    if key in self.pnl_subscriptions:
                        return
                    self.pnl_subscriptions[key] = True

                try:
                    observable = await self.client.subscribe_single_pnl(
                        portfolio_item.contract,
                    )
                    disposable = observable.subscribe(
                        self.pnl.create_observer(error_func=handle_subscription_exception),
                    )
                    self.disposables.append(disposable)
                except Exception as ex:
                    # Back out the registry entry so a retry can re-attempt.
                    with self._pnl_subscriptions_lock:
                        self.pnl_subscriptions.pop(key, None)
                    logging.warning(f'Failed to subscribe PnL for {portfolio_item.contract}: {ex}')

            try:
                loop = asyncio.get_running_loop()
                loop.create_task(__async_subscribe_pnl(portfolio_item))
                return
            except RuntimeError:
                pass

            # Off-loop-thread callback: hand off to the main loop if captured,
            # otherwise fall back to the legacy sync-run path.
            main_loop = self._main_loop
            if main_loop is not None and main_loop.is_running():
                asyncio.run_coroutine_threadsafe(__async_subscribe_pnl(portfolio_item), main_loop)
            else:
                self.run(__async_subscribe_pnl(portfolio_item))

        disposable = (await self.client.subscribe_portfolio()).subscribe(
            AnonymousObserver(
                on_next=__subscribe_pnl,
                on_error=handle_subscription_exception,
            )
        )
        self.disposables.append(disposable)

        pnl_router_disposable = self.pnl.subscribe(on_next=self.__dataclass_server_put, on_error=handle_subscription_exception)
        self.disposables.append(pnl_router_disposable)

        # push book updates
        def __update_book(trade_order: Union[Trade, Order]):
            event = UpdateEvent(trade_order)
            self.__dataclass_server_put(event)

        book_update_disposable = self.book.subscribe(
            Observer(
                on_next=__update_book,
                on_error=handle_subscription_exception,
            )
        )
        self.disposables.append(book_update_disposable)

        # make sure we're getting either live, or delayed data
        self.client.ib.reqMarketDataType(self.market_data)

        orders = await self.client.ib.reqAllOpenOrdersAsync()
        for o in orders:
            self.book.on_next(o)

        # ensure that pnl is getting pumped out of zmq
        if self.scheduler is None:
            self.scheduler = AsyncIOThreadSafeScheduler(asyncio.get_running_loop())
        scheduled_disposable = self.scheduler.schedule_periodic(10, lambda x: self.pnl.post_all())
        self.disposables.append(scheduled_disposable)

        # heartbeat: one INFO line per 30s proving the service is alive and
        # what it believes about the IB socket. All fields are local reads —
        # never blocks the loop.
        pulse_disposable = self.scheduler.schedule_periodic(30, lambda x: self._log_pulse())
        self.disposables.append(pulse_disposable)

    def _on_ib_error(self, error: IBAIORxError):
        """Track IB upstream connectivity from error codes.

        IB distinguishes two severities we care about:

        1. **1100 / 1102**: full gateway↔IBKR connectivity. 1100 means
           trading is actually disabled. This is the ONLY signal that
           should flip ``ib_upstream_connected`` to False.

        2. **2103 / 2105 / 2157**: per-data-farm status messages. IB
           Gateway has multiple farms (``usfarm``, ``euhmds``,
           ``cashfarm``, ``usfuture``, ...) and sends these warnings
           any time one farm briefly hiccups. Other farms stay up,
           trading keeps working, the Gateway UI stays green. Treating
           these as a hard disconnect produced false-positive "Gateway
           broken" warnings in the CLI while the user could see real-
           time P&L updating normally.

        We now track per-farm state as an informational dict so callers
        can surface "warning: usfarm hiccuped" without falsely reporting
        a full disconnect."""
        code = error.errorCode
        msg = error.errorString

        if code == 1100:
            # Real disconnect — trading disabled until 1102 arrives.
            self._ib_upstream_connected = False
            self._ib_upstream_error = msg
            logging.warning('IB upstream connection lost (code 1100): %s', msg)
        elif code == 1102:
            self._ib_upstream_connected = True
            self._ib_upstream_error = ''
            logging.info('IB upstream connection restored (code 1102): %s', msg)
        elif code in (2103, 2105, 2157):
            # Informational farm hiccup. Track so callers can query
            # ``_ib_farms_down`` if they really want farm-level detail,
            # but leave ``_ib_upstream_connected`` alone.
            if not hasattr(self, '_ib_farms_down'):
                self._ib_farms_down = {}
            self._ib_farms_down[code] = msg
            logging.info('IB farm warning (code %d, informational): %s', code, msg)
        elif code in (2104, 2106, 2158):
            if hasattr(self, '_ib_farms_down'):
                # 2104 ↔ 2103, 2106 ↔ 2105, 2158 ↔ 2157
                self._ib_farms_down.pop(code - 1, None)
            logging.info('IB farm restored (code %d): %s', code, msg)

    @log_method
    async def connected_event(self):
        # Coalesce the reconnect double-fire: connect_async() emits the IB
        # connectedEvent (→ this handler) AND the reconnect loop calls this
        # explicitly. Running both would dispose/rebuild subscriptions twice and
        # could double-subscribe the ticker feed. First one wins; skip the rest.
        if self._in_connected_event:
            logging.debug('connected_event already running — skipping duplicate invocation')
            return
        self._in_connected_event = True
        try:
            # Capture the main event loop now that we're running inside it. Used
            # to schedule async work from IB callback threads without spinning up
            # a throwaway loop.
            try:
                self._main_loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

            # Dispose old subscriptions before re-subscribing (happens on reconnect)
            for disposable in self.disposables:
                try:
                    disposable.dispose()
                except Exception:
                    pass
            self.disposables.clear()
            with self._pnl_subscriptions_lock:
                self.pnl_subscriptions.clear()

            await self.setup_subscriptions()

            # Re-establish live ticker (pubsub) subscriptions. Their IB
            # market-data lines died with the previous session, so on a reconnect
            # we must resubscribe or the strategy tick feed goes silently dead.
            # No-op on the first connect (nothing published yet).
            self._republish_ticker_subscriptions()

            # One-shot startup broker-truth reconciliation: after a restart,
            # cross-check proposals + positions against live IB and log any
            # divergence (report-only). Delayed so IB open-orders/positions have
            # populated; runs off the connected_event path so it can't block it.
            if not getattr(self, '_startup_reconciled', False):
                self._startup_reconciled = True

                async def _delayed_reconcile():
                    try:
                        await asyncio.sleep(8)
                        await self.reconcile_with_broker()
                    except Exception as ex:
                        logging.warning('startup reconciliation failed: %s', ex)

                try:
                    asyncio.get_event_loop().create_task(_delayed_reconcile())
                except RuntimeError:
                    pass
        finally:
            self._in_connected_event = False

    def _republish_ticker_subscriptions(self):
        """Replay remembered publish_contract() calls after a reconnect."""
        remembered = dict(self.zmq_pubsub_published_contracts)
        if not remembered:
            return
        logging.info('re-establishing %d live ticker subscription(s) after reconnect',
                     len(remembered))
        # Tear down the stale shared subscription + per-contract state; the old
        # session's market-data lines are gone.
        try:
            self.zmq_pubsub_contract_subscription.dispose()
        except Exception:
            pass
        self.zmq_pubsub_contract_subscription = Disposable()
        self.zmq_pubsub_contracts = {}
        self.zmq_pubsub_contract_filters = {}
        for con_id, (contract, delayed) in remembered.items():
            try:
                self.publish_contract(contract, delayed=delayed)
            except Exception as ex:
                logging.error('failed to re-publish ticker subscription for conId %s: %s',
                              con_id, ex)

    @log_method
    async def disconnected_event(self):
        # Guard against multiple concurrent reconnection attempts
        if hasattr(self, '_reconnecting') and self._reconnecting:
            logging.debug('reconnection already in progress, skipping')
            return

        self._reconnecting = True
        try:
            attempt = 0
            while True:
                attempt += 1
                delay = min(2 ** min(attempt, 7), 120)  # exponential backoff, cap at 2 minutes

                logging.warning(
                    'IB Gateway disconnected — reconnection attempt %d in %ds',
                    attempt, delay
                )

                t_before = asyncio.get_event_loop().time()
                await asyncio.sleep(delay)
                t_after = asyncio.get_event_loop().time()

                # Detect system sleep: if the actual elapsed time is much longer
                # than the requested delay, the system likely slept. Reset backoff
                # so we get a fresh set of fast retries after wake.
                elapsed = t_after - t_before
                if elapsed > delay * 3 and delay > 4:
                    logging.info(
                        'detected system sleep (requested %ds, elapsed %.0fs) — resetting backoff',
                        delay, elapsed,
                    )
                    attempt = 1

                try:
                    await self.client.connect_async()
                    # Re-attach event handlers to the fresh IB instance
                    if self.connected_event not in self.client.ib.connectedEvent:
                        self.client.ib.connectedEvent += self.connected_event
                    if self.disconnected_event not in self.client.ib.disconnectedEvent:
                        self.client.ib.disconnectedEvent += self.disconnected_event
                    logging.info('reconnected to IB Gateway on attempt %d', attempt)
                    await self.connected_event()
                    return
                except Exception as ex:
                    logging.error('reconnection attempt %d failed: %s', attempt, ex)
        finally:
            self._reconnecting = False

    @log_method
    async def enable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        try:
            return self.zmq_strategy_client.rpc().enable_strategy(name)
        except Exception as ex:
            logging.error('enable_strategy: {}'.format(ex))
            return SuccessFail.fail(exception=ex)

    @log_method
    async def disable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        try:
            return self.zmq_strategy_client.rpc().disable_strategy(name)
        except Exception as ex:
            logging.error('disable_strategy: {}'.format(ex))
            return SuccessFail.fail(exception=ex)

    @log_method
    async def get_strategies(self) -> SuccessFail[List[StrategyConfig]]:
        try:
            rpc_call = self.zmq_strategy_client.rpc().get_strategies()
            # rpc_call = SuccessFail.success(await (await self.zmq_strategy_client.awaitable_rpc()).get_strategies())
            return SuccessFail.success(rpc_call)
        except Exception as ex:
            return SuccessFail.fail(exception=ex)

    @log_method
    async def reload_strategies(self) -> SuccessFail[List[StrategyConfig]]:
        try:
            return self.zmq_strategy_client.rpc().reload_strategies()
        except Exception as ex:
            logging.error('reload_strategies: {}'.format(ex))
            return SuccessFail.fail(exception=ex)

    @log_method
    def clear_portfolio_universe(self):
        universe = self.universe_accessor.get('portfolio')
        universe.security_definitions.clear()
        self.universe_accessor.update(universe)

    @log_method
    async def resolve_contract(self, contract: Contract) -> List[SecurityDefinition]:
        """Resolve a partial Contract (e.g. with strike/expiry/right) to full SecurityDefinitions via IB."""
        contract_details = await self.client.ib.reqContractDetailsAsync(contract)
        if contract_details:
            return [SecurityDefinition.from_contract_details(cd) for cd in contract_details]
        return []

    @log_method
    async def resolve_symbol(
        self,
        symbol: Union[str, int],
        exchange: str = '',
        universe: str = '',
        sec_type: str = '',
    ) -> List[SecurityDefinition]:
        def __blocking_resolve_symbol_to_security_definitions(
            symbol: Union[str, int],
            exchange: str = '',
            universe: str = '',
            sec_type: str = '',
            first_only: bool = False,
        ) -> list[SecurityDefinition]:
            return self.universe_accessor.resolve_symbol(
                symbol=symbol,
                exchange=exchange,
                universe=universe,
                first_only=first_only
            )

        # if we're asking about conid's, we only want the first one
        first_only = False
        if type(symbol) is int:
            first_only = True

        # this could take a while
        result = await asyncio.to_thread(
            __blocking_resolve_symbol_to_security_definitions,
            symbol,
            exchange,
            universe,
            sec_type,
            first_only,
        )

        if len(result) > 0:
            return result

        # No IB fallback. resolve_symbol is a local DB lookup only.
        # Guessing with a partially-specified Contract can resolve to the
        # wrong instrument (e.g. SOXL → AEQLIT/CAD, 4391 → TSEJ).
        # Use resolve_contract() for explicit IB discovery with a
        # fully-specified Contract.
        if type(symbol) is int:
            logging.warning('conId %d not found in local universe DB', symbol)
        else:
            logging.warning("Symbol '%s' not found in local universe DB — "
                            "add it with `universe add` or use `resolve` CLI command", symbol)
        return []

    @log_method
    async def resolve_universe(
        self,
        symbol: Union[str, int],
        exchange: str = '',
        universe: str = '',
        sec_type: str = '',
    ) -> List[Tuple[str, SecurityDefinition]]:
        def __blocking_resolve_universe(
            symbol: Union[str, int],
            exchange: str = '',
            universe: str = '',
            sec_type: str = '',
        ) -> list[Tuple[str, SecurityDefinition]]:
            return self.universe_accessor.resolve_universe_name(
                symbol=symbol,
                exchange=exchange,
                universe=universe,
                sec_type=sec_type
            )

        # this could take a while
        return await asyncio.to_thread(__blocking_resolve_universe, symbol, exchange, universe, sec_type)

    @log_method
    def publish_contract(self, contract: Contract, delayed: bool) -> Observable[IBAIORxError]:
        # Remember the request so it can be replayed after a reconnect.
        self.zmq_pubsub_published_contracts[contract.conId] = (contract, delayed)
        if contract.conId in self.zmq_pubsub_contract_filters:
            return self.zmq_pubsub_contracts[contract.conId]

        def on_next(ticker: Ticker):
            self.zmq_pubsub_server.put(('ticker', ticker))

        def on_completed():
            del self.zmq_pubsub_contracts[contract.conId]
            del self.zmq_pubsub_contract_filters[contract.conId]
            logging.debug('publish_contract.aclose() for {}'.format(contract))

        def on_error(ex):
            del self.zmq_pubsub_contracts[contract.conId]
            del self.zmq_pubsub_contract_filters[contract.conId]
            raise trader_exception(self, TraderException, message='publish_contract() on_error', inner=ex)

        if len(self.zmq_pubsub_contract_filters) == 0:
            # setup the observable for the first time
            try:
                auto_detach = AutoDetachObserver(on_next=on_next, on_completed=on_completed, on_error=on_error)
                subscription = self.client.contracts_subject.subscribe(auto_detach)  # , scheduler=NewThreadScheduler())
                self.zmq_pubsub_contract_subscription = subscription
            except Exception as ex:
                # todo not sure how to deal with this error condition yet
                raise trader_exception(self, TraderException, message='publish_contract()', inner=ex)

        error_observable = self.client.subscribe_contract_direct(contract, delayed=delayed)
        self.zmq_pubsub_contract_filters[contract.conId] = True
        self.zmq_pubsub_contracts[contract.conId] = error_observable
        return error_observable

    async def update_portfolio_universe(self, portfolio_item: PortfolioItem):
        """Add new positions to the 'portfolio' universe so history downloads
        + strategy subscriptions auto-cover them.

        Called once per ``updatePortfolioEvent`` — which on a 49-position
        account produces a 49× burst at connect. We coalesce this via
        ``_known_portfolio_conids`` so repeated events for already-known
        conIds skip all DB work immediately. All ``universe_accessor``
        reads/writes run in a worker thread via ``asyncio.to_thread`` so
        the main event loop stays free for ticker dispatch / ZMQ traffic.
        """
        conid = portfolio_item.contract.conId

        # Fast path: conId already known from this session. Main-loop-
        # blocking reasons this is worth inlining:
        #   - The old path did 49 sync DuckDB reads + dill.loads + writes
        #     on the loop when 48 of them turned out to be no-ops.
        #   - `updatePortfolioEvent` fires on every position *update* too
        #     (price-mark refreshes), so this cache prevents a long-running
        #     session from paying the DB cost on every mark.
        if conid in self._known_portfolio_conids:
            return

        # First time we've seen this conId — reconcile with the persisted
        # universe. Thread the read so dill.loads on a multi-position
        # universe doesn't stall the loop.
        universe = await asyncio.to_thread(
            self.universe_accessor.get, 'portfolio',
        )

        # Seed / re-seed the in-memory set from the freshly read universe.
        # Any concurrent update_portfolio_universe calls that raced past
        # the fast-path check will all see the same set after this point.
        self._known_portfolio_conids = {
            d.conId for d in universe.security_definitions
        }
        if conid in self._known_portfolio_conids:
            # Another task added it while we were reading, or it's been
            # persisted across sessions. Either way, nothing to do.
            return

        # Genuinely new — go fetch contract details and persist.
        contract = portfolio_item.contract
        try:
            contract_details = await self.client.get_contract_details_async(contract)
        except Exception as ex:
            logging.warning(f'Failed to get contract details for {contract}: {ex}')
            return
        if not contract_details:
            return
        universe.security_definitions.append(
            SecurityDefinition.from_contract_details(contract_details[0])
        )
        self._known_portfolio_conids.add(conid)
        logging.debug('updating portfolio universe with %s', portfolio_item)

        # Thread the write — dill.dumps on the universe + DuckDB INSERT
        # can be tens of ms; no reason to run on the loop.
        await asyncio.to_thread(self.universe_accessor.update, universe)

    def _update_portfolio_universe_sync(self, portfolio_item: PortfolioItem):
        """Sync fallback when no event loop is running."""
        universe = self.universe_accessor.get('portfolio')
        if not ListHelper.isin(
            universe.security_definitions,
            lambda definition: definition.conId == portfolio_item.contract.conId
        ):
            contract = portfolio_item.contract
            contract_details = self.client.get_contract_details(contract)
            if contract_details and len(contract_details) >= 1:
                universe.security_definitions.append(
                    SecurityDefinition.from_contract_details(contract_details[0])
                )

            logging.debug('updating portfolio universe with {}'.format(portfolio_item))
            self.universe_accessor.update(universe)

    @log_method
    async def place_order(
        self,
        contract: Contract,
        order: Order,
        condition: ExecutorCondition,
    ) -> Observable[Trade]:
        return await self.executioner.place_order(contract_order=ContractOrderPair(contract, order), condition=condition)

    @log_method
    async def _margin_impact_or_refusal(self, contract: Contract, probe_order: Order):
        """whatIf margin impact, or a fail-closed SuccessFail refusal.

        Returns the margin dict on success; a SuccessFail.fail on any failure
        (exception or empty result) so the open branch refuses with a reason.
        CASH contracts never reach this — they carry the documented
        skipped:forex-cash exemption at the call site.
        """
        try:
            margin_impact = await self.check_order_margin(contract, probe_order)
        except Exception as ex:
            logging.warning('whatIfOrder failed — refusing open (fail-closed): %s', ex)
            return SuccessFail.fail(
                error=f'margin impact could not be computed (whatIfOrder '
                      f'failed: {ex}) — refusing to open new exposure without '
                      f'the leverage check (fail-closed; exits are exempt)')
        if not margin_impact:
            return SuccessFail.fail(
                error='margin impact came back empty — refusing to open new '
                      'exposure without the leverage check (fail-closed; '
                      'exits are exempt)')
        return margin_impact

    async def check_order_margin(self, contract: Contract, order: Order) -> dict:
        """Run whatIfOrder to get margin impact without placing."""
        order_state = await self.client.ib.whatIfOrderAsync(contract, order)
        # ib_async can hand back a LIST of order states — observed live
        # 2026-07-27 on a CASH/IDEALPRO whatIf, where `.numeric` on the list
        # raised AttributeError. Before the fail-closed flip that crash was
        # silently swallowed and the margin check simply never ran for forex;
        # after it, the crash refused the open, which is how it was found.
        # Normalize to the first state; an empty list is a failed whatIf and
        # raises so the caller's fail-closed refusal says why.
        if isinstance(order_state, (list, tuple)):
            if not order_state:
                raise ValueError('whatIfOrder returned no order state')
            # ty narrows a list/tuple element to `object`; the runtime type is
            # ib_async OrderState (duck-typed .numeric/.warningText below).
            order_state = cast(Any, order_state[0])
        numeric = order_state.numeric(2)
        return {
            'initMarginBefore': numeric.initMarginBefore,
            'maintMarginBefore': numeric.maintMarginBefore,
            'equityWithLoanBefore': numeric.equityWithLoanBefore,
            'initMarginChange': numeric.initMarginChange,
            'maintMarginChange': numeric.maintMarginChange,
            'equityWithLoanChange': numeric.equityWithLoanChange,
            'initMarginAfter': numeric.initMarginAfter,
            'maintMarginAfter': numeric.maintMarginAfter,
            'equityWithLoanAfter': numeric.equityWithLoanAfter,
            'commission': numeric.commission,
            'warningText': order_state.warningText,
        }

    def _signed_position(self, conid: int) -> Optional[float]:
        """Live broker position (signed) for ``conid`` in the pinned account.

        Returns 0.0 for a conId we hold no position in, and None when the
        position read itself failed — callers must treat None as "unknown",
        which the exit-class predicate maps to "not an exit" (fail-closed:
        an order we can't prove reduces exposure gets gated like an open).
        """
        try:
            positions = self.get_positions()
        except Exception as ex:
            logging.warning('could not read broker positions for exit-class check: %s', ex)
            return None
        total = 0.0
        for p in positions or []:
            c = getattr(p, 'contract', None)
            if c is not None and int(getattr(c, 'conId', 0) or 0) == conid:
                total += float(getattr(p, 'position', 0.0) or 0.0)
        return total

    def _signed_position_by_symbol(self, symbol: str) -> Optional[float]:
        """Live broker position (signed) summed across every row whose
        contract symbol matches ``symbol``. Fallback for the exit-class
        predicate when the order's conId doesn't resolve to a position row
        (missing conId, cache lag, conId change). None on a failed read.
        """
        try:
            positions = self.get_positions()
        except Exception as ex:
            logging.warning('could not read broker positions for symbol exit-class check: %s', ex)
            return None
        total = 0.0
        for p in positions or []:
            c = getattr(p, 'contract', None)
            if c is not None and (getattr(c, 'symbol', '') or '') == symbol:
                total += float(getattr(p, 'position', 0.0) or 0.0)
        return total

    def order_reduces_exposure(self, contract: Contract, action: str, quantity: float) -> bool:
        """The single server-side EXIT-CLASS predicate: True iff placing
        ``action quantity`` on ``contract`` reduces the live broker position
        for its conId — a SELL against ANY net-long position, or a BUY against
        ANY net-short. It is direction-aware, not size-clamped: you cannot
        INCREASE a long by selling, so an oversized SELL of a held long (a
        flip) is still exit-class and must never be refused as an open —
        refusing an exit is worse than any limit. Callers that must not
        OVERSELL (protective orders) clamp qty separately.

        Only a SELL with no long (opening a short) or a BUY with no short is a
        true open. No matching position or an unreadable portfolio returns
        False (gated like an open, fail-closed). When the order's conId is
        missing or its position row hasn't landed yet (cache lag right after a
        fill, or a conId change), a same-symbol position is used as a fallback
        — exit-class detection only (gate exemption), never contract
        resolution.
        """
        try:
            conid = int(getattr(contract, 'conId', 0) or 0)
            qty = float(quantity or 0.0)
        except (TypeError, ValueError):
            return False
        if qty <= 0 or not math.isfinite(qty):
            return False
        act = str(action).strip().upper()
        if act not in ('BUY', 'SELL'):
            return False
        held = self._signed_position(conid) if conid > 0 else None
        if held is None or held == 0.0:
            symbol = (getattr(contract, 'symbol', '') or '').strip()
            if symbol:
                by_symbol = self._signed_position_by_symbol(symbol)
                if by_symbol is not None and by_symbol != 0.0:
                    held = by_symbol
        if held is None:
            return False
        # The DECISION itself lives in the pure, deal-contracted, mutation- and
        # CrossHair-checked kernel (trader.trading.exit_class). This method's
        # remaining job is resolving `held` from the live portfolio; the
        # direction rule is verified there, where the toolchain can see it.
        return reduces_exposure(act, held, qty)

    def _opening_exposure_quantity(
        self, contract: Contract, action: str, quantity: float
    ) -> float:
        """The portion of ``action quantity`` on ``contract`` that opens
        NET-NEW exposure — the only quantity the approver notional tier gates.

        Mirrors ``order_reduces_exposure``'s direction-aware classification:
          * 0.0 for a pure exit (a SELL fully within a held long, a BUY fully
            within a held short) — never gated;
          * the FULL qty for a true open (a SELL with no long, a BUY with no
            short) — an added position;
          * the REMAINDER ``max(0, qty - |held opposing|)`` for a flip that
            crosses zero (e.g. SELL 30 against a +10 long opens a 20-short) —
            only the net-new side is gated.

        FAIL-CLOSED (treat the FULL qty as opening): an unreadable position, an
        unknown/blank conId with no same-symbol fallback, or a non-BUY/SELL
        action. Uses ``_signed_position`` and the ``_signed_position_by_symbol``
        fallback exactly like ``order_reduces_exposure``.
        """
        try:
            conid = int(getattr(contract, 'conId', 0) or 0)
            qty = float(quantity or 0.0)
        except (TypeError, ValueError):
            try:
                return abs(float(quantity or 0.0))
            except (TypeError, ValueError):
                return 0.0
        if qty <= 0 or not math.isfinite(qty):
            return 0.0
        act = str(action).strip().upper()
        if act not in ('BUY', 'SELL'):
            return qty  # fail-closed: unknown action → treat as opening
        held = self._signed_position(conid) if conid > 0 else None
        if held is None or held == 0.0:
            symbol = (getattr(contract, 'symbol', '') or '').strip()
            if symbol:
                by_symbol = self._signed_position_by_symbol(symbol)
                if by_symbol is not None and by_symbol != 0.0:
                    held = by_symbol
        if held is None:
            return qty  # fail-closed: unreadable position → treat as opening
        if act == 'SELL':
            # Only a long is reduced by a SELL. No long → full open (short);
            # a long → exit up to |held|, the remainder flips short (opening).
            return qty if held <= 0 else max(0.0, qty - abs(held))
        # BUY: only a short is reduced. No short → full open (long); a short →
        # exit up to |held|, the remainder flips long (opening).
        return qty if held >= 0 else max(0.0, qty - abs(held))

    async def _tier_notional(
        self, contract: Contract, action: str, quantity: float,
        order_type: str, limit_price: Optional[float],
    ) -> Tuple[float, bool]:
        """The NON-FORGEABLE notional for the approver tier and whether it is
        evaluable, as ``(notional, evaluable)``.

        The valuation price is anchored on a LIVE marketable snapshot
        (``get_snapshot`` — ask for a BUY, bid for a SELL, falling back to
        last/close; finite and > 0). A client-supplied ``limit_price`` is
        parsed only for non-MARKET orders and only when finite > 0. The price
        used is ``max(snapshot, limit)`` when both exist (a proposer can push
        it UP but never DOWN), the snapshot alone when there is no usable limit,
        and — critically — when there is NO snapshot the notional is NOT
        evaluable (``(0.0, False)``): a bare client limit is never trusted
        downward for the tier.
        """
        try:
            multiplier = float(contract.multiplier) if contract.multiplier else 1.0
        except (TypeError, ValueError):
            multiplier = 1.0

        act = str(action).strip().upper()
        snapshot_price: Optional[float] = None
        try:
            tick = await self.client.get_snapshot(contract)
            for candidate in (
                tick.ask if act == 'BUY' else tick.bid,
                getattr(tick, 'last', None),
                getattr(tick, 'close', None),
            ):
                try:
                    # candidate may be None (ask/bid/last/close unset); the
                    # TypeError from float(None) is caught below — ty can't see
                    # the guard (same pattern as the concentration snapshot loop).
                    price = float(candidate)  # ty: ignore[invalid-argument-type]
                except (TypeError, ValueError):
                    continue
                if math.isfinite(price) and price > 0:
                    snapshot_price = price
                    break
        except Exception as ex:
            logging.warning(
                'approver tier: no snapshot price to value order: %s', ex)

        limit_val: Optional[float] = None
        if str(order_type).strip().upper() != 'MARKET':
            try:
                # limit_price is Optional; float(None) → TypeError, caught below.
                lp = float(limit_price)  # ty: ignore[invalid-argument-type]
                if math.isfinite(lp) and lp > 0:
                    limit_val = lp
            except (TypeError, ValueError):
                pass

        if snapshot_price is None:
            # NOT evaluable — never trust a bare client limit downward.
            return (0.0, False)
        price = max(snapshot_price, limit_val) if limit_val is not None else snapshot_price
        notional = abs(float(quantity)) * price * multiplier
        return (notional, True)

    async def enforce_approver_tier(
        self, contract: Contract, action: str, quantity: float,
        order_type: str, limit_price: Optional[float], approver_key: str,
    ) -> Optional[str]:
        """The single server-side approver notional-tier enforcement point.
        Returns an error string to REFUSE the order, else ``None``.

        Safe to call UNCONDITIONALLY on any order path: the feature is OFF when
        ``approver_required_above_usd <= 0`` (byte-identical to prior
        behaviour), and any EXIT-CLASS order (``order_reduces_exposure`` — a
        reduction of the live position, flips included) is NEVER gated
        (preserving "exits never refused"). Only a pure open is gated: above the
        threshold it requires a constant-time-matching, non-empty configured
        approver key. The valuation is the SERVER-RECOMPUTED notional
        (``_tier_notional`` prices at ``max(limit, live snapshot)``, never below
        the live market) — a proposer cannot forge it downward with a lowball
        limit.
        """
        threshold = getattr(self, 'approver_required_above_usd', 0.0) or 0.0
        if threshold <= 0:
            return None  # feature OFF
        # Exit-class orders — a SELL against ANY held long, a BUY against ANY
        # held short, INCLUDING an oversized "flip" that crosses zero — are
        # NEVER gated, using the same server-side classifier the rest of the
        # system trusts (order_reduces_exposure). Two reasons this, not an
        # opening-remainder computation: (1) roadmap principle 2 — an order that
        # reduces the live position must never be blocked by an approval
        # requirement, and refusing an atomic flip refuses its embedded exit;
        # (2) a second position read here could race and gate a GENUINE exit on
        # a momentary unreadable position. The pure exit (SELL <= held) is
        # always available; a flip's net-new opening remainder is a documented
        # residual (SAFETY_ROADMAP), closed by turnover caps / order-splitting,
        # never by refusing a reduction.
        if self.order_reduces_exposure(contract, action, quantity):
            return None
        notional, evaluable = await self._tier_notional(
            contract, action, quantity, order_type, limit_price)
        if not evaluable:
            return (
                'approver notional tier is active but the order notional '
                'could not be valued (no usable price) — refusing the open '
                '(fail-closed on exposure). Threshold '
                f'${threshold:,.2f}.')
        if notional > threshold:
            supplied = str(approver_key or '')
            expected = str(getattr(self, 'approver_key', '') or '')
            if not expected or not hmac.compare_digest(supplied, expected):
                return (
                    f'order notional ${notional:,.2f} exceeds the approver '
                    f'threshold ${threshold:,.2f}: a valid '
                    'approver key is required and none/an incorrect one was '
                    'supplied. No order placed.')
        return None

    def _has_today_trading_activity(self) -> bool:
        """True if the account holds any position OR booked any fill today.

        Used to disambiguate an empty PnL cache: with activity present, an
        empty cache means the feed hasn't warmed (fail-closed on opens); with
        genuinely no activity, an empty cache is a real flat 0.0. An
        unreadable state returns True (assume activity → fail-closed).
        """
        try:
            if self.get_positions():
                return True
        except Exception as ex:
            logging.warning('risk inputs: could not read positions for activity check: %s', ex)
            return True
        try:
            midnight = dt.datetime.combine(dt.date.today(), dt.time.min)
            return self.event_store.count_since(
                since=midnight, event_type=EventType.ORDER_FILLED) > 0
        except Exception as ex:
            logging.warning('risk inputs: could not read fills for activity check: %s', ex)
            return True

    def gather_risk_inputs(self) -> RiskInputs:
        """Read the account state the risk gate needs, marking per-field
        evaluability — "read succeeded, value is 0" is distinct from "could
        not read". Shared by every gate call site (executioner.place_order
        and place_expressive_order) so daily-loss and concentration are
        never silently no-op'd against defaults.
        """
        open_order_count = self.book.get_open_order_count() if hasattr(self, 'book') else 0

        daily_pnl = 0.0
        daily_pnl_evaluable = True
        try:
            pnl_items = list(self.get_pnl() or [])
        except Exception as ex:
            logging.warning('risk inputs: could not read daily PnL: %s', ex)
            pnl_items = []
            daily_pnl_evaluable = False

        if daily_pnl_evaluable and not pnl_items:
            # An empty PnLSingle cache is ambiguous: genuinely flat-with-no-
            # activity (0.0 is the truth) vs the feed simply hasn't warmed yet
            # after a mid-day restart (0.0 is a LIE — realized losses booked
            # earlier are invisible). If the account holds positions or booked
            # any fill today, the feed hasn't populated: treat daily_pnl as
            # NOT-evaluable so the gate fails closed on opens until it warms,
            # rather than approving blind to today's loss. Exits are unaffected.
            if self._has_today_trading_activity():
                daily_pnl_evaluable = False
        elif daily_pnl_evaluable:
            for p in pnl_items:
                value = float(getattr(p, 'dailyPnL', 0.0) or 0.0)
                if not math.isfinite(value):
                    # IB hasn't delivered this position's PnL yet — the sum
                    # would be a lie, not a zero.
                    daily_pnl_evaluable = False
                    daily_pnl = 0.0
                    break
                daily_pnl += value

        portfolio_value = 0.0
        portfolio_value_evaluable = False
        try:
            active_account = self.ib_account or (
                (self.client.ib.managedAccounts() or [None])[0])
            for v in self.client.ib.accountValues():
                if v.tag != 'NetLiquidation' or v.currency == 'BASE':
                    continue
                if active_account and v.account and v.account != active_account:
                    continue
                portfolio_value = float(v.value)
                portfolio_value_evaluable = True
                break
        except Exception as ex:
            logging.warning('risk inputs: could not read NetLiquidation: %s', ex)

        return RiskInputs(
            open_order_count=open_order_count,
            daily_pnl=daily_pnl,
            daily_pnl_evaluable=daily_pnl_evaluable,
            portfolio_value=portfolio_value,
            portfolio_value_evaluable=portfolio_value_evaluable,
        )

    @log_method
    async def _place_flip_split(
        self,
        contract: Contract,
        action: str,
        plan,
        execution_spec: dict,
        algo_name: str,
        approver_key: str,
    ) -> SuccessFail:
        """Place a position-crossing order as its two real halves.

        Ordering is the safety property: the REDUCTION goes first and is never
        refusable, so a refused opening half leaves the caller flat instead of
        stuck in the position they asked to leave. The two outcomes are
        reported together, because "closed 3, refused the new short 2" is a
        materially different result from either half alone and the caller must
        not have to infer it.
        """
        logging.warning(
            'flip split: %s %s %s crosses zero — placing reduction %s (exit-class) '
            'then opening remainder %s (gated)',
            action, plan.reduce_qty + plan.open_qty, contract.symbol,
            plan.reduce_qty, plan.open_qty)

        # The reduction is a plain close: no bracket, no protective legs. Any
        # exit spec belongs to the NEW position, so it rides with the remainder.
        reduce_spec = dict(execution_spec)
        reduce_spec['exit_type'] = 'NONE'
        reduce_result = await self.place_expressive_order(
            contract, action, plan.reduce_qty, reduce_spec,
            algo_name=algo_name, approver_key=approver_key)

        if not reduce_result.is_success():
            # The unrefusable half failed for a non-gate reason (broker reject,
            # timeout). Do NOT place the opening half on top of an unknown
            # position.
            return SuccessFail.fail(
                error=f'flip split: the reduction of {plan.reduce_qty:g} failed '
                      f'({reduce_result.error}); the opening remainder of '
                      f'{plan.open_qty:g} was NOT attempted')

        open_result = await self.place_expressive_order(
            contract, action, plan.open_qty, execution_spec,
            algo_name=algo_name, approver_key=approver_key, force_open=True)

        if not open_result.is_success():
            logging.warning(
                'flip split: reduction of %s placed; opening remainder of %s '
                'REFUSED by the gates (%s) — caller is flat, not flipped',
                plan.reduce_qty, plan.open_qty, open_result.error)
            return SuccessFail.fail(
                error=f'flip split: reduced {plan.reduce_qty:g} (placed), but the '
                      f'opening remainder of {plan.open_qty:g} was refused: '
                      f'{open_result.error}')

        trades = list(reduce_result.obj or []) + list(open_result.obj or [])
        return SuccessFail.success(obj=trades)

    async def place_expressive_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        execution_spec: dict,
        algo_name: str = 'proposal',
        approver_key: str = '',
        force_open: bool = False,
    ) -> SuccessFail:
        """Place an order with full execution specification (brackets, trailing stops, etc.).

        ``force_open`` is set ONLY by the flip-splitting branch below, for the
        opening half of a position-crossing order. That half must be gated as
        new exposure even though the live position read may still show the old
        pre-reduction size, which would otherwise re-classify it as an exit and
        wave it through. It is never set by an external caller.
        """
        from trader.trading.proposal import ExecutionSpec
        spec = ExecutionSpec.from_dict(execution_spec)

        # Validate execution spec before placing any orders
        validation_errors = spec.validate()
        if validation_errors:
            return SuccessFail.fail(error=f'Invalid execution spec: {"; ".join(validation_errors)}')

        trades: List[Trade] = []

        reverse_action = 'SELL' if action == 'BUY' else 'BUY'

        common = dict(
            action=action,
            totalQuantity=quantity,
            account=self.ib_account,
            orderRef=algo_name,
            tif=spec.tif,
            outsideRth=spec.outside_rth,
        )
        if spec.tif == 'GTD' and spec.good_till_date:
            common['goodTillDate'] = spec.good_till_date

        def _build_entry(**common) -> Order:
            if spec.order_type == 'MARKET':
                return MarketOrder(**common)
            else:
                # spec.validate() (above) guarantees limit_price is non-None for LIMIT orders; ib_async stub-types lmtPrice as int|float.
                return LimitOrder(lmtPrice=spec.limit_price, **common)  # ty: ignore[invalid-argument-type]

        # STRUCTURAL SANITY, BEFORE ANY BROKER INTERACTION.
        #
        # The chokepoint enforces this too, and did so first — but it sits
        # DOWNSTREAM of the whatIfOrder margin probe below, so a malformed
        # order was still handed to IB before anything of ours refused it.
        # Proven by an adversarial probe 2026-07-27: proposing a NaN quantity
        # produced IB error 320, "Unable to parse field: 'Order Size' for input
        # string: 'nan'". Nothing traded (the fail-closed margin gate refused
        # the open when whatIf failed), so this is hygiene rather than a hole —
        # but the refusal was owned by the broker's validator, and a safety
        # property should not depend on the counterparty being fussy.
        #
        # Checking the built ENTRY order (not the raw arguments) reuses the
        # exact adapter the chokepoint uses, so the two can never disagree
        # about what "well-formed" means. _build_entry touches no I/O.
        # Applies to exits as well, for the chokepoint's reason: a malformed
        # order is not a working exit.
        structural_reason = rejection_for_order(_build_entry(**common))
        if structural_reason is not None:
            logging.warning(
                'refusing structurally malformed order before the broker sees it: %s',
                structural_reason)
            return SuccessFail.fail(
                error=f'structurally malformed order — {structural_reason}')

        # --- Pre-trade risk checks ---
        #
        # Exit-class orders (the entry action reduces the live broker position
        # — e.g. an AutoExecutor close, a strategy's own exit after its
        # protective stop was cancelled) are exempt from every gate: refusing
        # an exit is worse than any limit. Opens keep filter + leverage + gate.
        # FLIP SPLITTING — closes the documented flip residual.
        #
        # Exit-class is direction-aware and NOT size-clamped, so with 3 held a
        # SELL 5 is labelled an exit and all five shares skip every gate: three
        # close a position, two open an UNCHECKED short. Confirmed live
        # 2026-07-27 (accepted, no refusal from anything).
        #
        # The order was always two economically different things under one
        # label. Split it: the reduction stays exit-class and unrefusable, the
        # remainder is gated as the new exposure it is. Reduction goes FIRST so
        # a refused remainder leaves the caller flat rather than blocking the
        # close. See trader/trading/order_split.py.
        if not force_open:
            held_signed = self._signed_position(contract.conId)
            if held_signed is not None:
                plan = split_order(action, held_signed, quantity)
                if plan.is_flip:
                    return await self._place_flip_split(
                        contract, action, plan, execution_spec, algo_name,
                        approver_key)

        is_exit = False if force_open else self.order_reduces_exposure(
            contract, action, quantity)

        # Tri-state gate record carried into the entry leg's minted token
        # (empty for exit-class entries and for protective children).
        entry_checks: dict = {}

        if is_exit:
            if self.risk_gate is not None:
                # Observability only — never refuse an exit.
                try:
                    instrument_result = self.risk_gate.check_instrument(
                        symbol=contract.symbol, exchange=contract.exchange or '',
                        sec_type=contract.secType or '',
                    )
                    if not instrument_result.approved:
                        logging.warning(
                            'exit-class order %s %s %s would have been blocked by trading '
                            'filter (%s) — exits are never gated',
                            action, quantity, contract.symbol, instrument_result.reason)
                except Exception as ex:
                    logging.warning('exit-class filter observability check errored: %s', ex)
        else:
            # 0. Fail closed: a non-exit-class order with no gate is refused.
            if self.risk_gate is None:
                return SuccessFail.fail(
                    error='risk gate unavailable — refusing exposure-increasing order '
                          '(fail-closed; exit-class orders are exempt)')

            # 1. Trading filter check (denylist/allowlist)
            instrument_result = self.risk_gate.check_instrument(
                symbol=contract.symbol, exchange=contract.exchange or '', sec_type=contract.secType or '',
            )
            if not instrument_result.approved:
                return SuccessFail.fail(error=instrument_result.reason)

            # Build a temporary entry order for margin simulation
            probe_order = _build_entry(**common)

            # 2. whatIfOrder margin check
            # Tri-state record for the margin/leverage dimension, merged into the
            # gate's checks below. Without this a whatIfOrder failure produced a
            # clean-looking approval with NOTHING recording that the leverage
            # limit was never applied — the audit trail could not distinguish
            # "checked and fine" from "never checked".
            # FAIL CLOSED (2026-07-26): a whatIfOrder failure or an empty
            # margin dict REFUSES the open. This was the last gate input that
            # still failed open — the recorded 'skipped:' states existed so the
            # audit trail could distinguish "checked and fine" from "never
            # checked", and the flip makes "never checked" refuse like every
            # other unreadable critical input. Exits never reach this branch.
            leverage_checks: Dict[str, str] = {}
            if (contract.secType or '').upper() == 'CASH':
                # A forex order is a currency conversion, not leveraged stock
                # exposure — the same reasoning that exempts CASH from the
                # concentration check. This is also a practical necessity, not
                # just taste: IB's whatIfOrder returns NO order state for
                # CASH/IDEALPRO (observed live 2026-07-27), so without this
                # carve-out the fail-closed margin gate makes forex opens
                # permanently impossible. Recorded, never silent.
                leverage_checks = {
                    'leverage': 'skipped:forex-cash',
                    'margin_cushion': 'skipped:forex-cash',
                }
                margin_impact = None
            else:
                margin_impact = await self._margin_impact_or_refusal(contract, probe_order)
                if isinstance(margin_impact, SuccessFail):
                    return margin_impact

            # 3. Leverage limit check
            if margin_impact:
                # Scope NetLiquidation to the configured account. With a
                # multi-account login, accountValues() returns rows for every
                # managed account; picking the first NetLiquidation row blind
                # could size the leverage check against the wrong (e.g. master
                # aggregate) account. Pin to self.ib_account.
                active_account = self.ib_account
                if not active_account:
                    managed = self.client.ib.managedAccounts() or []
                    active_account = managed[0] if managed else None
                net_liq = 0.0
                for v in self.client.ib.accountValues():
                    if v.tag != 'NetLiquidation' or v.currency == 'BASE':
                        continue
                    if active_account and v.account and v.account != active_account:
                        continue
                    net_liq = float(v.value)
                    break

                leverage_result = self.risk_gate.check_leverage(margin_impact, net_liq)
                leverage_checks = dict(leverage_result.checks or {})
                if not leverage_result.approved:
                    return SuccessFail.fail(error=leverage_result.reason)
            elif not leverage_checks:
                # margin_impact was falsy but no exception fired (e.g. {}).
                leverage_checks = {
                    'leverage': 'skipped:no-margin-data',
                    'margin_cushion': 'skipped:no-margin-data',
                }

            # 4. Risk gate checks (open orders, daily loss, concentration)
            from trader.trading.strategy import Signal
            # source_name must match what ORDER_SUBMITTED is stamped with (the
            # order's orderRef == algo_name; approve passes proposal.metadata
            # ['strategy']) so the open-rate check queries the right bucket
            # instead of a dead 'proposal' constant that never matches.
            signal = Signal(
                source_name=algo_name,
                action=Action.BUY if action == 'BUY' else Action.SELL,
                probability=1.0,
                risk=0.0,
            )

            inputs = self.gather_risk_inputs()

            try:
                multiplier = float(contract.multiplier) if contract.multiplier else 1.0
            except (TypeError, ValueError):
                multiplier = 1.0

            # Concentration needs a notional. LIMIT orders carry their own
            # price; MARKET orders are valued off a snapshot so the check is
            # evaluable (previously they silently skipped it). No usable
            # price → not evaluable → the gate refuses the open.
            position_value = 0.0
            position_value_evaluable = False
            if spec.order_type != 'MARKET' and spec.limit_price:
                try:
                    position_value = abs(float(quantity) * float(spec.limit_price)) * multiplier
                    position_value_evaluable = True
                except (TypeError, ValueError):
                    pass
            else:
                try:
                    tick = await self.client.get_snapshot(contract)
                    for candidate in (
                        tick.ask if action == 'BUY' else tick.bid,
                        getattr(tick, 'last', None),
                        getattr(tick, 'close', None),
                    ):
                        try:
                            # Defensive coercion of a possibly-None/Any tick field; the except handles non-numeric candidates.
                            price = float(candidate)  # ty: ignore[invalid-argument-type]
                        except (TypeError, ValueError):
                            continue
                        if math.isfinite(price) and price > 0:
                            position_value = abs(float(quantity)) * price * multiplier
                            position_value_evaluable = True
                            break
                except Exception as ex:
                    logging.warning(
                        'risk gate: no snapshot price to value market order: %s', ex)

            gate_result = self.risk_gate.evaluate(
                signal=signal,
                open_order_count=inputs.open_order_count,
                daily_pnl=inputs.daily_pnl,
                portfolio_value=inputs.portfolio_value,
                position_value=position_value,
                daily_pnl_evaluable=inputs.daily_pnl_evaluable,
                portfolio_value_evaluable=inputs.portfolio_value_evaluable,
                position_value_evaluable=position_value_evaluable,
                sec_type=contract.secType or '',
            )
            if not gate_result.approved:
                return SuccessFail.fail(error=f'Risk gate: {gate_result.reason}')
            # New dict, not gate_result.checks by reference — mutating that would
            # rewrite the gate's own record. The leverage entries ride along so
            # the token minted below carries the full picture, including any
            # dimension that was skipped rather than passed.
            entry_checks = {**gate_result.checks, **leverage_checks}

            # 5. Server-side notional-tier approver gate (Phase 2). Delegated to
            # the single, unified enforcement point on the Trader — it values
            # the OPENING portion at max(client-limit, live snapshot) (a
            # proposer can't forge it downward), fails CLOSED when the notional
            # can't be valued, and NEVER gates a pure exit. Called here inside
            # the open branch (exits routed away above by is_exit); the direct
            # order path calls the same method unconditionally.
            tier_error = await self.enforce_approver_tier(
                contract, action, quantity,
                spec.order_type, spec.limit_price, approver_key)
            if tier_error is not None:
                return SuccessFail.fail(error=tier_error)

        async def _place_and_wait(
            c: Contract, o: Order, leg_is_exit: bool = False,
            leg_checks: Optional[dict] = None,
            leg_reason: ExitReason = ExitReason.PROTECTIVE_CHILD,
        ) -> Optional[Trade]:
            """Place a single child order and await the IB ack. Returns the
            Trade object or None on failure (observer emitted on_error)."""
            event = asyncio.Event()
            result: Dict[str, Optional[Trade]] = {'trade': None}

            def _on_next(trade: Trade):
                result['trade'] = trade
                event.set()

            # leg_reason defaults to PROTECTIVE_CHILD because the TP/SL legs are
            # the callers passing leg_is_exit=True; the entry leg overrides it with
            # the predicate's verdict. A protective leg is exit-class by
            # CONSTRUCTION — its entry is staged transmit=False and has not filled,
            # so there is no position to classify against yet.
            approved = mint_approved_order(
                c, o, is_exit=leg_is_exit, checks=leg_checks or {},
                exit_reason=leg_reason)
            obs = await self.executioner.subscribe_place_order_direct(approved)
            obs.subscribe(Observer(
                on_next=_on_next,
                on_error=lambda e: event.set(),
                on_completed=lambda: None,
            ))
            await event.wait()
            return result['trade']

        def _cancel_trade_safely(trade: Optional[Trade]) -> None:
            """Best-effort cancel of a staged (transmit=False) child order."""
            if trade is None or not getattr(trade, 'order', None):
                return
            try:
                self.client.ib.cancelOrder(trade.order)
            except Exception as ex:
                logging.warning(
                    'failed to cancel partial bracket leg %s: %s',
                    getattr(trade.order, 'orderId', '?'), ex,
                )

        try:
            if spec.exit_type == 'BRACKET':
                entry = _build_entry(**common)
                entry.transmit = False

                entry_trade = await _place_and_wait(
                    contract, entry, leg_is_exit=is_exit, leg_checks=entry_checks,
                    leg_reason=ExitReason.POSITION_CLASSIFIED)
                if entry_trade is None:
                    return SuccessFail.fail(error='Failed to place entry order')

                trades.append(entry_trade)
                parent_id = entry_trade.order.orderId

                # Take-profit
                tp = LimitOrder(
                    action=reverse_action,
                    totalQuantity=quantity,
                    # spec.validate() guarantees take_profit_price is non-None for BRACKET exits; ib_async stub-types lmtPrice as int|float.
                    lmtPrice=spec.take_profit_price,  # ty: ignore[invalid-argument-type]
                    parentId=parent_id,
                    transmit=False,
                    account=self.ib_account,
                    tif=spec.tif,
                    outsideRth=spec.outside_rth,
                )
                tp_trade = await _place_and_wait(contract, tp, leg_is_exit=True)
                if tp_trade is None:
                    # Roll back the staged entry — it was transmit=False so no
                    # market-side exposure yet; cancelling keeps the book
                    # consistent with the caller's understanding that the
                    # bracket failed atomically.
                    _cancel_trade_safely(entry_trade)
                    return SuccessFail.fail(
                        error='Bracket aborted: take-profit order rejected; entry rolled back'
                    )
                trades.append(tp_trade)

                # Stop-loss (transmit=True triggers the whole bracket)
                sl = StopOrder(
                    action=reverse_action,
                    totalQuantity=quantity,
                    # spec.validate() guarantees stop_loss_price is non-None for BRACKET exits; ib_async stub-types stopPrice as int|float.
                    stopPrice=spec.stop_loss_price,  # ty: ignore[invalid-argument-type]
                    parentId=parent_id,
                    transmit=True,
                    account=self.ib_account,
                    tif=spec.tif,
                    outsideRth=spec.outside_rth,
                )
                sl_trade = await _place_and_wait(contract, sl, leg_is_exit=True)
                if sl_trade is None:
                    # Same as above: cancel TP + entry before the bracket is
                    # ever transmitted to the market.
                    _cancel_trade_safely(tp_trade)
                    _cancel_trade_safely(entry_trade)
                    return SuccessFail.fail(
                        error='Bracket aborted: stop-loss order rejected; entry + TP rolled back'
                    )
                trades.append(sl_trade)

            elif spec.exit_type == 'TRAILING_STOP':
                entry = _build_entry(**common)
                entry.transmit = False

                task = asyncio.Event()
                entry_trade = None

                def on_entry_ts(trade: Trade):
                    nonlocal entry_trade
                    entry_trade = trade
                    task.set()

                observable = await self.executioner.subscribe_place_order_direct(
                    mint_approved_order(contract, entry, is_exit=is_exit, checks=entry_checks,
                                            exit_reason=ExitReason.POSITION_CLASSIFIED))
                observable.subscribe(Observer(on_next=on_entry_ts, on_error=lambda e: task.set(), on_completed=lambda: None))
                await task.wait()

                if entry_trade is None:
                    return SuccessFail.fail(error='Failed to place entry order')

                trades.append(entry_trade)
                parent_id = entry_trade.order.orderId

                trail = Order(
                    orderType='TRAIL',
                    action=reverse_action,
                    totalQuantity=quantity,
                    parentId=parent_id,
                    transmit=True,
                    account=self.ib_account,
                    tif=spec.tif,
                    outsideRth=spec.outside_rth,
                )
                if spec.trailing_stop_percent:
                    trail.trailingPercent = spec.trailing_stop_percent
                elif spec.trailing_stop_amount:
                    trail.auxPrice = spec.trailing_stop_amount

                trail_task = asyncio.Event()
                trail_trade: Optional[Trade] = None

                def on_trail(trade: Trade):
                    nonlocal trail_trade
                    trail_trade = trade
                    trail_task.set()

                trail_obs = await self.executioner.subscribe_place_order_direct(
                    mint_approved_order(contract, trail, is_exit=True,
                                            exit_reason=ExitReason.PROTECTIVE_CHILD))
                trail_obs.subscribe(Observer(on_next=on_trail, on_error=lambda e: trail_task.set(), on_completed=lambda: None))
                await trail_task.wait()
                if trail_trade is None:
                    # All-or-nothing: the trailing stop is what transmits the
                    # staged (transmit=False) entry. If it failed, roll back the
                    # entry so we don't leave a zombie staged order and, crucially,
                    # don't report success for an unprotected/undelivered order.
                    _cancel_trade_safely(entry_trade)
                    return SuccessFail.fail(
                        error='Trailing-stop aborted: protective leg rejected; entry rolled back'
                    )
                trades.append(trail_trade)

            elif spec.exit_type == 'STOP_LOSS':
                entry = _build_entry(**common)
                entry.transmit = False

                task = asyncio.Event()
                entry_trade = None

                def on_entry_sl(trade: Trade):
                    nonlocal entry_trade
                    entry_trade = trade
                    task.set()

                observable = await self.executioner.subscribe_place_order_direct(
                    mint_approved_order(contract, entry, is_exit=is_exit, checks=entry_checks,
                                            exit_reason=ExitReason.POSITION_CLASSIFIED))
                observable.subscribe(Observer(on_next=on_entry_sl, on_error=lambda e: task.set(), on_completed=lambda: None))
                await task.wait()

                if entry_trade is None:
                    return SuccessFail.fail(error='Failed to place entry order')

                trades.append(entry_trade)
                parent_id = entry_trade.order.orderId

                sl = StopOrder(
                    action=reverse_action,
                    totalQuantity=quantity,
                    # spec.validate() guarantees stop_loss_price is non-None for STOP_LOSS exits; ib_async stub-types stopPrice as int|float.
                    stopPrice=spec.stop_loss_price,  # ty: ignore[invalid-argument-type]
                    parentId=parent_id,
                    transmit=True,
                    account=self.ib_account,
                    tif=spec.tif,
                    outsideRth=spec.outside_rth,
                )

                sl_task = asyncio.Event()
                sl_trade: Optional[Trade] = None

                def on_sl_only(trade: Trade):
                    nonlocal sl_trade
                    sl_trade = trade
                    sl_task.set()

                sl_obs = await self.executioner.subscribe_place_order_direct(
                    mint_approved_order(contract, sl, is_exit=True,
                                         exit_reason=ExitReason.PROTECTIVE_CHILD))
                sl_obs.subscribe(Observer(on_next=on_sl_only, on_error=lambda e: sl_task.set(), on_completed=lambda: None))
                await sl_task.wait()
                if sl_trade is None:
                    # All-or-nothing: the stop-loss transmits the staged
                    # (transmit=False) entry. If it failed, roll back the entry
                    # rather than returning success for an unprotected order.
                    _cancel_trade_safely(entry_trade)
                    return SuccessFail.fail(
                        error='Stop-loss aborted: protective leg rejected; entry rolled back'
                    )
                trades.append(sl_trade)

            else:
                # NONE — simple entry only
                entry = _build_entry(**common)
                entry.transmit = True

                task = asyncio.Event()
                entry_trade = None

                def on_entry_simple(trade: Trade):
                    nonlocal entry_trade
                    entry_trade = trade
                    task.set()

                observable = await self.executioner.subscribe_place_order_direct(
                    mint_approved_order(contract, entry, is_exit=is_exit, checks=entry_checks,
                                            exit_reason=ExitReason.POSITION_CLASSIFIED))
                observable.subscribe(Observer(on_next=on_entry_simple, on_error=lambda e: task.set(), on_completed=lambda: None))
                await task.wait()
                if entry_trade is None:
                    return SuccessFail.fail(error='Failed to place entry order')
                trades.append(entry_trade)

            # Confirm IB actually ACCEPTED the order — the placeOrder echo above
            # returns a Trade even for an order IB then rejects. Only an explicit
            # rejection downgrades to failure; a slow (timeout) status leaves the
            # result as success, because the order is placed and may be working
            # and reporting failure there would be the more dangerous lie.
            tracker = getattr(self, 'order_tracker', None)
            if tracker is not None and trades:
                entry_id = int(getattr(trades[0].order, 'orderId', 0) or 0)
                if entry_id:
                    verdict = await tracker.wait_decisive(entry_id, timeout=8.0)
                    if verdict == 'rejected':
                        for t in trades:
                            _cancel_trade_safely(t)
                        reason = tracker.latest_status(entry_id) or 'rejected'
                        return SuccessFail.fail(
                            error=f'Order rejected by IB (entry status={reason})')

            return SuccessFail.success(obj=trades)

        except Exception as ex:
            logging.error(f'place_expressive_order error: {ex}')
            return SuccessFail.fail(exception=ex)

    async def place_standalone_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        order_type: str,
        aux_price: float = 0,
        limit_price: float = 0,
        trailing_percent: float = 0,
        tif: str = 'GTC',
        outside_rth: bool = True,
        order_ref: str = '',
    ) -> SuccessFail:
        """Place a standalone order (e.g. protective stop for an existing position).

        order_type: 'STP' (stop), 'TRAIL' (trailing stop), 'LMT' (take-profit limit)

        ``order_ref`` stamps the order's orderRef — for auto-executor
        protective stops this is the strategy name, so a fired stop's fill
        is strategy-attributed in the event store like any other exit.

        This path is exit-class ONLY: the order must reduce the live broker
        position for its conId (a protective stop/trail/limit covering an
        existing position). Anything else — a BUY with no short, an oversized
        SELL — is an ungated exposure door and is refused. Deliberately no
        risk-limit checks beyond that: protective orders must never be
        refusable by limits.
        """
        try:
            is_exit = self.order_reduces_exposure(contract, action, quantity)
            conid = int(getattr(contract, 'conId', 0) or 0)
            held = self._signed_position(conid) if conid > 0 else None
            if not is_exit:
                return SuccessFail.fail(
                    error=(
                        f'standalone orders are protective/exit-class only: '
                        f'{action} {quantity} {getattr(contract, "symbol", "?")} '
                        f'(conId {conid}) does not reduce the live broker position '
                        f'({"unreadable" if held is None else held})'
                    ))
            # order_reduces_exposure now treats an oversize close as exit-class
            # (a flip can't increase the same-direction exposure, so gates
            # mustn't refuse it). A PROTECTIVE order, though, must never exceed
            # the position it protects — an oversized leg would flip the book.
            # Clamp explicitly here (the caller's own qty is meant to match the
            # live position).
            eps = 1e-9 * max(1.0, abs(held or 0.0))
            if held is None or abs(float(quantity)) > abs(held) + eps:
                return SuccessFail.fail(
                    error=(
                        f'standalone order quantity {quantity} exceeds the live '
                        f'{getattr(contract, "symbol", "?")} position '
                        f'({"unreadable" if held is None else held}) — a protective '
                        f'leg must not exceed the position it protects'))

            if order_type == 'STP':
                order = StopOrder(
                    action=action,
                    totalQuantity=quantity,
                    stopPrice=aux_price,
                    account=self.ib_account,
                    tif=tif,
                    outsideRth=outside_rth,
                    transmit=True,
                )
            elif order_type == 'TRAIL':
                order = Order(
                    orderType='TRAIL',
                    action=action,
                    totalQuantity=quantity,
                    account=self.ib_account,
                    tif=tif,
                    outsideRth=outside_rth,
                    transmit=True,
                )
                if trailing_percent:
                    order.trailingPercent = trailing_percent
                elif aux_price:
                    order.auxPrice = aux_price
            elif order_type == 'LMT':
                order = LimitOrder(
                    action=action,
                    totalQuantity=quantity,
                    lmtPrice=limit_price,
                    account=self.ib_account,
                    tif=tif,
                    outsideRth=outside_rth,
                    transmit=True,
                )
            else:
                return SuccessFail.fail(error=f'Unsupported order_type: {order_type}')

            if order_ref:
                order.orderRef = order_ref

            task = asyncio.Event()
            result_trade: Optional[Trade] = None

            def on_next(trade: Trade):
                nonlocal result_trade
                result_trade = trade
                task.set()

            # Standalone orders are exit-class ONLY (validated above): mint a
            # protective-child token. No gate ran, so checks are empty.
            observable = await self.executioner.subscribe_place_order_direct(
                mint_approved_order(contract, order, is_exit=True,
                        exit_reason=ExitReason.VALIDATED_STANDALONE))
            observable.subscribe(Observer(on_next=on_next, on_error=lambda e: task.set(), on_completed=lambda: None))
            await task.wait()

            if result_trade:
                return SuccessFail.success(obj=result_trade)
            else:
                return SuccessFail.fail(error='Failed to place standalone order')

        except Exception as ex:
            logging.error(f'place_standalone_order error: {ex}')
            return SuccessFail.fail(exception=ex)

    @log_method
    async def place_order_simple(
        self,
        contract: Contract,
        action: Action,
        equity_amount: Optional[float],
        quantity: Optional[float],
        limit_price: Optional[float],
        market_order: bool,
        stop_loss_percentage: float,
        algo_name: str = 'global',
        debug: bool = False,
        skip_risk_gate: bool = False,
        approver_key: str = '',
    ) -> Observable[Trade]:
        latest_tick: Ticker = await self.client.get_snapshot(contract)

        contract_order = self.executioner.helper_create_order(
            contract,
            action,
            latest_tick,
            equity_amount,
            quantity,
            limit_price,
            market_order,
            stop_loss_percentage,
            algo_name=algo_name,
            debug=debug
        )

        # Position-value hint for the concentration check: quantity × the
        # best available price (limit price, else the snapshot this path just
        # fetched — ask for a BUY, bid for a SELL). None = no usable price,
        # which the gate treats as not-evaluable and refuses opens on.
        position_value_hint: Optional[float] = None
        try:
            multiplier = float(contract.multiplier) if contract.multiplier else 1.0
        except (TypeError, ValueError):
            multiplier = 1.0
        for candidate in (
            limit_price,
            latest_tick.ask if action == Action.BUY else latest_tick.bid,
        ):
            try:
                # Defensive coercion of a possibly-None limit_price / Any tick field; the except handles non-numeric candidates.
                price = float(candidate)  # ty: ignore[invalid-argument-type]
            except (TypeError, ValueError):
                continue
            if math.isfinite(price) and price > 0:
                position_value_hint = abs(
                    float(contract_order.order.totalQuantity)) * price * multiplier
                break

        # FLIP SPLITTING on the direct path too. This is the path the
        # 2026-07-27 probe used (`mmr sell QBTS --quantity 5` against 3 held),
        # and it is the one a HUMAN reaches for, so it is the likeliest place
        # for an oversized close to be typed by accident. Same contract as
        # place_expressive_order: reduction first and unrefusable, remainder
        # gated as the new exposure it is.
        final_qty = float(contract_order.order.totalQuantity or 0)
        held_signed = self._signed_position(contract.conId)
        if held_signed is not None:
            plan = split_order(str(action), held_signed, final_qty)
            if plan.is_flip:
                logging.warning(
                    'flip split (direct path): %s %s %s crosses zero — reduction %s '
                    '(exit-class) then remainder %s (gated)',
                    action, final_qty, contract.symbol, plan.reduce_qty, plan.open_qty)
                reduce_order = self.executioner.helper_create_order(
                    contract, action, latest_tick, None, plan.reduce_qty,
                    limit_price, market_order, stop_loss_percentage, algo_name, debug)
                reduce_obs = await self.executioner.place_order(
                    contract_order=reduce_order,
                    condition=ExecutorCondition.SANITY_CHECK,
                    position_value_hint=position_value_hint,
                    approver_key=approver_key,
                )
                open_order = self.executioner.helper_create_order(
                    contract, action, latest_tick, None, plan.open_qty,
                    limit_price, market_order, stop_loss_percentage, algo_name, debug)
                # The remainder may be refused; that leaves the caller flat,
                # which is the safe direction. Its observable carries the
                # refusal to the caller exactly as any gated order would.
                await self.executioner.place_order(
                    contract_order=open_order,
                    condition=ExecutorCondition.SANITY_CHECK,
                    position_value_hint=position_value_hint,
                    approver_key=approver_key,
                    force_open=True,
                )
                return reduce_obs

        return await self.executioner.place_order(
            contract_order=contract_order,
            condition=ExecutorCondition.SANITY_CHECK,
            skip_risk_gate=skip_risk_gate,
            position_value_hint=position_value_hint,
            approver_key=approver_key,
        )

    @log_method
    def cancel_order(self, order_id: int) -> Optional[Trade]:
        return self.executioner.cancel_order_id(order_id)

    @log_method
    def cancel_all(self) -> SuccessFail[List[int]]:
        cancelled = []
        failed_cancels = []
        for order_id, _ in self.book.get_orders().items():
            trade: Optional[Trade] = self.cancel_order(order_id)
            if trade:
                cancelled.append(order_id)
            else:
                failed_cancels.append(order_id)

        if failed_cancels:
            return SuccessFail.fail(error=f'Failed to cancel: {failed_cancels}')
        else:
            return SuccessFail.success(obj=cancelled)

    async def scanner_data(self, **kwargs) -> list[dict]:
        return await self.client.scanner_data(**kwargs)

    async def scanner_locations(self) -> list[dict]:
        """List every scanner location this account is authorised for.
        Diagnostic for error 162 (scanner not configured) — if your
        chosen location isn't in this list, either the string is wrong
        or your account/paper mode lacks the subscription."""
        return await self.client.scanner_locations()

    async def get_snapshots_batch(self, contracts, delayed: bool = False) -> list[dict]:
        return await self.client.get_snapshots_batch(contracts, delayed)

    async def get_history_bars(self, contract, duration: str = '60 D', bar_size: str = '1 day') -> list[dict]:
        return await self.client.get_history_bars(contract, duration, bar_size)

    async def get_fundamental_data(self, contract, report_type: str = 'ReportSnapshot') -> str:
        return await self.client.get_fundamental_data(contract, report_type)

    async def get_market_depth(self, contract, num_rows: int = 5, is_smart_depth: bool = False) -> dict:
        return await self.client.get_market_depth(contract, num_rows=num_rows, is_smart_depth=is_smart_depth)

    async def get_news_headlines(self, conId: int, provider_codes: str = '',
                                  total_results: int = 5) -> list[dict]:
        return await self.client.get_news_headlines(conId, provider_codes, total_results)

    def is_ib_connected(self) -> bool:
        return self.client.ib.isConnected()

    def _log_pulse(self) -> None:
        """Periodic heartbeat (scheduled in connected_event). Local reads
        only; never raises. The line's ABSENCE for >~2 intervals means the
        service (or its event loop) is wedged — greppable by the health
        monitor, unlike a silent hang."""
        try:
            book = getattr(self, 'book', None)
            logging.info(
                'pulse ib_connected=%s ib_upstream=%s open_orders=%s',
                self.client.ib.isConnected(),
                self._ib_upstream_connected,
                book.get_open_order_count() if book is not None else 0,
            )
        except Exception as ex:
            logging.warning('pulse failed: %s', ex)

    async def ping_ib(self) -> dict:
        """Live IB socket round-trip (reqCurrentTime with a 5s deadline).

        ``get_status()``'s flags can freeze true on a half-open socket —
        G3's 10.5h invisible outage — because they are driven by error
        codes that never arrive when the socket itself dies. An actual
        request/response is the only honest liveness signal; `mmr verify`
        and the container healthcheck use this instead of the flags.
        """
        try:
            server_time = await asyncio.wait_for(
                self.client.ib.reqCurrentTimeAsync(), timeout=5.0)
            return {'ok': True, 'ib_server_time': str(server_time)}
        except Exception as ex:
            return {'ok': False, 'error': '{}: {}'.format(type(ex).__name__, ex)}

    @log_method
    def red_button(self):
        self.client.ib.reqGlobalCancel()

    # status() is polled heavily by strategy_service, the CLI, and the
    # risk-gate. A 1-second TTL cache is invisible to every caller (these
    # are idempotent-diagnostic reads, not trade decisions) and avoids
    # re-walking IB state on every RPC. No @log_method — the decorator's
    # inspect.signature + repr for every call adds measurable overhead on a
    # hot path, and the RPC server already DEBUG-logs each dispatch.
    def status(self) -> dict:
        now = time.monotonic()
        cached_ts = getattr(self, '_status_cache_ts', 0.0)
        if now - cached_ts < 1.0:
            cached = getattr(self, '_status_cache', None)
            if cached is not None:
                return cached
        status = {
            'ib_connected': self.client.ib.isConnected(),
            'ib_upstream_connected': self._ib_upstream_connected,
            'storage_connected': self.data is not None,
        }
        if not self._ib_upstream_connected:
            status['ib_upstream_error'] = self._ib_upstream_error
        self._status_cache = status
        self._status_cache_ts = now
        return status

    def get_unique_client_id(self) -> int:
        new_client_id = max(self.tws_client_ids) + 1
        self.tws_client_ids.append(new_client_id)
        self.tws_client_ids.append(new_client_id + 1)
        return new_client_id

    def get_pnl(self) -> List[PnLSingle]:
        return self.pnl.get_all()

    # Async + thread-offloaded. This is the method strategy_service calls
    # every reconcile (30s) plus what the CLI's `portfolio` command hits,
    # so it's on a hot path. Iterating the portfolio dict is cheap, but
    # building PortfolioSummary dataclasses and doing the PnL-cache lookup
    # per item was one of the callsites starving the trader_service event
    # loop (RPC handler slow-callback warnings at ~1s). Running the body
    # in a worker thread keeps the loop responsive for ticker dispatch
    # and other RPC requests.
    async def get_portfolio_summary(self) -> List[PortfolioSummary]:
        return await asyncio.to_thread(self._get_portfolio_summary_sync)

    def _get_portfolio_summary_sync(self) -> List[PortfolioSummary]:
        def find_pnl_or_nan(account: str, contract: Contract) -> float:
            if str((account, contract.conId)) in self.pnl.cache:
                return self.pnl.cache[str((account, contract.conId))].dailyPnL
            else:
                return float('nan')

        # Source of truth: always ask ib_async's ib.portfolio() directly.
        # The old path read from self.portfolio (our Portfolio cache), which
        # is populated by the updatePortfolioEvent observer — if that
        # observer chain ever breaks (e.g. event handlers dropped across an
        # IB() replacement), the cache stays empty even though ib.portfolio()
        # returns the live data. Reading directly is O(N) in position count
        # and already fast; no reason to route through the cache.
        portfolio_items = []
        try:
            portfolio_items = self.client.ib.portfolio(
                account=self.ib_account
            ) if self.ib_account else self.client.ib.portfolio()
        except Exception as ex:
            logging.warning(
                'ib.portfolio() failed, falling back to local cache: %s', ex,
            )
            portfolio_items = self.portfolio.get_portfolio_items()

        # If ib_async returned empty (e.g. subscription not ready) but our
        # local cache has items from a prior event, prefer the cache —
        # belt-and-braces for the reverse failure.
        if not portfolio_items and self.portfolio.portfolio_items:
            portfolio_items = self.portfolio.get_portfolio_items()

        summary: List[PortfolioSummary] = []
        for portfolio_item in portfolio_items:
            summary.append(PortfolioSummary(
                contract=portfolio_item.contract,
                position=portfolio_item.position,
                marketValue=portfolio_item.marketValue,
                averageCost=portfolio_item.averageCost,
                unrealizedPNL=portfolio_item.unrealizedPNL,
                realizedPNL=portfolio_item.realizedPNL,
                account=portfolio_item.account,
                marketPrice=portfolio_item.marketPrice,
                dailyPNL=find_pnl_or_nan(portfolio_item.account, portfolio_item.contract)
            ))
        return summary

    def get_positions(self) -> List[Position]:
        # See _get_portfolio_summary_sync for rationale — hit ib_async
        # directly rather than relying on the event-driven local cache.
        try:
            positions = self.client.ib.positions(
                account=self.ib_account
            ) if self.ib_account else self.client.ib.positions()
            if positions:
                return list(positions)
        except Exception as ex:
            logging.warning('ib.positions() failed, using cache: %s', ex)
        return self.portfolio.get_positions()

    async def reconcile_with_broker(self) -> dict:
        """Cross-check recent proposals + positions against live IB truth.

        REPORT-ONLY: fetches IB open orders, executions and positions, compares
        them to the proposal store and current positions, and returns a
        divergence report. Places/cancels nothing and mutates no proposal status.
        """
        from trader.trading.reconciliation import reconcile
        from trader.data.proposal_store import ProposalStore

        def _action(o):
            return str(getattr(o, 'action', '') or '')

        # Open orders — reqAllOpenOrders returns Trade objects (order+contract+status).
        open_orders = []
        try:
            for t in (await self.client.get_open_orders()) or []:
                order = getattr(t, 'order', t)
                contract = getattr(t, 'contract', None)
                st = getattr(t, 'orderStatus', None)
                open_orders.append({
                    'order_id': int(getattr(order, 'orderId', 0) or 0),
                    'conId': int(getattr(contract, 'conId', 0) or 0) if contract else 0,
                    'symbol': getattr(contract, 'symbol', '') if contract else '',
                    'action': _action(order),
                    'orderType': str(getattr(order, 'orderType', '') or ''),
                    'status': str(getattr(st, 'status', '') or ''),
                })
        except Exception as ex:
            logging.warning('reconcile: get_open_orders failed: %s', ex)

        executions = []
        try:
            for fill in (await self.client.get_executions()) or []:
                ex_obj = getattr(fill, 'execution', None)
                contract = getattr(fill, 'contract', None)
                executions.append({
                    'order_id': int(getattr(ex_obj, 'orderId', 0) or 0) if ex_obj else 0,
                    'conId': int(getattr(contract, 'conId', 0) or 0) if contract else 0,
                    'symbol': getattr(contract, 'symbol', '') if contract else '',
                    'side': str(getattr(ex_obj, 'side', '') or '') if ex_obj else '',
                    'shares': float(getattr(ex_obj, 'shares', 0.0) or 0.0) if ex_obj else 0.0,
                    'price': float(getattr(ex_obj, 'price', 0.0) or 0.0) if ex_obj else 0.0,
                })
        except Exception as ex:
            logging.warning('reconcile: get_executions failed: %s', ex)

        positions = []
        try:
            for p in self.get_positions() or []:
                contract = getattr(p, 'contract', None)
                positions.append({
                    'conId': int(getattr(contract, 'conId', 0) or 0) if contract else 0,
                    'symbol': getattr(contract, 'symbol', '') if contract else '',
                    'position': float(getattr(p, 'position', 0.0) or 0.0),
                })
        except Exception as ex:
            logging.warning('reconcile: get_positions failed: %s', ex)

        proposals = []
        try:
            store = ProposalStore(self.duckdb_path)
            proposals = (store.query(status='EXECUTED', limit=100)
                         + store.query(status='APPROVED', limit=100))
        except Exception as ex:
            logging.warning('reconcile: proposal query failed: %s', ex)

        report = reconcile(proposals, open_orders, executions, positions)
        for f in report.findings:
            level = logging.error if f.severity == 'critical' else logging.warning
            level('reconcile [%s] %s (proposal=%s): %s',
                  f.severity, f.symbol, f.proposal_id, f.detail)
        if not report.findings:
            logging.info('reconcile: no divergence (%d proposals, %d positions, '
                         '%d open orders, %d executions checked)',
                         report.checked_proposals, report.checked_positions,
                         report.ib_open_orders, report.ib_executions)
        return report.to_dict()

    def diagnose_portfolio_feed(self) -> dict:
        """Dump raw IB portfolio/positions from every managed account.

        Bypasses MMR's ``Portfolio`` cache (which is populated by event
        callbacks) and hits ``ib.portfolio(account)`` / ``ib.positions(account)``
        directly. Used to diagnose the "status shows $1M in margin,
        positions=0" class of bug — usually means MMR is filtering by
        the wrong account string (FA paper accounts have sub-accounts),
        or the init subscriptions timed out and the event cache never
        populated."""
        result = {
            'configured_ib_account': self.ib_account,
            'managed_accounts': [],
            'accounts_from_client': [],
            'cache_portfolio_count': len(self.portfolio.portfolio_items),
            'cache_position_count': len(self.portfolio.positions),
            'per_account': {},
        }
        try:
            result['managed_accounts'] = list(self.client.ib.managedAccounts() or [])
        except Exception as ex:
            result['managed_accounts_error'] = str(ex)
        try:
            result['accounts_from_client'] = list(self.client.ib.client.getAccounts() or [])
        except Exception as ex:
            result['accounts_from_client_error'] = str(ex)

        # Try every account we know about, plus the empty-string "default"
        # query and the configured ib_account. Dedup.
        targets = set(result['managed_accounts'])
        targets.update(result['accounts_from_client'])
        if self.ib_account:
            targets.add(self.ib_account)
        targets.add('')  # empty = IB's default (= single managed account)

        for acct in sorted(targets):
            info: dict = {}
            try:
                items = self.client.ib.portfolio(account=acct) if acct else self.client.ib.portfolio()
                info['portfolio_count'] = len(items)
                info['portfolio_sample'] = [
                    {
                        'symbol': it.contract.symbol,
                        'secType': it.contract.secType,
                        'position': it.position,
                        'marketValue': it.marketValue,
                        'account': it.account,
                    }
                    for it in items[:5]
                ]
            except Exception as ex:
                info['portfolio_error'] = str(ex)
            try:
                positions = self.client.ib.positions(account=acct) if acct else self.client.ib.positions()
                info['positions_count'] = len(positions)
            except Exception as ex:
                info['positions_error'] = str(ex)
            result['per_account'][acct or '(default)'] = info
        return result

    @log_method
    async def get_shortable_shares(self, contract: Contract) -> float:
        return await self.client.get_shortable_shares(contract)

    @log_method
    def release_client_id(self, client_id: int):
        if client_id in self.tws_client_ids:
            self.tws_client_ids.remove(client_id)

    def start_load_test(self):
        async def _load_test_helper():
            amd = Contract(symbol='AMD', conId=4391, exchange='SMART', primaryExchange='NASDAQ', currency='USD')
            ticker = Ticker(
                contract=amd,
                time=dt.datetime.now(),
                bid=87.05,
                ask=87.06,
                prevBid=87.05,
                prevAsk=87.06,
                askSize=100.0,
                bidSize=100.0,
                prevAskSize=100.0,
                prevBidSize=100.0,
                lastSize=0,
                halted=0,
                close=85.00,
                low=84.00,
                high=86.00,
                open=85.50,
                last=87.05,
            )
            counter = 0
            timer = dt.datetime.now()
            while self.load_test:
                self.client._contracts_source.on_next(set([ticker]))

                # asyncio.sleep(0)
                # any asyncio.sleep here seems to give us a 100x slowdown.
                # await asyncio.sleep(0.000001)
                # sleep 0.000001 give us about 9000 /sec.
                # asyncio.sleep(0) gives us about 29k tickers/sec
                # no sleep gives us 400k/sec but no active control over the process
                counter = counter + 1
                delta = dt.datetime.now() - timer
                if delta.seconds >= 10:
                    task_num = len(asyncio.all_tasks())
                    threading_num = threading.active_count()
                    logging.critical(
                        '{} tickers per second, {} tasks, {} threads'.format(
                            float(counter) / 10.0,
                            task_num,
                            threading_num
                        )
                    )
                    counter = 0
                    timer = dt.datetime.now()
            logging.debug('load test stopped')

        self.load_test = True
        logging.critical('starting start_load_test()')
        task = asyncio.create_task(_load_test_helper())

    def run(self, *args):
        self.client.run(*args)
