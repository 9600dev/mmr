from ib_async.contract import Contract
from ib_async.objects import PnL, PnLSingle, PortfolioItem, Position
from ib_async.order import LimitOrder, MarketOrder, Order, StopLimitOrder, StopOrder, Trade
from ib_async.ticker import Ticker
from ib_async.util import UNSET_DOUBLE
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
from trader.trading.order_math import order_notional
from trader.trading.order_reference import split_order_reference
from trader.trading.order_split import SplitPlan, split_order
from trader.trading.order_structure import rejection_for_order
from trader.trading.risk_gate import RiskGate, RiskGateResult, RiskInputs, RiskLimits
from trader.listeners.ibreactive import IBAIORx, IBAIORxError
from trader.messaging.clientserver import MessageBusServer, MultithreadedTopicPubSub, RPCClient, RPCServer
from trader.objects import Action, ContractOrderPair, ExecutorCondition
from trader.trading.book import BookSubject
from trader.trading.executioner import TradeExecutioner, WorkingOrdersUnreadableError, _working_reduction_quantity
from trader.trading.portfolio import Portfolio
from trader.trading.strategy import Strategy, StrategyConfig, StrategyState
from typing import Any, Callable, cast, Dict, List, NamedTuple, Optional, Tuple, Union

import asyncio
import copy
from contextlib import asynccontextmanager
import backoff
import datetime as dt
import hmac
import hashlib
import inspect
import json
from contextvars import ContextVar
from dataclasses import dataclass, field
import math
import os
import reactivex as rx
import reactivex.operators as ops
import threading
import time
import trader.messaging.strategy_service_api as strategy_bus
import trader.messaging.trader_service_api as bus
import uuid


logging = setup_logging(module_name='trading_runtime')

# Identifies this trader_service PROCESS. Live market-data subscriptions exist
# only in its memory, so a restart silently drops every one of them — and
# subscribers cannot tell, because a reconnected RPC socket looks identical to
# one that never dropped. Exposed via status(); strategy_service re-subscribes
# when it changes. Found live 2026-07-27: restarting trader_service alone left
# every strategy blind for 30 minutes with both services reporting healthy.
_BOOT_ID = uuid.uuid4().hex

# Order-path parameters that carry a credential rather than describe the
# order. They are excluded from the durable intent fingerprint.
_SECRET_ARGUMENT_NAMES = frozenset({'approver_key'})
_SECRET_ARGUMENT_SUFFIXES = ('_key', '_secret', '_password', '_token')


def _is_secret_argument(name: str) -> bool:
    return name in _SECRET_ARGUMENT_NAMES or name.endswith(_SECRET_ARGUMENT_SUFFIXES)


_CURRENT_INTENT: ContextVar[Optional[str]] = ContextVar('mmr_order_intent', default=None)
_CURRENT_JOURNAL: ContextVar[Any] = ContextVar('mmr_order_journal', default=None)

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


@dataclass
class _AccountPnLSubscription:
    """One IB request and the value actually received for that request."""

    ib: Any
    account: str
    pnl: Optional[PnL] = None
    callback: Optional[Callable[[PnL], None]] = None
    value: Optional[float] = None
    request_started: bool = False
    pending: list[tuple[PnL, Optional[float]]] = field(default_factory=list)


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
        self._submitted_trades: list[Trade] = []
        self._order_lock: Optional[asyncio.Lock] = None
        self._order_lock_owner: Any = None
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
        # PnLSingle remains a display feed. Risk needs the exact account total,
        # including positions closed by other clients before this process ran.
        self._account_pnl_lock = threading.RLock()
        self._account_pnl_subscription: Optional[_AccountPnLSubscription] = None
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
        # Ticks the bounded publisher queue refused. Counted, never raised into
        # the shared Rx ticker stream: an exception there is terminal for every
        # subscriber and silenced the whole broadcast until restart.
        self.zmq_pubsub_dropped_ticks: int = 0
        self._pubsub_drop_logged_at: float = 0.0
        self._pubsub_drops_at_last_log: int = 0
        # Monotonic times the shared ticker subscription was rebuilt after an
        # error; bounds a tight rebuild loop on a broken source.
        self._publish_reestablish_times: List[float] = []
        # Coalesces the connected_event double-fire (eventkit emit + explicit call
        # on reconnect) so re-subscription doesn't run twice concurrently.
        self._in_connected_event: bool = False
        # Single sink for IB order-status truth (fill/cancel/reject events +
        # acceptance queries). Built here so it exists before the first
        # connected_event/setup_subscriptions runs; its event store is wired in
        # connect() and it's attached to orderStatusEvent in setup_subscriptions.
        from trader.trading.order_lifecycle import OrderLifecycleTracker
        self.order_tracker = OrderLifecycleTracker(None)
        self._execution_history_ready = False
        self._execution_replay_lock = asyncio.Lock()
        # Wall-clock instant the CURRENT session's broker replay completed;
        # None until it has. Reservations claimed after this instant, under
        # this client id, would have produced an order-status callback had
        # their send reached IB (see unobserved_reduction_quantity).
        self._execution_replay_completed_epoch: Optional[float] = None
        self._execution_replay_failed: bool = False
        # Identities behind the most recent unobserved-capacity evaluation,
        # so a DEFERRED reason can name what is reserving the shares.
        self._unobserved_reservation_detail: List[dict] = []

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
        self._ib_upstream_ib: Any = None
        self._ib_ping_completed: tuple[Any, float] | None = None

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
        self._invalidate_account_pnl()
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
        for name, method in (('zmq_pubsub_server', 'stop'), ('zmq_messagebus', 'stop'),
                             ('zmq_strategy_client', 'close')):
            resource = getattr(self, name, None)
            close = getattr(resource, method, None)
            if callable(close):
                await asyncio.to_thread(close)
        # ROUTER socket/task ownership belongs to this asyncio thread.
        self.zmq_rpc_server.close()
        if getattr(self, 'order_tracker', None) is not None:
            await asyncio.to_thread(self.order_tracker.close)
        await self.client.shutdown()

    @log_method
    def reconnect(self):
        # this will force a reconnect through the disconnected event
        self._invalidate_account_pnl()
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
        # Start this independently of portfolio events: a flat account can
        # still have a daily loss. reqPnL sends without waiting for a callback.
        self._ensure_account_pnl_subscription()

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
            exec_event = self.client.ib.execDetailsEvent
            try:
                exec_event.disconnect(self.order_tracker.on_execution)
            except Exception:
                pass
            exec_event.connect(self.order_tracker.on_execution, keep_ref=True)
            self._execution_history_ready = False
            await self._replay_broker_executions()


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
        # A startup replay that timed out leaves replay_required=True, which
        # refuses every open. Keep retrying on the same scheduler.
        self._start_execution_replay_retry()

    def _on_ib_error(self, error: IBAIORxError):
        """Track IB upstream connectivity from error codes.

        IB distinguishes two severities we care about:

        1. **1100 / 1101 / 1102**: full gateway↔IBKR connectivity. 1100 means
           trading is actually disabled. This is the only IB error code
           that clears ``ib_upstream_connected``; local socket loss also
           makes the upstream session unavailable.

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
            # Real disconnect — account inputs are invalid until a new receipt
            # after 1101 (data lost) or 1102 (data maintained) restores service.
            self._ib_upstream_connected = False
            self._ib_upstream_error = msg
            self._ib_upstream_ib = getattr(getattr(self, 'client', None), 'ib', None)
            self._invalidate_account_pnl()
            logging.warning('IB upstream connection lost (code 1100): %s', msg)
        elif code in (1101, 1102):
            self._invalidate_account_pnl()
            self._ib_upstream_ib = getattr(getattr(self, 'client', None), 'ib', None)
            self._ib_upstream_connected = True
            self._ib_upstream_error = ''
            self._ensure_account_pnl_subscription()
            logging.info('IB upstream connection restored (code %d): %s', code, msg)
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
        ib = self.client.ib
        if ib.isConnected() is True and getattr(self, '_ib_upstream_ib', None) is not ib:
            # An old IB object's loss flag is not a status report for this
            # connection. A 1100 already received from THIS instance remains
            # authoritative. Account risk still needs its new PnL callback.
            self._invalidate_account_pnl()
            self._ib_upstream_ib = ib
            self._ib_upstream_connected = True
            self._ib_upstream_error = ''
        # Replacement IB objects can arrive while a prior setup is unwinding.
        # Bind the account feed before coalescing or awaiting other setup work.
        self._ensure_account_pnl_subscription()
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
        self._invalidate_account_pnl()
        # A delayed event from the replaced IB object must not clear an
        # already connected replacement. Keep any explicit 1100 cause.
        if self.client.ib.isConnected() is not True:
            if self._ib_upstream_connected or not self._ib_upstream_error:
                self._ib_upstream_error = 'IB Gateway socket disconnected'
            self._ib_upstream_connected = False
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
                    logging.error('reconnection attempt %d failed: %s: %s',
                                  attempt, type(ex).__name__, ex)
        finally:
            self._reconnecting = False

    @log_method
    async def enable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        try:
            # Initialization may use the strategy worker's 60-second budget.
            # Keep broker callbacks responsive while strategy reconciliation
            # makes its own nested RPC calls back into this service.
            return await asyncio.to_thread(self.zmq_strategy_client.rpc(timeout=120).enable_strategy, name)
        except Exception as ex:
            logging.error('enable_strategy: {}'.format(ex))
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)

    @log_method
    async def disable_strategy(self, name: str) -> SuccessFail[StrategyState]:
        try:
            return await asyncio.to_thread(self.zmq_strategy_client.rpc(timeout=20).disable_strategy, name)
        except Exception as ex:
            logging.error('disable_strategy: {}'.format(ex))
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)

    @log_method
    async def get_strategies(self) -> SuccessFail[List[StrategyConfig]]:
        try:
            rpc_call = await asyncio.to_thread(self.zmq_strategy_client.rpc().get_strategies)
            return SuccessFail.success(rpc_call)
        except Exception as ex:
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)

    @log_method
    async def reload_strategies(self) -> SuccessFail[List[StrategyConfig]]:
        try:
            return await asyncio.to_thread(self.zmq_strategy_client.rpc(timeout=120).reload_strategies)
        except Exception as ex:
            logging.error('reload_strategies: {}'.format(ex))
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)

    @log_method
    async def adopt_legacy_holding(self, strategy: str, conid: int,
                                   avg_cost: Optional[float] = None) -> SuccessFail[dict]:
        """Forward an operator's ownership attestation to the strategy service.

        The executor corroborates the attributed quantity against a fresh
        broker position read (which calls back into this service), so this
        stays off the broker callback thread like the other strategy controls.
        """
        try:
            return await asyncio.to_thread(
                self.zmq_strategy_client.rpc(timeout=60).adopt_legacy_holding, strategy, int(conid), avg_cost)
        except Exception as ex:
            logging.error('adopt_legacy_holding: {}'.format(ex))
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)

    @log_method
    async def list_execution_intents(self, strategy: Optional[str] = None, conid: Optional[int] = None,
                                     active_only: bool = True) -> list[dict]:
        """Forward the executor intent listing to strategy_service (like adopt).

        The list return type has no failure channel, so an unreachable
        strategy_service raises ``ConnectionError`` — the RPC layer preserves
        stdlib exception types to the caller.
        """
        try:
            rows = await asyncio.to_thread(
                self.zmq_strategy_client.rpc(timeout=60).list_execution_intents,
                strategy, None if conid is None else int(conid), bool(active_only))
            return list(rows or [])
        except Exception as ex:
            logging.error('list_execution_intents: {}'.format(ex))
            raise ConnectionError(f'strategy_service unreachable: {str(ex) or type(ex).__name__}') from ex

    @log_method
    async def resolve_execution_intent(self, intent_id: str, reason: str) -> SuccessFail[dict]:
        """Forward an operator's intent resolution to strategy_service (like adopt)."""
        try:
            return await asyncio.to_thread(
                self.zmq_strategy_client.rpc(timeout=60).resolve_execution_intent, str(intent_id), str(reason))
        except Exception as ex:
            logging.error('resolve_execution_intent: {}'.format(ex))
            return SuccessFail.fail(error=str(ex) or type(ex).__name__, exception=ex)

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
                sec_type=sec_type,
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

        # These three callbacks observe the SHARED ticker stream
        # (client.contracts_subject, one subscription for every published
        # contract). With reactivex an exception escaping on_next is turned
        # into a terminal on_error on that chain, after which no ticker for
        # ANY contract is published again. So on_next must never raise, and
        # on_error must rebuild the whole chain rather than forget one conId.
        def on_next(ticker: Ticker):
            try:
                self.zmq_pubsub_server.put(('ticker', ticker))
            except Exception as ex:
                self._note_dropped_tick(ex)

        def on_completed():
            logging.info('ticker publish stream completed; clearing %d published contract filter(s)',
                         len(self.zmq_pubsub_contract_filters))
            self._clear_ticker_publish_state()

        def on_error(ex):
            self._reestablish_ticker_publishing(ex)

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

    # A full publisher queue is logged at most this often; the count says how
    # many ticks went missing in between.
    _PUBSUB_DROP_LOG_INTERVAL_S = 10.0
    # More rebuilds than this inside the window means the source itself is
    # broken; stop rebuilding and wait for the reconnect path to do it.
    _PUBLISH_REESTABLISH_WINDOW_S = 60.0
    _PUBLISH_REESTABLISH_LIMIT = 5

    def _note_dropped_tick(self, ex: Exception) -> None:
        """Count a tick the publisher refused (queue full, publisher stopped)
        and log it at a bounded rate. Never raises: it runs inside the shared
        Rx ticker chain, where an exception is terminal for every contract."""
        self.zmq_pubsub_dropped_ticks = getattr(self, 'zmq_pubsub_dropped_ticks', 0) + 1
        now = time.monotonic()
        if now - getattr(self, '_pubsub_drop_logged_at', 0.0) >= self._PUBSUB_DROP_LOG_INTERVAL_S:
            since_last = self.zmq_pubsub_dropped_ticks - getattr(self, '_pubsub_drops_at_last_log', 0)
            logging.error('ticker publish dropped %d tick(s) since last report (%d total): %s: %s',
                          since_last, self.zmq_pubsub_dropped_ticks, type(ex).__name__, ex)
            self._pubsub_drop_logged_at = now
            self._pubsub_drops_at_last_log = self.zmq_pubsub_dropped_ticks

    def _clear_ticker_publish_state(self) -> None:
        """Drop the shared subscription and every per-contract filter. The
        remembered publish requests survive, so a rebuild knows what to ask for."""
        try:
            self.zmq_pubsub_contract_subscription.dispose()
        except Exception:
            pass
        self.zmq_pubsub_contract_subscription = Disposable()
        self.zmq_pubsub_contracts = {}
        self.zmq_pubsub_contract_filters = {}

    def _reestablish_ticker_publishing(self, ex: Exception) -> None:
        """on_error for the shared ticker chain: the chain is dead for ALL
        contracts, so clear everything and rebuild from the remembered
        requests. Bounded so a source that errors on every subscribe cannot
        spin; past the bound the state is left cleared for connected_event."""
        logging.error('ticker publish stream errored (%d published contract(s)); rebuilding: %s: %s',
                      len(self.zmq_pubsub_contract_filters), type(ex).__name__, ex)
        self._clear_ticker_publish_state()
        now = time.monotonic()
        self._publish_reestablish_times = [
            t for t in getattr(self, '_publish_reestablish_times', [])
            if now - t < self._PUBLISH_REESTABLISH_WINDOW_S]
        if len(self._publish_reestablish_times) >= self._PUBLISH_REESTABLISH_LIMIT:
            logging.error('ticker publish stream failed %d times in %.0fs; not rebuilding until the next '
                          'IB (re)connect. Live ticker broadcast is DOWN.',
                          len(self._publish_reestablish_times), self._PUBLISH_REESTABLISH_WINDOW_S)
            return
        self._publish_reestablish_times.append(now)
        try:
            self._republish_ticker_subscriptions()
        except Exception as rebuild_error:
            logging.error('ticker publish rebuild failed: %s: %s', type(rebuild_error).__name__, rebuild_error)

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

    # Trader.place_order used to sit here: a direct call into
    # executioner.place_order outside serialized_orders() and outside any
    # durable intent, with no callers. Every placement now enters through
    # place_order_simple / place_expressive_order / place_standalone_order /
    # resize_position, all of which run under _run_order_intent.

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

    @asynccontextmanager
    async def serialized_orders(self):
        """Serialize exposure decisions through submission, reentrant for split legs."""
        task = asyncio.current_task()
        if getattr(self, '_order_lock_owner', None) is task:
            yield
            return
        if getattr(self, '_order_lock', None) is None:
            self._order_lock = asyncio.Lock()
        assert self._order_lock is not None
        async with self._order_lock:
            self._order_lock_owner = task
            try:
                yield
            finally:
                self._order_lock_owner = None

    def _journal_recovered(self) -> None:
        """A durable journal operation succeeded; clear the outage flag.

        ``_journal_degraded`` is set wherever a journal call fails, including
        read-only lookups, and used to be cleared nowhere, so one transient
        ``database is locked`` disabled new exposure until the process was
        restarted. Every open still fails closed at the actual reservation and
        submission-audit writes, so clearing on a successful transaction cannot
        admit an open that the journal would refuse.
        """
        previous = getattr(self, '_journal_degraded', '')
        if previous:
            logging.info('execution journal recovered after: %s', previous)
        self._journal_degraded = ''

    async def _probe_journal_if_degraded(self) -> None:
        """Re-test a flagged journal with a real transaction before refusing an open."""
        if not getattr(self, '_journal_degraded', ''):
            return
        try:
            journal = await asyncio.to_thread(self.server_order_journal)
            await asyncio.to_thread(journal.reservations, self.ib_account)
        except Exception as ex:
            self._journal_degraded = str(ex)
            return
        self._journal_recovered()

    async def margin_checks(self, contract: Contract, order: Order) -> RiskGateResult:
        """Shared opening-order margin policy for every execution path."""
        restored = self.opening_restore_error()
        if restored:
            return RiskGateResult(False, reason=restored,
                                  checks={'broker_reconciliation': 'unevaluable:restored-state'})
        tracker = getattr(self, 'order_tracker', None)
        await self._probe_journal_if_degraded()
        if getattr(self, '_journal_degraded', '') or (tracker is not None and not tracker.health['healthy']):
            return RiskGateResult(False, reason='execution journal degraded; new exposure is disabled',
                                  checks={'execution_journal': 'unevaluable:durable-storage'})
        if (contract.secType or '').upper() == 'CASH':
            return RiskGateResult(True, checks={
                'leverage': 'skipped:forex-cash', 'margin_cushion': 'skipped:forex-cash'})
        impact = await self._margin_impact_or_refusal(contract, order)
        if isinstance(impact, SuccessFail):
            return RiskGateResult(False, reason=impact.error or 'margin unavailable',
                                  checks={'leverage': 'unevaluable:margin-data'})
        inputs = self.gather_risk_inputs()
        if self.risk_gate is None:
            return RiskGateResult(False, reason='risk gate unavailable')
        return self.risk_gate.check_leverage(
            impact, inputs.portfolio_value if inputs.portfolio_value_evaluable else 0.0)

    def _restore_marker_directory(self) -> Optional[str]:
        database_path = getattr(self, 'duckdb_path', None)
        if not isinstance(database_path, (str, os.PathLike)):
            return None
        return os.path.dirname(os.fspath(database_path))

    def opening_restore_error(self) -> Optional[str]:
        """A restored journal cannot authorize exposure until explicitly reconciled."""
        directory = self._restore_marker_directory()
        if directory is None:
            return None
        marker = os.path.join(directory, 'BROKER_RECONCILIATION_REQUIRED.json')
        try:
            os.stat(marker)
        except FileNotFoundError:
            return None
        except OSError as ex:
            return f'broker reconciliation marker cannot be read: {ex}'
        return ('broker reconciliation is required after database restore; new exposure is disabled '
                '(review `mmr execution-snapshot`, then `mmr restore-marker ack --reason "..."`)')

    def restore_marker_status(self) -> dict:
        """What the restore marker says and whether it is refusing opens."""
        from trader.data.db_backup import read_restore_marker
        directory = self._restore_marker_directory()
        if directory is None:
            status: dict = {'present': False, 'path': None, 'marker': None, 'error': None}
        else:
            status = read_restore_marker(directory)
        reason = self.opening_restore_error()
        status['opening_blocked'] = bool(reason)
        status['reason'] = reason
        return status

    async def acknowledge_restore_marker(self, reason: str) -> dict:
        """Operator acknowledgment of the restore marker: the deliberate human
        act after which the stack may open exposure again.

        Requires a non-empty reason and a FRESH, COMPLETE broker snapshot —
        the acknowledgment says "I reviewed reconciliation against broker
        truth", and a snapshot that is incomplete (IB down, replay pending,
        account unconfirmed) is not something that could have been reviewed.
        The snapshot's identity is recorded as evidence beside the reason.
        Never invoked automatically.
        """
        from trader.data.db_backup import acknowledge_restore_marker, read_restore_marker
        if not isinstance(reason, str) or not reason.strip():
            return {'acknowledged': False, 'error': 'a non-empty reason is required'}
        directory = self._restore_marker_directory()
        if directory is None:
            return {'acknowledged': False, 'error': 'no database directory configured; nothing to acknowledge'}
        if not read_restore_marker(directory)['present']:
            return {'acknowledged': False, 'error': 'no restore marker present; opens are not blocked by a restore'}
        try:
            snapshot = await self.execution_snapshot()
        except Exception as ex:
            return {'acknowledged': False, 'error': f'refused: broker snapshot unavailable: {type(ex).__name__}: {ex}'}
        summary = {field: snapshot.get(field) for field in (
            'complete', 'positions_complete', 'orders_complete', 'executions_complete',
            'account_confirmed', 'journal_healthy', 'observed_at')}
        if not (snapshot.get('complete') and snapshot.get('positions_complete')):
            return {'acknowledged': False,
                    'error': 'refused: broker snapshot is not complete; reconciliation cannot have been '
                             'reviewed against it (see snapshot fields)',
                    'snapshot': summary}
        evidence = dict(summary, account=self.ib_account, boot_id=_BOOT_ID,
                        positions=len(snapshot.get('positions') or []),
                        orders=len(snapshot.get('orders') or []))
        # Serialized with placements so an open cannot evaluate the marker in
        # the middle of its removal.
        async with self.serialized_orders():
            try:
                record = await asyncio.to_thread(acknowledge_restore_marker, directory, reason, 'operator', evidence)
            except FileNotFoundError:
                return {'acknowledged': False, 'error': 'no restore marker present; opens are not blocked by a restore'}
            except Exception as ex:
                return {'acknowledged': False, 'error': f'acknowledgment not persisted: {type(ex).__name__}: {ex}'}
        logging.warning('RESTORE MARKER ACKNOWLEDGED by operator: %s (record %s); new exposure is enabled again',
                        reason.strip(), record['path'])
        await self._mirror_audit_event(
            'RESTORE_MARKER_ACKNOWLEDGED', strategy_name='operator',
            metadata={'reason': record['reason'], 'path': record['path'], 'evidence': evidence,
                      'acknowledged_at': record['acknowledged_at']})
        return {'acknowledged': True, **record}

    def fx_rates_to_base(self) -> dict[str, float]:
        """Finite, account-scoped conversion rates, with an explicit base row.

        reqAccountUpdates delivers per-currency FX as ``$LEDGER-ExchangeRate``
        rows (ib_async's rendering of IB's ``$LEDGER:ALL`` summary); only some
        account types expose the plain ``ExchangeRate`` tag. Both are read and
        the ledger form wins, exactly as ``get_account_cash_by_currency`` does.
        Matching the plain tag alone returned ``{base: 1.0}`` on a live ledger
        account, which made every non-base position value non-evaluable and
        refused every non-base open fail-closed.
        """
        ledger: dict[str, float] = {}
        plain: dict[str, float] = {}
        base_currency = None
        for value in self.client.ib.accountValues():
            if value.account != self.ib_account or not value.currency or value.currency == 'BASE':
                continue
            if value.tag == 'NetLiquidation':
                # The account's own NetLiquidation row is denominated in the
                # base currency; its rate is 1.0 by definition.
                base_currency = value.currency
                continue
            if value.tag not in ('$LEDGER-ExchangeRate', 'ExchangeRate'):
                continue
            try:
                rate = float(value.value)
            except (ValueError, TypeError):
                continue
            if math.isfinite(rate) and rate > 0:
                (ledger if value.tag.startswith('$LEDGER-') else plain)[value.currency] = rate
        rates = {**plain, **ledger}
        if base_currency:
            rates[base_currency] = 1.0
        return rates

    def convert_notional(self, value: float, currency: str, target: str = 'BASE') -> Optional[float]:
        """Convert financial units explicitly; missing FX never means rate one."""
        if not currency or not math.isfinite(value) or value < 0:
            return None
        if currency == target:
            return value
        try:
            rates = self.fx_rates_to_base()
            source_rate = rates[currency]
            target_rate = 1.0 if target == 'BASE' else rates[target]
            converted = value * source_rate / target_rate
            return converted if math.isfinite(converted) else None
        except (KeyError, TypeError, ValueError, AttributeError):
            return None

    def aggregate_position_value(self, contract: Contract, action: str,
                                 quantity: float, order_value: float) -> float:
        """Post-order exposure including same-direction working openings."""
        held = self._signed_position(int(contract.conId or 0))
        if held is None or not math.isfinite(held) or quantity <= 0:
            return float('nan')
        sign = 1 if str(action).upper() == 'BUY' else -1
        pending = 0.0
        for trade in self.working_trades(contract):
            order = trade.order
            if str(order.action).upper() == str(action).upper():
                # Not orderStatus.remaining alone: a native PendingSubmit
                # reports remaining=0 until IB acknowledges it, which read a
                # just-submitted same-direction open as no exposure at all.
                # Same treatment as the reduction coordinator.
                pending += _working_reduction_quantity(trade)
        return abs(held + sign * (quantity + pending)) * (order_value / quantity)

    def working_trades(self, contract: Contract) -> list:
        """Broker-known working orders plus locally submitted orders awaiting updates.

        Raises ``WorkingOrdersUnreadableError`` when the broker set cannot be
        read. It used to swallow that and continue with the locally submitted
        list alone, i.e. report the broker's working orders as EMPTY — the one
        answer that lets a second executable close be sent against the same
        shares and lets the concentration check under-count. Callers treat the
        raise as unevaluable: opens refuse, reductions defer.
        """
        self._submitted_trades = [t for t in getattr(self, '_submitted_trades', [])
                                  if t.orderStatus.status not in {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'}]
        candidates = list(self._submitted_trades)
        try:
            open_trades = self.client.ib.openTrades()
            candidates.extend(open_trades)
        except Exception as ex:
            raise WorkingOrdersUnreadableError(
                f'UNKNOWN: working orders unreadable: {type(ex).__name__}: {ex}') from ex
        unique = {}
        for trade in candidates:
            order = trade.order
            if (getattr(trade.contract, 'conId', None) != contract.conId
                    or getattr(order, 'account', '') != self.ib_account
                    or trade.orderStatus.status in {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'}):
                continue
            key = (getattr(order, 'clientId', 0), order.orderId)
            unique[key] = trade
        return list(unique.values())

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
            # get_positions owns account filtering at the broker/cache boundary.
            if c is not None and int(getattr(c, 'conId', 0) or 0) == conid:
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
        False (gated like an open, fail-closed). A symbol never substitutes for
        an exact conId: stocks, options and futures can share the same ticker.
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
        if held is None:
            return False
        # The DECISION itself lives in the pure, deal-contracted, mutation- and
        # CrossHair-checked kernel (trader.trading.exit_class). This method's
        # remaining job is resolving `held` from the live portfolio; the
        # direction rule is verified there, where the toolchain can see it.
        return reduces_exposure(act, held, qty)

    def split_for_order(
        self, contract: Contract, action: str, quantity: float
    ) -> SplitPlan:
        """Divide ``action quantity`` on ``contract`` into the half that
        REDUCES the live broker position and the half that OPENS new exposure.

        This is the ONE place the system answers that question. The proposal
        gate, the approver notional tier and both order paths all call it, so
        they cannot drift apart and re-open the flip residual from opposite
        sides. That is not hypothetical: the residual survived its own fix
        once, because the RPC-layer proposal gate asked a DIFFERENT question
        (the unsplit exit-class boolean) and exempted the whole order before
        the splitter downstream ever saw it. Found live 2026-07-27.

        Position resolution mirrors ``order_reduces_exposure`` exactly: the
        exact conId in the pinned broker account.

        FAIL-CLOSED — the WHOLE quantity is opening, so it faces every gate —
        when the position is unreadable, when the conId is blank, or when the
        action is not BUY/SELL. The arithmetic itself
        lives in the pure, contracted, mutation- and CrossHair-checked kernel
        (``trader.trading.order_split``); this method only resolves ``held``.
        """
        try:
            conid = int(getattr(contract, 'conId', 0) or 0)
            qty = float(quantity or 0.0)
        except (TypeError, ValueError):
            try:
                return SplitPlan(0.0, abs(float(quantity or 0.0)))
            except (TypeError, ValueError):
                return SplitPlan(0.0, 0.0)
        if qty <= 0 or not math.isfinite(qty):
            return SplitPlan(0.0, 0.0)
        act = str(action).strip().upper()
        if act not in ('BUY', 'SELL'):
            return SplitPlan(0.0, qty)  # fail-closed: unknown action → opening
        held = self._signed_position(conid) if conid > 0 else None
        if held is None:
            return SplitPlan(0.0, qty)  # fail-closed: unreadable → opening
        return split_order(act, held, qty)

    def _opening_exposure_quantity(
        self, contract: Contract, action: str, quantity: float
    ) -> float:
        """The portion of ``action quantity`` that opens NET-NEW exposure —
        the only quantity the approver notional tier gates.

        Thin accessor over ``split_for_order`` so the tier and the splitter
        can never disagree about how much of an order is new exposure.
        """
        return self.split_for_order(contract, action, quantity).open_qty

    async def _tier_notional(
        self, contract: Contract, action: str, quantity: float,
        order_type: str, limit_price: Optional[float], target_currency: str = 'USD',
        anchor: str = 'marketable', limit_pushes_up: bool = True,
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

        ``anchor='last'`` values at the last trade first (then the marketable
        side, then close) and ``limit_pushes_up=False`` ignores the client
        limit. That is the anchor ``sdk.approve`` sizes an amount-budgeted
        proposal with, so the budget match re-derives the same figure instead
        of a systematically higher marketable one.
        """
        multiplier = TradeExecutioner._multiplier(contract)

        act = str(action).strip().upper()
        snapshot_price: Optional[float] = None
        try:
            tick = await self.client.get_snapshot(contract)
            marketable = tick.ask if act == 'BUY' else tick.bid
            last = getattr(tick, 'last', None)
            candidates = ((last, marketable) if anchor == 'last' else (marketable, last))
            for candidate in (*candidates, getattr(tick, 'close', None)):
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
        if limit_pushes_up and str(order_type).strip().upper() != 'MARKET':
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
        # The POLICY (anchor on the live snapshot, allow a client limit to push
        # the valuation up but never down, refuse without a snapshot) stays
        # here, because it is specific to defending against a forged-low
        # notional. Only the ARITHMETIC is shared with the audit trail, which
        # has the opposite failure preference: best-effort valuation, and record
        # that it could not value rather than refuse to place.
        price = max(snapshot_price, limit_val) if limit_val is not None else snapshot_price
        notional, evaluable = order_notional((price,), quantity, multiplier)
        converted = self.convert_notional(notional, contract.currency, target_currency) if evaluable else None
        return (converted, True) if converted is not None else (0.0, False)

    async def enforce_approver_tier(
        self, contract: Contract, action: str, quantity: float,
        order_type: str, limit_price: Optional[float], approver_key: str,
        force_open: bool = False,
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
        # Exit-class orders are NEVER gated, using the same server-side
        # classifier the rest of the system trusts. The reason to keep asking
        # `order_reduces_exposure` here rather than recomputing an opening
        # remainder still holds: an order that reduces the live position must
        # never be blocked by an approval requirement, and a second position
        # read that comes back unreadable would gate a GENUINE exit.
        #
        # `force_open` is what keeps that exemption honest. It is set for the
        # OPENING half of a split flip, whose live position read still shows
        # the pre-reduction size and would therefore classify as an exit — so
        # without it this tier hands the remainder the very exemption the split
        # exists to take away. Verified: a $50k opening short with no approver
        # key was exempted as a flip remainder while the identical order from
        # flat was refused. Same shape as the RPC proposal-gate hole found live
        # the same day, in a third layer; found by auditing the other askers
        # rather than by another probe.
        #
        # A flip should never reach this method unsplit — both order paths
        # decompose upstream — so this is defence in depth for the remainder.
        if not force_open and self.order_reduces_exposure(contract, action, quantity):
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

    def _account_pnl_is_current(self, subscription: _AccountPnLSubscription,
                                *, pending: bool = False) -> bool:
        """Called under the account lock; all reads are local IB cache reads."""
        try:
            if (getattr(self, '_account_pnl_subscription', None) is not subscription
                    or self.client.ib is not subscription.ib
                    or self.ib_account != subscription.account
                    or not getattr(self, '_ib_upstream_connected', False)
                    or subscription.ib.isConnected() is not True):
                return False
            pnl = subscription.pnl
            if pnl is None:
                return pending
            return (pnl.account == subscription.account and pnl.modelCode == ''
                    and any(item is pnl for item in subscription.ib.pnl(
                        subscription.account, '')))
        except Exception:
            return False

    def _invalidate_account_pnl(self) -> None:
        """Revoke readiness before any disconnect/reconnect wait or cleanup."""
        lock = getattr(self, '_account_pnl_lock', None)
        if lock is None:
            return
        with lock:
            subscription = self._account_pnl_subscription
            self._account_pnl_subscription = None
            if subscription is None:
                return
            subscription.value = None
            subscription.pending.clear()
            # IBAIORx preserves event handlers when replacing its IB object.
            # Remove our saved callback from both, never a freshly bound method.
            current_ib = getattr(getattr(self, 'client', None), 'ib', None)
            sources = [subscription.ib]
            if current_ib is not None and current_ib is not subscription.ib:
                sources.append(current_ib)
            for ib in sources:
                try:
                    ib.pnlEvent.disconnect(subscription.callback)
                except Exception as ex:
                    logging.warning('account PnL callback cleanup failed: %s', ex)
            if subscription.request_started:
                try:
                    # reqPnL registers the request BEFORE sending it. A send
                    # failure can therefore need cancellation even when no PnL
                    # object was returned; otherwise every retry asserts.
                    subscription.ib.cancelPnL(subscription.account, '')
                except Exception as ex:
                    logging.warning('account PnL request cleanup failed: %s', ex)

    def _ensure_account_pnl_subscription(self) -> None:
        """On the IB loop, request exactly the pinned account's full PnL.

        A finite object returned by reqPnL is not a receipt. Only pnlEvent for
        that exact request can warm risk inputs, including after reconnect.
        """
        if not hasattr(self, '_account_pnl_lock'):
            self._account_pnl_lock = threading.RLock()
            self._account_pnl_subscription = None
        with self._account_pnl_lock:
            subscription = self._account_pnl_subscription
            if subscription is not None and self._account_pnl_is_current(
                    subscription, pending=True):
                return
            self._invalidate_account_pnl()
            ib = getattr(getattr(self, 'client', None), 'ib', None)
            account = getattr(self, 'ib_account', None)
            if (ib is None or not isinstance(account, str) or not account.strip()
                    or account != account.strip()
                    or not getattr(self, '_ib_upstream_connected', False)):
                return
            try:
                if ib.isConnected() is not True:
                    return
            except Exception:
                return

            subscription = _AccountPnLSubscription(ib=ib, account=account)
            self._account_pnl_subscription = subscription

            def observe(pnl: PnL) -> None:
                with self._account_pnl_lock:
                    if not self._account_pnl_is_current(subscription, pending=True):
                        return
                    if (getattr(pnl, 'account', None) != account
                            or getattr(pnl, 'modelCode', None) != ''):
                        return
                    raw = getattr(pnl, 'dailyPnL', None)
                    value = None
                    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                        try:
                            numeric = float(raw)
                            if math.isfinite(numeric) and abs(numeric) < UNSET_DOUBLE:
                                value = numeric
                        except (OverflowError, ValueError):
                            pass
                    if subscription.pnl is None:
                        # A synchronous callback during reqPnL is allowed, but
                        # it still must match the object that request returns.
                        subscription.pending.append((pnl, value))
                    elif pnl is subscription.pnl:
                        subscription.value = value

            subscription.callback = observe
            try:
                ib.pnlEvent.connect(observe, keep_ref=True)
                subscription.request_started = True
                subscription.pnl = ib.reqPnL(account, '')
                if not self._account_pnl_is_current(subscription):
                    raise ValueError('account PnL request did not return its registered account object')
                for pnl, value in subscription.pending:
                    if pnl is subscription.pnl:
                        subscription.value = value
                subscription.pending.clear()
            except Exception as ex:
                if self._account_pnl_subscription is subscription:
                    self._invalidate_account_pnl()
                logging.warning('account PnL subscription unavailable: %s', ex)

    def _read_account_daily_pnl(self) -> tuple[float, bool]:
        """Read one coherent callback checkpoint, without requesting data."""
        lock = getattr(self, '_account_pnl_lock', None)
        if lock is None:
            return 0.0, False
        with lock:
            subscription = self._account_pnl_subscription
            if (subscription is None or subscription.value is None
                    or not self._account_pnl_is_current(subscription)):
                return 0.0, False
            return subscription.value, True

    def gather_risk_inputs(self) -> RiskInputs:
        """Read the account state the risk gate needs, marking per-field
        evaluability — "read succeeded, value is 0" is distinct from "could
        not read". Shared by every gate call site (executioner.place_order
        and place_expressive_order) so daily-loss and concentration are
        never silently no-op'd against defaults.
        """
        open_order_count = self.book.get_open_order_count() if hasattr(self, 'book') else 0

        daily_pnl, daily_pnl_evaluable = self._read_account_daily_pnl()

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
        allow_open: bool = True,
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

        if allow_open:
            open_result = await self.place_expressive_order(
                contract, action, plan.open_qty, execution_spec,
                algo_name=algo_name, approver_key=approver_key, force_open=True)
        else:
            open_result = SuccessFail.fail(error='opening exposure requires an approved proposal matching this exact order')

        reduction = {'status': 'SUBMITTED',
                     'quantity': sum(float(t.order.totalQuantity) for t in reduce_result.obj or []),
                     'order_ids': [t.order.orderId for t in reduce_result.obj or []]}
        if not open_result.is_success():
            logging.warning(
                'flip split: reduction of %s placed; opening remainder of %s '
                'REFUSED by the gates (%s); reduction execution remains pending',
                plan.reduce_qty, plan.open_qty, open_result.error)
            result = SuccessFail.fail(
                error=f'PARTIAL: flip split: reduction {plan.reduce_qty:g} submitted, but the '
                      f'opening remainder of {plan.open_qty:g} was refused: '
                      f'{open_result.error}')
            result.obj = {'reduction': reduction,
                          'opening': {'status': 'UNKNOWN' if 'UNKNOWN' in str(open_result.error) else 'REJECTED',
                                      'quantity': plan.open_qty, 'error': open_result.error}}
            return result

        trades = list(reduce_result.obj or []) + list(open_result.obj or [])
        result = SuccessFail.success(obj=trades)
        result.execution_outcome = {'reduction': reduction,
                                    'opening': {'status': 'SUBMITTED', 'quantity': plan.open_qty,
                                                'order_ids': [t.order.orderId for t in open_result.obj or []]}}
        return result

    async def place_expressive_order(
        self, contract: Contract, action: str, quantity: float, execution_spec: dict,
        algo_name: str = 'proposal', approver_key: str = '', force_open: bool = False,
        allow_open: bool = True, client_intent_id: str = '',
    ) -> SuccessFail:
        args = (contract, action, quantity, execution_spec)
        kwargs = dict(algo_name=algo_name, approver_key=approver_key,
                      force_open=force_open, allow_open=allow_open)
        return await self._run_order_intent('expressive', client_intent_id,
                                           self._place_expressive_order, args, kwargs)

    async def _run_order_intent(self, operation: str, intent_id: str, call,
                                args: tuple, kwargs: dict) -> SuccessFail:
        async with self.serialized_orders():
            if not intent_id:
                if _CURRENT_INTENT.get():
                    # Split and protective legs are part of their parent's
                    # claim, so one recovery identity retains every broker ID.
                    return await call(*args, **kwargs)
                if isinstance(getattr(self, 'duckdb_path', None), str):
                    intent_id = f'{operation}:{uuid.uuid4().hex}'
                else:
                    return await call(*args, **kwargs)
            if not isinstance(intent_id, str) or len(intent_id.encode()) > 160 or '|mmr:' in intent_id:
                return SuccessFail.fail(error='client_intent_id must be a string of at most 160 bytes without the reserved delimiter')
            # Capture the complete normalized wire intent, including exact identity.
            bound = inspect.signature(call).bind(*args, **kwargs)
            bound.apply_defaults()
            # Positional/keyword spelling and mapping insertion order are not
            # part of an intent. The complete semantic payload is. Credentials
            # are not: the approver key authorizes the attempt, it does not
            # describe the order, so a retry carrying a different (or newly
            # supplied) key is the SAME intent — and the journal must never
            # hold an unsalted hash of a secret.
            payload = {'operation': operation,
                       'arguments': {name: value for name, value in bound.arguments.items()
                                     if not _is_secret_argument(name)}}
            def encode(value):
                if isinstance(value, Contract):
                    from dataclasses import asdict
                    return asdict(value)
                if isinstance(value, Action):
                    return str(value)
                return repr(value)
            fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, default=encode).encode()).hexdigest()
            try:
                emergency = getattr(self, '_emergency_order_journal', None)
                journal = (emergency if emergency is not None and emergency.get(intent_id)
                           else await asyncio.to_thread(self.server_order_journal))
                if not await asyncio.to_thread(journal.claim, intent_id, fingerprint, self.ib_account):
                    prior = await asyncio.to_thread(journal.get, intent_id)
                    if prior and prior['status'] == 'REJECTED':
                        return SuccessFail.fail(error=prior['error'])
                    return SuccessFail.fail(error=f'UNKNOWN: intent {intent_id} already claimed; '
                                            f'reconcile reserved broker orders {prior}')
            except ValueError as ex:
                return SuccessFail.fail(error=str(ex))
            except Exception as ex:
                self._journal_degraded = str(ex)
                contract = bound.arguments.get('contract')
                action = str(bound.arguments.get('action', ''))
                quantity = bound.arguments.get('quantity', 0)
                if (operation not in {'expressive', 'protective', 'simple'} or contract is None
                        or not self.order_reduces_exposure(contract, action, quantity)):
                    return SuccessFail.fail(error=f'Cannot durably claim intent {intent_id}: {ex}')
                from trader.data.server_order_journal import EmergencyOrderJournal
                journal = getattr(self, '_emergency_order_journal', None)
                if journal is None:
                    journal = EmergencyOrderJournal()
                    self._emergency_order_journal = journal
                if not journal.claim(intent_id, fingerprint, self.ib_account):
                    return SuccessFail.fail(error=f'UNKNOWN: emergency intent {intent_id} already claimed; reconcile broker orders')
                if operation in {'expressive', 'simple'}:
                    kwargs['allow_open'] = False
                logging.error('DURABILITY DEGRADED: submitting only the reduction for intent %s: %s', intent_id, ex)
            token = _CURRENT_INTENT.set(intent_id)
            journal_token = _CURRENT_JOURNAL.set(journal)
            try:
                result = await call(*args, **kwargs)
                if isinstance(result, Observable):
                    observable = result
                    result = SuccessFail.success(obj=await observable.pipe(ops.take(1)))
                    outcome = getattr(observable, 'execution_outcome', None)
                    if outcome is not None:
                        result.execution_outcome = outcome
                journal = _CURRENT_JOURNAL.get()
                reserved = await asyncio.to_thread(journal.get, intent_id)
                outcome = (result.obj if isinstance(result.obj, dict)
                           else getattr(result, 'execution_outcome', None))
                partial = bool(outcome and outcome.get('reduction', {}).get('status') == 'SUBMITTED'
                               and outcome.get('opening', {}).get('status') == 'REJECTED')
                unknown_opening = bool(outcome and outcome.get('opening', {}).get('status') == 'UNKNOWN')
                status = ('UNKNOWN' if unknown_opening else 'PARTIAL' if partial else
                          'SUBMITTED' if result.is_success() else
                          'UNKNOWN' if reserved and reserved['orders'] else
                          'RETRYABLE' if 'UNKNOWN:' in str(result.error) else 'REJECTED')
                await asyncio.to_thread(journal.finish, intent_id, status, str(result.error or ''), outcome)
                result.client_intent_id = intent_id
                if status == 'UNKNOWN':
                    result.error = f'UNKNOWN: intent {intent_id}: {result.error}'
                return result
            except Exception as ex:
                try:
                    reserved = await asyncio.to_thread(journal.get, intent_id)
                    status = ('UNKNOWN' if reserved and reserved['orders'] else
                              'RETRYABLE' if 'UNKNOWN:' in str(ex) else 'REJECTED')
                    await asyncio.to_thread(journal.finish, intent_id, status, str(ex))
                except Exception as storage_error:
                    self._journal_degraded = str(storage_error)
                    status = 'UNKNOWN'
                return SuccessFail.fail(error=f'{status}: intent {intent_id}: {ex}')
            finally:
                _CURRENT_INTENT.reset(token)
                _CURRENT_JOURNAL.reset(journal_token)

    def server_order_journal(self):
        from trader.data.server_order_journal import ServerOrderJournal
        journal = getattr(self, '_server_order_journal', None)
        if journal is None:
            journal = ServerOrderJournal(self.duckdb_path)
            self._server_order_journal = journal
        return journal

    # Reservation classification --------------------------------------------
    #
    # A reservation is a physical (intent, client, order) identity claimed
    # BEFORE a broker send. Until exact broker evidence retires it, it holds
    # executable capacity on its instrument. The decision of whether a claim
    # is retired, matched to a broker row, or still reserving lives in ONE
    # place, _classify_reservation, and both the capacity sum a close is
    # deferred on (unobserved_reduction_quantity) and the row an operator
    # sees (list_order_reservations) read that verdict. Two implementations
    # of one rule is the shape that produced the flip residual.

    _TERMINAL_STATUSES = frozenset({'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'})
    # A same-session, own-client reservation this young may still have its
    # order-status callback in flight; its absence proves nothing yet.
    _PHANTOM_SETTLE_GRACE_S = 30.0

    async def _load_reservations(self, account: str, include_settled: bool = False) -> tuple[list[dict], Any, Any]:
        """Durable reservation rows (else the complete process cache) merged
        with the emergency journal. Reads stay off the IB loop. Returns
        ``(rows, journal_or_None, emergency_or_None)``; ``journal`` is None
        when the durable read failed and the cache answered."""
        journal = None
        try:
            journal = await asyncio.to_thread(self.server_order_journal)
            # reservations() is the unsettled read every capacity decision goes
            # through (and the seam tests fail on purpose); the wider listing
            # is operator-only and never feeds the cache.
            if include_settled:
                rows = await asyncio.to_thread(journal.list_reservations, account, True)
            else:
                rows = await asyncio.to_thread(journal.reservations, account)
                self._server_reservation_cache = (account, rows)
            self._reservation_cache_error = ''
            self._journal_recovered()
        except Exception as ex:
            self._reservation_cache_error = str(ex)
            cached = getattr(self, '_server_reservation_cache', None)
            if cached is None or cached[0] != account:
                raise RuntimeError('UNKNOWN: durable competing reduction capacity is unreadable') from ex
            # This is valid only under the documented one-authoritative-
            # Trader-per-account contract: all this process's later reserves
            # update the cache. A new process starts without this proof.
            rows = cached[1]
            logging.warning('durable capacity read failed; using complete process cache: %s', ex)
        emergency = getattr(self, '_emergency_order_journal', None)
        merged = {(r['intent_id'], r['client_id'], r['order_id']): r for r in rows}
        emergency_rows = [] if emergency is None else (
            emergency.list_reservations(account, True) if include_settled else emergency.reservations(account))
        for row in emergency_rows:
            key = (row['intent_id'], row['client_id'], row['order_id'])
            previous = merged.get(key)
            if previous is not None and any(previous.get(field) != row.get(field)
                                            for field in ('conid', 'action', 'quantity', 'broker_reference')):
                raise RuntimeError('UNKNOWN: conflicting durable and emergency order reservations')
            if previous is not None:
                counts = (previous['leg_count'], row['leg_count'])
                row = dict(row, leg_count=max(counts) if all(counts) else 0)
            merged[key] = row
        return list(merged.values()), journal, emergency

    def _reservation_view(self, account: str, rows: list[dict]) -> dict:
        """Everything a classification needs besides the claim itself: the
        tracker's observations indexed by reference and by identity, the
        per-reference topology of the claims, whether the broker view is
        complete, and which identities this IB session has handed to
        ``placeOrder`` (None when that cannot be read)."""
        tracker = getattr(self, 'order_tracker', None)
        by_reference: dict[str, list[dict]] = {}
        by_identity: dict[tuple, list[dict]] = {}
        for observation in tracker.snapshot() if tracker is not None else []:
            if observation.get('account') != account:
                continue
            by_identity.setdefault((observation.get('clientId', 0), observation.get('orderId')), []).append(observation)
            if observation.get('clientIntentId'):
                by_reference.setdefault(observation['clientIntentId'], []).append(observation)
        # A migrated legacy claim records only a physical (clientId, orderId)
        # identity, never its instrument. Once this process has replayed the
        # broker's open orders and available execution history, every working
        # order placed under ITS OWN client id has been observed with that id:
        # a legacy identity of this client that is absent from the view is not
        # working and holds no future executable capacity, whatever it once
        # was. Before that replay completes the absence proves nothing. Another
        # client id's working orders are reported with orderId 0 and can never
        # be matched by identity, so those claims keep deferring.
        broker_view_complete = bool(
            getattr(self, '_execution_history_ready', False) and tracker is not None
            and not tracker.health['replay_required'] and self.client.ib.isConnected())
        topology: dict[str, set[tuple]] = {}
        topology_counts: dict[str, list[int]] = {}
        for reservation in rows:
            reference = reservation.get('broker_reference') or reservation['intent_id']
            topology.setdefault(reference, set()).add((reservation['client_id'], reservation['order_id']))
            topology_counts.setdefault(reference, []).append(reservation['leg_count'])
        # Identities ib_async has seen this session (placeOrder records the
        # Trade before the wire send) plus our own receipts. Unreadable is
        # None, and None never certifies a phantom.
        session_identities: Optional[set[tuple]] = None
        try:
            identities = {(t.order.clientId, t.order.orderId) for t in self.client.ib.trades()}
            identities |= {(t.order.clientId, t.order.orderId) for t in getattr(self, '_submitted_trades', [])}
            session_identities = identities
        except Exception:
            session_identities = None
        return dict(by_reference=by_reference, by_identity=by_identity, topology=topology,
                    topology_counts=topology_counts, broker_view_complete=broker_view_complete,
                    own_client_id=getattr(self, 'trading_runtime_ib_client_id', None),
                    session_identities=session_identities,
                    replay_completed_epoch=getattr(self, '_execution_replay_completed_epoch', None),
                    now=time.time())

    def _is_unobserved_own_session_send(self, reservation: dict, identity_rows: list,
                                        related_rows: list, view: dict) -> bool:
        """The narrow case in which absence IS evidence: a normally scoped
        claim under THIS client id, reserved in the CURRENT IB session after
        this process's broker replay completed, that the tracker has never
        seen under any identity, that ib_async never handed to placeOrder,
        and that is old enough for its status callback to have arrived. Had
        the send reached IB, this session would have received orderStatus
        for it. Anything predating the replay, or from another session, or
        from another client, or whose provenance cannot be read, is left to
        exact broker evidence."""
        if reservation.get('conid') is None or identity_rows or related_rows:
            return False
        if not view['broker_view_complete'] or reservation['client_id'] != view['own_client_id']:
            return False
        session = view['session_identities']
        if session is None or (reservation['client_id'], reservation['order_id']) in session:
            return False
        reserved_at = reservation.get('reserved_at')
        completed = view['replay_completed_epoch']
        if (not isinstance(reserved_at, (int, float)) or isinstance(reserved_at, bool)
                or not math.isfinite(reserved_at) or reserved_at <= 0
                or not isinstance(completed, (int, float)) or isinstance(completed, bool)
                or not math.isfinite(completed)):
            return False
        if reserved_at < completed:
            return False  # predates this session's replay: IB's bounded history decides, not absence
        return view['now'] - reserved_at >= self._PHANTOM_SETTLE_GRACE_S

    def _classify_reservation(self, reservation: dict, view: dict) -> dict:
        """One verdict for one physical claim. Pure with respect to storage.

        ``settle`` means broker evidence (or, narrowly, its proven absence)
        retires the claim; ``settle_reason`` says which rule. ``scope_error``
        is set when a legacy claim's instrument or direction cannot be proven,
        which defers every reduction until replay completes. Otherwise the
        claim's scope (``conid``, ``side``, ``quantity``) and the single
        unambiguous tracker ``observation`` (or None) are returned for the
        capacity arithmetic in ``_reserved_quantity``.
        """
        client_id, order_id = reservation['client_id'], reservation['order_id']
        reference = reservation.get('broker_reference') or reservation['intent_id']
        candidates = view['by_reference'].get(reference, [])
        matching = [row for row in candidates if row.get('clientId') == client_id
                    and row.get('orderId') == order_id]
        if (not matching and len(view['topology'][reference]) == 1
                and all(count == 1 for count in view['topology_counts'][reference]) and len(candidates) == 1):
            row = candidates[0]
            if (row.get('orderId') == 0 and row.get('permId', 0) > 0
                    and row.get('clientId') in (0, client_id)):
                matching = [row]
        observation = matching[0] if len(matching) == 1 and not matching[0].get('identityAmbiguous', False) else None
        conid, side, quantity = (reservation.get('conid'), reservation.get('action'), reservation.get('quantity'))
        identity_rows = view['by_identity'].get((client_id, order_id), [])
        verdict = dict(physical_key=(reference, client_id, order_id), reference=reference,
                       observation=None, conid=conid, side=side, quantity=quantity,
                       settle=False, settle_reason=None, scope_error=None)
        if observation is None and conid is None:
            # Legacy claim: the only evidence is its physical identity, and
            # legacy orderRefs need not carry the intent reference, so
            # look the identity up directly rather than by reference.
            if len(identity_rows) == 1 and not identity_rows[0].get('identityAmbiguous', False):
                observation = identity_rows[0]
            elif not identity_rows and view['broker_view_complete'] and client_id == view['own_client_id']:
                verdict.update(settle=True, settle_reason='legacy-own-client-absent-after-replay')
                return verdict
        if observation is not None and (conid is not None and observation.get('conId') != conid
                                         or side is not None and observation.get('action') != side):
            observation = None  # a contradictory row cannot release this physical claim
        verdict['observation'] = observation
        if (observation is not None and observation.get('fillQuantityKnown') is True
                and observation.get('status') in self._TERMINAL_STATUSES):
            verdict.update(settle=True, settle_reason='terminal-known-fill')
            return verdict
        if observation is None and conid is not None:
            related = [row for row in candidates if row.get('orderId') in (0, order_id)]
            if self._is_unobserved_own_session_send(reservation, identity_rows, related, view):
                verdict.update(settle=True, settle_reason='phantom-own-session')
                return verdict
        # For a legacy reservation, only the exact broker row can supply
        # scope/quantity. The migration itself supplies none of these.
        if conid is None and observation is not None:
            conid, side, quantity = (observation.get('conId'), observation.get('action'), observation.get('totalQuantity'))
            verdict.update(conid=conid, side=side, quantity=quantity)
        if (not isinstance(conid, int) or isinstance(conid, bool) or conid <= 0
                or side not in {'BUY', 'SELL'}):
            verdict['scope_error'] = (
                'UNKNOWN: legacy competing order has no proven instrument or direction '
                f'(intent {reservation["intent_id"]}, client {client_id}, order {order_id}); '
                'deferring until the broker open-order and execution replay completes')
        return verdict

    def _reserved_quantity(self, verdict: dict, visible: dict) -> float:
        """Executable capacity a classified, still-open claim reserves, net of
        the confirmed fills and the visible working remainder of its own
        broker row. Raises ``RuntimeError('UNKNOWN: ...')`` when a quantity
        it needs cannot be read."""
        quantity = verdict['quantity']
        observation = verdict['observation']
        if (not isinstance(quantity, (int, float)) or isinstance(quantity, bool)
                or not math.isfinite(quantity) or not 0 < quantity < UNSET_DOUBLE):
            raise RuntimeError('UNKNOWN: competing order quantity is unavailable')
        reserve = float(quantity)
        if observation is not None and observation.get('status') != 'Unknown':
            filled = observation.get('filled')
            if (not isinstance(filled, (int, float)) or isinstance(filled, bool)
                    or not math.isfinite(filled) or filled < 0 or filled >= UNSET_DOUBLE):
                raise RuntimeError('UNKNOWN: competing cumulative fill quantity is unreadable')
            reserve = max(0.0, reserve - filled - visible.get(verdict['physical_key'], 0.0))
        return reserve

    def _visible_working(self, contract: Contract, action: str) -> dict:
        return {(split_order_reference(trade.order.orderRef)[1], trade.order.clientId, trade.order.orderId):
                _working_reduction_quantity(trade)
                for trade in self.working_trades(contract) if trade.order.action == action}

    async def unobserved_reduction_quantity(self, contract: Contract, action: str) -> float:
        """Capacity of attempted orders absent from the live Trade collection.

        A complete open-order read cannot prove that an older unknown send
        never executed. Only exact broker final-quantity evidence retires a
        claim — plus one narrow, audited exception: a claim under this client
        id, reserved in the current IB session after replay, that neither the
        tracker nor ib_async has ever seen (see _is_unobserved_own_session_send).
        Reads/writes of the indexed active claims stay off the IB loop.
        """
        account = self.ib_account
        rows, journal, emergency = await self._load_reservations(account)
        view = self._reservation_view(account, rows)
        visible = self._visible_working(contract, action)
        settled: list[tuple] = []
        phantoms: list[tuple[tuple, dict]] = []
        missing: dict[tuple, float] = {}
        detail: list[dict] = []
        for reservation in rows:
            key = (reservation['intent_id'], reservation['client_id'], reservation['order_id'])
            verdict = self._classify_reservation(reservation, view)
            if verdict['settle']:
                if verdict['settle_reason'] == 'phantom-own-session':
                    phantoms.append((key, reservation))
                else:
                    settled.append(key)
                continue
            if verdict['scope_error']:
                raise RuntimeError(verdict['scope_error'])
            if verdict['conid'] != contract.conId or verdict['side'] != action:
                continue
            reserve = self._reserved_quantity(verdict, visible)
            # A verified in-place modification preserves the original broker
            # reference and physical ID. Parent claims are not extra orders.
            physical_key = verdict['physical_key']
            missing[physical_key] = max(missing.get(physical_key, 0.0), reserve)
            if reserve > 0:
                detail.append(dict(intent_id=reservation['intent_id'], client_id=reservation['client_id'],
                                   order_id=reservation['order_id'], quantity=reserve))
        self._unobserved_reservation_detail = detail
        for key, reservation in phantoms:
            # Absence-as-evidence is retired DURABLY with its reason, or not at
            # all: an in-memory-only release would come back after a restart
            # and, worse, would have let a close through in between.
            if journal is None:
                continue
            evidence = dict(rule='phantom-own-session', reserved_at=reservation.get('reserved_at'),
                            replay_completed_epoch=view['replay_completed_epoch'],
                            client_id=view['own_client_id'], boot_id=_BOOT_ID)
            try:
                await asyncio.to_thread(
                    journal.record_settlement, *key, account=account, actor='trader_service:automatic',
                    reason='unconfirmed own-client send in the current IB session, after broker replay, '
                           'never observed by the lifecycle tracker or ib_async; it did not reach IB',
                    evidence=evidence)
            except Exception as ex:
                logging.warning('could not persist phantom reservation settlement %s: %s', key, ex)
                continue
            logging.warning('RESERVATION SETTLED automatically: intent %s client %s order %s (%s %s x %s) '
                            'was reserved this session after replay and never observed; releasing its capacity',
                            key[0], key[1], key[2], reservation.get('action'), reservation.get('conid'),
                            reservation.get('quantity'))
            settled.append(key)
        if settled:
            # Keep the source claim/audit intact. This index records only the
            # authoritative retirement of its executable capacity.
            evidence_based = [key for key in settled if key not in {k for k, _ in phantoms}]
            if journal is not None and evidence_based:
                try:
                    await asyncio.to_thread(journal.settle_reservations, evidence_based)
                except Exception as ex:
                    logging.warning('could not persist terminal reduction capacity: %s', ex)
            if emergency is not None:
                emergency.settle_reservations(settled)
            keys = set(settled)
            cached = getattr(self, '_server_reservation_cache', None)
            if cached is not None and cached[0] == account:
                cached[1][:] = [row for row in cached[1]
                                if (row['intent_id'], row['client_id'], row['order_id']) not in keys]
        total = sum(missing.values())
        if not math.isfinite(total):
            raise RuntimeError('UNKNOWN: competing reduction capacity overflow')
        return total

    @staticmethod
    def _iso(epoch: Any) -> Optional[str]:
        if not isinstance(epoch, (int, float)) or isinstance(epoch, bool) or not math.isfinite(epoch):
            return None
        return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat()

    @staticmethod
    def _observation_summary(observation: Optional[dict]) -> Optional[dict]:
        if observation is None:
            return None
        return {field: observation.get(field) for field in (
            'status', 'brokerStatus', 'filled', 'remaining', 'fillQuantityKnown', 'totalQuantity',
            'permId', 'clientId', 'orderId', 'conId', 'action', 'clientIntentId', 'identityAmbiguous')}

    async def list_order_reservations(self, account: Optional[str] = None,
                                      include_settled: bool = False) -> list[dict]:
        """Operator view of the physical reservation ledger.

        Each row carries the claim, the matching tracker observation (or
        None), and ``blocking``: whether it currently reserves executable
        capacity on its instrument, computed by the SAME classification the
        reduction coordinator uses. A blocking row with ``observation`` None
        is the phantom-send signature. Read-only: it never settles anything.
        """
        account = account or self.ib_account
        rows, _journal, _emergency = await self._load_reservations(account, include_settled)
        view = self._reservation_view(account, rows)
        visible_cache: dict[tuple, Any] = {}
        result = []
        for reservation in rows:
            verdict = self._classify_reservation(reservation, view)
            blocking = False
            reserved: Optional[float] = None
            note: Optional[str] = None
            if reservation.get('settled'):
                note = 'settled'
            elif verdict['settle']:
                note = f'retired on next evaluation: {verdict["settle_reason"]}'
            elif verdict['scope_error']:
                blocking = True
                note = verdict['scope_error']
            else:
                scope = (verdict['conid'], verdict['side'])
                try:
                    if scope not in visible_cache:
                        visible_cache[scope] = self._visible_working(Contract(conId=int(scope[0])), str(scope[1]))
                    visible = visible_cache[scope]
                    if isinstance(visible, Exception):
                        raise visible
                    reserved = self._reserved_quantity(verdict, visible)
                    blocking = reserved > 0
                    note = 'reserving executable capacity' if blocking else 'matched to a broker observation'
                except Exception as ex:
                    visible_cache.setdefault(scope, ex)
                    blocking = True  # unreadable defers, exactly as the coordinator does
                    note = str(ex)
            result.append(dict(
                intent_id=reservation['intent_id'], client_id=reservation['client_id'],
                order_id=reservation['order_id'], account=reservation.get('account', account),
                conid=verdict['conid'], action=verdict['side'], quantity=verdict['quantity'],
                is_exit=None if reservation.get('is_exit') is None else bool(reservation.get('is_exit')),
                broker_reference=reservation.get('broker_reference') or reservation['intent_id'],
                created=self._iso(reservation.get('created_at')),
                reserved_at=self._iso(reservation.get('reserved_at')),
                intent_status=reservation.get('intent_status'),
                leg_count=reservation.get('leg_count'),
                settled=bool(reservation.get('settled')),
                observation=self._observation_summary(verdict['observation']),
                blocking=blocking, reserved_quantity=reserved, note=note))
        return result

    async def settle_order_reservation(self, intent_id: str, client_id: int, order_id: int,
                                       reason: str) -> dict:
        """Explicit operator retirement of a reservation that has NO broker
        observation. Never invoked automatically. Refused while the broker
        view is incomplete, while any tracker observation or session trade
        matches the identity, or when the settlement cannot be made durable
        with its reason. Serialized with placements so it cannot race a
        capacity evaluation."""
        if not isinstance(reason, str) or not reason.strip():
            return {'settled': False, 'error': 'a non-empty reason is required'}
        try:
            client_id, order_id = int(client_id), int(order_id)
        except (TypeError, ValueError):
            return {'settled': False, 'error': 'client_id and order_id must be integers'}
        key = (str(intent_id), client_id, order_id)
        async with self.serialized_orders():
            account = self.ib_account
            try:
                rows, journal, emergency = await self._load_reservations(account)
            except RuntimeError as ex:
                return {'settled': False, 'error': str(ex)}
            target = next((r for r in rows if (r['intent_id'], r['client_id'], r['order_id']) == key), None)
            if target is None:
                return {'settled': False,
                        'error': f'no unsettled reservation intent={key[0]} client={key[1]} order={key[2]} '
                                 f'for account {account}'}
            view = self._reservation_view(account, rows)
            if not view['broker_view_complete']:
                return {'settled': False,
                        'error': 'refused: broker view incomplete (execution replay pending or IB disconnected); '
                                 'retry after replay completes'}
            verdict = self._classify_reservation(target, view)
            reference = verdict['reference']
            observations = list(view['by_identity'].get((client_id, order_id), []))
            observations += [row for row in view['by_reference'].get(reference, [])
                             if row.get('orderId') in (0, order_id) and row not in observations]
            if observations and not verdict['settle']:
                return {'settled': False,
                        'error': 'refused: broker evidence matches this identity '
                                 f'({", ".join(str(o.get("status")) for o in observations)}); '
                                 'only exact broker final-quantity evidence can retire it',
                        'observation': [self._observation_summary(o) for o in observations]}
            session = view['session_identities']
            if session is not None and (client_id, order_id) in session:
                return {'settled': False,
                        'error': 'refused: ib_async handed this identity to placeOrder in the current session'}
            conid = verdict['conid']
            if isinstance(conid, int) and not isinstance(conid, bool) and conid > 0:
                try:
                    working = [t for t in self.working_trades(Contract(conId=conid))
                               if (t.order.clientId, t.order.orderId) == (client_id, order_id)]
                except WorkingOrdersUnreadableError as ex:
                    return {'settled': False, 'error': f'refused: {ex}'}
                if working:
                    return {'settled': False,
                            'error': f'refused: a working broker order matches this identity '
                                     f'({working[0].orderStatus.status})'}
            if journal is None:
                return {'settled': False,
                        'error': 'refused: execution journal unavailable; a settlement must be durable'}
            evidence = dict(rule='operator', broker_view_complete=True, boot_id=_BOOT_ID,
                            conid=conid, action=verdict['side'], quantity=verdict['quantity'],
                            reserved_at=target.get('reserved_at'), created_at=target.get('created_at'))
            try:
                record = await asyncio.to_thread(
                    journal.record_settlement, *key, account=account, reason=reason,
                    actor='operator', evidence=evidence)
            except Exception as ex:
                self._journal_degraded = str(ex)
                return {'settled': False, 'error': f'settlement not persisted: {ex}'}
            if emergency is not None:
                emergency.settle_reservations([key])
            cached = getattr(self, '_server_reservation_cache', None)
            if cached is not None and cached[0] == account:
                cached[1][:] = [row for row in cached[1]
                                if (row['intent_id'], row['client_id'], row['order_id']) != key]
            self._unobserved_reservation_detail = [
                item for item in getattr(self, '_unobserved_reservation_detail', [])
                if (item['intent_id'], item['client_id'], item['order_id']) != key]
            logging.warning('RESERVATION SETTLED by operator: intent %s client %s order %s (%s %s x %s): %s',
                            key[0], key[1], key[2], verdict['side'], conid, verdict['quantity'], reason.strip())
            await self._mirror_audit_event(
                'RESERVATION_SETTLED', strategy_name=record['actor'],
                conid=conid, action=verdict['side'], quantity=verdict['quantity'], order_id=record['order_id'],
                metadata={'intent_id': record['intent_id'], 'client_id': record['client_id'],
                          'account': record['account'], 'reason': record['reason'],
                          'evidence': record.get('evidence', {}), 'settled_at': record['settled_at']})
            record['settled_at'] = self._iso(record['settled_at'])
            return {'settled': True, **record, 'observation': None}

    async def _mirror_audit_event(self, event_type_name: str, *, strategy_name: str, conid: Any = 0,
                                  action: Any = '', quantity: Any = 0.0, order_id: Any = 0,
                                  metadata: Optional[dict] = None) -> None:
        """Mirror an operator/automatic audit record into the DuckDB event
        store when its vocabulary has the named EventType. The durable record
        already exists where the act happened (execution journal, marker
        file); this is the cross-service trail. Writing an event type the
        store cannot read back would poison every unfiltered query, so an
        unknown name is logged and skipped rather than coerced."""
        store = getattr(self, 'event_store', None)
        event_type = getattr(EventType, event_type_name, None)
        if event_type is None:
            logging.info('EventType.%s is not defined; audit record kept at its source only', event_type_name)
            return
        if store is None:
            return
        try:
            event = TradingEvent(
                event_type=event_type, timestamp=dt.datetime.now(), strategy_name=strategy_name,
                conid=int(conid) if isinstance(conid, int) and not isinstance(conid, bool) else 0,
                action=str(action or ''),
                quantity=float(quantity) if isinstance(quantity, (int, float)) and not isinstance(quantity, bool) else 0.0,
                order_id=int(order_id) if isinstance(order_id, int) and not isinstance(order_id, bool) else 0,
                metadata=metadata or {})
            await asyncio.to_thread(store.append, event)
        except Exception as ex:
            logging.warning('%s audit event not mirrored to the event store: %s', event_type_name, ex)

    def _cache_broker_reservation(self, intent_id: str, order: Order, contract: Optional[Contract],
                                   is_exit: bool, broker_reference: str) -> None:
        cached = getattr(self, '_server_reservation_cache', None)
        if cached is None or cached[0] != self.ib_account:
            return  # a partial process view cannot certify all pre-restart claims
        rows = cached[1]
        key = (intent_id, self.trading_runtime_ib_client_id, order.orderId)
        if any((row['intent_id'], row['client_id'], row['order_id']) == key for row in rows):
            return  # a just-completed full read already includes this leg
        rows.append(dict(intent_id=intent_id, client_id=key[1], order_id=key[2],
                         account=self.ib_account, conid=contract.conId if contract is not None else None,
                         action=order.action if contract is not None else None,
                         quantity=float(order.totalQuantity) if contract is not None else None,
                         is_exit=int(is_exit) if contract is not None else None,
                         broker_reference=broker_reference if contract is not None else None,
                         settled=0, leg_count=0, reserved_at=time.time(), created_at=None,
                         intent_status='SUBMITTING', intent_error=''))
        # Until the next complete journal read, a new physical leg makes this
        # cached topology unsuitable for single-leg orderId=0 association.
        legs = [row for row in rows if row['intent_id'] == intent_id]
        for row in legs:
            row['leg_count'] = 0

    async def reserve_broker_order(self, order: Order, is_exit: bool = False,
                                   contract: Optional[Contract] = None) -> None:
        intent_id = _CURRENT_INTENT.get()
        if not intent_id:
            return
        broker_reference = split_order_reference(order.orderRef)[1] or intent_id
        if broker_reference != intent_id:
            # Resize changes OCA metadata on a proved existing order without
            # replacing its broker identity. A caller-supplied foreign ref on
            # a new order cannot manufacture this alias.
            if contract is None:
                raise ValueError('physical modification requires an exact contract')
            matches = [trade for trade in self.working_trades(contract)
                       if order.orderId > 0 and trade.order.orderId == order.orderId
                       and trade.order.clientId == order.clientId == self.trading_runtime_ib_client_id
                       and split_order_reference(trade.order.orderRef)[1] == broker_reference
                       and trade.order.action == order.action
                       and trade.order.totalQuantity == order.totalQuantity]
            if len(matches) != 1:
                raise ValueError('foreign broker reference is not a verified same-quantity modification')
        if not order.orderId:
            allocated_id = self.client.ib.client.getReqId()
            # IB.placeOrder treats zero as unallocated, so never reserve it.
            if type(allocated_id) is int and allocated_id == 0:
                allocated_id = self.client.ib.client.getReqId()
            if (not isinstance(allocated_id, int) or isinstance(allocated_id, bool)
                    or allocated_id <= 0):
                raise ValueError('broker order ID allocation requires a positive integer')
            order.orderId = allocated_id
        journal = _CURRENT_JOURNAL.get() or await asyncio.to_thread(self.server_order_journal)
        metadata = (dict(conid=contract.conId, action=order.action,
                         quantity=float(order.totalQuantity), is_exit=is_exit,
                         broker_reference=broker_reference) if contract is not None else {})
        try:
            await asyncio.to_thread(journal.reserve_order, intent_id, order.orderId, self.trading_runtime_ib_client_id, **metadata)
        except ValueError:
            raise  # contradictory immutable identity is not a storage outage
        except Exception as ex:
            self._journal_degraded = str(ex)
            if not is_exit:
                raise
            from trader.data.server_order_journal import EmergencyOrderJournal
            emergency = getattr(self, '_emergency_order_journal', None)
            if emergency is None:
                emergency = EmergencyOrderJournal()
                self._emergency_order_journal = emergency
            emergency.claim(intent_id, 'durable-reservation-failed', self.ib_account)
            emergency.reserve_order(intent_id, order.orderId, self.trading_runtime_ib_client_id, **metadata)
            _CURRENT_JOURNAL.set(emergency)
            logging.error('DURABILITY DEGRADED: reserved emergency exit %s/%s in memory: %s', intent_id, order.orderId, ex)
        cached = getattr(self, '_server_reservation_cache', None)
        from trader.data.server_order_journal import ServerOrderJournal
        if ((cached is None or cached[0] != self.ib_account)
                and isinstance(journal, ServerOrderJournal) and _CURRENT_JOURNAL.get() is journal):
            try:
                # A successful first OPEN also establishes the process's
                # full prior-claim view before the wire call. Emergency-only
                # rows can never establish absence of older durable claims.
                rows = await asyncio.to_thread(journal.reservations, self.ib_account)
                self._server_reservation_cache = (self.ib_account, rows)
                self._reservation_cache_error = ''
            except Exception as ex:
                self._reservation_cache_error = str(ex)
                logging.warning('could not warm complete reduction capacity cache: %s', ex)
        self._cache_broker_reservation(intent_id, order, contract, is_exit, broker_reference)
        # Preserve the strategy prefix for the ledger; keep the complete ID for
        # broker-side recovery even when the sidecar is unavailable.
        if '|mmr:' not in order.orderRef:
            suffix = f'|mmr:{intent_id}'
            if len(suffix.encode()) > 200:
                raise ValueError('client_intent_id is too long for broker recovery identity')
            prefix = order.orderRef.encode()[:max(0, 255 - len(suffix.encode()))].decode(errors='ignore')
            order.orderRef = prefix + suffix

    async def unreserve_broker_order(self, order: Order) -> None:
        """Release a preallocated ID only before any call that can send it."""
        intent_id = _CURRENT_INTENT.get()
        journal = _CURRENT_JOURNAL.get()
        if intent_id and journal is not None:
            await asyncio.to_thread(journal.discard_order, intent_id,
                                    order.orderId, self.trading_runtime_ib_client_id)
            cached = getattr(self, '_server_reservation_cache', None)
            if cached is not None and cached[0] == self.ib_account:
                cached[1][:] = [row for row in cached[1] if (row['intent_id'], row['client_id'], row['order_id'])
                                != (intent_id, self.trading_runtime_ib_client_id, order.orderId)]

    async def _replay_broker_executions(self) -> bool:
        from trader.trading.execution_replay import replay_execution_history
        tracker = getattr(self, 'order_tracker', None)
        if tracker is None:
            return False
        if not hasattr(self, '_execution_replay_lock'):
            self._execution_replay_lock = asyncio.Lock()
        async with self._execution_replay_lock:
            if getattr(self, '_execution_history_ready', False) and not tracker.health['replay_required']:
                return True
            self._execution_replay_started_at = dt.datetime.now(dt.timezone.utc).isoformat()
            self._execution_replay_completed_at = None
            try:
                await replay_execution_history(self.client.ib, tracker)
                self._execution_history_ready = True
                self._execution_replay_completed_at = dt.datetime.now(dt.timezone.utc).isoformat()
                self._execution_replay_completed_epoch = time.time()
                # Broker completeness and journal durability are distinct.
                # An unavailable journal must not hide fresh broker evidence
                # from an already-owned emergency reduction.
                await asyncio.to_thread(tracker.flush, 1.0)
                tracker.mark_replay_complete()
                if getattr(self, '_execution_replay_failed', False):
                    logging.info('broker execution replay RECOVERED; opening readiness restored '
                                 '(replay_required=%s)', tracker.health['replay_required'])
                self._execution_replay_failed = False
                return True
            except Exception as ex:
                self._execution_history_ready = False
                self._execution_replay_failed = True
                logging.warning('broker execution replay incomplete: %s: %s (new exposure stays disabled; '
                                'retrying every %ss while replay is required)',
                                type(ex).__name__, ex, self._EXECUTION_REPLAY_RETRY_S)
                return False

    # A failed startup replay used to disable every open until something else
    # (a reconnect, an execution_snapshot RPC) happened to trigger a replay.
    # The tracker's replay_required stays True after a timeout and
    # margin_checks refuses on it, so a 5s IB hiccup at connect was a silent,
    # indefinite opening outage. Retry on the pulse scheduler instead.
    _EXECUTION_REPLAY_RETRY_S = 30

    def _start_execution_replay_retry(self) -> None:
        """Schedule the bounded periodic retry (called from setup_subscriptions,
        next to the pulse). Idempotent per setup: the disposable is tracked
        with the other subscriptions and torn down on reconnect."""
        if self.scheduler is None:
            return
        disposable = self.scheduler.schedule_periodic(
            self._EXECUTION_REPLAY_RETRY_S, lambda _state: self._retry_execution_replay_if_required())
        self.disposables.append(disposable)

    def _retry_execution_replay_if_required(self) -> Optional['asyncio.Task']:
        """One scheduler tick: if the broker replay is still required and IB
        is connected, run another replay attempt as a task on the loop. Never
        blocks the scheduler thread; never raises. Returns the task (or None
        when nothing needed doing) so tests can await it."""
        try:
            tracker = getattr(self, 'order_tracker', None)
            if tracker is None:
                return None
            if getattr(self, '_execution_history_ready', False) and not tracker.health['replay_required']:
                return None
            if not self.client.ib.isConnected():
                return None  # the reconnect path replays when the socket is back
            lock = getattr(self, '_execution_replay_lock', None)
            if lock is not None and lock.locked():
                return None  # an attempt is already in flight
            loop = getattr(self, '_main_loop', None)
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                pass
            if loop is None or not loop.is_running():
                return None
            logging.info('broker execution replay still required; retrying')
            return loop.create_task(self._replay_broker_executions())
        except Exception as ex:
            logging.warning('execution replay retry tick failed: %s', ex)
            return None

    async def execution_snapshot(self, intent_id: str = '', order_ids: Optional[list[int]] = None) -> dict:
        """Fresh positions/orders plus IB's bounded execution-history replay.

        Completeness is observation, not proof an absent older intent never
        executed. Journal health is separate so confirmed reductions can
        continue during a local storage outage.
        """
        ids = list(order_ids or [])
        prior = None
        if intent_id:
            # A split parent may have made its first leg visible while it is
            # still preparing a second. Wait for that account-order operation
            # to finish extending the journal topology. Broker snapshot I/O
            # below runs after releasing this barrier.
            async with self.serialized_orders():
                emergency = getattr(self, '_emergency_order_journal', None)
                prior = emergency.get(intent_id) if emergency is not None else None
                if prior is None:
                    try:
                        journal = await asyncio.to_thread(self.server_order_journal)
                        prior = await asyncio.to_thread(journal.get, intent_id)
                        self._journal_recovered()
                    except Exception as ex:
                        self._journal_degraded = str(ex)
            if prior:
                if prior['account'] != self.ib_account:
                    raise ValueError('execution intent belongs to a different broker account')
                ids.extend(item['orderId'] for item in prior['orders'])
        expected = {(item['clientId'], item['orderId']) for item in prior['orders']} if prior else set()
        journal_ids = {oid for _client, oid in expected}
        expected.update((self.trading_runtime_ib_client_id, oid) for oid in ids if oid not in journal_ids)
        orders_complete = False
        positions_complete = False
        account_confirmed = False
        positions = []
        tracker = getattr(self, 'order_tracker', None)
        if (not getattr(self, '_execution_history_ready', False)
                or tracker is not None and tracker.health['replay_required']):
            await self._replay_broker_executions()
        try:
            fresh_trades = await asyncio.wait_for(self.client.ib.reqOpenOrdersAsync(), 5.0)
            if tracker is not None:
                for trade in fresh_trades:
                    tracker.on_trade(trade)
            orders_complete = self.client.ib.isConnected()
        except Exception as ex:
            logging.warning('open orders snapshot incomplete: %s', ex)
        try:
            positions = [p for p in await asyncio.wait_for(self.client.ib.reqPositionsAsync(), 5.0)
                         if p.account == self.ib_account]
            account_confirmed = self.ib_account in self.client.ib.managedAccounts()
            positions_complete = self.client.ib.isConnected() and account_confirmed
        except Exception as ex:
            logging.warning('position snapshot incomplete: %s', ex)
        orders = []
        provenance_rows: dict[str, list[dict]] = {}
        reference_observations: dict[str, int] = {}
        expected_ids = {oid for _client, oid in expected}
        # Read reference/permId-only completed observations too. Filtering by
        # numeric ID first would hide exactly the recovery evidence needed
        # when IB completedOrder reports an orderId of zero.
        for row in tracker.snapshot() if tracker is not None else []:
            if row.get('account') != self.ib_account:
                continue
            row = dict(row)
            row.pop('brokerIntentCreatedAt', None)
            observed_reference = row.get('clientIntentId')
            if isinstance(observed_reference, str) and observed_reference:
                reference_observations[observed_reference] = reference_observations.get(observed_reference, 0) + 1
            key = (row.get('clientId', 0), row['orderId'])
            reference = row.get('clientIntentId') or ''
            if intent_id:
                if reference:
                    if reference != intent_id:
                        continue
                    if row['orderId'] in expected_ids and key not in expected:
                        continue  # same numeric ID from another broker client
                elif key not in expected:
                    continue
                if key in expected:
                    row['clientIntentId'] = intent_id
            elif ids and key not in expected:
                continue
            orders.append(row)
            if isinstance(observed_reference, str) and observed_reference:
                # An identity inferred solely from requested numeric IDs is
                # not original broker-reference provenance across restarts.
                provenance_rows.setdefault(observed_reference, []).append(row)
        known = {(row.get('clientId', 0), row['orderId']) for row in orders if row['orderId'] > 0}
        if prior and len(prior['orders']) == 1 and len(expected) == 1 and len(orders) == 1:
            row = orders[0]
            if row['orderId'] == 0 and row.get('permId', 0) > 0 and row.get('clientIntentId') == intent_id:
                # Exactly one durable physical leg and one permanent broker
                # identity under its reference have one possible association.
                # Multiple legs or a provisional scoped alias require more
                # replay evidence. Preserve orderId=0, never mint a cancel ID.
                known.update(expected)
        history_ready = bool(getattr(self, '_execution_history_ready', False))
        complete = (orders_complete and history_ready and account_confirmed and expected <= known
                    and not any(row.get('identityAmbiguous', False) for row in orders))
        if provenance_rows:
            try:
                provenance_journal = await asyncio.to_thread(self.server_order_journal)
                claims = await asyncio.to_thread(provenance_journal.get_many, list(provenance_rows))
            except Exception as ex:
                logging.warning('broker intent creation provenance unavailable: %s', ex)
                claims = {}
            for reference, candidates in provenance_rows.items():
                claim = claims.get(reference)
                if claim is None or claim.get('account') != self.ib_account:
                    continue
                created = claim.get('created_at')
                if (not isinstance(created, (int, float)) or isinstance(created, bool)
                        or not math.isfinite(created) or created <= 0):
                    continue
                legs = claim.get('orders')
                if not isinstance(legs, list) or not legs or any(
                    not isinstance(leg, dict)
                    or any(not isinstance(leg.get(field), int) or isinstance(leg.get(field), bool)
                           for field in ('clientId', 'orderId'))
                    or leg['orderId'] <= 0 for leg in legs
                ):
                    continue
                reserved = {(leg['clientId'], leg['orderId']) for leg in legs}
                for row in candidates:
                    if row.get('identityAmbiguous', False):
                        continue
                    client_id, order_id = row.get('clientId', 0), row['orderId']
                    if any(not isinstance(value, int) or isinstance(value, bool)
                           for value in (client_id, order_id)):
                        continue
                    matches = (client_id, order_id) in reserved
                    if order_id == 0:
                        perm_id = row.get('permId', 0)
                        # The same singleton recovery used for completeness,
                        # applied per intent for an unscoped snapshot. A
                        # contradictory nonzero client cannot be omitted data.
                        matches = (len(legs) == 1 and reference_observations[reference] == 1
                                   and len(candidates) == 1
                                   and isinstance(perm_id, int) and not isinstance(perm_id, bool) and perm_id > 0
                                   and client_id in (0, legs[0]['clientId'])
                                   and (not intent_id or len(expected) == 1 and len(orders) == 1))
                    if matches:
                        row['brokerIntentCreatedAt'] = float(created)
        receipts = []
        receipts_available = False
        try:
            if tracker is not None:
                receipts = await asyncio.to_thread(tracker.execution_receipts, ids or None)
                receipts_available = True
        except Exception as ex:
            self._journal_degraded = str(ex)
        journal_healthy = (not bool(getattr(self, '_journal_degraded', ''))
                           and tracker is not None and tracker.health['healthy'])
        return {'complete': complete, 'positions_complete': positions_complete,
                'account': self.ib_account, 'account_confirmed': account_confirmed,
                'client_id': getattr(self, 'trading_runtime_ib_client_id', None),
                'orders_complete': orders_complete, 'executions_complete': history_ready,
                'execution_replay': {
                    'scope': 'broker_available_recent_history',
                    'history_start': None,  # IB supplies no completeness boundary for older history
                    'requested_at': getattr(self, '_execution_replay_started_at', None),
                    'completed_at': getattr(self, '_execution_replay_completed_at', None)},
                'observed_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                'orders': orders,
                'retry_safe': bool(prior and prior['status'] == 'RETRYABLE' and not prior['orders']),
                'intent_outcome': prior.get('outcome') if prior else None,
                'journal_healthy': journal_healthy, 'execution_receipts_available': receipts_available,
                'positions': [{'conId': p.contract.conId, 'position': p.position, 'avgCost': p.avgCost,
                               'account': p.account, 'contract': p.contract} for p in positions],
                'executions': receipts}

    async def _place_expressive_order(
        self,
        contract: Contract,
        action: str,
        quantity: float,
        execution_spec: dict,
        algo_name: str = 'proposal',
        approver_key: str = '',
        force_open: bool = False,
        allow_open: bool = True,
    ) -> SuccessFail:
        """Place an order with full execution specification (brackets, trailing stops, etc.).

        ``force_open`` is set ONLY by the flip-splitting branch below, for the
        opening half of a position-crossing order. That half must be gated as
        new exposure even though the live position read may still show the old
        pre-reduction size, which would otherwise re-classify it as an exit and
        wave it through. It is never set by an external caller.
        """
        from trader.trading.proposal import ExecutionSpec
        try:
            spec = ExecutionSpec.from_dict(execution_spec)
        except (TypeError, ValueError) as ex:
            return SuccessFail.fail(error=f'Invalid execution spec: {ex}')

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
            plan = self.split_for_order(contract, action, quantity)
            if plan.is_flip:
                return await self._place_flip_split(
                    contract, action, plan, execution_spec, algo_name,
                    approver_key, allow_open=allow_open)

        is_exit = False if force_open else self.order_reduces_exposure(
            contract, action, quantity)
        if not is_exit and not allow_open:
            return SuccessFail.fail(error='Opening exposure requires an approved proposal matching this exact order')

        # Tri-state gate record carried into the entry leg's minted token
        # (empty for exit-class entries and for protective children).
        entry_checks: dict = {}

        if is_exit:
            # The opposite child of a closing entry opens a new position.
            # Protection belongs only to an exposure-opening parent.
            spec.exit_type = 'NONE'
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

            margin_result = await self.margin_checks(contract, probe_order)
            if not margin_result.approved:
                return SuccessFail.fail(error=margin_result.reason)
            leverage_checks = dict(margin_result.checks)

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

            multiplier = TradeExecutioner._multiplier(contract)

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

            converted = self.convert_notional(position_value, contract.currency)
            position_value_evaluable = position_value_evaluable and converted is not None
            position_value = converted if converted is not None else 0.0
            try:
                aggregate_value = self.aggregate_position_value(contract, action, quantity, position_value)
            except WorkingOrdersUnreadableError as ex:
                # Working openings on this instrument are part of the
                # concentration input. Unreadable is unevaluable: refuse.
                logging.error('refusing open (fail-closed): %s', ex)
                return SuccessFail.fail(error=f'Risk gate: concentration unevaluable — {ex}')
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
                aggregate_position_value=aggregate_value,
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
                spec.order_type, spec.limit_price, approver_key,
                force_open=force_open)
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
            errors: list[Exception] = []

            def _on_next(trade: Trade):
                result['trade'] = trade
                event.set()

            def _on_error(error):
                errors.append(error)
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
            disposable = obs.pipe(ops.take(1)).subscribe(Observer(
                on_next=_on_next,
                on_error=_on_error,
                on_completed=event.set,
            ))
            try:
                await asyncio.wait_for(event.wait(), timeout=8.0)
            except TimeoutError as ex:
                raise RuntimeError('UNKNOWN: broker submission did not return a receipt') from ex
            finally:
                disposable.dispose()
            if errors and ('UNKNOWN:' in str(errors[0]) or leg_reason is ExitReason.POSITION_CLASSIFIED):
                raise RuntimeError(str(errors[0]))
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

                entry_trade = await _place_and_wait(
                    contract, entry, leg_is_exit=is_exit, leg_checks=entry_checks,
                    leg_reason=ExitReason.POSITION_CLASSIFIED)

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

                trail_trade = await _place_and_wait(contract, trail, leg_is_exit=True)
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

                entry_trade = await _place_and_wait(
                    contract, entry, leg_is_exit=is_exit, leg_checks=entry_checks,
                    leg_reason=ExitReason.POSITION_CLASSIFIED)

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

                sl_trade = await _place_and_wait(contract, sl, leg_is_exit=True)
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

                entry_trade = await _place_and_wait(
                    contract, entry, leg_is_exit=is_exit, leg_checks=entry_checks,
                    leg_reason=ExitReason.POSITION_CLASSIFIED)
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
                    verdict = await tracker.wait_decisive(entry_id, timeout=8.0, trade=trades[0])
                    if verdict == 'rejected':
                        for t in trades:
                            _cancel_trade_safely(t)
                        reason = tracker.latest_status(entry_id, trade=trades[0]) or 'rejected'
                        return SuccessFail.fail(
                            error=f'Order rejected by IB (entry status={reason})')

            return SuccessFail.success(obj=trades)

        except Exception as ex:
            logging.error(f'place_expressive_order error: {ex}')
            return SuccessFail.fail(error=str(ex), exception=ex)

    async def place_standalone_order(
        self, contract: Contract, action: str, quantity: float, order_type: str,
        aux_price: float = 0, limit_price: float = 0, trailing_percent: float = 0,
        tif: str = 'GTC', outside_rth: bool = True, order_ref: str = '',
        client_intent_id: str = '',
    ) -> SuccessFail:
        args = (contract, action, quantity, order_type)
        kwargs = dict(aux_price=aux_price, limit_price=limit_price,
                      trailing_percent=trailing_percent, tif=tif,
                      outside_rth=outside_rth, order_ref=order_ref)
        return await self._run_order_intent('protective', client_intent_id,
                                           self._place_standalone_order, args, kwargs)

    async def resize_position(self, contract: Contract, target_quantity: float,
                              client_intent_id: str = '') -> SuccessFail:
        """Submit a coordinated trim, preserving the protection of pending shares.

        An existing protective tranche equal to the requested trim can share
        IB's reduce-with-block OCA group with the trim. A single larger stop
        cannot be atomically split into two orders with this API, so that shape
        is deferred *before* any order modification. Success means submitted;
        callers reconcile executions before treating the target as achieved.
        """
        return await self._run_order_intent(
            'resize', client_intent_id, self._resize_position,
            (contract, target_quantity), {})

    async def _resize_position(self, contract: Contract, target_quantity: float) -> SuccessFail:
        if not math.isfinite(target_quantity) or int(contract.conId or 0) <= 0:
            return SuccessFail.fail(error='resize requires finite target and exact conId')
        try:
            snapshot = await self.client.ib.reqOpenOrdersAsync()
            positions = await self.client.ib.reqPositionsAsync()
        except Exception as ex:
            return SuccessFail.fail(error=f'DEFERRED: complete broker state unavailable: {ex}')
        held = sum(float(p.position) for p in positions
                   if p.account == self.ib_account and p.contract.conId == contract.conId)
        if not math.isfinite(held):
            return SuccessFail.fail(error='DEFERRED: broker position is not finite')
        if target_quantity == held:
            return SuccessFail.success(obj={'status': 'UNCHANGED', 'order_ids': []})
        if held * target_quantity < 0 or abs(target_quantity) > abs(held):
            return SuccessFail.fail(error='DEFERRED: coordinated resize supports reductions; use a reviewed proposal to grow')
        action = 'SELL' if held > 0 else 'BUY'
        trim_qty = abs(held - target_quantity)
        working = [t for t in snapshot
                   if t.contract.conId == contract.conId and t.order.account == self.ib_account
                   and t.order.action == action
                   and t.orderStatus.status not in {'Cancelled', 'ApiCancelled', 'Filled', 'Inactive'}]
        protective = None
        if working:
            # Only independent protective tranches with exact quantities have a
            # proved handoff here. Arbitrary bracket/OCA webs need a separately
            # specified coordinator; guessing their ownership is unsafe.
            if any(t.order.orderType not in {'STP', 'TRAIL'} or t.order.ocaGroup
                   or t.order.parentId or t.orderStatus.status not in {'Submitted', 'PreSubmitted'}
                   for t in working):
                return SuccessFail.fail(error='DEFERRED: unsupported protective order topology')
            quantities = [float(t.orderStatus.remaining) for t in working]
            if (not all(math.isfinite(q) and q > 0 for q in quantities)
                    or abs(sum(quantities) - abs(held)) > 1e-8):
                return SuccessFail.fail(error='DEFERRED: protective coverage does not exactly match broker inventory')
            protective = next((t for t in working
                               if abs(float(t.orderStatus.remaining) - trim_qty) < 1e-8), None)
            if protective is None:
                return SuccessFail.fail(error='DEFERRED: partial trim requires an existing matching protective tranche; no orders changed')

        oca_group = ''
        if protective is not None:
            oca_group = f'mmr-resize-{uuid.uuid4().hex}'
            edit = copy.copy(protective.order)
            edit.ocaGroup, edit.ocaType = oca_group, 2
            edit.transmit = True
            modified = await self.executioner.subscribe_place_order_direct(
                mint_approved_order(contract, edit, is_exit=True,
                                    exit_reason=ExitReason.VALIDATED_STANDALONE))
            try:
                await asyncio.wait_for(modified.pipe(ops.take(1)), timeout=8.0)
                confirmed = await self.client.ib.reqOpenOrdersAsync()
                positions = await self.client.ib.reqPositionsAsync()
            except Exception as ex:
                return SuccessFail.fail(error=f'UNKNOWN: protective OCA modification requires reconciliation: {ex}')
            match = next((t for t in confirmed if t.order.orderId == edit.orderId
                          and t.order.clientId == edit.clientId
                          and t.order.account == self.ib_account
                          and t.contract.conId == contract.conId), None)
            new_held = sum(float(p.position) for p in positions
                           if p.account == self.ib_account and p.contract.conId == contract.conId)
            if (match is None or match.order.ocaGroup != oca_group or match.order.ocaType != 2
                    or match.orderStatus.status not in {'Submitted', 'PreSubmitted'}
                    or float(match.orderStatus.remaining) != trim_qty or new_held != held):
                return SuccessFail.fail(error='UNKNOWN: broker has not confirmed the matching protected tranche; trim not submitted')

        order = MarketOrder(action, trim_qty, account=self.ib_account, transmit=True,
                            ocaGroup=oca_group, ocaType=2 if oca_group else 0)
        observable = await self.executioner.subscribe_place_order_direct(
            mint_approved_order(contract, order, is_exit=True,
                                exit_reason=ExitReason.POSITION_CLASSIFIED))
        try:
            trade = await asyncio.wait_for(observable.pipe(ops.take(1)), timeout=8.0)
        except Exception as ex:
            return SuccessFail.fail(error=f'UNKNOWN: resize submission requires reconciliation: {ex}')
        return SuccessFail.success(obj={
            'status': 'SUBMITTED', 'order_ids': [trade.order.orderId],
            'target_quantity': target_quantity,
            'protection': 'OCA_REDUCE_WITH_BLOCK' if protective else 'NO_EXISTING_PROTECTION'})

    async def _place_standalone_order(
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
            errors: list[Exception] = []

            def on_next(trade: Trade):
                nonlocal result_trade
                result_trade = trade
                task.set()

            def on_error(error):
                errors.append(error)
                task.set()

            # Standalone orders are exit-class ONLY (validated above): mint a
            # protective-child token. No gate ran, so checks are empty.
            observable = await self.executioner.subscribe_place_order_direct(
                mint_approved_order(contract, order, is_exit=True,
                        exit_reason=ExitReason.VALIDATED_STANDALONE))
            observable.pipe(ops.take(1)).subscribe(Observer(on_next=on_next, on_error=on_error, on_completed=lambda: None))
            await task.wait()

            if result_trade:
                return SuccessFail.success(obj=result_trade)
            else:
                return SuccessFail.fail(error=str(errors[0]) if errors else 'Failed to place standalone order')

        except Exception as ex:
            logging.error(f'place_standalone_order error: {ex}')
            return SuccessFail.fail(exception=ex)

    @log_method
    async def place_order_simple(
        self, contract: Contract, action: Action, equity_amount: Optional[float],
        quantity: Optional[float], limit_price: Optional[float], market_order: bool,
        stop_loss_percentage: float, algo_name: str = 'global', debug: bool = False,
        skip_risk_gate: bool = False, approver_key: str = '', allow_open: bool = True,
        client_intent_id: str = '',
    ) -> Observable[Trade]:
        args = (contract, action, equity_amount, quantity, limit_price,
                market_order, stop_loss_percentage)
        kwargs = dict(algo_name=algo_name, debug=debug, skip_risk_gate=skip_risk_gate,
                      approver_key=approver_key, allow_open=allow_open)
        async with self.serialized_orders():
            intent_id = client_intent_id
            if not intent_id and not isinstance(getattr(self, 'duckdb_path', None), str):
                return await self._place_order_simple(
                    contract, action, equity_amount, quantity, limit_price,
                    market_order, stop_loss_percentage, algo_name, debug,
                    skip_risk_gate, approver_key, allow_open)
            result = await self._run_order_intent('simple', intent_id,
                                                 self._place_order_simple, args, kwargs)
            if not result.is_success():
                return rx.throw(RuntimeError(result.error))
            observable = rx.of(cast(Trade, result.obj))
            if hasattr(result, 'execution_outcome'):
                setattr(observable, 'execution_outcome', result.execution_outcome)
            setattr(observable, 'client_intent_id', getattr(result, 'client_intent_id', intent_id))
            return observable

    async def _place_order_simple(
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
        allow_open: bool = True,
    ) -> Observable[Trade]:
        """``allow_open=False`` means this path may reduce a position but may
        not open one. The RPC layer sets it when ``require_proposal_approval``
        is on: a close must never be blocked behind a proposal, but the
        OPENING half of a position-crossing order is a new trade and has to go
        through propose → approve like any other."""
        latest_tick: Ticker = await self.client.get_snapshot(contract)

        if quantity is None and equity_amount is not None:
            base_per_local = self.convert_notional(1.0, contract.currency)
            if base_per_local is None or base_per_local <= 0:
                return rx.throw(ValueError('Cannot size base-currency amount: FX rate unavailable'))
            equity_amount = equity_amount / base_per_local

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
        multiplier = TradeExecutioner._multiplier(contract)
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
        plan = self.split_for_order(contract, str(action), final_qty)
        if plan.is_flip:
            logging.warning(
                'flip split (direct path): %s %s %s crosses zero — reduction %s '
                '(exit-class) then remainder %s (%s)',
                action, final_qty, contract.symbol, plan.reduce_qty, plan.open_qty,
                'gated' if allow_open else 'REFUSED: opens need propose → approve')
            reduce_order = self.executioner.helper_create_order(
                contract, action, latest_tick, None, plan.reduce_qty,
                limit_price, market_order, stop_loss_percentage, algo_name, debug)
            reduce_obs = await self.executioner.place_order(
                contract_order=reduce_order,
                condition=ExecutorCondition.SANITY_CHECK,
                position_value_hint=position_value_hint,
                approver_key=approver_key,
            )
            try:
                reduction = await reduce_obs.pipe(ops.take(1))
            except Exception as ex:
                return rx.throw(ex)
            outcome = {
                'reduction': {'status': 'SUBMITTED', 'quantity': float(reduction.order.totalQuantity),
                              'order_ids': [reduction.order.orderId]},
                'opening': {'status': 'REJECTED', 'quantity': plan.open_qty,
                            'error': 'opening exposure requires an approved proposal'},
            }
            if allow_open:
                try:
                    open_order = self.executioner.helper_create_order(
                        contract, action, latest_tick, None, plan.open_qty,
                        limit_price, market_order, stop_loss_percentage, algo_name, debug)
                    open_value = position_value_hint * plan.open_qty / final_qty if position_value_hint is not None else None
                    open_obs = await self.executioner.place_order(
                        contract_order=open_order,
                        condition=ExecutorCondition.SANITY_CHECK,
                        position_value_hint=open_value,
                        approver_key=approver_key,
                        force_open=True,
                    )
                    opening = await open_obs.pipe(ops.take(1))
                    outcome['opening'] = {'status': 'SUBMITTED', 'quantity': float(opening.order.totalQuantity),
                                          'order_ids': [opening.order.orderId]}
                except Exception as ex:
                    outcome['opening'] = {'status': 'UNKNOWN' if 'UNKNOWN' in str(ex) else 'REJECTED',
                                          'quantity': plan.open_qty, 'error': str(ex)}
            result_obs = rx.of(reduction)
            setattr(result_obs, 'execution_outcome', outcome)
            return result_obs

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
            ib_connected = self.client.ib.isConnected()
            tracker = getattr(self, 'order_tracker', None)
            logging.info(
                'pulse ib_connected=%s ib_upstream=%s open_orders=%s dropped_ticks=%d '
                'replay_required=%s unsettled_reservations=%s',
                ib_connected,
                bool(ib_connected and self._ib_upstream_connected),
                book.get_open_order_count() if book is not None else 0,
                # Ticks the bounded publisher refused; a rising count is a
                # degraded feed (subscribers see gaps), MONITORING.md.
                getattr(self, 'zmq_pubsub_dropped_ticks', 0),
                tracker.health['replay_required'] if tracker is not None else None,
                self._unsettled_reservation_count(),
            )
        except Exception as ex:
            logging.warning('pulse failed: %s', ex)

    async def ping_ib(self) -> dict:
        """Live IB socket round-trip (reqCurrentTime with a 5s deadline).

        ``get_status()``'s flags can freeze true on a half-open socket —
        G3's 10.5h invisible outage — because they are driven by error
        codes that never arrive when the socket itself dies. An actual
        request/response is the liveness signal used by `mmr verify`.
        """
        ib = self.client.ib
        pending = getattr(self, '_ib_ping_request', None)
        if pending is None or pending[0] is not ib or pending[1].done():
            # ib_async uses one fixed 'currentTime' future per IB instance.
            # Concurrent requests overwrite it and strand an earlier caller.
            # RPC handlers share this loop; publish the task before yielding.
            async def request():
                async def round_trip():
                    # Gateway can suppress subsecond currentTime requests.
                    # Space new sends from the previous attempt's completion,
                    # while still requiring a new response for every task.
                    completed = getattr(self, '_ib_ping_completed', None)
                    if completed is not None and completed[0] is ib:
                        delay = 1.0 - (asyncio.get_running_loop().time() - completed[1])
                        if delay > 0:
                            await asyncio.sleep(delay)
                    if self.client.ib is not ib:
                        raise ConnectionError('IB connection replaced during ping')
                    return await ib.reqCurrentTimeAsync()

                try:
                    server_time = await asyncio.wait_for(
                        round_trip(), timeout=5.0)
                    if self.client.ib is not ib:
                        raise ConnectionError('IB connection replaced during ping')
                    return {'ok': True, 'ib_server_time': str(server_time)}
                except Exception as ex:
                    return {'ok': False, 'error': '{}: {}'.format(type(ex).__name__, ex)}
                finally:
                    if self.client.ib is ib:
                        self._ib_ping_completed = (ib, asyncio.get_running_loop().time())

            pending = (ib, asyncio.create_task(request()))
            self._ib_ping_request = pending
        # One cancelled RPC must not cancel another caller's shared probe.
        # The task owns one five-second deadline including any spacing wait;
        # completed results are never reused, and an IB replacement gets an
        # independent fresh request.
        return await asyncio.shield(pending[1])

    @log_method
    def red_button(self):
        self.client.ib.reqGlobalCancel()

    # status() is polled heavily by strategy_service, the CLI, and the
    # risk-gate. Retain a 1-second cache, but sample the cheap local socket
    # flag on every call: disconnects and upstream error events must bypass
    # an otherwise fresh connected result. No @log_method — the decorator's
    # inspect.signature + repr for every call adds measurable overhead on a
    # hot path, and the RPC server already DEBUG-logs each dispatch.
    def status(self) -> dict:
        ib_connected = self.client.ib.isConnected()
        upstream_connected = bool(ib_connected and self._ib_upstream_connected)
        upstream_error = None
        if not upstream_connected:
            upstream_error = self._ib_upstream_error
            if not ib_connected and not upstream_error:
                upstream_error = 'IB Gateway socket disconnected'
        now = time.monotonic()
        cached_ts = getattr(self, '_status_cache_ts', 0.0)
        if now - cached_ts < 1.0:
            cached = getattr(self, '_status_cache', None)
            if (cached is not None
                    and cached.get('ib_connected') == ib_connected
                    and cached.get('ib_upstream_connected') == upstream_connected
                    and cached.get('ib_upstream_error') == upstream_error):
                return cached
        status = {
            'ib_connected': ib_connected,
            'ib_upstream_connected': upstream_connected,
            'storage_connected': self.data is not None,
            # Identifies THIS trader_service process. Market-data
            # subscriptions live only in its memory, so when this value
            # changes every subscription is gone and subscribers must ask
            # again. strategy_service watches it; see _reconcile_sync.
            'boot_id': _BOOT_ID,
        }
        if not upstream_connected:
            status['ib_upstream_error'] = upstream_error
        # Unsettled physical reservations known to this process — from the
        # cache the last complete journal read left behind (maintained by
        # reserve/unreserve/settle), NOT a fresh SQLite read: status() must
        # stay cheap. None until a complete read has happened. A nonzero value
        # with nothing working at the broker is the phantom-send signal;
        # `mmr reservations` (list_order_reservations) says which rows block.
        status['unsettled_reservations'] = self._unsettled_reservation_count()
        status['dropped_ticks'] = getattr(self, 'zmq_pubsub_dropped_ticks', 0)
        self._status_cache = status
        self._status_cache_ts = now
        return status

    def _unsettled_reservation_count(self) -> Optional[int]:
        cached = getattr(self, '_server_reservation_cache', None)
        if cached is None or cached[0] != getattr(self, 'ib_account', None):
            return None
        return len(cached[1])

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

    @staticmethod
    def routable_contract(contract: Contract) -> Contract:
        """An order-routable copy of a broker position record.

        ``ib.positions()`` reports contracts with ``exchange=''``; IB rejects
        ``placeOrder`` on that shape with error 321 ("Please enter exchange").
        Every client-originated path normalises through ``sdk._to_contract``;
        the server-side emergency reduction placed the raw record and failed
        at the broker on every retry. The conId is kept exactly; only the
        routing venue is supplied, and only when the record has none.
        """
        routed = copy.copy(contract)
        if (routed.exchange or '').strip():
            return routed
        sec_type = (routed.secType or '').upper()
        if sec_type == 'CASH':
            routed.exchange = 'IDEALPRO'
        elif sec_type in {'STK', 'ETF', 'OPT', 'WAR', 'CFD', 'BOND', 'FUND', ''}:
            routed.exchange = 'SMART'
        elif (routed.primaryExchange or '').strip():
            routed.exchange = routed.primaryExchange
        return routed

    def get_positions(self) -> List[Position]:
        # See _get_portfolio_summary_sync for rationale — hit ib_async
        # directly rather than relying on the event-driven local cache.
        try:
            positions = self.client.ib.positions(
                account=self.ib_account
            ) if self.ib_account else self.client.ib.positions()
            if positions:
                return [p for p in positions if p.account == self.ib_account]
        except Exception as ex:
            logging.warning('ib.positions() failed, using cache: %s', ex)
        return [p for p in self.portfolio.get_positions() if p.account == self.ib_account]

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
