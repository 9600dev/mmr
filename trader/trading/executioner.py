from enum import Enum
from ib_async import (
    Contract,
    ExecutionCondition,
    LimitOrder,
    MarketOrder,
    Order,
    StopLimitOrder,
    StopOrder,
    Ticker,
    Trade
)
from reactivex import Observable, Observer
from reactivex.abc import DisposableBase
from reactivex.disposable import Disposable
from reactivex.subject import Subject
from trader.common.exceptions import trader_exception, TraderException
from trader.common.logging_helper import get_callstack, log_method, setup_logging
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.data.universe import Universe, UniverseAccessor
from trader.objects import Action, Basket, ContractOrderPair, ExecutorCondition
from trader.trading.approved_order import ApprovedOrder, ExitReason, mint_approved_order
from trader.trading.order_math import order_notional, whole_shares_for_notional
from trader.trading.order_structure import rejection_for_order
from trader.trading.order_reference import split_order_reference
from trader.trading.order_validator import OrderValidator
from trader.trading.risk_gate import RiskGate, RiskGateResult
from typing import cast, List, Optional, TYPE_CHECKING

import datetime as dt
import asyncio
import inspect
import time
import math
import reactivex as rx
import reactivex.operators as ops
import sys
import uuid


logging = setup_logging(module_name='trading_runtime')

if TYPE_CHECKING:
    from trader.trading.trading_runtime import Trader



def _price_or_none(value):
    """An ib_async numeric order field as a real price, or None if unset.

    ib_async writes ``UNSET_DOUBLE`` (``sys.float_info.max``) into fields an
    order does not use, so a MARKET order's ``lmtPrice`` is not 0 but
    1.7976931348623157e+308. Treating that as a price is how a market order
    acquires a $1.8e308 notional.
    """
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(price) or price <= 0 or price >= sys.float_info.max:
        return None
    return price


class WorkingOrdersUnreadableError(RuntimeError):
    """The broker's working-order set could not be read.

    Raised by ``Trader.working_trades`` instead of pretending the set is
    empty. An empty set is the one answer that lets a second executable
    close be sent against the same shares and lets a concentration check
    under-count; "unreadable" is a different fact and callers must treat it
    as unevaluable: an open is refused, a reduction is deferred.
    """


# What an operator does about an unconfirmed earlier send. Named in the
# DEFERRED reason so the log line that describes the stuck state also names
# the way out of it (the tooling is `mmr reservations`, wired to the
# list_order_reservations / settle_order_reservation RPCs).
_RESERVATION_TOOLING_HINT = (
    'inspect them with `mmr reservations`; once the broker confirms a send never '
    'reached IB, release it with `mmr reservations settle <intent_id> <client_id> '
    '<order_id> --reason "..."` (refused while a live broker observation matches)')


def _working_reduction_quantity(trade: Trade) -> float:
    try:
        total, filled, remaining = (float(trade.order.totalQuantity),
                                    float(trade.orderStatus.filled),
                                    float(trade.orderStatus.remaining))
    except (ValueError, TypeError) as ex:
        raise RuntimeError('UNKNOWN: competing reduction quantity is unreadable') from ex
    if any(not math.isfinite(value) or value < 0 or value >= sys.float_info.max
           for value in (total, filled, remaining)):
        raise RuntimeError('UNKNOWN: competing reduction quantity is unreadable')
    # Native PendingSubmit initially reports remaining=0 despite its send.
    return max(0.0, total - filled, remaining)


class TradeExecutioner():
    # Documented waits on the placement path, in seconds. A caller that bounds
    # a whole placement (the RPC wrapper) derives its deadline from these so
    # a legitimately slow-but-successful send is not reported as timed out.
    CANCEL_WAIT_TIMEOUT_S = 8.0   # own competing reduction confirmed terminal
    RECEIPT_TIMEOUT_S = 8.0       # broker submission receipt
    AUDIT_TIMEOUT_S = 5.0         # durable ORDER_SUBMITTED before the send

    @classmethod
    def placement_deadline_s(cls) -> float:
        """Upper bound for one placement through the chokepoint, plus slack."""
        return cls.CANCEL_WAIT_TIMEOUT_S + cls.RECEIPT_TIMEOUT_S + cls.AUDIT_TIMEOUT_S + 2.0

    def __init__(
        self,
    ):
        self.trader: 'Trader'
        self.connected: bool = False
        self.validator: OrderValidator = OrderValidator()

    def connect(self, trader: 'Trader'):
        self.trader = trader
        self.connected = True

    async def _log_event(self, event_type: EventType, contract: Contract, order: Order,
                   strategy_name: Optional[str] = None, is_exit: bool = False) -> None:
        if hasattr(self.trader, 'event_store'):
            # Stamp the real originator: the order's orderRef (approve derives
            # it from proposal.metadata['strategy']; the direct path stamps its
            # algo_name). That is exactly the source the risk gate's pseudo-
            # signal names, so the open-rate check counts the right bucket
            # instead of a dead 'manual'/'proposal' constant.
            if strategy_name is None:
                strategy_name = (getattr(order, 'orderRef', '') or '').split('|mmr:', 1)[0].strip() or 'manual'
            # Exit-class submissions (closes, protective stops, bracket legs)
            # are stamped so the rate limit — which counts only exposure-
            # increasing opens — can exclude them.
            metadata = {'exit_class': True} if is_exit else {}
            # Value the order for the audit trail. The `price` column cannot
            # carry this: it records order.lmtPrice, which for a MARKET or STOP
            # order is ib_async's UNSET_DOUBLE. Every market submission in the
            # live event store sits at 1.797e308. Harmless for the order-RATE
            # limit (it counts rows) and useless for anything summing VALUE,
            # which is what a cumulative notional cap has to do.
            #
            # Prices already to hand only: no snapshot is awaited here. This is
            # the placement chokepoint, and a network call on it would add
            # latency and a new failure mode to every order. A cached ticker is
            # a dict lookup. When nothing is usable we record that fact rather
            # than a zero that reads like a real valuation.
            # ib_async leaves an unused numeric field at UNSET_DOUBLE
            # (sys.float_info.max), so a MARKET order's lmtPrice and a STOP
            # order's lmtPrice both arrive as 1.797e308. Stripped here, where we
            # know the convention is ib_async's; order_notional refuses it too,
            # because the value is finite and positive and so defeats every
            # ordinary sanity guard.
            notional, notional_evaluable = order_notional(
                (_price_or_none(getattr(order, 'lmtPrice', None)),
                 _price_or_none(getattr(order, 'auxPrice', None)))
                + self._cached_prices(contract),
                float(order.totalQuantity or 0),
                self._multiplier(contract),
            )
            converted = self.trader.convert_notional(notional, getattr(contract, 'currency', '')) if notional_evaluable else None
            notional_evaluable = converted is not None
            notional = converted if converted is not None else 0.0
            metadata['account'] = self.trader.ib_account
            metadata['notional_currency'] = 'BASE'
            metadata['notional'] = notional
            metadata['notional_evaluable'] = notional_evaluable
            if event_type == EventType.ORDER_SUBMITTED:
                _, intent_id = split_order_reference(getattr(order, 'orderRef', ''))
                metadata['submission_identity'] = ':'.join([
                    'submission', self.trader.ib_account,
                    str(getattr(self.trader, 'trading_runtime_ib_client_id', 0)),
                    intent_id or uuid.uuid4().hex, str(order.orderId)])
                metadata['submission_phase'] = 'BEFORE_BROKER_SEND'
            if not notional_evaluable:
                logging.warning(
                    'could not value %s %s %s for the audit trail (no limit, stop '
                    'or cached price) — recorded as not evaluable',
                    order.action, order.totalQuantity, contract.symbol)
            event = TradingEvent(
                event_type=event_type,
                timestamp=dt.datetime.now(),
                strategy_name=strategy_name,
                conid=contract.conId or 0,
                symbol=contract.symbol or '',
                action=str(order.action),
                quantity=float(order.totalQuantity or 0),
                price=float(order.lmtPrice or 0),
                order_id=order.orderId or 0,
                metadata=metadata,
            )
            # Capture broker fields on their owner loop, then await the durable
            # write off-loop. The account order lock remains held, so the next
            # order cannot race ahead of the rate/turnover accounting.
            tracker = getattr(self.trader, 'order_tracker', None)
            record = getattr(tracker, 'record_submission', None)
            if event_type == EventType.ORDER_SUBMITTED and callable(record):
                await asyncio.to_thread(record, event, getattr(self, '_audit_timeout', self.AUDIT_TIMEOUT_S))
            else:
                await asyncio.to_thread(self.trader.event_store.append, event)

    @staticmethod
    def _multiplier(contract: Contract) -> float:
        try:
            if not contract.multiplier:
                return float('nan') if getattr(contract, 'secType', '') in {'OPT', 'FUT', 'FOP'} else 1.0
            return float(contract.multiplier)
        except (TypeError, ValueError):
            return float('nan')

    def _cached_prices(self, contract: Contract) -> tuple:
        """Live prices for ``contract`` from ib_async's ticker cache, best
        first. Never blocks and never raises — a valuation for the audit trail
        must not be able to fail a placement."""
        try:
            conid = int(getattr(contract, 'conId', 0) or 0)
            for ticker in self.trader.client.ib.tickers():
                tc = getattr(ticker, 'contract', None)
                if tc is None or int(getattr(tc, 'conId', 0) or 0) != conid:
                    continue
                return (getattr(ticker, 'last', None),
                        getattr(ticker, 'close', None),
                        getattr(ticker, 'ask', None),
                        getattr(ticker, 'bid', None))
        except Exception as ex:
            logging.debug('no cached price for %s: %s', getattr(contract, 'symbol', '?'), ex)
        return ()

    async def _coordinate_reduction(self, contract: Contract, order: Order,
                                   protective: bool = False) -> None:
        """Replace conflicting reductions only after cancellation is terminal.

        PendingCancel still owns its executable capacity. An uncertain cancel
        leaves the new reduction pending/unknown; it never submits a second
        independently executable close. Confirmed fills force a fresh clamp.
        """
        try:
            unobserved = await self.trader.unobserved_reduction_quantity(contract, order.action)
            working = [t for t in self.trader.working_trades(contract)
                       if t.order.action == order.action and t.order.orderId != order.orderId]
        except WorkingOrdersUnreadableError as ex:
            # Nothing has been sent, so this is a clean deferral, not an
            # UNKNOWN: the broker's working set is what decides whether this
            # close would be a second executable reduction of the same shares.
            raise RuntimeError(
                f'DEFERRED: {order.action} {float(order.totalQuantity):g} not placed: '
                f'{str(ex).removeprefix("UNKNOWN: ")}; retry once the broker order book is readable') from ex
        if not working and not unobserved:
            return
        held = self.trader._signed_position(int(contract.conId or 0))
        if held is None or not math.isfinite(held):
            raise RuntimeError('UNKNOWN: cannot coordinate reductions without broker inventory')
        def capacity(trades):
            # Blocking OCA alternatives reserve their maximum, not their sum.
            # Type 3 lacks overfill protection and must remain additive.
            groups = {}
            independent = 0.0
            for trade in trades:
                qty = _working_reduction_quantity(trade)
                if trade.order.ocaGroup and trade.order.ocaType in (1, 2):
                    key = trade.order.ocaGroup
                    groups[key] = max(groups.get(key, 0.0), qty)
                else:
                    independent += qty
            return independent + sum(groups.values())

        reserved = capacity(working) + unobserved
        if order.ocaGroup and order.ocaType in (1, 2):
            matching = [t for t in working if t.order.ocaGroup == order.ocaGroup
                        and t.order.ocaType == order.ocaType]
            if matching:
                reserved -= min(float(order.totalQuantity), capacity(matching))
        if reserved + float(order.totalQuantity) <= abs(held):
            return
        # Only the caller's own working reductions may be displaced, whatever
        # the exit reason. Cancelling another owner's order (typically a
        # strategy's disaster stop under a manual close) and then refusing
        # this exit — cancel unconfirmed, inventory unreadable, an unconfirmed
        # earlier send still reserving capacity — left the position naked.
        # Nothing has been sent at this point, so a shortfall against orders
        # owned elsewhere is a clean DEFERRED refusal, never an UNKNOWN.
        owner = self._owner_class(order.orderRef)
        own = [t for t in working if self._owner_class(t.order.orderRef) == owner]
        foreign = [t for t in working if self._owner_class(t.order.orderRef) != owner]
        untouchable = capacity(foreign) + unobserved
        if abs(held) - untouchable <= 0:
            competing = ', '.join(
                f'{t.order.orderId} (owner {split_order_reference(t.order.orderRef)[0] or "operator"})'
                for t in foreign)
            claims = f' and {unobserved:g} by unconfirmed earlier sends' if unobserved else ''
            if unobserved:
                # Name the reservations and the way out. Without this the
                # operator sees a deferral that cites no order they can find
                # on the broker and no command that would release it.
                claims += self._unobserved_reservation_hint()
            raise RuntimeError(
                f'DEFERRED: {order.action} {float(order.totalQuantity):g} not placed: all {abs(held):g} held '
                f'are reserved by working reductions owned elsewhere [{competing}]{claims}; '
                'that owner must retire them (or cancel them explicitly) before this close')
        for trade in own:
            self.trader.client.ib.cancelOrder(trade.order)
            deadline = time.monotonic() + getattr(self, '_cancel_wait_timeout', self.CANCEL_WAIT_TIMEOUT_S)
            while trade.orderStatus.status not in {'Cancelled', 'ApiCancelled', 'Filled', 'Inactive'}:
                if time.monotonic() >= deadline:
                    raise RuntimeError('UNKNOWN: competing reduction cancellation not confirmed')
                await asyncio.sleep(0.05)
        # Own reductions have been cancelled above, so from here an unreadable
        # broker view is UNKNOWN (the book has been touched), not DEFERRED.
        unobserved = await self.trader.unobserved_reduction_quantity(contract, order.action)
        held = self.trader._signed_position(int(contract.conId or 0))
        if held is None or not math.isfinite(held):
            raise RuntimeError('UNKNOWN: broker inventory unreadable after cancellation')
        remaining = unobserved + capacity([t for t in self.trader.working_trades(contract)
                                          if t.order.action == order.action])
        available = max(0.0, abs(held) - remaining)
        correct_direction = (order.action == 'SELL' and held > 0) or (order.action == 'BUY' and held < 0)
        if not correct_direction or available <= 0:
            raise RuntimeError('UNKNOWN: reduction already filled or reserved; reconcile intent')
        if available < float(order.totalQuantity):
            logging.warning(
                'reduction %s %g clamped to %g: %g reserved by other working reductions or unconfirmed sends',
                order.action, float(order.totalQuantity), available, remaining)
        order.totalQuantity = min(float(order.totalQuantity), available)

    def _unobserved_reservation_hint(self) -> str:
        """The identities behind the last unobserved-capacity evaluation, plus
        the operator tooling that releases them. Formats nothing it cannot
        read (a stub trader without the detail attribute gets the bare hint)."""
        detail = getattr(self.trader, '_unobserved_reservation_detail', None)
        identities = ''
        if isinstance(detail, list) and detail:
            identities = ' ' + ', '.join(
                f'{item["intent_id"]}/{item["client_id"]}/{item["order_id"]} ({item["quantity"]:g})'
                for item in detail if isinstance(item, dict)) + ';'
        return f' (unconfirmed sends:{identities} {_RESERVATION_TOOLING_HINT})'

    # Manual paths stamp different algo names (``global`` for mmr buy/sell,
    # ``proposal`` for approve); they are all the operator and may displace
    # one another's working orders. A strategy name is its own class.
    _OPERATOR_OWNERS = frozenset({'', 'global', 'proposal', 'manual'})

    @classmethod
    def _owner_class(cls, order_ref) -> str:
        owner = split_order_reference(order_ref)[0]
        return 'operator' if owner in cls._OPERATOR_OWNERS else owner

    async def subscribe_place_order_direct(
        self,
        approved: ApprovedOrder,
    ) -> Observable[Trade]:
        # The single IB placement chokepoint. It accepts ONLY an ApprovedOrder
        # capability token — a code path that never reached the gate cannot
        # construct this argument (the token is mint-only; see approved_order).
        # Account-pinning below is unchanged.
        contract = approved.contract
        order = approved.order
        is_exit = approved.is_exit

        # Structural sanity, applied to EVERY order including exits.
        #
        # This check already existed (OrderValidator), but only on the
        # ExecutorCondition.SANITY_CHECK path — i.e. only on `mmr buy`/`mmr sell`,
        # the path with a human watching. Everything automated (approve, the
        # AutoExecutor, every bracket leg, the protective stop) arrives here
        # instead and was never structurally checked: place_expressive_order
        # takes `quantity: float` straight from the client, and
        # ExecutionSpec.validate() rejects a missing limit price but not a zero
        # one. Enforcing it at the chokepoint covers every path by construction.
        #
        # It applies to exits despite "an exit is never refused" because a
        # malformed order is not a working exit — a SELL of NaN shares reduces
        # nothing. IB would reject it from the far side of the wire; refusing it
        # here names the reason instead.
        structural_reason = rejection_for_order(order)
        if structural_reason is not None:
            logging.error(
                'placement refused: structurally malformed order (%s) — %r',
                structural_reason, approved)
            return rx.throw(ValueError(
                f'placement refused: structurally malformed order — {structural_reason}'))

        # Spend-time authorization check. The token's TYPE only proves someone
        # called mint(); mint deliberately does not validate (a human-owned
        # invariant pins that a token may be constructed with an empty record).
        # So the evidence is demanded HERE, where the token is spent and where
        # it actually matters — which also covers every future mint site
        # automatically, including ones nobody remembered to audit.
        #
        # An exposure-INCREASING order must carry the tri-state gate record that
        # approved it: non-empty, with no check left in the 'fail' state. Exits
        # are exempt by design — they are never gate-refusable, so they
        # legitimately arrive with an empty record.
        # An exit exemption must say WHICH rule justifies it. The claim cannot be
        # verified wholesale — PROTECTIVE_CHILD legs are exit-class by
        # construction and their position does not exist yet — but the
        # POSITION_CLASSIFIED category is checkable, so it is checked.
        #
        # This corroboration is observability-only: a mismatch or failed
        # re-read does not decide placement here. Non-child exits still pass
        # the independent reduction-capacity checks below before reservation;
        # those checks can defer the send.
        if is_exit:
            reason = approved.exit_reason
            if reason is None:
                logging.error(
                    'UNATTRIBUTED exit exemption reached the chokepoint — no ExitReason '
                    'on %r. Gates were skipped without a stated justification.', approved)
            elif reason is ExitReason.POSITION_CLASSIFIED:
                try:
                    still_exit = self.trader.order_reduces_exposure(
                        contract, str(order.action), float(order.totalQuantity or 0))
                except Exception as ex:      # a failed re-read must not block an exit
                    logging.warning('could not corroborate exit claim for %r: %s', approved, ex)
                else:
                    if not still_exit:
                        logging.error(
                            'STALE exit claim: %r was minted POSITION_CLASSIFIED but the '
                            'live position no longer makes it a reduction — continuing to '
                            'reduction-capacity checks. Position likely moved between '
                            'classification and placement.', approved)
                        try:
                            await self._log_event(EventType.RISK_GATE_REJECTED, contract, order)
                        except Exception as ex:
                            self.trader._journal_degraded = str(ex)
                            logging.error('exit observability write failed; reduction remains available: %s', ex)

        if not is_exit:
            restored = self.trader.opening_restore_error()
            if isinstance(restored, str) and restored:
                return rx.throw(ValueError(restored))
            recorded = approved.checks or {}
            # 'fail' is a check that ran and refused. 'unevaluable:' is a check
            # whose INPUT could not be read, which is equally disqualifying for
            # an opening order: the gate has no more idea than we do whether
            # this order is safe. Neither should ever reach here, because both
            # refuse upstream; treating them alike is defence in depth, and it
            # gives the vocabulary teeth rather than leaving it a naming
            # convention. 'skipped:' means the check did not APPLY (forex is
            # not position concentration) and is not disqualifying.
            failed = sorted(k for k, v in recorded.items()
                            if str(v).split(':', 1)[0] in ('fail', 'unevaluable'))
            if not recorded or failed:
                why = (f'failed or unevaluable checks {failed}' if failed
                       else 'no gate record (checks was empty)')
                logging.error(
                    'placement refused: exposure-increasing order with %s — %r', why, approved)
                return rx.throw(ValueError(
                    f'placement refused: exposure-increasing order reached the IB '
                    f'chokepoint with {why}. Only the APPROVE branch of the risk '
                    f'gate may place an opening order.'))

        def trader_exception_helper(ex):
            return rx.throw(
                exception=trader_exception(self.trader, exception_type=TraderException, message='place_order()', inner=ex)
            )

        # Validate the order is pinned to the exact configured ib_account.
        # A blank order.account routes to IB's *default* account — with a
        # multi-account login (e.g. a master + sub-accounts sharing one login)
        # that could silently be the wrong account. Fail loud on a blank
        # configured account, a blank order account, or any mismatch — never
        # let a non-specific account reach IB.
        configured = self.trader.ib_account
        if not configured:
            return trader_exception_helper(ValueError(
                'Refusing to place order: no ib_account is configured on the trader'))
        if not order.account:
            return trader_exception_helper(ValueError(
                'Refusing to place order: order.account is blank '
                '(a blank account routes to IB\'s default account)'))
        if order.account != configured:
            return trader_exception_helper(ValueError(
                f'Refusing to place order: order.account {order.account!r} '
                f'!= configured ib_account {configured!r}'))

        submission_started = False
        try:
            if is_exit and approved.exit_reason != ExitReason.PROTECTIVE_CHILD:
                await self._coordinate_reduction(
                    contract, order,
                    protective=approved.exit_reason == ExitReason.VALIDATED_STANDALONE)
            reservation = self.trader.reserve_broker_order(order, is_exit=is_exit, contract=contract)
            if inspect.isawaitable(reservation):
                await reservation
            try:
                # Charge the attempted submission durably before the first
                # possible broker side effect. An unconfirmed attempt may
                # consume budget, but a crash cannot give that budget back.
                await self._log_event(EventType.ORDER_SUBMITTED, contract, order, is_exit=is_exit)
            except Exception as ex:
                self.trader._journal_degraded = str(ex)
                if not is_exit:
                    released = self.trader.unreserve_broker_order(order)
                    if inspect.isawaitable(released):
                        await released
                    return rx.throw(ValueError(f'submission audit unavailable; order was not sent: {ex}'))
                logging.error('DURABILITY DEGRADED: exit audit unavailable; reduction remains available: %s', ex)
            submission_started = True
            observable = await self.trader.client.subscribe_place_order(contract, order)
            # Capture the placement receipt while the account decision lock is
            # still held. Returning an unsubscribed cold stream used to leave a
            # window in which a second close saw no reserved broker quantity.
            trade = await asyncio.wait_for(observable.pipe(ops.take(1)), timeout=self.RECEIPT_TIMEOUT_S)
            if not hasattr(self.trader, '_submitted_trades'):
                self.trader._submitted_trades = []
            self.trader._submitted_trades.append(trade)
        except Exception as ex:
            if 'UNKNOWN:' in str(ex) or 'DEFERRED:' in str(ex):
                # Coordination verdicts carry their own actionable text (what
                # reserves the shares, who owns it); wrapping them in the
                # generic placement exception hid it from the caller.
                return rx.throw(ex)
            if submission_started:
                return rx.throw(RuntimeError(f'UNKNOWN: broker submission did not return a receipt: {ex}'))
            return trader_exception_helper(ex)

        return rx.of(trade)

    async def place_order(
        self,
        contract_order: ContractOrderPair,
        condition: ExecutorCondition,
        skip_risk_gate: bool = False,
        position_value_hint: Optional[float] = None,
        approver_key: str = '',
        force_open: bool = False,
    ) -> Observable[Trade]:
        # ``force_open`` is set ONLY by the flip-splitting branch in
        # place_order_simple, for the OPENING half of a position-crossing
        # order. That half must be gated as new exposure even though the live
        # position read may still show the pre-reduction size, which would
        # otherwise re-classify it as an exit and wave it through.
        contract = contract_order.contract
        order = contract_order.order

        # The tri-state gate record carried into the minted token for
        # observability. Exit-class and non-gated paths leave it empty.
        gate_checks: dict = {}

        # skip_risk_gate stays in the signature for wire compatibility but is
        # no longer trusted: whether gates apply is decided server-side by the
        # exit-class predicate (does this order reduce the live position?),
        # not by a client-supplied flag.
        if skip_risk_gate:
            logging.warning(
                'place_order: skip_risk_gate=True is deprecated and IGNORED — '
                'exit-class orders are detected server-side from the live broker position')

        is_exit = False if force_open else self.trader.order_reduces_exposure(
            contract, str(order.action), float(order.totalQuantity or 0))

        # Hard attribute access — Trader.__init__ declares risk_gate = None,
        # so a missing gate is a real None, and non-exit-class orders fail
        # CLOSED against it rather than sailing through a getattr default.
        gate = self.trader.risk_gate

        if is_exit:
            # Exit-class: never refusable by gates. Observability only.
            if gate is not None:
                try:
                    instrument_result = gate.check_instrument(
                        symbol=contract.symbol,
                        exchange=contract.exchange or '',
                        sec_type=contract.secType or '',
                    )
                    if not instrument_result.approved:
                        logging.warning(
                            'exit-class order %s %s %s would have been blocked by trading '
                            'filter (%s) — exits are never gated',
                            order.action, order.totalQuantity, contract.symbol,
                            instrument_result.reason)
                except Exception as ex:
                    logging.warning('exit-class filter observability check errored: %s', ex)
        else:
            if gate is None:
                logging.error(
                    'risk gate unavailable — refusing exposure-increasing order %s %s %s '
                    '(fail-closed)', order.action, order.totalQuantity, contract.symbol)
                return rx.throw(
                    trader_exception(
                        trader=self.trader,
                        exception_type=TraderException,
                        message='risk gate unavailable — refusing exposure-increasing order '
                                '(fail-closed; exit-class orders are exempt)'
                    )
                )

            # Trading filter (denylist/allowlist)
            instrument_result = gate.check_instrument(
                symbol=contract.symbol,
                exchange=contract.exchange or '',
                sec_type=contract.secType or '',
            )
            if not instrument_result.approved:
                await self._log_event(EventType.RISK_GATE_REJECTED, contract, order)
                logging.warning(f'trading filter rejected order: {instrument_result.reason}')
                return rx.throw(
                    trader_exception(
                        trader=self.trader,
                        exception_type=TraderException,
                        message=f'trading filter rejected: {instrument_result.reason}'
                    )
                )

            structural_reason = rejection_for_order(order)
            if structural_reason is not None:
                return rx.throw(ValueError(structural_reason))
            margin_result = await self.trader.margin_checks(contract, order)
            if not margin_result.approved:
                return rx.throw(ValueError(margin_result.reason))

            from trader.trading.strategy import Signal
            # Create a pseudo-signal for risk evaluation. Its source_name must
            # match what the ORDER_SUBMITTED event is stamped with (the order's
            # orderRef) so the open-rate check queries the right bucket.
            signal = Signal(
                source_name=(getattr(order, 'orderRef', '') or '').strip() or 'manual',
                action=Action.BUY if str(order.action) == 'BUY' else Action.SELL,
                probability=1.0,
                risk=0.0,
            )
            # No hint from the caller but the order carries its own price —
            # a limit order is always valuable for the concentration check.
            if position_value_hint is None:
                try:
                    lmt = float(order.lmtPrice or 0)
                    multiplier = self._multiplier(contract)
                    if lmt > 0:
                        position_value_hint = abs(float(order.totalQuantity or 0)) * lmt * multiplier
                except (TypeError, ValueError):
                    pass

            inputs = self.trader.gather_risk_inputs()
            if position_value_hint is not None:
                position_value_hint = self.trader.convert_notional(
                    position_value_hint, getattr(contract, 'currency', ''))
            try:
                aggregate_value = self.trader.aggregate_position_value(
                    contract, str(order.action), float(order.totalQuantity),
                    position_value_hint or 0.0)
            except WorkingOrdersUnreadableError as ex:
                # The concentration check needs the working openings on this
                # instrument. Unreadable is not zero: refuse the open.
                await self._log_event(EventType.RISK_GATE_REJECTED, contract, order)
                logging.error('refusing open (fail-closed): %s', ex)
                return rx.throw(
                    trader_exception(
                        trader=self.trader,
                        exception_type=TraderException,
                        message=f'risk gate rejected: concentration unevaluable — {ex}'
                    )
                )
            result = gate.evaluate(
                signal=signal,
                open_order_count=inputs.open_order_count,
                daily_pnl=inputs.daily_pnl,
                portfolio_value=inputs.portfolio_value,
                position_value=position_value_hint or 0.0,
                daily_pnl_evaluable=inputs.daily_pnl_evaluable,
                portfolio_value_evaluable=inputs.portfolio_value_evaluable,
                position_value_evaluable=position_value_hint is not None,
                sec_type=contract.secType or '',
                aggregate_position_value=aggregate_value,
            )
            if not result.approved:
                await self._log_event(EventType.RISK_GATE_REJECTED, contract, order)
                logging.warning(f'risk gate rejected order: {result.reason}')
                return rx.throw(
                    trader_exception(
                        trader=self.trader,
                        exception_type=TraderException,
                        message=f'risk gate rejected: {result.reason}'
                    )
                )
            gate_checks = {**result.checks, **margin_result.checks}

        if condition == condition.SANITY_CHECK:
            logging.debug('sanity_check_order for {}'.format(contract_order))
            snapshot: Ticker = await self.trader.client.get_snapshot(contract_order.contract, delayed=False)
            if not self.validator.sanity_check_order(contract_order, self.trader.book, snapshot):
                return rx.throw(
                    trader_exception(
                        trader=self.trader,
                        exception_type=TraderException,
                        message='sanity_check_order failed for {}'.format(contract_order)
                    )
                )

        # Server-side notional-tier approver gate (Phase 2), unified with the
        # approve() path. Called unconditionally: it no-ops when the feature is
        # off and for ALL exit-class orders, so it is safe on every direct order —
        # but it DOES gate an above-threshold pure open that arrived through the
        # direct buy/sell path without a valid key. It also gates a split flip's
        # opening remainder: `force_open` is passed through, because that half's
        # position read still shows the pre-reduction size and would otherwise
        # claim the exit exemption. (Until 2026-07-27 it did exactly that, and
        # this comment described it as a documented residual; splitting plus
        # force_open closes it.)
        tier_error = await self.trader.enforce_approver_tier(
            contract, str(order.action), float(order.totalQuantity or 0),
            str(getattr(order, 'orderType', '') or ''),
            getattr(order, 'lmtPrice', 0.0), approver_key,
            force_open=force_open)
        if tier_error:
            await self._log_event(EventType.RISK_GATE_REJECTED, contract, order)
            logging.warning('approver tier rejected order: %s', tier_error)
            return rx.throw(
                trader_exception(
                    trader=self.trader,
                    exception_type=TraderException,
                    message=tier_error,
                )
            )

        logging.debug('placing order {}'.format(contract_order.order))
        # Gate passed (or exit-class exempt): mint the capability token and
        # hand it to the sink. This is the ONLY mint on the direct path.
        approved = mint_approved_order(
            contract_order.contract, contract_order.order,
            is_exit=is_exit, checks=gate_checks,
            exit_reason=ExitReason.POSITION_CLASSIFIED)
        return await self.subscribe_place_order_direct(approved)

    def place_basket(
        self,
        basket: Basket
    ):
        pass

    def cancel_order_id(self, order_id: int) -> Optional[Trade]:
        # get the Order
        order = self.trader.book.get_order(order_id)
        if order and order.clientId == self.trader.trading_runtime_ib_client_id:
            logging.info('cancelling order {}'.format(order))
            trade = self.trader.client.ib.cancelOrder(order)
            return trade
        else:
            logging.error('either order does not exist, or originating client_id is different: {} {}'
                          .format(order, self.trader.trading_runtime_ib_client_id))
            return None

    def cancel_basket(
        self,
        basket: Basket
    ):
        pass

    def helper_create_order(
        self,
        contract: Contract,
        action: Action,
        latest_tick: Ticker,
        equity_amount: Optional[float],
        quantity: Optional[float],
        limit_price: Optional[float],
        market_order: bool,
        stop_loss_percentage: float,
        algo_name: str,
        debug: bool = False,
    ) -> ContractOrderPair:
        if limit_price and limit_price <= 0.0:
            raise ValueError('limit_price specified but invalid: {}'.format(limit_price))
        if stop_loss_percentage >= 1.0 or stop_loss_percentage < 0.0:
            raise ValueError('stop_loss_percentage invalid: {}'.format(stop_loss_percentage))
        if not equity_amount and not quantity:
            raise ValueError('equity_amount or quantity need to be specified')

        order_price = 0.0

        if not quantity and equity_amount:
            # Size a BUY by what we'd pay (ask) and a SELL by what we'd
            # receive (bid). Floors and refuses (ValueError) when the amount
            # doesn't cover one whole share — never bumps to 1, which turned
            # a small sized notional into an oversized full share.
            multiplier = self._multiplier(contract)
            ref_price = latest_tick.ask if action == Action.BUY else latest_tick.bid
            quantity = float(whole_shares_for_notional(equity_amount, ref_price, multiplier))
            assert quantity * ref_price * multiplier <= equity_amount * 1.05, (
                f'sized quantity {quantity} x {ref_price} x {multiplier} exceeds '
                f'equity_amount {equity_amount}'
            )
            logging.debug('helper_create_order assessed quantity: {} on {} price: {}'.format(
                quantity, 'ask' if action == Action.BUY else 'bid', ref_price
            ))

        if limit_price:
            order_price = float(limit_price)
        elif market_order:
            order_price = latest_tick.ask

        # if debug, move the buy/sell by 10%
        if debug and action == Action.BUY:
            order_price = order_price * 0.9
            order_price = round(order_price * 0.9, ndigits=2)
        if debug and action == Action.SELL:
            order_price = round(order_price * 1.1, ndigits=2)

        # This single-order helper cannot express "enter now + attach a
        # protective stop" — it builds ONE order. The old code, when asked for a
        # market order WITH a stop loss, made a bare StopOrder the *entry*: the
        # stop-loss level became the trigger to open the position (a stop-entry),
        # so the position had no protection and, for a BUY, a stop below market
        # fires immediately. That is a wrong-direction trade. Refuse loudly and
        # point at the path that does protection correctly (propose / bracket).
        if stop_loss_percentage > 0.0:
            raise ValueError(
                'stop-loss protection is not supported on the simple order path — '
                'it would be placed as the entry trigger (wrong). Use `mmr propose ... '
                '--stop-loss <price>` / place_expressive_order (bracket) for a '
                'protected order.'
            )

        order: Order = Order()

        # outsideRth=True, matching every OTHER order path in the system
        # (ExecutionSpec.outside_rth and place_standalone_order both default
        # True). Without it IB applies the account's RTH-only preset and
        # answers warning 399: "Your order will not be placed at the exchange
        # until <next session open>". Found live 2026-07-27 in extended hours:
        # a direct `mmr sell` reported PendingSubmit and sat there, which is
        # exactly the wrong behaviour for THIS path — `mmr sell` / `mmr close`
        # is the manual emergency close a human reaches for out of hours, and
        # it silently became a resting order for the next session instead.
        # (The auto-executor is unaffected: its closes go through
        # place_expressive_order, and protective stops through
        # place_standalone_order, both of which already set it.)
        if market_order:
            order = MarketOrder(
                action=str(action),
                totalQuantity=cast(float, quantity),
                orderRef=algo_name,
                account=self.trader.ib_account,
                outsideRth=True,
            )
        else:
            order = LimitOrder(
                action=str(action),
                totalQuantity=cast(float, quantity),
                lmtPrice=order_price,
                orderRef=algo_name,
                account=self.trader.ib_account,
                outsideRth=True,
            )
        return ContractOrderPair(contract=contract, order=order)
