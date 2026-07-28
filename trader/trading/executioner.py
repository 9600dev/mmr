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
from trader.trading.order_validator import OrderValidator
from trader.trading.risk_gate import RiskGate, RiskGateResult
from typing import cast, List, Optional, TYPE_CHECKING

import datetime as dt
import math
import reactivex as rx
import reactivex.operators as ops
import sys


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


class TradeExecutioner():
    def __init__(
        self,
    ):
        self.trader: 'Trader'
        self.connected: bool = False
        self.validator: OrderValidator = OrderValidator()

    def connect(self, trader: 'Trader'):
        self.trader = trader
        self.connected = True

    def _log_event(self, event_type: EventType, contract: Contract, order: Order,
                   strategy_name: Optional[str] = None, is_exit: bool = False) -> None:
        if hasattr(self.trader, 'event_store'):
            # Stamp the real originator: the order's orderRef (approve derives
            # it from proposal.metadata['strategy']; the direct path stamps its
            # algo_name). That is exactly the source the risk gate's pseudo-
            # signal names, so the open-rate check counts the right bucket
            # instead of a dead 'manual'/'proposal' constant.
            if strategy_name is None:
                strategy_name = (getattr(order, 'orderRef', '') or '').strip() or 'manual'
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
            metadata['notional'] = notional
            metadata['notional_evaluable'] = notional_evaluable
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
            self.trader.event_store.append(event)

    def _multiplier(self, contract: Contract) -> float:
        try:
            return float(contract.multiplier) if contract.multiplier else 1.0
        except (TypeError, ValueError):
            return 1.0

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
        # OBSERVABILITY ONLY, deliberately: a mismatch means the position moved
        # between classification and placement, and refusing an exit is worse
        # than acting on a stale classification. This logs and records; it never
        # blocks. (Refusing here would also re-introduce exactly the read-race
        # that enforce_approver_tier documents as its reason not to re-check.)
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
                            'live position no longer makes it a reduction — placing anyway '
                            '(an exit is never refused). Position likely moved between '
                            'classification and placement.', approved)
                        self._log_event(EventType.RISK_GATE_REJECTED, contract, order)

        if not is_exit:
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

        try:
            observable = await self.trader.client.subscribe_place_order(contract, order)
        except Exception as ex:
            return trader_exception_helper(ex)

        # The order is now LIVE at IB. Event-store logging must NEVER be able to
        # turn a successful placement into a reported failure — a caller that
        # sees failure may retry and place a duplicate order. Isolate it.
        try:
            self._log_event(EventType.ORDER_SUBMITTED, contract, order, is_exit=is_exit)
        except Exception as ex:
            logging.error(
                'order placed but ORDER_SUBMITTED event-store append failed '
                '(order IS live, not retrying): %s', ex)
        return observable.pipe(
            ops.catch(lambda ex, src: trader_exception_helper(ex))
        )

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
                self._log_event(EventType.RISK_GATE_REJECTED, contract, order)
                logging.warning(f'trading filter rejected order: {instrument_result.reason}')
                return rx.throw(
                    trader_exception(
                        trader=self.trader,
                        exception_type=TraderException,
                        message=f'trading filter rejected: {instrument_result.reason}'
                    )
                )

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
                    multiplier = float(contract.multiplier) if contract.multiplier else 1.0
                    if lmt > 0:
                        position_value_hint = abs(float(order.totalQuantity or 0)) * lmt * multiplier
                except (TypeError, ValueError):
                    pass

            inputs = self.trader.gather_risk_inputs()
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
            )
            if not result.approved:
                self._log_event(EventType.RISK_GATE_REJECTED, contract, order)
                logging.warning(f'risk gate rejected order: {result.reason}')
                return rx.throw(
                    trader_exception(
                        trader=self.trader,
                        exception_type=TraderException,
                        message=f'risk gate rejected: {result.reason}'
                    )
                )
            gate_checks = result.checks

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
            self._log_event(EventType.RISK_GATE_REJECTED, contract, order)
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
            multiplier = float(contract.multiplier) if contract.multiplier else 1.0
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
