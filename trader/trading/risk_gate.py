from dataclasses import dataclass, field, fields
from pathlib import Path
from trader.common.logging_helper import setup_logging
from trader.data.event_store import EventStore, EventType
from trader.trading.order_math import order_notional
from trader.trading.strategy import Signal
from typing import Dict, Optional, Tuple, TYPE_CHECKING

import datetime as dt
import math
import os
import yaml

if TYPE_CHECKING:
    from trader.trading.trading_filter import TradingFilter


logging = setup_logging(module_name='risk_gate')


@dataclass
class RiskLimits:
    max_position_size_pct: float = 0.10
    max_daily_loss: float = 1000.0
    max_open_orders: int = 15
    max_signals_per_hour: int = 20
    max_leverage: float = 1.0        # 1.0 = cash only, 2.0 = 2x margin allowed
    min_margin_cushion: float = 0.10  # minimum Cushion (excess liq / net liq)
    # Cumulative OPENING notional per trading day. 0.0 = OFF (the default, so
    # deploying this is byte-identical until an operator opts in).
    #
    # Every other limit here is per-order: size, leverage, loss, open-order
    # count. None of them bound cumulative activity, so open/close/open/close
    # churns indefinitely with each individual order passing every check.
    # max_signals_per_hour is the closest existing control and counts ORDERS,
    # not value — 20/hour is nothing at $500 each and $1M at $50k each.
    #
    # These cap the OPENING side only, which is what lets the cap exist at all:
    # exits must never be refusable, and you cannot churn without opening, so
    # bounding opens bounds the loop while leaving every close untouchable.
    max_daily_open_notional: float = 0.0              # whole account
    max_daily_open_notional_per_strategy: float = 0.0  # one runaway strategy

    @staticmethod
    def load(path: Optional[str] = None) -> 'RiskLimits':
        """Load limits from the ``risk_limits`` mapping in trader.yaml.

        A missing file or missing section means "use defaults". A malformed
        section, an unknown key, or a malformed value is an operator error —
        but this runs at ``connect()`` while live positions are held, and
        supervise's 5-rapid-deaths circuit breaker would tear the whole stack
        down if we raised here. So log an ERROR naming the offending key and
        fall back to the safe default for it rather than crash-looping: coming
        up under default limits beats not coming up at all.
        """
        if path:
            filepath = Path(path)
        elif os.getenv('TRADER_CONFIG'):
            filepath = Path(str(os.getenv('TRADER_CONFIG')))
        else:
            from trader.container import default_config_path
            filepath = Path(default_config_path())
        if not filepath.exists():
            return RiskLimits()
        try:
            with open(filepath) as f:
                data = yaml.safe_load(f) or {}
        except Exception as ex:
            logging.error(
                '%s: could not read risk_limits — using default limits: %s', filepath, ex)
            return RiskLimits()
        section = data.get('risk_limits')
        if section is None:
            return RiskLimits()
        if not isinstance(section, dict):
            logging.error(
                '%s: risk_limits must be a mapping, got %s — using default limits',
                filepath, type(section).__name__)
            return RiskLimits()
        known = {f.name: f.default for f in fields(RiskLimits)}
        kwargs = {}
        for key, value in section.items():
            if key not in known:
                logging.error(
                    '%s: unknown risk_limits key %r (ignored; using defaults for it); '
                    'known keys: %s', filepath, key, sorted(known))
                continue
            try:
                kwargs[key] = type(known[key])(value)
            except (TypeError, ValueError) as ex:
                logging.error(
                    '%s: risk_limits.%s has malformed value %r (%s) — using default %r',
                    filepath, key, value, ex, known[key])
        # **kwargs is dynamically built from validated RiskLimits fields; ty can't track the dict-value types through the unpack.
        return RiskLimits(**kwargs)  # ty: ignore[invalid-argument-type]


@dataclass(frozen=True)
class RiskInputs:
    """Snapshot of the account state the risk gate evaluates against.

    Each per-field ``*_evaluable`` flag distinguishes "read succeeded and the
    value is genuinely this (possibly 0.0)" from "could not read" — the gate
    refuses exposure-increasing orders on the latter rather than silently
    no-op'ing the check against a default.
    """
    open_order_count: int = 0
    daily_pnl: float = 0.0
    daily_pnl_evaluable: bool = False
    portfolio_value: float = 0.0
    portfolio_value_evaluable: bool = False


@dataclass
class RiskGateResult:
    approved: bool
    reason: str = ''
    # check name -> 'pass' | 'fail' | 'skipped:<reason>'
    checks: Dict[str, str] = field(default_factory=dict)


class RiskGate:
    def __init__(self, limits: RiskLimits, event_store: EventStore):
        self.limits = limits
        self.event_store = event_store
        self.trading_filter: Optional['TradingFilter'] = None

    def evaluate(
        self,
        signal: Signal,
        open_order_count: int = 0,
        daily_pnl: float = 0.0,
        portfolio_value: float = 0.0,
        position_value: float = 0.0,
        daily_pnl_evaluable: bool = True,
        portfolio_value_evaluable: bool = True,
        position_value_evaluable: bool = True,
        sec_type: str = '',
    ) -> RiskGateResult:
        """Evaluate risk limits for an exposure-increasing order.

        Only ever called for non-exit-class orders — exit-class orders (those
        that reduce the live broker position) are exempt at the call sites and
        must never reach here. A critical input that could not be read
        (``*_evaluable`` False) REFUSES the order naming the missing datum:
        opening exposure blind to daily loss or concentration is exactly the
        state the gate exists to prevent.
        """
        checks: Dict[str, str] = {}

        # Max open orders check
        if open_order_count >= self.limits.max_open_orders:
            checks['max_open_orders'] = 'fail'
            return RiskGateResult(
                approved=False,
                reason=f'max open orders exceeded: {open_order_count} >= {self.limits.max_open_orders}',
                checks=checks,
            )
        checks['max_open_orders'] = 'pass'

        # Daily loss check — an unreadable daily PnL is NOT a pass.
        if not daily_pnl_evaluable:
            checks['daily_loss'] = 'skipped:not-evaluable'
            return RiskGateResult(
                approved=False,
                reason='daily PnL could not be read — refusing to open new exposure '
                       'without the daily-loss check (fail-closed)',
                checks=checks,
            )
        if daily_pnl < -self.limits.max_daily_loss:
            checks['daily_loss'] = 'fail'
            return RiskGateResult(
                approved=False,
                reason=f'daily loss limit exceeded: {daily_pnl} < -{self.limits.max_daily_loss}',
                checks=checks,
            )
        checks['daily_loss'] = 'pass'

        # Position concentration check. A forex order (sec_type CASH) is a
        # currency conversion, not position concentration — a $21.6k EUR buy
        # on a $17k CAD account is not a 127%-of-NetLiq stock position. Exempt
        # CASH entirely; everything else keeps the concentration cap.
        if str(sec_type).upper() == 'CASH':
            checks['concentration'] = 'skipped:forex-cash'
        elif not portfolio_value_evaluable:
            checks['concentration'] = 'skipped:portfolio-value-not-evaluable'
            return RiskGateResult(
                approved=False,
                reason='portfolio value (NetLiquidation) could not be read — refusing to '
                       'open new exposure without the concentration check (fail-closed)',
                checks=checks,
            )
        elif not position_value_evaluable:
            checks['concentration'] = 'skipped:no-price'
            return RiskGateResult(
                approved=False,
                reason='no price available to value the position — refusing to open new '
                       'exposure without the concentration check (fail-closed)',
                checks=checks,
            )
        elif position_value > 0:
            if portfolio_value <= 0:
                checks['concentration'] = 'fail'
                return RiskGateResult(
                    approved=False,
                    reason=f'position concentration too high: position value {position_value:.2f} '
                           f'against portfolio value {portfolio_value:.2f}',
                    checks=checks,
                )
            concentration = position_value / portfolio_value
            if concentration > self.limits.max_position_size_pct:
                checks['concentration'] = 'fail'
                return RiskGateResult(
                    approved=False,
                    reason=f'position concentration too high: {concentration:.2%} > {self.limits.max_position_size_pct:.2%}',
                    checks=checks,
                )
            checks['concentration'] = 'pass'
        else:
            checks['concentration'] = 'skipped:zero-notional'

        # Order rate limit check. Counts ORDER_SUBMITTED (which the order
        # paths actually write) rather than SIGNAL (which they never did —
        # the old check was dead code). Two corrections over count_since:
        #   1. Only exposure-INCREASING submissions count — exit-class orders
        #      (closes, protective stops, bracket legs) are stamped exit_class
        #      and excluded, so an active session's own exits can't false-trip
        #      the open-rate limit.
        #   2. Only submissions attributable to the SAME source the pseudo-
        #      signal names (signal.source_name == the order's orderRef) count,
        #      so two sources don't contaminate each other's bucket.
        # An empty event store is a legitimate count of 0: checked and passed,
        # not "not evaluable".
        one_hour_ago = dt.datetime.now() - dt.timedelta(hours=1)
        recent = self.event_store.query_since(
            since=one_hour_ago, event_type=EventType.ORDER_SUBMITTED)
        order_count = sum(
            1 for e in recent
            if e.strategy_name == signal.source_name
            and not (e.metadata or {}).get('exit_class'))
        if order_count >= self.limits.max_signals_per_hour:
            checks['order_rate'] = 'fail'
            return RiskGateResult(
                approved=False,
                reason=f'order rate limit exceeded: {order_count} >= {self.limits.max_signals_per_hour}/hour',
                checks=checks,
            )
        checks['order_rate'] = 'pass'

        turnover_result = self._check_daily_turnover(
            signal, checks, position_value, position_value_evaluable, sec_type)
        if turnover_result is not None:
            return turnover_result

        logging.debug(f'risk gate approved signal from {signal.source_name}')
        return RiskGateResult(approved=True, checks=checks)

    def _check_daily_turnover(
        self, signal: Signal, checks: Dict[str, str],
        position_value: float, position_value_evaluable: bool, sec_type: str,
    ) -> Optional[RiskGateResult]:
        """Cumulative OPENING notional for the trading day, per strategy and
        for the account. Returns a refusal, or None to allow.

        Bounds what no other limit does: total activity. Every other check is
        per-order, so open/close/open/close passes all of them forever while
        bleeding commissions, slippage and market impact.

        Only opens are counted, and that is what makes the cap compatible with
        "an exit is never refusable": a churn cycle needs an open, so bounding
        opens bounds the cycle without ever standing in the way of a close.
        This method is only reached on the non-exit path.
        """
        account_cap = float(self.limits.max_daily_open_notional or 0.0)
        strategy_cap = float(self.limits.max_daily_open_notional_per_strategy or 0.0)
        if account_cap <= 0 and strategy_cap <= 0:
            # Record NOTHING when the feature is off, matching the approver
            # tier: `checks` describes how THIS order was evaluated, not which
            # features the deployment has enabled. It is also the difference
            # between satisfying and revising the human-owned property that an
            # approved order carries no unexplained skip
            # (tests/invariants/test_gate_properties.py).
            return None

        # Forex is exempt, for the same reason concentration exempts it: a
        # currency conversion is not a position, and its notional is not
        # comparable to an equity order's. Turnover in a CASH pair is real, so
        # this is a known limitation rather than a claim that it does not
        # matter (recorded in docs/SAFETY_ROADMAP.md).
        if str(sec_type).upper() == 'CASH':
            logging.debug('daily turnover cap does not apply to a CASH order')
            return None

        # No fail-closed branch for an unvaluable order here, deliberately: the
        # concentration check above already refuses a non-evaluable
        # position_value for every non-CASH order, and CASH is exempt from this
        # cap, so such an order can never reach this line. A branch that cannot
        # fire is worse than no branch — it reads as protection while doing
        # nothing. A zero notional DOES arrive here (concentration records
        # skipped:zero-notional and allows it); it simply contributes nothing to
        # the running total.
        this_order = position_value if position_value > 0 else 0.0

        spent, spent_by_strategy, unvaluable, legacy = self._daily_open_notional()

        # Two very different reasons a submission cannot be valued, and
        # collapsing them was a footgun.
        #
        # LEGACY: written before order notionals were recorded at all, so the
        # key is simply absent. Nothing is wrong with the system; there is
        # history from before the feature existed. Refusing on this turned
        # "enable the cap" into "no opens for the rest of today", which is how
        # a safety control gets switched off and left off. It self-clears at
        # the day boundary, so proceed on the lower bound and say so loudly.
        #
        # LIVE: the CURRENT build tried to value the order and could not. That
        # is a real degradation of the input this cap runs on, and it is the
        # fail-closed case the design wants.
        if legacy:
            logging.warning(
                "daily turnover: %d of today's opening submissions predate "
                "order-notional recording, so today's turnover (>= $%s) is a LOWER "
                "BOUND. Proceeding on it; this clears at the day boundary.",
                legacy, f'{spent:,.0f}')
        if unvaluable:
            checks['daily_turnover'] = 'skipped:unvaluable-history'
            return RiskGateResult(
                approved=False,
                reason=f'{unvaluable} of today\'s opening submissions could not be valued '
                       f'by this build, so today\'s turnover (>= ${spent:,.0f}) is only a '
                       f'lower bound — refusing to open while a turnover cap is active '
                       f'(fail-closed). This is a live valuation failure, not old history: '
                       f'check that market data is flowing for the symbols being traded.',
                checks=checks,
            )

        mine = spent_by_strategy.get(signal.source_name, 0.0)
        if strategy_cap > 0 and mine + this_order > strategy_cap:
            checks['daily_turnover'] = 'fail'
            return RiskGateResult(
                approved=False,
                reason=f'daily turnover cap for {signal.source_name}: '
                       f'${mine:,.0f} opened today + ${this_order:,.0f} this order '
                       f'> ${strategy_cap:,.0f}',
                checks=checks,
            )
        if account_cap > 0 and spent + this_order > account_cap:
            checks['daily_turnover'] = 'fail'
            return RiskGateResult(
                approved=False,
                reason=f'daily account turnover cap: ${spent:,.0f} opened today + '
                       f'${this_order:,.0f} this order > ${account_cap:,.0f}',
                checks=checks,
            )
        checks['daily_turnover'] = 'pass'
        return None

    def _daily_open_notional(self) -> Tuple[float, Dict[str, float], int, int]:
        """Today's opening notional as
        ``(total, per_strategy, unvaluable_count, legacy_count)``.

        The two counts are separate on purpose: ``unvaluable`` is a LIVE
        failure of this build to value an order it placed, ``legacy`` is
        history from before notionals were recorded at all. Only the first is
        worth refusing over.

        Read from the event store, so it survives a restart. In-memory counters
        would reset on every crash, and supervise() restarts the service
        automatically — a crash loop would hand back a fresh budget each time.

        Exit-class submissions are excluded by their stamp, so a session's own
        closes and protective stops never consume the opening budget.
        """
        start_of_day = dt.datetime.combine(dt.date.today(), dt.time.min)
        events = self.event_store.query_since(
            since=start_of_day, event_type=EventType.ORDER_SUBMITTED)
        # Fills carry a real avgFillPrice, so they can value a submission whose
        # own record could not be valued — a market order placed on a symbol
        # with no cached price, for instance. Most submissions that matter do
        # fill, so this shrinks the unvaluable set to orders that never traded.
        fill_price: Dict[int, float] = {}
        for f in self.event_store.query_since(
                since=start_of_day, event_type=EventType.ORDER_FILLED):
            try:
                price = float(f.price)
            except (TypeError, ValueError):
                continue
            if math.isfinite(price) and price > 0 and f.order_id:
                fill_price.setdefault(int(f.order_id), price)
        total = 0.0
        per_strategy: Dict[str, float] = {}
        unvaluable = 0
        legacy = 0
        for e in events:
            meta = e.metadata or {}
            if meta.get('exit_class'):
                continue
            notional = None
            raw = meta.get('notional')
            if meta.get('notional_evaluable') and raw is not None:
                # metadata is JSON off disk, so `raw` can be any shape. A gate
                # that raises while reading history refuses nothing and breaks
                # every open, so every conversion here is guarded.
                try:
                    candidate = float(raw)
                except (TypeError, ValueError):
                    candidate = None
                if candidate is not None and math.isfinite(candidate) and candidate >= 0:
                    notional = candidate
            if notional is None:
                # Fall back to the event's own price column, then to the price
                # this order actually filled at. order_notional rejects
                # ib_async's unset sentinel, which is what a MARKET order's
                # recorded price column actually holds.
                value, ok = order_notional(
                    (e.price, fill_price.get(int(e.order_id or 0))), e.quantity)
                notional = value if ok else None
            if notional is None:
                # 'notional_evaluable' absent entirely means this row predates
                # notional recording: old history, not a live failure. The
                # distinction is the difference between "enable the cap" and
                # "no opens for the rest of today" — see _check_daily_turnover.
                if 'notional_evaluable' in meta:
                    unvaluable += 1
                else:
                    legacy += 1
                continue
            total += notional
            per_strategy[e.strategy_name] = per_strategy.get(e.strategy_name, 0.0) + notional
        return (total, per_strategy, unvaluable, legacy)

    def check_instrument(
        self,
        symbol: str,
        exchange: str = '',
        sec_type: str = '',
    ) -> RiskGateResult:
        """Check if an instrument is allowed by trading filters."""
        if not self.trading_filter:
            return RiskGateResult(approved=True)
        allowed, reason = self.trading_filter.is_allowed(symbol, exchange, sec_type)
        if not allowed:
            return RiskGateResult(approved=False, reason=f'trading filter: {reason}')
        return RiskGateResult(approved=True)

    def check_leverage(
        self,
        margin_impact: dict,
        net_liquidation: float,
    ) -> RiskGateResult:
        """Check an order's margin impact against the leverage limits — and
        REFUSE when the inputs cannot be read, exactly like ``evaluate``.

        FLIPPED TO FAIL-CLOSED 2026-07-26 (operator decision). This was the one
        gate that still approved on missing data: an absent ``initMarginAfter``,
        an unreadable ``NetLiquidation`` or an empty ``margin_impact`` skipped
        the checks and returned a bare approval. The old rationale (the
        concentration check backstops NetLiquidation; whatIfOrder has never
        failed in production; the limit sits ~33x from binding) is preserved in
        git history — the counterargument that won is consistency: every OTHER
        critical input refuses opens when unreadable, and a gate that quietly
        stops checking is the audit-trail failure the tri-state records were
        built to prevent.

        Only ever called for exposure-increasing orders — exits are routed away
        before the margin section and can never be refused here. Expect brief
        open-refusals right after connect while IB's account feeds warm up,
        matching the documented behaviour of the other gates.
        """
        checks: Dict[str, str] = {}
        init_margin_after = margin_impact.get('initMarginAfter', 0)
        equity_after = margin_impact.get('equityWithLoanAfter', 0)

        if net_liquidation <= 0:
            checks['leverage'] = 'skipped:net-liq-not-evaluable'
            checks['margin_cushion'] = 'skipped:net-liq-not-evaluable'
            return RiskGateResult(
                approved=False,
                reason='NetLiquidation could not be read (or is not positive) — '
                       'refusing to open new exposure without the leverage check '
                       '(fail-closed; exits are exempt)',
                checks=checks,
            )

        if not init_margin_after:
            checks['leverage'] = 'skipped:no-margin-data'
            checks['margin_cushion'] = 'skipped:no-margin-data'
            return RiskGateResult(
                approved=False,
                reason='margin impact carried no initMarginAfter — refusing to '
                       'open new exposure without the leverage check '
                       '(fail-closed; exits are exempt)',
                checks=checks,
            )

        post_leverage = init_margin_after / net_liquidation
        if post_leverage > self.limits.max_leverage:
            checks['leverage'] = 'fail'
            return RiskGateResult(
                approved=False,
                reason=f'post-trade leverage {post_leverage:.2f}x exceeds limit {self.limits.max_leverage:.2f}x',
                checks=checks,
            )
        checks['leverage'] = 'pass'

        if not equity_after:
            checks['margin_cushion'] = 'skipped:no-equity-data'
            return RiskGateResult(
                approved=False,
                reason='margin impact carried no equityWithLoanAfter — refusing '
                       'to open new exposure without the margin-cushion check '
                       '(fail-closed; exits are exempt)',
                checks=checks,
            )

        cushion = (equity_after - init_margin_after) / net_liquidation
        if cushion < self.limits.min_margin_cushion:
            checks['margin_cushion'] = 'fail'
            return RiskGateResult(
                approved=False,
                reason=f'margin cushion {cushion:.2%} below minimum {self.limits.min_margin_cushion:.2%}',
                checks=checks,
            )
        checks['margin_cushion'] = 'pass'

        return RiskGateResult(approved=True, checks=checks)
