from dataclasses import dataclass, field, fields
from pathlib import Path
from trader.common.logging_helper import setup_logging
from trader.data.event_store import EventStore, EventType
from trader.trading.strategy import Signal
from typing import Dict, Optional, TYPE_CHECKING

import datetime as dt
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

        logging.debug(f'risk gate approved signal from {signal.source_name}')
        return RiskGateResult(approved=True, checks=checks)

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
        """Check if an order's margin impact exceeds leverage limits.

        Records a tri-state per check, like ``evaluate``. The OUTCOMES are
        unchanged — this still approves when the data is missing rather than
        refusing — but the skip is no longer SILENT. Previously an absent
        ``initMarginAfter``, an empty ``margin_impact`` or a non-positive
        ``net_liquidation`` skipped both branches and returned a bare approval,
        so nothing downstream could tell "leverage checked and fine" from
        "leverage never checked". That distinction is the whole point of the
        tri-state, and mutation testing found the missing-data paths were
        untested precisely because nothing observable depended on them.

        Deliberately NOT fail-closed, unlike ``evaluate``: an unreadable
        NetLiquidation is already refused downstream by the concentration check
        (``portfolio_value_evaluable``), the only unbackstopped case is a
        whatIfOrder failure which has never occurred in production, and the
        limit sits ~33x from binding at current book size. Refusing here would
        add a new way to miss trades to guard something that is not close. The
        recorded skips are the evidence to revisit that if it changes.
        """
        checks: Dict[str, str] = {}
        init_margin_after = margin_impact.get('initMarginAfter', 0)
        equity_after = margin_impact.get('equityWithLoanAfter', 0)

        if net_liquidation <= 0:
            # Both branches are guarded on net_liquidation > 0; say so once.
            checks['leverage'] = 'skipped:net-liq-not-evaluable'
            checks['margin_cushion'] = 'skipped:net-liq-not-evaluable'
            return RiskGateResult(approved=True, checks=checks)

        if not init_margin_after:
            checks['leverage'] = 'skipped:no-margin-data'
        else:
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
        else:
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
