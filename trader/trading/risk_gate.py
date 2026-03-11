from dataclasses import dataclass
from trader.common.logging_helper import setup_logging
from trader.data.event_store import EventStore, EventType
from trader.trading.strategy import Signal
from typing import Optional

import datetime as dt


logging = setup_logging(module_name='risk_gate')


@dataclass
class RiskLimits:
    max_position_size_pct: float = 0.10
    max_daily_loss: float = 1000.0
    max_open_orders: int = 10
    max_signals_per_hour: int = 20


@dataclass
class RiskGateResult:
    approved: bool
    reason: str = ''


class RiskGate:
    def __init__(self, limits: RiskLimits, event_store: EventStore):
        self.limits = limits
        self.event_store = event_store

    def evaluate(
        self,
        signal: Signal,
        open_order_count: int = 0,
        daily_pnl: float = 0.0,
        portfolio_value: float = 0.0,
        position_value: float = 0.0,
    ) -> RiskGateResult:
        # Max open orders check
        if open_order_count >= self.limits.max_open_orders:
            return RiskGateResult(
                approved=False,
                reason=f'max open orders exceeded: {open_order_count} >= {self.limits.max_open_orders}'
            )

        # Daily loss check
        if daily_pnl < -self.limits.max_daily_loss:
            return RiskGateResult(
                approved=False,
                reason=f'daily loss limit exceeded: {daily_pnl} < -{self.limits.max_daily_loss}'
            )

        # Position concentration check
        if portfolio_value > 0 and position_value > 0:
            concentration = position_value / portfolio_value
            if concentration > self.limits.max_position_size_pct:
                return RiskGateResult(
                    approved=False,
                    reason=f'position concentration too high: {concentration:.2%} > {self.limits.max_position_size_pct:.2%}'
                )

        # Signal rate limit check
        one_hour_ago = dt.datetime.now() - dt.timedelta(hours=1)
        signal_count = self.event_store.count_since(
            since=one_hour_ago,
            event_type=EventType.SIGNAL,
            strategy_name=signal.source_name,
        )
        if signal_count >= self.limits.max_signals_per_hour:
            return RiskGateResult(
                approved=False,
                reason=f'signal rate limit exceeded: {signal_count} >= {self.limits.max_signals_per_hour}/hour'
            )

        logging.debug(f'risk gate approved signal from {signal.source_name}')
        return RiskGateResult(approved=True)
