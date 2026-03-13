"""Position sizing — portfolio-aware sizing with hard limits and risk appetite."""

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import yaml


_DEFAULT_CONFIG_NAME = 'position_sizing.yaml'

RISK_MULTIPLIERS = {
    'conservative': 0.5,
    'moderate': 1.0,
    'aggressive': 1.5,
}


def _default_path() -> Path:
    from trader.container import ensure_config_dir
    return ensure_config_dir() / _DEFAULT_CONFIG_NAME


@dataclass
class PositionSizingConfig:
    """Hard limits + appetite knobs. Loaded from YAML, editable via CLI."""

    # Hard limits (cannot be overridden)
    min_position_usd: float = 500.0
    max_position_usd: float = 25_000.0
    max_position_pct: float = 0.10        # max 10% of net liq in one position
    max_total_exposure_pct: float = 0.80  # max 80% of net liq deployed
    max_positions: int = 20

    # Risk appetite (adjustable per session via CLI)
    base_position_usd: float = 5000.0     # default position size at confidence=1.0
    base_position_pct: float = 0.0        # % of net liq as base (0 = use fixed base_position_usd)
    risk_level: str = 'moderate'          # conservative / moderate / aggressive
    daily_loss_limit_usd: float = 2000.0  # stop proposing after this daily loss

    # Confidence scaling
    min_confidence_scale: float = 0.3     # at confidence=0.0, size = base * 0.3

    # Liquidity constraints
    max_adv_pct: float = 0.02            # max 2% of avg daily volume (exit within a day)
    spread_penalty_threshold: float = 0.005  # start penalizing above 0.5% spread
    spread_penalty_factor: float = 0.5   # at threshold, reduce size by up to 50%

    @staticmethod
    def load(path: Optional[str] = None) -> 'PositionSizingConfig':
        """Load from YAML file. Returns default if file doesn't exist."""
        filepath = Path(path) if path else _default_path()
        if not filepath.exists():
            return PositionSizingConfig()
        try:
            with open(filepath) as f:
                data = yaml.safe_load(f) or {}
            return PositionSizingConfig(
                min_position_usd=float(data.get('min_position_usd', 500.0)),
                max_position_usd=float(data.get('max_position_usd', 25_000.0)),
                max_position_pct=float(data.get('max_position_pct', 0.10)),
                max_total_exposure_pct=float(data.get('max_total_exposure_pct', 0.80)),
                max_positions=int(data.get('max_positions', 20)),
                base_position_usd=float(data.get('base_position_usd', 5000.0)),
                base_position_pct=float(data.get('base_position_pct', 0.0)),
                risk_level=str(data.get('risk_level', 'moderate')),
                daily_loss_limit_usd=float(data.get('daily_loss_limit_usd', 2000.0)),
                min_confidence_scale=float(data.get('min_confidence_scale', 0.3)),
                max_adv_pct=float(data.get('max_adv_pct', 0.02)),
                spread_penalty_threshold=float(data.get('spread_penalty_threshold', 0.005)),
                spread_penalty_factor=float(data.get('spread_penalty_factor', 0.5)),
            )
        except Exception:
            return PositionSizingConfig()

    def save(self, path: Optional[str] = None) -> None:
        """Save to YAML file."""
        filepath = Path(path) if path else _default_path()
        filepath.parent.mkdir(parents=True, exist_ok=True)
        content = (
            '# Position Sizing — hard limits and risk appetite.\n'
            '# Edit this file or use the `session` CLI command.\n\n'
        )
        content += yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)
        with open(filepath, 'w') as f:
            f.write(content)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PortfolioState:
    """Snapshot of current portfolio for sizing decisions."""
    net_liquidation: float = 0.0
    gross_position_value: float = 0.0
    available_funds: float = 0.0
    daily_pnl: float = 0.0
    position_count: int = 0
    pending_proposal_value: float = 0.0   # sum of PENDING proposal amounts

    @property
    def current_exposure_pct(self) -> float:
        if self.net_liquidation <= 0:
            return 0.0
        return self.gross_position_value / self.net_liquidation

    @property
    def remaining_capacity_usd(self) -> float:
        if self.net_liquidation <= 0:
            return 0.0
        max_exposure = self.net_liquidation * 0.80  # uses default; overridden by sizer
        return max(0.0, max_exposure - self.gross_position_value - self.pending_proposal_value)


@dataclass
class LiquidityInfo:
    """Market microstructure snapshot for liquidity-aware sizing."""
    avg_daily_volume: float = 0.0      # shares/day (e.g. 20-day ADV)
    bid: float = 0.0
    ask: float = 0.0
    last: float = 0.0
    bid_size: float = 0.0              # top-of-book bid size (shares)
    ask_size: float = 0.0              # top-of-book ask size (shares)

    @property
    def spread(self) -> float:
        """Absolute bid-ask spread in dollars."""
        if self.bid > 0 and self.ask > 0 and self.ask >= self.bid:
            return self.ask - self.bid
        return 0.0

    @property
    def spread_pct(self) -> float:
        """Spread as a fraction of mid price."""
        mid = self.mid_price
        if mid > 0:
            return self.spread / mid
        return 0.0

    @property
    def mid_price(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2
        return self.last or 0.0

    @property
    def has_data(self) -> bool:
        """True if we have enough data for liquidity checks."""
        return self.avg_daily_volume > 0 or (self.bid > 0 and self.ask > 0)


@dataclass
class SizingResult:
    """Output of the sizer — includes the amount and the reasoning."""
    amount_usd: float
    quantity: int = 0                     # filled in if price is known
    reasoning: str = ''                   # human/LLM-readable explanation
    warnings: list[str] = field(default_factory=list)
    capped_by: str = ''                   # which limit capped it, if any


class PositionSizer:
    """Stateless sizing algorithm. Takes config + state -> SizingResult."""

    def __init__(self, config: PositionSizingConfig):
        self.config = config

    def compute(
        self,
        confidence: float = 0.0,
        portfolio_state: Optional[PortfolioState] = None,
        price: float = 0.0,
        liquidity: Optional[LiquidityInfo] = None,
    ) -> SizingResult:
        """Compute position size.

        1. Start with base_position_usd * risk_multiplier
        2. Scale by confidence (min_confidence_scale -> 1.0)
        3. Clamp to [min_position_usd, max_position_usd]
        4. Check portfolio constraints (max_position_pct, max_total_exposure_pct, max_positions)
        5. Check daily loss limit
        6. Apply liquidity constraints (ADV cap, spread penalty)
        7. Return result with reasoning
        """
        cfg = self.config
        state = portfolio_state or PortfolioState()
        warnings: list[str] = []
        capped_by = ''

        # 1. Base * risk multiplier
        risk_mult = RISK_MULTIPLIERS.get(cfg.risk_level, 1.0)
        if cfg.base_position_pct > 0 and state.net_liquidation > 0:
            base = state.net_liquidation * cfg.base_position_pct
        else:
            base = cfg.base_position_usd
        raw = base * risk_mult

        # 2. Scale by confidence
        confidence = max(0.0, min(1.0, confidence))
        scale = cfg.min_confidence_scale + (1.0 - cfg.min_confidence_scale) * confidence
        sized = raw * scale

        if cfg.base_position_pct > 0 and state.net_liquidation > 0:
            parts = [f'base ${base:,.0f} ({cfg.base_position_pct:.1%} of ${state.net_liquidation:,.0f})']
        else:
            parts = [f'base ${base:,.0f}']
        if risk_mult != 1.0:
            parts.append(f'risk {cfg.risk_level} ({risk_mult}x)')
        parts.append(f'confidence {confidence:.1f} (scale {scale:.2f})')
        parts.append(f'= ${sized:,.0f}')

        # 3. Clamp to hard limits
        if sized < cfg.min_position_usd:
            sized = cfg.min_position_usd
            capped_by = 'min_position_usd'
            parts.append(f'clamped to min ${cfg.min_position_usd:,.0f}')
        elif sized > cfg.max_position_usd:
            sized = cfg.max_position_usd
            capped_by = 'max_position_usd'
            parts.append(f'clamped to max ${cfg.max_position_usd:,.0f}')

        # 4. Portfolio constraints (only if we have portfolio data)
        if state.net_liquidation > 0:
            # Max % of net liq per position
            max_by_pct = state.net_liquidation * cfg.max_position_pct
            if sized > max_by_pct:
                sized = max_by_pct
                capped_by = 'max_position_pct'
                parts.append(f'capped by {cfg.max_position_pct:.0%} of net liq (${max_by_pct:,.0f})')

            # Max total exposure
            current_total = state.gross_position_value + state.pending_proposal_value
            max_total = state.net_liquidation * cfg.max_total_exposure_pct
            remaining = max(0.0, max_total - current_total)
            if sized > remaining:
                sized = max(0.0, remaining)
                capped_by = 'max_total_exposure_pct'
                parts.append(f'capped by exposure limit (${remaining:,.0f} remaining)')

            exposure_pct = state.current_exposure_pct
            if exposure_pct >= cfg.max_total_exposure_pct * 0.9:
                warnings.append(
                    f'Exposure at {exposure_pct:.0%} '
                    f'(limit {cfg.max_total_exposure_pct:.0%})'
                )

        # Max positions
        if state.position_count >= cfg.max_positions:
            sized = 0.0
            capped_by = 'max_positions'
            warnings.append(
                f'At position limit ({state.position_count}/{cfg.max_positions})'
            )

        # 5. Daily loss limit
        if state.daily_pnl < 0 and abs(state.daily_pnl) >= cfg.daily_loss_limit_usd:
            sized = 0.0
            capped_by = 'daily_loss_limit'
            warnings.append(
                f'Daily loss ${abs(state.daily_pnl):,.0f} '
                f'exceeds limit ${cfg.daily_loss_limit_usd:,.0f}'
            )
        elif state.daily_pnl < 0 and abs(state.daily_pnl) >= cfg.daily_loss_limit_usd * 0.8:
            warnings.append(
                f'Daily loss ${abs(state.daily_pnl):,.0f} '
                f'approaching limit ${cfg.daily_loss_limit_usd:,.0f}'
            )

        # 6. Liquidity constraints
        liq = liquidity
        # Use liquidity mid/last as effective price if price not explicitly provided
        effective_price = price
        if effective_price <= 0 and liq and liq.mid_price > 0:
            effective_price = liq.mid_price

        if liq and liq.has_data and sized > 0:
            # ADV cap — don't take more than max_adv_pct of average daily volume
            if liq.avg_daily_volume > 0 and effective_price > 0:
                max_shares_by_adv = liq.avg_daily_volume * cfg.max_adv_pct
                max_by_adv = max_shares_by_adv * effective_price
                if sized > max_by_adv:
                    sized = max_by_adv
                    capped_by = 'adv_liquidity'
                    parts.append(
                        f'capped by {cfg.max_adv_pct:.1%} of ADV '
                        f'({liq.avg_daily_volume:,.0f} shares, '
                        f'max {max_shares_by_adv:,.0f} shares = ${max_by_adv:,.0f})'
                    )
                elif liq.avg_daily_volume > 0 and effective_price > 0:
                    position_shares = sized / effective_price
                    adv_pct = position_shares / liq.avg_daily_volume
                    if adv_pct > cfg.max_adv_pct * 0.5:
                        warnings.append(
                            f'Position is {adv_pct:.1%} of ADV '
                            f'(limit {cfg.max_adv_pct:.1%})'
                        )

            # Spread penalty — wide spreads = illiquidity, reduce size proportionally
            if liq.spread_pct > 0 and liq.spread_pct > cfg.spread_penalty_threshold:
                # Linear penalty: at threshold spread = full penalty factor reduction
                # spread_ratio of 1.0 = at threshold, 2.0 = 2x threshold, etc.
                spread_ratio = liq.spread_pct / cfg.spread_penalty_threshold
                # Penalty scales from 0 at threshold to spread_penalty_factor at 2x threshold+
                penalty = min(cfg.spread_penalty_factor, cfg.spread_penalty_factor * (spread_ratio - 1.0))
                reduction = 1.0 - penalty
                before = sized
                sized = sized * reduction
                if capped_by == '':
                    capped_by = 'spread_penalty'
                parts.append(
                    f'spread penalty {penalty:.0%} '
                    f'(spread {liq.spread_pct:.2%}, threshold {cfg.spread_penalty_threshold:.2%})'
                )
                warnings.append(
                    f'Wide spread: {liq.spread_pct:.2%} '
                    f'(${liq.spread:.2f}, bid {liq.bid:.2f} / ask {liq.ask:.2f})'
                )

            # Book depth warning — if sized shares > top-of-book, you'll walk the book
            if effective_price > 0 and sized > 0:
                sized_shares = sized / effective_price
                book_size = liq.bid_size if liq.bid_size > 0 else liq.ask_size
                if book_size > 0 and sized_shares > book_size:
                    warnings.append(
                        f'Order ({sized_shares:,.0f} shares) exceeds '
                        f'top-of-book depth ({book_size:,.0f} shares) — expect slippage'
                    )

        # Ensure minimum if still positive
        if 0 < sized < cfg.min_position_usd:
            sized = 0.0
            capped_by = 'below_minimum_after_constraints'
            warnings.append(
                f'Sized amount below minimum ${cfg.min_position_usd:,.0f} after constraints'
            )

        # Compute quantity if price known (use effective_price which may come from liquidity)
        quantity = 0
        qty_price = effective_price if effective_price > 0 else price
        if qty_price > 0 and sized > 0:
            quantity = int(sized / qty_price)
            if quantity == 0 and sized >= cfg.min_position_usd:
                warnings.append(f'Price ${qty_price:,.2f} too high for sized amount ${sized:,.0f}')

        reasoning = '; '.join(parts)

        return SizingResult(
            amount_usd=round(sized, 2),
            quantity=quantity,
            reasoning=reasoning,
            warnings=warnings,
            capped_by=capped_by,
        )

    def session_summary(self, portfolio_state: Optional[PortfolioState] = None) -> dict:
        """Returns a dict suitable for LLM consumption."""
        cfg = self.config
        state = portfolio_state or PortfolioState()

        # Compute recommended sizes at different confidence levels
        high = self.compute(confidence=0.9, portfolio_state=state)
        medium = self.compute(confidence=0.5, portfolio_state=state)
        low = self.compute(confidence=0.2, portfolio_state=state)

        # Capacity
        remaining_usd = 0.0
        remaining_positions = cfg.max_positions - state.position_count
        if state.net_liquidation > 0:
            current_total = state.gross_position_value + state.pending_proposal_value
            max_total = state.net_liquidation * cfg.max_total_exposure_pct
            remaining_usd = max(0.0, max_total - current_total)

        # Collect all warnings
        all_warnings = list(set(high.warnings + medium.warnings + low.warnings))

        # Effective base: pct-derived if active, otherwise fixed
        if cfg.base_position_pct > 0 and state.net_liquidation > 0:
            effective_base = state.net_liquidation * cfg.base_position_pct
        else:
            effective_base = cfg.base_position_usd

        return {
            'config': {
                'min_position_usd': cfg.min_position_usd,
                'max_position_usd': cfg.max_position_usd,
                'max_position_pct': cfg.max_position_pct,
                'max_total_exposure_pct': cfg.max_total_exposure_pct,
                'max_positions': cfg.max_positions,
                'base_position_usd': cfg.base_position_usd,
                'base_position_pct': cfg.base_position_pct,
                'effective_base_usd': effective_base,
                'risk_level': cfg.risk_level,
                'daily_loss_limit_usd': cfg.daily_loss_limit_usd,
                'min_confidence_scale': cfg.min_confidence_scale,
                'max_adv_pct': cfg.max_adv_pct,
                'spread_penalty_threshold': cfg.spread_penalty_threshold,
                'spread_penalty_factor': cfg.spread_penalty_factor,
            },
            'portfolio': {
                'net_liquidation': state.net_liquidation,
                'gross_position_value': state.gross_position_value,
                'available_funds': state.available_funds,
                'daily_pnl': state.daily_pnl,
                'position_count': state.position_count,
                'pending_proposal_value': state.pending_proposal_value,
                'exposure_pct': round(state.current_exposure_pct, 4),
            },
            'capacity': {
                'remaining_usd': round(remaining_usd, 2),
                'remaining_positions': max(0, remaining_positions),
            },
            'recommended_sizes': {
                'high_confidence': {
                    'amount_usd': high.amount_usd,
                    'reasoning': high.reasoning,
                    'capped_by': high.capped_by,
                },
                'medium_confidence': {
                    'amount_usd': medium.amount_usd,
                    'reasoning': medium.reasoning,
                    'capped_by': medium.capped_by,
                },
                'low_confidence': {
                    'amount_usd': low.amount_usd,
                    'reasoning': low.reasoning,
                    'capped_by': low.capped_by,
                },
            },
            'warnings': all_warnings,
        }
