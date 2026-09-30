"""Sleeve holdings, order sizing and the D15 refusals.

Pure where it can be: :func:`reconcile_holdings` and :func:`size_orders` take
plain data and return plain data; :func:`make_plan` is the only function here
that talks to the ports, and it does so read-only.

D15/D13 rules implemented here:
  * holdings = per-ticker sums of the ``sleeve_fills`` journal (D13), clamped to
    IB — a sleeve never owns shares IB does not show;
  * whole shares, ``floor(weight * capital / price)`` — exactly, never adjusted;
  * SELLs before BUYs; changes inside the no-trade band are skipped — exactly,
    never un-skipped;
  * refusals: stale targets (> 5 trading sessions old), weights > 1.0 (already
    refused by the loader), an unpriceable ticker, POST-EXECUTION gross exposure
    above the sleeve's capital (turnover within a batch is not capped). Expected
    cash after execution is capital minus that exposure, so it is never negative
    on a plan that is not refused. There is no repair mode: a plan is the
    targets' sizing or it is refused.
"""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Optional, Sequence

from trader.rebalance.ports import Gateway, Pricer, ProposalRecord
from trader.rebalance.targets import Targets
from trader.trading.order_math import whole_shares_for_notional

MAX_TARGET_AGE_SESSIONS = 5          # D15: targets older than 5 trading days are refused
DEFAULT_BAND_PCT = 0.25              # no-trade band as % of sleeve capital, per name
OPEN_STATUSES = frozenset({'PENDING', 'APPROVED'})   # in flight: not yet inventory, not yet dead
_CENTS = 0.005                       # float slack when comparing dollar sums


class Refusal(Exception):
    """A D15 refusal: the CLI exits non-zero and nothing is created."""

    def __init__(self, code: str, message: str):
        super().__init__(f'{code}: {message}')
        self.code = code
        self.message = message


@dataclass(frozen=True)
class PositionLimits:
    """The per-position cap the live risk gate applies at approval, reported (not enforced) by ``plan``.

    The gate refuses an opening order when the resulting position's value exceeds
    ``max_position_size_pct`` of NetLiquidation (``risk_limits:`` in trader.yaml).
    ``max_position_usd`` is an optional absolute cap for operators who want one.
    """

    max_position_size_pct: Optional[float] = None
    net_liquidation: Optional[float] = None
    max_position_usd: Optional[float] = None

    def unevaluable_reason(self) -> Optional[str]:
        if self.max_position_size_pct is not None and not (self.net_liquidation and self.net_liquidation > 0):
            return (f'position limit {self.max_position_size_pct:.0%} of net liquidation could not be '
                    f'evaluated: NetLiquidation is unreadable (the live gate refuses opens in that state)')
        return None

    def refusal_reason(self, position_value: float) -> Optional[str]:
        if (self.max_position_size_pct is not None and self.net_liquidation and self.net_liquidation > 0
                and position_value > self.max_position_size_pct * self.net_liquidation + _CENTS):
            return (f'position ${position_value:,.0f} = {position_value / self.net_liquidation:.1%} of net '
                    f'liquidation ${self.net_liquidation:,.0f} > max_position_size_pct '
                    f'{self.max_position_size_pct:.0%}')
        if self.max_position_usd is not None and position_value > self.max_position_usd + _CENTS:
            return f'position ${position_value:,.0f} > max_position_usd ${self.max_position_usd:,.0f}'
        return None


@dataclass(frozen=True)
class Order:
    symbol: str
    action: str                 # 'BUY' | 'SELL'
    quantity: int               # whole shares, > 0
    price: float
    held_before: int
    target_shares: int
    target_weight: float
    limit_flag: Optional[str] = None

    @property
    def notional(self) -> float:
        return self.quantity * self.price

    @property
    def shares_after(self) -> int:
        return self.held_before + (self.quantity if self.action == 'BUY' else -self.quantity)


@dataclass(frozen=True)
class Skipped:
    symbol: str
    delta: int
    notional: float
    reason: str


@dataclass(frozen=True)
class Plan:
    sleeve: str
    as_of: dt.date
    capital_usd: float
    holdings: Mapping[str, int]                 # reconciled, before execution
    prices: Mapping[str, float]
    orders: Sequence[Order]                     # SELLs first, then BUYs
    skipped: Sequence[Skipped]
    cash_before: float
    cash_after: float
    turnover_notional: float                    # sum of |order notional| (informational; D15 does not cap it)
    gross_exposure_after: float                 # sum of resulting position values at plan prices (D15 cap)
    warnings: Sequence[str] = field(default_factory=tuple)
    open_proposals: Sequence[ProposalRecord] = field(default_factory=tuple)

    @property
    def sells(self) -> list[Order]:
        return [o for o in self.orders if o.action == 'SELL']

    @property
    def buys(self) -> list[Order]:
        return [o for o in self.orders if o.action == 'BUY']

    @property
    def limit_flags(self) -> list[Order]:
        return [o for o in self.orders if o.limit_flag]

    def holdings_after(self) -> dict[str, int]:
        after = dict(self.holdings)
        for o in self.orders:
            after[o.symbol] = o.shares_after
        return {s: q for s, q in after.items() if q != 0}


@dataclass(frozen=True)
class SleeveState:
    holdings: Mapping[str, int]
    warnings: Sequence[str]
    open_proposals: Sequence[ProposalRecord]
    ib_positions: Mapping[str, float]
    journal: Mapping[str, float]


# ---------------------------------------------------------------------------
# Freshness (D15: targets older than 5 trading days are refused)
# ---------------------------------------------------------------------------

def sessions_after(as_of: dt.date, today: dt.date, calendar: str = 'XNYS') -> int:
    """Number of exchange sessions strictly after ``as_of`` and on or before ``today``."""
    if today <= as_of:
        return 0
    import exchange_calendars as xcals
    cal = xcals.get_calendar(calendar, start=str(as_of - dt.timedelta(days=7)),
                             end=str(today + dt.timedelta(days=7)))
    sessions = cal.sessions_in_range(as_of.isoformat(), today.isoformat())
    return int(sum(1 for s in sessions if s.date() > as_of))


def check_freshness(targets: Targets, today: dt.date) -> int:
    """Return the age in sessions; raise ``Refusal`` when stale or from the future."""
    if targets.as_of > today:
        raise Refusal('FUTURE_TARGETS', f'as_of {targets.as_of} is after today {today}')
    age = sessions_after(targets.as_of, today)
    if age > MAX_TARGET_AGE_SESSIONS:
        raise Refusal('STALE_TARGETS',
                      f'targets as_of {targets.as_of} are {age} trading sessions old '
                      f'(limit {MAX_TARGET_AGE_SESSIONS}); regenerate them')
    return age


# ---------------------------------------------------------------------------
# Holdings (D13 journal sums, D15 reconciliation with IB)
# ---------------------------------------------------------------------------

def reconcile_holdings(journal: Mapping[str, float],
                       ib_positions: Mapping[str, float]) -> tuple[dict[str, int], list[str]]:
    """Sleeve holdings = min(journal sum, IB) per symbol, whole shares, never negative.

    IB positions the journal does not know about are outside the sleeve and are
    left alone. A journal quantity IB does not (fully) show is clamped to what IB
    shows and reported — the sleeve may never assume shares IB does not have.
    """
    ib = {str(k).upper(): float(v) for k, v in ib_positions.items()}
    holdings: dict[str, int] = {}
    warnings: list[str] = []
    for sym_raw, journal_qty in sorted(journal.items()):
        sym = str(sym_raw).upper()
        journal_qty = float(journal_qty)
        if not math.isfinite(journal_qty):
            # A NaN sum used to fall through to math.floor and crash with "cannot convert float NaN
            # to integer" on every later plan (2026-09-29, bug B). Name the symbol; the caller refuses.
            raise ValueError(f'{sym}: journal quantity {journal_qty!r} is not a finite number — '
                             f'the sleeve inventory cannot be established')
        if journal_qty <= 0:
            if journal_qty < 0:
                warnings.append(f'{sym}: journal is net short {journal_qty:g} — treated as flat')
            continue
        broker_qty = ib.get(sym, 0.0)
        if not math.isfinite(broker_qty) or broker_qty <= 0:
            warnings.append(f'{sym}: journal holds {journal_qty:g} but IB shows none — treated as flat')
            continue
        held = int(math.floor(min(journal_qty, broker_qty)))
        if broker_qty + 1e-9 < journal_qty:
            warnings.append(f'{sym}: journal holds {journal_qty:g}, IB shows {broker_qty:g} — using {held}')
        if held > 0:
            holdings[sym] = held
    return holdings, warnings


def open_proposals(records: Iterable[ProposalRecord]) -> list[ProposalRecord]:
    return sorted((r for r in records if r.status in OPEN_STATUSES), key=lambda r: r.id)


# ---------------------------------------------------------------------------
# Sizing (pure)
# ---------------------------------------------------------------------------

def target_shares(weight: float, capital: float, price: float) -> int:
    """``floor(weight * capital / price)`` through the contracted kernel; 0 below one share."""
    try:
        return whole_shares_for_notional(weight * capital, price)
    except ValueError:
        return 0


def size_orders(targets: Targets, holdings: Mapping[str, int], prices: Mapping[str, Optional[float]], *,
                band_pct: float = DEFAULT_BAND_PCT, limits: PositionLimits | None = None,
                sells_only: bool = False, warnings: Sequence[str] = (),
                open_proposals: Sequence[ProposalRecord] = (),
                ib_positions: Mapping[str, float] | None = None) -> Plan:
    """Turn targets + reconciled holdings + prices into a :class:`Plan` (D15 sizing).

    Every order is exactly ``floor(weight * capital / price) - held``; nothing
    is trimmed, rounded up, or un-skipped to make a plan fit. Raises ``Refusal``
    for an unpriceable ticker and for a post-execution gross exposure above
    capital (``GROSS_EXPOSURE_EXCEEDS_CAPITAL``) — the only way a compliant plan
    can overhang capital is through names left inside the no-trade band, so the
    remedy is a smaller ``band_pct`` or targets that leave cash. ``ib_positions``
    (the account's total per symbol) lets the position-limit flag value the whole
    resulting position, outside shares included, the way the live gate does.
    """
    if not (math.isfinite(band_pct) and band_pct >= 0):
        raise Refusal('BAD_BAND', f'no-trade band must be a finite percentage >= 0, got {band_pct!r}')
    limits = limits or PositionLimits()
    capital = targets.capital_usd
    universe = sorted(set(targets.weights) | set(holdings))

    bad = [t for t in universe if not _priced(prices.get(t))]
    if bad:
        raise Refusal('UNPRICEABLE', f'no usable price for {", ".join(bad)} — nothing sized')
    px = {t: float(prices[t] or 0.0) for t in universe}

    wanted = {t: target_shares(targets.weights.get(t, 0.0), capital, px[t]) for t in universe}
    band_usd = band_pct / 100.0 * capital
    deltas: dict[str, int] = {}
    skipped: list[Skipped] = []
    for t in universe:
        delta = wanted[t] - holdings.get(t, 0)
        if delta == 0:
            continue
        notional = abs(delta) * px[t]
        leaving = t not in targets.weights          # a name that left the targets always exits fully
        if notional < band_usd and not leaving:
            skipped.append(Skipped(t, delta, notional, f'inside no-trade band ${band_usd:,.2f}'))
            continue
        if sells_only and delta > 0:
            skipped.append(Skipped(t, delta, notional, 'sells-only run'))
            continue
        deltas[t] = delta

    ib = {str(k).upper(): float(v) for k, v in (ib_positions or {}).items()}
    orders: list[Order] = []
    for t, delta in deltas.items():
        action = 'BUY' if delta > 0 else 'SELL'
        held = holdings.get(t, 0)
        outside = max(ib.get(t, float(held)) - held, 0.0)      # account shares the sleeve does not own
        account_after_value = (outside + held + delta) * px[t]
        orders.append(Order(symbol=t, action=action, quantity=abs(delta), price=px[t],
                            held_before=held, target_shares=wanted[t],
                            target_weight=targets.weights.get(t, 0.0),
                            limit_flag=limits.refusal_reason(account_after_value) if delta > 0 else None))
    sells = sorted((o for o in orders if o.action == 'SELL'), key=lambda o: (-o.notional, o.symbol))
    buys = sorted((o for o in orders if o.action == 'BUY'), key=lambda o: (-o.notional, o.symbol))
    ordered = sells + buys

    turnover = sum(o.notional for o in ordered)
    value_before = sum(holdings.get(t, 0) * px[t] for t in universe)
    exposure_after = sum((holdings.get(t, 0) + deltas.get(t, 0)) * px[t] for t in universe)
    # D15: the cap is on the sleeve's resulting gross exposure, not on turnover. No repair:
    # the plan is the targets' sizing or it is refused, and the message names the remedy.
    if exposure_after > capital + _CENTS:
        banded = [s.symbol for s in skipped if s.delta < 0]
        raise Refusal('GROSS_EXPOSURE_EXCEEDS_CAPITAL',
                      f'post-execution gross exposure ${exposure_after:,.2f} exceeds sleeve capital '
                      f'${capital:,.2f} (D15); nothing sized. '
                      + (f'Names inside the no-trade band hold more than target: {", ".join(banded)} — '
                         f'lower --band-pct (currently {band_pct:g}%) so they trade to target, '
                         if banded else '')
                      + 'or regenerate targets that leave cash')
    cash_after = capital - exposure_after
    all_warnings = list(warnings)
    unevaluable = limits.unevaluable_reason()
    if unevaluable and buys:
        all_warnings.append(unevaluable)
    return Plan(
        sleeve=targets.sleeve, as_of=targets.as_of, capital_usd=capital,
        holdings=dict(holdings), prices=px, orders=tuple(ordered), skipped=tuple(skipped),
        cash_before=capital - value_before, cash_after=cash_after,
        turnover_notional=turnover, gross_exposure_after=exposure_after,
        warnings=tuple(all_warnings), open_proposals=tuple(open_proposals),
    )


def _priced(value: Optional[float]) -> bool:
    if value is None or isinstance(value, bool):
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0


# ---------------------------------------------------------------------------
# Orchestration through the ports (read-only)
# ---------------------------------------------------------------------------

def fetch_prices(pricer: Pricer, tickers: Iterable[str]) -> dict[str, Optional[float]]:
    out: dict[str, Optional[float]] = {}
    for t in sorted(set(tickers)):
        try:
            out[t] = pricer.price(t)
        except Exception as exc:  # noqa: BLE001 - an exception is "cannot price", reported per ticker
            out[t] = None
            _log().warning('rebalance: pricing %s failed: %s', t, exc)
    return out


def sleeve_state(sleeve: str, gateway: Gateway) -> SleeveState:
    """Holdings (journal clamped to IB), reconciliation warnings, in-flight proposals, IB book."""
    from trader.rebalance.targets import sleeve_group
    records = list(gateway.proposals_in_group(sleeve_group(sleeve)))
    try:
        journal = dict(gateway.sleeve_holdings(sleeve))
    except Exception as exc:
        raise Refusal('JOURNAL_UNREADABLE', f'cannot read the sleeve_fills journal: {exc}') from exc
    try:
        ib_positions = dict(gateway.stock_positions())
    except Exception as exc:
        raise Refusal('IB_UNREADABLE', f'cannot read IB positions to reconcile the sleeve: {exc}') from exc
    try:
        holdings, warnings = reconcile_holdings(journal, ib_positions)
    except ValueError as exc:
        raise Refusal('JOURNAL_UNREADABLE', f'the sleeve_fills journal cannot be reconciled: {exc}') from exc
    return SleeveState(holdings=holdings, warnings=tuple(warnings), open_proposals=tuple(open_proposals(records)),
                       ib_positions=ib_positions, journal=journal)


def make_plan(targets: Targets, gateway: Gateway, *, today: dt.date | None = None,
              band_pct: float = DEFAULT_BAND_PCT, limits: PositionLimits | None = None,
              sells_only: bool = False) -> Plan:
    """The dry run: freshness, holdings, prices, sizing. Read-only against the ports."""
    check_freshness(targets, today or dt.date.today())
    state = sleeve_state(targets.sleeve, gateway)
    prices = fetch_prices(gateway, set(targets.weights) | set(state.holdings))
    return size_orders(targets, state.holdings, prices, band_pct=band_pct, limits=limits,
                       sells_only=sells_only, warnings=state.warnings, open_proposals=state.open_proposals,
                       ib_positions=state.ib_positions)


def _log():
    from trader.common.logging_helper import setup_logging
    return setup_logging(module_name='rebalance')
