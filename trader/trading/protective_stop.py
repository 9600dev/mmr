"""Pure protective-stop plan — size and price for the broker-side disaster stop.

WHY THIS MODULE EXISTS
    Every attributed auto-executed open gets a GTC STP SELL some percentage
    below entry. It is not trade management — the strategy's own exits fire long
    before it. It exists because it is the ONLY protection that survives a dead
    feed or a dead strategy_service while a position is held: the stale-bar gate
    guards opens, nothing else guards a position already on the book.

    So the arithmetic that sizes and prices it is load-bearing in exactly the
    situation where nothing else is watching, and it lived inline in
    ``AutoExecutor._ensure_protective`` between two SDK calls — unreachable by
    contracts, by CrossHair, and by any test that did not stand up a fake broker.
    Every existing test used a $100 entry and a healthy position, so the whole
    degenerate region was unexplored.

TWO THINGS IT MUST NEVER DO
    * OVERSELL. The stop covers ``min(attributed, broker)`` — never more than
      the position actually at the broker, and never more than this strategy is
      attributed. Exit-class orders are exempt from every gate by design, so a
      too-large protective SELL would be placed without argument and, if it
      fired, would open a SHORT out of a protective mechanism.
    * SIT AT OR ABOVE ENTRY. A stop that is not strictly below the entry price
      is not protection, it is an immediate market exit — and the code this
      replaces could produce one. ``round(avg_cost * 0.92, 2)`` at an entry of
      $0.05 returns $0.05: the stop equals the entry and fires at once. Any
      entry under about 6 cents rounds UP into that state at the default 8%.
      Pinned in tests/invariants/test_protective_stop.py.

    Hence: floor rather than round (never step TOWARD entry), and refuse
    outright when no representable price is strictly below entry. Refusing is
    honest — the caller retries on the next bar and logs — whereas placing a
    stop at entry silently converts the disaster stop into the disaster.
"""

import math

from typing import NamedTuple, Optional

import deal

from trader.trading.order_math import reducible_quantity


class ProtectiveStop(NamedTuple):
    quantity: float
    stop_price: float


def _as_float(x) -> Optional[float]:
    """Coerce to a finite float, or None. Everything here arrives from a
    broker DataFrame or an env var, so 'not a number' is a real input."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


@deal.has()  # side-effect free: no I/O, no global mutation
@deal.pure
@deal.ensure(
    lambda _: _.result is None or _.result.quantity > 0,
    message='planned a protective stop for a non-positive quantity')
@deal.ensure(
    lambda _: (_.result is None
               or (_as_float(_.broker_qty) is not None
                   and _.result.quantity <= _as_float(_.broker_qty))),
    message='protective stop would OVERSELL the live broker position')
@deal.ensure(
    lambda _: (_.result is None
               or (_as_float(_.attributed_qty) is not None
                   and _.result.quantity <= _as_float(_.attributed_qty))),
    message='protective stop exceeds the quantity attributed to this strategy')
@deal.ensure(
    lambda _: (_.result is None
               or (_as_float(_.avg_cost) is not None
                   and 0.0 < _.result.stop_price < _as_float(_.avg_cost))),
    message='protective stop is not strictly below entry — that is an '
            'immediate market exit, not protection')
def protective_stop_plan(
    attributed_qty: float,
    broker_qty: float,
    avg_cost: float,
    stop_pct: float,
) -> Optional[ProtectiveStop]:
    """Size and price the disaster stop, or None if one must not be placed.

    None means "do not place, try again later" for every reason: the feature is
    disabled (``stop_pct <= 0``), an input was unreadable, the position is not
    visible at the broker yet, or no price strictly below entry is expressible.
    The caller self-heals on the next bar, so None is never terminal.
    """
    pct = _as_float(stop_pct)
    if pct is None or pct <= 0:
        return None                      # disabled, or malformed → disabled

    cost = _as_float(avg_cost)
    if cost is None or cost <= 0:
        return None                      # no entry price to measure down from

    # One definition of the never-oversell clamp, shared with the close path.
    quantity = reducible_quantity(attributed_qty, broker_qty)
    if quantity <= 0:
        return None                      # nothing (or nothing of ours) to cover

    target = cost * (1.0 - pct / 100.0)
    if not math.isfinite(target) or not math.isfinite(target * 100.0):
        return None
    # FLOOR, not round: stepping toward entry is the one direction that can
    # destroy the stop's meaning, and rounding does exactly that at low prices.
    cents = math.floor(target * 100.0)
    # ...and then step down if the float boundary put the floor ABOVE target
    # anyway. `target * 100.0` can round up onto an exact integer — at an entry
    # of 99999.99999999999 and 1.00001% it lands on 9899999.0, one cent nearer
    # entry than asked for. Same step-down idiom as
    # order_math._floor_shares_for_notional, and for the same reason.
    while cents > 0 and cents / 100.0 > target:
        cents -= 1
    stop_price = cents / 100.0
    if not (0.0 < stop_price < cost):
        return None                      # no representable stop below entry

    return ProtectiveStop(quantity=quantity, stop_price=stop_price)
