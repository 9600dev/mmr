"""Pure order-quantity arithmetic — the single amount→shares conversion.

Every path that turns a dollar notional into a share count must use
``whole_shares_for_notional``. Ad-hoc ``round(amount / price)`` plus a
bump-to-1 turned a ~$340 auto-sized amount on a >$510 stock into a full
share at inflated notional; the only safe conversion is floor-and-refuse.

Design-by-Contract: the flagship guarantee — a returned quantity's notional
never exceeds ``amount``, and refusal (ValueError) rather than a bump-to-1 —
is encoded as ``deal`` contracts so it is enforced at runtime on every call
and symbolically checked by CrossHair (see ``scripts/crosshair_check.py``).

The finite-&-positive input requirement is expressed as a real ``@deal.pre``
on the pure arithmetic core ``_floor_shares_for_notional`` (which CrossHair
verifies under that precondition). The public ``whole_shares_for_notional``
deliberately keeps NO fatal precondition: it is defensively total — it
validates its own inputs and raises ``ValueError`` with the offending
parameter named (the "fail loudly" contract pinned by
``tests/invariants/test_order_notional.py`` and ``tests/test_order_math.py``).
A fatal ``@deal.pre`` there would turn those documented ``ValueError``\\s into
``PreContractError``\\s and change observable behaviour.
"""

import math
import sys

import deal

# ib_async represents an unset numeric order field as sys.float_info.max
# (its UNSET_DOUBLE). Named here rather than imported so this module stays
# free of broker dependencies and CrossHair-checkable.
_UNSET_SENTINEL = sys.float_info.max


def _all_finite_positive(*values: float) -> bool:
    """True iff every value is a finite real number strictly greater than zero.

    Non-numeric inputs (str, None) are treated as invalid rather than raising,
    so this can be used both as a plain guard and inside a ``deal`` predicate.
    """
    for value in values:
        try:
            if not (math.isfinite(value) and value > 0):
                return False
        except TypeError:
            return False
    return True


@deal.has()  # side-effect free (no I/O, no global mutation)
@deal.pure
@deal.ensure(
    lambda _: _.result >= 0.0,
    message='a reducible quantity is never negative')
@deal.ensure(
    lambda _: (_.result == 0.0
               or (_all_finite_positive(_.broker_qty) and _.result <= _.broker_qty)),
    message='reducible quantity would OVERSELL the live broker position')
@deal.ensure(
    lambda _: (_.result == 0.0
               or (_all_finite_positive(_.attributed_qty) and _.result <= _.attributed_qty)),
    message='reducible quantity exceeds what this strategy is attributed')
def reducible_quantity(attributed_qty: float, broker_qty: float) -> float:
    """How much of an attributed long position may be reduced right now.

    ``0.0`` means "none, or not knowable" — the caller must not place a
    reducing order. Both bounds are load-bearing and for different reasons:
    exceeding the BROKER quantity opens a short, and exceeding the ATTRIBUTED
    quantity closes shares this strategy has no claim on (a manual position in
    the same instrument).

    This exists because ``min(attributed, broker)`` — written inline at both
    call sites — is NOT the clamp it looks like. ``min(140.0, float('nan'))``
    returns 140.0, and ``nan <= 0`` is False, so a NaN broker read sailed
    through both the clamp and the guard after it and would have sold the full
    attributed size against an unknown position. Every non-number, NaN, inf,
    zero or negative input is 0.0 here.
    """
    if not _all_finite_positive(attributed_qty, broker_qty):
        return 0.0
    return min(float(attributed_qty), float(broker_qty))


@deal.has()  # side-effect free (no I/O, no global mutation)
@deal.raises(ValueError)
@deal.pre(lambda amount, price, multiplier=1.0: _all_finite_positive(amount, price, multiplier))
@deal.ensure(lambda _: _.result >= 1 and _.result * _.price * _.multiplier <= _.amount)
def _floor_shares_for_notional(amount: float, price: float, multiplier: float = 1.0) -> int:
    """Pure share-count core. Precondition: ``amount``, ``price``, ``multiplier``
    are all finite and > 0.

    Postcondition: ``result >= 1 and result * price * multiplier <= amount``.
    Raises ``ValueError`` when the amount doesn't cover a single whole share —
    never rounds up (fractional shares are not enabled on this path, and
    bumping to 1 exceeds the sized notional).
    """
    denom = price * multiplier
    # Each of price/multiplier is finite & > 0, but their product can still
    # underflow to 0.0 (e.g. 3e-154 * 3e-308) or overflow to inf (e.g.
    # 1e300 * 1e300). Dividing by 0.0 raises ZeroDivisionError and math.floor
    # of a non-finite ratio raises OverflowError — both undeclared. A degenerate
    # per-share cost has no meaningful whole-share count: refuse loudly.
    if not math.isfinite(denom) or denom <= 0:
        raise ValueError(
            f'price {price} x multiplier {multiplier} is degenerate ({denom!r}) '
            f'— cannot compute a whole-share count for amount {amount}'
        )
    ratio = amount / denom
    if not math.isfinite(ratio):
        raise ValueError(
            f'amount {amount} over per-share cost {denom} overflows the '
            f'share-count computation — inputs are degenerate'
        )
    shares = math.floor(ratio)
    # Float division can round up across an integer boundary (e.g. 8.28 / 2.76
    # -> 3.0000000000000004); step down until the postcondition holds.
    while shares >= 1 and shares * price * multiplier > amount:
        shares -= 1
    if shares < 1:
        raise ValueError(
            f'amount {amount} does not cover one whole share at price {price}'
            f'{f" (multiplier {multiplier})" if multiplier != 1.0 else ""}'
            f' — fractional shares are not enabled on this path'
        )
    return shares


@deal.has()  # side-effect free (no I/O, no global mutation)
@deal.raises(ValueError)
@deal.ensure(lambda _: _.result >= 1 and _.result * _.price * _.multiplier <= _.amount)
def whole_shares_for_notional(amount: float, price: float, multiplier: float = 1.0) -> int:
    """Whole shares (or contracts) purchasable with ``amount`` at ``price``.

    Postcondition: result >= 1 and result * price * multiplier <= amount.

    Raises ValueError when any input is not a finite number > 0, or when the
    amount doesn't cover a single share — never rounds up: fractional shares
    are not enabled on this path, and bumping to 1 exceeds the sized notional.
    """
    for name, value in (('amount', amount), ('price', price), ('multiplier', multiplier)):
        try:
            valid = math.isfinite(value) and value > 0
        except TypeError:
            valid = False
        if not valid:
            raise ValueError(f'{name} must be a finite number > 0, got {value!r}')

    return _floor_shares_for_notional(amount, price, multiplier)


@deal.has()  # side-effect free (no I/O, no global mutation)
@deal.pure
@deal.ensure(
    lambda _: _.result[0] >= 0.0,
    message='a notional cannot be negative')
@deal.ensure(
    lambda _: _.result[1] or _.result[0] == 0.0,
    message='a non-evaluable notional must be reported as 0.0, never guessed')
def order_notional(
    price_candidates: tuple, quantity: float, multiplier: float = 1.0,
) -> tuple:
    """Value an order as ``(notional, evaluable)`` from the first usable price.

    ``price_candidates`` is tried in order; the first finite, strictly-positive
    one wins. Callers own the ORDER of that tuple, because the right price
    policy differs by purpose and must not be silently unified:

      * the approver tier anchors on a live snapshot and takes
        ``max(snapshot, client_limit)``, so a proposer can push the valuation UP
        but never DOWN, and refuses to value at all without a snapshot;
      * the audit trail wants best-effort valuation from whatever is already to
        hand, and records that it could not value rather than refusing to place.

    What IS shared is the arithmetic and the honesty about failure. Returning
    ``(0.0, False)`` rather than a zero notional is the whole point: a market
    order carries no usable limit price, and inventing a number for it is how a
    cumulative notional limit ends up meaningless. That was real: every
    ORDER_SUBMITTED event before 2026-07-27 recorded ``price=order.lmtPrice``,
    which for a MARKET or STOP order is ib_async's unset sentinel — verified in
    the live event store, where market submissions sit at
    1.7976931348623157e+308.

    Quantity is taken as ``abs`` — a notional is a size, not a direction.

    THE LARGEST FINITE DOUBLE IS A SENTINEL, NOT A PRICE. ib_async represents an
    unset numeric order field as ``sys.float_info.max`` (``UNSET_DOUBLE``), so a
    MARKET order's ``lmtPrice`` and a STOP order's ``lmtPrice`` both read
    1.7976931348623157e+308. That value is FINITE and POSITIVE, so the obvious
    guard — ``math.isfinite(price) and price > 0`` — waves it straight through,
    and at a quantity of 1.0 the product is still finite: a $1.8e308 notional
    reported as a valid valuation. One of those in a cumulative total exceeds
    every conceivable cap forever. Callers strip the sentinel at the boundary
    where they know it is ib_async's; this rule is here as well because the
    consequence is severe and the guard that misses it looks correct.
    """
    try:
        qty = abs(float(quantity))
        mult = float(multiplier)
    except (TypeError, ValueError):
        return (0.0, False)
    if not (math.isfinite(qty) and qty > 0):
        return (0.0, False)
    if not (math.isfinite(mult) and mult > 0):
        return (0.0, False)

    for candidate in price_candidates:
        try:
            price = float(candidate)
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(price) and price > 0):
            continue
        if price >= _UNSET_SENTINEL:
            continue      # an unset field, not a price — see the docstring
        value = qty * price * mult
        if math.isfinite(value):
            return (value, True)
        return (0.0, False)   # overflowed to inf — not a usable valuation
    return (0.0, False)
