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

import deal


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
