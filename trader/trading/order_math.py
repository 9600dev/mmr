"""Pure order-quantity arithmetic — the single amount→shares conversion.

Every path that turns a dollar notional into a share count must use
``whole_shares_for_notional``. Ad-hoc ``round(amount / price)`` plus a
bump-to-1 turned a ~$340 auto-sized amount on a >$510 stock into a full
share at inflated notional; the only safe conversion is floor-and-refuse.
"""

import math


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

    shares = math.floor(amount / (price * multiplier))
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
