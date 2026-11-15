"""`compute_resize_deltas`: which way target quantities round, and what that
guarantees.

AUDIT_2026-11-14 B5.8 ("resize truncates toward zero"). Truncating the target
toward zero is the intended rounding for whole-share holdings, and these tests
pin the bound it gives each direction:

* TRIM (`--max-bound`): the resized book is never worth MORE than the bound.
  Rounding to nearest, or up, could leave it above.
* GROW (`--min-bound`): never buys more than the proportional target, so the
  book can land short of the bound by less than one share per position. A grow
  is exposure-increasing; overshooting is the wrong side to err on (and would
  break `--max-bound` when both are given).

What truncation got wrong: a holding with a FRACTIONAL quantity. Truncating the
target to a whole number can put it below the current holding, turning a grow
into a sale (1.5 shares, scale 1.2 -> target 1 -> SELL 0.5).
"""
import random

import pytest

from trader.sdk import compute_resize_deltas


def _positions(rows):
    return [{'symbol': s, 'conId': i, 'position': qty, 'mktPrice': price,
             'marketValue': qty * price}
            for i, (s, qty, price) in enumerate(rows, start=1)]


def _resized_value(positions, adjustments):
    """Gross value of the book after applying the plan."""
    targets = {a['conId']: a['target_qty'] for a in adjustments}
    return sum(abs(targets.get(p['conId'], p['position'])) * p['mktPrice'] for p in positions)


@pytest.mark.parametrize('seed', range(25))
def test_trim_never_leaves_more_than_the_bound(seed):
    rng = random.Random(seed)
    positions = _positions([
        (f'S{i}', rng.choice([1, -1]) * rng.randint(1, 500), round(rng.uniform(1, 900), 2))
        for i in range(rng.randint(1, 12))])
    total = sum(abs(p['marketValue']) for p in positions)
    max_bound = total * rng.uniform(0.05, 0.99)

    factor, adjustments = compute_resize_deltas(positions, max_bound=max_bound, min_bound=None)

    assert factor < 1.0
    assert _resized_value(positions, adjustments) <= max_bound + 1e-6
    for a in adjustments:
        # Every delta reduces: SELL a long, BUY (cover) a short; never past flat.
        assert abs(a['target_qty']) < abs(a['current_qty'])
        assert a['target_qty'] * a['current_qty'] >= 0
        assert a['action'] == ('SELL' if a['current_qty'] > 0 else 'BUY')


@pytest.mark.parametrize('seed', range(25))
def test_grow_never_exceeds_the_bound_and_never_reduces(seed):
    rng = random.Random(1000 + seed)
    positions = _positions([
        (f'S{i}', rng.choice([1, -1]) * rng.randint(1, 500), round(rng.uniform(1, 900), 2))
        for i in range(rng.randint(1, 12))])
    total = sum(abs(p['marketValue']) for p in positions)
    min_bound = total * rng.uniform(1.01, 3.0)

    factor, adjustments = compute_resize_deltas(positions, max_bound=None, min_bound=min_bound)

    assert factor > 1.0
    resized = _resized_value(positions, adjustments)
    assert total <= resized <= min_bound + 1e-6
    # Short of the bound by less than one share per position.
    assert min_bound - resized < sum(p['mktPrice'] for p in positions) + 1e-6
    for a in adjustments:
        assert abs(a['target_qty']) > abs(a['current_qty'])
        assert a['action'] == ('BUY' if a['current_qty'] > 0 else 'SELL')


@pytest.mark.parametrize('quantity,price,min_bound', [
    (1.5, 100.0, 180.0),        # x1.2: target truncates to 1, below the 1.5 held
    (2.5, 40.0, 110.0),         # x1.1: target truncates to 2
    (0.75, 1000.0, 900.0),      # x1.2: target truncates to 0 — a grow that closed the position
])
def test_grow_of_a_fractional_long_never_sells(quantity, price, min_bound):
    positions = _positions([('FRAC', quantity, price)])
    factor, adjustments = compute_resize_deltas(positions, max_bound=None, min_bound=min_bound)
    assert factor > 1.0
    assert all(a['action'] == 'BUY' and a['delta_qty'] > 0 for a in adjustments), adjustments
    assert _resized_value(positions, adjustments) >= quantity * price


def test_grow_of_a_fractional_short_never_covers():
    positions = _positions([('FRAC', -1.5, 100.0)])
    factor, adjustments = compute_resize_deltas(positions, max_bound=None, min_bound=180.0)
    assert factor > 1.0
    assert all(a['action'] == 'SELL' and a['delta_qty'] < 0 for a in adjustments), adjustments


def test_grow_of_a_fractional_holding_still_buys_when_the_target_is_above_it():
    # 1.5 x 1.5 = 2.25 -> 2: half a share to buy, and the result is whole.
    positions = _positions([('FRAC', 1.5, 100.0)])
    _, adjustments = compute_resize_deltas(positions, max_bound=None, min_bound=225.0)
    assert len(adjustments) == 1
    assert adjustments[0]['target_qty'] == 2 and adjustments[0]['delta_qty'] == pytest.approx(0.5)
    assert adjustments[0]['action'] == 'BUY'


def test_trim_of_a_fractional_holding_still_only_reduces():
    # Truncation makes a fractional trim LARGER (0.75 -> 0), which is inside
    # the bound; it is not changed.
    positions = _positions([('FRAC', 0.75, 1000.0), ('WHOLE', 10, 100.0)])
    _, adjustments = compute_resize_deltas(positions, max_bound=1500.0, min_bound=None)
    assert _resized_value(positions, adjustments) <= 1500.0
    assert all(a['action'] == 'SELL' for a in adjustments)
