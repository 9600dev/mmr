"""`compute_rsi` is Wilder's RSI, the definition the preset thresholds assume.

docs/AUDIT_2026-11-14.md B5.8. The Massive path reads RSI from the vendor,
which smooths the way Wilder defined it. The local `compute_rsi` (the IB path
and the TwelveData scanner) averaged the LAST `period` gains and losses with a
plain mean — Cutler's variant — so `rsi < 30` or `50 < rsi < 80` in a preset
meant one thing on one path and another on the other two.

Wilder: the first average gain / loss is the plain mean of the first `period`
changes; after that, avg = (previous * (period - 1) + this change) / period.

The expected numbers below are worked by hand, in fractions, in the comments.
"""
import math
from fractions import Fraction

import pytest

from trader.tools.idea_scanner import compute_rsi

# period 3
#   closes    10   11   12   11   13   12   12   14
#   change         +1   +1   -1   +2   -1    0   +2
#   gain            1    1    0    2    0    0    2
#   loss            0    0    1    0    1    0    0
#
#   after change 3 (seed, plain mean of the first three):
#       gain 2/3, loss 1/3                      RS 2        RSI 100 - 100/3    = 66.67
#   after change 4 (+2):
#       gain (2/3*2 + 2)/3 = 10/9
#       loss (1/3*2 + 0)/3 = 2/9                RS 5        RSI 100 - 100/6    = 83.33
#   after change 5 (-1):
#       gain (10/9*2 + 0)/3 = 20/27
#       loss (2/9*2 + 1)/3  = 13/27             RS 20/13    RSI 100 - 1300/33  = 60.61
#   after change 6 (0):
#       gain 40/81, loss 26/81                  RS 20/13    RSI                = 60.61
#   after change 7 (+2):
#       gain (40/81*2 + 2)/3 = 242/243
#       loss (26/81*2 + 0)/3 = 52/243           RS 121/26   RSI 100 - 2600/147 = 82.31
CLOSES = [10, 11, 12, 11, 13, 12, 12, 14]
HAND = {4: 66.67, 5: 83.33, 6: 60.61, 7: 60.61, 8: 82.31}


def test_the_hand_computed_series():
    assert compute_rsi(CLOSES, period=3) == 82.31


@pytest.mark.parametrize('n_closes', sorted(HAND))
def test_every_step_of_the_hand_computed_series(n_closes):
    assert compute_rsi(CLOSES[:n_closes], period=3) == HAND[n_closes]


def test_the_arithmetic_in_the_comment_is_right():
    """The fractions above, recomputed exactly (not with compute_rsi)."""
    def rsi(gain, loss):
        return round(float(100 - 100 / (1 + gain / loss)), 2)
    third = Fraction(1, 3)
    steps = [(Fraction(2, 3), Fraction(1, 3))]
    for gain, loss in [(2, 0), (0, 1), (0, 0), (2, 0)]:
        prev_gain, prev_loss = steps[-1]
        steps.append(((prev_gain * 2 + gain) * third, (prev_loss * 2 + loss) * third))
    assert steps[-1] == (Fraction(242, 243), Fraction(52, 243))
    assert [rsi(g, l) for g, l in steps] == [HAND[n] for n in sorted(HAND)]


def test_it_is_not_the_plain_mean_of_the_last_period_changes():
    """Cutler's variant on the same series: the last three changes are
    -1, 0, +2, so gain 2/3, loss 1/3, RSI 66.67. That is what this function
    returned before."""
    assert compute_rsi(CLOSES, period=3) != 66.67
    # After four changes Cutler sees +1, -1, +2: gain 1, loss 1/3, RSI 75.
    assert compute_rsi(CLOSES[:5], period=3) != 75.0


def test_earlier_history_matters():
    """Wilder's average never forgets: the same last `period` changes after a
    different past give a different RSI. A plain mean cannot tell them apart."""
    tail = [50, 51, 50, 52, 51]                       # +1 -1 +2 -1
    after_a_rally = [40, 44, 48, 50] + tail[1:]
    after_a_slide = [60, 56, 52, 50] + tail[1:]
    assert compute_rsi(after_a_rally, period=3) > compute_rsi(after_a_slide, period=3)


def test_with_exactly_period_plus_one_closes_it_is_the_seed_average():
    closes = [100, 102, 101, 104, 103, 103, 106, 105, 107, 110, 108, 109, 111, 110, 112]
    assert len(closes) == 15
    gains = sum(max(b - a, 0) for a, b in zip(closes, closes[1:])) / 14
    losses = sum(max(a - b, 0) for a, b in zip(closes, closes[1:])) / 14
    assert compute_rsi(closes) == round(100 - 100 / (1 + gains / losses), 2)


def test_default_period_is_fourteen():
    closes = [100 + ((i * 7) % 5) - 0.3 * i for i in range(40)]
    assert compute_rsi(closes) == compute_rsi(closes, period=14)
    assert compute_rsi(closes) != compute_rsi(closes, period=13)


def test_only_gains_is_100_and_only_losses_is_0():
    assert compute_rsi([100 + i for i in range(20)]) == 100.0
    assert compute_rsi([100 - i for i in range(20)]) == 0.0


def test_a_series_that_never_moved_has_no_rsi():
    """0 / 0: unknown, not 0, 50 or 100."""
    assert compute_rsi([25.0] * 30) is None


@pytest.mark.parametrize('closes', [[], [100.0], [100 + i for i in range(14)]])
def test_fewer_than_period_plus_one_closes_has_no_rsi(closes):
    assert compute_rsi(closes) is None


def test_the_result_is_a_rounded_python_float_in_range():
    closes = [100 + math.sin(i) * 3 + i * 0.1 for i in range(60)]
    value = compute_rsi(closes)
    assert type(value) is float and 0.0 <= value <= 100.0 and round(value, 2) == value


def test_closes_that_are_missing_are_skipped_like_the_callers_skip_them():
    """The IB path drops bars whose close is None and the TwelveData path
    drops NaN before calling; a NaN that arrives anyway is the same thing."""
    with_holes = [10, 11, None, 12, float('nan'), 11, 13, 12, 12, 14]
    assert compute_rsi(with_holes, period=3) == compute_rsi(CLOSES, period=3)


def test_numpy_values_are_numbers():
    import numpy as np
    assert compute_rsi(np.array(CLOSES, dtype=float), period=3) == 82.31
    assert compute_rsi([np.float64(c) for c in CLOSES], period=3) == 82.31
