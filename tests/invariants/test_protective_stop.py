"""Invariants of record: the broker-side disaster stop.

This is the only protection that survives a dead feed or a dead
strategy_service while a position is held — the stale-bar gate guards opens,
and nothing else guards a position already on the book. So its arithmetic is
load-bearing precisely when nobody is watching, which is the worst possible
place for an untested branch.

Two properties are the whole point:

  * it must never OVERSELL. A protective SELL larger than the live position is
    exit-class, therefore exempt from every gate, and would open a SHORT out of
    a mechanism whose entire job is to close one.
  * it must sit STRICTLY BELOW entry. A stop at or above entry is not
    protection, it is an immediate market exit.

The second was not hypothetical. ``round(avg_cost * (1 - pct/100), 2)`` returns
the entry price itself for any entry under about 6 cents at the default 8%, so
the disaster stop became the disaster. Pinned below.

Both properties are one-directional on their own, so the spec also states the
converse — a healthy position always gets a plan. "Never oversell" and "always
below entry" are trivially satisfied by never planning a stop at all, and a
position silently left naked is the failure this mechanism exists to prevent.
"""

import math

from hypothesis import assume, given, settings, strategies as st

from trader.trading.protective_stop import protective_stop_plan

_SETTINGS = settings(max_examples=500, deadline=None)

# Spans the degenerate region deliberately — sub-cent prices, zero, negative,
# NaN, inf — because every pre-existing test of this code used a $100 entry and
# a healthy position, and that is exactly why the rounding bug survived.
_QTY = st.one_of(
    st.floats(min_value=-1e4, max_value=1e6),
    st.sampled_from([0, 0.0, -0.0, 0.5, 1, float('nan'), float('inf'), None, 'x']),
)
_PRICE = st.one_of(
    st.floats(min_value=-100.0, max_value=1e5),
    st.sampled_from([0, 0.0, 0.001, 0.01, 0.05, 0.06, 1e-300,
                     float('nan'), float('inf'), None, 'x']),
)
_PCT = st.one_of(
    st.floats(min_value=-10.0, max_value=200.0),
    st.sampled_from([0, 0.0, 8.0, 100.0, 1e300, float('nan'), None, 'x']),
)


def _as_float(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


# ---------------------------------------------------------------------------
# Never oversell
# ---------------------------------------------------------------------------

@_SETTINGS
@given(attributed=_QTY, broker=_QTY, avg_cost=_PRICE, pct=_PCT)
def test_a_protective_stop_never_exceeds_the_live_position(
        attributed, broker, avg_cost, pct):
    """The stop covers min(attributed, broker) and never more.

    Both bounds matter for different reasons: exceeding the BROKER position
    would short the account, and exceeding the ATTRIBUTED quantity would close
    someone else's position — a manual holding in the same instrument that this
    strategy has no claim on.
    """
    plan = protective_stop_plan(attributed, broker, avg_cost, pct)
    if plan is None:
        return
    assert plan.quantity > 0
    assert plan.quantity <= _as_float(broker)
    assert plan.quantity <= _as_float(attributed)


# ---------------------------------------------------------------------------
# Strictly below entry
# ---------------------------------------------------------------------------

@_SETTINGS
@given(attributed=_QTY, broker=_QTY, avg_cost=_PRICE, pct=_PCT)
def test_a_planned_stop_is_always_strictly_below_entry(
        attributed, broker, avg_cost, pct):
    """At or above entry, a STP SELL on a long fires immediately at market."""
    plan = protective_stop_plan(attributed, broker, avg_cost, pct)
    if plan is None:
        return
    assert 0.0 < plan.stop_price < _as_float(avg_cost)


@_SETTINGS
@given(
    qty=st.floats(min_value=1.0, max_value=1e5, allow_nan=False, allow_infinity=False),
    avg_cost=st.floats(min_value=0.001, max_value=1e5, allow_nan=False,
                       allow_infinity=False),
    pct=st.floats(min_value=0.5, max_value=99.0, allow_nan=False, allow_infinity=False),
)
def test_the_stop_is_never_closer_to_entry_than_the_requested_distance(
        qty, avg_cost, pct):
    """Rounding must only ever step AWAY from entry.

    An 8% stop that lands 7.9% away is a stop that fires sooner than the
    operator asked for, on a mechanism deliberately set wide so it never
    competes with the strategy's own exits. Floor guarantees the direction;
    round does not.
    """
    plan = protective_stop_plan(qty, qty, avg_cost, pct)
    assume(plan is not None)
    assert plan.stop_price <= avg_cost * (1.0 - pct / 100.0)


def test_a_five_cent_entry_never_gets_a_stop_at_five_cents():
    """Pinned counterexample (2026-07-26).

    The previous implementation computed round(avg_cost * (1 - pct/100), 2).
    At an entry of $0.05 and the default 8% that is round(0.046, 2) == 0.05 —
    the stop equals the entry price, so the position is sold at market the
    moment the stop is accepted. Every entry below about $0.0625 rounds up into
    that state, and no test reached the region because they all used $100.
    """
    plan = protective_stop_plan(100, 100, 0.05, 8.0)
    assert plan is None or plan.stop_price < 0.05


# ---------------------------------------------------------------------------
# The converse: a healthy position is never left naked
# ---------------------------------------------------------------------------

@_SETTINGS
@given(
    attributed=st.floats(min_value=1.0, max_value=1e5, allow_nan=False,
                         allow_infinity=False),
    broker_extra=st.floats(min_value=0.0, max_value=1e5, allow_nan=False,
                           allow_infinity=False),
    avg_cost=st.floats(min_value=1.0, max_value=1e5, allow_nan=False,
                       allow_infinity=False),
    pct=st.floats(min_value=1.0, max_value=50.0, allow_nan=False,
                  allow_infinity=False),
)
def test_a_normal_position_always_gets_a_stop(
        attributed, broker_extra, avg_cost, pct):
    """Without this, both safety properties above are satisfiable by a function
    that returns None forever — and a naked position is the thing the disaster
    stop exists to prevent. Any position at a sane price gets covered."""
    plan = protective_stop_plan(attributed, attributed + broker_extra, avg_cost, pct)
    assert plan is not None
    assert plan.quantity == attributed


def test_a_disabled_percentage_plans_nothing():
    """MMR_PROTECTIVE_STOP_PCT=0 is the documented off switch; a malformed
    value degrades to off rather than to some default distance."""
    assert protective_stop_plan(100, 100, 50.0, 0) is None
    assert protective_stop_plan(100, 100, 50.0, -1) is None
    assert protective_stop_plan(100, 100, 50.0, 'nonsense') is None


def test_the_off_switch_holds_for_a_fractional_cent_entry():
    """IB reports avgCost to more than two decimals, and at zero percent the
    'stop' is the entry price itself — which the below-entry rule then rejects
    only because it compares equal. At $100.005 it does NOT compare equal: the
    floor lands at $100.00, a cent below, and a disabled stop would be placed.
    The off switch has to be an off switch, not an emergent consequence."""
    assert protective_stop_plan(100, 100, 100.005, 0) is None


def test_an_entry_near_the_float_ceiling_plans_nothing():
    """The price is scaled by 100 to work in cents, and that product can
    overflow to infinity while the price itself is finite. math.floor(inf)
    raises OverflowError — inside the protective-stop path, which is a bare
    `except Exception` away from silently leaving a position naked."""
    assert protective_stop_plan(100, 100, 1e307, 1.0) is None


def test_a_floor_that_lands_above_target_is_stepped_down():
    """Pinned counterexample (2026-07-26), found by the property above.

    Flooring is supposed to guarantee the stop is never nearer to entry than
    asked. It does not, on its own: at an entry of 99999.99999999999 and
    1.00001%, `target * 100` rounds up onto exactly 9899999.0, so the floor is
    a cent CLOSER to entry than the operator requested. The step-down is what
    makes the guarantee hold — same idiom, same reason, as the step-down in
    order_math._floor_shares_for_notional.
    """
    plan = protective_stop_plan(1, 1, 99999.99999999999, 1.00001)
    assert plan is not None
    assert plan.stop_price == 98999.98


def test_a_position_not_yet_visible_at_the_broker_plans_nothing():
    """The fill can lag the open. Planning against a zero broker position would
    mean placing no stop at all or, worse, one sized from attribution alone."""
    assert protective_stop_plan(100, 0, 50.0, 8.0) is None
