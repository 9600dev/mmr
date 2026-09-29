"""D09 planner: holdings from the sleeve ledger reconciled with IB, floor sizing, band, ordering, refusals."""
from __future__ import annotations

import datetime as dt
import math

import pytest
from hypothesis import given, settings, strategies as st

from rebalance_fakes import AS_OF, TODAY, FakeGateway, executed, make_doc
from trader.rebalance.plan import (
    MAX_TARGET_AGE_SESSIONS, PositionLimits, Refusal, check_freshness, make_plan,
    reconcile_holdings, sessions_after, size_orders, target_shares,
)
from trader.rebalance.targets import validate_targets

PRICES = {'AAPL': 200.0, 'MSFT': 400.0, 'TSLA': 250.0}


def _targets(**over):
    return validate_targets(make_doc(**over), path='/targets/s2p/2026-09-28.json')


# ---------------------------------------------------------------- freshness

def test_sessions_after_counts_nyse_sessions_only():
    assert sessions_after(dt.date(2026, 9, 28), TODAY) == 1
    assert sessions_after(dt.date(2026, 9, 22), TODAY) == 5          # Wed 23 .. Tue 29
    assert sessions_after(dt.date(2026, 9, 19), TODAY) == 7          # Saturday as_of: Mon 21 .. Tue 29
    assert sessions_after(TODAY, TODAY) == 0
    assert sessions_after(dt.date(2026, 7, 2), dt.date(2026, 7, 6)) == 1   # July 3 2026 observed holiday


def test_freshness_limit_is_five_sessions_inclusive():
    assert MAX_TARGET_AGE_SESSIONS == 5
    assert check_freshness(_targets(as_of='2026-09-22', prices_as_of='2026-09-22'), TODAY) == 5
    with pytest.raises(Refusal) as exc:
        check_freshness(_targets(as_of='2026-09-21', prices_as_of='2026-09-21'), TODAY)
    assert exc.value.code == 'STALE_TARGETS'
    with pytest.raises(Refusal) as exc:
        check_freshness(_targets(as_of='2026-10-05', prices_as_of='2026-10-05'), TODAY)
    assert exc.value.code == 'FUTURE_TARGETS'


# ---------------------------------------------------------------- holdings

def test_reconcile_clamps_to_ib_and_ignores_outside_positions():
    records = [executed(1, 'AAPL', 'BUY', 30), executed(2, 'AAPL', 'SELL', 5), executed(3, 'MSFT', 'BUY', 7),
               executed(4, 'GOOG', 'BUY', 4, status='FAILED'), executed(5, 'NVDA', 'BUY', 9, status='PENDING')]
    holdings, warnings = reconcile_holdings(records, {'AAPL': 25, 'MSFT': 3, 'TSLA': 10})
    assert holdings == {'AAPL': 25, 'MSFT': 3}            # MSFT clamped to IB's 3; TSLA is outside the sleeve
    assert any('MSFT' in w and 'IB shows 3' in w for w in warnings)
    assert 'GOOG' not in holdings and 'NVDA' not in holdings   # only EXECUTED counts


def test_reconcile_name_ib_no_longer_shows_is_flat_with_warning():
    holdings, warnings = reconcile_holdings([executed(1, 'MSFT', 'BUY', 7)], {'AAPL': 25})
    assert holdings == {}
    assert warnings and 'MSFT' in warnings[0] and 'IB shows none' in warnings[0]


def test_reconcile_net_short_ledger_is_flat_and_flagged():
    holdings, warnings = reconcile_holdings([executed(1, 'AAPL', 'SELL', 5)], {'AAPL': 100})
    assert holdings == {} and 'net short' in warnings[0]


def test_reconcile_uses_submission_quantity_over_quantity():
    rec = executed(1, 'AAPL', 'BUY', 30)
    rec = rec.__class__(**{**rec.__dict__, 'quantity': None, 'metadata': {'submission_quantity': 12.0}})
    assert reconcile_holdings([rec], {'AAPL': 12})[0] == {'AAPL': 12}


# ---------------------------------------------------------------- sizing

def test_target_shares_floors_and_returns_zero_below_one_share():
    assert target_shares(0.5, 10_000, 200.0) == 25
    assert target_shares(0.3, 10_000, 400.0) == 7            # 3000/400 = 7.5 -> 7
    assert target_shares(0.001, 10_000, 200.0) == 0          # $10 buys no share at $200
    assert target_shares(0.0, 10_000, 200.0) == 0


def test_fresh_sleeve_buys_floor_sized_whole_shares_largest_first():
    plan = size_orders(_targets(), {}, PRICES)
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('BUY', 'AAPL', 25), ('BUY', 'MSFT', 7)]
    assert plan.cash_before == 10_000 and plan.cash_after == pytest.approx(10_000 - 5_000 - 2_800)
    assert plan.gross_notional == pytest.approx(7_800) and plan.book_value_after == pytest.approx(7_800)
    assert plan.holdings_after() == {'AAPL': 25, 'MSFT': 7}
    assert all(isinstance(o.quantity, int) and o.quantity > 0 for o in plan.orders)


def test_rebalance_with_overlap_sells_before_buys_and_leaves_outside_positions_alone():
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 30), executed(2, 'MSFT', 'BUY', 5)],
                     positions={'AAPL': 30, 'MSFT': 5, 'TSLA': 10}, prices=PRICES)
    plan = make_plan(_targets(), gw, today=TODAY)
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'AAPL', 5), ('BUY', 'MSFT', 2)]
    assert 'TSLA' not in plan.holdings and all(o.symbol != 'TSLA' for o in plan.orders)
    assert plan.holdings == {'AAPL': 30, 'MSFT': 5}
    assert plan.cash_after == pytest.approx(10_000 - 25 * 200 - 7 * 400)
    assert plan.sells[0].held_before == 30 and plan.sells[0].target_shares == 25


def test_name_leaving_the_targets_is_sold_in_full_even_inside_the_band():
    t = _targets(weights={'AAPL': 0.5})
    plan = size_orders(t, {'AAPL': 25, 'MSFT': 7, 'TSLA': 1}, PRICES, band_pct=5.0)   # band = $500
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'MSFT', 7), ('SELL', 'TSLA', 1)]
    assert plan.holdings_after() == {'AAPL': 25}


def test_sleeve_name_ib_no_longer_shows_is_rebought_and_warned():
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25), executed(2, 'MSFT', 'BUY', 7)],
                     positions={'AAPL': 25}, prices=PRICES)
    plan = make_plan(_targets(), gw, today=TODAY)
    assert plan.holdings == {'AAPL': 25}
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('BUY', 'MSFT', 7)]
    assert any('MSFT' in w and 'IB shows none' in w for w in plan.warnings)


def test_no_trade_band_skips_small_changes_and_reports_them():
    plan = size_orders(_targets(), {'AAPL': 24, 'MSFT': 7}, PRICES, band_pct=5.0)       # +1 AAPL = $200 < $500
    assert plan.orders == ()
    assert [(s.symbol, s.delta) for s in plan.skipped] == [('AAPL', 1)]
    default = size_orders(_targets(), {'AAPL': 24, 'MSFT': 7}, PRICES)                   # default band 0.25% = $25
    assert [(o.action, o.symbol, o.quantity) for o in default.orders] == [('BUY', 'AAPL', 1)]


def test_sells_only_plans_the_sell_side_and_reports_deferred_buys():
    plan = size_orders(_targets(weights={'MSFT': 1.0}), {'AAPL': 50}, PRICES, sells_only=True)
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'AAPL', 50)]
    assert plan.skipped[0].symbol == 'MSFT' and 'sells-only' in plan.skipped[0].reason


def test_position_limit_flags_are_reported_but_not_refused():
    plan = size_orders(_targets(), {}, PRICES, limits=PositionLimits(max_position_usd=3_000))
    flagged = {o.symbol: o.limit_flag for o in plan.orders}
    assert 'max_position_usd' in flagged['AAPL'] and flagged['MSFT'] is None
    pct = size_orders(_targets(), {}, PRICES, limits=PositionLimits(max_position_pct=0.1, net_liquidation=20_000))
    assert {o.symbol for o in pct.limit_flags} == {'AAPL', 'MSFT'}


# ---------------------------------------------------------------- refusals

def test_unpriceable_ticker_refuses_the_whole_plan():
    for prices in ({'AAPL': 200.0}, {'AAPL': 200.0, 'MSFT': None}, {'AAPL': 200.0, 'MSFT': float('nan')},
                   {'AAPL': 200.0, 'MSFT': 0.0}, {'AAPL': 200.0, 'MSFT': -3.0}):
        with pytest.raises(Refusal) as exc:
            size_orders(_targets(), {}, prices)
        assert exc.value.code == 'UNPRICEABLE' and 'MSFT' in exc.value.message
    with pytest.raises(Refusal) as exc:                       # a HELD name must be priceable too
        size_orders(_targets(), {'TSLA': 3}, {'AAPL': 200.0, 'MSFT': 400.0})
    assert 'TSLA' in exc.value.message


def test_gross_notional_above_capital_is_refused_and_sells_only_is_the_escape_hatch():
    t = _targets(weights={'MSFT': 1.0})
    with pytest.raises(Refusal) as exc:
        size_orders(t, {'AAPL': 50}, PRICES)                  # sell $10k + buy $10k = $20k gross
    assert exc.value.code == 'GROSS_EXCEEDS_CAPITAL' and '--sells-only' in exc.value.message
    plan = size_orders(t, {'AAPL': 50}, PRICES, sells_only=True)
    assert plan.gross_notional == pytest.approx(10_000)


def test_unreadable_ib_book_refuses_instead_of_assuming_flat():
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25)], prices=PRICES, fail_positions=True)
    with pytest.raises(Refusal) as exc:
        make_plan(_targets(), gw, today=TODAY)
    assert exc.value.code == 'IB_UNREADABLE'


def test_stale_targets_refuse_before_touching_any_port():
    gw = FakeGateway(prices=PRICES)
    with pytest.raises(Refusal) as exc:
        make_plan(_targets(as_of='2026-09-18', prices_as_of='2026-09-18'), gw, today=TODAY)
    assert exc.value.code == 'STALE_TARGETS' and gw.calls == []


def test_bad_band_is_refused():
    with pytest.raises(Refusal, match='BAD_BAND'):
        size_orders(_targets(), {}, PRICES, band_pct=-1)


def test_cash_never_negative_trims_buys_when_a_banded_overweight_overhangs_capital():
    # AAPL holds 27 (target 25): -2 shares = $400, inside a 5% ($500) band, so it is skipped and its
    # $5,400 stays on the book. Buying MSFT to its 12-share target would put the book at $10,200 > capital,
    # so the planner trims the BUY by ceil($200 / $400) = 1 share and says so.
    t = _targets(weights={'AAPL': 0.5, 'MSFT': 0.5})
    prices = {'AAPL': 200.0, 'MSFT': 400.0}
    plan = size_orders(t, {'AAPL': 27}, prices, band_pct=5.0)
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('BUY', 'MSFT', 11)]
    assert plan.cash_after == pytest.approx(200.0) and plan.book_value_after <= t.capital_usd
    assert plan.notes and 'MSFT: BUY trimmed by 1' in plan.notes[0]
    assert [(s.symbol, s.delta) for s in plan.skipped] == [('AAPL', -2)]


def test_cash_never_negative_restores_a_banded_sell_when_there_is_nothing_to_trim():
    # Same overhang but MSFT is already at target: no BUY to trim, so the banded AAPL SELL is restored.
    t = _targets(weights={'AAPL': 0.5, 'MSFT': 0.5})
    prices = {'AAPL': 200.0, 'MSFT': 400.0}
    plan = size_orders(t, {'AAPL': 27, 'MSFT': 12}, prices, band_pct=5.0)
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'AAPL', 2)]
    assert plan.cash_after == pytest.approx(200.0) and plan.skipped == ()
    assert any('banded SELL of 2 restored' in n for n in plan.notes)


@settings(max_examples=150, deadline=None)
@given(
    weights=st.lists(st.floats(0.01, 0.6, allow_nan=False), min_size=1, max_size=5),
    held=st.lists(st.integers(0, 80), min_size=5, max_size=5),
    prices=st.lists(st.floats(1.0, 900.0, allow_nan=False), min_size=5, max_size=5),
    capital=st.floats(1_000.0, 50_000.0, allow_nan=False),
    band=st.floats(0.0, 10.0, allow_nan=False),
)
def test_property_plan_is_whole_share_long_only_and_never_overdraws(weights, held, prices, capital, band):
    names = ['A', 'B', 'C', 'D', 'E']
    total = sum(weights)
    scale = min(1.0, 0.999 / total) if total > 0.999 else 1.0
    wmap = {names[i]: w * scale for i, w in enumerate(weights)}
    t = validate_targets(make_doc(weights=wmap, capital_usd=capital))
    holdings = {names[i]: h for i, h in enumerate(held) if h}
    pmap = {names[i]: p for i, p in enumerate(prices)}
    try:
        plan = size_orders(t, holdings, pmap, band_pct=band)
    except Refusal as exc:
        assert exc.value.code if False else exc.code in ('GROSS_EXCEEDS_CAPITAL', 'CASH_NEGATIVE')
        return
    assert plan.cash_after >= 0.0
    assert plan.book_value_after <= capital + 0.005
    assert plan.gross_notional <= capital + 0.005
    after = plan.holdings_after()
    assert all(q > 0 for q in after.values())                          # long-only, never short
    assert all(o.quantity >= 1 and isinstance(o.quantity, int) for o in plan.orders)
    actions = [o.action for o in plan.orders]
    assert actions == sorted(actions, key=lambda a: 0 if a == 'SELL' else 1)   # SELLs listed first
    for o in plan.orders:
        if o.action == 'SELL':
            assert o.quantity <= o.held_before                          # never oversell the sleeve
    assert math.isclose(plan.cash_after, capital - plan.book_value_after, abs_tol=1e-6)
