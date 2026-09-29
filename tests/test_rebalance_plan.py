"""D15/D13 planner: journal holdings clamped to IB, floor sizing, band, ordering, refusals, limit flags."""
from __future__ import annotations

import datetime as dt
import math

import pytest
from hypothesis import given, settings, strategies as st

from rebalance_fakes import TODAY, FakeGateway, executed, held, make_doc
from trader.rebalance.plan import (
    MAX_TARGET_AGE_SESSIONS, PositionLimits, Refusal, check_freshness, make_plan,
    reconcile_holdings, sessions_after, size_orders, target_shares,
)
from trader.rebalance.targets import validate_targets
from trader.trading.order_math import whole_shares_for_notional

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


# ---------------------------------------------------------------- holdings (D13 journal, clamped to IB)

def test_reconcile_clamps_to_ib_and_ignores_outside_positions():
    holdings, warnings = reconcile_holdings({'AAPL': 25.0, 'MSFT': 7.0}, {'AAPL': 25, 'MSFT': 3, 'TSLA': 10})
    assert holdings == {'AAPL': 25, 'MSFT': 3}            # MSFT clamped to IB's 3; TSLA is outside the sleeve
    assert any('MSFT' in w and 'IB shows 3' in w for w in warnings)


def test_reconcile_name_ib_no_longer_shows_is_flat_with_warning():
    holdings, warnings = reconcile_holdings({'MSFT': 7.0}, {'AAPL': 25})
    assert holdings == {}
    assert warnings and 'MSFT' in warnings[0] and 'IB shows none' in warnings[0]


def test_reconcile_net_short_journal_is_flat_and_flagged():
    holdings, warnings = reconcile_holdings({'AAPL': -5.0}, {'AAPL': 100})
    assert holdings == {} and 'net short' in warnings[0]


def test_reconcile_never_exceeds_ib_even_when_journal_is_larger_by_a_fraction():
    assert reconcile_holdings({'aapl': 25.0}, {'AAPL': 24.5})[0] == {'AAPL': 24}


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
    assert plan.turnover_notional == pytest.approx(7_800) and plan.gross_exposure_after == pytest.approx(7_800)
    assert plan.holdings_after() == {'AAPL': 25, 'MSFT': 7}
    assert all(isinstance(o.quantity, int) and o.quantity > 0 for o in plan.orders)


def test_rebalance_with_overlap_sells_before_buys_and_leaves_outside_positions_alone():
    gw = FakeGateway(fills=held(AAPL=30, MSFT=5), positions={'AAPL': 30, 'MSFT': 5, 'TSLA': 10}, prices=PRICES)
    plan = make_plan(_targets(), gw, today=TODAY)
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'AAPL', 5), ('BUY', 'MSFT', 2)]
    assert 'TSLA' not in plan.holdings and all(o.symbol != 'TSLA' for o in plan.orders)
    assert plan.holdings == {'AAPL': 30, 'MSFT': 5}
    assert plan.cash_after == pytest.approx(10_000 - 25 * 200 - 7 * 400)
    assert plan.sells[0].held_before == 30 and plan.sells[0].target_shares == 25
    assert ('sleeve_holdings', {'sleeve': 's2p'}) in gw.calls          # holdings come from the journal (D13)


def test_holdings_come_from_the_journal_not_from_executed_proposals():
    # EXECUTED proposals alone (no journaled fill) are submissions, not inventory (D13).
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 30)], positions={'AAPL': 30}, prices=PRICES)
    plan = make_plan(_targets(), gw, today=TODAY)
    assert plan.holdings == {} and [(o.action, o.symbol) for o in plan.orders] == [('BUY', 'AAPL'), ('BUY', 'MSFT')]


def test_name_leaving_the_targets_is_sold_in_full_even_inside_the_band():
    t = _targets(weights={'AAPL': 0.5})
    plan = size_orders(t, {'AAPL': 25, 'MSFT': 7, 'TSLA': 1}, PRICES, band_pct=5.0)   # band = $500
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'MSFT', 7), ('SELL', 'TSLA', 1)]
    assert plan.holdings_after() == {'AAPL': 25}


def test_sleeve_name_ib_no_longer_shows_is_rebought_and_warned():
    gw = FakeGateway(fills=held(AAPL=25, MSFT=7), positions={'AAPL': 25}, prices=PRICES)
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


def test_full_turnover_is_allowed_when_post_execution_exposure_fits_capital():
    # D15: turnover within a batch is not capped — sell $10k of AAPL and buy $10k of MSFT in one plan.
    plan = size_orders(_targets(weights={'MSFT': 1.0}), {'AAPL': 50}, PRICES)
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'AAPL', 50), ('BUY', 'MSFT', 25)]
    assert plan.turnover_notional == pytest.approx(20_000) and plan.gross_exposure_after == pytest.approx(10_000)


# ---------------------------------------------------------------- position limits (reported, not enforced)

def test_position_limit_flags_use_the_gate_rule_and_are_reported_not_refused():
    limits = PositionLimits(max_position_size_pct=0.10, net_liquidation=40_000)      # cap $4,000 per position
    plan = size_orders(_targets(), {}, PRICES, limits=limits)
    flagged = {o.symbol: o.limit_flag for o in plan.orders}
    assert flagged['MSFT'] is None
    assert '12.5% of net liquidation' in flagged['AAPL'] and 'max_position_size_pct 10%' in flagged['AAPL']
    assert plan.warnings == ()


def test_position_limit_values_the_whole_account_position_including_outside_shares():
    limits = PositionLimits(max_position_size_pct=0.10, net_liquidation=100_000)     # cap $10,000
    inside_only = size_orders(_targets(), {}, PRICES, limits=limits, ib_positions={'AAPL': 0})
    assert inside_only.limit_flags == []                                            # 25 sh = $5,000 fits
    with_outside = size_orders(_targets(), {}, PRICES, limits=limits, ib_positions={'AAPL': 40})
    assert [o.symbol for o in with_outside.limit_flags] == ['AAPL']                 # 40 + 25 = $13,000 > cap
    assert 'position $13,000' in (with_outside.limit_flags[0].limit_flag or '')


def test_unreadable_net_liquidation_makes_the_limit_unevaluable_and_says_so():
    plan = size_orders(_targets(), {}, PRICES, limits=PositionLimits(max_position_size_pct=0.10, net_liquidation=None))
    assert plan.limit_flags == []
    assert any('could not be evaluated' in w and 'NetLiquidation' in w for w in plan.warnings)
    sells_only = size_orders(_targets(weights={'AAPL': 0.5}), {'MSFT': 7}, PRICES,
                             limits=PositionLimits(max_position_size_pct=0.10, net_liquidation=None), band_pct=100)
    assert sells_only.buys == [] and sells_only.warnings == ()                      # exits are never gated


def test_absolute_usd_cap_is_available_for_operators():
    plan = size_orders(_targets(), {}, PRICES, limits=PositionLimits(max_position_usd=3_000))
    assert [o.symbol for o in plan.limit_flags] == ['AAPL']


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


def test_banded_overhang_is_refused_by_default_and_never_repaired():
    """AAPL holds 27 (target 25): -2 shares = $400 sits inside a 5% ($500) band, so it is skipped and its $5,400
    stays on the book; buying MSFT to its 12-share target would put the book at $10,200 > capital. D15 fixes the
    sizing and the band, so the plan is refused — the BUY is never cut to 11 and the banded SELL never un-skipped."""
    t = _targets(weights={'AAPL': 0.5, 'MSFT': 0.5})
    prices = {'AAPL': 200.0, 'MSFT': 400.0}
    with pytest.raises(Refusal) as exc:
        size_orders(t, {'AAPL': 27}, prices, band_pct=5.0)
    assert exc.value.code == 'GROSS_EXPOSURE_EXCEEDS_CAPITAL'
    assert '$10,200.00 exceeds sleeve capital $10,000.00' in exc.value.message
    assert 'AAPL' in exc.value.message and 'lower --band-pct (currently 5%)' in exc.value.message
    with pytest.raises(Refusal) as exc:                          # nothing to cut: still a refusal, not an un-skipped sell
        size_orders(t, {'AAPL': 27, 'MSFT': 12}, prices, band_pct=5.0)
    assert exc.value.code == 'GROSS_EXPOSURE_EXCEEDS_CAPITAL'


def test_the_remedy_is_a_smaller_band_which_trades_every_name_exactly_to_target():
    t = _targets(weights={'AAPL': 0.5, 'MSFT': 0.5})
    prices = {'AAPL': 200.0, 'MSFT': 400.0}
    plan = size_orders(t, {'AAPL': 27}, prices, band_pct=0.0)
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'AAPL', 2), ('BUY', 'MSFT', 12)]
    assert plan.gross_exposure_after == pytest.approx(9_800.0) and plan.cash_after == pytest.approx(200.0)   # 12 x $400 + 25 x $200
    assert plan.skipped == () and not hasattr(plan, 'notes')
    for o in plan.orders:
        assert o.target_shares == whole_shares_for_notional(o.target_weight * t.capital_usd, o.price)
        assert o.quantity == abs(o.target_shares - o.held_before)


def test_unreadable_ib_book_refuses_instead_of_assuming_flat():
    gw = FakeGateway(fills=held(AAPL=25), prices=PRICES, fail_positions=True)
    with pytest.raises(Refusal) as exc:
        make_plan(_targets(), gw, today=TODAY)
    assert exc.value.code == 'IB_UNREADABLE'


def test_unreadable_journal_refuses():
    class NoJournal(FakeGateway):
        def sleeve_holdings(self, sleeve):
            raise OSError('database locked')
    with pytest.raises(Refusal, match='JOURNAL_UNREADABLE'):
        make_plan(_targets(), NoJournal(prices=PRICES), today=TODAY)


def test_stale_targets_refuse_before_touching_any_port():
    gw = FakeGateway(prices=PRICES)
    with pytest.raises(Refusal) as exc:
        make_plan(_targets(as_of='2026-09-18', prices_as_of='2026-09-18'), gw, today=TODAY)
    assert exc.value.code == 'STALE_TARGETS' and gw.calls == []


def test_bad_band_is_refused():
    with pytest.raises(Refusal, match='BAD_BAND'):
        size_orders(_targets(), {}, PRICES, band_pct=-1)


@settings(max_examples=150, deadline=None)
@given(
    weights=st.lists(st.floats(0.01, 0.6, allow_nan=False), min_size=1, max_size=5),
    held_=st.lists(st.integers(0, 80), min_size=5, max_size=5),
    prices=st.lists(st.floats(1.0, 900.0, allow_nan=False), min_size=5, max_size=5),
    capital=st.floats(1_000.0, 50_000.0, allow_nan=False),
    band=st.floats(0.0, 10.0, allow_nan=False),
)
def test_property_plan_is_whole_share_long_only_and_never_overdraws(weights, held_, prices, capital, band):
    names = ['A', 'B', 'C', 'D', 'E']
    total = sum(weights)
    scale = min(1.0, 0.999 / total) if total > 0.999 else 1.0
    wmap = {names[i]: w * scale for i, w in enumerate(weights)}
    t = validate_targets(make_doc(weights=wmap, capital_usd=capital))
    holdings = {names[i]: h for i, h in enumerate(held_) if h}
    pmap = {names[i]: p for i, p in enumerate(prices)}
    try:
        plan = size_orders(t, holdings, pmap, band_pct=band)
    except Refusal as exc:
        assert exc.code == 'GROSS_EXPOSURE_EXCEEDS_CAPITAL'          # the only refusal sizing itself can raise
        return
    assert plan.cash_after >= 0.0
    band_usd = band / 100.0 * capital
    for o in plan.orders:                                            # D15 sizing, exactly, never adjusted to fit
        expected = whole_shares_for_notional(o.target_weight * capital, o.price) if o.target_weight * capital >= o.price else 0
        assert o.target_shares == expected
        assert o.quantity == abs(o.target_shares - o.held_before)
        assert o.notional >= band_usd or o.target_weight == 0.0      # inside the band only a full exit trades
    for sk in plan.skipped:
        assert sk.notional < band_usd
    assert plan.gross_exposure_after <= capital + 0.005
    after = plan.holdings_after()
    assert all(q > 0 for q in after.values())                          # long-only, never short
    assert all(o.quantity >= 1 and isinstance(o.quantity, int) for o in plan.orders)
    actions = [o.action for o in plan.orders]
    assert actions == sorted(actions, key=lambda a: 0 if a == 'SELL' else 1)   # SELLs listed first
    for o in plan.orders:
        if o.action == 'SELL':
            assert o.quantity <= o.held_before                          # never oversell the sleeve
    assert math.isclose(plan.cash_after, capital - plan.gross_exposure_after, abs_tol=1e-6)
