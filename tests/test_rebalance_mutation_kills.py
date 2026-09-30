"""Targeted tests for behavioural mutants that survived the 2026-09-29 mutation pass over
trader/rebalance (targets, propose, plan, fills). Each test names the mutant(s) it kills; the
ledger is docs/MUTATION_REBALANCE.md. These are ordinary example tests — the properties live in
tests/test_rebalance_properties.py."""
from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal

import pytest

from rebalance_fakes import TODAY, FakeGateway, evidence, executed, held, make_doc, observed, receipt
from trader.rebalance.fills import (
    ExecutionMismatch, ExecutionsIncomplete, SleeveFillsStore, executed_order_ids, match_executions,
    reconcile_sleeve, submitted_quantity, trade_date_of,
)
from trader.rebalance.plan import (
    PositionLimits, Refusal, check_freshness, make_plan, reconcile_holdings, size_orders, sleeve_state,
)
from trader.rebalance.ports import Fill
from trader.rebalance.propose import propose_plan
from trader.rebalance.targets import TargetsError, load_targets, validate_targets

PATH = '/p/t.json'
PRICES = {'AAPL': 200.0, 'MSFT': 400.0}


def _t(**over):
    return validate_targets(make_doc(**over), path=PATH)


# ============================================================ targets.py

def test_bool_capital_is_refused_naming_bool():                          # _require_5
    with pytest.raises(TargetsError, match='bool'):
        _t(capital_usd=True)


def test_huge_int_capital_is_refused_as_not_finite_not_crashed():        # _finite_3
    with pytest.raises(TargetsError, match='finite'):
        _t(capital_usd=10 ** 400)


def test_fractional_capital_is_accepted():                               # _finite_positive_9
    assert _t(capital_usd=0.5).capital_usd == 0.5


def test_decimal_params_become_json_native_numbers_with_exact_types():   # _jsonable_1,2,3,4,7,8,10,11
    strat = dict(make_doc()['strategy'])
    strat['params'] = {'a': Decimal('3'), 'b': Decimal('3.0'), 'c': Decimal('2.5'), 'd': Decimal('1E+2'),
                       'e': [Decimal('4')], 'f': {'g': Decimal('0.5')}, 'h': 7}
    params = _t(strategy=strat).strategy.params
    assert params == {'a': 3, 'b': 3.0, 'c': 2.5, 'd': 100.0, 'e': [4], 'f': {'g': 0.5}, 'h': 7}
    assert type(params['a']) is int and type(params['b']) is float and type(params['d']) is float
    assert type(params['e'][0]) is int and type(params['h']) is int


@pytest.mark.parametrize('key', ['schema', 'sleeve', 'as_of', 'generated_at', 'prices_as_of', 'strategy',
                                 'capital_usd', 'weights'])
def test_missing_top_level_field_names_the_file_and_the_field(key):      # validate_targets_13,26,63,80,95 (where=None)
    doc = make_doc()
    del doc[key]
    with pytest.raises(TargetsError) as exc:
        validate_targets(doc, path=PATH)
    assert str(exc.value).startswith(PATH) and repr(key) in str(exc.value)


@pytest.mark.parametrize('key', ['family', 'id', 'module', 'class', 'params', 'source_sha256'])
def test_missing_strategy_field_names_the_file_and_the_field(key):       # validate_targets_102,107,118,144,154,164,174
    doc = make_doc()
    del doc['strategy'][key]
    with pytest.raises(TargetsError) as exc:
        validate_targets(doc, path=PATH)
    assert str(exc.value).startswith(f'{PATH}: strategy') and repr(key) in str(exc.value)


def test_bad_prices_as_of_names_file_and_field():                        # validate_targets_72,73
    with pytest.raises(TargetsError) as exc:
        _t(prices_as_of='2026-13-01')
    assert str(exc.value).startswith(PATH) and "'prices_as_of'" in str(exc.value)


def test_weight_above_one_is_refused_by_the_per_weight_rule():           # validate_targets_243
    with pytest.raises(TargetsError, match=r'weight for AAPL is 1\.5 > 1\.0'):
        _t(weights={'AAPL': 1.5})


def test_notes_default_to_empty_and_fields_round_trip(tmp_path):         # validate_targets_255,257,268,270,280
    doc = make_doc()
    del doc['notes']
    assert validate_targets(doc).notes == ''
    p = tmp_path / 't.json'
    p.write_text(json.dumps(make_doc(notes='héllo')), encoding='utf-8')
    loaded = load_targets(p)
    assert loaded.notes == 'héllo' and loaded.prices_as_of == dt.date(2026, 9, 28)
    assert loaded.as_of == dt.date(2026, 9, 28) and loaded.sleeve == 's2p'


# ============================================================ propose.py

def _plan(**over):
    return size_orders(_t(**over), {}, PRICES)


def test_sleeve_busy_lists_the_open_ids_in_id_order():                   # check_no_open_batch_24,28,30,31
    prior = [executed(8, 'AAPL', 'BUY', 5, status='PENDING', source='rebalance:s2p:2026-08-31'),
             executed(7, 'MSFT', 'BUY', 5, status='APPROVED', source='rebalance:s2p:2026-08-31')]
    gw = FakeGateway(proposals=prior, prices=PRICES)
    with pytest.raises(Refusal) as exc:
        propose_plan(_t(), _plan(), gw, gw)
    assert exc.value.code == 'SLEEVE_BUSY'
    msg = exc.value.message
    assert '#7 (rebalance:s2p:2026-08-31)' in msg and '#8' in msg and msg.index('#7') < msg.index('#8')


def test_proposals_carry_market_day_rth_spec_and_the_strategy_params():  # propose_plan_21,64
    gw = FakeGateway(prices=PRICES)
    propose_plan(_t(), _plan(), gw, gw)
    for kw in gw.propose_calls:
        spec = kw['execution']
        assert spec.order_type == 'MARKET' and spec.tif == 'DAY' and spec.outside_rth is False
        assert kw['metadata']['rebalance']['strategy']['params'] == {'quantile': 0.2}


def test_midway_failure_reports_batch_identity_and_the_real_ids():       # propose_plan_94,95
    class FailsSecond(FakeGateway):
        def propose(self, **kw):
            if len(self.propose_calls) == 1:
                raise ConnectionError('trader_service gone')
            return super().propose(**kw)
    gw = FailsSecond(prices=PRICES)
    t = _t()
    res = propose_plan(t, _plan(), gw, gw)
    assert not res.complete and 'ConnectionError' in res.error
    assert res.source == t.source == 'rebalance:s2p:2026-09-28' and res.group == t.group == 'sleeve_s2p'
    assert len(res.proposal_ids) == 1 and res.orders_total == 2


# ============================================================ plan.py

def test_targets_dated_today_are_fresh():                                # check_freshness_1
    assert check_freshness(_t(as_of=TODAY.isoformat(), prices_as_of=TODAY.isoformat()), TODAY) == 0


def test_reconcile_zero_journal_is_silently_flat():                      # reconcile_holdings_16,18,19
    assert reconcile_holdings({'AAPL': 0.0}, {}) == ({}, [])


def test_reconcile_one_share_matching_ib_is_held_without_warning():      # reconcile_holdings_17,32,47,42,44
    assert reconcile_holdings({'AAPL': 1.0}, {'AAPL': 1.0}) == ({'AAPL': 1}, [])
    assert reconcile_holdings({'AAPL': 25.0}, {'AAPL': 25.0}) == ({'AAPL': 25}, [])


def test_reconcile_keeps_going_after_a_short_or_missing_name():          # reconcile_holdings_21,34
    holdings, warnings = reconcile_holdings({'AAPL': -5.0, 'MSFT': 7.0}, {'AAPL': 0.0, 'MSFT': 7.0})
    assert holdings == {'MSFT': 7} and len(warnings) == 1
    holdings, warnings = reconcile_holdings({'AAPL': 5.0, 'MSFT': 7.0}, {'MSFT': 7.0})
    assert holdings == {'MSFT': 7} and len(warnings) == 1


def test_reconcile_warns_on_a_fractional_overhang_and_drops_sub_share_holdings():   # reconcile_holdings_43,46
    holdings, warnings = reconcile_holdings({'AAPL': 25.5}, {'AAPL': 25.0})
    assert holdings == {'AAPL': 25} and any('using 25' in w for w in warnings)
    assert reconcile_holdings({'AAPL': 0.5}, {'AAPL': 0.5}) == ({}, [])


def test_band_is_a_percentage_of_capital_and_the_skip_reports_it():      # size_orders_52,77
    plan = size_orders(_t(weights={'AAPL': 0.62}), {'AAPL': 23}, {'AAPL': 248.0}, band_pct=5.0)   # +2 sh = $496 < $500
    assert plan.orders == () and len(plan.skipped) == 1
    sk = plan.skipped[0]
    assert sk.delta == 2 and sk.notional == pytest.approx(496.0) and 'inside no-trade band $500.00' in sk.reason


def test_sells_only_skips_a_one_share_buy_and_reports_it_exactly():      # size_orders_85,88,89
    plan = size_orders(_t(), {'AAPL': 24, 'MSFT': 7}, PRICES, band_pct=0.0, sells_only=True)
    assert plan.orders == ()
    assert [(s.symbol, s.delta, s.notional, s.reason) for s in plan.skipped] == [('AAPL', 1, 200.0, 'sells-only run')]


def test_sells_only_still_sells_names_after_a_deferred_buy():            # size_orders_97
    plan = size_orders(_t(weights={'AAA': 0.5}), {'AAA': 20, 'ZZZ': 10}, {'AAA': 200.0, 'ZZZ': 100.0},
                       sells_only=True)
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'ZZZ', 10)]
    assert [s.symbol for s in plan.skipped] == ['AAA']


def test_limit_flag_values_only_the_outside_excess_plus_the_resulting_sleeve_position():   # size_orders_123,133
    cap = PositionLimits(max_position_usd=10_000)
    no_outside = size_orders(_t(weights={'AAPL': 0.5}), {'AAPL': 20}, {'AAPL': 200.0}, limits=cap,
                             ib_positions={'AAPL': 20})            # 25 x $200 = $5,000: no flag
    assert [o.quantity for o in no_outside.buys] == [5] and no_outside.limit_flags == []
    over = size_orders(_t(weights={'AAPL': 0.6}, capital_usd=20_000), {'AAPL': 20}, {'AAPL': 200.0}, limits=cap,
                       ib_positions={'AAPL': 20})                  # 60 x $200 = $12,000 > cap
    assert [o.quantity for o in over.buys] == [40] and [o.symbol for o in over.limit_flags] == ['AAPL']


def test_a_one_share_buy_is_still_limit_flagged():                       # size_orders_159
    cap = PositionLimits(max_position_usd=4_000)
    plan = size_orders(_t(weights={'AAPL': 0.5}), {'AAPL': 24}, {'AAPL': 200.0}, limits=cap, ib_positions={'AAPL': 24})
    assert [(o.action, o.quantity) for o in plan.orders] == [('BUY', 1)]        # 25 x $200 = $5,000 > cap
    assert [o.symbol for o in plan.limit_flags] == ['AAPL']


def test_plan_carries_its_identity_capital_and_prices():                 # size_orders_231,232,233,235
    plan = size_orders(_t(), {}, PRICES)
    assert plan.sleeve == 's2p' and plan.as_of == dt.date(2026, 9, 28)
    assert plan.capital_usd == 10_000.0 and plan.prices == {'AAPL': 200.0, 'MSFT': 400.0}


def test_bool_and_text_prices_are_unpriceable():                         # _priced_1,6
    for bad in (True, 'abc'):
        with pytest.raises(Refusal) as exc:
            size_orders(_t(), {}, {'AAPL': 200.0, 'MSFT': bad})
        assert exc.value.code == 'UNPRICEABLE' and 'MSFT' in exc.value.message


def test_make_plan_uses_the_injected_today_not_the_clock():              # make_plan_6
    gw = FakeGateway(prices=PRICES)
    with pytest.raises(Refusal) as exc:
        make_plan(_t(), gw, today=dt.date(2026, 9, 27))          # the day BEFORE as_of
    assert exc.value.code == 'FUTURE_TARGETS'


def test_make_plan_threads_limits_ib_book_band_and_sells_only_through():  # make_plan_24,25,28,32,33,34,37; sleeve_state_36
    gw = FakeGateway(positions={'AAPL': 40}, prices=PRICES)      # 40 AAPL held OUTSIDE the sleeve
    plan = make_plan(_t(), gw, today=TODAY, limits=PositionLimits(max_position_usd=10_000))
    assert [o.symbol for o in plan.limit_flags] == ['AAPL']       # (40 + 25) x $200 = $13,000 > cap
    gw = FakeGateway(fills=held(AAPL=24, MSFT=7), positions={'AAPL': 24, 'MSFT': 7}, prices=PRICES)
    banded = make_plan(_t(), gw, today=TODAY, band_pct=5.0)
    assert banded.orders == () and [s.symbol for s in banded.skipped] == ['AAPL']
    sells_only = make_plan(_t(), gw, today=TODAY, band_pct=0.0, sells_only=True)
    assert sells_only.orders == () and sells_only.skipped[0].reason == 'sells-only run'


def test_sleeve_state_exposes_the_raw_journal_and_ib_book():             # sleeve_state_37
    gw = FakeGateway(fills=held(AAPL=25.5), positions={'AAPL': 25, 'TSLA': 3}, prices=PRICES)
    state = sleeve_state('s2p', gw)
    assert state.journal == {'AAPL': 25.5} and state.ib_positions == {'AAPL': 25, 'TSLA': 3}
    assert state.holdings == {'AAPL': 25}


# ============================================================ fills.py

def test_trade_date_is_the_new_york_session_date_not_local_or_utc():    # trade_date_of_4
    assert trade_date_of(dt.datetime(2026, 9, 2, 2, 0, tzinfo=dt.timezone.utc)) == dt.date(2026, 9, 1)   # 22:00 ET Sep 1
    assert trade_date_of(dt.datetime(2026, 9, 2, 6, 0, tzinfo=dt.timezone.utc)) == dt.date(2026, 9, 2)   # 02:00 ET Sep 2


def _fill(exec_id: str, qty: float, price: float | None, commission: float | None, order_id: str = '901') -> Fill:
    return Fill(sleeve='s2p', ticker='AAPL', trade_date=dt.date(2026, 9, 1), quantity=qty, price=price,
                commission=commission, exec_id=exec_id, order_id=order_id, proposal_id=1,
                recorded_at=dt.datetime(2026, 9, 1, 15, 0, tzinfo=dt.timezone.utc))


def test_store_round_trips_every_fill_field(tmp_path):                   # __init___1, record_20, list_fills_10..46
    path = str(tmp_path / 'fills.duckdb')
    store = SleeveFillsStore(path)
    assert store.duckdb_path == path
    a, b = _fill('x1', 25.0, 100.5, 1.25), _fill('x2', -5.0, 101.0, None, order_id='902')
    assert store.record([a, b]) == 2
    back = store.list_fills('s2p')
    assert [(f.sleeve, f.ticker, f.trade_date, f.quantity, f.price, f.commission, f.exec_id, f.order_id, f.proposal_id)
            for f in back] == [(f.sleeve, f.ticker, f.trade_date, f.quantity, f.price, f.commission, f.exec_id,
                                f.order_id, f.proposal_id) for f in (a, b)]
    assert all(f.recorded_at.astimezone(dt.timezone.utc) == a.recorded_at for f in back)
    assert store.holdings('s2p') == {'AAPL': 20.0}


def test_executed_order_ids_keeps_going_after_a_proposal_without_ids():  # executed_order_ids_9
    recs = [executed(1, 'AAPL', 'BUY', 5, order_ids=()), executed(2, 'MSFT', 'BUY', 5)]
    mapping, without = executed_order_ids(recs)
    assert set(mapping) == {902} and without == [1]


def test_submitted_quantity_prefers_the_recorded_submission_and_refuses_non_positive():   # submitted_quantity_2,3,5,14,16,17
    rec = executed(1, 'AAPL', 'BUY', 25)
    assert submitted_quantity(rec) == 25.0
    assert submitted_quantity(rec.__class__(**{**rec.__dict__, 'metadata': {'submission_quantity': 30.0}})) == 30.0
    assert submitted_quantity(rec.__class__(**{**rec.__dict__, 'metadata': {'con_id': 1}})) == 25.0
    for bad in (float('inf'), 0.0, -3.0):
        assert submitted_quantity(rec.__class__(**{**rec.__dict__, 'metadata': {'submission_quantity': bad}})) is None
    assert submitted_quantity(rec.__class__(**{**rec.__dict__, 'metadata': {'submission_quantity': 1.0}})) == 1.0


def test_one_share_orders_journal_and_recorded_at_defaults_to_an_aware_instant():   # match_executions_3,40,48
    recs = [executed(1, 'AAPL', 'BUY', 1)]
    ev = evidence(orders=[observed(901, 'AAPL', 'BUY', 'Filled', filled=1.0)], executions=[receipt('x1', 901, 1.0)])
    match = match_executions('s2p', recs, ev)
    assert [f.quantity for f in match.fills] == [1.0]
    assert match.fills[0].recorded_at.tzinfo is not None


def test_two_observations_of_one_order_pick_the_most_filled_status():    # match_executions_112,114,115
    recs = [executed(1, 'AAPL', 'BUY', 25)]
    ev = evidence(orders=[observed(901, 'AAPL', 'BUY', 'PendingSubmit', filled=0.0, total=25.0),
                          observed(901, 'AAPL', 'BUY', 'Submitted', filled=5.0, total=25.0)],
                  executions=[receipt('x1', 901, 5.0)])
    match = match_executions('s2p', recs, ev)
    assert len(match.working) == 1
    assert match.working[0].broker_status == 'Submitted' and match.working[0].filled == 5.0


def test_a_mismatch_on_a_later_order_is_still_a_mismatch():              # match_executions_146
    recs = [executed(1, 'AAPL', 'BUY', 25), executed(2, 'MSFT', 'BUY', 5)]
    ev = evidence(orders=[observed(902, 'TSLA', 'BUY', 'Filled', filled=5.0)], executions=[receipt('x2', 902, 5.0)])
    with pytest.raises(ExecutionMismatch):                       # order 901 is a problem; order 902 is a MISMATCH
        match_executions('s2p', recs, ev)


def test_reconcile_stamps_the_given_now_and_reports_identity_book_and_unfilled():   # reconcile_sleeve_77,81,141,151,156,158,160,174
    now = dt.datetime(2026, 9, 29, 15, 0, tzinfo=dt.timezone.utc)
    obs, ex = observed(901, 'AAPL', 'BUY', 'Filled', filled=1.0), receipt('x1', 901, 1.0)
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 1), executed(2, 'MSFT', 'BUY', 1, order_ids=()),
                                executed(3, 'TSLA', 'BUY', 1)],
                     positions={'TSLA': 1},
                     evidence=evidence(orders=[obs, observed(903, 'TSLA', 'BUY', 'Cancelled', filled=0.0)],
                                       executions=[ex]))
    report = reconcile_sleeve('s2p', gw, now=now)
    assert gw.recorded[0].recorded_at == now
    assert report.sleeve == 's2p' and report.ib_positions == {'TSLA': 1}
    assert report.proposals_without_order_ids == (2,)
    assert [(u.proposal_id, u.order_id, u.broker_status) for u in report.unfilled] == [(3, 903, 'Cancelled')]
    assert any('AAPL' in w and 'IB shows 0' in w for w in report.warnings)   # journal 1, IB none


def test_receipt_only_resolution_needs_an_exact_match():                 # (pins the receipt-only rule the mutants around it guard)
    recs = [executed(1, 'AAPL', 'BUY', 25)]
    ok = evidence(orders=[], executions=[receipt('x1', 901, 25.0)])
    assert match_executions('s2p', recs, ok).receipt_only_orders == (901,)
    short = evidence(orders=[], executions=[receipt('x1', 901, 24.0)])
    with pytest.raises(ExecutionsIncomplete):
        match_executions('s2p', recs, short)
