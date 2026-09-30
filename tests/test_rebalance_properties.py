"""Hypothesis properties over the paper-sleeve order path (trader.rebalance + trader.lowfreq_live).

These are CANDIDATE invariants, drafted for human promotion into tests/invariants/
(see docs/INVARIANTS_PROPOSED_REBALANCE.md). They live here, outside the
human-owned spec, until a human signs them off. Each states a harm the sleeve
must never cause and stays silent about mechanism:

  P1  a plan never SELLs more shares of a symbol than the sleeve holds;
  P2  the plan's BUY notional never exceeds sleeve cash plus its own SELL proceeds,
      at the prices the plan uses;
  P3  every SELL leg reduces the pinned account's live position:
      sell qty <= min(sleeve holding, broker position), so the server classifies it exit-class;
  P4  shares held OUTSIDE the sleeve are never sold and never counted as sleeve holdings;
  P5  targets written by trader.lowfreq_live load through trader.rebalance.targets.load_targets
      with strictly positive weights whose EXACT decimal sum is <= 1;
  P6  propose creates exactly one PENDING proposal per planned order, in plan order,
      and creates nothing when the sleeve has an open batch;
  P7  a journaled fill is always a finite, side-signed quantity (found bug A below).

Where the code's true behaviour is stricter than the property as worded, the test
encodes the TRUE behaviour and the docstring says so (P5: weights are strictly
positive, not merely non-negative — the reader refuses a zero weight).

Bugs these properties found on 2026-09-29 (pinned below as named regressions):
  A. fills.match_executions journaled a receipt whose ``shares`` was NaN/inf: the
     ``<= 0`` guard is False for NaN, the receipt-vs-broker comparisons are all
     False for NaN, and a Fill(quantity=nan) reached the D13 journal — after which
     SUM(quantity) is NaN for that ticker forever.
  B. plan.reconcile_holdings / make_plan crashed with a bare ValueError
     ("cannot convert float NaN to integer") on a non-finite journal quantity,
     instead of refusing with a named Refusal. The sleeve was bricked: every
     later plan crashed the same way.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import tempfile
from decimal import Decimal
from pathlib import Path

import pytest
from hypothesis import assume, event, example, given, settings, strategies as st

from rebalance_fakes import TODAY, FakeGateway, evidence, executed, make_doc, observed, receipt
from trader.lowfreq_live import targets as writer
from trader.rebalance.fills import ExecutionsIncomplete, match_executions
from trader.rebalance.plan import Refusal, make_plan, reconcile_holdings, size_orders
from trader.rebalance.propose import propose_plan
from trader.rebalance.targets import TargetsError, exact_weight_sum, load_targets, validate_targets

POOL = ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF']
_SLACK = 0.01           # plan.py compares dollar sums with half-cent slack; allow a full cent here


# ---------------------------------------------------------------------------
# generators
# ---------------------------------------------------------------------------

@st.composite
def weight_maps(draw) -> dict[str, float]:
    """1..6 names from POOL with weights in (0, 1] whose float sum is <= 0.999."""
    n = draw(st.integers(1, len(POOL)))
    names = draw(st.permutations(POOL))[:n]
    raw = draw(st.lists(st.floats(0.001, 1.0, allow_nan=False, allow_infinity=False), min_size=n, max_size=n))
    total = math.fsum(raw)
    scale = min(1.0, 0.999 / total) if total > 0.999 else 1.0
    return {names[i]: raw[i] * scale for i in range(n)}


holdings_maps = st.dictionaries(st.sampled_from(POOL), st.integers(1, 400), max_size=len(POOL))
price_maps = st.fixed_dictionaries({t: st.floats(0.5, 2000.0, allow_nan=False, allow_infinity=False) for t in POOL})
capitals = st.floats(500.0, 200_000.0, allow_nan=False, allow_infinity=False)
bands = st.floats(0.0, 10.0, allow_nan=False, allow_infinity=False)
# The D13 journal: per-ticker signed sums (fractional and negative values are possible in a corrupt journal).
journal_maps = st.dictionaries(st.sampled_from(POOL),
                               st.floats(-50.0, 600.0, allow_nan=False, allow_infinity=False), max_size=len(POOL))
# IB's book for the whole account: may hold names the sleeve does not, more or fewer shares than the journal.
ib_maps = st.dictionaries(st.sampled_from(POOL),
                          st.floats(-20.0, 800.0, allow_nan=False, allow_infinity=False), max_size=len(POOL))


def _targets(weights: dict[str, float], capital: float):
    return validate_targets(make_doc(weights=weights, capital_usd=capital), path='/targets/s2p/2026-09-28.json')


def _plan_or_refusal(targets, holdings, prices, band):
    try:
        return size_orders(targets, holdings, prices, band_pct=band)
    except Refusal as exc:
        assert exc.code == 'GROSS_EXPOSURE_EXCEEDS_CAPITAL'     # the only refusal sizing itself may raise here
        event('refused: gross exposure exceeds capital')
        return None


# ---------------------------------------------------------------------------
# P1 / P2 — pure sizing
# ---------------------------------------------------------------------------

@settings(max_examples=120, deadline=None)
@given(weights=weight_maps(), holdings=holdings_maps, prices=price_maps, capital=capitals, band=bands)
def test_p1_plan_never_sells_more_than_the_sleeve_holds(weights, holdings, prices, capital, band):
    plan = _plan_or_refusal(_targets(weights, capital), holdings, prices, band)
    if plan is None:
        return
    for o in plan.sells:
        assert o.quantity <= holdings.get(o.symbol, 0), (o, holdings)
        assert o.held_before == holdings.get(o.symbol, 0)
    assert all(q > 0 for q in plan.holdings_after().values())     # long-only: never short after execution


@settings(max_examples=120, deadline=None)
@given(weights=weight_maps(), holdings=holdings_maps, prices=price_maps, capital=capitals, band=bands)
def test_p2_buy_notional_never_exceeds_cash_plus_own_sell_proceeds(weights, holdings, prices, capital, band):
    plan = _plan_or_refusal(_targets(weights, capital), holdings, prices, band)
    if plan is None:
        return
    buys = math.fsum(o.notional for o in plan.buys)
    sells = math.fsum(o.notional for o in plan.sells)
    # cash the sleeve has at plan prices = capital - value of holdings at plan prices
    assert math.isclose(plan.cash_before, capital - math.fsum(holdings.get(t, 0) * prices[t] for t in POOL),
                        abs_tol=1e-6)
    assert buys <= plan.cash_before + sells + _SLACK, (buys, plan.cash_before, sells)
    assert plan.cash_after >= -_SLACK
    assert plan.gross_exposure_after <= capital + _SLACK


# ---------------------------------------------------------------------------
# P3 / P4 — through make_plan: journal clamped to IB, outside shares untouched
# ---------------------------------------------------------------------------

def _make_plan(weights, capital, journal, ib, prices, band):
    gw = FakeGateway(fills={'s2p': dict(journal)}, positions=dict(ib), prices=dict(prices))
    try:
        return gw, make_plan(_targets(weights, capital), gw, today=TODAY, band_pct=band)
    except Refusal as exc:
        assert exc.code == 'GROSS_EXPOSURE_EXCEEDS_CAPITAL'
        event('refused: gross exposure exceeds capital')
        return gw, None


@settings(max_examples=100, deadline=None)
@given(weights=weight_maps(), journal=journal_maps, ib=ib_maps, prices=price_maps, capital=capitals, band=bands)
def test_p3_every_sell_reduces_the_live_broker_position(weights, journal, ib, prices, capital, band):
    """sell qty <= min(sleeve holding, broker position): the server classifies such an order exit-class."""
    gw, plan = _make_plan(weights, capital, journal, ib, prices, band)
    if plan is None:
        return
    for o in plan.sells:
        broker = ib.get(o.symbol, 0.0)
        sleeve = journal.get(o.symbol, 0.0)
        assert o.quantity <= min(sleeve, broker), (o, sleeve, broker)
        assert broker - o.quantity >= 0.0                         # the account is never taken short by the sleeve
        assert o.held_before - o.quantity >= 0                    # nor is the sleeve
    assert gw.method_names <= {'proposals_in_group', 'sleeve_holdings', 'stock_positions', 'price'}   # read-only


@settings(max_examples=100, deadline=None)
@given(weights=weight_maps(), journal=journal_maps, ib=ib_maps, prices=price_maps, capital=capitals, band=bands)
def test_p4_shares_outside_the_sleeve_are_never_sold_nor_counted(weights, journal, ib, prices, capital, band):
    gw, plan = _make_plan(weights, capital, journal, ib, prices, band)
    if plan is None:
        return
    for sym in POOL:
        j = journal.get(sym, 0.0)
        b = ib.get(sym, 0.0)
        held = plan.holdings.get(sym, 0)
        if j <= 0 or b <= 0:
            assert held == 0, (sym, j, b, held)                   # nothing journaled, or IB shows none: flat
        else:
            assert held <= math.floor(j) and held <= b, (sym, j, b, held)   # never above the journal, never above IB
        assert held >= 0
    for o in plan.orders:
        assert o.held_before == plan.holdings.get(o.symbol, 0)  # every order sizes from the SLEEVE's holding
        if o.action == 'SELL':
            assert o.quantity <= plan.holdings.get(o.symbol, 0)  # outside shares are never sold
    outside_only = [s for s in POOL if ib.get(s, 0.0) > 0 and journal.get(s, 0.0) <= 0]
    for sym in outside_only:
        assert sym not in plan.holdings
        assert all(o.symbol != sym for o in plan.sells)


# ---------------------------------------------------------------------------
# P5 — the writer's file loads through the reader with sum <= 1 (exact)
# ---------------------------------------------------------------------------

@st.composite
def raw_weight_vectors(draw) -> dict[str, float]:
    n = draw(st.integers(1, 80))
    names = draw(st.lists(st.from_regex(r'\A[A-Z][A-Z0-9]{0,4}\Z', fullmatch=True), unique=True,
                          min_size=n, max_size=n))
    raw = draw(st.lists(st.floats(1e-9, 1.0, allow_nan=False, allow_infinity=False), min_size=n, max_size=n))
    gross = draw(st.floats(0.05, 1.0, allow_nan=False, allow_infinity=False))
    scale = gross / math.fsum(raw)
    out = {names[i]: raw[i] * scale for i in range(n)}
    assume(math.fsum(out.values()) <= 1.0)          # a valid (long-only, unlevered) weight vector
    return out


def _writer_doc(weights: dict[str, float], capital: float = 25_000.0) -> dict:
    return {'schema': writer.SCHEMA, 'sleeve': 'paper_s2p', 'as_of': '2026-09-28',
            'generated_at': dt.datetime(2026, 9, 28, 21, 5, tzinfo=dt.timezone.utc).isoformat(),
            'strategy': {'family': 'equity', 'id': 's2p', 'module': 'strategies.lowfreq_equity_s2p',
                         'class': 'S2P', 'params': {'quantile': 0.2}, 'source_sha256': 'a' * 64},
            'capital_usd': float(capital), 'prices_as_of': '2026-09-28', 'weights': weights, 'notes': ''}


@settings(max_examples=60, deadline=None)
@given(raw=raw_weight_vectors())
def test_p5_written_targets_load_through_the_reader_with_positive_weights_summing_at_most_one(raw):
    """TRUE behaviour is STRICTER than 'non-negative': the reader refuses a zero weight, so every
    loaded weight is > 0 (the writer drops weights that floor to zero). The sum bound is EXACT —
    judged on the decimal strings in the file, the way the reader judges it."""
    written = writer.written_weights(raw)
    assume(written)                                   # an all-flat instruction is refused upstream by compute_targets
    doc = _writer_doc(written)
    writer.validate_targets_document(doc)             # the writer's own D08 check passes
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / 'paper_s2p' / '2026-09-28.json'
        writer.write_targets(writer.TargetsResult(document=doc, benchmarks={}), path=path)
        loaded = load_targets(path)                   # the reader accepts the file as written
        on_disk = json.loads(path.read_text(), parse_float=Decimal)
    assert set(loaded.weights) == set(written) <= set(raw)
    assert all(w > 0 for w in loaded.weights.values())
    assert exact_weight_sum(on_disk['weights']) <= 1              # exact decimal sum, no tolerance
    assert loaded.weight_sum <= 1.0 + 1e-12
    for t, w in loaded.weights.items():
        assert w <= raw[t] and raw[t] - w < 1e-10                 # floored, never raised, by < 1e-10
    assert loaded.capital_usd == doc['capital_usd'] and loaded.sleeve == 'paper_s2p'


def test_p5_regression_writer_regex_is_wider_than_the_readers():
    """Not a capital-safety bug (the reader refuses, fail-closed) but a real cross-component gap: the writer
    accepts tickers the reader will not load. Pinned so the divergence is visible, not silently relied on."""
    for ticker in ('ABCDEFG', 'A.B.C', 'AB-CDEF'):
        doc = _writer_doc({ticker: 0.5})
        writer.validate_targets_document(doc)                    # writer: fine
        with pytest.raises(TargetsError, match='not a US common-stock symbol'):   # reader: refused, by the ticker rule
            validate_targets(doc)


# ---------------------------------------------------------------------------
# P6 — propose: one PENDING proposal per planned order, nothing when the sleeve is busy
# ---------------------------------------------------------------------------

@settings(max_examples=60, deadline=None)
@given(weights=weight_maps(), holdings=holdings_maps, prices=price_maps, capital=capitals,
       busy=st.sampled_from([None, 'PENDING', 'APPROVED', 'EXECUTED', 'REJECTED']))
def test_p6_propose_mirrors_the_plan_exactly_or_creates_nothing(weights, holdings, prices, capital, busy):
    targets = _targets(weights, capital)
    plan = _plan_or_refusal(targets, holdings, prices, 0.0)
    if plan is None or not plan.orders:
        return
    prior = [executed(7, 'AAA', 'BUY', 5, status=busy, source='rebalance:s2p:2026-08-31')] if busy else []
    gw = FakeGateway(proposals=prior, positions={}, prices=dict(prices))
    if busy in ('PENDING', 'APPROVED'):
        with pytest.raises(Refusal) as exc:
            propose_plan(targets, plan, gw, gw)
        assert exc.value.code == 'SLEEVE_BUSY'
        assert gw.propose_calls == [] and gw.groups == {}          # refused BEFORE anything was created
        return
    result = propose_plan(targets, plan, gw, gw)
    assert result.complete and len(result.proposal_ids) == len(plan.orders)
    calls = gw.propose_calls
    assert [(c['symbol'], c['action'], c['quantity']) for c in calls] == \
        [(o.symbol, o.action, float(o.quantity)) for o in plan.orders]
    assert all(c['group'] == targets.group and c['source'] == targets.source for c in calls)
    assert [c['action'] for c in calls] == sorted((c['action'] for c in calls), key=lambda a: a != 'SELL')
    assert all(p.status == 'PENDING' for p in gw.proposals if p.id in result.proposal_ids)


# ---------------------------------------------------------------------------
# P7 — the journal only ever receives finite, side-signed quantities (bug A)
# ---------------------------------------------------------------------------

any_float = st.floats(allow_nan=True, allow_infinity=True, width=64)


@settings(max_examples=150, deadline=None)
@example(action='BUY', shares=float('nan'), cum=25.0, filled=25.0, known=True, status='Filled')     # bug A, as found
@example(action='SELL', shares=float('inf'), cum=float('inf'), filled=float('inf'), known=True, status='Filled')
@given(action=st.sampled_from(['BUY', 'SELL']), shares=any_float, cum=any_float, filled=any_float,
       known=st.booleans(), status=st.sampled_from(['Filled', 'Submitted', 'Cancelled']))
def test_p7_journaled_fills_are_finite_and_signed_by_side(action, shares, cum, filled, known, status):
    recs = [executed(1, 'AAPL', action, 25)]
    ev = evidence(orders=[observed(901, 'AAPL', action, status, filled=filled, known=known)],
                  executions=[receipt('x1', 901, shares, cum_qty=cum)])
    try:
        match = match_executions('s2p', recs, ev)
    except ExecutionsIncomplete:
        event('refused')
        return
    for f in match.fills:
        assert math.isfinite(f.quantity) and f.quantity != 0
        assert (f.quantity > 0) == (action == 'BUY')
        assert abs(f.quantity) == shares


def test_regression_bug_a_nan_or_inf_shares_receipt_is_refused_not_journaled():
    """Bug A: broker says 25 filled, the receipt says NaN shares — every comparison is False for NaN and a
    Fill(quantity=nan) reached the journal. Now: ExecutionsIncomplete, nothing journaled."""
    recs = [executed(1, 'AAPL', 'BUY', 25)]
    for bad in (float('nan'), float('inf'), -float('inf')):
        ev = evidence(orders=[observed(901, 'AAPL', 'BUY', 'Filled', filled=25.0)],
                      executions=[receipt('x1', 901, bad, cum_qty=25.0)])
        with pytest.raises(ExecutionsIncomplete, match='x1'):
            match_executions('s2p', recs, ev)
        ev = evidence(orders=[observed(901, 'AAPL', 'BUY', 'Filled', filled=bad)],
                      executions=[receipt('x1', 901, bad, cum_qty=bad)])
        with pytest.raises(ExecutionsIncomplete):
            match_executions('s2p', recs, ev)


def test_regression_bug_a_non_finite_broker_fill_quantity_is_not_evidence():
    """A Filled observation whose cumulative quantity is NaN matched a 25-share receipt because NaN compares
    False both ways. D13: the receipts must match the broker's number EXACTLY; NaN matches nothing."""
    recs = [executed(1, 'AAPL', 'BUY', 25)]
    ev = evidence(orders=[observed(901, 'AAPL', 'BUY', 'Filled', filled=float('nan'))],
                  executions=[receipt('x1', 901, 25.0, cum_qty=25.0)])
    with pytest.raises(ExecutionsIncomplete, match='not a finite'):
        match_executions('s2p', recs, ev)


def test_regression_bug_b_non_finite_journal_quantity_refuses_the_plan_instead_of_crashing():
    """Bug B: a NaN journal sum crashed reconcile_holdings with a bare ValueError from math.floor and bricked
    every later plan. Now the pure function raises a ValueError NAMING the symbol, and make_plan turns it into
    the JOURNAL_UNREADABLE refusal it already uses for a journal it cannot read."""
    for bad in (float('nan'), float('inf')):
        with pytest.raises(ValueError, match='AAPL'):
            reconcile_holdings({'AAPL': bad}, {'AAPL': 25})
        gw = FakeGateway(fills={'s2p': {'AAPL': bad}}, positions={'AAPL': 25}, prices={'AAPL': 200.0, 'MSFT': 400.0})
        with pytest.raises(Refusal) as exc:
            make_plan(validate_targets(make_doc()), gw, today=TODAY)
        assert exc.value.code == 'JOURNAL_UNREADABLE' and 'AAPL' in exc.value.message
        assert 'price' not in gw.method_names                   # refused before pricing anything
