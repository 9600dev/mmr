"""D09 propose: one PENDING proposal per order via the SDK propose call, and nothing else — ever."""
from __future__ import annotations

import pytest

from rebalance_fakes import AS_OF, SHA, TODAY, FakeGateway, executed, held, make_doc
from trader.rebalance.plan import Refusal, make_plan
from trader.rebalance.propose import check_no_open_batch, duplicate_batch, propose_plan
from trader.rebalance.targets import validate_targets
from trader.trading.proposal import ExecutionSpec

PRICES = {'AAPL': 200.0, 'MSFT': 400.0}
PATH = '/data/targets/s2p/2026-09-28.json'


def _targets(**over):
    return validate_targets(make_doc(**over), path=PATH)


def test_propose_makes_exact_calls_and_never_touches_approve_or_orders():
    gw = FakeGateway(prices=PRICES)
    t = _targets()
    plan = make_plan(t, gw, today=TODAY)
    result = propose_plan(t, plan, gw, gw)

    assert result.complete and list(result.proposal_ids) == [101, 102]
    assert result.source == f'rebalance:s2p:{AS_OF}' and result.group == 'sleeve_s2p'
    calls = gw.propose_calls
    assert [(c['symbol'], c['action'], c['quantity']) for c in calls] == [('AAPL', 'BUY', 25.0), ('MSFT', 'BUY', 7.0)]
    for c in calls:
        assert c['group'] == 'sleeve_s2p'
        assert c['source'] == f'rebalance:s2p:{AS_OF}'
        assert isinstance(c['quantity'], float) and c['quantity'] == int(c['quantity']) and c['quantity'] >= 1
        assert isinstance(c['execution'], ExecutionSpec)
        assert c['execution'].order_type == 'MARKET' and c['execution'].tif == 'DAY' and not c['execution'].outside_rth
        assert PATH in c['reasoning'] and SHA in c['reasoning'] and 'equity/s2p_top_quintile' in c['reasoning']
        rb = c['metadata']['rebalance']
        assert rb['sleeve'] == 's2p' and rb['as_of'] == AS_OF and rb['targets_file'] == PATH
        assert rb['strategy']['source_sha256'] == SHA and rb['capital_usd'] == 10_000.0
    assert gw.groups == {'sleeve_s2p': 'rebalancer sleeve s2p (D09 holdings ledger)'}
    # The only gateway methods ever invoked are reads plus propose/ensure_group.
    assert gw.method_names <= {'proposals_in_group', 'stock_positions', 'sleeve_holdings', 'price',
                               'ensure_group', 'propose'}
    assert not (gw.method_names & set(FakeGateway.FORBIDDEN))


def test_second_batch_for_same_sleeve_and_as_of_is_refused_and_creates_nothing():
    gw = FakeGateway(prices=PRICES)
    t = _targets()
    plan = make_plan(t, gw, today=TODAY)
    propose_plan(t, plan, gw, gw)
    before = len(gw.propose_calls)
    with pytest.raises(Refusal) as exc:
        propose_plan(t, make_plan(t, gw, today=TODAY), gw, gw)
    assert exc.value.code == 'DUPLICATE_BATCH' and '#101' in exc.value.message
    assert len(gw.propose_calls) == before
    with pytest.raises(Refusal, match='DUPLICATE_BATCH'):
        check_no_open_batch(t, gw)


def test_open_proposals_from_another_batch_in_the_sleeve_also_refuse():
    pending = executed(7, 'AAPL', 'BUY', 25, source='rebalance:s2p:2026-08-31', status='PENDING')
    gw = FakeGateway(proposals=[pending], prices=PRICES)
    t = _targets()
    plan = make_plan(t, gw, today=TODAY)
    assert [r.id for r in plan.open_proposals] == [7]
    with pytest.raises(Refusal) as exc:
        propose_plan(t, plan, gw, gw)
    assert exc.value.code == 'SLEEVE_BUSY' and gw.propose_calls == []


def test_approved_but_unexecuted_proposal_counts_as_open():
    recs = [executed(3, 'AAPL', 'BUY', 25, source=f'rebalance:s2p:{AS_OF}', status='APPROVED')]
    assert [r.id for r in duplicate_batch(recs, f'rebalance:s2p:{AS_OF}')] == [3]
    assert duplicate_batch(recs, 'rebalance:s2p:2026-01-01') == []


def test_terminal_proposals_do_not_block_a_new_batch():
    recs = [executed(1, 'AAPL', 'BUY', 25, source=f'rebalance:s2p:{AS_OF}', status='EXECUTED'),
            executed(2, 'MSFT', 'BUY', 7, source=f'rebalance:s2p:{AS_OF}', status='REJECTED'),
            executed(3, 'MSFT', 'BUY', 7, source=f'rebalance:s2p:{AS_OF}', status='FAILED')]
    gw = FakeGateway(proposals=recs, fills=held(AAPL=25), positions={'AAPL': 25}, prices=PRICES)
    t = _targets()
    plan = make_plan(t, gw, today=TODAY)
    check_no_open_batch(t, gw)
    result = propose_plan(t, plan, gw, gw)
    assert [(c['symbol'], c['action'], c['quantity']) for c in gw.propose_calls] == [('MSFT', 'BUY', 7.0)]
    assert result.complete


def test_empty_plan_is_refused_not_silently_proposed():
    gw = FakeGateway(fills=held(AAPL=25, MSFT=7), positions={'AAPL': 25, 'MSFT': 7}, prices=PRICES)
    t = _targets()
    plan = make_plan(t, gw, today=TODAY)
    assert plan.orders == ()
    with pytest.raises(Refusal, match='NOTHING_TO_DO'):
        propose_plan(t, plan, gw, gw)
    assert gw.propose_calls == []


def test_midway_failure_returns_the_partial_ids_and_stops():
    class Flaky(FakeGateway):
        def propose(self, **kw):
            if kw['symbol'] == 'MSFT':
                raise ConnectionError('store locked')
            return super().propose(**kw)

    gw = Flaky(prices=PRICES)
    t = _targets()
    plan = make_plan(t, gw, today=TODAY)
    result = propose_plan(t, plan, gw, gw)
    assert not result.complete and list(result.proposal_ids) == [101] and result.orders_total == 2
    assert 'BUY 7 MSFT' in result.error and 'ConnectionError' in result.error
    assert [c['symbol'] for c in gw.propose_calls] == ['AAPL']       # stopped at the failure, no retries
