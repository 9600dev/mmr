"""End to end through the REAL SDK propose path and a real DuckDB proposal store.

IB and prices are faked at the gateway's port boundary; the proposal store,
position-group store and ``MMR.propose`` are the production code. Proves that
``propose`` lands PENDING rows with the sleeve group, batch source and explicit
quantity the criteria require — and that the next run reads them back and refuses.
"""
from __future__ import annotations

import io
from typing import Mapping, Optional
from unittest.mock import MagicMock

import pytest

from rebalance_fakes import AS_OF, TODAY, make_doc, write_targets
from trader.data.position_groups import PositionGroupStore
from trader.data.proposal_store import ProposalStore
from trader.rebalance.__main__ import main
from trader.rebalance.plan import PositionLimits, Refusal, make_plan
from trader.rebalance.propose import propose_plan
from trader.rebalance.sdk_gateway import SdkGateway
from trader.rebalance.targets import validate_targets
from trader.sdk import MMR

PRICES = {'AAPL': 200.0, 'MSFT': 400.0}


def _shell_mmr(duckdb_path: str) -> MMR:
    """A real MMR wired to real stores; RPC absent so the snapshot enrichment degrades."""
    mmr = MMR.__new__(MMR)
    mmr._client = None
    mmr._data_client = None
    mmr._massive_rest_client = None
    mmr._twelvedata_rest_client = None
    mmr._subscriptions = []
    mmr._position_map = {}
    mmr._contract_map = {}
    container = MagicMock()
    container.config.return_value = {'duckdb_path': duckdb_path}
    mmr._container = container
    return mmr


class GatewayWithFakedIB(SdkGateway):
    """Real proposal store + real propose; IB positions and prices from the test."""

    def __init__(self, mmr, positions: Mapping[str, float], prices: Mapping[str, Optional[float]]):
        super().__init__(mmr)
        self._positions = dict(positions)
        self._prices = dict(prices)

    def stock_positions(self) -> Mapping[str, float]:
        return dict(self._positions)

    def price(self, ticker: str) -> Optional[float]:
        return self._prices.get(ticker)


@pytest.fixture
def gateway(tmp_duckdb_path):
    return GatewayWithFakedIB(_shell_mmr(tmp_duckdb_path), positions={}, prices=PRICES)


def test_propose_lands_pending_rows_in_the_real_store(tmp_duckdb_path, gateway):
    t = validate_targets(make_doc(), path='/targets/s2p/2026-09-28.json')
    plan = make_plan(t, gateway, today=TODAY)
    result = propose_plan(t, plan, gateway, gateway)
    assert result.complete and len(result.proposal_ids) == 2

    store = ProposalStore(tmp_duckdb_path)
    rows = {p.symbol: p for p in store.query(limit=100)}
    assert set(rows) == {'AAPL', 'MSFT'}
    for sym, qty in (('AAPL', 25.0), ('MSFT', 7.0)):
        p = rows[sym]
        assert p.status == 'PENDING' and p.action == 'BUY' and p.quantity == qty and p.amount is None
        assert p.group == 'sleeve_s2p' and p.source == f'rebalance:s2p:{AS_OF}'
        assert p.execution.order_type == 'MARKET' and p.execution.tif == 'DAY' and p.execution.outside_rth is False
        assert '/targets/s2p/2026-09-28.json' in p.reasoning and 'a' * 64 in p.reasoning
        assert p.metadata['rebalance']['target_shares'] == int(qty)
        assert 'auto_sized' not in p.metadata                      # explicit quantity: the sizer never ran
    groups = PositionGroupStore(tmp_duckdb_path)
    assert groups.get_group('sleeve_s2p') is not None
    assert set(groups.get_members('sleeve_s2p')) == {'AAPL', 'MSFT'}

    # The very next run reads the PENDING batch back from the real store and refuses.
    with pytest.raises(Refusal) as exc:
        propose_plan(t, make_plan(t, gateway, today=TODAY), gateway, gateway)
    assert exc.value.code == 'DUPLICATE_BATCH'
    assert len(store.query(limit=100)) == 2                          # nothing new was created


def test_executed_rows_become_holdings_and_a_new_as_of_rebalances_them(tmp_duckdb_path, gateway):
    t0 = validate_targets(make_doc(), path='/targets/s2p/2026-09-28.json')
    propose_plan(t0, make_plan(t0, gateway, today=TODAY), gateway, gateway)
    store = ProposalStore(tmp_duckdb_path)
    # The operator approves by hand and the orders fill: drive the state machine as the SDK's approve does.
    for p in store.query(limit=100):
        assert p.id is not None
        store.update_metadata(p.id, {'submission_quantity': float(p.quantity or 0)})
        assert store.try_transition(p.id, 'PENDING', 'APPROVED')
        assert store.try_transition(p.id, 'APPROVED', 'EXECUTED', order_ids=[900 + p.id])
    gateway._positions = {'AAPL': 25.0, 'MSFT': 7.0, 'TSLA': 3.0}   # IB now shows the fills (+ an outside name)

    t1 = validate_targets(make_doc(as_of='2026-09-29', prices_as_of='2026-09-29', weights={'AAPL': 0.3}))
    plan = make_plan(t1, gateway, today=TODAY)
    assert plan.holdings == {'AAPL': 25, 'MSFT': 7}
    assert [(o.action, o.symbol, o.quantity) for o in plan.orders] == [('SELL', 'MSFT', 7), ('SELL', 'AAPL', 10)]
    result = propose_plan(t1, plan, gateway, gateway)
    assert result.complete
    pending = [p for p in store.query(status='PENDING', limit=100)]
    assert sorted((p.symbol, p.action, p.quantity) for p in pending) == [('AAPL', 'SELL', 10.0), ('MSFT', 'SELL', 7.0)]
    assert all(p.source == 'rebalance:s2p:2026-09-29' for p in pending)


def test_cli_propose_end_to_end_over_the_real_store(tmp_path, tmp_duckdb_path, gateway):
    path = write_targets(tmp_path)
    out = io.StringIO()
    code = main(['propose', str(path), '--today', '2026-09-29'], gateway_factory=lambda: gateway,
                limits=PositionLimits(), out=out)
    assert code == 0 and 'PROPOSED 2 PENDING proposal(s) complete' in out.getvalue()
    assert {p.status for p in ProposalStore(tmp_duckdb_path).query(limit=10)} == {'PENDING'}
    code = main(['propose', str(path), '--today', '2026-09-29'], gateway_factory=lambda: gateway,
                limits=PositionLimits(), out=io.StringIO())
    assert code == 1
    assert len(ProposalStore(tmp_duckdb_path).query(limit=10)) == 2
