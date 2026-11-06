"""Proposal expiry: a PENDING proposal past proposal_ttl_hours can't be approved.

Field incident 2026-11-05: 44 rebalance MARKET proposals written 2026-10-01
were still PENDING and approvable five weeks later, sized from Oct-1 prices.
These tests pin the rule at every layer that can open a position on a
proposal: the pure predicate, the store sweep, sdk.approve, and the
server-side authorization in TraderServiceApi.place_expressive_order.
"""
import datetime as dt

import pytest

from trader.data.proposal_expiry import (
    DEFAULT_PROPOSAL_TTL_HOURS, describe_ttl, is_expired, proposal_ttl,
    proposal_ttl_from_config,
)
from trader.data.proposal_store import ProposalStore
from trader.trading.proposal import ExecutionSpec, TradeProposal

DAY = dt.timedelta(hours=24)


def _backdate(store: ProposalStore, pid: int, age: dt.timedelta, field: str = 'created_at'):
    ts = dt.datetime.now() - age
    store.db.execute(f'UPDATE trade_proposals SET {field} = ? WHERE id = ?', [ts, pid], fetch='none')


def _add(store: ProposalStore, symbol='AMD', age: dt.timedelta = dt.timedelta(0), **kw) -> int:
    pid = store.add(TradeProposal(symbol=symbol, action='BUY', quantity=kw.pop('quantity', 10.0),
                                  execution=ExecutionSpec(), **kw))
    if age:
        _backdate(store, pid, age)
    return pid


# --- pure rule ---------------------------------------------------------------

class TestTtlParsing:
    def test_default_when_missing(self):
        assert proposal_ttl(None) == dt.timedelta(hours=DEFAULT_PROPOSAL_TTL_HOURS)
        assert proposal_ttl_from_config({}) == DAY

    def test_zero_or_negative_disables(self):
        assert proposal_ttl(0) is None
        assert proposal_ttl(-1) is None

    def test_numeric_strings_accepted(self):
        assert proposal_ttl('6') == dt.timedelta(hours=6)

    @pytest.mark.parametrize('bad', ['abc', True, float('inf'), float('nan'), [24]])
    def test_malformed_values_fail_loudly(self, bad):
        with pytest.raises(ValueError):
            proposal_ttl(bad)

    def test_describe(self):
        assert describe_ttl(None) == 'disabled'
        assert describe_ttl(DAY) == '24h'


class TestIsExpired:
    now = dt.datetime(2026, 11, 5, 12, 0)

    def test_boundary(self):
        assert not is_expired(self.now - DAY, self.now, DAY)
        assert is_expired(self.now - DAY - dt.timedelta(seconds=1), self.now, DAY)

    def test_disabled_never_expires(self):
        assert not is_expired(self.now - 100 * DAY, self.now, None)

    def test_missing_created_at_is_expired(self):
        assert is_expired(None, self.now, DAY)

    def test_tz_aware_now_against_naive_created(self):
        aware_now = dt.datetime.now().astimezone()
        assert not is_expired(dt.datetime.now() - dt.timedelta(hours=1), aware_now, DAY)
        assert is_expired(dt.datetime.now() - 2 * DAY, aware_now, DAY)


# --- store sweep ---------------------------------------------------------------

class TestExpireStale:
    def test_expires_only_old_pending(self, proposal_store):
        old = _add(proposal_store, 'OLD', age=35 * DAY)
        fresh = _add(proposal_store, 'NEW', age=dt.timedelta(hours=1))
        expired = proposal_store.expire_stale(DAY)
        assert [p.id for p in expired] == [old]
        assert proposal_store.get(old).status == 'EXPIRED'
        assert 'proposal_ttl_hours' in proposal_store.get(old).rejection_reason
        assert proposal_store.get(fresh).status == 'PENDING'

    def test_dry_run_changes_nothing(self, proposal_store):
        old = _add(proposal_store, age=2 * DAY)
        would = proposal_store.expire_stale(DAY, dry_run=True)
        assert [p.id for p in would] == [old]
        assert proposal_store.get(old).status == 'PENDING'

    def test_disabled_is_noop(self, proposal_store):
        old = _add(proposal_store, age=100 * DAY)
        assert proposal_store.expire_stale(None) == []
        assert proposal_store.get(old).status == 'PENDING'

    def test_approved_and_terminal_rows_untouched(self, proposal_store):
        approved = _add(proposal_store, 'APR', age=10 * DAY)
        proposal_store.try_transition(approved, 'PENDING', 'APPROVED')
        _backdate(proposal_store, approved, 10 * DAY, 'updated_at')
        rejected = _add(proposal_store, 'REJ', age=10 * DAY)
        proposal_store.try_transition(rejected, 'PENDING', 'REJECTED')
        assert proposal_store.expire_stale(DAY) == []
        assert proposal_store.get(approved).status == 'APPROVED'
        assert proposal_store.get(rejected).status == 'REJECTED'
        # ...but a stale APPROVED claim is reported for the operator.
        assert [p.id for p in proposal_store.stale_approved(DAY)] == [approved]

    def test_concurrent_claim_wins_over_sweep(self, proposal_store):
        """A proposal claimed between the sweep's read and its CAS is skipped."""
        pid = _add(proposal_store, age=2 * DAY)
        original = proposal_store.try_transition

        def claim_first(id, from_status, to_status, **kw):
            if to_status == 'EXPIRED':
                original(id, 'PENDING', 'APPROVED')
            return original(id, from_status, to_status, **kw)

        proposal_store.try_transition = claim_first
        assert proposal_store.expire_stale(DAY) == []
        assert proposal_store.get(pid).status == 'APPROVED'


# --- sdk.approve / listing --------------------------------------------------------

class TestSdkApprove:
    def _mmr(self, tmp_duckdb_path, ttl_hours=24):
        from test_propose_approve_integration import _make_mmr, _StubRPCClient
        from trader.common.reactivex import SuccessFail
        rpc = _StubRPCClient(SuccessFail.success(obj=[]))
        mmr = _make_mmr(tmp_duckdb_path, rpc)
        mmr._container.config.return_value = {'duckdb_path': tmp_duckdb_path,
                                              'proposal_ttl_hours': ttl_hours}
        return mmr, rpc

    def test_stale_proposal_refused_and_expired_without_placing(self, tmp_duckdb_path):
        mmr, rpc = self._mmr(tmp_duckdb_path)
        store = mmr._proposal_store()
        pid = _add(store, age=35 * DAY)
        result = mmr.approve(pid)
        assert not result.is_success()
        assert 'expired' in result.error.lower()
        assert store.get(pid).status == 'EXPIRED'
        assert not [c for c in rpc.calls if c['method'].endswith('place_expressive_order')]

    def test_listing_expires_stale_rows(self, tmp_duckdb_path):
        mmr, _ = self._mmr(tmp_duckdb_path)
        store = mmr._proposal_store()
        old = _add(store, 'OLD', age=3 * DAY)
        fresh = _add(store, 'NEW')
        df = mmr.proposals(status='PENDING')
        assert list(df['id']) == [fresh]
        assert 'status' in df.columns
        assert store.get(old).status == 'EXPIRED'

    def test_expire_command_dry_run_and_override(self, tmp_duckdb_path):
        mmr, _ = self._mmr(tmp_duckdb_path)
        store = mmr._proposal_store()
        pid = _add(store, age=dt.timedelta(hours=3))
        assert mmr.expire_stale_proposals()['expired'] == []          # 3h < 24h
        res = mmr.expire_stale_proposals(older_than_hours=1, dry_run=True)
        assert [r['id'] for r in res['expired']] == [pid] and res['dry_run']
        assert store.get(pid).status == 'PENDING'
        res = mmr.expire_stale_proposals(older_than_hours=1)
        assert store.get(pid).status == 'EXPIRED'

    def test_disabled_ttl_allows_old_proposal_through_expiry_check(self, tmp_duckdb_path):
        mmr, _ = self._mmr(tmp_duckdb_path, ttl_hours=0)
        store = mmr._proposal_store()
        pid = _add(store, age=35 * DAY)
        mmr.approve(pid)
        assert store.get(pid).status != 'EXPIRED'


# --- server-side authorization ------------------------------------------------------

@pytest.mark.asyncio
async def test_server_refuses_to_open_on_expired_approved_proposal(tmp_path):
    from test_proposal_budget_match import _approving_trader, _submit
    trader = _approving_trader(tmp_path, last=100.0, ask=100.02, bid=99.98)
    trader.proposal_ttl = DAY
    store = ProposalStore(trader.duckdb_path)
    proposal = TradeProposal('AUDIT', 'BUY', quantity=5.0, metadata={'con_id': 100},
                             execution=ExecutionSpec())
    pid = store.add(proposal)
    store.try_transition(pid, 'PENDING', 'APPROVED')
    _backdate(store, pid, 2 * DAY)
    result = await _submit(trader, pid, proposal, 5)
    assert not result.is_success()
    assert not trader.placed


@pytest.mark.asyncio
async def test_server_admits_fresh_approved_proposal(tmp_path):
    from test_proposal_budget_match import _approving_trader, _submit
    trader = _approving_trader(tmp_path, last=100.0, ask=100.02, bid=99.98)
    trader.proposal_ttl = DAY
    store = ProposalStore(trader.duckdb_path)
    proposal = TradeProposal('AUDIT', 'BUY', quantity=5.0, metadata={'con_id': 100},
                             execution=ExecutionSpec())
    pid = store.add(proposal)
    store.try_transition(pid, 'PENDING', 'APPROVED')
    result = await _submit(trader, pid, proposal, 5)
    assert result.is_success(), result.error


def test_trader_sweep_expires_and_reports(tmp_path):
    from trader.trading.trading_runtime import Trader
    t = Trader.__new__(Trader)
    t.duckdb_path = str(tmp_path / 't.duckdb')
    t.proposal_ttl = DAY
    store = ProposalStore(t.duckdb_path)
    old = _add(store, age=3 * DAY)
    apr = _add(store, 'APR')
    store.try_transition(apr, 'PENDING', 'APPROVED')
    _backdate(store, apr, 3 * DAY, 'updated_at')
    res = t.sweep_stale_proposals()
    assert res == {'expired': [old], 'stale_approved': [apr]}
