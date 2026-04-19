"""End-to-end integration test for the propose → approve → execute pipeline.

Uses a real DuckDB-backed ProposalStore + EventStore, a stubbed RPC client
(so no trader_service needed), and exercises the full state machine including
failure paths.
"""

from dataclasses import dataclass
from typing import List
from unittest.mock import MagicMock

import pytest

from trader.common.reactivex import SuccessFail, SuccessFailEnum
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.data.proposal_store import InvalidProposalTransition, ProposalStore
from trader.sdk import MMR
from trader.trading.proposal import ExecutionSpec, TradeProposal
import datetime as _dt


# ---------------------------------------------------------------------------
# Minimal MMR shell
# ---------------------------------------------------------------------------

def _make_mmr(tmp_duckdb_path: str, rpc_client) -> MMR:
    """Build a just-enough MMR wired to real DuckDB stores and a stub RPC."""
    mmr = MMR.__new__(MMR)
    mmr._client = rpc_client
    mmr._data_client = None
    mmr._massive_rest_client = None
    mmr._rpc_address = 'tcp://127.0.0.1'
    mmr._rpc_port = 42001
    mmr._timeout = 5
    mmr._subscriptions = []
    mmr._position_map = {}
    mmr._contract_map = {}

    container = MagicMock()
    container.config_file = '/tmp/test_trader.yaml'
    container.config.return_value = {'duckdb_path': tmp_duckdb_path}
    mmr._container = container
    mmr._duckdb_path = tmp_duckdb_path

    # _proposal_store() and _group_store() in MMR look up duckdb_path from the
    # container; we pre-populate the singleton.
    return mmr


@dataclass
class _StubTrade:
    orderId: int


class _StubRPCClient:
    """Minimal RPC client shape used by MMR.approve().

    ``place_expressive_order_result`` lets each test choose success vs fail.
    """

    def __init__(self, place_expressive_order_result):
        self.is_setup = True
        self._result = place_expressive_order_result
        self.calls: List[dict] = []

    def rpc(self, return_type=None):
        outer = self

        class _Chain:
            def __init__(self, names=()):
                self._names = names

            def __getattr__(self, name):
                return _Chain(self._names + (name,))

            def __call__(self, *args, **kwargs):
                outer.calls.append({
                    'method': '.'.join(self._names),
                    'args': args,
                    'kwargs': kwargs,
                })
                method = self._names[-1] if self._names else ''
                if method == 'place_expressive_order':
                    return outer._result
                if method == 'resolve_symbol':
                    # Return an empty list (not found in local DB). MMR falls
                    # through to resolve_contract, which we also stub below.
                    return []
                if method == 'resolve_contract':
                    from trader.data.data_access import SecurityDefinition
                    import inspect as _inspect
                    sig = _inspect.signature(SecurityDefinition)
                    # Only pass required fields; rely on defaults for the rest
                    kwargs_init = {}
                    sym = kwargs.get('symbol') or (args[0].symbol if args and hasattr(args[0], 'symbol') else 'AMD')
                    defaults = {
                        'symbol': sym, 'exchange': 'SMART', 'conId': 1,
                        'secType': 'STK', 'primaryExchange': 'NASDAQ',
                        'currency': 'USD', 'localSymbol': sym, 'tradingClass': '',
                        'multiplier': '', 'strike': 0.0, 'right': '',
                        'lastTradeDateOrContractMonth': '', 'includeExpired': False,
                    }
                    for p in sig.parameters:
                        if p in defaults:
                            kwargs_init[p] = defaults[p]
                        elif sig.parameters[p].default is not _inspect.Parameter.empty:
                            continue
                        else:
                            # Required param we don't know — fall back to zero/empty
                            ann = sig.parameters[p].annotation
                            kwargs_init[p] = 0 if ann is int else (0.0 if ann is float else '')
                    try:
                        return [SecurityDefinition(**kwargs_init)]
                    except Exception:
                        return []
                if method == 'get_snapshot':
                    tkr = MagicMock()
                    tkr.last = 100.0
                    tkr.ask = 100.5
                    tkr.bid = 99.5
                    return tkr
                if method == 'get_account_values':
                    return {}
                return None

        return _Chain()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def _create_proposal(store: ProposalStore, symbol='AMD', quantity=10) -> int:
    return store.add(TradeProposal(
        symbol=symbol, action='BUY', quantity=float(quantity),
        execution=ExecutionSpec(order_type='MARKET'),
        reasoning='integration test',
    ))


class TestProposeApproveFlow:
    def test_happy_path_execute(self, tmp_duckdb_path):
        """Propose → Approve → EXECUTED with recorded order IDs."""
        store = ProposalStore(tmp_duckdb_path)
        pid = _create_proposal(store)

        rpc = _StubRPCClient(
            place_expressive_order_result=SuccessFail.success(obj=[_StubTrade(orderId=777)])
        )
        mmr = _make_mmr(tmp_duckdb_path, rpc)

        result = mmr.approve(pid)

        assert result.success_fail == SuccessFailEnum.SUCCESS
        stored = store.get(pid)
        assert stored.status == 'EXECUTED'
        assert 777 in stored.order_ids
        # place_expressive_order should have been called exactly once
        placed = [c for c in rpc.calls if c['method'] == 'place_expressive_order']
        assert len(placed) == 1

    def test_double_approve_rejected_by_state_machine(self, tmp_duckdb_path):
        """A proposal that's already executed can't be approved again."""
        store = ProposalStore(tmp_duckdb_path)
        pid = _create_proposal(store)

        rpc = _StubRPCClient(
            place_expressive_order_result=SuccessFail.success(obj=[_StubTrade(orderId=1)])
        )
        mmr = _make_mmr(tmp_duckdb_path, rpc)

        # First approval succeeds
        r1 = mmr.approve(pid)
        assert r1.success_fail == SuccessFailEnum.SUCCESS

        # Second approval: SDK short-circuits with a clear error because the
        # proposal isn't PENDING anymore, and the underlying store would also
        # refuse the transition.
        r2 = mmr.approve(pid)
        assert r2.success_fail == SuccessFailEnum.FAIL
        assert 'not PENDING' in (r2.error or '') or 'EXECUTED' in (r2.error or '')

    def test_reject_transitions_to_rejected(self, tmp_duckdb_path):
        store = ProposalStore(tmp_duckdb_path)
        pid = _create_proposal(store)

        rpc = _StubRPCClient(
            place_expressive_order_result=SuccessFail.success(obj=[])
        )
        mmr = _make_mmr(tmp_duckdb_path, rpc)

        ok = mmr.reject(pid, reason='changed thesis')
        assert ok is True
        stored = store.get(pid)
        assert stored.status == 'REJECTED'
        assert stored.rejection_reason == 'changed thesis'

    def test_rejected_cannot_be_approved(self, tmp_duckdb_path):
        """REJECTED is a terminal state — approve() must fail."""
        store = ProposalStore(tmp_duckdb_path)
        pid = _create_proposal(store)
        store.update_status(pid, 'REJECTED', rejection_reason='nope')

        rpc = _StubRPCClient(
            place_expressive_order_result=SuccessFail.success(obj=[])
        )
        mmr = _make_mmr(tmp_duckdb_path, rpc)

        result = mmr.approve(pid)
        assert result.success_fail == SuccessFailEnum.FAIL

    def test_place_expressive_order_failure_marks_proposal_failed(self, tmp_duckdb_path):
        """If trader_service refuses the order (e.g. bracket rollback), the
        proposal transitions APPROVED → FAILED, not stuck at APPROVED."""
        store = ProposalStore(tmp_duckdb_path)
        pid = _create_proposal(store)

        rpc = _StubRPCClient(
            place_expressive_order_result=SuccessFail.fail(error='bracket aborted: TP rejected')
        )
        mmr = _make_mmr(tmp_duckdb_path, rpc)

        result = mmr.approve(pid)
        assert result.success_fail == SuccessFailEnum.FAIL
        stored = store.get(pid)
        assert stored.status == 'FAILED', (
            f'expected FAILED, got {stored.status} — the approve flow should '
            'surface broker failures through the state machine'
        )

    def test_nonexistent_proposal_rejected(self, tmp_duckdb_path):
        store = ProposalStore(tmp_duckdb_path)  # noqa: F841 (creates schema)
        rpc = _StubRPCClient(
            place_expressive_order_result=SuccessFail.success(obj=[])
        )
        mmr = _make_mmr(tmp_duckdb_path, rpc)

        result = mmr.approve(99999)
        assert result.success_fail == SuccessFailEnum.FAIL
        assert 'not found' in (result.error or '').lower()


class TestEventStoreAuditTrail:
    """The atomic event_store writes should record SIGNAL → etc. entries and
    durably survive a read from a fresh connection."""

    def test_append_and_query_back_after_reconnect(self, tmp_duckdb_path):
        from trader.data.duckdb_store import DuckDBConnection

        store = EventStore(tmp_duckdb_path)
        store.append(TradingEvent(
            event_type=EventType.SIGNAL,
            timestamp=_dt.datetime.now(),
            strategy_name='integration',
            conid=1, symbol='AMD', action='BUY',
            signal_probability=0.8, signal_risk=0.1,
        ))

        # Simulate a fresh process: clear singleton, re-open, read.
        DuckDBConnection._instances.pop(tmp_duckdb_path, None)
        store2 = EventStore(tmp_duckdb_path)
        events = store2.query_all()
        assert len(events) == 1
        assert events[0].symbol == 'AMD'
        assert events[0].event_type == EventType.SIGNAL
