import datetime as dt
import pytest
from trader.data.proposal_store import ProposalStore
from trader.trading.proposal import ExecutionSpec, TradeProposal


def _make_proposal(
    symbol='AMD',
    action='BUY',
    quantity=100.0,
    execution=None,
    reasoning='Test reasoning',
    source='manual',
):
    return TradeProposal(
        symbol=symbol,
        action=action,
        quantity=quantity,
        execution=execution or ExecutionSpec(),
        reasoning=reasoning,
        source=source,
    )


class TestProposalStore:
    def test_table_creation(self, proposal_store):
        assert proposal_store is not None

    def test_add_returns_id(self, proposal_store):
        pid = proposal_store.add(_make_proposal())
        assert pid == 1

    def test_add_auto_increments(self, proposal_store):
        id1 = proposal_store.add(_make_proposal())
        id2 = proposal_store.add(_make_proposal(symbol='AAPL'))
        id3 = proposal_store.add(_make_proposal(symbol='MSFT'))
        assert id1 == 1
        assert id2 == 2
        assert id3 == 3

    def test_get_retrieves_all_fields(self, proposal_store):
        spec = ExecutionSpec(
            order_type='LIMIT',
            limit_price=165.0,
            exit_type='BRACKET',
            take_profit_price=180.0,
            stop_loss_price=150.0,
            tif='GTC',
            outside_rth=True,
        )
        proposal = TradeProposal(
            symbol='AMD',
            action='BUY',
            quantity=100.0,
            amount=16500.0,
            execution=spec,
            reasoning='Breakout play',
            confidence=0.85,
            thesis='AMD momentum',
            source='llm',
            metadata={'key': 'value'},
            sec_type='STK',
        )
        pid = proposal_store.add(proposal)
        result = proposal_store.get(pid)

        assert result is not None
        assert result.id == pid
        assert result.symbol == 'AMD'
        assert result.action == 'BUY'
        assert result.quantity == 100.0
        assert result.amount == 16500.0
        assert result.execution.order_type == 'LIMIT'
        assert result.execution.limit_price == 165.0
        assert result.execution.exit_type == 'BRACKET'
        assert result.execution.take_profit_price == 180.0
        assert result.execution.stop_loss_price == 150.0
        assert result.execution.tif == 'GTC'
        assert result.execution.outside_rth is True
        assert result.reasoning == 'Breakout play'
        assert result.confidence == 0.85
        assert result.thesis == 'AMD momentum'
        assert result.source == 'llm'
        assert result.metadata == {'key': 'value'}
        assert result.status == 'PENDING'
        assert result.sec_type == 'STK'
        assert result.created_at is not None
        assert result.updated_at is not None

    def test_get_nonexistent_returns_none(self, proposal_store):
        assert proposal_store.get(999) is None

    def test_query_all(self, proposal_store):
        proposal_store.add(_make_proposal(symbol='AMD'))
        proposal_store.add(_make_proposal(symbol='AAPL'))
        proposal_store.add(_make_proposal(symbol='MSFT'))
        results = proposal_store.query()
        assert len(results) == 3

    def test_query_by_status(self, proposal_store):
        pid1 = proposal_store.add(_make_proposal(symbol='AMD'))
        pid2 = proposal_store.add(_make_proposal(symbol='AAPL'))
        proposal_store.update_status(pid1, 'APPROVED')

        pending = proposal_store.query(status='PENDING')
        assert len(pending) == 1
        assert pending[0].symbol == 'AAPL'

        approved = proposal_store.query(status='APPROVED')
        assert len(approved) == 1
        assert approved[0].symbol == 'AMD'

    def test_query_limit(self, proposal_store):
        for i in range(10):
            proposal_store.add(_make_proposal(symbol=f'SYM{i}'))
        results = proposal_store.query(limit=3)
        assert len(results) == 3

    def test_query_ordering_desc(self, proposal_store):
        pid1 = proposal_store.add(_make_proposal(symbol='FIRST'))
        pid2 = proposal_store.add(_make_proposal(symbol='SECOND'))
        results = proposal_store.query()
        # Most recent first
        assert results[0].symbol == 'SECOND'
        assert results[1].symbol == 'FIRST'

    def test_update_status(self, proposal_store):
        pid = proposal_store.add(_make_proposal())
        proposal_store.update_status(pid, 'APPROVED')
        p = proposal_store.get(pid)
        assert p.status == 'APPROVED'

    def test_update_status_with_order_ids(self, proposal_store):
        pid = proposal_store.add(_make_proposal())
        proposal_store.update_status(pid, 'EXECUTED', order_ids=[101, 102, 103])
        p = proposal_store.get(pid)
        assert p.status == 'EXECUTED'
        assert p.order_ids == [101, 102, 103]

    def test_update_status_with_rejection_reason(self, proposal_store):
        pid = proposal_store.add(_make_proposal())
        proposal_store.update_status(pid, 'REJECTED', rejection_reason='Changed thesis')
        p = proposal_store.get(pid)
        assert p.status == 'REJECTED'
        assert p.rejection_reason == 'Changed thesis'

    def test_status_transitions(self, proposal_store):
        pid = proposal_store.add(_make_proposal())
        assert proposal_store.get(pid).status == 'PENDING'

        proposal_store.update_status(pid, 'APPROVED')
        assert proposal_store.get(pid).status == 'APPROVED'

        proposal_store.update_status(pid, 'EXECUTED', order_ids=[42])
        p = proposal_store.get(pid)
        assert p.status == 'EXECUTED'
        assert p.order_ids == [42]

    def test_delete(self, proposal_store):
        pid = proposal_store.add(_make_proposal())
        assert proposal_store.get(pid) is not None
        result = proposal_store.delete(pid)
        assert result is True
        assert proposal_store.get(pid) is None

    def test_delete_nonexistent(self, proposal_store):
        result = proposal_store.delete(999)
        assert result is True  # no row to delete, but query returns 0 count

    def test_json_round_trip_execution(self, proposal_store):
        """Verify ExecutionSpec survives JSON serialization in DuckDB."""
        spec = ExecutionSpec(
            order_type='LIMIT',
            limit_price=250.0,
            exit_type='TRAILING_STOP',
            trailing_stop_percent=2.5,
            tif='GTC',
            outside_rth=True,
        )
        pid = proposal_store.add(TradeProposal(
            symbol='NVDA',
            action='BUY',
            quantity=50,
            execution=spec,
        ))
        result = proposal_store.get(pid)
        assert result.execution.order_type == 'LIMIT'
        assert result.execution.limit_price == 250.0
        assert result.execution.exit_type == 'TRAILING_STOP'
        assert result.execution.trailing_stop_percent == 2.5
        assert result.execution.tif == 'GTC'
        assert result.execution.outside_rth is True

    def test_json_round_trip_metadata(self, proposal_store):
        """Verify metadata dict survives JSON serialization in DuckDB."""
        meta = {
            'scanner_preset': 'momentum',
            'indicators': {'rsi': 72.5, 'macd_signal': True},
            'news': 'Earnings beat',
        }
        pid = proposal_store.add(TradeProposal(
            symbol='AAPL',
            action='BUY',
            quantity=10,
            metadata=meta,
        ))
        result = proposal_store.get(pid)
        assert result.metadata == meta
        assert result.metadata['indicators']['rsi'] == 72.5

    def test_update_metadata_merges(self, proposal_store):
        """update_metadata merges new keys into existing metadata."""
        pid = proposal_store.add(TradeProposal(
            symbol='AMD',
            action='BUY',
            quantity=100,
            metadata={'original_key': 'original_value'},
        ))
        leverage_info = {
            'current_leverage': 1.12,
            'estimated_leverage': 1.45,
            'net_liquidation': 987654.0,
            'buying_power': 523450.0,
            'uses_margin': True,
        }
        proposal_store.update_metadata(pid, {'leverage_estimate': leverage_info})
        result = proposal_store.get(pid)
        assert result.metadata['original_key'] == 'original_value'
        assert result.metadata['leverage_estimate']['current_leverage'] == 1.12
        assert result.metadata['leverage_estimate']['uses_margin'] is True

    def test_update_metadata_nonexistent(self, proposal_store):
        """update_metadata on non-existent proposal does nothing."""
        proposal_store.update_metadata(999, {'key': 'value'})  # should not raise

    def test_updated_at_changes_on_status_update(self, proposal_store):
        pid = proposal_store.add(_make_proposal())
        p1 = proposal_store.get(pid)

        import time
        time.sleep(0.01)

        proposal_store.update_status(pid, 'APPROVED')
        p2 = proposal_store.get(pid)
        assert p2.updated_at >= p1.updated_at
