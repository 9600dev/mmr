import pytest
from trader.trading.proposal import (
    ExecutionSpec,
    ExitType,
    OrderType,
    ProposalStatus,
    TradeProposal,
)


class TestExecutionSpec:
    def test_defaults(self):
        spec = ExecutionSpec()
        assert spec.order_type == 'MARKET'
        assert spec.limit_price is None
        assert spec.exit_type == 'NONE'
        assert spec.tif == 'DAY'
        assert spec.outside_rth is True

    def test_to_dict_omits_none(self):
        spec = ExecutionSpec()
        d = spec.to_dict()
        assert 'limit_price' not in d
        assert d['order_type'] == 'MARKET'
        assert d['exit_type'] == 'NONE'
        assert d['tif'] == 'DAY'
        assert d['outside_rth'] is True

    def test_to_dict_includes_set_values(self):
        spec = ExecutionSpec(
            order_type='LIMIT',
            limit_price=165.0,
            exit_type='BRACKET',
            take_profit_price=180.0,
            stop_loss_price=150.0,
            tif='GTC',
        )
        d = spec.to_dict()
        assert d['order_type'] == 'LIMIT'
        assert d['limit_price'] == 165.0
        assert d['take_profit_price'] == 180.0
        assert d['stop_loss_price'] == 150.0
        assert d['tif'] == 'GTC'

    def test_from_dict_round_trip(self):
        spec = ExecutionSpec(
            order_type='LIMIT',
            limit_price=165.0,
            exit_type='BRACKET',
            take_profit_price=180.0,
            stop_loss_price=150.0,
            tif='GTC',
            outside_rth=True,
        )
        d = spec.to_dict()
        restored = ExecutionSpec.from_dict(d)
        assert restored.order_type == spec.order_type
        assert restored.limit_price == spec.limit_price
        assert restored.exit_type == spec.exit_type
        assert restored.take_profit_price == spec.take_profit_price
        assert restored.stop_loss_price == spec.stop_loss_price
        assert restored.tif == spec.tif
        assert restored.outside_rth == spec.outside_rth

    def test_from_dict_ignores_unknown_keys(self):
        d = {'order_type': 'MARKET', 'unknown_field': 42}
        spec = ExecutionSpec.from_dict(d)
        assert spec.order_type == 'MARKET'

    def test_validate_market_no_errors(self):
        spec = ExecutionSpec(order_type='MARKET', exit_type='NONE')
        assert spec.validate() == []

    def test_validate_limit_requires_price(self):
        spec = ExecutionSpec(order_type='LIMIT')
        errors = spec.validate()
        assert any('limit_price' in e for e in errors)

    def test_validate_bracket_requires_both_prices(self):
        spec = ExecutionSpec(exit_type='BRACKET')
        errors = spec.validate()
        assert any('take_profit_price' in e for e in errors)
        assert any('stop_loss_price' in e for e in errors)

    def test_validate_bracket_with_prices_ok(self):
        spec = ExecutionSpec(exit_type='BRACKET', take_profit_price=180.0, stop_loss_price=150.0)
        assert spec.validate() == []

    def test_validate_stop_loss_requires_price(self):
        spec = ExecutionSpec(exit_type='STOP_LOSS')
        errors = spec.validate()
        assert any('stop_loss_price' in e for e in errors)

    def test_validate_trailing_stop_requires_pct_or_amt(self):
        spec = ExecutionSpec(exit_type='TRAILING_STOP')
        errors = spec.validate()
        assert any('trailing_stop' in e for e in errors)

    def test_validate_trailing_stop_with_pct_ok(self):
        spec = ExecutionSpec(exit_type='TRAILING_STOP', trailing_stop_percent=2.0)
        assert spec.validate() == []

    def test_validate_trailing_stop_with_amt_ok(self):
        spec = ExecutionSpec(exit_type='TRAILING_STOP', trailing_stop_amount=5.0)
        assert spec.validate() == []


class TestTradeProposal:
    def test_creation_minimal(self):
        p = TradeProposal(symbol='AMD', action='BUY')
        assert p.symbol == 'AMD'
        assert p.action == 'BUY'
        assert p.status == 'PENDING'
        assert p.quantity is None
        assert p.order_ids == []

    def test_creation_full(self):
        spec = ExecutionSpec(
            order_type='LIMIT',
            limit_price=165.0,
            exit_type='BRACKET',
            take_profit_price=180.0,
            stop_loss_price=150.0,
        )
        p = TradeProposal(
            symbol='AMD',
            action='BUY',
            quantity=100,
            execution=spec,
            reasoning='Breakout above resistance',
            confidence=0.8,
            thesis='AMD momentum',
            source='llm',
            metadata={'scanner': 'ideas'},
            sec_type='STK',
        )
        assert p.quantity == 100
        assert p.execution.limit_price == 165.0
        assert p.reasoning == 'Breakout above resistance'
        assert p.confidence == 0.8
        assert p.source == 'llm'
        assert p.metadata == {'scanner': 'ideas'}

    def test_default_execution_spec(self):
        p = TradeProposal(symbol='AAPL', action='SELL')
        assert p.execution.order_type == 'MARKET'
        assert p.execution.exit_type == 'NONE'


class TestLeverageMetadata:
    def test_leverage_estimate_in_metadata(self):
        """Leverage estimate stored in metadata survives round-trip."""
        leverage_info = {
            'current_leverage': 1.12,
            'estimated_leverage': 1.45,
            'net_liquidation': 987654.0,
            'buying_power': 523450.0,
            'uses_margin': True,
        }
        p = TradeProposal(
            symbol='AMD',
            action='BUY',
            quantity=100,
            metadata={'leverage_estimate': leverage_info},
        )
        assert p.metadata['leverage_estimate']['current_leverage'] == 1.12
        assert p.metadata['leverage_estimate']['estimated_leverage'] == 1.45
        assert p.metadata['leverage_estimate']['uses_margin'] is True

    def test_leverage_metadata_absent(self):
        """Proposal without leverage metadata works fine."""
        p = TradeProposal(symbol='AAPL', action='SELL')
        assert 'leverage_estimate' not in p.metadata


class TestEnums:
    def test_proposal_status_values(self):
        assert ProposalStatus.PENDING == 'PENDING'
        assert ProposalStatus.EXECUTED == 'EXECUTED'
        assert ProposalStatus.REJECTED == 'REJECTED'

    def test_order_type_values(self):
        assert OrderType.MARKET == 'MARKET'
        assert OrderType.LIMIT == 'LIMIT'

    def test_exit_type_values(self):
        assert ExitType.NONE == 'NONE'
        assert ExitType.BRACKET == 'BRACKET'
        assert ExitType.STOP_LOSS == 'STOP_LOSS'
        assert ExitType.TRAILING_STOP == 'TRAILING_STOP'
