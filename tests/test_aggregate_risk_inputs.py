"""Concentration uses an evaluable aggregate, including already pending orders."""
import pytest

from trader.objects import Action
from trader.trading.strategy import Signal


def signal():
    return Signal(source_name='aggregate-risk-test', action=Action.BUY,
                  probability=0.8, risk=0.2, conid=4391)


@pytest.mark.parametrize('aggregate', [float('nan'), float('inf'), -float('inf'), -1.0])
def test_unreadable_aggregate_exposure_refuses_open_with_structured_reason(risk_gate, aggregate):
    result = risk_gate.evaluate(signal(), portfolio_value=100_000,
                                position_value=1_000, aggregate_position_value=aggregate)
    assert result.approved is False
    assert result.checks == {
        'max_open_orders': 'pass',
        'daily_loss': 'pass',
        'concentration': 'unevaluable:aggregate-position',
    }
    assert isinstance(result.reason, str)
    assert 'aggregate' in result.reason.lower()
    assert 'valued' in result.reason.lower()


@pytest.mark.parametrize('aggregate,approved', [
    (0.0, True), (9_999.0, True), (10_000.0, True), (10_000.01, False),
])
def test_aggregate_exposure_cap_includes_zero_and_exact_limit(risk_gate, aggregate, approved):
    result = risk_gate.evaluate(signal(), portfolio_value=100_000,
                                position_value=1_000, aggregate_position_value=aggregate)
    assert result.approved is approved
    assert result.checks['concentration'] == ('pass' if approved else 'fail')


def test_forex_concentration_exemption_does_not_require_an_aggregate_input(risk_gate):
    result = risk_gate.evaluate(signal(), portfolio_value=100_000,
                                position_value=1_000, aggregate_position_value=float('nan'),
                                sec_type='CASH')
    assert result.approved is True
    assert result.checks['concentration'] == 'skipped:forex-cash'
