"""Exchange test securities must not enter the research liquidity ranking."""
import pytest

from trader.data.bar_quality import is_test_ticker


@pytest.mark.parametrize('symbol', ['ZWZZT', 'ZVZZT', 'IBM.TEST', 'TESTA'])
def test_known_exchange_test_securities_are_excluded(symbol):
    assert is_test_ticker(symbol) is True


@pytest.mark.parametrize('symbol', [' zwzzt ', '\tzvZzT\n', ' ibm.test '])
def test_exchange_test_security_matching_normalizes_case_and_whitespace(symbol):
    assert is_test_ticker(symbol) is True


@pytest.mark.parametrize('symbol', [
    'AAPL', 'AMD', 'IBM', 'ZTS', 'ZS', 'ZWZZTA', 'XZWZZT', 'TEST',
])
def test_real_symbols_and_near_matches_are_not_excluded(symbol):
    assert is_test_ticker(symbol) is False
