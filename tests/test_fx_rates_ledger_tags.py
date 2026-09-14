"""fx_rates_to_base must read the account's real FX rows (review 2026-09-11).

reqAccountUpdates delivers per-currency FX as ``$LEDGER-ExchangeRate`` rows;
only some account types expose the plain ``ExchangeRate`` tag. Matching the
plain tag alone returned ``{base: 1.0}`` on a live ledger account, which made
every non-base position value non-evaluable and refused every non-base open
fail-closed (and broke amount-based sizing with "FX rate unavailable").
"""
from types import SimpleNamespace

import pytest

from trader.trading.trading_runtime import Trader

ACCOUNT = 'U26774889'


def _av(tag, value, currency, account=ACCOUNT):
    return SimpleNamespace(account=account, tag=tag, value=value, currency=currency)


def _trader(values):
    trader = object.__new__(Trader)
    trader.ib_account = ACCOUNT
    trader.client = SimpleNamespace(ib=SimpleNamespace(accountValues=lambda: values))
    return trader


def test_ledger_exchange_rate_rows_are_read():
    """The real shape of a ledger account (see test_trading_runtime's cash tests)."""
    trader = _trader([
        _av('NetLiquidation', '17005', 'CAD'),
        _av('$LEDGER-CashBalance', '17005', 'BASE'),
        _av('$LEDGER-ExchangeRate', '0.9808467', 'AUD'),
        _av('$LEDGER-ExchangeRate', '1.00', 'CAD'),
        _av('$LEDGER-ExchangeRate', '1.4202328', 'USD'),
    ])
    assert trader.fx_rates_to_base() == pytest.approx({'AUD': 0.9808467, 'CAD': 1.0, 'USD': 1.4202328})
    assert trader.convert_notional(1000.0, 'USD') == pytest.approx(1420.2328)
    assert trader.convert_notional(1000.0, 'AUD') == pytest.approx(980.8467)
    assert trader.convert_notional(1000.0, 'CAD') == pytest.approx(1000.0)


def test_ledger_form_wins_over_plain_and_bad_rows_are_dropped():
    trader = _trader([
        _av('NetLiquidation', '100000', 'USD'),
        _av('ExchangeRate', '0.5', 'JPY'),
        _av('$LEDGER-ExchangeRate', '0.01', 'JPY'),
        _av('$LEDGER-ExchangeRate', '99', 'JPY', account='OTHER_ACCOUNT'),
        _av('$LEDGER-ExchangeRate', 'nan', 'GBP'),
        _av('$LEDGER-ExchangeRate', '-1', 'EUR'),
        _av('$LEDGER-ExchangeRate', '7', 'BASE'),
    ])
    assert trader.fx_rates_to_base() == pytest.approx({'USD': 1.0, 'JPY': 0.01})
    assert trader.convert_notional(100.0, 'GBP') is None, 'missing FX is never an implicit rate of one'
    assert trader.convert_notional(100.0, 'EUR') is None


def test_plain_exchange_rate_rows_still_work():
    trader = _trader([
        _av('NetLiquidation', '1000', 'USD'),
        _av('ExchangeRate', '0.01', 'JPY'),
    ])
    assert trader.fx_rates_to_base() == pytest.approx({'USD': 1.0, 'JPY': 0.01})
    assert trader.convert_notional(1000.0, 'JPY') == pytest.approx(10.0)
