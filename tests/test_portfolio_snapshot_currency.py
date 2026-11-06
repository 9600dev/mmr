"""portfolio-snapshot totals are in the account base currency (2026-11-05).

CAD-base paper account holding USD stocks: total_value summed USD market values
and divided by CAD net liquidation (exposure 13.7% vs the true 19.2%), and
per-position change_pct divided by today's value instead of the prior value.
"""
import pytest

from test_sdk import FakeContract, FakePortfolioSummary, _make_mmr_with_mock, _make_mock_rpc


def _mmr(fx, net_liq=100_000.0, ccy='CAD', positions=None):
    client = _make_mock_rpc()
    positions = positions or [('AAA', 'USD', 1000.0, -100.0), ('BBB', 'CAD', 2000.0, 50.0)]
    client.rpc.return_value.get_portfolio_summary.return_value = [
        FakePortfolioSummary(contract=FakeContract(conId=i + 1, symbol=s, localSymbol=s, currency=c),
                             position=10, marketPrice=mv / 10, marketValue=mv, averageCost=1.0,
                             unrealizedPNL=0.0, realizedPNL=0.0, account='DU1', dailyPNL=dp)
        for i, (s, c, mv, dp) in enumerate(positions)]
    client.rpc.return_value.get_account_values.return_value = {
        'NetLiquidation': {'value': net_liq, 'currency': ccy}}
    if isinstance(fx, Exception):
        client.rpc.return_value.get_fx_rates.side_effect = fx
    else:
        client.rpc.return_value.get_fx_rates.return_value = fx
    return _make_mmr_with_mock(client)


def test_totals_converted_to_base_currency():
    snap = _mmr({'CAD': 1.0, 'USD': 1.4}).portfolio_snapshot()
    assert snap['currency'] == 'CAD'
    assert snap['total_value'] == pytest.approx(1000 * 1.4 + 2000)
    assert snap['daily_pnl'] == pytest.approx(-100 * 1.4 + 50)
    assert snap['exposure_pct'] == pytest.approx((1400 + 2000) / 100_000, abs=1e-4)
    by = {m['symbol']: m for m in snap['movers']}
    assert by['AAA']['currency'] == 'USD' and by['AAA']['value'] == 1000.0


def test_change_pct_uses_prior_value():
    snap = _mmr({'CAD': 1.0, 'USD': 1.4}).portfolio_snapshot()
    by = {m['symbol']: m for m in snap['movers']}
    assert by['AAA']['change_pct'] == pytest.approx(-100 / 1100, abs=1e-4)


def test_missing_fx_rate_omits_totals_with_warning():
    snap = _mmr({'CAD': 1.0}).portfolio_snapshot()          # no USD rate
    assert snap['total_value'] is None and snap['exposure_pct'] is None
    assert any('USD' in w for w in snap['warnings'])


def test_fx_rpc_failure_omits_totals_with_warning():
    snap = _mmr(TimeoutError('boom')).portfolio_snapshot()
    assert snap['exposure_pct'] is None and snap['warnings']
