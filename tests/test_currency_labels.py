"""`mmr session` and `mmr status` say which currency their amounts are in.

AUDIT_2026-11-14 B6. The account is CAD-based. `session` reported base-currency
amounts under `_usd` keys and `$` labels with no currency anywhere; `status`
summed each position's PnL (reported in the INSTRUMENT's currency) into one
unlabelled number, and its docstring called that base currency.

No key is renamed (consumers read `remaining_usd`, `amount_usd`, `pnl.daily`);
the currency is stated next to them.
"""
import io
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from rich.console import Console

from trader.sdk import MMR
from trader.trading.position_sizing import PositionSizingConfig


def _contract(symbol, currency, conid):
    return SimpleNamespace(conId=conid, symbol=symbol, localSymbol=symbol, secType='STK',
                           exchange='SMART', primaryExchange='', currency=currency)


def _summary(symbol, currency, conid, *, position=10.0, unrealized=0.0, realized=0.0, daily=0.0):
    return SimpleNamespace(contract=_contract(symbol, currency, conid), position=position,
                           marketPrice=100.0, marketValue=position * 100.0, averageCost=100.0,
                           unrealizedPNL=unrealized, realizedPNL=realized, account='U1',
                           dailyPNL=daily)


def _mmr(summaries, account_values):
    client = MagicMock()
    client.is_setup = True
    rpc = client.rpc.return_value
    if isinstance(account_values, Exception):
        rpc.get_account_values.side_effect = account_values
    else:
        rpc.get_account_values.return_value = account_values
    rpc.get_portfolio_summary.return_value = summaries
    rpc.get_ib_account.return_value = 'U1'
    rpc.get_status.return_value = {'ib_upstream_connected': True}
    rpc.get_trades.return_value = {}
    rpc.get_published_contracts.return_value = []
    mmr = MMR.__new__(MMR)
    mmr._client = client
    mmr._position_map, mmr._contract_map = {}, {}
    mmr._container = MagicMock()
    store = MagicMock()
    store.query.return_value = []
    mmr._prop_store = store
    return mmr


CAD_ACCOUNT = {
    'NetLiquidation': {'value': 100_000.0, 'currency': 'CAD'},
    'GrossPositionValue': {'value': 20_000.0, 'currency': 'CAD'},
    'AvailableFunds': {'value': 60_000.0, 'currency': 'CAD'},
}


@pytest.fixture(autouse=True)
def _default_sizing_config(monkeypatch):
    # Never read the operator's ~/.config/mmr/position_sizing.yaml.
    monkeypatch.setattr(PositionSizingConfig, 'load', classmethod(lambda cls, *a, **k: cls()))
    monkeypatch.setattr('trader.sdk._gate_daily_loss_limit', lambda: None)


# ---------------------------------------------------------------------------
# session
# ---------------------------------------------------------------------------

def test_session_states_the_base_currency_next_to_every_usd_key():
    mmr = _mmr([_summary('AAPL', 'USD', 1, daily=-40.0)], CAD_ACCOUNT)
    summary = mmr.session_status()

    assert summary['base_currency'] == 'CAD'
    for section in ('config', 'portfolio', 'capacity'):
        assert summary[section]['currency'] == 'CAD', section
    for size in summary['recommended_sizes'].values():
        assert size['currency'] == 'CAD'
    assert 'CAD' in summary['currency_note'] and '_usd' in summary['currency_note']
    # Keys consumers depend on are still there, unrenamed.
    assert 'remaining_usd' in summary['capacity']
    assert 'amount_usd' in summary['recommended_sizes']['high_confidence']
    assert 'base_position_usd' in summary['config']


def test_session_daily_pnl_names_its_own_currency():
    # daily_pnl is a sum of per-position PnL in each INSTRUMENT's currency; it
    # is not the base-currency figure the rest of the block is.
    mmr = _mmr([_summary('AAPL', 'USD', 1, daily=-40.0),
                _summary('MSFT', 'USD', 2, daily=15.0)], CAD_ACCOUNT)
    portfolio = mmr.session_status()['portfolio']
    assert portfolio['daily_pnl'] == pytest.approx(-25.0)
    assert portfolio['daily_pnl_currency'] == 'USD'
    assert portfolio['daily_pnl_currencies'] == ['USD']


def test_session_daily_pnl_across_currencies_is_labelled_mixed():
    mmr = _mmr([_summary('AAPL', 'USD', 1, daily=-40.0),
                _summary('BHP', 'AUD', 2, daily=15.0)], CAD_ACCOUNT)
    portfolio = mmr.session_status()['portfolio']
    assert portfolio['daily_pnl_currency'] == 'MIXED'
    assert portfolio['daily_pnl_currencies'] == ['AUD', 'USD']


def test_session_without_a_readable_account_does_not_invent_a_currency():
    mmr = _mmr([], TimeoutError('down'))
    summary = mmr.session_status()
    assert summary['base_currency'] is None
    assert summary['capacity']['currency'] is None
    assert 'not read' in summary['currency_note']


def _render_session(monkeypatch, summary):
    from trader import mmr_cli
    buffer = io.StringIO()
    monkeypatch.setattr(mmr_cli, 'console', Console(file=buffer, width=200, force_terminal=False))
    monkeypatch.setattr(mmr_cli, '_json_mode', False)
    fake = SimpleNamespace(session_status=lambda: summary)
    mmr_cli._handle_session(fake, SimpleNamespace(session_action=None))
    return buffer.getvalue()


def test_session_table_labels_amounts_with_the_base_currency(monkeypatch):
    mmr = _mmr([_summary('AAPL', 'USD', 1, daily=-40.0),
                _summary('BHP', 'AUD', 2, daily=15.0)], CAD_ACCOUNT)
    text = _render_session(monkeypatch, mmr.session_status())

    def row(label):
        return next(line for line in text.splitlines() if label in line)

    for label in ('Net Liquidation', 'Position Value', 'Available Funds', 'Remaining Budget',
                  'Min Position', 'Max Position ', 'Daily Loss Limit', 'High (0.9)'):
        assert 'CAD' in row(label), row(label)
    daily = row('Daily P&L')
    assert 'CAD' not in daily
    assert 'AUD' in daily and 'USD' in daily and 'not converted' in daily


def test_session_table_without_a_currency_says_so(monkeypatch):
    mmr = _mmr([], TimeoutError('down'))
    text = _render_session(monkeypatch, mmr.session_status())
    assert 'account base currency' in text


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

def test_status_pnl_in_one_currency_names_it():
    mmr = _mmr([_summary('AAPL', 'USD', 1, unrealized=100.0, realized=5.0, daily=-40.0),
                _summary('MSFT', 'USD', 2, unrealized=-30.0, realized=0.0, daily=10.0)],
               CAD_ACCOUNT)
    pnl = mmr.status()['pnl']
    # Existing keys and values are untouched.
    assert pnl['daily'] == pytest.approx(-30.0)
    assert pnl['unrealized'] == pytest.approx(70.0)
    assert pnl['realized'] == pytest.approx(5.0)
    assert pnl['total'] == pytest.approx(75.0)
    assert pnl['currency'] == 'USD'
    assert pnl['currencies'] == ['USD']
    assert pnl['mixed_currencies'] is False


def test_status_pnl_across_currencies_says_it_mixes_them():
    mmr = _mmr([_summary('AAPL', 'USD', 1, unrealized=100.0, realized=5.0, daily=-40.0),
                _summary('BHP', 'AUD', 2, unrealized=-30.0, realized=0.0, daily=10.0)],
               CAD_ACCOUNT)
    pnl = mmr.status()['pnl']
    assert pnl['daily'] == pytest.approx(-30.0)          # the unconverted sum, as before
    assert pnl['currency'] == 'MIXED'
    assert pnl['mixed_currencies'] is True
    assert pnl['currencies'] == ['AUD', 'USD']
    assert pnl['by_currency']['USD'] == {
        'daily': pytest.approx(-40.0), 'unrealized': pytest.approx(100.0),
        'realized': pytest.approx(5.0), 'total': pytest.approx(105.0)}
    assert pnl['by_currency']['AUD']['daily'] == pytest.approx(10.0)
    assert 'not converted' in pnl['note']


def test_status_pnl_with_no_positions_has_no_currency():
    pnl = _mmr([], CAD_ACCOUNT).status()['pnl']
    assert pnl['daily'] == 0 and pnl['currency'] is None and pnl['currencies'] == []
    assert pnl['mixed_currencies'] is False


def test_status_pnl_is_not_documented_as_base_currency():
    assert 'in account base currency' not in (MMR.status.__doc__ or '')


def test_status_display_labels_single_currency_pnl():
    from trader.mmr_cli import _status_pnl_display
    display = _status_pnl_display({
        'daily': -30.0, 'unrealized': 70.0, 'realized': 5.0, 'total': 75.0,
        'currency': 'USD', 'currencies': ['USD'], 'mixed_currencies': False,
        'by_currency': {'USD': {'daily': -30.0, 'unrealized': 70.0, 'realized': 5.0, 'total': 75.0}}})
    assert set(display) == {'DailyPnL', 'UnrealizedPnL', 'RealizedPnL', 'TotalPnL'}
    assert display['DailyPnL'].endswith(' USD') and '$30.00' in display['DailyPnL']


def test_status_display_shows_mixed_pnl_per_currency_not_as_one_sum():
    from trader.mmr_cli import _status_pnl_display
    display = _status_pnl_display({
        'daily': -30.0, 'unrealized': 70.0, 'realized': 5.0, 'total': 75.0,
        'currency': 'MIXED', 'currencies': ['AUD', 'USD'], 'mixed_currencies': True,
        'by_currency': {
            'USD': {'daily': -40.0, 'unrealized': 100.0, 'realized': 5.0, 'total': 105.0},
            'AUD': {'daily': 10.0, 'unrealized': -30.0, 'realized': 0.0, 'total': -30.0}}})
    daily = display['DailyPnL']
    assert '$40.00' in daily and 'USD' in daily and '$10.00' in daily and 'AUD' in daily
    assert '$30.00' not in daily            # the cross-currency sum is not shown


def test_status_display_tolerates_a_pnl_block_without_currency_fields():
    # An older trader/sdk on the other end of the proxy: label nothing rather
    # than guess.
    from trader.mmr_cli import _status_pnl_display
    display = _status_pnl_display({'daily': 1.0, 'unrealized': 2.0, 'realized': 3.0, 'total': 5.0})
    assert '$1.00' in display['DailyPnL']
