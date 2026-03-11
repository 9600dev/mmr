"""Tests for options support — SDK helpers, chain math, executioner multiplier, CLI parser."""

import os
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from trader.sdk import MMR
from trader.common.reactivex import SuccessFail


# ---------------------------------------------------------------------------
# Helpers (shared with test_sdk.py pattern)
# ---------------------------------------------------------------------------

def _make_mock_rpc():
    client = MagicMock()
    client.is_setup = True
    return client


def _make_mmr_with_mock(mock_client) -> MMR:
    mmr = MMR.__new__(MMR)
    mmr._client = mock_client
    mmr._data_client = None
    mmr._massive_rest_client = None
    mmr._rpc_address = 'tcp://127.0.0.1'
    mmr._rpc_port = 42001
    mmr._pubsub_address = 'tcp://127.0.0.1'
    mmr._pubsub_port = 42002
    mmr._timeout = 5
    mmr._subscriptions = []
    mmr._position_map = {}
    mmr._container = MagicMock()
    mmr._container.config.return_value = {'massive_api_key': 'test_key'}
    mmr._container.config_file = '/tmp/test_trader.yaml'
    return mmr


class FakeSecurityDefinition:
    def __init__(self, symbol='AAPL', conId=265598, secType='OPT',
                 exchange='SMART', primaryExchange='NASDAQ', currency='USD'):
        self.symbol = symbol
        self.conId = conId
        self.secType = secType
        self.exchange = exchange
        self.primaryExchange = primaryExchange
        self.currency = currency
        self.longName = f'{symbol} Inc.'


# ---------------------------------------------------------------------------
# Ticker Parsing / Building
# ---------------------------------------------------------------------------

class TestOptionTickerParsing:
    def test_parse_standard_ticker(self):
        parsed = MMR._parse_massive_option_ticker('O:AAPL260320C00250000')
        assert parsed['symbol'] == 'AAPL'
        assert parsed['expiration'] == '2026-03-20'
        assert parsed['right'] == 'C'
        assert parsed['strike'] == 250.0

    def test_parse_put_ticker(self):
        parsed = MMR._parse_massive_option_ticker('O:SPY260619P00520500')
        assert parsed['symbol'] == 'SPY'
        assert parsed['expiration'] == '2026-06-19'
        assert parsed['right'] == 'P'
        assert parsed['strike'] == 520.5

    def test_parse_without_prefix(self):
        parsed = MMR._parse_massive_option_ticker('AAPL260320C00250000')
        assert parsed['symbol'] == 'AAPL'
        assert parsed['strike'] == 250.0

    def test_parse_small_strike(self):
        parsed = MMR._parse_massive_option_ticker('O:F260320C00012500')
        assert parsed['symbol'] == 'F'
        assert parsed['strike'] == 12.5

    def test_parse_invalid_ticker(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            MMR._parse_massive_option_ticker('O:X')

    def test_build_standard_ticker(self):
        ticker = MMR._build_massive_option_ticker('AAPL', '2026-03-20', 250.0, 'C')
        assert ticker == 'O:AAPL260320C00250000'

    def test_build_put_ticker(self):
        ticker = MMR._build_massive_option_ticker('SPY', '2026-06-19', 520.5, 'P')
        assert ticker == 'O:SPY260619P00520500'

    def test_build_lowercase_right_normalized(self):
        ticker = MMR._build_massive_option_ticker('AAPL', '2026-03-20', 250.0, 'c')
        assert 'C' in ticker

    def test_roundtrip(self):
        original = 'O:AAPL260320C00250000'
        parsed = MMR._parse_massive_option_ticker(original)
        rebuilt = MMR._build_massive_option_ticker(
            parsed['symbol'], parsed['expiration'], parsed['strike'], parsed['right']
        )
        assert rebuilt == original

    def test_roundtrip_fractional_strike(self):
        ticker = MMR._build_massive_option_ticker('TSLA', '2026-12-18', 175.5, 'P')
        parsed = MMR._parse_massive_option_ticker(ticker)
        assert parsed['strike'] == 175.5
        assert parsed['right'] == 'P'
        assert parsed['symbol'] == 'TSLA'


# ---------------------------------------------------------------------------
# Resolve Option Contract
# ---------------------------------------------------------------------------

class TestResolveOptionContract:
    def test_resolve_builds_correct_contract(self):
        mock_client = _make_mock_rpc()
        sec_def = FakeSecurityDefinition(symbol='AAPL', conId=999999, secType='OPT')
        mock_client.rpc.return_value.resolve_contract.return_value = [sec_def]

        mmr = _make_mmr_with_mock(mock_client)
        contract = mmr._resolve_option_contract('AAPL', '2026-03-20', 250.0, 'C')

        assert contract.conId == 999999
        assert contract.secType == 'OPT'
        assert contract.symbol == 'AAPL'
        assert contract.strike == 250.0
        assert contract.right == 'C'
        assert contract.multiplier == '100'
        assert contract.lastTradeDateOrContractMonth == '20260320'

    def test_resolve_raises_on_empty(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.resolve_contract.return_value = []

        mmr = _make_mmr_with_mock(mock_client)
        with pytest.raises(ValueError, match="Could not resolve option contract"):
            mmr._resolve_option_contract('ZZZZZ', '2026-01-01', 999.0, 'C')


# ---------------------------------------------------------------------------
# Buy / Sell Option
# ---------------------------------------------------------------------------

class TestOptionTrading:
    def test_buy_option_validates_market_or_limit(self):
        mock_client = _make_mock_rpc()
        mmr = _make_mmr_with_mock(mock_client)

        with pytest.raises(ValueError, match="market=True or provide a limit_price"):
            mmr.buy_option('AAPL', '2026-03-20', 250.0, 'C', 5)

    def test_sell_option_validates_market_or_limit(self):
        mock_client = _make_mock_rpc()
        mmr = _make_mmr_with_mock(mock_client)

        with pytest.raises(ValueError, match="market=True or provide a limit_price"):
            mmr.sell_option('AAPL', '2026-03-20', 250.0, 'C', 5)

    def test_buy_option_calls_rpc(self):
        mock_client = _make_mock_rpc()
        sec_def = FakeSecurityDefinition(symbol='AAPL', conId=999999, secType='OPT')
        mock_client.rpc.return_value.resolve_contract.return_value = [sec_def]
        mock_client.rpc.return_value.place_order_simple.return_value = SuccessFail.success()

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.buy_option('AAPL', '2026-03-20', 250.0, 'C', 5, market=True)

        assert result.is_success()

    def test_sell_option_calls_rpc(self):
        mock_client = _make_mock_rpc()
        sec_def = FakeSecurityDefinition(symbol='AAPL', conId=999999, secType='OPT')
        mock_client.rpc.return_value.resolve_contract.return_value = [sec_def]
        mock_client.rpc.return_value.place_order_simple.return_value = SuccessFail.success()

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.sell_option('AAPL', '2026-03-20', 250.0, 'P', 3, limit_price=2.50)

        assert result.is_success()

    def test_buy_option_passes_none_equity_amount(self):
        """Options should always pass quantity directly, never equity_amount."""
        mock_client = _make_mock_rpc()
        sec_def = FakeSecurityDefinition(symbol='AAPL', conId=999999, secType='OPT')
        mock_client.rpc.return_value.resolve_contract.return_value = [sec_def]
        mock_client.rpc.return_value.place_order_simple.return_value = SuccessFail.success()

        mmr = _make_mmr_with_mock(mock_client)
        mmr.buy_option('AAPL', '2026-03-20', 250.0, 'C', 5, market=True)

        call_kwargs = mock_client.rpc.return_value.place_order_simple.call_args
        # equity_amount should be None (passed as positional or keyword)
        if call_kwargs.kwargs:
            assert call_kwargs.kwargs.get('equity_amount') is None
        else:
            # positional: contract, action, equity_amount, quantity, ...
            assert call_kwargs.args[2] is None  # equity_amount
            assert call_kwargs.args[3] == 5     # quantity


# ---------------------------------------------------------------------------
# Options Chain (mocked Massive API)
# ---------------------------------------------------------------------------

class TestOptionsChain:
    def _make_fake_snap(self, strike, contract_type, iv=0.3, bid=2.0, ask=2.5,
                        delta=0.5, underlying_price=150.0, expiration='2026-03-20',
                        ticker='AAPL260320C00250000'):
        snap = MagicMock()
        snap.details = MagicMock()
        snap.details.strike_price = strike
        snap.details.contract_type = contract_type
        snap.details.expiration_date = expiration
        snap.details.ticker = ticker
        snap.implied_volatility = iv
        snap.open_interest = 1000.0
        snap.break_even_price = strike + bid
        snap.last_quote = MagicMock()
        snap.last_quote.bid = bid
        snap.last_quote.ask = ask
        snap.last_trade = MagicMock()
        snap.last_trade.price = (bid + ask) / 2
        snap.day = MagicMock()
        snap.day.volume = 500.0
        snap.greeks = MagicMock()
        snap.greeks.delta = delta
        snap.greeks.gamma = 0.02
        snap.greeks.theta = -0.05
        snap.greeks.vega = 0.15
        snap.underlying_asset = MagicMock()
        snap.underlying_asset.price = underlying_price
        snap.fair_market_value = None
        return snap

    @patch('trader.sdk.RESTClient', create=True)
    def test_options_chain_returns_dataframe(self, mock_rest_cls):
        mock_client_instance = MagicMock()
        mock_rest_cls.return_value = mock_client_instance

        snaps = [
            self._make_fake_snap(245.0, 'call', delta=0.6),
            self._make_fake_snap(250.0, 'call', delta=0.5),
            self._make_fake_snap(245.0, 'put', delta=-0.4),
        ]
        mock_client_instance.list_snapshot_options_chain.return_value = snaps

        mock_rpc = _make_mock_rpc()
        mmr = _make_mmr_with_mock(mock_rpc)

        # Patch the RESTClient import inside the method
        with patch('massive.RESTClient', mock_rest_cls):
            df = mmr.options_chain('AAPL', expiration='2026-03-20')

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'strike' in df.columns
        assert 'delta' in df.columns
        assert 'iv' in df.columns
        # IV should be in percentage
        assert df.iloc[0]['iv'] == 30.0

    @patch('trader.sdk.RESTClient', create=True)
    def test_options_chain_filters_by_type(self, mock_rest_cls):
        mock_client_instance = MagicMock()
        mock_rest_cls.return_value = mock_client_instance

        snaps = [
            self._make_fake_snap(245.0, 'call'),
            self._make_fake_snap(250.0, 'call'),
            self._make_fake_snap(245.0, 'put'),
        ]
        mock_client_instance.list_snapshot_options_chain.return_value = snaps

        mock_rpc = _make_mock_rpc()
        mmr = _make_mmr_with_mock(mock_rpc)

        with patch('massive.RESTClient', mock_rest_cls):
            df = mmr.options_chain('AAPL', expiration='2026-03-20', contract_type='call')

        assert len(df) == 2
        assert all(df['type'] == 'call')

    @patch('trader.sdk.RESTClient', create=True)
    def test_options_chain_filters_by_strike_range(self, mock_rest_cls):
        mock_client_instance = MagicMock()
        mock_rest_cls.return_value = mock_client_instance

        snaps = [
            self._make_fake_snap(240.0, 'call'),
            self._make_fake_snap(250.0, 'call'),
            self._make_fake_snap(260.0, 'call'),
        ]
        mock_client_instance.list_snapshot_options_chain.return_value = snaps

        mock_rpc = _make_mock_rpc()
        mmr = _make_mmr_with_mock(mock_rpc)

        with patch('massive.RESTClient', mock_rest_cls):
            df = mmr.options_chain('AAPL', expiration='2026-03-20',
                                   strike_min=245.0, strike_max=255.0)

        assert len(df) == 1
        assert df.iloc[0]['strike'] == 250.0


# ---------------------------------------------------------------------------
# Options Snapshot (mocked Massive API)
# ---------------------------------------------------------------------------

class TestOptionsSnapshot:
    @patch('massive.RESTClient')
    def test_options_snapshot_returns_dict(self, mock_rest_cls):
        mock_client_instance = MagicMock()
        mock_rest_cls.return_value = mock_client_instance

        snap = MagicMock()
        snap.break_even_price = 253.0
        snap.implied_volatility = 0.35
        snap.open_interest = 5000
        snap.last_quote = MagicMock()
        snap.last_quote.bid = 3.0
        snap.last_quote.ask = 3.5
        snap.last_trade = MagicMock()
        snap.last_trade.price = 3.25
        snap.greeks = MagicMock()
        snap.greeks.delta = 0.45
        snap.greeks.gamma = 0.02
        snap.greeks.theta = -0.08
        snap.greeks.vega = 0.20
        snap.underlying_asset = MagicMock()
        snap.underlying_asset.price = 248.0
        snap.day = MagicMock()
        snap.day.volume = 1200

        mock_client_instance.get_snapshot_option.return_value = snap

        mock_rpc = _make_mock_rpc()
        mmr = _make_mmr_with_mock(mock_rpc)

        result = mmr.options_snapshot('O:AAPL260320C00250000')

        assert isinstance(result, dict)
        assert result['symbol'] == 'AAPL'
        assert result['strike'] == 250.0
        assert result['right'] == 'C'
        assert result['expiration'] == '2026-03-20'
        assert result['delta'] == 0.45
        assert result['underlying_price'] == 248.0
        assert '35.00%' in result['implied_volatility']


# ---------------------------------------------------------------------------
# Chain Math (pure functions, no mocking needed)
# ---------------------------------------------------------------------------

class TestChainMath:
    def test_d2_basic(self):
        from trader.tools.chain import d2
        result = d2(100, 100, 1.0, 0.05, 0.2)
        assert isinstance(result, float)

    def test_binary_call_atm(self):
        from trader.tools.chain import binary_call
        # ATM call with 1yr expiry should be roughly 0.5 (risk-neutral probability)
        result = binary_call(100, 100, 1.0, 0.05, 0.2)
        assert 0.4 < result < 0.7

    def test_binary_put_atm(self):
        from trader.tools.chain import binary_put
        result = binary_put(100, 100, 1.0, 0.05, 0.2)
        assert 0.3 < result < 0.6

    def test_binary_call_deep_itm(self):
        from trader.tools.chain import binary_call
        # Deep ITM call (S >> K) should be close to e^(-rT)
        result = binary_call(200, 100, 1.0, 0.05, 0.2)
        assert result > 0.9

    def test_binary_put_deep_otm(self):
        from trader.tools.chain import binary_put
        # Deep OTM put (S >> K) should be close to 0
        result = binary_put(200, 100, 1.0, 0.05, 0.2)
        assert result < 0.05

    def test_implied_constant_helper(self):
        from trader.tools.chain import implied_constant_helper
        # Build a synthetic chain DataFrame
        strikes = np.arange(80, 121, 5.0)
        df = pd.DataFrame({
            'IV': np.linspace(0.35, 0.25, len(strikes)),  # vol smile
            'T': [0.25] * len(strikes),
            'S': [100.0] * len(strikes),
            'K': strikes,
        })
        result = implied_constant_helper(df, risk_free_rate=0.05)
        assert 'x' in result
        assert 'market_implied' in result
        assert 'constant' in result
        assert len(result['market_implied']) == len(result['x']) - 1
        assert len(result['constant']) == len(result['x']) - 1

    def test_vol_by_strike(self):
        from trader.tools.chain import vol_by_strike
        poly = np.polyfit([90, 100, 110], [0.3, 0.25, 0.3], 2)
        vol = vol_by_strike(poly, 100)
        assert abs(vol - 0.25) < 0.01

    def test_monte_carlo_binary_call(self):
        from trader.tools.chain import monte_carlo_binary
        result = monte_carlo_binary(100, 100, 1.0, 0.05, 0.2, 1.0, type_='call', Ndraws=100_000)
        # Should be approximately equal to analytical binary call
        from trader.tools.chain import binary_call
        analytical = binary_call(100, 100, 1.0, 0.05, 0.2)
        assert abs(result - analytical) < 0.05

    def test_monte_carlo_binary_invalid_type(self):
        from trader.tools.chain import monte_carlo_binary
        with pytest.raises(ValueError, match="Type must be put or call"):
            monte_carlo_binary(100, 100, 1.0, 0.05, 0.2, 1.0, type_='straddle')


# ---------------------------------------------------------------------------
# Executioner Multiplier Fix
# ---------------------------------------------------------------------------

class TestExecutionerMultiplier:
    def _make_executioner(self):
        from trader.trading.executioner import TradeExecutioner
        executioner = TradeExecutioner()
        mock_trader = MagicMock()
        mock_trader.ib_account = 'DU123456'
        executioner.connect(mock_trader)
        return executioner

    def test_stock_multiplier_default(self):
        """Stock contracts have empty multiplier, should use 1.0."""
        from trader.objects import Action

        executioner = self._make_executioner()

        contract = MagicMock()
        contract.multiplier = ''  # stocks have empty multiplier
        contract.conId = 4391

        ticker = MagicMock()
        ticker.bid = 100.0
        ticker.ask = 100.5

        result = executioner.helper_create_order(
            contract=contract,
            action=Action.BUY,
            latest_tick=ticker,
            equity_amount=1000.0,
            quantity=None,
            limit_price=None,
            market_order=True,
            stop_loss_percentage=0.0,
            algo_name='test',
        )
        # 1000 / (100 * 1.0) = 10 shares
        assert result.order.totalQuantity == 10.0

    def test_option_multiplier_100(self):
        """Option contracts have multiplier=100, so equity_amount / (bid * 100)."""
        from trader.objects import Action

        executioner = self._make_executioner()

        contract = MagicMock()
        contract.multiplier = '100'
        contract.conId = 999999

        ticker = MagicMock()
        ticker.bid = 3.0   # option premium
        ticker.ask = 3.5

        result = executioner.helper_create_order(
            contract=contract,
            action=Action.BUY,
            latest_tick=ticker,
            equity_amount=3000.0,
            quantity=None,
            limit_price=None,
            market_order=True,
            stop_loss_percentage=0.0,
            algo_name='test',
        )
        # 3000 / (3.0 * 100) = 10 contracts
        assert result.order.totalQuantity == 10.0

    def test_option_multiplier_small_amount(self):
        """If equity_amount yields < 1 contract, should round up to 1."""
        from trader.objects import Action

        executioner = self._make_executioner()

        contract = MagicMock()
        contract.multiplier = '100'
        contract.conId = 999999

        ticker = MagicMock()
        ticker.bid = 5.0
        ticker.ask = 5.5

        result = executioner.helper_create_order(
            contract=contract,
            action=Action.BUY,
            latest_tick=ticker,
            equity_amount=200.0,  # 200 / (5 * 100) = 0.4 → rounds to 1
            quantity=None,
            limit_price=None,
            market_order=True,
            stop_loss_percentage=0.0,
            algo_name='test',
        )
        assert result.order.totalQuantity == 1.0

    def test_explicit_quantity_ignores_multiplier(self):
        """When quantity is passed directly, multiplier should not affect it."""
        from trader.objects import Action

        executioner = self._make_executioner()

        contract = MagicMock()
        contract.multiplier = '100'
        contract.conId = 999999

        ticker = MagicMock()
        ticker.bid = 3.0
        ticker.ask = 3.5

        result = executioner.helper_create_order(
            contract=contract,
            action=Action.BUY,
            latest_tick=ticker,
            equity_amount=None,
            quantity=5.0,
            limit_price=None,
            market_order=True,
            stop_loss_percentage=0.0,
            algo_name='test',
        )
        assert result.order.totalQuantity == 5.0


# ---------------------------------------------------------------------------
# CLI Parser
# ---------------------------------------------------------------------------

class TestCLIOptionsParser:
    def setup_method(self):
        from trader.mmr_cli import build_parser
        self.parser = build_parser()

    def test_parse_expirations(self):
        args = self.parser.parse_args(['options', 'expirations', 'AAPL'])
        assert args.command == 'options'
        assert args.opt_action == 'expirations'
        assert args.symbol == 'AAPL'

    def test_parse_chain_basic(self):
        args = self.parser.parse_args(['options', 'chain', 'AAPL'])
        assert args.opt_action == 'chain'
        assert args.symbol == 'AAPL'
        assert args.expiration is None
        assert args.contract_type is None

    def test_parse_chain_with_filters(self):
        args = self.parser.parse_args([
            'options', 'chain', 'AAPL',
            '-e', '2026-03-20', '--type', 'call',
            '--strike-min', '200', '--strike-max', '250',
        ])
        assert args.expiration == '2026-03-20'
        assert args.contract_type == 'call'
        assert args.strike_min == 200.0
        assert args.strike_max == 250.0

    def test_parse_snapshot(self):
        args = self.parser.parse_args(['options', 'snapshot', 'O:AAPL260320C00250000'])
        assert args.opt_action == 'snapshot'
        assert args.ticker == 'O:AAPL260320C00250000'

    def test_parse_implied(self):
        args = self.parser.parse_args([
            'options', 'implied', 'AAPL',
            '-e', '2026-03-20', '--risk-free-rate', '0.03',
        ])
        assert args.opt_action == 'implied'
        assert args.expiration == '2026-03-20'
        assert args.risk_free_rate == 0.03

    def test_parse_buy(self):
        args = self.parser.parse_args([
            'options', 'buy', 'AAPL',
            '-e', '2026-03-20', '-s', '250', '-r', 'C', '-q', '5', '--market',
        ])
        assert args.opt_action == 'buy'
        assert args.strike == 250.0
        assert args.right == 'C'
        assert args.quantity == 5.0
        assert args.market is True
        assert args.limit is None

    def test_parse_sell_with_limit(self):
        args = self.parser.parse_args([
            'options', 'sell', 'AAPL',
            '-e', '2026-03-20', '-s', '250', '-r', 'P', '-q', '3', '--limit', '2.50',
        ])
        assert args.opt_action == 'sell'
        assert args.right == 'P'
        assert args.quantity == 3.0
        assert args.limit == 2.50
        assert args.market is False

    def test_parse_alias_opt(self):
        args = self.parser.parse_args(['opt', 'chain', 'AAPL'])
        assert args.command == 'opt'
        assert args.opt_action == 'chain'

    def test_parse_expirations_alias_exp(self):
        args = self.parser.parse_args(['options', 'exp', 'TSLA'])
        assert args.opt_action == 'exp'
        assert args.symbol == 'TSLA'

    def test_right_restricted_to_c_or_p(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args([
                'options', 'buy', 'AAPL',
                '-e', '2026-03-20', '-s', '250', '-r', 'X', '-q', '5', '--market',
            ])

    def test_type_restricted_to_call_or_put(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args([
                'options', 'chain', 'AAPL', '--type', 'straddle',
            ])

    def test_implied_expiration_optional(self):
        args = self.parser.parse_args(['options', 'implied', 'AAPL'])
        assert args.expiration is None

    def test_implied_relative_expiration(self):
        args = self.parser.parse_args(['options', 'implied', 'AAPL', '-e', '3m'])
        assert args.expiration == '3m'

    def test_chain_relative_expiration(self):
        args = self.parser.parse_args(['options', 'chain', 'AAPL', '-e', '90d'])
        assert args.expiration == '90d'


# ---------------------------------------------------------------------------
# Relative Expiration Parsing & Resolution
# ---------------------------------------------------------------------------

class TestRelativeExpiration:
    def test_parse_days(self):
        from trader.mmr_cli import _parse_relative_expiration
        assert _parse_relative_expiration('90d') == 90
        assert _parse_relative_expiration('30 days') == 30
        assert _parse_relative_expiration('7day') == 7

    def test_parse_weeks(self):
        from trader.mmr_cli import _parse_relative_expiration
        assert _parse_relative_expiration('2w') == 14
        assert _parse_relative_expiration('4 weeks') == 28

    def test_parse_months(self):
        from trader.mmr_cli import _parse_relative_expiration
        assert _parse_relative_expiration('3m') == 90
        assert _parse_relative_expiration('6 months') == 180
        assert _parse_relative_expiration('1mo') == 30
        assert _parse_relative_expiration('1month') == 30

    def test_parse_years(self):
        from trader.mmr_cli import _parse_relative_expiration
        assert _parse_relative_expiration('1y') == 365
        assert _parse_relative_expiration('2 years') == 730

    def test_parse_exact_date_returns_negative(self):
        from trader.mmr_cli import _parse_relative_expiration
        assert _parse_relative_expiration('2026-03-20') == -1

    def test_parse_garbage_returns_negative(self):
        from trader.mmr_cli import _parse_relative_expiration
        assert _parse_relative_expiration('foobar') == -1

    def test_case_insensitive(self):
        from trader.mmr_cli import _parse_relative_expiration
        assert _parse_relative_expiration('3M') == 90
        assert _parse_relative_expiration('90D') == 90

    def test_resolve_none_defaults_to_90_days(self):
        from trader.mmr_cli import _resolve_expiration
        import datetime as dt_mod

        mock_mmr = MagicMock()
        today = dt_mod.date.today()
        # Create expirations at 30, 60, 90, 120 days out
        expirations = [
            (today + dt_mod.timedelta(days=d)).strftime('%Y-%m-%d')
            for d in [30, 60, 90, 120]
        ]
        mock_mmr.options_expirations.return_value = expirations

        result = _resolve_expiration(mock_mmr, 'AAPL', None)
        # Should pick the 90-day one (closest to default 90)
        assert result == expirations[2]

    def test_resolve_relative_3m(self):
        from trader.mmr_cli import _resolve_expiration
        import datetime as dt_mod

        mock_mmr = MagicMock()
        today = dt_mod.date.today()
        expirations = [
            (today + dt_mod.timedelta(days=d)).strftime('%Y-%m-%d')
            for d in [30, 60, 85, 120]
        ]
        mock_mmr.options_expirations.return_value = expirations

        result = _resolve_expiration(mock_mmr, 'AAPL', '3m')
        # 3m = 90 days, closest is 85 days
        assert result == expirations[2]

    def test_resolve_exact_date_passthrough(self):
        from trader.mmr_cli import _resolve_expiration

        mock_mmr = MagicMock()
        # Should not call options_expirations at all
        result = _resolve_expiration(mock_mmr, 'AAPL', '2026-03-20')
        assert result == '2026-03-20'
        mock_mmr.options_expirations.assert_not_called()

    def test_resolve_no_expirations_returns_none(self):
        from trader.mmr_cli import _resolve_expiration

        mock_mmr = MagicMock()
        mock_mmr.options_expirations.return_value = []

        result = _resolve_expiration(mock_mmr, 'AAPL', None)
        assert result is None

    def test_resolve_picks_closest(self):
        from trader.mmr_cli import _resolve_expiration
        import datetime as dt_mod

        mock_mmr = MagicMock()
        today = dt_mod.date.today()
        expirations = [
            (today + dt_mod.timedelta(days=d)).strftime('%Y-%m-%d')
            for d in [14, 45, 75, 180]
        ]
        mock_mmr.options_expirations.return_value = expirations

        # 30d should pick 14 or 45 — 45 is 15 away, 14 is 16 away → picks 14? no...
        # 30 - 14 = 16, 45 - 30 = 15 → picks 45
        result = _resolve_expiration(mock_mmr, 'AAPL', '30d')
        assert result == expirations[1]  # 45 days (closest to 30)
