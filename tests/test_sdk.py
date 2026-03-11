"""Unit tests for trader.sdk.MMR — mocks RPCClient so no live connection needed."""

import asyncio
import os
import sys
import threading
import time
from dataclasses import dataclass
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from trader.sdk import MMR, Subscription
from trader.common.reactivex import SuccessFail, SuccessFailEnum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_rpc():
    """Create a mock RPCClient that tracks .rpc() chains."""
    client = MagicMock()
    client.is_setup = True
    return client


def _make_mmr_with_mock(mock_client) -> MMR:
    """Create an MMR instance wired to a mock RPCClient (skipping real connect)."""
    mmr = MMR.__new__(MMR)
    mmr._client = mock_client
    mmr._data_client = None
    mmr._massive_rest_client = None
    mmr._rpc_address = 'tcp://127.0.0.1'
    mmr._rpc_port = 42001
    mmr._pubsub_address = 'tcp://127.0.0.1'
    mmr._pubsub_port = 42002
    mmr._data_rpc_address = 'tcp://127.0.0.1'
    mmr._data_rpc_port = 42003
    mmr._timeout = 5
    mmr._subscriptions = []
    mmr._position_map = {}
    mmr._container = MagicMock()
    mmr._container.config_file = '/tmp/test_trader.yaml'
    return mmr


# ---------------------------------------------------------------------------
# Fake ib_async objects for testing
# ---------------------------------------------------------------------------

class FakeContract:
    def __init__(self, conId=0, symbol='', localSymbol='', secType='STK',
                 exchange='SMART', primaryExchange='NASDAQ', currency='USD',
                 strike=0.0):
        self.conId = conId
        self.symbol = symbol
        self.localSymbol = localSymbol
        self.secType = secType
        self.exchange = exchange
        self.primaryExchange = primaryExchange
        self.currency = currency
        self.strike = strike


class FakePortfolioSummary:
    def __init__(self, contract, position, marketPrice, marketValue,
                 averageCost, unrealizedPNL, realizedPNL, account, dailyPNL):
        self.contract = contract
        self.position = position
        self.marketPrice = marketPrice
        self.marketValue = marketValue
        self.averageCost = averageCost
        self.unrealizedPNL = unrealizedPNL
        self.realizedPNL = realizedPNL
        self.account = account
        self.dailyPNL = dailyPNL


class FakePosition:
    def __init__(self, account, contract, position, avgCost):
        self.account = account
        self.contract = contract
        self.position = position
        self.avgCost = avgCost


class FakeOrderStatus:
    def __init__(self, status='Submitted', filled=0):
        self.status = status
        self.filled = filled


class FakeOrder:
    def __init__(self, orderId=1, action='BUY', orderType='MKT',
                 lmtPrice=0.0, totalQuantity=10.0):
        self.orderId = orderId
        self.action = action
        self.orderType = orderType
        self.lmtPrice = lmtPrice
        self.totalQuantity = totalQuantity


class FakeTrade:
    def __init__(self, contract=None, order=None, orderStatus=None):
        self.contract = contract or FakeContract()
        self.order = order or FakeOrder()
        self.orderStatus = orderStatus or FakeOrderStatus()


class FakeTicker:
    def __init__(self, contract=None):
        self.contract = contract or FakeContract(conId=4391, symbol='AMD')
        self.time = None
        self.bid = 150.0
        self.bidSize = 100
        self.ask = 150.5
        self.askSize = 200
        self.last = 150.25
        self.lastSize = 50
        self.open = 148.0
        self.high = 151.0
        self.low = 147.5
        self.close = 149.0
        self.halted = 0.0
        self.shortableShares = 0


class FakeSecurityDefinition:
    def __init__(self, symbol='AMD', conId=4391, secType='STK',
                 exchange='SMART', primaryExchange='NASDAQ', currency='USD'):
        self.symbol = symbol
        self.conId = conId
        self.secType = secType
        self.exchange = exchange
        self.primaryExchange = primaryExchange
        self.currency = currency
        self.longName = f'{symbol} Inc.'
        self.tradingClass = symbol
        self.includeExpired = False
        self.secIdType = ''
        self.secId = ''
        self.description = ''
        self.minTick = 0.01
        self.orderTypes = ''
        self.validExchanges = 'SMART'
        self.priceMagnifier = 1
        self.category = ''
        self.subcategory = ''
        self.tradingHours = ''
        self.timeZoneId = ''
        self.liquidHours = ''
        self.stockType = ''
        self.minSize = 1
        self.sizeIncrement = 1
        self.suggestedSizeIncrement = 1
        self.bondType = ''
        self.couponType = ''
        self.callable = False
        self.putable = False
        self.coupon = 0.0
        self.convertable = False
        self.maturity = ''
        self.issueDate = ''
        self.nextOptionDate = ''
        self.nextOptionPartial = False
        self.nextOptionType = ''
        self.marketRuleIds = ''
        self.company_name = ''
        self.industry = ''
        self.contractMonth = ''


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSubscription:
    def test_stop_before_start(self):
        sub = Subscription()
        sub.stop()  # should not raise
        assert not sub.is_active()

    def test_stop_running_thread(self):
        sub = Subscription()
        sub._thread = threading.Thread(target=lambda: sub._stop_event.wait())
        sub._thread.daemon = True
        sub._thread.start()
        assert sub.is_active()
        sub.stop()
        assert not sub.is_active()


class TestMMRConnect:
    def test_context_manager(self):
        mock_client = _make_mock_rpc()
        with patch.object(MMR, 'connect', return_value=None) as mock_connect:
            mmr = _make_mmr_with_mock(mock_client)
            mmr.close()
            assert mmr._client is None

    def test_rpc_raises_when_disconnected(self):
        mmr = MMR.__new__(MMR)
        mmr._client = None
        mmr._subscriptions = []
        mmr._position_map = {}
        with pytest.raises(ConnectionError, match="Not connected"):
            _ = mmr._rpc


class TestPortfolio:
    def test_portfolio_returns_dataframe(self):
        mock_client = _make_mock_rpc()
        contract = FakeContract(conId=4391, symbol='AMD', localSymbol='AMD')
        summary = FakePortfolioSummary(
            contract=contract,
            position=100,
            marketPrice=150.0,
            marketValue=15000.0,
            averageCost=140.0,
            unrealizedPNL=1000.0,
            realizedPNL=0.0,
            account='DU123',
            dailyPNL=50.0,
        )
        mock_client.rpc.return_value.get_portfolio_summary.return_value = [summary]

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.portfolio()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['symbol'] == 'AMD'
        assert df.iloc[0]['position'] == 100
        assert df.iloc[0]['mktPrice'] == 150.0
        assert df.iloc[0]['unrealizedPNL'] == 1000.0

    def test_portfolio_updates_position_map(self):
        mock_client = _make_mock_rpc()
        contracts = [
            FakeContract(conId=4391, symbol='AMD', localSymbol='AMD'),
            FakeContract(conId=265598, symbol='AAPL', localSymbol='AAPL'),
        ]
        summaries = [
            FakePortfolioSummary(contracts[0], 100, 150.0, 15000.0, 140.0, 1000.0, 0.0, 'DU123', 50.0),
            FakePortfolioSummary(contracts[1], 50, 180.0, 9000.0, 170.0, 500.0, 0.0, 'DU123', -10.0),
        ]
        mock_client.rpc.return_value.get_portfolio_summary.return_value = summaries

        mmr = _make_mmr_with_mock(mock_client)
        mmr.portfolio()

        assert len(mmr._position_map) == 2
        # The map should contain the symbols (sorted by dailyPNL desc)
        assert 'AMD' in mmr._position_map.values()
        assert 'AAPL' in mmr._position_map.values()

    def test_portfolio_empty(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_portfolio_summary.return_value = []

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.portfolio()

        assert isinstance(df, pd.DataFrame)
        assert df.empty


class TestPositions:
    def test_positions_returns_dataframe(self):
        mock_client = _make_mock_rpc()
        contract = FakeContract(conId=4391, symbol='AMD', localSymbol='AMD')
        position = FakePosition(account='DU123', contract=contract, position=100, avgCost=140.0)
        mock_client.rpc.return_value.get_positions.return_value = [position]

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.positions()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['symbol'] == 'AMD'
        assert df.iloc[0]['position'] == 100


class TestOrders:
    def test_orders_returns_dataframe(self):
        mock_client = _make_mock_rpc()
        order = FakeOrder(orderId=42, action='BUY', orderType='LMT', lmtPrice=145.0, totalQuantity=50)
        mock_client.rpc.return_value.get_orders.return_value = {42: [order]}

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.orders()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['orderId'] == 42

    def test_orders_empty(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_orders.return_value = {}

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.orders()

        assert isinstance(df, pd.DataFrame)
        assert df.empty


class TestTrades:
    def test_trades_returns_dataframe(self):
        mock_client = _make_mock_rpc()
        contract = FakeContract(conId=4391, symbol='AMD')
        order = FakeOrder(orderId=1, action='BUY')
        status = FakeOrderStatus(status='Submitted', filled=0)
        trade = FakeTrade(contract=contract, order=order, orderStatus=status)
        mock_client.rpc.return_value.get_trades.return_value = {1: [trade]}

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.trades()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['symbol'] == 'AMD'


class TestTrading:
    def test_buy_validates_market_or_limit(self):
        mock_client = _make_mock_rpc()
        mmr = _make_mmr_with_mock(mock_client)

        with pytest.raises(ValueError, match="market=True or provide a limit_price"):
            mmr.buy('AMD', quantity=10)

    def test_buy_validates_amount_or_quantity(self):
        mock_client = _make_mock_rpc()
        mmr = _make_mmr_with_mock(mock_client)

        with pytest.raises(ValueError, match="amount.*or quantity"):
            mmr.buy('AMD', market=True)

    def test_buy_calls_rpc(self):
        mock_client = _make_mock_rpc()

        # Mock resolve_symbol to return a definition
        sec_def = FakeSecurityDefinition()
        mock_client.rpc.return_value.resolve_symbol.return_value = [sec_def]

        # Mock place_order_simple to return success
        mock_client.rpc.return_value.place_order_simple.return_value = SuccessFail.success()

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.buy('AMD', market=True, quantity=10)

        assert result.is_success()

    def test_sell_calls_rpc(self):
        mock_client = _make_mock_rpc()
        sec_def = FakeSecurityDefinition()
        mock_client.rpc.return_value.resolve_symbol.return_value = [sec_def]
        mock_client.rpc.return_value.place_order_simple.return_value = SuccessFail.success()

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.sell('AMD', market=True, quantity=10)

        assert result.is_success()


class TestCancel:
    def test_cancel_order(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.cancel_order.return_value = SuccessFail.success()

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.cancel(42)

        assert result.is_success()

    def test_cancel_all(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.cancel_all.return_value = SuccessFail.success()

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.cancel_all()

        assert result.is_success()


class TestSnapshot:
    def test_snapshot_returns_dict(self):
        mock_client = _make_mock_rpc()
        sec_def = FakeSecurityDefinition()
        mock_client.rpc.return_value.resolve_symbol.return_value = [sec_def]

        ticker = FakeTicker()
        mock_client.rpc.return_value.get_snapshot.return_value = ticker

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.snapshot('AMD')

        assert isinstance(result, dict)
        assert result['symbol'] == 'AMD'
        assert result['bid'] == 150.0
        assert result['ask'] == 150.5


class TestResolve:
    def test_resolve_returns_list(self):
        mock_client = _make_mock_rpc()
        sec_def = FakeSecurityDefinition()
        mock_client.rpc.return_value.resolve_symbol.return_value = [sec_def]

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.resolve('AMD')

        assert len(result) == 1
        assert result[0].symbol == 'AMD'

    def test_resolve_contract_raises_on_empty(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.resolve_symbol.return_value = []

        mmr = _make_mmr_with_mock(mock_client)
        with pytest.raises(ValueError, match="Could not resolve"):
            mmr._resolve_contract('NOTREAL')


class TestStrategies:
    def test_strategies_returns_dataframe(self):
        mock_client = _make_mock_rpc()

        class FakeStrategyConfig:
            def __init__(self):
                self.name = 'smi_crossover'
                self.state = 'RUNNING'
                self.paper = True
                self.bar_size = '1 min'
                self.conids = [4391]
                self.historical_days_prior = 5

        mock_client.rpc.return_value.get_strategies.return_value = SuccessFail.success(
            obj=[FakeStrategyConfig()]
        )

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.strategies()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['name'] == 'smi_crossover'

    def test_strategies_empty_on_failure(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_strategies.return_value = SuccessFail.fail()

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.strategies()

        assert isinstance(df, pd.DataFrame)
        assert df.empty


class TestClosePosition:
    def test_close_position_no_positions(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_portfolio_summary.return_value = []

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.close_position('AMD')

        assert not result.is_success()

    def test_close_position_symbol_not_found(self):
        mock_client = _make_mock_rpc()
        contract = FakeContract(conId=4391, symbol='AMD', localSymbol='AMD')
        summary = FakePortfolioSummary(contract, 100, 150.0, 15000.0, 140.0, 1000.0, 0.0, 'DU123', 50.0)
        mock_client.rpc.return_value.get_portfolio_summary.return_value = [summary]

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.close_position('ZZZZZ')

        assert not result.is_success()


class TestAccount:
    def test_account_returns_string(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_ib_account.return_value = 'DU123456'

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.account()

        assert result == 'DU123456'
