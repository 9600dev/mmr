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
    mmr._contract_map = {}
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
    def __init__(self, status='Submitted', filled=0, remaining=0, avgFillPrice=0.0):
        self.status = status
        self.filled = filled
        self.remaining = remaining
        self.avgFillPrice = avgFillPrice


class FakeOrder:
    def __init__(self, orderId=1, action='BUY', orderType='MKT',
                 lmtPrice=0.0, auxPrice=0.0, totalQuantity=10.0,
                 tif='DAY', parentId=0):
        self.orderId = orderId
        self.action = action
        self.orderType = orderType
        self.lmtPrice = lmtPrice
        self.auxPrice = auxPrice
        self.totalQuantity = totalQuantity
        self.tif = tif
        self.parentId = parentId


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
        contract = FakeContract(conId=4391, symbol='AMD')
        order = FakeOrder(orderId=42, action='BUY', orderType='LMT', lmtPrice=145.0, totalQuantity=50)
        status = FakeOrderStatus(status='Submitted', filled=0, remaining=50)
        trade = FakeTrade(contract=contract, order=order, orderStatus=status)
        mock_client.rpc.return_value.get_trades.return_value = {42: [trade]}

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.orders()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['orderId'] == 42
        assert df.iloc[0]['symbol'] == 'AMD'
        assert df.iloc[0]['action'] == 'BUY'
        assert df.iloc[0]['status'] == 'Submitted'
        assert df.iloc[0]['lmtPrice'] == 145.0
        assert df.iloc[0]['quantity'] == 50
        # Market data columns should be present
        assert 'bid' in df.columns
        assert 'ask' in df.columns
        assert 'last' in df.columns

    def test_orders_empty(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_trades.return_value = {}

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.orders()

        assert isinstance(df, pd.DataFrame)
        assert df.empty


    def test_orders_filters_cancelled(self):
        """After cancel-all, cancelled orders should not appear."""
        mock_client = _make_mock_rpc()
        contract = FakeContract(conId=4391, symbol='AMD')

        active_order = FakeOrder(orderId=10, action='BUY', orderType='LMT',
                                  lmtPrice=145.0, totalQuantity=50)
        active_trade = FakeTrade(contract=contract, order=active_order,
                                  orderStatus=FakeOrderStatus(status='Submitted', remaining=50))

        cancelled_order = FakeOrder(orderId=11, action='BUY', orderType='LMT',
                                     lmtPrice=140.0, totalQuantity=100)
        cancelled_trade = FakeTrade(contract=contract, order=cancelled_order,
                                     orderStatus=FakeOrderStatus(status='Cancelled'))

        filled_order = FakeOrder(orderId=12, action='SELL', orderType='MKT',
                                  totalQuantity=25)
        filled_trade = FakeTrade(contract=contract, order=filled_order,
                                  orderStatus=FakeOrderStatus(status='Filled', filled=25))

        mock_client.rpc.return_value.get_trades.return_value = {
            10: [active_trade],
            11: [cancelled_trade],
            12: [filled_trade],
        }

        mmr = _make_mmr_with_mock(mock_client)
        df = mmr.orders()

        assert len(df) == 1
        assert df.iloc[0]['orderId'] == 10
        assert df.iloc[0]['status'] == 'Submitted'

    def test_orders_all_cancelled_returns_empty(self):
        """If every order is cancelled, orders() returns empty DataFrame."""
        mock_client = _make_mock_rpc()
        contract = FakeContract(conId=4391, symbol='AMD')
        order = FakeOrder(orderId=20, action='BUY', orderType='MKT', totalQuantity=10)
        trade = FakeTrade(contract=contract, order=order,
                          orderStatus=FakeOrderStatus(status='Cancelled'))

        mock_client.rpc.return_value.get_trades.return_value = {20: [trade]}

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


class TestToMarket:
    def test_to_market_cancels_and_replaces(self):
        mock_client = _make_mock_rpc()
        contract = FakeContract(conId=4391, symbol='AMD')
        parent_order = FakeOrder(orderId=36, action='BUY', orderType='LMT',
                                 lmtPrice=41.50, totalQuantity=100)
        stop_order = FakeOrder(orderId=37, action='SELL', orderType='STP',
                               auxPrice=38.00, totalQuantity=100, parentId=36)
        parent_trade = FakeTrade(contract=contract, order=parent_order)
        stop_trade = FakeTrade(contract=contract, order=stop_order)

        mock_client.rpc.return_value.get_trades.return_value = {
            36: [parent_trade],
            37: [stop_trade],
        }
        mock_client.rpc.return_value.cancel_order.return_value = SuccessFail.success()
        mock_client.rpc.return_value.place_expressive_order.return_value = SuccessFail.success()

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.to_market(36)

        assert result.is_success()
        # Verify cancel was called with the original order ID
        mock_client.rpc.return_value.cancel_order.assert_called_with(36)
        # Verify place_expressive_order was called with market + stop_loss
        call_kwargs = mock_client.rpc.return_value.place_expressive_order.call_args
        assert call_kwargs[1]['action'] == 'BUY'
        assert call_kwargs[1]['quantity'] == 100.0
        spec = call_kwargs[1]['execution_spec']
        assert spec['order_type'] == 'MARKET'
        assert spec['exit_type'] == 'STOP_LOSS'
        assert spec['stop_loss_price'] == 38.00

    def test_to_market_without_stop(self):
        mock_client = _make_mock_rpc()
        contract = FakeContract(conId=4391, symbol='AMD')
        order = FakeOrder(orderId=10, action='BUY', orderType='LMT',
                          lmtPrice=100.0, totalQuantity=50)
        trade = FakeTrade(contract=contract, order=order)

        mock_client.rpc.return_value.get_trades.return_value = {10: [trade]}
        mock_client.rpc.return_value.cancel_order.return_value = SuccessFail.success()
        mock_client.rpc.return_value.place_expressive_order.return_value = SuccessFail.success()

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.to_market(10)

        assert result.is_success()
        call_kwargs = mock_client.rpc.return_value.place_expressive_order.call_args
        spec = call_kwargs[1]['execution_spec']
        assert spec['order_type'] == 'MARKET'
        assert 'stop_loss_price' not in spec

    def test_to_market_order_not_found(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_trades.return_value = {}

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.to_market(999)

        assert not result.is_success()
        assert '999' in result.error

    def test_to_market_already_market(self):
        mock_client = _make_mock_rpc()
        contract = FakeContract(conId=4391, symbol='AMD')
        order = FakeOrder(orderId=5, action='BUY', orderType='MKT', totalQuantity=100)
        trade = FakeTrade(contract=contract, order=order)

        mock_client.rpc.return_value.get_trades.return_value = {5: [trade]}

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.to_market(5)

        assert not result.is_success()
        assert 'already a market order' in result.error


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
        mock_client.rpc.return_value.resolve_contract.return_value = []

        mmr = _make_mmr_with_mock(mock_client)
        with pytest.raises(ValueError, match="Could not resolve"):
            mmr._resolve_contract('NOTREAL')


class TestResolveIBDiscovery:
    """The v2 resolve() ships partial Contracts (no SMART/USD defaults) so
    IB's reqContractDetails does the discovery. Previously we forced
    exchange='SMART' currency='USD' when the caller didn't pass hints,
    which silently picked wrong ADRs for non-US primary listings. The
    dedupe layer collapses venue duplicates (same conId) while preserving
    real cross-exchange ambiguity (different conIds)."""

    def test_no_hints_passes_empty_exchange_currency_to_ib(self):
        """Regression guard for the "close enough" bug. With no exchange
        or currency hints, we must NOT pre-fill the Contract with
        SMART/USD — IB does the discovery and we rank what comes back."""
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.resolve_symbol.return_value = []
        asx_def = FakeSecurityDefinition(
            symbol='STO', conId=9999, exchange='ASX',
            primaryExchange='ASX', currency='AUD',
        )
        mock_client.rpc.return_value.resolve_contract.return_value = [asx_def]

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.resolve('STO')

        # The call to resolve_contract must have sent exchange='', currency=''
        # (not 'SMART' / 'USD').
        call_args = mock_client.rpc.return_value.resolve_contract.call_args
        sent_contract = call_args.args[0]
        assert sent_contract.exchange == '', f'expected empty exchange, got {sent_contract.exchange!r}'
        assert sent_contract.currency == '', f'expected empty currency, got {sent_contract.currency!r}'
        assert sent_contract.symbol == 'STO'
        assert len(result) == 1
        assert result[0].exchange == 'ASX'

    def test_hints_flow_through(self):
        """When hints ARE passed, use them — no defaulting."""
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.resolve_symbol.return_value = []
        mock_client.rpc.return_value.resolve_contract.return_value = []

        mmr = _make_mmr_with_mock(mock_client)
        mmr.resolve('BHP', exchange='ASX', currency='AUD')
        sent = mock_client.rpc.return_value.resolve_contract.call_args.args[0]
        assert sent.exchange == 'ASX'
        assert sent.currency == 'AUD'

    def test_integer_conid_does_not_hit_ib(self):
        """Integer conIds must be exact — no IB discovery fallback even
        when the local DB misses."""
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.resolve_symbol.return_value = []

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.resolve(4391)
        assert result == []
        mock_client.rpc.return_value.resolve_contract.assert_not_called()


class TestResolveDedupe:
    """Venue-duplicate collapsing: IB reports a stock on every venue it
    trades (NASDAQ, BATS, ARCA, ISLAND, …) all sharing one conId. One
    row per *listing* is what the caller wants."""

    def test_venue_duplicates_collapse_by_conid_and_currency(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.resolve_symbol.return_value = []
        # All 4 "copies" of AAPL share conId 265598, currency USD — just
        # different exchanges.
        venues = [
            FakeSecurityDefinition(symbol='AAPL', conId=265598, exchange='NASDAQ',
                                   primaryExchange='NASDAQ', currency='USD'),
            FakeSecurityDefinition(symbol='AAPL', conId=265598, exchange='BATS',
                                   primaryExchange='NASDAQ', currency='USD'),
            FakeSecurityDefinition(symbol='AAPL', conId=265598, exchange='ARCA',
                                   primaryExchange='NASDAQ', currency='USD'),
            FakeSecurityDefinition(symbol='AAPL', conId=265598, exchange='ISLAND',
                                   primaryExchange='NASDAQ', currency='USD'),
        ]
        mock_client.rpc.return_value.resolve_contract.return_value = venues

        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.resolve('AAPL')
        assert len(result) == 1, f'expected 1 after dedupe, got {len(result)}'
        # The row we kept should be the primary-exchange one
        assert result[0].exchange == 'NASDAQ'
        assert result[0].primaryExchange == 'NASDAQ'

    def test_dual_listing_survives_dedupe(self):
        """Dual-listed tickers (BHP on ASX + NYSE) have *different* conIds,
        so dedupe keeps both and surfaces real ambiguity to the caller."""
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.resolve_symbol.return_value = []
        mock_client.rpc.return_value.resolve_contract.return_value = [
            FakeSecurityDefinition(symbol='BHP', conId=1001, exchange='ASX',
                                   primaryExchange='ASX', currency='AUD'),
            FakeSecurityDefinition(symbol='BHP', conId=2002, exchange='NYSE',
                                   primaryExchange='NYSE', currency='USD'),
        ]
        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.resolve('BHP')
        assert len(result) == 2
        currencies = {r.currency for r in result}
        assert currencies == {'AUD', 'USD'}

    def test_dedupe_prefers_primary_exchange_match(self):
        """Within a conId group, keep the row where exchange ==
        primaryExchange (the "home" listing, not a routed venue copy).
        Previous row gets dropped even when seen first."""
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.resolve_symbol.return_value = []
        mock_client.rpc.return_value.resolve_contract.return_value = [
            # Routed venue copy comes first
            FakeSecurityDefinition(symbol='AAPL', conId=265598, exchange='BATS',
                                   primaryExchange='NASDAQ', currency='USD'),
            # Primary listing comes second — should win
            FakeSecurityDefinition(symbol='AAPL', conId=265598, exchange='NASDAQ',
                                   primaryExchange='NASDAQ', currency='USD'),
        ]
        mmr = _make_mmr_with_mock(mock_client)
        result = mmr.resolve('AAPL')
        assert len(result) == 1
        assert result[0].exchange == 'NASDAQ'

    def test_dedupe_empty_input_returns_empty(self):
        mmr = _make_mmr_with_mock(_make_mock_rpc())
        assert mmr._dedupe_venue_duplicates([]) == []

    def test_unknown_symbol_returns_empty(self):
        """IB has no listing → empty list, no error raised."""
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.resolve_symbol.return_value = []
        mock_client.rpc.return_value.resolve_contract.return_value = []
        mmr = _make_mmr_with_mock(mock_client)
        assert mmr.resolve('ZZZZZZ') == []


class TestResolveContractExchangeCurrency:
    """Test that _resolve_contract respects exchange/currency hints."""

    def test_prefers_asx_when_exchange_hint_given(self):
        """With exchange='ASX', pick the ASX definition over the US one."""
        mock_client = _make_mock_rpc()
        us_def = FakeSecurityDefinition(
            symbol='BHP', conId=1234, exchange='SMART',
            primaryExchange='NYSE', currency='USD',
        )
        asx_def = FakeSecurityDefinition(
            symbol='BHP', conId=5678, exchange='ASX',
            primaryExchange='ASX', currency='AUD',
        )
        mock_client.rpc.return_value.resolve_symbol.return_value = [us_def, asx_def]

        mmr = _make_mmr_with_mock(mock_client)
        contract = mmr._resolve_contract('BHP', exchange='ASX')

        assert contract.conId == 5678
        assert contract.currency == 'AUD'
        # Non-US exchanges use SMART routing with primaryExchange set
        assert contract.exchange == 'SMART'
        assert contract.primaryExchange == 'ASX'

    def test_prefers_currency_hint(self):
        """With currency='AUD', pick the AUD definition."""
        mock_client = _make_mock_rpc()
        us_def = FakeSecurityDefinition(
            symbol='BHP', conId=1234, exchange='SMART',
            primaryExchange='NYSE', currency='USD',
        )
        asx_def = FakeSecurityDefinition(
            symbol='BHP', conId=5678, exchange='ASX',
            primaryExchange='ASX', currency='AUD',
        )
        mock_client.rpc.return_value.resolve_symbol.return_value = [us_def, asx_def]

        mmr = _make_mmr_with_mock(mock_client)
        contract = mmr._resolve_contract('BHP', currency='AUD')

        assert contract.conId == 5678
        assert contract.currency == 'AUD'

    def test_exchange_and_currency_together(self):
        """Both exchange and currency narrow the selection."""
        mock_client = _make_mock_rpc()
        us_def = FakeSecurityDefinition(
            symbol='BHP', conId=1111, exchange='SMART',
            primaryExchange='NYSE', currency='USD',
        )
        lse_def = FakeSecurityDefinition(
            symbol='BHP', conId=2222, exchange='LSE',
            primaryExchange='LSE', currency='GBP',
        )
        asx_def = FakeSecurityDefinition(
            symbol='BHP', conId=3333, exchange='ASX',
            primaryExchange='ASX', currency='AUD',
        )
        mock_client.rpc.return_value.resolve_symbol.return_value = [us_def, lse_def, asx_def]

        mmr = _make_mmr_with_mock(mock_client)
        contract = mmr._resolve_contract('BHP', exchange='ASX', currency='AUD')

        assert contract.conId == 3333

    def test_no_hint_prefers_usd(self):
        """Without hints, the existing US/USD preference is preserved."""
        mock_client = _make_mock_rpc()
        asx_def = FakeSecurityDefinition(
            symbol='BHP', conId=5678, exchange='ASX',
            primaryExchange='ASX', currency='AUD',
        )
        us_def = FakeSecurityDefinition(
            symbol='BHP', conId=1234, exchange='SMART',
            primaryExchange='NYSE', currency='USD',
        )
        # ASX listed first, but USD should still win
        mock_client.rpc.return_value.resolve_symbol.return_value = [asx_def, us_def]

        mmr = _make_mmr_with_mock(mock_client)
        contract = mmr._resolve_contract('BHP')

        assert contract.conId == 1234
        assert contract.currency == 'USD'

    def test_matches_primary_exchange(self):
        """exchange hint should match against primaryExchange too."""
        mock_client = _make_mock_rpc()
        us_def = FakeSecurityDefinition(
            symbol='BHP', conId=1234, exchange='SMART',
            primaryExchange='NYSE', currency='USD',
        )
        asx_def = FakeSecurityDefinition(
            symbol='BHP', conId=5678, exchange='SMART',
            primaryExchange='ASX', currency='AUD',
        )
        mock_client.rpc.return_value.resolve_symbol.return_value = [us_def, asx_def]

        mmr = _make_mmr_with_mock(mock_client)
        contract = mmr._resolve_contract('BHP', exchange='ASX')

        assert contract.conId == 5678

    def test_case_insensitive_hints(self):
        """exchange/currency hints should be case-insensitive."""
        mock_client = _make_mock_rpc()
        us_def = FakeSecurityDefinition(
            symbol='BHP', conId=1234, exchange='SMART',
            primaryExchange='NYSE', currency='USD',
        )
        asx_def = FakeSecurityDefinition(
            symbol='BHP', conId=5678, exchange='ASX',
            primaryExchange='ASX', currency='AUD',
        )
        mock_client.rpc.return_value.resolve_symbol.return_value = [us_def, asx_def]

        mmr = _make_mmr_with_mock(mock_client)
        contract = mmr._resolve_contract('BHP', exchange='asx', currency='aud')

        assert contract.conId == 5678

    def test_re_resolves_via_ib_when_universe_returns_wrong_exchange(self):
        """When the local universe only has a USD def but caller wants ASX/AUD,
        re-resolve via IB with the correct exchange/currency."""
        mock_client = _make_mock_rpc()
        us_def = FakeSecurityDefinition(
            symbol='NAB', conId=1111, exchange='SMART',
            primaryExchange='NYSE', currency='USD',
        )
        asx_def = FakeSecurityDefinition(
            symbol='NAB', conId=9999, exchange='ASX',
            primaryExchange='ASX', currency='AUD',
        )
        # Universe returns only the US definition
        mock_client.rpc.return_value.resolve_symbol.return_value = [us_def]
        # IB re-resolve returns the ASX definition
        mock_client.rpc.return_value.resolve_contract.return_value = [asx_def]

        mmr = _make_mmr_with_mock(mock_client)
        contract = mmr._resolve_contract('NAB', exchange='ASX', currency='AUD')

        assert contract.conId == 9999
        assert contract.currency == 'AUD'
        assert contract.primaryExchange == 'ASX'

    def test_single_matching_def_no_re_resolve(self):
        """When the single definition matches the hints, don't re-resolve."""
        mock_client = _make_mock_rpc()
        asx_def = FakeSecurityDefinition(
            symbol='NAB', conId=9999, exchange='ASX',
            primaryExchange='ASX', currency='AUD',
        )
        mock_client.rpc.return_value.resolve_symbol.return_value = [asx_def]

        mmr = _make_mmr_with_mock(mock_client)
        contract = mmr._resolve_contract('NAB', exchange='ASX', currency='AUD')

        assert contract.conId == 9999
        assert contract.currency == 'AUD'
        # resolve_contract (IB fallback) should NOT have been called
        mock_client.rpc.return_value.resolve_contract.assert_not_called()


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


class TestMarketHours:
    def test_market_hours_returns_list_of_dicts(self):
        mmr = _make_mmr_with_mock(_make_mock_rpc())
        rows = mmr.market_hours()

        assert isinstance(rows, list)
        assert len(rows) == len(MMR._MARKET_CALENDARS)

        required_keys = {'exchange', 'region', 'status', 'next_event', 'next_event_time', 'relative'}
        for row in rows:
            assert isinstance(row, dict)
            assert required_keys.issubset(row.keys()), f"Missing keys: {required_keys - row.keys()}"

    def test_market_hours_status_values(self):
        mmr = _make_mmr_with_mock(_make_mock_rpc())
        rows = mmr.market_hours()

        for row in rows:
            assert row['status'] in ('OPEN', 'CLOSED')
            assert row['next_event'] in ('opens', 'closes')

    def test_market_hours_exchanges_present(self):
        mmr = _make_mmr_with_mock(_make_mock_rpc())
        rows = mmr.market_hours()

        exchange_names = {r['exchange'] for r in rows}
        assert 'NYSE' in exchange_names
        assert 'NASDAQ' in exchange_names
        assert 'ASX' in exchange_names
        assert 'TSE' in exchange_names
