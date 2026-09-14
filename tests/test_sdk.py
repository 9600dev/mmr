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

    def test_no_hint_refuses_distinct_listings(self):
        """An unqualified ambiguous ticker cannot silently choose a listing."""
        mock_client = _make_mock_rpc()
        asx_def = FakeSecurityDefinition(
            symbol='BHP', conId=5678, exchange='ASX',
            primaryExchange='ASX', currency='AUD',
        )
        us_def = FakeSecurityDefinition(
            symbol='BHP', conId=1234, exchange='SMART',
            primaryExchange='NYSE', currency='USD',
        )
        # Neither list order nor USD status grants execution authority.
        mock_client.rpc.return_value.resolve_symbol.return_value = [asx_def, us_def]

        mmr = _make_mmr_with_mock(mock_client)
        with pytest.raises(ValueError, match='Ambiguous contract'):
            mmr._resolve_contract('BHP')

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
                self.paper_only = False
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


class TestProtectiveOrder:
    def test_invalid_order_type_rejected(self):
        mmr = _make_mmr_with_mock(_make_mock_rpc())
        result = mmr.place_protective_order('PLS', 'SELL', 100, 'MKT')
        assert not result.is_success()
        assert 'STP/TRAIL/LMT' in (result.error or '')

    def test_non_positive_quantity_rejected(self):
        mmr = _make_mmr_with_mock(_make_mock_rpc())
        result = mmr.place_protective_order('PLS', 'SELL', 0, 'STP', aux_price=4.75)
        assert not result.is_success()

    def test_stp_uses_cached_contract_and_forwards_args(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.place_standalone_order.return_value = SuccessFail.success()
        mmr = _make_mmr_with_mock(mock_client)
        contract = FakeContract(conId=71294096, symbol='PLS', localSymbol='PLS')
        mmr._contract_map = {'PLS': contract}

        result = mmr.place_protective_order(
            'PLS', 'sell', 3800, 'stp', aux_price=4.75, tif='GTC')
        assert result.is_success()
        call = mock_client.rpc.return_value.place_standalone_order.call_args
        assert call.kwargs['action'] == 'SELL'          # upper-cased
        assert call.kwargs['order_type'] == 'STP'
        assert call.kwargs['quantity'] == 3800
        assert call.kwargs['aux_price'] == 4.75
        assert call.kwargs['contract'] is contract      # cached, no re-resolve

    def test_trail_forwards_trailing_percent(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.place_standalone_order.return_value = SuccessFail.success()
        mmr = _make_mmr_with_mock(mock_client)
        mmr._contract_map = {'PLS': FakeContract(conId=1, symbol='PLS', localSymbol='PLS')}
        result = mmr.place_protective_order('PLS', 'SELL', 3800, 'TRAIL', trailing_percent=8.0)
        assert result.is_success()
        call = mock_client.rpc.return_value.place_standalone_order.call_args
        assert call.kwargs['order_type'] == 'TRAIL'
        assert call.kwargs['trailing_percent'] == 8.0


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


def _stub_proposal_store(mmr, pending=None):
    """Pre-seed _prop_store so _proposal_store() returns a controlled stub."""
    store = MagicMock()
    store.query.return_value = pending or []
    mmr._prop_store = store


class TestGetPortfolioStateErrorSurfacing:
    """A silent RPC failure in _get_portfolio_state used to be indistinguishable
    from a genuinely empty account — session_status returned zeros either way.
    These tests pin the new contract: failures are recorded on rpc_errors and
    (via session_summary) surfaced as warnings."""

    def test_account_values_failure_recorded(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_account_values.side_effect = TimeoutError('deadline')
        # portfolio() must still succeed so we isolate the account_values path
        mock_client.rpc.return_value.get_portfolio_summary.return_value = []

        mmr = _make_mmr_with_mock(mock_client)
        _stub_proposal_store(mmr)

        state = mmr._get_portfolio_state()

        assert state.net_liquidation == 0.0
        assert any('account_values' in e and 'TimeoutError' in e for e in state.rpc_errors), (
            f'expected account_values error in rpc_errors, got {state.rpc_errors!r}'
        )

    def test_portfolio_failure_recorded(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_account_values.return_value = {
            'NetLiquidation': {'value': 97248.18},
            'GrossPositionValue': {'value': 22382.0},
            'AvailableFunds': {'value': 50000.0},
        }
        mock_client.rpc.return_value.get_portfolio_summary.side_effect = ConnectionError('lost')

        mmr = _make_mmr_with_mock(mock_client)
        _stub_proposal_store(mmr)

        state = mmr._get_portfolio_state()

        # account_values succeeded so net_liq is correct
        assert state.net_liquidation == 97248.18
        # position_count stays at 0 but the failure is *flagged*
        assert state.position_count == 0
        assert any('portfolio' in e and 'ConnectionError' in e for e in state.rpc_errors), (
            f'expected portfolio error in rpc_errors, got {state.rpc_errors!r}'
        )

    def test_both_failures_recorded_simultaneously(self):
        # This is the exact scenario observed in the LLMVM trajectory:
        # portfolio_risk worked, then session_status returned all zeros.
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_account_values.side_effect = TimeoutError('a')
        mock_client.rpc.return_value.get_portfolio_summary.side_effect = TimeoutError('b')

        mmr = _make_mmr_with_mock(mock_client)
        _stub_proposal_store(mmr)

        state = mmr._get_portfolio_state()

        assert state.net_liquidation == 0.0
        assert state.position_count == 0
        assert any('account_values' in e for e in state.rpc_errors)
        assert any('portfolio' in e for e in state.rpc_errors)

    def test_clean_success_leaves_rpc_errors_empty(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_account_values.return_value = {
            'NetLiquidation': {'value': 50000.0},
            'GrossPositionValue': {'value': 0.0},
            'AvailableFunds': {'value': 50000.0},
        }
        mock_client.rpc.return_value.get_portfolio_summary.return_value = []

        mmr = _make_mmr_with_mock(mock_client)
        _stub_proposal_store(mmr)

        state = mmr._get_portfolio_state()

        assert state.rpc_errors == []
        assert state.net_liquidation == 50000.0


class TestSessionStatusSurfacesErrors:
    def test_session_status_flags_rpc_failure(self):
        # session_status should loudly signal when its data is incomplete —
        # otherwise an LLM can't tell "account empty" from "RPC failed".
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_account_values.side_effect = TimeoutError('x')
        mock_client.rpc.return_value.get_portfolio_summary.return_value = []

        mmr = _make_mmr_with_mock(mock_client)
        _stub_proposal_store(mmr)

        summary = mmr.session_status()

        assert summary['portfolio']['rpc_errors'], 'rpc_errors must be populated'
        assert any('account_values' in e for e in summary['portfolio']['rpc_errors'])
        assert any(
            'RPC' in w and 'account_values' in w for w in summary['warnings']
        ), f'expected RPC-failure warning, got {summary["warnings"]!r}'


class TestRiskReportPropagatesFailures:
    """risk_report is explicitly documented as requiring trader_service — a
    silent failure yielding net_liq=0 silently corrupts every exposure %,
    HHI and group-budget number it produces. Better to raise."""

    def test_account_values_failure_propagates(self):
        mock_client = _make_mock_rpc()
        mock_client.rpc.return_value.get_portfolio_summary.return_value = []
        mock_client.rpc.return_value.get_account_values.side_effect = TimeoutError('boom')

        mmr = _make_mmr_with_mock(mock_client)
        # avoid PortfolioRiskAnalyzer needing a real duckdb path
        mmr._container.config.return_value = {'duckdb_path': ''}

        with pytest.raises(TimeoutError):
            mmr.risk_report()


class TestPortfolioSnapshotPropagatesFailures:
    def test_account_values_failure_propagates(self):
        mock_client = _make_mock_rpc()
        # Must return a non-empty portfolio so we get past the early-return
        contract = FakeContract(conId=4391, symbol='AMD', localSymbol='AMD')
        summary = FakePortfolioSummary(
            contract=contract, position=100, marketPrice=150.0, marketValue=15000.0,
            averageCost=140.0, unrealizedPNL=1000.0, realizedPNL=0.0,
            account='DU123', dailyPNL=50.0,
        )
        mock_client.rpc.return_value.get_portfolio_summary.return_value = [summary]
        mock_client.rpc.return_value.get_account_values.side_effect = TimeoutError('boom')

        mmr = _make_mmr_with_mock(mock_client)

        with pytest.raises(TimeoutError):
            mmr.portfolio_snapshot()


class TestApproveAmountConversion:
    """approve() converts a dollar amount to whole shares via floor-and-refuse
    (trader.trading.order_math) — the old round() + bump-to-1 silently turned
    a too-small amount into a full share at inflated notional."""

    def _make_mmr(self, mock_client, tmp_duckdb_path):
        mmr = _make_mmr_with_mock(mock_client)
        mmr._container.config.return_value = {'duckdb_path': tmp_duckdb_path}
        return mmr

    def _add_amount_proposal(self, tmp_duckdb_path, amount):
        from trader.data.proposal_store import ProposalStore
        from trader.trading.proposal import ExecutionSpec, TradeProposal
        store = ProposalStore(tmp_duckdb_path)
        pid = store.add(TradeProposal(
            symbol='AMD', action='BUY', amount=amount,
            execution=ExecutionSpec(order_type='MARKET'),
        ))
        return store, pid

    def _wire_market(self, mock_client, last_price):
        mock_client.rpc.return_value.resolve_symbol.return_value = [FakeSecurityDefinition()]
        ticker = FakeTicker()
        ticker.last = last_price
        mock_client.rpc.return_value.get_snapshot.return_value = ticker
        mock_client.rpc.return_value.get_fx_rates.return_value = {'USD': 1.0}

    def test_amount_below_price_fails_proposal_never_bumps_to_one(self, tmp_duckdb_path):
        """BRK.A-class regression: a $5000 amount on a $700k stock must FAIL
        the proposal, never place a 1-share order."""
        store, pid = self._add_amount_proposal(tmp_duckdb_path, amount=5000.0)

        mock_client = _make_mock_rpc()
        self._wire_market(mock_client, last_price=700_000.0)

        mmr = self._make_mmr(mock_client, tmp_duckdb_path)
        result = mmr.approve(pid)

        assert result.success_fail == SuccessFailEnum.FAIL
        assert '5000' in (result.error or '')
        assert '700000' in (result.error or '')
        assert store.get(pid).status == 'FAILED'
        mock_client.rpc.return_value.place_expressive_order.assert_not_called()

    def test_amount_conversion_floors_never_rounds_up(self, tmp_duckdb_path):
        """$5000 at $140 is 35.71 shares: round() would order 36 ($5040 —
        exceeding the sized notional); floor must order 35."""
        store, pid = self._add_amount_proposal(tmp_duckdb_path, amount=5000.0)

        mock_client = _make_mock_rpc()
        self._wire_market(mock_client, last_price=140.0)
        mock_client.rpc.return_value.place_expressive_order.return_value = (
            SuccessFail.success(obj=[]))

        mmr = self._make_mmr(mock_client, tmp_duckdb_path)
        result = mmr.approve(pid)

        assert result.success_fail == SuccessFailEnum.SUCCESS
        placed = mock_client.rpc.return_value.place_expressive_order.call_args
        assert placed.kwargs['quantity'] == 35.0
        assert store.get(pid).status == 'EXECUTED'

    def _add_opt_proposal(self, tmp_duckdb_path, amount):
        from trader.data.proposal_store import ProposalStore
        from trader.trading.proposal import ExecutionSpec, TradeProposal
        store = ProposalStore(tmp_duckdb_path)
        pid = store.add(TradeProposal(
            symbol='AAPL', action='BUY', amount=amount, sec_type='OPT',
            execution=ExecutionSpec(order_type='MARKET'),
        ))
        return store, pid

    def _wire_opt_market(self, mock_client, premium, multiplier='100'):
        sec = FakeSecurityDefinition(symbol='AAPL', secType='OPT')
        sec.multiplier = multiplier
        mock_client.rpc.return_value.resolve_symbol.return_value = [sec]
        ticker = FakeTicker()
        ticker.last = premium
        mock_client.rpc.return_value.get_snapshot.return_value = ticker
        mock_client.rpc.return_value.get_fx_rates.return_value = {'USD': 1.0}

    def test_option_multiplier_prevents_100x_oversize(self, tmp_duckdb_path):
        """An OPT at $5 premium with multiplier 100 costs $500/contract. A $300
        amount cannot cover a single contract → the proposal must FAIL, never
        place 60 contracts ($30k actual) by dividing $300 by the bare premium."""
        store, pid = self._add_opt_proposal(tmp_duckdb_path, amount=300.0)

        mock_client = _make_mock_rpc()
        self._wire_opt_market(mock_client, premium=5.0, multiplier='100')

        mmr = self._make_mmr(mock_client, tmp_duckdb_path)
        result = mmr.approve(pid)

        assert result.success_fail == SuccessFailEnum.FAIL
        assert store.get(pid).status == 'FAILED'
        mock_client.rpc.return_value.place_expressive_order.assert_not_called()

    def test_option_multiplier_sizes_by_contract_notional(self, tmp_duckdb_path):
        """$500 at a $5 premium × 100 multiplier is exactly 1 contract ($500),
        not 100 contracts ($50k). Proves contracts × premium × multiplier <= amount."""
        store, pid = self._add_opt_proposal(tmp_duckdb_path, amount=500.0)

        mock_client = _make_mock_rpc()
        self._wire_opt_market(mock_client, premium=5.0, multiplier='100')
        mock_client.rpc.return_value.place_expressive_order.return_value = (
            SuccessFail.success(obj=[]))

        mmr = self._make_mmr(mock_client, tmp_duckdb_path)
        result = mmr.approve(pid)

        assert result.success_fail == SuccessFailEnum.SUCCESS
        placed = mock_client.rpc.return_value.place_expressive_order.call_args
        assert placed.kwargs['quantity'] == 1.0
        assert store.get(pid).status == 'EXECUTED'

    def test_stock_multiplier_one_unchanged(self, tmp_duckdb_path):
        """Control: a stock (no multiplier) still sizes on bare price — $5000 at
        $140 → 35 shares, identical to pre-multiplier behaviour."""
        store, pid = self._add_amount_proposal(tmp_duckdb_path, amount=5000.0)

        mock_client = _make_mock_rpc()
        self._wire_market(mock_client, last_price=140.0)  # FakeSecurityDefinition has no multiplier
        mock_client.rpc.return_value.place_expressive_order.return_value = (
            SuccessFail.success(obj=[]))

        mmr = self._make_mmr(mock_client, tmp_duckdb_path)
        result = mmr.approve(pid)

        assert result.success_fail == SuccessFailEnum.SUCCESS
        placed = mock_client.rpc.return_value.place_expressive_order.call_args
        assert placed.kwargs['quantity'] == 35.0


class TestExecuteResizeGrowProtection:
    """Resize delegates protection coordination to the server.

    Cached positions and an accepted delta are never authority for cancelling
    protection on the client. Unsupported grows are explicitly deferred.
    """

    def _grow_plan(self):
        return {'adjustments': [{
            'symbol': 'AMD', 'conId': 4391,
            'current_qty': 100, 'target_qty': 150, 'delta_qty': 50, 'action': 'BUY',
            'associated_orders': [{
                'orderId': 7, 'orderType': 'STP', 'action': 'SELL',
                'auxPrice': 140.0, 'lmtPrice': 0.0, 'trailingPercent': 0.0, 'tif': 'GTC',
            }],
        }]}

    def _trim_plan(self):
        return {'adjustments': [{
            'symbol': 'AMD', 'conId': 4391,
            'current_qty': 100, 'target_qty': 60, 'delta_qty': -40, 'action': 'SELL',
            'associated_orders': [{
                'orderId': 7, 'orderType': 'STP', 'action': 'SELL',
                'auxPrice': 140.0, 'lmtPrice': 0.0, 'trailingPercent': 0.0, 'tif': 'GTC',
            }],
        }]}

    def _wire(self, mock_client, held_qty):
        # Delta market order resolves on SUBMISSION.
        mock_client.rpc.return_value.place_order_simple.return_value = SuccessFail.success(obj=None)
        # Real predicate: place_standalone_order refuses a protective larger than held.
        def _standalone(*args, **kwargs):
            if kwargs.get('quantity', 0) > held_qty:
                return SuccessFail.fail(error='order would not reduce the live position')
            return SuccessFail.success(obj=None)
        mock_client.rpc.return_value.place_standalone_order.side_effect = _standalone
        mock_client.rpc.return_value.cancel_order.return_value = SuccessFail.success()
        mock_client.rpc.return_value.resize_position.return_value = SuccessFail.fail(
            error='DEFERRED: coordinated resize supports reductions; use a reviewed proposal to grow')

    def test_grow_unfilled_leaves_old_protective_uncancelled(self):
        """Broker still shows the OLD (100) qty at re-create time — the delta
        hasn't filled. The old STP must NOT be cancelled (no naked position)."""
        mock_client = _make_mock_rpc()
        self._wire(mock_client, held_qty=100)

        mmr = _make_mmr_with_mock(mock_client)
        mmr._contract_map['AMD'] = FakeContract(conId=4391, symbol='AMD')
        # Fill never confirms within the poll window (e.g. outside RTH).
        mmr._await_grown_position = MagicMock(return_value=False)
        mmr._live_position_qty = MagicMock(return_value=100)
        mmr.cancel = MagicMock()

        results = mmr.execute_resize_plan(self._grow_plan())

        # Old protective retained: cancel never called, no re-create attempted.
        mmr.cancel.assert_not_called()
        mock_client.rpc.return_value.place_standalone_order.assert_not_called()
        assert not results['successes']
        assert any('DEFERRED' in failure for failure in results['failures'])
        mock_client.rpc.return_value.place_order_simple.assert_not_called()

    def test_already_achieved_target_preserves_existing_protection(self):
        """A server-confirmed unchanged target does not rewrite protection."""
        mock_client = _make_mock_rpc()
        self._wire(mock_client, held_qty=150)  # fill landed
        mock_client.rpc.return_value.resize_position.return_value = SuccessFail.success(
            obj={'status': 'UNCHANGED', 'order_ids': []})

        mmr = _make_mmr_with_mock(mock_client)
        mmr._contract_map['AMD'] = FakeContract(conId=4391, symbol='AMD')
        mmr._await_grown_position = MagicMock(return_value=True)
        mmr.cancel = MagicMock(return_value=SuccessFail.success())

        results = mmr.execute_resize_plan(self._grow_plan())

        mmr.cancel.assert_not_called()
        mock_client.rpc.return_value.place_standalone_order.assert_not_called()
        assert any('UNCHANGED' in s for s in results['successes'])
        assert not results['failures']

    def test_trim_submission_retains_identity_without_client_side_handoff(self):
        """An accepted coordinated trim remains execution-pending."""
        mock_client = _make_mock_rpc()
        self._wire(mock_client, held_qty=100)
        mock_client.rpc.return_value.resize_position.return_value = SuccessFail.success(
            obj={'status': 'SUBMITTED', 'order_ids': [42]})

        mmr = _make_mmr_with_mock(mock_client)
        mmr._contract_map['AMD'] = FakeContract(conId=4391, symbol='AMD')
        mmr._await_grown_position = MagicMock()
        mmr.cancel = MagicMock(return_value=SuccessFail.success())

        plan = self._trim_plan()
        results = mmr.execute_resize_plan(plan)

        mmr._await_grown_position.assert_not_called()
        mmr.cancel.assert_not_called()
        mock_client.rpc.return_value.place_standalone_order.assert_not_called()
        placed = mock_client.rpc.return_value.resize_position.call_args
        assert placed.kwargs['target_quantity'] == 60
        assert placed.kwargs['client_intent_id'] == plan['adjustments'][0]['client_intent_id']
        assert plan['adjustments'][0]['order_ids'] == [42]
        assert any('SUBMITTED' in s for s in results['successes'])
        assert any('execution pending' in s for s in results['warnings'])


class TestResolveContractForexRouting:
    """CASH resolutions must come out on IDEALPRO, never SMART-routed.

    Live 2026-07-27: approve of an EUR.USD proposal built exchange=SMART /
    primaryExchange=IDEALPRO (the non-US-stock SMART rule applied to forex),
    IB answered error 200 and cancelled the order. The direct buy path builds
    Forex on IDEALPRO explicitly and always worked — the two paths must agree.
    """

    def test_cash_resolution_stays_on_idealpro(self):
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        from trader.sdk import MMR
        mmr = MMR.__new__(MMR)
        sec = SimpleNamespace(conId=12087792, symbol='EUR', secType='CASH',
                              exchange='IDEALPRO', primaryExchange='',
                              currency='USD', multiplier='')
        mmr.resolve = MagicMock(return_value=[sec])
        c = mmr._resolve_contract('EUR', sec_type='CASH', currency='USD')
        assert c.exchange == 'IDEALPRO'
        assert (c.primaryExchange or '') == ''
        assert c.secType == 'CASH'

    def test_asx_stock_still_smart_routed(self):
        """The stock rule the carve-out must not disturb."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        from trader.sdk import MMR
        mmr = MMR.__new__(MMR)
        sec = SimpleNamespace(conId=4036812, symbol='BHP', secType='STK',
                              exchange='ASX', primaryExchange='ASX',
                              currency='AUD', multiplier='', validExchanges='ASX,SMART')
        mmr.resolve = MagicMock(return_value=[sec])
        c = mmr._resolve_contract('BHP', sec_type='STK', exchange='ASX', currency='AUD')
        assert c.exchange == 'SMART'
        assert c.primaryExchange == 'ASX'


@pytest.fixture
def native_definition_multiplier_sdk(tmp_path, monkeypatch):
    """Native catalogue, proposal/approval and server checks with offline quotes."""
    from types import SimpleNamespace
    from ib_async import Contract, ContractDetails, Ticker
    from review.test_review_order_contract import _coordinated_trader
    from trader.data.data_access import SecurityDefinition
    from trader.data.proposal_store import ProposalStore
    from trader.data.universe import Universe, UniverseAccessor
    from trader.messaging.trader_service_api import TraderServiceApi
    from trader.trading.risk_gate import RiskGate, RiskLimits

    server = _coordinated_trader(tmp_path, held=0)
    server.risk_gate = RiskGate(RiskLimits(), server.event_store)
    server.require_proposal_approval = True
    api = TraderServiceApi(server)
    proposals = ProposalStore(server.duckdb_path)
    catalogue_path = str(tmp_path / 'native_multiplier_catalogue.duckdb')
    ctx = SimpleNamespace(server=server, proposals=proposals, price=10.0, calls=[])

    def install(sec_type, multiplier, price, *, legacy=False):
        contract = Contract(conId=200, symbol='MULTIPLIER_NATIVE', secType=sec_type,
                            exchange='CME' if sec_type == 'FUT' else 'SMART',
                            currency='USD', multiplier=multiplier)
        definition = SecurityDefinition.from_contract_details(ContractDetails(contract=contract))
        if legacy:
            # Explicit dated storage shape: objects persisted before this field
            # existed have no multiplier in their instance state. Do not invent
            # one from the current broker definition when reopening that record.
            vars(definition).pop('multiplier', None)
        writer = UniverseAccessor(catalogue_path, 'native_multiplier')
        writer.update(Universe('native_multiplier', [definition]))
        # Real DuckDB/dill reload, followed by the real local resolver cache.
        reader = UniverseAccessor(catalogue_path, 'native_multiplier')
        restored, = reader.get('native_multiplier').security_definitions
        monkeypatch.setattr(server, 'universe_accessor', reader, raising=False)
        ctx.contract, ctx.definition, ctx.restored = contract, definition, restored
        ctx.price = price
        return restored

    def quote(contract):
        ticker = Ticker(contract=contract)
        # Native __post_init__ clears constructor prices before quotes arrive.
        ticker.last = ticker.bid = ticker.ask = ctx.price
        return ticker

    def resolve_symbol(symbol, exchange='', universe='', sec_type=''):
        return asyncio.run(api.resolve_symbol(symbol, exchange, universe, sec_type))

    def place_expressive_order(**kwargs):
        ctx.calls.append(dict(kwargs))
        return asyncio.run(api.place_expressive_order(**kwargs))

    rpc = SimpleNamespace(
        resolve_symbol=resolve_symbol,
        resolve_contract=lambda contract: [],
        get_snapshot=lambda contract, delayed: quote(contract),
        get_fx_rates=lambda: {'USD': 1.0},
        get_account_values=lambda: {},
        place_expressive_order=place_expressive_order,
    )
    sdk = MMR.__new__(MMR)
    sdk._prop_store = proposals
    sdk._client = SimpleNamespace(is_setup=True, rpc=lambda **kwargs: rpc)
    monkeypatch.setattr(server.client.get_snapshot, 'side_effect', lambda contract: quote(contract))
    monkeypatch.setattr(server.client.ib, 'tickers', lambda: [quote(ctx.contract)])
    ctx.sdk, ctx.install = sdk, install
    try:
        yield ctx
    finally:
        server.order_tracker.close(timeout=1)
        if server.order_tracker._journal is not None:
            server.order_tracker._journal.close()
        journal = getattr(server, '_server_order_journal', None)
        if journal is not None:
            journal.journal.close()
        if server.order_tracker._temporary is not None:
            server.order_tracker._temporary.cleanup()


@pytest.mark.parametrize('sec_type,multiplier,price,amount,expected_quantity', [
    pytest.param('OPT', '100', 5.0, 500.0, 1.0, id='option'),
    pytest.param('FUT', '50', 10.0, 1000.0, 2.0, id='future'),
    pytest.param('STK', '', 10.0, 100.0, 10.0, id='stock'),
])
def test_native_factory_multiplier_survives_catalogue_and_amount_approval(
        native_definition_multiplier_sdk, sec_type, multiplier, price, amount, expected_quantity):
    from trader.data.data_access import SecurityDefinition
    from trader.data.universe import Universe

    ctx = native_definition_multiplier_sdk
    restored = ctx.install(sec_type, multiplier, price)
    proposal_id, _, _ = ctx.sdk.propose(
        ctx.contract.symbol, 'BUY', amount=amount, sec_type=sec_type,
        exchange=ctx.contract.exchange, currency='USD', metadata={'con_id': 200})
    result = ctx.sdk.approve(proposal_id)

    assert result.is_success(), result.error
    assert getattr(restored, 'multiplier', '') == multiplier
    resolved = ctx.sdk._resolve_contract(200, sec_type=sec_type,
                                         exchange=ctx.contract.exchange, currency='USD')
    assert resolved.multiplier == multiplier and resolved.secType == sec_type
    for convert in (SecurityDefinition.to_contract, Universe.to_contract):
        converted = convert(restored)
        assert (converted.conId, converted.secType, converted.multiplier) == (200, sec_type, multiplier)
        assert convert(ctx.contract) is ctx.contract
    assert len(ctx.calls) == len(ctx.server.placed) == 1
    placed, = ctx.server.placed
    assert placed.contract.conId == 200 and placed.contract.multiplier == multiplier
    assert placed.order.totalQuantity == expected_quantity
    assert ctx.calls[0]['quantity'] == expected_quantity
    assert expected_quantity * price * float(multiplier or 1) <= amount
    stored = ctx.proposals.get(proposal_id)
    assert stored.status == 'EXECUTED'
    assert stored.metadata['submission_quantity'] == expected_quantity
    assert stored.order_ids == [placed.order.orderId]


@pytest.mark.parametrize('sec_type,multiplier', [
    pytest.param('OPT', '', id='missing'),
    pytest.param('FUT', 'not-a-number', id='nonnumeric'),
    pytest.param('OPT', '0', id='zero'),
    pytest.param('FUT', 'nan', id='nan'),
    pytest.param('OPT', 'inf', id='infinite'),
])
def test_native_invalid_derivative_multiplier_still_refuses_amount(
        native_definition_multiplier_sdk, sec_type, multiplier):
    ctx = native_definition_multiplier_sdk
    ctx.install(sec_type, multiplier, 5.0)
    proposal_id, _, _ = ctx.sdk.propose(
        ctx.contract.symbol, 'BUY', amount=500.0, sec_type=sec_type,
        exchange=ctx.contract.exchange, currency='USD', metadata={'con_id': 200})
    result = ctx.sdk.approve(proposal_id)

    assert not result.is_success()
    assert isinstance(result.exception, ValueError)
    assert 'multiplier' in str(result.error)
    assert ctx.proposals.get(proposal_id).status == 'FAILED'
    assert 'submission_quantity' not in ctx.proposals.get(proposal_id).metadata
    assert ctx.calls == [] and ctx.server.placed == []


@pytest.mark.parametrize('sec_type', ['OPT', 'STK'])
def test_legacy_definition_without_multiplier_reopens_without_inventing_one(
        native_definition_multiplier_sdk, sec_type):
    from dataclasses import replace
    from ib_async import ContractDetails
    from trader.data.data_access import SecurityDefinition
    from trader.data.universe import Universe

    ctx = native_definition_multiplier_sdk
    restored = ctx.install(sec_type, '100' if sec_type == 'OPT' else '', 5.0, legacy=True)
    assert 'multiplier' not in vars(restored)
    assert getattr(restored, 'multiplier', '') == ''
    fresh_blank = SecurityDefinition.from_contract_details(
        ContractDetails(contract=replace(ctx.contract, multiplier='')))
    assert restored == fresh_blank and len({restored, fresh_blank}) == 1
    resolved = ctx.sdk._resolve_contract(200, sec_type=sec_type,
                                         exchange=ctx.contract.exchange, currency='USD')
    assert resolved.multiplier == ''
    assert SecurityDefinition.to_contract(restored).multiplier == ''
    assert Universe.to_contract(restored).multiplier == ''
    proposal_id, _, _ = ctx.sdk.propose(
        ctx.contract.symbol, 'BUY', amount=500.0, sec_type=sec_type,
        exchange=ctx.contract.exchange, currency='USD', metadata={'con_id': 200})
    result = ctx.sdk.approve(proposal_id)

    if sec_type == 'OPT':
        assert not result.is_success() and 'multiplier' in str(result.error)
        assert ctx.proposals.get(proposal_id).status == 'FAILED'
        assert ctx.calls == [] and ctx.server.placed == []
    else:
        assert result.is_success(), result.error
        assert len(ctx.server.placed) == 1
        assert ctx.server.placed[0].order.totalQuantity == 100
        assert ctx.proposals.get(proposal_id).status == 'EXECUTED'


def _assert_native_catalogue_conflict(resolve):
    error = None
    try:
        resolve()
    except ValueError as caught:
        error = caught
    assert error is not None, 'Conflicting catalogue multipliers were accepted'
    assert 'Conflicting multipliers' in str(error)
    assert 'conId 200' in str(error)


def _install_native_catalogue_copies(ctx, multipliers, *, sec_type='OPT'):
    """Persist actual ContractDetails with two venues for one exact listing."""
    from ib_async import Contract, ContractDetails
    from trader.data.data_access import SecurityDefinition
    from trader.data.universe import Universe

    ctx.install(sec_type, multipliers[0], 5.0)
    definitions = [SecurityDefinition.from_contract_details(ContractDetails(
        contract=Contract(conId=200, symbol=ctx.contract.symbol, secType=sec_type,
                          currency='USD', exchange=venue, primaryExchange='NASDAQ',
                          multiplier=multiplier)))
        for venue, multiplier in zip(('SMART', 'NASDAQ'), multipliers)]
    accessor = ctx.server.universe_accessor
    accessor.update(Universe('native_multiplier', definitions))
    return accessor, definitions


@pytest.mark.parametrize('multipliers', [
    pytest.param(('100', '50'), id='known-conflict'),
    pytest.param(('50', '100'), id='known-reversed'),
    pytest.param(('', '100'), id='missing-known'),
    pytest.param(('100', ''), id='known-missing'),
])
def test_native_catalogue_conflict_precedes_first_only(
        native_definition_multiplier_sdk, multipliers):
    ctx = native_definition_multiplier_sdk
    accessor, _ = _install_native_catalogue_copies(ctx, multipliers)
    calls = [
        lambda: ctx.sdk.resolve(200, sec_type='OPT'),
        lambda: ctx.sdk.resolve(ctx.contract.symbol, sec_type='OPT'),
        lambda: accessor.resolve_symbol(200, sec_type='OPT', first_only=True),
    ]
    for resolve in calls:
        accessor.invalidate_resolver_cache()
        _assert_native_catalogue_conflict(resolve)
    assert ctx.calls == [] and ctx.server.placed == []


@pytest.mark.parametrize('multipliers', [
    pytest.param(('100', '50'), id='known-conflict'),
    pytest.param(('50', '100'), id='known-reversed'),
    pytest.param(('', '100'), id='missing-known'),
    pytest.param(('100', ''), id='known-missing'),
])
def test_native_catalogue_filtered_warmup_cannot_hide_conflict(
        native_definition_multiplier_sdk, multipliers):
    ctx = native_definition_multiplier_sdk
    _install_native_catalogue_copies(ctx, multipliers)
    # A scoped symbol lookup is legitimate, but cannot seed a singleton for
    # a subsequent unfiltered exact-ID request that has two matching records.
    selected = ctx.sdk.resolve(ctx.contract.symbol, sec_type='OPT', exchange='SMART')
    assert len(selected) == 1 and selected[0].exchange == 'SMART'
    _assert_native_catalogue_conflict(lambda: ctx.sdk.resolve(200, sec_type='OPT'))
    assert ctx.calls == [] and ctx.server.placed == []


@pytest.mark.parametrize('sec_type,multipliers,expected_quantity', [
    pytest.param('OPT', ('100', '100'), 1.0, id='identical-option'),
    pytest.param('OPT', ('100', '100.0'), 1.0, id='numeric-equivalent'),
    pytest.param('OPT', ('', ''), None, id='missing-option'),
    pytest.param('STK', ('', ''), 100.0, id='default-stock'),
])
def test_native_catalogue_consistent_copies_keep_amount_contract(
        native_definition_multiplier_sdk, sec_type, multipliers, expected_quantity):
    ctx = native_definition_multiplier_sdk
    _install_native_catalogue_copies(ctx, multipliers, sec_type=sec_type)
    proposal_id, _, _ = ctx.sdk.propose(
        ctx.contract.symbol, 'BUY', amount=500.0, sec_type=sec_type,
        currency='USD', metadata={'con_id': 200})
    result = ctx.sdk.approve(proposal_id)
    if expected_quantity is None:
        assert not result.is_success()
        assert isinstance(result.exception, ValueError) and 'multiplier' in str(result.error)
        assert ctx.calls == [] and ctx.server.placed == []
        assert ctx.proposals.get(proposal_id).status == 'FAILED'
    else:
        assert result.is_success(), result.error
        assert len(ctx.calls) == len(ctx.server.placed) == 1
        placed, = ctx.server.placed
        assert placed.order.totalQuantity == expected_quantity
        assert placed.contract.conId == 200 and placed.contract.secType == sec_type
        assert float(placed.contract.multiplier or 1) == float(multipliers[0] or 1)
        assert ctx.proposals.get(proposal_id).metadata['submission_quantity'] == expected_quantity


def test_native_catalogue_first_only_does_not_truncate_cached_candidates(
        native_definition_multiplier_sdk):
    ctx = native_definition_multiplier_sdk
    accessor, _ = _install_native_catalogue_copies(ctx, ('', ''), sec_type='STK')
    first = accessor.resolve_symbol(200, sec_type='STK', first_only=True)
    assert len(first) == 1 and first[0].exchange == 'SMART'
    complete = accessor.resolve_symbol(200, sec_type='STK')
    assert [row.exchange for row in complete] == ['SMART', 'NASDAQ']
    complete.clear()
    assert [row.exchange for row in accessor.resolve_symbol(200, sec_type='STK')] == ['SMART', 'NASDAQ']
    named = accessor.resolve_universe_name(200, sec_type='STK')
    assert [(name, row.exchange) for name, row in named] == [
        ('native_multiplier', 'SMART'), ('native_multiplier', 'NASDAQ')]


def test_native_catalogue_service_preserves_type_and_universe_filters(
        native_definition_multiplier_sdk):
    from ib_async import Contract, ContractDetails
    from trader.data.data_access import SecurityDefinition
    from trader.data.universe import Universe

    ctx = native_definition_multiplier_sdk
    ctx.install('STK', '', 5.0)
    accessor = ctx.server.universe_accessor
    option = SecurityDefinition.from_contract_details(ContractDetails(contract=Contract(
        conId=200, symbol=ctx.contract.symbol, secType='OPT', currency='USD',
        exchange='CBOE', multiplier='100')))
    accessor.update(Universe('option_only', [option]))
    # This warms the existing stock query before requesting another type.
    stock = ctx.sdk.resolve(200, sec_type='STK', universe='native_multiplier')
    assert len(stock) == 1 and stock[0].secType == 'STK'
    option_rows = ctx.sdk.resolve(200, sec_type='OPT')
    assert len(option_rows) == 1 and option_rows[0].secType == 'OPT'
    assert ctx.sdk.resolve(200, sec_type='STK', universe='option_only') == []
    assert ctx.sdk.resolve(200, sec_type='OPT', exchange='SMART') == []
    selected = ctx.sdk.resolve(200, sec_type='OPT', exchange='CBOE', universe='option_only')
    assert len(selected) == 1 and selected[0].conId == 200
    assert ctx.calls == [] and ctx.server.placed == []


def test_native_catalogue_numeric_ticker_is_distinct_from_integer_id(
        native_definition_multiplier_sdk):
    from ib_async import Contract, ContractDetails
    from trader.data.data_access import SecurityDefinition
    from trader.data.universe import Universe

    ctx = native_definition_multiplier_sdk
    ctx.install('STK', '', 5.0)
    accessor = ctx.server.universe_accessor
    numeric_ticker = SecurityDefinition.from_contract_details(ContractDetails(contract=Contract(
        conId=700, symbol='200', secType='STK', currency='HKD', exchange='SEHK')))
    accessor.update(Universe('numeric_tickers', [numeric_ticker]))
    for _ in range(2):
        assert [row.conId for row in ctx.sdk.resolve('200', sec_type='STK')] == [700]
        assert [row.conId for row in ctx.sdk.resolve(200, sec_type='STK')] == [200]
    assert ctx.calls == [] and ctx.server.placed == []


def test_native_catalogue_update_invalidates_complete_query_result(
        native_definition_multiplier_sdk):
    from dataclasses import replace
    from trader.data.universe import Universe

    ctx = native_definition_multiplier_sdk
    accessor, definitions = _install_native_catalogue_copies(ctx, ('', ''), sec_type='STK')
    before = ctx.sdk.resolve(200, sec_type='STK')
    assert len(before) == 1 and before[0].exchange == 'SMART'
    accessor.update(Universe('native_multiplier', [replace(definitions[0], exchange='NYSE')]))
    after = ctx.sdk.resolve(200, sec_type='STK')
    assert len(after) == 1 and after[0].exchange == 'NYSE'
    assert ctx.sdk.resolve(200, sec_type='STK', exchange='SMART') == []
    accessor.delete('native_multiplier')
    assert ctx.sdk.resolve(200, sec_type='STK') == []


@pytest.mark.parametrize('multipliers', [
    pytest.param(('100', '50'), id='known-conflict'),
    pytest.param(('', '100'), id='missing-known'),
])
def test_native_discovery_refuses_multiplier_conflict_before_venue_dedup(
        native_definition_multiplier_sdk, monkeypatch, multipliers):
    from ib_async import Contract, ContractDetails
    from trader.messaging.trader_service_api import TraderServiceApi

    ctx = native_definition_multiplier_sdk
    ctx.install('OPT', '100', 5.0)
    ctx.server.universe_accessor.delete('native_multiplier')
    details = [ContractDetails(contract=Contract(
        conId=200, symbol=ctx.contract.symbol, secType='OPT', currency='USD',
        exchange=venue, primaryExchange='NASDAQ', multiplier=multiplier))
        for venue, multiplier in zip(('SMART', 'NASDAQ'), multipliers)]
    requests = []

    async def discover(contract):
        requests.append(contract)
        return details

    monkeypatch.setattr(ctx.server.client.ib, 'reqContractDetailsAsync', discover, raising=False)
    api = TraderServiceApi(ctx.server)
    rpc = ctx.sdk._client.rpc()
    rpc.resolve_contract = lambda contract: asyncio.run(api.resolve_contract(contract))
    _assert_native_catalogue_conflict(lambda: ctx.sdk.resolve(ctx.contract.symbol, sec_type='OPT'))
    assert len(requests) == 1 and requests[0].secType == 'OPT'
    assert ctx.calls == [] and ctx.server.placed == []
