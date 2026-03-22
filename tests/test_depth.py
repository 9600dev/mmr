"""Tests for market depth: chart rendering, async behavior, and concurrency."""

import asyncio
import os
import time
from unittest.mock import MagicMock, patch

import pytest

from ib_async import Contract, DOMLevel, Ticker

from trader.tools.depth_chart import render_depth_chart, render_depth_table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_depth_data(num_bids=5, num_asks=5, base_price=150.0, spread=0.02):
    """Generate synthetic depth data for testing."""
    bids = []
    asks = []
    for i in range(num_bids):
        bids.append({
            'price': base_price - spread * (i + 1),
            'size': (num_bids - i) * 100,
            'marketMaker': '',
        })
    for i in range(num_asks):
        asks.append({
            'price': base_price + spread * (i + 1),
            'size': (num_asks - i) * 80,
            'marketMaker': '',
        })
    best_bid = bids[0]['price'] if bids else float('nan')
    best_ask = asks[0]['price'] if asks else float('nan')
    return {
        'symbol': 'TEST',
        'conId': 12345,
        'bids': bids,
        'asks': asks,
        'last': base_price,
        'bid': best_bid,
        'ask': best_ask,
        'close': base_price - 1.0,
        'time': None,
    }


def _make_dom_levels(base_price=150.0, spread=0.01, levels=5):
    """Build lists of DOMLevel objects for bid/ask sides."""
    bids = [
        DOMLevel(price=base_price - spread * (i + 1), size=(levels - i) * 100, marketMaker='')
        for i in range(levels)
    ]
    asks = [
        DOMLevel(price=base_price + spread * (i + 1), size=(levels - i) * 80, marketMaker='')
        for i in range(levels)
    ]
    return bids, asks


def _make_ibrx_with_mock_ib():
    """Create an IBAIORx instance with a mocked IB connection.

    Returns (ibrx, mock_ib) so tests can configure mock_ib behavior.
    """
    from trader.listeners.ibreactive import IBAIORx

    ibrx = IBAIORx.__new__(IBAIORx)
    ibrx.ib = MagicMock()
    return ibrx, ibrx.ib


def _fake_req_depth_with_immediate_delivery(bids, asks, delay=0.01):
    """Create a fake reqMktDepth that delivers data after a tiny delay.

    The delay ensures the updateEvent callback is connected before data arrives,
    matching real IB behavior (network round-trip > 0).
    """
    def fake_req_depth(contract, numRows=5, isSmartDepth=False):
        ticker = Ticker()

        async def _deliver():
            await asyncio.sleep(delay)
            ticker.domBids = bids
            ticker.domAsks = asks
            ticker.updateEvent.emit(ticker)

        asyncio.get_event_loop().create_task(_deliver())
        return ticker

    return fake_req_depth


# ---------------------------------------------------------------------------
# Rendering tests
# ---------------------------------------------------------------------------

class TestRenderDepthChart:
    def test_produces_png_file(self, tmp_path):
        data = _make_depth_data()
        out = str(tmp_path / 'depth.png')
        result = render_depth_chart(data, out)
        assert os.path.exists(result)
        assert os.path.getsize(result) > 0

    def test_returns_absolute_path(self, tmp_path):
        data = _make_depth_data()
        out = str(tmp_path / 'depth.png')
        result = render_depth_chart(data, out)
        assert os.path.isabs(result)

    def test_creates_parent_directories(self, tmp_path):
        data = _make_depth_data()
        out = str(tmp_path / 'sub' / 'dir' / 'depth.png')
        result = render_depth_chart(data, out)
        assert os.path.exists(result)

    def test_empty_bids_and_asks(self, tmp_path):
        data = _make_depth_data(num_bids=0, num_asks=0)
        out = str(tmp_path / 'empty.png')
        result = render_depth_chart(data, out)
        assert os.path.exists(result)
        assert os.path.getsize(result) > 0

    def test_single_level(self, tmp_path):
        data = _make_depth_data(num_bids=1, num_asks=1)
        out = str(tmp_path / 'single.png')
        result = render_depth_chart(data, out)
        assert os.path.exists(result)

    def test_only_bids(self, tmp_path):
        data = _make_depth_data(num_bids=3, num_asks=0)
        out = str(tmp_path / 'bids_only.png')
        result = render_depth_chart(data, out)
        assert os.path.exists(result)

    def test_only_asks(self, tmp_path):
        data = _make_depth_data(num_bids=0, num_asks=3)
        out = str(tmp_path / 'asks_only.png')
        result = render_depth_chart(data, out)
        assert os.path.exists(result)

    def test_wide_spread(self, tmp_path):
        data = _make_depth_data(spread=5.0)
        out = str(tmp_path / 'wide.png')
        result = render_depth_chart(data, out)
        assert os.path.exists(result)

    def test_nan_last_price(self, tmp_path):
        data = _make_depth_data()
        data['last'] = float('nan')
        out = str(tmp_path / 'nan_last.png')
        result = render_depth_chart(data, out)
        assert os.path.exists(result)

    def test_many_levels(self, tmp_path):
        data = _make_depth_data(num_bids=20, num_asks=20)
        out = str(tmp_path / 'many.png')
        result = render_depth_chart(data, out)
        assert os.path.exists(result)


class TestRenderDepthTable:
    def test_returns_rich_table(self):
        from rich.table import Table
        data = _make_depth_data()
        table = render_depth_table(data)
        assert isinstance(table, Table)

    def test_table_has_title(self):
        data = _make_depth_data()
        table = render_depth_table(data)
        assert 'TEST' in table.title

    def test_table_has_caption_with_spread(self):
        data = _make_depth_data()
        table = render_depth_table(data)
        assert table.caption is not None
        assert 'Spread' in table.caption

    def test_table_column_count(self):
        data = _make_depth_data()
        table = render_depth_table(data)
        assert len(table.columns) == 7

    def test_table_row_count(self):
        data = _make_depth_data(num_bids=3, num_asks=5)
        table = render_depth_table(data)
        assert table.row_count == 3 + 5 + 1

    def test_empty_data(self):
        data = _make_depth_data(num_bids=0, num_asks=0)
        table = render_depth_table(data)
        assert table.row_count == 0

    def test_uneven_sides(self):
        data = _make_depth_data(num_bids=2, num_asks=7)
        table = render_depth_table(data)
        assert table.row_count == 2 + 7 + 1

    def test_nan_prices(self):
        data = _make_depth_data()
        data['bid'] = float('nan')
        data['ask'] = float('nan')
        data['last'] = float('nan')
        table = render_depth_table(data)
        assert table is not None

    def test_bid_ask_ratio_in_caption(self):
        data = _make_depth_data()
        table = render_depth_table(data)
        assert 'B/A Ratio' in table.caption


# ---------------------------------------------------------------------------
# Async behavior tests — IBAIORx.get_market_depth
# ---------------------------------------------------------------------------

class TestGetMarketDepthAsync:
    """Verify get_market_depth is event-driven and non-blocking."""

    @pytest.mark.asyncio
    async def test_returns_promptly_on_data(self):
        """When depth data arrives quickly, should resolve fast — no polling delay."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()
        bids, asks = _make_dom_levels()

        mock_ib.reqMktDepth.side_effect = _fake_req_depth_with_immediate_delivery(bids, asks)

        contract = Contract(conId=12345, symbol='TEST', secType='STK',
                            exchange='ASX', currency='AUD')

        start = time.monotonic()
        result = await ibrx.get_market_depth(contract)
        elapsed = time.monotonic() - start

        assert elapsed < 0.2, f'Should resolve promptly, took {elapsed:.3f}s'
        assert len(result['bids']) == 5
        assert len(result['asks']) == 5
        assert result['symbol'] == 'TEST'
        mock_ib.cancelMktDepth.assert_called_once()

    @pytest.mark.asyncio
    async def test_waits_for_delayed_data(self):
        """Data arrives after a delay — should resolve promptly after delivery."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()
        bids, asks = _make_dom_levels()

        mock_ib.reqMktDepth.side_effect = _fake_req_depth_with_immediate_delivery(
            bids, asks, delay=0.2
        )

        contract = Contract(conId=12345, symbol='TEST', secType='STK',
                            exchange='ASX', currency='AUD')

        start = time.monotonic()
        result = await ibrx.get_market_depth(contract)
        elapsed = time.monotonic() - start

        # Should resolve shortly after the 200ms delivery, not at 5s timeout
        assert 0.15 < elapsed < 0.5, f'Expected ~200ms, got {elapsed:.3f}s'
        assert len(result['bids']) == 5

    @pytest.mark.asyncio
    async def test_timeout_raises(self):
        """No data arrives — should raise TimeoutError."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()

        # Return a ticker that never gets populated
        mock_ib.reqMktDepth.return_value = Ticker()

        contract = Contract(conId=99, symbol='NODATA', secType='STK',
                            exchange='ASX', currency='AUD')

        # Patch to use short timeout
        original_wait_for = asyncio.wait_for

        async def fast_wait_for(coro, timeout=None):
            return await original_wait_for(coro, timeout=0.1)

        with patch('trader.listeners.ibreactive.asyncio.wait_for', side_effect=fast_wait_for):
            with pytest.raises(TimeoutError, match='NODATA'):
                await ibrx.get_market_depth(contract)

        mock_ib.cancelMktDepth.assert_called_once()

    @pytest.mark.asyncio
    async def test_callback_cleaned_up_on_success(self):
        """updateEvent listener is removed after successful data retrieval."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()
        bids, asks = _make_dom_levels()

        # Use a shared ticker so we can inspect its event listeners
        ticker = Ticker()
        listeners_before = len(ticker.updateEvent)

        def fake_req(contract, numRows=5, isSmartDepth=False):
            async def _deliver():
                await asyncio.sleep(0.01)
                ticker.domBids = bids
                ticker.domAsks = asks
                ticker.updateEvent.emit(ticker)
            asyncio.get_event_loop().create_task(_deliver())
            return ticker

        mock_ib.reqMktDepth.side_effect = fake_req

        contract = Contract(conId=12345, symbol='TEST', secType='STK',
                            exchange='ASX', currency='AUD')

        await ibrx.get_market_depth(contract)

        listeners_after = len(ticker.updateEvent)
        assert listeners_after == listeners_before, \
            f'Listener leak: {listeners_after} listeners after, expected {listeners_before}'

    @pytest.mark.asyncio
    async def test_callback_cleaned_up_on_timeout(self):
        """updateEvent listener is removed even when timeout occurs."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()

        ticker = Ticker()
        listeners_before = len(ticker.updateEvent)
        mock_ib.reqMktDepth.return_value = ticker

        contract = Contract(conId=99, symbol='SLOW', secType='STK',
                            exchange='ASX', currency='AUD')

        original_wait_for = asyncio.wait_for

        async def fast_wait_for(coro, timeout=None):
            return await original_wait_for(coro, timeout=0.1)

        with patch('trader.listeners.ibreactive.asyncio.wait_for', side_effect=fast_wait_for):
            with pytest.raises(TimeoutError):
                await ibrx.get_market_depth(contract)

        listeners_after = len(ticker.updateEvent)
        assert listeners_after == listeners_before, \
            f'Listener leak on timeout: {listeners_after} listeners, expected {listeners_before}'

    @pytest.mark.asyncio
    async def test_smart_exchange_rewritten(self):
        """SMART exchange is rewritten to primaryExchange for depth requests."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()
        bids, asks = _make_dom_levels()

        mock_ib.reqMktDepth.side_effect = _fake_req_depth_with_immediate_delivery(bids, asks)

        contract = Contract(conId=4036812, symbol='BHP', secType='STK',
                            exchange='SMART', primaryExchange='ASX', currency='AUD')

        await ibrx.get_market_depth(contract)

        call_args = mock_ib.reqMktDepth.call_args
        actual_contract = call_args[0][0]
        assert actual_contract.exchange == 'ASX', \
            f'Expected exchange=ASX, got {actual_contract.exchange}'
        assert actual_contract.symbol == 'BHP'

    @pytest.mark.asyncio
    async def test_non_smart_exchange_preserved(self):
        """Non-SMART exchange is passed through unchanged."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()
        bids, asks = _make_dom_levels()

        mock_ib.reqMktDepth.side_effect = _fake_req_depth_with_immediate_delivery(bids, asks)

        contract = Contract(conId=265598, symbol='AAPL', secType='STK',
                            exchange='NASDAQ', currency='USD')

        await ibrx.get_market_depth(contract)

        call_args = mock_ib.reqMktDepth.call_args
        actual_contract = call_args[0][0]
        assert actual_contract.exchange == 'NASDAQ'

    @pytest.mark.asyncio
    async def test_no_ib_sleep_called(self):
        """get_market_depth must never call ib.sleep() (synchronous loop pump)."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()
        bids, asks = _make_dom_levels(levels=1)

        mock_ib.reqMktDepth.side_effect = _fake_req_depth_with_immediate_delivery(bids, asks)

        contract = Contract(conId=1, symbol='TEST', secType='STK',
                            exchange='ASX', currency='AUD')

        await ibrx.get_market_depth(contract)

        mock_ib.sleep.assert_not_called()


class TestGetMarketDepthConcurrency:
    """Verify multiple concurrent depth requests don't block each other."""

    @pytest.mark.asyncio
    async def test_concurrent_requests_run_in_parallel(self):
        """N concurrent depth requests should complete in ~1x time, not Nx."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()

        delay = 0.2  # Each request takes 200ms to get data
        n_requests = 5
        bids, asks = _make_dom_levels(levels=1)

        mock_ib.reqMktDepth.side_effect = _fake_req_depth_with_immediate_delivery(
            bids, asks, delay=delay
        )

        contracts = [
            Contract(conId=i, symbol=f'SYM{i}', secType='STK',
                     exchange='ASX', currency='AUD')
            for i in range(n_requests)
        ]

        start = time.monotonic()
        results = await asyncio.gather(
            *[ibrx.get_market_depth(c) for c in contracts]
        )
        elapsed = time.monotonic() - start

        assert len(results) == n_requests

        # If sequential, would take n * delay = 1.0s
        # If parallel, should take ~delay = ~0.2s (plus overhead)
        max_parallel_time = delay + 0.3
        assert elapsed < max_parallel_time, \
            f'Concurrent requests took {elapsed:.3f}s, expected < {max_parallel_time:.1f}s ' \
            f'(sequential would be {delay * n_requests:.1f}s)'

    @pytest.mark.asyncio
    async def test_one_timeout_doesnt_block_others(self):
        """A timing-out request must not block other concurrent requests."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()

        bids = [DOMLevel(price=100.0, size=50, marketMaker='')]
        asks = [DOMLevel(price=100.01, size=50, marketMaker='')]

        def fake_req_depth(contract, numRows=5, isSmartDepth=False):
            ticker = Ticker()
            if contract.symbol != 'TIMEOUT':
                # Deliver data after a tiny delay
                async def _deliver():
                    await asyncio.sleep(0.01)
                    ticker.domBids = bids
                    ticker.domAsks = asks
                    ticker.updateEvent.emit(ticker)
                asyncio.get_event_loop().create_task(_deliver())
            # TIMEOUT contract: never delivers data
            return ticker

        mock_ib.reqMktDepth.side_effect = fake_req_depth

        fast_contract = Contract(conId=1, symbol='FAST', secType='STK',
                                 exchange='ASX', currency='AUD')
        timeout_contract = Contract(conId=2, symbol='TIMEOUT', secType='STK',
                                    exchange='ASX', currency='AUD')

        original_wait_for = asyncio.wait_for

        async def fast_wait_for(coro, timeout=None):
            return await original_wait_for(coro, timeout=min(timeout or 5.0, 0.3))

        with patch('trader.listeners.ibreactive.asyncio.wait_for', side_effect=fast_wait_for):
            start = time.monotonic()

            fast_task = asyncio.create_task(ibrx.get_market_depth(fast_contract))
            timeout_task = asyncio.create_task(ibrx.get_market_depth(timeout_contract))

            # Fast one should complete quickly
            fast_result = await fast_task
            fast_elapsed = time.monotonic() - start
            assert fast_elapsed < 0.15, \
                f'Fast request blocked for {fast_elapsed:.3f}s — likely blocked by timeout request'

            # Timeout one should raise
            with pytest.raises(TimeoutError):
                await timeout_task

        assert fast_result['symbol'] == 'FAST'
        assert len(fast_result['bids']) == 1

    @pytest.mark.asyncio
    async def test_cancel_called_on_timeout(self):
        """cancelMktDepth must be called when the request times out."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()

        mock_ib.reqMktDepth.return_value = Ticker()

        contract = Contract(conId=1, symbol='SLOW', secType='STK',
                            exchange='ASX', currency='AUD')

        original_wait_for = asyncio.wait_for

        async def fast_wait_for(coro, timeout=None):
            return await original_wait_for(coro, timeout=0.1)

        with patch('trader.listeners.ibreactive.asyncio.wait_for', side_effect=fast_wait_for):
            with pytest.raises(TimeoutError):
                await ibrx.get_market_depth(contract)

        mock_ib.cancelMktDepth.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancel_called_on_success(self):
        """cancelMktDepth must be called after successful data retrieval."""
        ibrx, mock_ib = _make_ibrx_with_mock_ib()
        bids, asks = _make_dom_levels(levels=1)

        mock_ib.reqMktDepth.side_effect = _fake_req_depth_with_immediate_delivery(bids, asks)

        contract = Contract(conId=1, symbol='TEST', secType='STK',
                            exchange='ASX', currency='AUD')

        await ibrx.get_market_depth(contract)

        mock_ib.cancelMktDepth.assert_called_once()
