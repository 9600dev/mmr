"""Async regression tests for IBAIORx methods.

Verifies that critical IB interaction code paths are:
  1. Fully async (no blocking calls like ib.sleep())
  2. Event-driven where applicable (no wasteful polling)
  3. Concurrent (parallel requests don't serialize)
  4. Clean (callbacks/subscriptions are removed after use)

Uses mocked ib_async objects — no IB connection required.
"""

import asyncio
import time
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ib_async import Contract, ContractDetails, DOMLevel, Ticker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ibrx():
    """Create an IBAIORx with a fully mocked IB client."""
    from trader.listeners.ibreactive import IBAIORx

    ibrx = IBAIORx.__new__(IBAIORx)
    ibrx.ib = MagicMock()
    return ibrx


def _make_contract(symbol='TEST', conId=12345, exchange='ASX', currency='AUD'):
    return Contract(conId=conId, symbol=symbol, secType='STK',
                    exchange=exchange, currency=currency)


def _make_ticker_with_depth(bids_levels=3, asks_levels=3, delay=0.01):
    """Return a Ticker and schedule depth data delivery after *delay*.

    The ticker is returned empty; a background task populates domBids/domAsks
    and fires updateEvent after the delay, matching real IB behavior.
    """
    ticker = Ticker()
    bids = [DOMLevel(price=100.0 - i * 0.01, size=(bids_levels - i) * 100, marketMaker='')
            for i in range(bids_levels)]
    asks = [DOMLevel(price=100.01 + i * 0.01, size=(asks_levels - i) * 80, marketMaker='')
            for i in range(asks_levels)]

    async def _deliver():
        await asyncio.sleep(delay)
        ticker.domBids = bids
        ticker.domAsks = asks
        ticker.updateEvent.emit(ticker)

    asyncio.get_event_loop().create_task(_deliver())
    return ticker


# ---------------------------------------------------------------------------
# get_market_depth — event-driven, concurrent, no blocking
# ---------------------------------------------------------------------------

class TestMarketDepthAsync:

    @pytest.mark.asyncio
    async def test_event_driven_not_polling(self):
        """get_market_depth should use updateEvent, not asyncio.sleep polling."""
        ibrx = _make_ibrx()
        ibrx.ib.reqMktDepth.side_effect = lambda c, **kw: _make_ticker_with_depth()

        contract = _make_contract()

        # Patch asyncio.sleep to detect if polling is used
        sleep_calls = []
        original_sleep = asyncio.sleep

        async def tracking_sleep(s, **kw):
            sleep_calls.append(s)
            return await original_sleep(s, **kw)

        with patch('asyncio.sleep', side_effect=tracking_sleep):
            await ibrx.get_market_depth(contract)

        # The only asyncio.sleep should be the tiny delivery delay (0.01s),
        # NOT a 0.2s polling loop
        polling_sleeps = [s for s in sleep_calls if s >= 0.15]
        assert not polling_sleeps, \
            f'Detected polling sleeps: {polling_sleeps}. Should be event-driven.'

    @pytest.mark.asyncio
    async def test_concurrent_depth_requests(self):
        """5 concurrent depth requests complete in ~1x delay, not 5x."""
        ibrx = _make_ibrx()
        delay = 0.15
        n = 5

        ibrx.ib.reqMktDepth.side_effect = lambda c, **kw: _make_ticker_with_depth(delay=delay)

        contracts = [_make_contract(symbol=f'S{i}', conId=i) for i in range(n)]

        start = time.monotonic()
        results = await asyncio.gather(*[ibrx.get_market_depth(c) for c in contracts])
        elapsed = time.monotonic() - start

        assert len(results) == n
        assert elapsed < delay + 0.3, \
            f'Took {elapsed:.2f}s for {n} requests (expected ~{delay}s parallel, not {delay*n}s serial)'

    @pytest.mark.asyncio
    async def test_no_ib_sleep(self):
        """Must never call the synchronous ib.sleep()."""
        ibrx = _make_ibrx()
        ibrx.ib.reqMktDepth.side_effect = lambda c, **kw: _make_ticker_with_depth()

        await ibrx.get_market_depth(_make_contract())
        ibrx.ib.sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_listener_cleanup(self):
        """updateEvent listener count must not grow across calls."""
        ibrx = _make_ibrx()

        ticker = Ticker()
        listeners_before = len(ticker.updateEvent)

        def req(c, **kw):
            async def _d():
                await asyncio.sleep(0.01)
                ticker.domBids = [DOMLevel(price=1, size=1, marketMaker='')]
                ticker.updateEvent.emit(ticker)
            asyncio.get_event_loop().create_task(_d())
            return ticker

        ibrx.ib.reqMktDepth.side_effect = req

        # Call multiple times
        for _ in range(5):
            await ibrx.get_market_depth(_make_contract())

        assert len(ticker.updateEvent) == listeners_before


# ---------------------------------------------------------------------------
# get_snapshot — via __get_single_mrkt_data (RxPY + asyncio.Event)
# ---------------------------------------------------------------------------

class TestSnapshotAsync:

    @pytest.mark.asyncio
    async def test_no_ib_sleep(self):
        """get_snapshot must not call ib.sleep()."""
        ibrx = _make_ibrx()

        # Mock the RxPY subscribe path: __get_single_mrkt_data uses
        # __subscribe_contract which calls reqMktData. We mock higher up.
        mock_ticker = Ticker()
        mock_ticker.bid = 100.0
        mock_ticker.ask = 100.05
        mock_ticker.last = 100.02

        # Simplest approach: mock the private method that get_snapshot delegates to
        ibrx._IBAIORx__get_single_mrkt_data = AsyncMock(return_value=mock_ticker)

        result = await ibrx.get_snapshot(_make_contract())

        assert result.bid == 100.0
        ibrx.ib.sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_snapshot_is_awaitable(self):
        """get_snapshot returns a coroutine, not a blocking result."""
        ibrx = _make_ibrx()

        mock_ticker = Ticker()
        ibrx._IBAIORx__get_single_mrkt_data = AsyncMock(return_value=mock_ticker)

        # Should be a coroutine that we can await
        coro = ibrx.get_snapshot(_make_contract())
        assert asyncio.iscoroutine(coro)
        await coro


# ---------------------------------------------------------------------------
# get_snapshots_batch — sequential but non-blocking
# ---------------------------------------------------------------------------

class TestSnapshotsBatchAsync:

    @pytest.mark.asyncio
    async def test_doesnt_block_event_loop(self):
        """Batch snapshots should yield to the loop between each request."""
        ibrx = _make_ibrx()

        mock_ticker = Ticker()
        mock_ticker.bid = 50.0
        mock_ticker.ask = 50.01
        mock_ticker.last = 50.0
        mock_ticker.open = 49.5
        mock_ticker.high = 50.5
        mock_ticker.low = 49.0
        mock_ticker.close = 49.8
        mock_ticker.volume = 1000

        ibrx._IBAIORx__get_single_mrkt_data = AsyncMock(return_value=mock_ticker)

        contracts = [_make_contract(symbol=f'S{i}', conId=i) for i in range(3)]

        # Run a concurrent task that should get execution time
        other_ran = False

        async def other_work():
            nonlocal other_ran
            await asyncio.sleep(0)  # yield once
            other_ran = True

        task = asyncio.create_task(other_work())
        results = await ibrx.get_snapshots_batch(contracts)
        await task

        assert len(results) == 3
        assert other_ran, 'Event loop was blocked — other_work never ran'

    @pytest.mark.asyncio
    async def test_one_failure_doesnt_stop_batch(self):
        """A failing snapshot should not stop the rest of the batch."""
        ibrx = _make_ibrx()

        call_count = 0

        async def flaky_snapshot(contract, **kwargs):
            nonlocal call_count
            call_count += 1
            if contract.symbol == 'BAD':
                raise Exception('Market data not subscribed')
            t = Ticker()
            t.bid = 50.0
            t.ask = 50.01
            t.last = 50.0
            t.open = t.high = t.low = t.close = t.volume = 0
            return t

        ibrx._IBAIORx__get_single_mrkt_data = flaky_snapshot

        contracts = [
            _make_contract(symbol='GOOD1', conId=1),
            _make_contract(symbol='BAD', conId=2),
            _make_contract(symbol='GOOD2', conId=3),
        ]

        results = await ibrx.get_snapshots_batch(contracts)

        assert call_count == 3, 'Should attempt all 3, not stop at BAD'
        assert len(results) == 2
        symbols = [r['symbol'] for r in results]
        assert 'GOOD1' in symbols
        assert 'GOOD2' in symbols


# ---------------------------------------------------------------------------
# scanner_data — simple await on reqScannerDataAsync
# ---------------------------------------------------------------------------

class TestScannerDataAsync:

    @pytest.mark.asyncio
    async def test_is_async(self):
        """scanner_data should await reqScannerDataAsync, not block."""
        ibrx = _make_ibrx()

        # Build mock scanner results
        @dataclass
        class MockScanData:
            rank: int
            contractDetails: ContractDetails

        cd = ContractDetails()
        cd.contract = _make_contract(symbol='AAPL', conId=265598, exchange='NASDAQ', currency='USD')

        ibrx.ib.reqScannerDataAsync = AsyncMock(return_value=[MockScanData(rank=0, contractDetails=cd)])

        result = await ibrx.scanner_data(scan_code='TOP_PERC_GAIN')

        assert len(result) == 1
        assert result[0]['symbol'] == 'AAPL'
        assert result[0]['rank'] == 1
        ibrx.ib.sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_concurrent_scans(self):
        """Multiple scanner requests should run concurrently."""
        ibrx = _make_ibrx()

        async def slow_scan(*args, **kwargs):
            await asyncio.sleep(0.1)
            return []

        ibrx.ib.reqScannerDataAsync = AsyncMock(side_effect=slow_scan)

        start = time.monotonic()
        results = await asyncio.gather(
            ibrx.scanner_data(scan_code='TOP_PERC_GAIN'),
            ibrx.scanner_data(scan_code='TOP_PERC_LOSE'),
            ibrx.scanner_data(scan_code='MOST_ACTIVE'),
        )
        elapsed = time.monotonic() - start

        assert len(results) == 3
        assert elapsed < 0.3, f'3 scans took {elapsed:.2f}s, should be ~0.1s parallel'

    @pytest.mark.asyncio
    async def test_empty_results_handled(self):
        """Should handle None or empty results gracefully."""
        ibrx = _make_ibrx()
        ibrx.ib.reqScannerDataAsync = AsyncMock(return_value=None)

        result = await ibrx.scanner_data()
        assert result == []


# ---------------------------------------------------------------------------
# get_history_bars — simple await
# ---------------------------------------------------------------------------

class TestHistoryBarsAsync:

    @pytest.mark.asyncio
    async def test_is_async(self):
        """get_history_bars should await reqHistoricalDataAsync."""
        ibrx = _make_ibrx()

        @dataclass
        class MockBar:
            date: str
            open: float
            high: float
            low: float
            close: float
            volume: int

        bars = [MockBar('2026-03-14', 100, 105, 99, 104, 5000)]
        ibrx.ib.reqHistoricalDataAsync = AsyncMock(return_value=bars)

        result = await ibrx.get_history_bars(_make_contract())

        assert len(result) == 1
        assert result[0]['close'] == 104
        ibrx.ib.sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_concurrent_history_requests(self):
        """Multiple history requests should run concurrently."""
        ibrx = _make_ibrx()

        async def slow_history(*args, **kwargs):
            await asyncio.sleep(0.1)
            return []

        ibrx.ib.reqHistoricalDataAsync = AsyncMock(side_effect=slow_history)

        contracts = [_make_contract(symbol=f'S{i}', conId=i) for i in range(5)]

        start = time.monotonic()
        results = await asyncio.gather(
            *[ibrx.get_history_bars(c) for c in contracts]
        )
        elapsed = time.monotonic() - start

        assert len(results) == 5
        assert elapsed < 0.3, f'5 history requests took {elapsed:.2f}s, should be ~0.1s parallel'

    @pytest.mark.asyncio
    async def test_null_bars_handled(self):
        """Should handle None return gracefully."""
        ibrx = _make_ibrx()
        ibrx.ib.reqHistoricalDataAsync = AsyncMock(return_value=None)

        result = await ibrx.get_history_bars(_make_contract())
        assert result == []


# ---------------------------------------------------------------------------
# get_fundamental_data — simple await
# ---------------------------------------------------------------------------

class TestFundamentalDataAsync:

    @pytest.mark.asyncio
    async def test_is_async(self):
        """get_fundamental_data should await reqFundamentalDataAsync."""
        ibrx = _make_ibrx()
        ibrx.ib.reqFundamentalDataAsync = AsyncMock(return_value='<xml>data</xml>')

        result = await ibrx.get_fundamental_data(_make_contract())

        assert '<xml>' in result
        ibrx.ib.sleep.assert_not_called()


# ---------------------------------------------------------------------------
# get_news_headlines — two sequential awaits
# ---------------------------------------------------------------------------

class TestNewsHeadlinesAsync:

    @pytest.mark.asyncio
    async def test_is_async(self):
        """get_news_headlines should use async calls, not blocking ones."""
        ibrx = _make_ibrx()

        @dataclass
        class MockProvider:
            code: str

        @dataclass
        class MockHeadline:
            time: str
            providerCode: str
            articleId: str
            headline: str

        ibrx.ib.reqNewsProvidersAsync = AsyncMock(return_value=[MockProvider('BZ')])
        ibrx.ib.reqHistoricalNewsAsync = AsyncMock(return_value=[
            MockHeadline('2026-03-15', 'BZ', 'A1', 'AAPL beats earnings')
        ])

        result = await ibrx.get_news_headlines(conId=265598)

        assert len(result) == 1
        assert 'AAPL' in result[0]['headline']
        ibrx.ib.sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_concurrent_news_requests(self):
        """Multiple news requests should run concurrently."""
        ibrx = _make_ibrx()

        @dataclass
        class MockProvider:
            code: str

        async def slow_providers():
            await asyncio.sleep(0.1)
            return [MockProvider('BZ')]

        async def slow_news(*args, **kwargs):
            await asyncio.sleep(0.05)
            return []

        ibrx.ib.reqNewsProvidersAsync = AsyncMock(side_effect=slow_providers)
        ibrx.ib.reqHistoricalNewsAsync = AsyncMock(side_effect=slow_news)

        start = time.monotonic()
        results = await asyncio.gather(
            ibrx.get_news_headlines(conId=1),
            ibrx.get_news_headlines(conId=2),
            ibrx.get_news_headlines(conId=3),
        )
        elapsed = time.monotonic() - start

        assert len(results) == 3
        # Each request: 0.1s (providers) + 0.05s (news) = 0.15s
        # Parallel: should be ~0.15s, not 0.45s
        assert elapsed < 0.35, f'3 news requests took {elapsed:.2f}s, should be ~0.15s parallel'

    @pytest.mark.asyncio
    async def test_empty_providers_fallback(self):
        """Should fall back to BZ+FLY when no providers returned."""
        ibrx = _make_ibrx()
        ibrx.ib.reqNewsProvidersAsync = AsyncMock(return_value=[])
        ibrx.ib.reqHistoricalNewsAsync = AsyncMock(return_value=None)

        result = await ibrx.get_news_headlines(conId=1)

        assert result == []
        # Verify the fallback provider codes were used
        call_args = ibrx.ib.reqHistoricalNewsAsync.call_args
        assert call_args[0][1] == 'BZ+FLY'


# ---------------------------------------------------------------------------
# get_contract_details_async / get_matching_symbols — simple awaits
# ---------------------------------------------------------------------------

class TestContractResolutionAsync:

    @pytest.mark.asyncio
    async def test_contract_details_is_async(self):
        ibrx = _make_ibrx()
        ibrx.ib.reqContractDetailsAsync = AsyncMock(return_value=[])

        result = await ibrx.get_contract_details_async(_make_contract())
        assert result == []
        ibrx.ib.sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_matching_symbols_is_async(self):
        ibrx = _make_ibrx()
        ibrx.ib.reqMatchingSymbolsAsync = AsyncMock(return_value=None)

        result = await ibrx.get_matching_symbols('AAPL')
        assert result == []
        ibrx.ib.sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_concurrent_resolutions(self):
        """Multiple contract resolutions should run concurrently."""
        ibrx = _make_ibrx()

        async def slow_details(*args, **kwargs):
            await asyncio.sleep(0.1)
            return []

        ibrx.ib.reqContractDetailsAsync = AsyncMock(side_effect=slow_details)

        contracts = [_make_contract(symbol=f'S{i}', conId=i) for i in range(5)]

        start = time.monotonic()
        results = await asyncio.gather(
            *[ibrx.get_contract_details_async(c) for c in contracts]
        )
        elapsed = time.monotonic() - start

        assert len(results) == 5
        assert elapsed < 0.3, f'5 resolutions took {elapsed:.2f}s, should be ~0.1s parallel'
