from ib_async.contract import Contract
from ib_async.ticker import Ticker
from massive import WebSocketClient
from massive.websocket.models.common import Feed, Market
from massive.websocket.models.models import CurrencyAgg, EquityAgg, EquityTrade, ForexQuote
from reactivex.subject import Subject
from trader.common.logging_helper import setup_logging
from trader.data.universe import UniverseAccessor
from trader.data.data_access import SecurityDefinition
from typing import Dict, List, Optional

import asyncio
import datetime as dt
import pytz
import threading


logging = setup_logging(module_name='massive_reactive')


class MassiveReactive:
    # How long stop() waits for the close, and then for the thread. The
    # client's own close handshake timeout is 1 s.
    STOP_TIMEOUT_S = 5.0

    def __init__(
        self,
        massive_api_key: str,
        massive_feed: str = 'stocks',
        massive_delayed: bool = False,
        duckdb_path: str = '',
        universe_library: str = 'Universes',
    ):
        self.massive_api_key = massive_api_key
        self.massive_feed = massive_feed
        self.massive_delayed = massive_delayed
        self.duckdb_path = duckdb_path
        self.universe_library = universe_library

        self.agg_subject: Subject[Ticker] = Subject()
        self.trade_subject: Subject[Ticker] = Subject()
        self.quote_subject: Subject[Ticker] = Subject()

        self._symbol_cache: Dict[str, Optional[SecurityDefinition]] = {}
        self._ws_client: Optional[WebSocketClient] = None
        self._thread: Optional[threading.Thread] = None
        # The stream thread's event loop and the task running the connection,
        # published under _lifecycle so stop() and a just-started thread agree
        # on which of them acts: the thread sees _stopped and never connects,
        # or stop() sees the loop and closes on it.
        self._lifecycle = threading.Lock()
        self._stopped = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stream_task: Optional['asyncio.Task[None]'] = None

    def _get_market(self) -> Market:
        market_map = {
            'stocks': Market.Stocks,
            'options': Market.Options,
            'forex': Market.Forex,
            'crypto': Market.Crypto,
            'indices': Market.Indices,
            'futures': Market.Futures,
        }
        return market_map.get(self.massive_feed, Market.Stocks)

    def _get_feed(self) -> Feed:
        return Feed.Delayed if self.massive_delayed else Feed.RealTime

    def _resolve_symbol(self, symbol: str) -> Optional[SecurityDefinition]:
        if symbol in self._symbol_cache:
            return self._symbol_cache[symbol]

        if not self.duckdb_path:
            self._symbol_cache[symbol] = None
            return None

        try:
            accessor = UniverseAccessor(self.duckdb_path, self.universe_library)
            results = accessor.resolve_symbol(symbol)
            definition = results[0] if results else None
            self._symbol_cache[symbol] = definition
            return definition
        except Exception as ex:
            logging.warning('failed to resolve symbol {}: {}'.format(symbol, ex))
            self._symbol_cache[symbol] = None
            return None

    def _get_contract(self, symbol: str) -> Contract:
        definition = self._resolve_symbol(symbol)
        if definition:
            return Contract(
                conId=definition.conId,
                symbol=definition.symbol,
                secType=definition.secType,
                exchange=definition.exchange,
                primaryExchange=definition.primaryExchange,
                currency=definition.currency,
            )
        return Contract(symbol=symbol, secType='STK', exchange='SMART', currency='USD')

    @staticmethod
    def _get_forex_contract(pair: str) -> Contract:
        """Parse a forex pair like 'EURUSD' or 'EUR/USD' into an IB CASH contract."""
        pair = pair.replace('/', '').replace('C:', '').upper()
        if len(pair) != 6:
            return Contract(symbol=pair, secType='CASH', exchange='IDEALPRO', currency='USD')
        base = pair[:3]
        quote = pair[3:]
        return Contract(symbol=base, secType='CASH', exchange='IDEALPRO', currency=quote)

    def _currency_agg_to_ticker(self, agg: CurrencyAgg) -> Ticker:
        pair = agg.pair or ''
        contract = self._get_forex_contract(pair)

        ts = None
        if agg.end_timestamp:
            ts = dt.datetime.fromtimestamp(agg.end_timestamp / 1000, tz=pytz.utc)

        ticker = Ticker()
        ticker.contract = contract
        ticker.time = ts
        ticker.last = agg.close if agg.close is not None else float('nan')
        ticker.lastSize = float(agg.volume) if agg.volume is not None else float('nan')
        ticker.volume = float('nan')
        ticker.open = agg.open if agg.open is not None else float('nan')
        ticker.high = agg.high if agg.high is not None else float('nan')
        ticker.low = agg.low if agg.low is not None else float('nan')
        ticker.close = agg.close if agg.close is not None else float('nan')
        ticker.vwap = agg.vwap if agg.vwap is not None else float('nan')
        return ticker

    def _forex_quote_to_ticker(self, quote: ForexQuote) -> Ticker:
        pair = quote.pair or ''
        contract = self._get_forex_contract(pair)

        ts = None
        if quote.timestamp:
            ts = dt.datetime.fromtimestamp(quote.timestamp / 1000000000, tz=pytz.utc)

        bid = quote.bid_price if quote.bid_price is not None else float('nan')
        ask = quote.ask_price if quote.ask_price is not None else float('nan')
        mid = (bid + ask) / 2.0 if (bid == bid and ask == ask) else float('nan')

        ticker = Ticker()
        ticker.contract = contract
        ticker.time = ts
        ticker.bid = bid
        ticker.ask = ask
        ticker.last = mid
        return ticker

    def _agg_to_ticker(self, agg: EquityAgg) -> Ticker:
        symbol = agg.symbol or ''
        contract = self._get_contract(symbol)

        ts = None
        if agg.end_timestamp:
            ts = dt.datetime.fromtimestamp(agg.end_timestamp / 1000, tz=pytz.utc)

        ticker = Ticker()
        ticker.contract = contract
        ticker.time = ts
        ticker.last = agg.close if agg.close is not None else float('nan')
        ticker.lastSize = float(agg.volume) if agg.volume is not None else float('nan')
        ticker.volume = float(agg.accumulated_volume) if agg.accumulated_volume is not None else float('nan')
        ticker.open = agg.open if agg.open is not None else float('nan')
        ticker.high = agg.high if agg.high is not None else float('nan')
        ticker.low = agg.low if agg.low is not None else float('nan')
        ticker.close = agg.close if agg.close is not None else float('nan')
        ticker.vwap = agg.vwap if agg.vwap is not None else float('nan')
        return ticker

    def _trade_to_ticker(self, trade: EquityTrade) -> Ticker:
        symbol = trade.symbol or ''
        contract = self._get_contract(symbol)

        ts = None
        if trade.timestamp:
            ts = dt.datetime.fromtimestamp(trade.timestamp / 1000000000, tz=pytz.utc)

        ticker = Ticker()
        ticker.contract = contract
        ticker.time = ts
        ticker.last = trade.price if trade.price is not None else float('nan')
        ticker.lastSize = float(trade.size) if trade.size is not None else float('nan')
        return ticker

    def _handle_messages(self, messages: list):
        for msg in messages:
            try:
                if isinstance(msg, EquityAgg):
                    ticker = self._agg_to_ticker(msg)
                    self.agg_subject.on_next(ticker)
                elif isinstance(msg, EquityTrade):
                    ticker = self._trade_to_ticker(msg)
                    self.trade_subject.on_next(ticker)
                elif isinstance(msg, CurrencyAgg):
                    ticker = self._currency_agg_to_ticker(msg)
                    self.agg_subject.on_next(ticker)
                elif isinstance(msg, ForexQuote):
                    ticker = self._forex_quote_to_ticker(msg)
                    self.quote_subject.on_next(ticker)
            except Exception as ex:
                logging.error('error handling message: {}'.format(ex))

    def subscribe(self, symbols: List[str], data_type: str = 'A'):
        if self._ws_client:
            subs = ['{}.{}'.format(data_type, s) for s in symbols]
            self._ws_client.subscribe(*subs)

    def start(self, symbols: List[str] = None, data_type: str = 'A'):
        subscriptions = []
        if symbols:
            subscriptions = ['{}.{}'.format(data_type, s) for s in symbols]

        client = WebSocketClient(
            api_key=self.massive_api_key,
            feed=self._get_feed(),
            market=self._get_market(),
            subscriptions=subscriptions,
        )
        self._ws_client = client
        with self._lifecycle:
            self._stopped = False
            self._loop = None
            self._stream_task = None

        async def _on_messages(messages):
            self._handle_messages(messages)

        async def _stream():
            # What WebSocketClient.run() does — asyncio.run(client.connect(...))
            # — with the loop and the task kept, because stop() needs both:
            # the socket belongs to this loop, so close() has to be awaited
            # HERE, and nothing outside the thread could reach run()'s loop.
            with self._lifecycle:
                if self._stopped:
                    return      # stop() got in before there was anything to close
                self._loop = asyncio.get_running_loop()
                self._stream_task = asyncio.current_task()
            await client.connect(_on_messages)

        def _run():
            try:
                asyncio.run(_stream())
            except asyncio.CancelledError:
                pass            # how stop() ends the stream; see _close_stream

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        logging.info('MassiveReactive started with subscriptions: {}'.format(subscriptions))

    @staticmethod
    async def _close_stream(client, stream_task) -> None:
        """Runs on the stream's own loop."""
        try:
            await client.close()
        finally:
            # A clean close ends connect() by itself, but two cases do not:
            # close() has nothing to close while the handshake is still in
            # progress, and a close the server does not answer in time reads
            # as a lost connection, which the client reconnects. Stopping
            # means the stream ends, so the task is cancelled either way.
            stream_task.cancel()

    def stop(self):
        """Close the websocket and end the stream thread, then complete the subjects.

        ``WebSocketClient.close`` is a coroutine function. Until 2026-11-15 it
        was called here and the coroutine dropped, so nothing was closed: the
        connection stayed open and the thread kept receiving behind completed
        subjects (docs/AUDIT_2026-11-14.md B2.4).
        """
        client = self._ws_client
        with self._lifecycle:
            self._stopped = True
            loop, stream_task, thread = self._loop, self._stream_task, self._thread
        if client is not None and loop is not None and stream_task is not None:
            closing = self._close_stream(client, stream_task)
            on_stream_thread = thread is threading.current_thread()
            try:
                future = asyncio.run_coroutine_threadsafe(closing, loop)
            except RuntimeError:
                closing.close()     # the loop has already finished: nothing is open
            else:
                # Called from a message handler, this IS the stream's thread:
                # the close is scheduled and runs when the handler returns.
                # Waiting for it here would be waiting for ourselves.
                if not on_stream_thread:
                    try:
                        future.result(timeout=self.STOP_TIMEOUT_S)
                    except Exception as ex:
                        logging.warning('massive stream did not close cleanly: %r', ex)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=self.STOP_TIMEOUT_S)
            if thread.is_alive():
                logging.warning('massive stream thread still running %ss after stop()',
                                self.STOP_TIMEOUT_S)
        self.agg_subject.on_completed()
        self.trade_subject.on_completed()
        self.quote_subject.on_completed()
        logging.info('MassiveReactive stopped')
