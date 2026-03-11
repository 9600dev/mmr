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

import datetime as dt
import pytz
import threading


logging = setup_logging(module_name='massive_reactive')


class MassiveReactive:
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

        self._ws_client = WebSocketClient(
            api_key=self.massive_api_key,
            feed=self._get_feed(),
            market=self._get_market(),
            subscriptions=subscriptions,
        )

        def _run():
            self._ws_client.run(self._handle_messages)

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        logging.info('MassiveReactive started with subscriptions: {}'.format(subscriptions))

    def stop(self):
        if self._ws_client:
            try:
                self._ws_client.close()
            except Exception:
                pass
        self.agg_subject.on_completed()
        self.trade_subject.on_completed()
        self.quote_subject.on_completed()
        logging.info('MassiveReactive stopped')
