from ib_async.contract import Contract
from ib_async.ticker import Ticker
from twelvedata import TDClient
from reactivex.subject import Subject
from trader.common.logging_helper import setup_logging
from trader.data.universe import UniverseAccessor
from trader.data.data_access import SecurityDefinition
from typing import Any, Dict, List, Optional

import datetime as dt
import pytz


logging = setup_logging(module_name='twelvedata_reactive')


# Currency codes that, when seen as the second 3-letter half of a 6-letter
# upper-case symbol, identify a forex pair worth formatting as ``BASE/QUOTE``
# for TwelveData. Anything else stays as-is (e.g. ``AAPL``).
_FOREX_QUOTES = frozenset({
    'USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'CNY',
    'HKD', 'NZD', 'SEK', 'NOK', 'DKK', 'SGD', 'MXN', 'ZAR',
})

# Common crypto base symbols. TwelveData crypto subscriptions need the
# ``BASE/QUOTE`` form (e.g. ``BTC/USD``).
_CRYPTO_BASES = frozenset({
    'BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'ADA', 'SOL', 'DOT', 'DOGE',
    'AVAX', 'MATIC', 'LINK', 'UNI', 'ATOM', 'XLM', 'TRX', 'NEAR',
})


def _normalize_td_symbol(symbol: str) -> str:
    """Translate a user-friendly symbol (``EURUSD``, ``BTCUSD``) into the
    TwelveData WebSocket form (``EUR/USD``, ``BTC/USD``).

    Pass-through for anything that doesn't pattern-match a forex/crypto
    pair — equity tickers stay verbatim.
    """
    s = symbol.strip().upper().replace('C:', '')
    if '/' in s:
        return s
    if len(s) == 6:
        base, quote = s[:3], s[3:]
        if quote in _FOREX_QUOTES and base.isalpha():
            return f'{base}/{quote}'
    # Variable-length crypto split (BTC + USD = 6, but DOGE + USD = 7)
    for n in (3, 4, 5):
        if len(s) > n and s[:n] in _CRYPTO_BASES:
            tail = s[n:]
            if tail in _FOREX_QUOTES:
                return f'{s[:n]}/{tail}'
    return s


def _denormalize_td_symbol(symbol: str) -> str:
    """Reverse of :func:`_normalize_td_symbol` — strip the ``/`` so the
    rest of the dashboard layer (which uses bare keys like ``EURUSD``)
    can look up state coherently."""
    return (symbol or '').replace('/', '').upper()


def _is_forex_pair(symbol: str) -> bool:
    s = (symbol or '').replace('/', '').upper()
    if len(s) != 6:
        return False
    return s[3:] in _FOREX_QUOTES and s[:3].isalpha() and s[:3] not in _CRYPTO_BASES


class TwelveDataReactive:
    """TwelveData WebSocket consumer with the same surface as
    :class:`trader.listeners.massive_reactive.MassiveReactive`.

    TwelveData ships a single ``price`` event type per symbol that carries
    last/bid/ask/day_volume. We fan it out onto:

    - ``agg_subject`` — always (treat each tick as the latest 1-min agg)
    - ``quote_subject`` — when both bid and ask are present in the event

    The ``trade_subject`` exists for surface parity but TD does not stream
    per-trade prints, so it stays silent.

    The ``data_type`` parameter on :meth:`start` is accepted for surface
    parity with ``MassiveReactive`` but is otherwise unused — TD only has
    one stream type.
    """

    def __init__(
        self,
        twelvedata_api_key: str,
        duckdb_path: str = '',
        universe_library: str = 'Universes',
    ):
        if not twelvedata_api_key:
            raise ValueError('twelvedata_api_key is required')

        self.twelvedata_api_key = twelvedata_api_key
        self.duckdb_path = duckdb_path
        self.universe_library = universe_library

        self.agg_subject: Subject[Ticker] = Subject()
        self.trade_subject: Subject[Ticker] = Subject()
        self.quote_subject: Subject[Ticker] = Subject()

        self._symbol_cache: Dict[str, Optional[SecurityDefinition]] = {}
        self._client: Optional[TDClient] = None
        self._ws = None  # twelvedata.websocket.TDWebSocket
        self._symbols: List[str] = []

    # ------------------------------------------------------------------
    # Symbol/contract resolution (kept congruent with MassiveReactive)
    # ------------------------------------------------------------------

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

    def _get_contract(self, raw_symbol: str) -> Contract:
        """Build a Contract from a TD-style symbol (``AAPL`` or ``EUR/USD``).

        Forex pairs go to a CASH/IDEALPRO contract; equities try the
        universe DB first then fall back to a SMART/USD STK guess.
        """
        flat = _denormalize_td_symbol(raw_symbol)

        # Forex — match the same shape MassiveReactive uses
        if _is_forex_pair(flat):
            base = flat[:3]
            quote = flat[3:]
            return Contract(
                symbol=base, secType='CASH',
                exchange='IDEALPRO', currency=quote,
            )

        # Equity / crypto — look in the universe DB first
        definition = self._resolve_symbol(flat)
        if definition:
            return Contract(
                conId=definition.conId,
                symbol=definition.symbol,
                secType=definition.secType,
                exchange=definition.exchange,
                primaryExchange=definition.primaryExchange,
                currency=definition.currency,
            )
        return Contract(symbol=flat, secType='STK', exchange='SMART', currency='USD')

    # ------------------------------------------------------------------
    # Event → Ticker translation
    # ------------------------------------------------------------------

    def _event_to_ticker(self, event: Dict[str, Any]) -> Optional[Ticker]:
        sym = event.get('symbol') or ''
        if not sym:
            return None

        contract = self._get_contract(sym)

        ts = None
        epoch = event.get('timestamp')
        if epoch:
            try:
                # TD timestamp is seconds since epoch
                ts = dt.datetime.fromtimestamp(int(epoch), tz=pytz.utc)
            except (TypeError, ValueError, OSError):
                ts = None

        def _f(key: str) -> float:
            v = event.get(key)
            if v is None:
                return float('nan')
            try:
                return float(v)
            except (TypeError, ValueError):
                return float('nan')

        last = _f('price')
        bid = _f('bid')
        ask = _f('ask')
        day_vol = _f('day_volume')

        ticker = Ticker()
        ticker.contract = contract
        ticker.time = ts
        ticker.last = last
        ticker.bid = bid
        ticker.ask = ask
        # TD doesn't ship size on the WS price event
        ticker.lastSize = float('nan')
        ticker.volume = day_vol
        # TD doesn't break out OHLC on the WS event — leave NaN.
        ticker.open = float('nan')
        ticker.high = float('nan')
        ticker.low = float('nan')
        ticker.close = last  # surrogate so dashboard's "last or close" works
        ticker.vwap = float('nan')
        return ticker

    def _on_event(self, event: Dict[str, Any]):
        """TD WebSocket on_event callback. Runs on the TD library's
        EventHandler thread. Must be lightweight + exception-safe."""
        try:
            etype = (event or {}).get('event')
            if etype == 'subscribe-status':
                # Surface bad symbols / plan-tier failures clearly.
                status = event.get('status', '')
                if status and status != 'ok':
                    logging.warning('TD subscribe status: {}'.format(event))
                return
            if etype != 'price':
                # heartbeat / other admin events — ignore
                return

            ticker = self._event_to_ticker(event)
            if ticker is None:
                return

            self.agg_subject.on_next(ticker)
            # Only fan out to quote_subject when we actually have a bid+ask
            if ticker.bid == ticker.bid and ticker.ask == ticker.ask:  # NaN check
                self.quote_subject.on_next(ticker)
        except Exception as ex:
            logging.error('error handling TD event: {}'.format(ex))

    # ------------------------------------------------------------------
    # Lifecycle (matches MassiveReactive)
    # ------------------------------------------------------------------

    def start(self, symbols: Optional[List[str]] = None, data_type: str = 'A'):
        """Open the TD WebSocket and subscribe to ``symbols``.

        ``data_type`` is accepted for parity with :class:`MassiveReactive`
        but ignored — TD only streams a single price-event type. The
        equivalent of forex/crypto vs. equities is encoded in the symbol
        (``EUR/USD`` vs. ``AAPL``); we normalize here.
        """
        del data_type  # surface parity only
        symbols = symbols or []
        td_symbols = [_normalize_td_symbol(s) for s in symbols]
        self._symbols = td_symbols

        if self._client is None:
            self._client = TDClient(apikey=self.twelvedata_api_key)

        self._ws = self._client.websocket(
            symbols=td_symbols,
            on_event=self._on_event,
        )
        try:
            self._ws.connect()
        except Exception as ex:
            logging.error(
                'TD WebSocket connect failed: {}. '
                'Note: WebSocket access requires a TwelveData Pro plan or higher.'.format(ex)
            )
            raise

        logging.info(
            'TwelveDataReactive started with subscriptions: {}'.format(td_symbols)
        )

    def stop(self):
        if self._ws is not None:
            try:
                self._ws.disconnect()
            except Exception:
                pass
            self._ws = None
        try:
            self.agg_subject.on_completed()
            self.trade_subject.on_completed()
            self.quote_subject.on_completed()
        except Exception:
            pass
        logging.info('TwelveDataReactive stopped')
