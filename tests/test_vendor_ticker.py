"""An IB class-share symbol is spelled the vendor's way at the vendor boundary.

docs/AUDIT_2026-11-14.md B5.8. IB writes a share class after a SPACE
(`BRK B`, `BF B`); TwelveData and Massive write it after a DOT (`BRK.B`).
Universes hold IB's spelling (`SecurityDefinition.symbol`), and both history
workers sent it verbatim: Massive answers an unknown ticker with an empty
result and TwelveData with "not found", so the series was never filled and
nothing said why.

The vendors' convention, from what is installed here rather than from memory:
TwelveData's own SDK documents it on every `symbol` parameter ("use dot(.)
delimiter. E.g. BRK.A or BRK.B will be correct", twelvedata/time_series.py);
Massive/Polygon is `BRK.B` in trader/data/insider.py and in the ticker-keyed
PIT panel built from its grouped-daily endpoint.
"""
import datetime as dt
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytz

from trader.listeners.twelvedata_history import TwelveDataHistoryWorker
from trader.objects import BarSize
from trader.sdk import MMR

START, END = dt.datetime(2026, 4, 14), dt.datetime(2026, 4, 18)


# --------------------------------------------------------------------------
# the translation
# --------------------------------------------------------------------------

def _vendor_ticker(symbol):
    from trader.data.listing_guard import vendor_ticker
    return vendor_ticker(symbol)


@pytest.mark.parametrize('ib_symbol, vendor', [
    ('BRK B', 'BRK.B'), ('BRK A', 'BRK.A'), ('BF B', 'BF.B'), ('LEN B', 'LEN.B'),
    ('HEI A', 'HEI.A'), ('PBR A', 'PBR.A'), ('F B', 'F.B'),
    # IB writes a unit the same way, and so do the vendors (`.U`).
    ('AIIA U', 'AIIA.U'),
])
def test_a_symbol_with_a_one_letter_suffix_after_a_space_gets_a_dot(ib_symbol, vendor):
    assert _vendor_ticker(ib_symbol) == vendor


@pytest.mark.parametrize('symbol', [
    'BRK.B', 'BF.B', 'AIIA.U', 'XYZ.WS', 'XYZ.WS.A', 'JW.A',
])
def test_a_symbol_that_already_has_a_dot_is_unchanged(symbol):
    assert _vendor_ticker(symbol) == symbol


@pytest.mark.parametrize('symbol', [
    'AAPL', 'F', 'GOOGL', 'SPY', 'RUSHA',
    # Vendor-native spellings that are not IB's and must not be touched:
    # a preferred (lowercase p), forex, an option, an index, a dash.
    'KKRpD', 'EUR/USD', 'C:EURUSD', 'O:AAPL260320C00250000', 'I:SPX', 'BF-B', '0700',
])
def test_a_symbol_without_a_space_is_unchanged(symbol):
    assert _vendor_ticker(symbol) == symbol


def test_surrounding_whitespace_is_not_part_of_a_symbol():
    assert _vendor_ticker(' AAPL ') == 'AAPL'
    assert _vendor_ticker('BRK B\n') == 'BRK.B'


@pytest.mark.parametrize('symbol', [
    # IB's other space suffixes. Their vendor spellings are not a dot and
    # differ per vendor (a preferred is `JPMpC`); nothing is guessed.
    'JPM PRC', 'NMZ RT', 'XYZ WS', 'XYZ WS A', 'XYZ UN',
    # Not IB's class-share form at all.
    'BRK  B', 'BRK\tB', 'brk b', 'BRK b', 'BRK 2', 'BR1 B', 'BRK B C', ' ', '',
])
def test_any_other_symbol_with_a_space_is_refused_not_sent(symbol):
    with pytest.raises(ValueError) as refused:
        _vendor_ticker(symbol)
    assert repr(symbol) in str(refused.value)


@pytest.mark.parametrize('not_a_symbol', [None, 4391, 4391.0, b'BRK B', ['BRK B']])
def test_only_a_string_is_a_ticker(not_a_symbol):
    """A conId is never a ticker (4391 is AMD's conId and a Tokyo ticker)."""
    with pytest.raises(ValueError):
        _vendor_ticker(not_a_symbol)


# --------------------------------------------------------------------------
# TwelveData history
# --------------------------------------------------------------------------

def _td_frame(n=3):
    index = pd.DatetimeIndex([START + dt.timedelta(days=i) for i in range(n)],
                             name='datetime').tz_localize(pytz.timezone('US/Eastern'))
    return pd.DataFrame({'open': [150.0] * n, 'high': [155.0] * n, 'low': [149.0] * n,
                         'close': [154.0] * n, 'volume': [1_000_000] * n}, index=index)


def _td_worker():
    worker = TwelveDataHistoryWorker(twelvedata_api_key='test')
    series = MagicMock()
    series.as_pandas.return_value = _td_frame()
    worker.client = MagicMock()
    worker.client.time_series.return_value = series
    return worker


def _td_symbols_asked(worker):
    return [call.kwargs['symbol'] for call in worker.client.time_series.call_args_list]


@pytest.mark.parametrize('bar_size', [BarSize.Days1, BarSize.Mins1])
def test_twelvedata_is_asked_for_the_dotted_class_share(bar_size):
    worker = _td_worker()
    frame = worker.get_history('BRK B', bar_size, START, END)
    asked = _td_symbols_asked(worker)
    assert asked and set(asked) == {'BRK.B'}
    assert len(frame) == 3


@pytest.mark.parametrize('ticker', ['AAPL', 'BRK.B'])
def test_twelvedata_plain_and_dotted_tickers_are_sent_as_given(ticker):
    worker = _td_worker()
    worker.get_history(ticker, BarSize.Days1, START, END)
    assert _td_symbols_asked(worker) == [ticker]


@pytest.mark.parametrize('bar_size', [BarSize.Days1, BarSize.Mins1])
def test_twelvedata_is_not_asked_for_a_symbol_it_cannot_spell(bar_size):
    """Raised to the caller as a failure of that symbol — never swallowed as
    one more empty chunk, and never sent with the space in it."""
    worker = _td_worker()
    with pytest.raises(ValueError, match='JPM PRC'):
        worker.get_history('JPM PRC', bar_size, START, END)
    assert worker.client.time_series.call_count == 0


# --------------------------------------------------------------------------
# Massive history
# --------------------------------------------------------------------------

def _agg(ts_ms):
    return MagicMock(timestamp=ts_ms, open=100.0, high=101.0, low=99.0, close=100.5,
                     volume=1000.0, vwap=100.3, transactions=50)


def _massive_worker(rest_cls):
    from trader.listeners.massive_history import MassiveHistoryWorker
    base = int(dt.datetime(2024, 6, 3, 14, 30, tzinfo=dt.timezone.utc).timestamp() * 1000)
    client = MagicMock()
    client.list_aggs.return_value = [_agg(base), _agg(base + 60_000)]
    rest_cls.return_value = client
    return MassiveHistoryWorker('key'), client


@patch('trader.listeners.massive_history.RESTClient')
def test_massive_is_asked_for_the_dotted_class_share(rest_cls):
    worker, client = _massive_worker(rest_cls)
    frame = worker.get_history('BRK B', BarSize.Mins1, START, END)
    assert client.list_aggs.call_args.kwargs['ticker'] == 'BRK.B'
    assert len(frame) == 2


@pytest.mark.parametrize('ticker', ['AAPL', 'BRK.B', 'C:EURUSD'])
@patch('trader.listeners.massive_history.RESTClient')
def test_massive_plain_and_dotted_tickers_are_sent_as_given(rest_cls, ticker):
    worker, client = _massive_worker(rest_cls)
    worker.get_history(ticker, BarSize.Mins1, START, END)
    assert client.list_aggs.call_args.kwargs['ticker'] == ticker


@patch('trader.listeners.massive_history.RESTClient')
def test_massive_is_not_asked_for_a_symbol_it_cannot_spell(rest_cls):
    worker, client = _massive_worker(rest_cls)
    with pytest.raises(ValueError, match='NMZ RT'):
        worker.get_history('NMZ RT', BarSize.Mins1, START, END)
    assert client.list_aggs.call_count == 0


# --------------------------------------------------------------------------
# TwelveData snapshots (`mmr snapshot "BRK B"` with the default data source)
# --------------------------------------------------------------------------

class _Payload:
    def __init__(self, payload):
        self._payload = payload

    def as_json(self):
        return self._payload


def _quote(symbol, close):
    return {'symbol': symbol, 'open': '1', 'high': '2', 'low': '1', 'close': str(close),
            'previous_close': '1', 'volume': '10', 'change': '0', 'percent_change': '0',
            'datetime': '2026-11-13', 'exchange': 'NYSE', 'currency': 'USD'}


def _sdk():
    mmr = object.__new__(MMR)
    mmr._twelvedata_rest_client = MagicMock()
    return mmr


def test_twelvedata_snapshot_asks_for_the_dotted_class_share():
    mmr = _sdk()
    mmr._twelvedata_rest_client.quote.return_value = _Payload(_quote('BRK.B', 512.25))
    out = mmr.snapshot('BRK B', source='twelvedata')
    mmr._twelvedata_rest_client.quote.assert_called_once_with(symbol='BRK.B')
    assert out['last'] == 512.25


def test_twelvedata_snapshot_batch_asks_for_and_finds_the_dotted_class_share():
    mmr = _sdk()
    mmr._twelvedata_rest_client.quote.return_value = _Payload({
        'AAPL': _quote('AAPL', 270.0), 'BRK.B': _quote('BRK.B', 512.25)})
    rows = mmr.snapshot_batch(['aapl', 'BRK B'], source='twelvedata')
    mmr._twelvedata_rest_client.quote.assert_called_once_with(symbol='AAPL,BRK.B')
    assert [row['last'] for row in rows] == [270.0, 512.25]


def test_twelvedata_snapshot_refuses_a_symbol_it_cannot_spell():
    mmr = _sdk()
    with pytest.raises(ValueError, match='JPM PRC'):
        mmr.snapshot('JPM PRC', source='twelvedata')
    with pytest.raises(ValueError, match='JPM PRC'):
        mmr.snapshot_batch(['AAPL', 'JPM PRC'], source='twelvedata')
    assert mmr._twelvedata_rest_client.quote.call_count == 0
