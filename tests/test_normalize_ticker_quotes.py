"""A one-sided quote is not a midpoint (audit 2026-11-14, B3.7).

IB reports "no bid" / "no ask" as ``-1`` (and sometimes ``0``), not NaN. The
live tick normaliser fell back to ``(bid + ask) / 2`` whenever there was no
last trade and both sides were merely not-NaN, so bid 100 / ask -1 priced the
tick at 49.5 and a live bar opened at half the market.

The rule pinned here is the one the function's own comment states: last trade
when there is one, otherwise the midpoint of bid AND ask, otherwise nothing.
A side that is ``-1``, ``0``, NaN or missing is not a quote; one side alone is
not a price.
"""
import datetime as dt
import math

import numpy as np
import pandas as pd
import pytest
from ib_async import Contract
from ib_async.ticker import Ticker

from trader.data.market_data import normalize_ticker, resample_ticks_to_bars
from trader.strategy.live_bars import LiveBarBuffer

NAN = float('nan')
T0 = pd.Timestamp('2026-11-12 09:05', tz='UTC')      # 04:05 ET, a thin pre-market book


def _tick(seconds=0, last=NAN, bid=NAN, ask=NAN, volume=NAN):
    ticker = Ticker(contract=Contract(conId=5437, symbol='CAT', secType='STK', exchange='SMART',
                                      primaryExchange='NYSE', currency='USD'))
    ticker.time = (T0 + pd.Timedelta(seconds=seconds)).to_pydatetime()
    ticker.last, ticker.bid, ticker.ask, ticker.volume = last, bid, ask, volume
    return ticker


def _price(**kwargs):
    return float(normalize_ticker(_tick(**kwargs))['close'].iloc[0])


@pytest.mark.parametrize('bid, ask', [
    (100.0, -1.0),      # IB "no ask"
    (-1.0, 100.0),      # IB "no bid"
    (100.0, 0.0),
    (0.0, 100.0),
    (100.0, NAN),
    (NAN, 100.0),
    (100.0, None),
    (-1.0, -1.0),       # an empty book is not a quote at -1 either
    (0.0, 0.0),
])
def test_one_sided_or_empty_quote_is_not_a_price(bid, ask):
    row = normalize_ticker(_tick(bid=bid, ask=ask)).iloc[0]
    for column in ('open', 'high', 'low', 'close'):
        assert math.isnan(row[column]), f'{column}={row[column]} from bid={bid} ask={ask}'


def test_reported_case_bid_100_ask_minus_one_is_not_49_5():
    assert not _price(bid=100.0, ask=-1.0) == pytest.approx(49.5)
    assert math.isnan(_price(bid=100.0, ask=-1.0))


def test_two_sided_quote_still_gives_the_midpoint():
    assert _price(bid=200.0, ask=200.10) == pytest.approx(200.05)


def test_last_trade_is_used_whatever_the_book_looks_like():
    assert _price(last=1249.5, bid=-1.0, ask=1250.0) == 1249.5
    assert _price(last=1249.5, bid=-1.0, ask=-1.0) == 1249.5


@pytest.mark.parametrize('last', [-1.0, 0.0])
def test_a_sentinel_last_is_not_a_trade_price(last):
    """``-1`` / ``0`` in ``last`` means "no last trade": same fallback as NaN."""
    assert _price(last=last, bid=99.0, ask=101.0) == pytest.approx(100.0)
    assert math.isnan(_price(last=last, bid=99.0, ask=-1.0))
    assert math.isnan(_price(last=last))


def test_sentinels_do_not_reach_the_quote_columns():
    """The bar's ``bid`` / ``ask`` / ``last`` columns carry quotes, not ``-1``."""
    row = normalize_ticker(_tick(last=-1.0, bid=-1.0, ask=1250.0)).iloc[0]
    assert math.isnan(row['bid'])
    assert math.isnan(row['last'])
    assert row['ask'] == 1250.0


def test_real_quote_columns_are_preserved():
    row = normalize_ticker(_tick(last=150.2, bid=150.0, ask=150.5)).iloc[0]
    assert (row['bid'], row['ask'], row['last']) == (150.0, 150.5, 150.2)


# A fresh subscription on a thin book: quotes arrive before the first last
# price and the book is one-sided for the first seconds.
_SEQUENCE = [dict(seconds=0, bid=-1.0, ask=1250.0),
             dict(seconds=5, last=1249.5, bid=-1.0, ask=1250.0),
             dict(seconds=10, last=1249.5, bid=1248.0, ask=1250.0),
             dict(seconds=65, last=1249.5, bid=1248.0, ask=1250.0)]


def test_live_bar_is_not_opened_at_half_price_by_a_one_sided_book():
    buffer = LiveBarBuffer(None, '1min')
    for kwargs in _SEQUENCE:
        buffer.append(normalize_ticker(_tick(**kwargs)))
    bar = buffer.completed.iloc[0]
    assert (bar['open'], bar['high'], bar['low'], bar['close']) == (1249.5, 1249.5, 1249.5, 1249.5)


def test_resampled_bar_is_not_opened_at_half_price_by_a_one_sided_book():
    """The batch path (a buffer primed from retained ticks) reads the same price."""
    ticks = pd.concat([normalize_ticker(_tick(**kwargs)) for kwargs in _SEQUENCE])
    bars = resample_ticks_to_bars(ticks, '1min')
    assert len(bars) == 1
    bar = bars.iloc[0]
    assert (bar['open'], bar['high'], bar['low'], bar['close']) == (1249.5, 1249.5, 1249.5, 1249.5)
    assert np.isfinite(bars[['open', 'high', 'low', 'close']].to_numpy()).all()


def test_a_minute_of_one_sided_quotes_only_forms_no_bar():
    buffer = LiveBarBuffer(None, '1min')
    for seconds in (0, 20, 40):
        buffer.append(normalize_ticker(_tick(seconds=seconds, bid=1248.0, ask=-1.0)))
    buffer.append(normalize_ticker(_tick(seconds=65, last=1249.5, bid=1248.0, ask=1250.0)))
    buffer.append(normalize_ticker(_tick(seconds=125, last=1249.5, bid=1248.0, ask=1250.0)))
    assert list(buffer.completed.index) == [T0 + pd.Timedelta(minutes=1)]
    assert buffer.completed['open'].iloc[0] == 1249.5
