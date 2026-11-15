"""A sell-off is not momentum: the ideas presets respect direction on every path.

docs/AUDIT_2026-11-14.md B5.8 ("IB ideas path scores sell-offs as BUY").

The three backends share the scoring functions, and those took `abs()` of the
day's change and of the gap. On the Massive and TwelveData paths that was
masked: the preset's signed change filter (momentum `min_change_pct=2.0`)
removes a falling name before it is scored. The IB path drops that filter
when the symbols are given explicitly (`--tickers` / `--universe`: "the user
chose these symbols"), so there a -8% day reached `_score_momentum`, earned
the same points as a +8% day, and came back `BUY` at the top of a momentum
list. That is the divergence; the `abs()` itself is in the shared functions.

Fixed in the shared functions, so no path can score the wrong direction
whatever its filters are: a move against the preset's direction earns no
points, and the preset's actionable signal requires the day to have gone the
preset's way.
"""
from unittest.mock import MagicMock, patch

import pytest

from trader.tools.idea_scanner import (
    PRESETS,
    IBIdeaScanner,
    IdeaScanner,
    _SCORE_FUNCTIONS,
    apply_filters,
    merge_filters,
)
from trader.trading.trading_filter import TradingFilter

PREV_CLOSE = 50.0


def _history(prev_close=PREV_CLOSE, bars=30):
    """Daily bars: flat at `prev_close`, yesterday on 400k shares."""
    return [{'date': f'2026-10-{1 + i:02d}', 'open': prev_close, 'high': prev_close + 0.5,
             'low': prev_close - 0.5, 'close': prev_close, 'volume': 400_000} for i in range(bars)]


def _snapshot(symbol, change_pct, volume=2_000_000):
    """Today: opened at the gap, last trade `change_pct` from yesterday's close."""
    last = round(PREV_CLOSE * (1 + change_pct / 100.0), 4)
    return {'symbol': symbol, 'conId': 100, 'exchange': 'ASX', 'currency': 'AUD',
            'bid': last - 0.01, 'ask': last + 0.01, 'last': last, 'open': last,
            'high': max(last, PREV_CLOSE), 'low': min(last, PREV_CLOSE), 'close': PREV_CLOSE,
            'volume': volume}


def _resolved(symbol):
    definition = MagicMock()
    definition.conId, definition.symbol = 100, symbol
    definition.primaryExchange, definition.currency = 'ASX', 'AUD'
    definition.stockType, definition.longName = 'COMMON', symbol
    return definition


def _ib_scan(preset, change_pct, symbol='BHP'):
    """`mmr ideas <preset> --location STK.AU.ASX --tickers BHP`, one row."""
    rpc = MagicMock()
    calls = MagicMock()
    rpc.rpc.return_value = calls
    calls.resolve_contract.return_value = [_resolved(symbol)]
    calls.get_snapshots_batch.return_value = [_snapshot(symbol, change_pct)]
    calls.get_history_bars.return_value = _history()
    with patch('trader.trading.trading_filter.TradingFilter.load', return_value=TradingFilter.default()):
        frame = IBIdeaScanner(rpc).scan(preset=preset, location='STK.AU.ASX', tickers=[symbol])
    calls.scanner_data.assert_not_called()
    assert len(frame) == 1, 'explicit tickers are not filtered out by the preset change filter'
    return frame.iloc[0]


# --------------------------------------------------------------------------
# the reproduction: one row, IB path, explicit ticker
# --------------------------------------------------------------------------

@pytest.mark.parametrize('preset', ['momentum', 'gap-up', 'breakout'])
def test_ib_path_does_not_call_an_eight_percent_sell_off_a_buy(preset):
    down = _ib_scan(preset, -8.0)
    assert down['change_pct'] == -8.0 and down['rel_vol'] == 5.0
    assert down['signal'] != 'BUY', f'{preset}: a -8% day on 5x volume came back {down["signal"]}'


@pytest.mark.parametrize('preset', ['momentum', 'gap-up', 'breakout'])
def test_ib_path_scores_a_sell_off_below_the_same_sized_rally(preset):
    down, up = _ib_scan(preset, -8.0), _ib_scan(preset, +8.0)
    assert up['signal'] == 'BUY'
    assert down['score'] < up['score'], (down['score'], up['score'])


def test_ib_path_gap_down_does_not_call_a_rally_a_sell():
    up, down = _ib_scan('gap-down', +8.0), _ib_scan('gap-down', -8.0)
    assert up['signal'] != 'SELL'
    assert down['score'] > up['score']


def test_ib_path_mean_reversion_does_not_buy_a_name_that_is_already_up():
    """The preset's own filter is `max_change_pct=0.0`: a bounce candidate has
    not bounced. Relaxing the filter must not relax what BUY means."""
    oversold = {'ticker': 'X', 'price': 40.0, 'change_pct': -2.0, 'volume': 1_000_000, 'gap_pct': 0.0,
                'rel_vol': 4.0, 'range_pct': 3.0, 'rsi': 20.0, 'sma_20': 46.0, 'sma_50': 50.0}
    score_fn = _SCORE_FUNCTIONS['_score_mean_reversion']
    assert score_fn(oversold)[1] == 'BUY'
    assert score_fn(dict(oversold, change_pct=0.0))[1] == 'BUY'
    already_up = score_fn(dict(oversold, change_pct=6.0))
    assert already_up[1] == 'WATCH' and already_up[0] == score_fn(oversold)[0]


# --------------------------------------------------------------------------
# parity: a row that goes the preset's way scores exactly as before. (What
# does differ on the Massive / TwelveData paths is pinned further down: a gap
# AGAINST the preset on a row whose day's change still passes the filter.)
# --------------------------------------------------------------------------

def _row(**overrides):
    row = {'ticker': 'X', 'price': 54.0, 'change_pct': 8.0, 'volume': 2_000_000, 'gap_pct': 8.0,
           'rel_vol': 5.0, 'range_pct': 8.0, 'spread_pct': 0.04, 'vwap': 53.0,
           'rsi': 62.0, 'ema_9': 51.0, 'sma_20': 50.0, 'sma_50': 49.0}
    row.update(overrides)
    return row


# (preset, row, the score and signal the functions gave BEFORE this change —
# recorded by running the unchanged functions over these rows)
_UNCHANGED = [
    ('momentum', _row(), (57.0, 'BUY')),
    ('momentum', _row(change_pct=2.0, rel_vol=1.0, rsi=55.0), (21.5, 'WATCH')),
    ('gap-up', _row(), (67.0, 'BUY')),
    ('gap-up', _row(change_pct=3.0, gap_pct=0.0, rel_vol=6.0), (46.0, 'BUY')),
    ('breakout', _row(), (85.0, 'BUY')),
    ('breakout', _row(change_pct=1.0, rel_vol=0.5, ema_9=55.0, sma_20=56.0), (20.5, 'WATCH')),
    ('gap-down', _row(price=46.0, change_pct=-8.0, gap_pct=-8.0, rsi=30.0, sma_20=50.0), (49.5, 'SELL')),
    ('mean-reversion', _row(price=40.0, change_pct=-2.0, rsi=20.0, sma_20=46.0, sma_50=50.0,
                            rel_vol=4.0), (80.0, 'BUY')),
    ('volatile', _row(), (61.0, 'SELL')),
    ('volatile', _row(change_pct=-8.0), (61.0, 'BUY')),
]


@pytest.mark.parametrize('preset, row, expected', _UNCHANGED)
def test_a_row_that_goes_the_presets_way_scores_as_it_always_did(preset, row, expected):
    filters = merge_filters(PRESETS[preset])
    assert apply_filters([row], filters) == [row], 'the row passes the preset filter on the Massive path'
    score, signal = _SCORE_FUNCTIONS[PRESETS[preset].score_fn](row)
    assert (round(score, 1), signal) == expected


@pytest.mark.parametrize('preset', ['momentum', 'gap-up', 'breakout'])
def test_massive_path_never_showed_the_sell_off_and_still_does_not(preset):
    """Why this was invisible there: the falling row never reached a score."""
    snapshot = MagicMock()
    snapshot.ticker = 'XYZ'
    snapshot.day = MagicMock(close=46.0, volume=2_000_000, open=46.0, high=50.0, low=46.0, vwap=47.0)
    snapshot.prev_day = MagicMock(close=50.0, volume=400_000)
    snapshot.todays_change_percent = -8.0
    snapshot.last_quote = None
    client = MagicMock()
    client.get_snapshot_all.return_value = [snapshot]
    with patch('trader.trading.trading_filter.TradingFilter.load', return_value=TradingFilter.default()):
        frame = IdeaScanner(client).scan(preset=preset, source='tickers', tickers=['XYZ'])
    assert frame.empty


# --------------------------------------------------------------------------
# the shared functions, wrong direction
# --------------------------------------------------------------------------

@pytest.mark.parametrize('preset, wrong_way', [
    ('momentum', {'change_pct': -8.0}),
    ('gap-up', {'change_pct': -8.0, 'gap_pct': -8.0}),
    ('breakout', {'change_pct': -8.0}),
])
def test_a_move_against_a_bullish_preset_earns_nothing_and_is_never_a_buy(preset, wrong_way):
    score_fn = _SCORE_FUNCTIONS[PRESETS[preset].score_fn]
    flat = {name: 0.0 for name in wrong_way}
    against, none = score_fn(_row(**wrong_way)), score_fn(_row(**flat))
    assert against[0] == none[0], 'a fall must not add points a flat day would not get'
    assert against[1] == 'WATCH' and none[1] == 'WATCH'


def test_a_gap_up_is_not_a_gap_down():
    score_fn = _SCORE_FUNCTIONS['_score_gap_down']
    falling = _row(price=46.0, change_pct=-8.0, gap_pct=-8.0, rsi=30.0)
    assert score_fn(falling)[1] == 'SELL'
    gapped_up = score_fn(dict(falling, gap_pct=+8.0))
    assert gapped_up[0] == score_fn(dict(falling, gap_pct=0.0))[0] < score_fn(falling)[0]
    assert score_fn(dict(falling, change_pct=+8.0))[1] == 'WATCH'


def test_a_gap_down_is_not_a_gap_up():
    """The one change a Massive-path row can see: it passes the gap-up change
    filter (+4% on the day) having gapped DOWN 6%. That gap used to earn the
    12 points an up-gap earns."""
    score_fn = _SCORE_FUNCTIONS['_score_gap_up']
    rising = _row(change_pct=4.0, gap_pct=6.0)
    assert apply_filters([dict(rising, gap_pct=-6.0)], merge_filters(PRESETS['gap-up']))
    reversed_gap = score_fn(dict(rising, gap_pct=-6.0))
    assert reversed_gap[0] == score_fn(dict(rising, gap_pct=0.0))[0] == score_fn(rising)[0] - 12


def test_volatile_is_unsigned_on_purpose():
    """Size of the move is the point of this preset; its signal already reads
    the sign (it fades the move)."""
    score_fn = _SCORE_FUNCTIONS['_score_volatile']
    assert score_fn(_row(change_pct=-8.0))[0] == score_fn(_row(change_pct=8.0))[0]


@pytest.mark.parametrize('preset', sorted(PRESETS))
def test_missing_change_and_gap_are_scored_as_no_move(preset):
    row = _row()
    row.pop('change_pct')
    row['gap_pct'] = None
    score, signal = _SCORE_FUNCTIONS[PRESETS[preset].score_fn](row)
    assert 0 <= score <= 100 and signal in ('BUY', 'SELL', 'WATCH')
