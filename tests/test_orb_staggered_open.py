"""Bars that did not trade are not part of the opening range (audit 2026-11-14, B3.3).

The ASX opens in groups by ticker initial between 10:00:00 and about 10:09:15
Sydney. Until a name's own group opens, its LIVE one-minute bars are built
from quotes: flat at yesterday's close, volume 0. They are inside the session
and inside ORB's window (``RTH_OPEN_MIN`` = 600 on an ASX deployment), so the
live opening range began at 10:00 with yesterday's close in it:

* gap up   — the range LOW was yesterday's close (and so was the exit level);
* gap down — the range HIGH was yesterday's close, above anything that traded,
             so the real breakout was never taken.

Stored history is trades only, so a backtest of the same morning saw a window
whose first bar is the first trade. The rule now is the same for both: a
window bar with no traded volume neither sets the range, nor marks its start,
nor counts toward ``RANGE_MIN_BAR_FRACTION``.

A session in which every bar trades is unchanged
(``tests/test_strategy_partial_session.py``, ``tests/test_backtester_pin.py``).
"""
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from strategies.opening_range_breakout import OpeningRangeBreakout
from trader.objects import Action, BarSize
from trader.simulation.lookahead_check import assert_no_lookahead

SYD = 'Australia/Sydney'
ASX = {'SESSION_TZ': SYD, 'RTH_OPEN_MIN': 600, 'RTH_CLOSE_MIN': 960, 'RANGE_MINUTES': 30}
DAY = '2026-11-12'


def _bars(index, price, volume, wick=0.02):
    price = np.asarray(price, dtype=float) if not np.isscalar(price) else np.full(len(index), float(price))
    volume = np.asarray(volume, dtype=float) if not np.isscalar(volume) else np.full(len(index), float(volume))
    frame = pd.DataFrame({'open': price, 'high': price + wick, 'low': price - wick,
                          'close': price, 'volume': volume}, index=index.tz_convert('UTC'))
    frame.index.name = 'date'
    return frame


def _morning(prev_close, first_trade='10:09', pre_open_from='10:00', events=()):
    """Yesterday's full session at ``prev_close``; today to 11:30 Sydney.

    Before ``first_trade`` today's bars are quote-only: flat at yesterday's
    close with NO wick and volume 0 (what the live bar builder makes of quote
    updates that carry yesterday's last price). From ``first_trade`` the name
    trades around 25.65 between 25.50 and 25.80 (bar lows/highs 25.48 / 25.82).
    ``events``: ``(HH:MM, price)`` bars on five times the volume.
    """
    prev = pd.date_range('2026-11-11 10:00', '2026-11-11 16:00', freq='1min', tz=SYD, inclusive='left')
    quotes = pd.date_range(f'{DAY} {pre_open_from}', f'{DAY} {first_trade}', freq='1min', tz=SYD,
                           inclusive='left')
    trading = pd.date_range(f'{DAY} {first_trade}', f'{DAY} 11:30', freq='1min', tz=SYD, inclusive='left')
    price = np.full(len(trading), 25.65)
    price[1::4], price[3::4] = 25.80, 25.50
    volume = np.full(len(trading), 2000.0)
    for hhmm, level in events:
        k = trading.get_loc(pd.Timestamp(f'{DAY} {hhmm}', tz=SYD))
        price[k], volume[k] = level, 10000.0
    return pd.concat([_bars(prev, prev_close, 5000.0),
                      _bars(quotes, prev_close, 0.0, wick=0.0),
                      _bars(trading, price, volume)])


def _orb(**params):
    orb = OpeningRangeBreakout()
    orb._context = SimpleNamespace(name='orb_asx', paper_only=False, bar_size=BarSize.Mins1,
                                   params={**ASX, **params})
    orb.RANGE_MINUTES, orb.VOLUME_MULT = int({**ASX, **params}['RANGE_MINUTES']), 1.3
    return orb


def _signals(strategy, frame):
    state = strategy.precompute(frame)
    out = {}
    for i in range(len(frame)):
        signal = strategy.on_bar(frame, state, i)
        if signal is not None:
            out[frame.index[i].tz_convert(SYD).strftime('%H:%M')] = signal.action
    return out


def _at(frame, hhmm):
    return frame[frame.index <= pd.Timestamp(f'{DAY} {hhmm}', tz=SYD).tz_convert('UTC')]


class TestGroupThatOpensInsideTheTolerance:
    """A group-2 name: quote-only 10:00 and 10:01, first trade 10:02."""

    def test_gap_up_range_low_is_not_yesterdays_close(self):
        frame = _morning(prev_close=25.00, first_trade='10:02')
        state = _orb().precompute(frame)
        assert state['orb_low'][-1] == pytest.approx(25.48), 'was 25.00: yesterday\'s close'
        assert state['orb_high'][-1] == pytest.approx(25.82)
        assert state['exit_low'][-1] == pytest.approx(25.48)

    def test_gap_down_range_high_is_not_yesterdays_close(self):
        frame = _morning(prev_close=26.00, first_trade='10:02')
        state = _orb().precompute(frame)
        assert state['orb_high'][-1] == pytest.approx(25.82), 'was 26.00: yesterday\'s close'
        assert state['orb_low'][-1] == pytest.approx(25.48)

    def test_gap_down_real_breakout_is_taken(self):
        """25.90 on volume is above everything that traded in the window; it is
        below yesterday's close, which used to be the range high."""
        frame = _morning(prev_close=26.00, first_trade='10:02', events=[('10:45', 25.90)])
        orb = _orb()
        assert _signals(orb, frame) == {'10:45': Action.BUY}
        signal = orb.on_prices(_at(frame, '10:45'))
        assert signal is not None and signal.action == Action.BUY

    def test_gap_up_exit_fires_below_the_traded_low(self):
        """25.40 on volume is below the traded range low (25.48) and far above
        yesterday's close (25.00), which used to be the exit level."""
        frame = _morning(prev_close=25.00, first_trade='10:02', events=[('10:50', 25.40)])
        orb = _orb()
        assert _signals(orb, frame) == {'10:50': Action.SELL}
        signal = orb.on_prices(_at(frame, '10:50'))
        assert signal is not None and signal.action == Action.SELL

    def test_quote_bars_do_not_count_toward_the_bar_fraction(self):
        """28 of 30 window minutes traded: valid at the default 0.8, and not
        at a fraction that needs all 30 — the two quote bars are not bars."""
        frame = _morning(prev_close=25.00, first_trade='10:02')
        assert not np.isnan(_orb().precompute(frame)['orb_high'][-1])
        assert np.isnan(_orb(RANGE_MIN_BAR_FRACTION=1.0).precompute(frame)['orb_high'][-1])


class TestGroupThatOpensAfterTheTolerance:
    """A group-5 name (S-Z): quote-only 10:00-10:08, first trade 10:09."""

    def test_no_entry_range_when_trading_starts_nine_minutes_after_the_open(self):
        """Same answer a backtest gives on trades-only history of this morning.
        Before: a VALID range 25.00-25.82 from 10:00 and a BUY at 10:45."""
        frame = _morning(prev_close=25.00, events=[('10:45', 25.95)])
        orb = _orb()
        state = orb.precompute(frame)
        today = frame.index >= pd.Timestamp(f'{DAY} 00:00', tz=SYD).tz_convert('UTC')
        assert np.isnan(state['orb_high'][today]).all()
        assert np.isnan(state['orb_low'][today]).all()
        assert Action.BUY not in _signals(orb, frame).values()
        assert orb.on_prices(_at(frame, '10:45')) is None

    def test_live_frame_and_trades_only_history_give_the_same_range(self):
        """The live frame (quote bars before the first trade) and the stored
        one (no bars before it) are the same morning."""
        live = _morning(prev_close=25.00, events=[('10:45', 25.95), ('10:55', 25.30)])
        stored = live[~((live['volume'] == 0))]
        assert len(stored) == len(live) - 9
        for params in ({}, {'RTH_OPEN_MIN': 609}, {'RANGE_START_TOLERANCE_MIN': 10}):
            a, b = _orb(**params), _orb(**params)
            assert _signals(a, live) == _signals(b, stored), params
            sa, sb = a.precompute(live), b.precompute(stored)
            for key in ('orb_high', 'orb_low', 'exit_low'):
                np.testing.assert_array_equal(sa[key][-1], sb[key][-1], err_msg=f'{key} {params}')

    def test_exit_level_is_the_traded_low_even_though_the_range_is_not_valid(self):
        frame = _morning(prev_close=25.00, events=[('10:50', 25.40)])
        orb = _orb()
        assert orb.precompute(frame)['exit_low'][-1] == pytest.approx(25.48), 'was 25.00'
        assert _signals(orb, frame) == {'10:50': Action.SELL}

    def test_per_instrument_open_gives_the_traded_range(self):
        """The documented remedy: RTH_OPEN_MIN = the group's own open."""
        frame = _morning(prev_close=25.00, events=[('10:45', 25.95)])
        orb = _orb(RTH_OPEN_MIN=609)
        state = orb.precompute(frame)
        assert (state['orb_high'][-1], state['orb_low'][-1]) == pytest.approx((25.82, 25.48))
        assert _signals(orb, frame) == {'10:45': Action.BUY}

    def test_wider_tolerance_gives_the_traded_range_not_one_from_ten_oclock(self):
        """Window 10:00-10:30 with a 10-minute tolerance: 21 traded minutes of
        30, so the fraction has to allow it too — and the range is still only
        what traded."""
        frame = _morning(prev_close=25.00)
        assert np.isnan(_orb(RANGE_START_TOLERANCE_MIN=10).precompute(frame)['orb_high'][-1])
        state = _orb(RANGE_START_TOLERANCE_MIN=10, RANGE_MIN_BAR_FRACTION=0.7).precompute(frame)
        assert (state['orb_high'][-1], state['orb_low'][-1]) == pytest.approx((25.82, 25.48))


class TestBarsThatDidNotTrade:
    def test_a_window_in_which_nothing_traded_keeps_an_exit_level(self):
        """Halted through the whole window: no entry range, and the exit level
        falls back to the bars that were seen, as before — an exit level that
        vanished would be a refused exit."""
        frame = _morning(prev_close=25.00, first_trade='10:30', events=[('10:50', 24.50)])
        orb = _orb()
        state = orb.precompute(frame)
        assert np.isnan(state['orb_high'][-1])
        assert state['exit_low'][-1] == pytest.approx(25.00)
        assert _signals(orb, frame) == {'10:50': Action.SELL}

    def test_a_quote_bar_in_the_middle_of_the_window_does_not_set_the_range(self):
        frame = _morning(prev_close=25.00, first_trade='10:00')
        stamp = pd.Timestamp(f'{DAY} 10:15', tz=SYD).tz_convert('UTC')
        frame.loc[stamp, ['open', 'high', 'low', 'close', 'volume']] = [27.0, 27.0, 24.0, 27.0, 0.0]
        state = _orb().precompute(frame)
        assert (state['orb_high'][-1], state['orb_low'][-1]) == pytest.approx((25.82, 25.48))

    def test_nan_volume_is_not_a_trade(self):
        frame = _morning(prev_close=25.00, first_trade='10:02')
        quotes = frame['volume'] == 0
        frame.loc[quotes, 'volume'] = np.nan
        state = _orb().precompute(frame)
        assert (state['orb_high'][-1], state['orb_low'][-1]) == pytest.approx((25.82, 25.48))


class TestEveryBarTrades:
    """The control: a session with no quote-only bar is exactly as before."""

    def test_session_trading_from_the_open(self):
        frame = _morning(prev_close=25.00, first_trade='10:00', events=[('10:45', 25.95), ('10:55', 25.30)])
        orb = _orb()
        state = orb.precompute(frame)
        assert (state['orb_high'][-1], state['orb_low'][-1]) == pytest.approx((25.82, 25.48))
        assert state['exit_low'][-1] == pytest.approx(25.48)
        assert _signals(orb, frame) == {'10:45': Action.BUY, '10:55': Action.SELL}


@pytest.mark.parametrize('first_trade', ['10:00', '10:02', '10:09', '10:30'])
def test_live_and_backtest_paths_agree_at_every_bar(first_trade):
    frame = _morning(prev_close=26.00, first_trade=first_trade,
                     events=[('10:45', 25.95), ('10:55', 25.30)])
    orb = _orb()
    state = orb.precompute(frame)
    for i in range(orb.MIN_BARS, len(frame)):
        live = orb.on_prices(frame.iloc[:i + 1])
        backtest = orb.on_bar(frame, state, i)
        assert (live is None) == (backtest is None), frame.index[i]
        if live is not None:
            assert live.action == backtest.action


@pytest.mark.parametrize('first_trade', ['10:02', '10:09', '10:30'])
def test_precompute_has_no_lookahead(first_trade):
    frame = _morning(prev_close=25.00, first_trade=first_trade, events=[('10:45', 25.95)])
    hides = (1, 2, 5, 10, 20, 46, 61, 80, 85, 90)
    assert_no_lookahead(_orb(), frame, hide_bars_sequence=hides)
    assert_no_lookahead(_orb(), _at(frame, '10:20'), hide_bars_sequence=hides)
