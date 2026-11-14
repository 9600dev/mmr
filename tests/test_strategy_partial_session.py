"""A session that starts late must not be traded as if it were complete (audit 2026-11-14, B3.1).

``OpeningRangeBreakout`` took the high/low of whatever bars of today existed in
the opening-range window; ``VwapReclaim`` built session VWAP, its "open"
reference and the day's low the same way. A frame whose today starts late (a
deploy or reload between 09:30 and 10:15 ET, a start whose history backfill
failed) therefore yielded the range of a PARTIAL window and a BUY on a breakout
of it: bars from 09:50 gave range high 100.02 and a BUY against a real range
high of 101.52.

Both dispatch paths are pinned (the backtester uses ``precompute``/``on_bar``,
live uses ``on_prices``) and must agree at every bar. Complete sessions are
unchanged — ``tests/test_backtester_pin.py`` holds that bit for bit.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent))

from backtest_synth import FrameStorage, make_minute_bars  # noqa: E402
from strategies.opening_range_breakout import OpeningRangeBreakout  # noqa: E402
from strategies.vwap_reclaim import VwapReclaim  # noqa: E402
from trader.objects import Action, BarSize  # noqa: E402
from trader.simulation.backtester import Backtester, BacktestConfig  # noqa: E402
from trader.simulation.lookahead_check import assert_no_lookahead  # noqa: E402

NY = 'America/New_York'
ORB_PATH = 'strategies/opening_range_breakout.py'


# --------------------------------------------------------------------------
# OpeningRangeBreakout
# --------------------------------------------------------------------------

def _bars(index, price, volume):
    frame = pd.DataFrame({'open': price, 'high': price + .02, 'low': price - .02,
                          'close': price, 'volume': volume}, index=index.tz_convert('UTC'))
    frame.index.name = 'date'
    return frame


def _orb_frame():
    """A full previous session, then today 09:30-11:00 ET (1-min bars).

    09:35-09:44 trade at 101.5: the REAL 45-minute range high is 101.52.
    10:30 drifts to 100.6 on volume: above everything seen since 09:45, far
                 below the real range high.
    10:40 prints 101.8 on volume: a true breakout.
    10:50 prints 99.0 on volume: below the range low (99.98) — the exit.
    """
    prev = pd.date_range('2026-11-11 09:30', '2026-11-11 16:00', freq='1min', tz=NY, inclusive='left')
    day = pd.date_range('2026-11-12 09:30', '2026-11-12 11:00', freq='1min', tz=NY, inclusive='left')
    price = np.full(len(day), 100.0)
    volume = np.full(len(day), 1000.0)
    price[5:15] = 101.5
    for minute, level in ((60, 100.6), (70, 101.8), (80, 99.0)):
        price[minute], volume[minute] = level, 5000.0
    return pd.concat([_bars(prev, 100.0, 1000.0), _bars(day, price, volume)]), day


def _today_from(frame, day, start):
    """The same frame with today's bars before ``start`` (HH:MM ET) missing."""
    cut = pd.Timestamp(f'2026-11-12 {start}', tz=NY).tz_convert('UTC')
    return frame[(frame.index < day[0].tz_convert('UTC')) | (frame.index >= cut)]


def _at(frame, hhmm):
    """The frame up to and including today's ``hhmm`` ET bar."""
    return frame[frame.index <= pd.Timestamp(f'2026-11-12 {hhmm}', tz=NY).tz_convert('UTC')]


def _orb(bar_size=BarSize.Mins1, **params):
    orb = OpeningRangeBreakout()
    # The live idiom: deployment params arrive on the context.
    orb._context = SimpleNamespace(name='orb', paper_only=False, bar_size=bar_size,
                                   params={'RANGE_MINUTES': 45, **params})
    orb.RANGE_MINUTES, orb.VOLUME_MULT = 45, 1.3
    return orb


def _signals(strategy, frame):
    """{HH:MM ET: action} over the whole frame, via the backtest path."""
    state = strategy.precompute(frame)
    out = {}
    for i in range(len(frame)):
        signal = strategy.on_bar(frame, state, i)
        if signal is not None:
            out[frame.index[i].tz_convert(NY).strftime('%m-%d %H:%M')] = signal.action
    return out


class TestOrbPartialOpeningRange:
    def test_complete_session_trades_the_real_range(self):
        frame, _ = _orb_frame()
        orb = _orb()
        state = orb.precompute(frame)
        assert state['orb_high'][-1] == pytest.approx(101.52)
        assert state['orb_low'][-1] == pytest.approx(99.98)
        assert _signals(orb, frame) == {'11-12 10:40': Action.BUY, '11-12 10:50': Action.SELL}
        assert orb.on_prices(_at(frame, '10:30')) is None, '100.6 is inside the real range'

    @pytest.mark.parametrize('start', ['09:50', '10:05', '10:14'])
    def test_late_start_takes_no_entry(self, start):
        """The reviewer's reproduction: before the fix this was range high
        100.02 and a BUY at 10:30 (and again at 10:40)."""
        frame, day = _orb_frame()
        partial = _today_from(frame, day, start)
        orb = _orb()

        assert orb.on_prices(_at(partial, '10:30')) is None
        assert orb.on_prices(_at(partial, '10:40')) is None
        state = orb.precompute(partial)
        today = partial.index >= day[0].tz_convert('UTC')
        assert np.isnan(state['orb_high'][today]).all(), 'no entry range on a day that starts late'
        assert np.isnan(state['orb_low'][today]).all()
        assert Action.BUY not in _signals(orb, partial).values()

    def test_exit_still_fires_on_a_late_start_day(self):
        """A position carried into such a day can still be closed: the exit
        level is the low of the window bars that WERE seen."""
        frame, day = _orb_frame()
        partial = _today_from(frame, day, '09:50')
        orb = _orb()
        signal = orb.on_prices(_at(partial, '10:50'))
        assert signal is not None and signal.action == Action.SELL
        assert _signals(orb, partial) == {'11-12 10:50': Action.SELL}

    def test_start_within_the_tolerance_is_a_valid_range(self):
        frame, day = _orb_frame()
        orb = _orb()
        on_time = _today_from(frame, day, '09:32')          # default tolerance: 2 minutes
        assert orb.precompute(on_time)['orb_high'][-1] == pytest.approx(101.52)
        assert _signals(orb, on_time) == {'11-12 10:40': Action.BUY, '11-12 10:50': Action.SELL}
        too_late = _today_from(frame, day, '09:33')
        assert np.isnan(orb.precompute(too_late)['orb_high'][-1])

    def test_sparse_window_is_not_a_range(self):
        """On time, but under RANGE_MIN_BAR_FRACTION (0.8) of the 45 expected bars."""
        frame, day = _orb_frame()
        window = [stamp.tz_convert('UTC') for stamp in day[:45]]
        orb = _orb()

        dense = frame.drop(index=window[20:28])              # 37 of 45 bars: valid
        assert orb.precompute(dense)['orb_high'][-1] == pytest.approx(101.52)

        thin = frame.drop(index=window[20:30])               # 35 of 45 bars: not enough
        assert np.isnan(orb.precompute(thin)['orb_high'][-1])
        assert Action.BUY not in _signals(orb, thin).values()

        half = frame.drop(index=window[1::2])                # every other bar
        assert np.isnan(orb.precompute(half)['orb_high'][-1])

    def test_duplicate_timestamps_do_not_pad_the_bar_count(self):
        frame, day = _orb_frame()
        window = [stamp.tz_convert('UTC') for stamp in day[:45]]
        thin = frame.drop(index=window[20:30])               # 35 distinct minutes
        padded = pd.concat([thin, thin.loc[window[:5]]]).sort_index(kind='stable')
        assert np.isnan(_orb().precompute(padded)['orb_high'][-1])

    def test_thresholds_are_tunables_on_both_param_idioms(self):
        frame, day = _orb_frame()
        partial = _today_from(frame, day, '09:50')
        # live: deployment params on the context
        live = _orb(RANGE_START_TOLERANCE_MIN=30, RANGE_MIN_BAR_FRACTION=0.1)
        assert live.on_prices(_at(partial, '10:30')).action == Action.BUY
        # backtest: upper-case attribute shadowed on the instance
        backtest = _orb()
        backtest.RANGE_START_TOLERANCE_MIN, backtest.RANGE_MIN_BAR_FRACTION = 30, 0.1
        assert backtest.precompute(partial)['orb_high'][-1] == pytest.approx(100.02)
        # each condition holds alone
        assert _orb(RANGE_START_TOLERANCE_MIN=30).on_prices(_at(partial, '10:30')) is None
        assert _orb(RANGE_MIN_BAR_FRACTION=0.1).on_prices(_at(partial, '10:30')) is None

    def test_expected_bar_count_follows_the_bar_size(self):
        """5-minute bars: a complete 45-minute window holds 9, not 45."""
        frame, day = _orb_frame()
        five = frame.resample('5min').agg({'open': 'first', 'high': 'max', 'low': 'min',
                                           'close': 'last', 'volume': 'sum'}).dropna()
        orb = _orb(bar_size=BarSize.Mins5)
        orb.MIN_BARS = 5
        assert orb.precompute(five)['orb_high'][-1] == pytest.approx(101.52)
        late = five[(five.index < day[0].tz_convert('UTC'))
                    | (five.index >= pd.Timestamp('2026-11-12 09:50', tz=NY).tz_convert('UTC'))]
        assert np.isnan(orb.precompute(late)['orb_high'][-1])

    @pytest.mark.parametrize('start', [None, '09:32', '09:50', '10:05'])
    def test_live_and_backtest_paths_agree_at_every_bar(self, start):
        frame, day = _orb_frame()
        if start:
            frame = _today_from(frame, day, start)
        orb = _orb()
        state = orb.precompute(frame)
        seen = 0
        for i in range(orb.MIN_BARS, len(frame)):
            live = orb.on_prices(frame.iloc[:i + 1])
            backtest = orb.on_bar(frame, state, i)
            assert (live is None) == (backtest is None), frame.index[i]
            if live is not None:
                assert live.action == backtest.action
                seen += 1
        assert seen >= 1, 'the comparison must cover at least one signal'

    @pytest.mark.parametrize('start', [None, '09:32', '09:50', '10:05'])
    def test_precompute_has_no_lookahead(self, start):
        frame, day = _orb_frame()
        if start:
            frame = _today_from(frame, day, start)
        # Truncations that cross the end of the window, the window itself and the open.
        hides = (1, 2, 5, 10, 20, 46, 61, 80)
        assert_no_lookahead(_orb(), frame, hide_bars_sequence=hides)
        assert_no_lookahead(_orb(), _at(frame, '10:20'), hide_bars_sequence=hides)


class TestOrbBacktesterPath:
    """The backtester replays through precompute/on_bar: same rule there."""

    def _run(self, frame):
        import datetime as dt
        config = BacktestConfig(start_date=dt.datetime(2023, 1, 1), end_date=dt.datetime(2030, 1, 1),
                                bar_size=BarSize.Mins1)
        result = Backtester(FrameStorage({1: frame}), config).run_from_module(
            ORB_PATH, 'OpeningRangeBreakout', [1])
        return [str(signal.action) for signal in result.signals]

    def test_days_missing_their_first_twenty_minutes_are_never_bought(self):
        frame = make_minute_bars(n_days=12, seed=7)
        assert any('BUY' in action for action in self._run(frame)), 'control: complete days do trade'

        local = frame.index.tz_convert(NY)
        minute = local.hour * 60 + local.minute
        late = frame[~((minute >= 9 * 60 + 30) & (minute < 9 * 60 + 50))]
        assert not any('BUY' in action for action in self._run(late))


# --------------------------------------------------------------------------
# VwapReclaim
# --------------------------------------------------------------------------

def _reclaim_frame(first_bar='09:30', pre_market=False):
    """Heavy higher first bar, a long dip under VWAP, then a volume-confirmed
    reclaim as the last bar — the setup, starting at ``first_bar`` ET."""
    n = 75
    base = pd.Timestamp(f'2026-07-06 {first_bar}', tz=NY).tz_convert('UTC')
    index = pd.date_range(base, periods=n, freq='1min', name='date')
    close = [102.0] + [99.0] * (n - 2) + [99.6]
    volume = [5000.0] + [1000.0] * (n - 2) + [5000.0]
    frame = pd.DataFrame({'open': close, 'high': [c + .05 for c in close],
                          'low': [c - .05 for c in close], 'close': close, 'volume': volume}, index=index)
    if pre_market:
        early = pd.date_range(pd.Timestamp('2026-07-06 08:00', tz=NY).tz_convert('UTC'),
                              pd.Timestamp('2026-07-06 09:30', tz=NY).tz_convert('UTC'),
                              freq='1min', inclusive='left', name='date')
        quiet = pd.DataFrame({'open': 99.0, 'high': 99.05, 'low': 98.95, 'close': 99.0, 'volume': 10.0},
                             index=early)
        frame = pd.concat([quiet, frame])
    return frame


def _vwap(**attrs):
    strategy = VwapReclaim()
    strategy._context = SimpleNamespace(name='vwap', paper_only=False, params={})
    for key, value in attrs.items():
        setattr(strategy, key, value)
    return strategy


class TestVwapReclaimPartialSession:
    def test_session_from_the_open_still_fires(self):
        signal = _vwap().on_prices(_reclaim_frame('09:30'))
        assert signal is not None and signal.action == Action.BUY

    @pytest.mark.parametrize('first_bar', ['09:33', '09:50', '10:15'])
    def test_late_start_takes_no_entry(self, first_bar):
        """Before the fix: a BUY off the VWAP / open / low of a partial session."""
        frame = _reclaim_frame(first_bar)
        strategy = _vwap()
        assert strategy.on_prices(frame) is None
        assert not strategy.precompute(frame)['session_ok'].any()

    def test_start_within_the_tolerance_fires(self):
        assert _vwap().on_prices(_reclaim_frame('09:32')).action == Action.BUY

    def test_tolerance_is_a_tunable(self):
        frame = _reclaim_frame('09:50')
        assert _vwap(SESSION_START_TOLERANCE_MIN=30).on_prices(frame).action == Action.BUY
        assert _vwap(SESSION_START_TOLERANCE_MIN=0).on_prices(_reclaim_frame('09:31')) is None

    def test_pre_market_bars_do_not_stand_in_for_the_open(self):
        """Bars before 09:30 exist, the session itself starts at 09:50."""
        frame = _reclaim_frame('09:50', pre_market=True)
        state = _vwap().precompute(frame)
        assert not state['session_ok'].any()
        assert _vwap().on_prices(frame) is None

    def test_the_flag_is_per_day(self):
        late = _reclaim_frame('09:50')
        next_day = _reclaim_frame('09:30')
        next_day.index = next_day.index + pd.Timedelta(days=1)
        state = _vwap().precompute(pd.concat([late, next_day]))
        assert not state['session_ok'][:len(late)].any()
        assert state['session_ok'][len(late):].all()

    @pytest.mark.parametrize('first_bar,pre_market', [('09:30', False), ('09:30', True), ('09:50', False)])
    def test_live_and_backtest_paths_agree_at_every_bar(self, first_bar, pre_market):
        frame = _reclaim_frame(first_bar, pre_market)
        strategy = _vwap()
        state = strategy.precompute(frame)
        for i in range(strategy.MIN_BARS, len(frame)):
            live = strategy.on_prices(frame.iloc[:i + 1])
            backtest = strategy.on_bar(frame, state, i)
            assert (live is None) == (backtest is None), frame.index[i]
            if live is not None:
                assert live.action == backtest.action

    @pytest.mark.parametrize('first_bar,pre_market', [('09:30', False), ('09:30', True),
                                                      ('09:50', False), ('09:50', True)])
    def test_precompute_has_no_lookahead(self, first_bar, pre_market):
        """Truncations that cross the first in-session bar included: nothing
        about the session may be written onto the pre-market bars before it."""
        frame = _reclaim_frame(first_bar, pre_market)
        hides = (1, 2, 5, 10, 40, 74, 76, 100)
        strategy = _vwap(MIN_BARS=10)
        assert_no_lookahead(strategy, frame, hide_bars_sequence=hides)
        assert_no_lookahead(strategy, frame.iloc[:len(frame) - 70], hide_bars_sequence=(1, 2, 5, 10))
