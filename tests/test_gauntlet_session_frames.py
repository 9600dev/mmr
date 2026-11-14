"""Gauntlet S2 (lookahead) and S3 (battery) must not pass by examining nothing.

docs/AUDIT_2026-11-14.md B4.3. Both stages ran the strategy on frames that
start 09:30 **UTC** — 04:30 New York — and so contain no regular-session bar.
A session-gated strategy computed nothing on them: every array NaN, zero
signals, both stages green. An opening-range breakout whose "range" was the
whole day's high and low passed S2.

What is pinned here:

* the session-stamped frames really cover the session, in the strategy's own
  zone and hours;
* the lookahead check cuts at interior points, which is what finds a
  per-day leak (trailing cuts alone miss it on most frames);
* a stage that compared nothing, or saw no signal on any frame, does not
  pass — and a strategy that is legitimately quiet on most frames still does;
* the real deployed strategies are genuinely exercised.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from strategies.opening_range_breakout import OpeningRangeBreakout
from strategies.vwap_reclaim import VwapReclaim
from trader.data.backtest_store import compute_strategy_hash
from trader.data.bar_quality import impossible_mask
from trader.data.gauntlet_store import GauntletStore
from trader.mmr_cli import (
    _gauntlet_check_lookahead,
    _gauntlet_frames,
    _gauntlet_lookahead_coverage,
    _gauntlet_run,
    _gauntlet_run_battery,
    _gauntlet_session_spec,
    _handle_strategies_gauntlet,
)
from trader.objects import Action
from trader.simulation import synthetic_markets as sm
from trader.simulation.lookahead_check import (
    KeyCoverage,
    LookaheadDetected,
    LookaheadReport,
    assert_no_lookahead,
    interior_cut_points,
    lookahead_report,
)
from trader.trading.strategy import Strategy

REPO = Path(__file__).resolve().parent.parent
NY = 'America/New_York'
LEGACY_FRAMES = set(sm.battery())


class LeakyORB(OpeningRangeBreakout):
    """The reviewer's leak: the "breakout level" is the WHOLE day's session
    high/low — known only at the close — broadcast to every bar of the day."""

    def precompute(self, prices):
        state = super().precompute(prices)
        if not state:
            return state
        local = prices.index.tz_convert(NY)
        day = pd.Series(local.date, index=prices.index)
        rth = pd.Series(state['rth'], index=prices.index)
        state['orb_high'] = prices['high'].where(rth).groupby(day).transform('max').to_numpy()
        state['orb_low'] = prices['low'].where(rth).groupby(day).transform('min').to_numpy()
        return state


def _local_minutes(frame: pd.DataFrame, tz: str = NY) -> np.ndarray:
    local = frame.index.tz_convert(tz)
    return (local.hour * 60 + local.minute).to_numpy()


def _write(directory: Path, filename: str, body: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    path.write_text(body)
    return path


# ---------------------------------------------------------------------------
# The frames
# ---------------------------------------------------------------------------

class TestSessionFrames:
    def test_the_original_intraday_frames_contain_no_session_bar(self):
        """The defect, stated as data: nothing in battery() is inside
        09:30-16:00 New York, so a session-gated strategy is idle on all of it."""
        for name in ('with_gaps', 'zero_volume', 'halted', 'nan_rows', 'trending', 'choppy'):
            minutes = _local_minutes(sm.battery()[name])
            assert not ((minutes >= 570) & (minutes < 960)).any(), name

    @pytest.mark.parametrize('generator', [sm.session_trending, sm.session_choppy])
    def test_default_frames_are_whole_new_york_sessions(self, generator):
        frame = generator()
        local = frame.index.tz_convert(NY)
        minutes = _local_minutes(frame)

        assert list(frame.columns) == ['open', 'high', 'low', 'close', 'volume']
        assert frame.index.name == 'date' and str(frame.index.tz) == 'UTC'
        assert frame.index.is_monotonic_increasing and frame.index.is_unique
        # Five sessions, each the full 09:30-15:59, weekdays only, and the run
        # crosses a weekend (a boundary a per-day computation must survive).
        per_day = pd.Series(minutes, index=local.date).groupby(level=0).agg(['min', 'max', 'size'])
        assert len(per_day) == 5
        assert (per_day['min'] == 570).all() and (per_day['max'] == 959).all()
        assert (per_day['size'] == 390).all()
        assert (local.dayofweek < 5).all()
        days = pd.to_datetime(pd.Series(local.date).drop_duplicates())
        assert days.diff().dt.days.max() == 3

    def test_frames_follow_a_declared_session(self):
        frame = sm.session_trending(session_tz='Australia/Sydney',
                                    open_minute=600, close_minute=960)
        minutes = _local_minutes(frame, 'Australia/Sydney')
        assert minutes.min() == 600 and minutes.max() == 959
        assert len(frame) == 5 * 360
        # ...and are NOT New York session bars under another name.
        ny = _local_minutes(frame)
        assert not ((ny >= 570) & (ny < 960)).any()

    def test_extended_frame_has_thin_bars_around_each_session(self):
        frame = sm.session_extended()
        minutes = _local_minutes(frame)
        in_session = (minutes >= 570) & (minutes < 960)
        assert minutes.min() == 570 - 120 and minutes.max() == 960 + 60 - 1
        assert in_session.sum() == 3 * 390
        assert (frame['volume'][~in_session] == 0).any()
        assert (frame['volume'][in_session] > 0).all()

    def test_halts_frame_puts_the_nasty_shapes_inside_sessions(self):
        frame = sm.session_halts()
        missing = frame['close'].isna().to_numpy()
        minutes = _local_minutes(frame)
        # An interior run of missing rows, mid-session...
        interior = np.flatnonzero(missing[:-5])
        assert len(interior) >= 3
        assert ((minutes[interior] >= 570) & (minutes[interior] < 960)).all()
        # ...the newest bars missing...
        assert missing[-5:].all()
        # ...and a stretch stuck at one price on zero volume.
        stuck = (frame['volume'] == 0) & (frame['high'] == frame['low'])
        assert stuck.sum() >= 3

    @pytest.mark.parametrize('generator', [sm.daily_trending, sm.daily_choppy])
    def test_daily_frames_are_long_business_day_series(self, generator):
        frame = generator()
        assert len(frame) == 400
        assert (frame.index.dayofweek < 5).all()
        assert frame.index.to_series().diff().min() >= pd.Timedelta(days=1)

    def test_every_bar_is_structurally_possible(self):
        for name, frame in {**sm.session_battery(), **sm.daily_battery()}.items():
            assert not impossible_mask(frame).any(), name

    def test_generators_are_deterministic(self):
        for generator in (sm.session_trending, sm.session_choppy, sm.session_extended,
                          sm.session_halts, sm.daily_trending, sm.daily_choppy):
            pd.testing.assert_frame_equal(generator(), generator())

    def test_bad_session_bounds_are_refused(self):
        with pytest.raises(ValueError, match='open < close'):
            sm.session_trending(open_minute=960, close_minute=570)


# ---------------------------------------------------------------------------
# Interior truncation points
# ---------------------------------------------------------------------------

class TestInteriorCutPoints:
    def test_session_anchored_cuts_land_inside_every_session(self):
        frame = sm.session_trending()
        cuts = set(interior_cut_points(frame.index, session_tz=NY,
                                       open_minute=570, close_minute=960))
        for day in range(5):
            start = day * 390
            # inside the opening range, either side of its end, an hour in,
            # mid-session, half an hour before the close, the last bar.
            for offset in (5, 15, 30, 31, 60, 195, 360, 389):
                assert start + offset in cuts, (day, offset)
            if day:
                assert start in cuts            # the boundary between two days

    def test_extended_frame_is_cut_at_the_open_and_the_close(self):
        frame = sm.session_extended()
        cuts = set(interior_cut_points(frame.index, session_tz=NY,
                                       open_minute=570, close_minute=960))
        per_day = 120 + 390 + 60
        for day in range(3):
            assert day * per_day + 120 in cuts          # pre-market visible, session hidden
            assert day * per_day + 120 + 390 in cuts    # session visible, post-market hidden

    def test_cuts_are_strictly_interior_sorted_and_unique(self):
        for frame in (sm.session_extended(), sm.daily_trending(), sm.ohlcv_trending()):
            cuts = interior_cut_points(frame.index, session_tz=NY,
                                       open_minute=570, close_minute=960)
            assert cuts == sorted(set(cuts))
            assert cuts and cuts[0] > 0 and cuts[-1] < len(frame)

    def test_daily_and_non_datetime_indexes_get_evenly_spread_cuts(self):
        expected = [40 * k for k in range(1, 10)]
        assert interior_cut_points(sm.daily_trending().index) == expected
        assert interior_cut_points(pd.RangeIndex(400)) == expected
        assert interior_cut_points(pd.RangeIndex(2)) == []

    def test_a_long_frame_does_not_get_a_cut_per_day(self):
        frame = sm.driftless_session_bars(days=60, seed=1)
        cuts = interior_cut_points(frame.index, session_tz=NY,
                                   open_minute=570, close_minute=960)
        assert len(cuts) <= 9 + 8 * 11
        assert min(cuts) < 390 and max(cuts) > 59 * 390      # first and last day both cut


# ---------------------------------------------------------------------------
# The lookahead check itself
# ---------------------------------------------------------------------------

class TestLookaheadCheck:
    def test_per_day_leak_is_invisible_on_the_utc_frames(self):
        """Why S2 passed it: on a frame with no session bar the leaked arrays
        are all-NaN, so every truncation agrees."""
        frame = sm.ohlcv_trending()
        assert_no_lookahead(LeakyORB(), frame)
        report = lookahead_report(LeakyORB(), frame)
        assert report.keys['orb_high'].all_nan and report.keys['orb_low'].all_nan
        assert 'orb_high' not in report.exercised

    @pytest.mark.parametrize('generator', [sm.session_trending, sm.session_choppy,
                                           sm.session_extended])
    def test_per_day_leak_is_caught_on_session_frames(self, generator):
        with pytest.raises(LookaheadDetected) as caught:
            assert_no_lookahead(LeakyORB(), generator())
        assert caught.value.key in ('orb_high', 'orb_low')

    def test_interior_cuts_are_what_catch_it(self):
        """Hiding only the last 1/2/5/10 bars sees the leak when the day's
        extreme happens to fall in those bars; cutting inside the session
        sees it every time."""
        trailing = interior = 0
        seeds = range(12)
        for seed in seeds:
            frame = sm.driftless_session_bars(days=3, seed=seed)
            try:
                assert_no_lookahead(LeakyORB(), frame, cut_points=())
            except LookaheadDetected:
                trailing += 1
            try:
                assert_no_lookahead(LeakyORB(), frame)
            except LookaheadDetected:
                interior += 1
        assert interior == len(seeds)
        assert trailing < len(seeds) // 2

    @pytest.mark.parametrize('generator', [sm.session_trending, sm.session_choppy,
                                           sm.session_extended])
    def test_honest_orb_is_clean_and_examined(self, generator):
        frame = generator()
        report = lookahead_report(
            OpeningRangeBreakout(), frame,
            cut_points=interior_cut_points(frame.index, session_tz=NY,
                                           open_minute=570, close_minute=960))
        assert {'orb_high', 'orb_low', 'vol_avg'} <= set(report.exercised)
        assert len(report.truncations) > 20
        assert np.isfinite(OpeningRangeBreakout().precompute(frame)['orb_high']).sum() > 300

    def test_report_separates_examined_from_degenerate(self):
        class Mixed(Strategy):
            def precompute(self, prices):
                n = len(prices)
                return {
                    'sma': prices['close'].rolling(5).mean().to_numpy(),
                    'never': np.full(n, np.nan),
                    'flat': np.zeros(n),
                    'table': prices[['open', 'close']],
                    'scalar': 3.0,
                }

        report = lookahead_report(Mixed(), sm.ohlcv_trending())
        assert report.exercised == ['sma']
        assert set(report.degenerate) == {'never', 'flat'}
        assert report.keys['never'].all_nan and not report.keys['flat'].all_nan
        assert set(report.uncheckable) == {'table', 'scalar'}
        assert report.keys['sma'].compared == len(report.truncations) > 4

    def test_empty_state_reports_nothing_compared(self):
        # Ten bars: under ORB's MIN_BARS, so precompute opts out with {}.
        report = lookahead_report(OpeningRangeBreakout(), sm.session_trending().iloc[:10])
        assert report.keys == {} and report.truncations == () and report.exercised == []

    def test_empty_cut_points_means_trailing_bars_only(self):
        seen = []

        class Recorder(Strategy):
            def precompute(self, prices):
                seen.append(len(prices))
                return {'c': prices['close'].to_numpy()}

        assert_no_lookahead(Recorder(), sm.ohlcv_trending(), cut_points=())
        assert seen == [240, 239, 238, 235, 230]


# ---------------------------------------------------------------------------
# S2 vacuity — the pure decision
# ---------------------------------------------------------------------------

def _report(**keys) -> LookaheadReport:
    return LookaheadReport(total_bars=100, truncations=(90, 50), keys=keys)


LIVE = KeyCoverage(compared=2, informative=True, all_nan=False)
NAN = KeyCoverage(compared=2, informative=False, all_nan=True)
FLAT = KeyCoverage(compared=2, informative=False, all_nan=False)


class TestLookaheadCoverage:
    def test_examined_everything(self):
        got = _gauntlet_lookahead_coverage({'a': _report(x=LIVE, y=LIVE)})
        assert got['vacuous'] is None
        assert got['exercised_keys'] == ['x', 'y']

    def test_no_state_on_any_frame_is_vacuous(self):
        got = _gauntlet_lookahead_coverage({'a': None, 'b': None})
        assert 'empty state on every frame' in got['vacuous']

    def test_only_degenerate_arrays_is_vacuous(self):
        got = _gauntlet_lookahead_coverage({'a': _report(x=NAN, y=FLAT)})
        assert 'all-NaN, constant' in got['vacuous']
        assert "state['x'], state['y']" in got['vacuous']

    def test_an_array_never_computed_is_vacuous_even_beside_live_ones(self):
        """The shape of the original defect: `close` and `vol_avg` were
        informative, the opening range beside them was NaN throughout."""
        got = _gauntlet_lookahead_coverage({
            'a': _report(close=LIVE, orb_high=NAN),
            'b': _report(close=LIVE, orb_high=NAN),
        })
        assert "state['orb_high'] all-NaN on every frame" in got['vacuous']
        assert got['unexercised_keys'] == ['orb_high']

    def test_computed_on_one_frame_is_enough(self):
        got = _gauntlet_lookahead_coverage({
            'utc': _report(close=LIVE, orb_high=NAN),
            'session': _report(close=LIVE, orb_high=LIVE),
            'short': None,
        })
        assert got['vacuous'] is None
        assert got['exercised_keys'] == ['close', 'orb_high']

    def test_a_constant_array_is_reported_not_failed(self):
        got = _gauntlet_lookahead_coverage({'a': _report(close=LIVE, flag=FLAT)})
        assert got['vacuous'] is None
        assert got['constant_keys'] == ['flag']

    def test_compared_zero_times_is_not_examined(self):
        never = KeyCoverage(compared=0, informative=True, all_nan=False)
        got = _gauntlet_lookahead_coverage({'a': _report(x=never)})
        assert 'never re-derived' in got['vacuous']

    def test_state_without_arrays_is_vacuous(self):
        report = LookaheadReport(total_bars=100, uncheckable=('table',))
        got = _gauntlet_lookahead_coverage({'a': report})
        assert 'no 1-D array' in got['vacuous'] and "state['table']" in got['vacuous']


# ---------------------------------------------------------------------------
# S2 / S3 through the gauntlet
# ---------------------------------------------------------------------------

class TestRealStrategies:
    def test_orb_is_exercised_and_passes(self):
        s2 = _gauntlet_check_lookahead(OpeningRangeBreakout)
        assert s2['status'] == 'pass', s2
        assert {'orb_high', 'orb_low', 'rth'} <= set(s2['exercised_keys'])
        assert s2['unexercised_keys'] == []
        assert s2['session'] == {'tz': NY, 'open_minute': 570, 'close_minute': 960}

        s3 = _gauntlet_run_battery(OpeningRangeBreakout)
        assert s3['status'] == 'pass', s3
        for name in ('session_trending', 'session_choppy', 'session_extended', 'session_halts'):
            assert s3['frames'][name]['signals'] > 0, name
        # Every signal came from a session frame: the originals are still idle.
        assert sum(s3['frames'][name]['signals'] for name in LEGACY_FRAMES) == 0

    def test_orb_signals_both_ways(self):
        frame = sm.session_trending()
        strategy = OpeningRangeBreakout()
        state = strategy.precompute(frame)
        actions = {s.action for s in (strategy.on_bar(frame, state, i) for i in range(len(frame)))
                   if s is not None}
        assert actions == {Action.BUY, Action.SELL}

    def test_vwap_reclaim_is_exercised(self):
        s3 = _gauntlet_run_battery(VwapReclaim)
        assert s3['status'] == 'pass', s3
        assert s3['frames']['session_trending']['signals'] > 0
        assert s3['frames']['session_choppy']['signals'] > 0
        assert sum(s3['frames'][name]['signals'] for name in LEGACY_FRAMES) == 0

        frame = sm.session_choppy()
        report = lookahead_report(
            VwapReclaim(), frame,
            cut_points=interior_cut_points(frame.index, session_tz=NY,
                                           open_minute=570, close_minute=960))
        assert {'vwap', 'day_low', 'open_vwap', 'entry_window'} <= set(report.exercised)

    def test_leaky_orb_fails_s2(self):
        s2 = _gauntlet_check_lookahead(LeakyORB)
        assert s2['status'] == 'fail'
        assert s2['frame'].startswith('session_')
        assert 'Lookahead' in s2['detail'] and "state['orb_" in s2['detail']

    def test_leaky_orb_fails_the_gauntlet_and_cannot_be_armed(self, tmp_path, tmp_duckdb_path,
                                                              monkeypatch):
        source = (REPO / 'strategies' / 'opening_range_breakout.py').read_text() + '''

class LeakyORB(OpeningRangeBreakout):
    def precompute(self, prices):
        state = super().precompute(prices)
        if not state:
            return state
        local = prices.index.tz_convert('America/New_York')
        day = pd.Series(local.date, index=prices.index)
        rth = pd.Series(state['rth'], index=prices.index)
        state['orb_high'] = prices['high'].where(rth).groupby(day).transform('max').to_numpy()
        state['orb_low'] = prices['low'].where(rth).groupby(day).transform('min').to_numpy()
        return state
'''
        module = _write(tmp_path / 'strategies', 'leaky_orb.py', source)
        import trader.mmr_cli as cli
        monkeypatch.setattr(cli, '_gauntlet_db_path', lambda: tmp_duckdb_path)

        _handle_strategies_gauntlet(argparse.Namespace(
            module_path=str(module), class_name='LeakyORB', min_psr=None, name=None))
        store = GauntletStore(tmp_duckdb_path)
        code_hash = compute_strategy_hash(str(module))
        record = store.latest_for_hash(code_hash, 'LeakyORB')
        assert record.verdict == 'FAIL'
        assert record.checks['s2_lookahead']['status'] == 'fail'
        assert store.has_pass(code_hash, 'LeakyORB') is False

        # The honest class in the same file passes on the same frames.
        result = _gauntlet_run(module, 'OpeningRangeBreakout', tmp_duckdb_path)
        assert result['verdict'] == 'PASS', result['checks']
        assert result['checks']['s3_battery']['signals'] > 0

    def test_real_orb_file_passes_end_to_end(self, tmp_duckdb_path):
        result = _gauntlet_run(REPO / 'strategies' / 'opening_range_breakout.py',
                               'OpeningRangeBreakout', tmp_duckdb_path, bar_size='1 min')
        assert result['verdict'] == 'PASS', result['checks']
        assert result['checks']['s2_lookahead']['status'] == 'pass'
        assert 'daily_trending' not in result['checks']['s3_battery']['frames']


class TestExtendedHoursFrame:
    """A session value broadcast back over that day's pre-market bars is a
    future value at those bars. It only shows on a frame that HAS pre-market
    bars, which is why one is in the set."""

    class OpenBroadcast(Strategy):
        def precompute(self, prices):
            local = prices.index.tz_convert(NY)
            minute = (local.hour * 60 + local.minute).to_numpy()
            rth = pd.Series((minute >= 570) & (minute < 960), index=prices.index)
            day = pd.Series(local.date, index=prices.index)
            first = prices['close'].where(rth).groupby(day).transform('first')
            return {'session_open': first.to_numpy()}

    def test_clean_when_the_frame_starts_at_the_open(self):
        assert_no_lookahead(self.OpenBroadcast(), sm.session_trending())

    def test_flagged_on_the_extended_frame(self):
        s2 = _gauntlet_check_lookahead(self.OpenBroadcast)
        assert s2['status'] == 'fail' and s2['frame'] == 'session_extended'
        assert "state['session_open']" in s2['detail']


# -- vacuity -----------------------------------------------------------------

_HIDDEN_SESSION_BODY = '''
    def precompute(self, prices):
        idx = prices.index.tz_convert(self.ZONE)
        minute = (idx.hour * 60 + idx.minute).to_numpy()
        live = pd.Series((minute >= self.OPEN) & (minute < self.CLOSE), index=prices.index)
        level = prices['high'].where(live).rolling(20, min_periods=20).max().shift(1)
        return {'close': prices['close'].to_numpy(), 'level': level.to_numpy()}

    def on_bar(self, prices, state, index):
        level = state['level'][index]
        if level != level:
            return None
        if state['close'][index] > level:
            return Signal(source_name=self.name or 'h', action=Action.BUY,
                          probability=0.6, risk=0.4)
        return None
'''

_HEADER = '''
import numpy as np
import pandas as pd
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action
'''

# Trades 01:00-03:00 UTC and tells nobody: no frame the gauntlet builds has a
# bar in that window.
HIDDEN_SESSION = _HEADER + '''
class HiddenSession(Strategy):
    ZONE, OPEN, CLOSE = 'UTC', 60, 180
''' + _HIDDEN_SESSION_BODY

# The same strategy, declaring its session the way ORB does.
DECLARED_SESSION = _HEADER + '''
class DeclaredSession(Strategy):
    SESSION_TZ = 'UTC'
    RTH_OPEN_MIN = 60
    RTH_CLOSE_MIN = 180
    ZONE, OPEN, CLOSE = SESSION_TZ, RTH_OPEN_MIN, RTH_CLOSE_MIN
''' + _HIDDEN_SESSION_BODY

SILENT = _HEADER + '''
class Silent(Strategy):
    def on_prices(self, prices):
        return None
'''

EMPTY_STATE = _HEADER + '''
class EmptyState(Strategy):
    def precompute(self, prices):
        return {}

    def on_bar(self, prices, state, index):
        if index % 50 == 49:
            return Signal(source_name=self.name or 'e', action=Action.BUY,
                          probability=0.6, risk=0.4)
        return None
'''

TABLE_STATE = _HEADER + '''
class TableState(Strategy):
    def precompute(self, prices):
        return {'indicators': pd.DataFrame({'sma': prices['close'].rolling(5).mean()})}

    def on_bar(self, prices, state, index):
        if index % 50 == 49:
            return Signal(source_name=self.name or 't', action=Action.SELL,
                          probability=0.6, risk=0.4)
        return None
'''

CONSTANT_KEY = _HEADER + '''
class ConstantKey(Strategy):
    def precompute(self, prices):
        return {'ema': prices['close'].ewm(span=10, adjust=False).mean().to_numpy(),
                'halted': np.zeros(len(prices), dtype=bool)}

    def on_bar(self, prices, state, index):
        close = float(prices['close'].iloc[index])
        if index > 10 and close == close and close > state['ema'][index] * 1.001:
            return Signal(source_name=self.name or 'c', action=Action.BUY,
                          probability=0.6, risk=0.4)
        return None
'''

# One signal in the whole battery, on a bar the 40-point sample does not land on.
ONE_SHOT = _HEADER + '''
class OneShot(Strategy):
    def on_prices(self, prices):
        ts = prices.index[-1].tz_convert('America/New_York')
        if (ts.day, ts.hour, ts.minute) == (5, 10, 7):
            return Signal(source_name=self.name or 'o', action=Action.BUY,
                          probability=0.6, risk=0.4)
        return None
'''

# Acts on daily bars only, and needs 200 of them.
DAILY_ONLY = _HEADER + '''
class DailyOnly(Strategy):
    def precompute(self, prices):
        idx = prices.index
        if len(idx) < 2 or (idx[1] - idx[0]) < pd.Timedelta(days=1):
            return {}
        close = prices['close']
        return {'fast': close.rolling(20).mean().to_numpy(),
                'slow': close.rolling(200).mean().to_numpy()}

    def on_bar(self, prices, state, index):
        if not state or index < 1:
            return None
        fast, slow = state['fast'], state['slow']
        if np.isnan(slow[index - 1]):
            return None
        if fast[index - 1] <= slow[index - 1] and fast[index] > slow[index]:
            return Signal(source_name=self.name or 'd', action=Action.BUY,
                          probability=0.6, risk=0.4)
        if fast[index - 1] >= slow[index - 1] and fast[index] < slow[index]:
            return Signal(source_name=self.name or 'd', action=Action.SELL,
                          probability=0.6, risk=0.4)
        return None
'''


def _run(tmp_path, tmp_duckdb_path, body, class_name, **kw):
    module = _write(tmp_path / 'strategies', f'{class_name.lower()}.py', body)
    return _gauntlet_run(module, class_name, tmp_duckdb_path, **kw)


class TestVacuityDoesNotPass:
    def test_undeclared_session_fails_both_stages(self, tmp_path, tmp_duckdb_path):
        result = _run(tmp_path, tmp_duckdb_path, HIDDEN_SESSION, 'HiddenSession')
        s2, s3 = result['checks']['s2_lookahead'], result['checks']['s3_battery']
        assert s2['status'] == 'fail' and s2['vacuous'] is True
        assert "state['level'] all-NaN on every frame" in s2['detail']
        assert 'SESSION_TZ' in s2['detail']
        assert s2['unexercised_keys'] == ['level'] and s2['exercised_keys'] == ['close']
        assert s3['status'] == 'fail' and s3['vacuous'] is True and s3['signals'] == 0
        assert 'no signal on any of the' in s3['detail']
        assert result['verdict'] == 'FAIL'

    def test_declaring_the_session_makes_the_same_strategy_evaluable(self, tmp_path,
                                                                     tmp_duckdb_path):
        result = _run(tmp_path, tmp_duckdb_path, DECLARED_SESSION, 'DeclaredSession')
        s2, s3 = result['checks']['s2_lookahead'], result['checks']['s3_battery']
        assert s2['session'] == {'tz': 'UTC', 'open_minute': 60, 'close_minute': 180}
        assert s2['status'] == 'pass' and 'level' in s2['exercised_keys']
        assert s3['status'] == 'pass' and s3['signals'] > 0
        assert result['verdict'] == 'PASS'

    def test_a_strategy_that_never_signals_fails_s3(self, tmp_path, tmp_duckdb_path):
        result = _run(tmp_path, tmp_duckdb_path, SILENT, 'Silent')
        s3 = result['checks']['s3_battery']
        assert s3['status'] == 'fail' and s3['vacuous'] is True
        # It was walked bar by bar before being called silent.
        assert all(r.get('every_bar') for r in s3['frames'].values())
        # on_prices-only: S2 stays not_evaluable (nothing precomputed to leak).
        assert result['checks']['s2_lookahead']['status'] == 'not_evaluable'
        assert result['verdict'] == 'FAIL'

    def test_a_silent_strategy_gets_no_pass_record(self, tmp_path, tmp_duckdb_path, monkeypatch):
        import trader.mmr_cli as cli
        monkeypatch.setattr(cli, '_gauntlet_db_path', lambda: tmp_duckdb_path)
        module = _write(tmp_path / 'strategies', 'silent.py', SILENT)
        _handle_strategies_gauntlet(argparse.Namespace(
            module_path=str(module), class_name='Silent', min_psr=None, name=None))
        assert GauntletStore(tmp_duckdb_path).has_pass(compute_strategy_hash(str(module))) is False

    def test_empty_precompute_state_fails_s2(self, tmp_path, tmp_duckdb_path):
        result = _run(tmp_path, tmp_duckdb_path, EMPTY_STATE, 'EmptyState')
        s2 = result['checks']['s2_lookahead']
        assert s2['status'] == 'fail' and s2['vacuous'] is True
        assert 'empty state on every frame' in s2['detail']
        assert result['checks']['s3_battery']['status'] == 'pass'
        assert result['verdict'] == 'FAIL'

    def test_state_with_nothing_comparable_fails_s2(self, tmp_path, tmp_duckdb_path):
        result = _run(tmp_path, tmp_duckdb_path, TABLE_STATE, 'TableState')
        s2 = result['checks']['s2_lookahead']
        assert s2['status'] == 'fail' and s2['vacuous'] is True
        assert 'no 1-D array' in s2['detail']
        assert result['verdict'] == 'FAIL'

    def test_a_constant_array_is_noted_but_passes(self, tmp_path, tmp_duckdb_path):
        result = _run(tmp_path, tmp_duckdb_path, CONSTANT_KEY, 'ConstantKey')
        s2 = result['checks']['s2_lookahead']
        assert s2['status'] == 'pass'
        assert s2['constant_keys'] == ['halted'] and 'halted' in s2['note']
        assert result['verdict'] == 'PASS'


class TestQuietStrategiesStillEvaluable:
    def test_one_signal_in_the_whole_battery_is_enough(self, tmp_path, tmp_duckdb_path):
        result = _run(tmp_path, tmp_duckdb_path, ONE_SHOT, 'OneShot')
        s3 = result['checks']['s3_battery']
        assert s3['status'] == 'pass' and s3['signals'] == 1
        # The 40-point sample missed it; the bar-by-bar pass (shortest frame
        # first) found it and stopped there, leaving the longest unwalked.
        walked = [name for name, r in s3['frames'].items() if r.get('every_bar')]
        assert sum(s3['frames'][name]['signals'] for name in walked) == 1
        assert 'session_halts' in walked and 'session_trending' not in walked
        assert result['verdict'] == 'PASS'

    def test_daily_strategy_passes_on_the_daily_frames(self, tmp_path, tmp_duckdb_path):
        for bar_size in (None, '1 day'):
            result = _run(tmp_path, tmp_duckdb_path, DAILY_ONLY, 'DailyOnly', bar_size=bar_size)
            s2, s3 = result['checks']['s2_lookahead'], result['checks']['s3_battery']
            assert s2['status'] == 'pass' and set(s2['exercised_keys']) == {'fast', 'slow'}
            assert s3['status'] == 'pass' and s3['frames']['daily_trending']['signals'] > 0
            assert result['verdict'] == 'PASS', bar_size

    def test_daily_strategy_on_intraday_frames_is_vacuous_not_passed(self, tmp_path,
                                                                     tmp_duckdb_path):
        result = _run(tmp_path, tmp_duckdb_path, DAILY_ONLY, 'DailyOnly', bar_size='1 min')
        assert result['checks']['s2_lookahead']['vacuous'] is True
        assert result['checks']['s3_battery']['vacuous'] is True
        assert result['verdict'] == 'FAIL'

    def test_orb_on_daily_frames_is_vacuous_not_passed(self):
        s2 = _gauntlet_check_lookahead(OpeningRangeBreakout, '1 day')
        assert s2['status'] == 'fail' and s2['vacuous'] is True
        # every range-derived array is NaN on daily bars (exit_low was added with the
        # partial-session guard, B3.1)
        assert set(s2['unexercised_keys']) == {'orb_high', 'orb_low', 'exit_low'}
        assert '--bar-size' in s2['detail']


# -- frame selection ----------------------------------------------------------

class TestFrameSelection:
    SESSION = {'tz': NY, 'open_minute': 570, 'close_minute': 960}

    def test_families_by_bar_size(self):
        intraday = {'session_trending', 'session_choppy', 'session_extended', 'session_halts'}
        daily = {'daily_trending', 'daily_choppy'}
        assert set(_gauntlet_frames(self.SESSION, None, True)) == LEGACY_FRAMES | intraday | daily
        assert set(_gauntlet_frames(self.SESSION, '5 mins', True)) == LEGACY_FRAMES | intraday
        assert set(_gauntlet_frames(self.SESSION, '1 week', True)) == LEGACY_FRAMES | daily
        # S2 uses the clean frames only.
        assert set(_gauntlet_frames(self.SESSION, None, False)) == (
            {'trending', 'choppy'} | (intraday - {'session_halts'}) | daily)

    def test_unknown_bar_size_is_refused(self, tmp_path, tmp_duckdb_path):
        module = _write(tmp_path / 'strategies', 'silent.py', SILENT)
        with pytest.raises(ValueError, match='unknown bar size'):
            _gauntlet_run(module, 'Silent', tmp_duckdb_path, bar_size='1 fortnight')

    def test_session_spec_defaults_and_declarations(self):
        class Plain(Strategy):
            pass

        class Asx(Strategy):
            SESSION_TZ = 'Australia/Sydney'
            RTH_OPEN_MIN = 600
            RTH_CLOSE_MIN = 960

        class OpenOnly(Strategy):          # e.g. VwapReclaim declares only the open
            RTH_OPEN_MIN = 600

        class Broken(Strategy):
            SESSION_TZ = 'Mars/Olympus_Mons'
            RTH_OPEN_MIN = 960
            RTH_CLOSE_MIN = 570

        default = {'tz': NY, 'open_minute': 570, 'close_minute': 960}
        assert _gauntlet_session_spec(Plain) == default
        assert _gauntlet_session_spec(VwapReclaim) == default
        assert _gauntlet_session_spec(Asx) == {
            'tz': 'Australia/Sydney', 'open_minute': 600, 'close_minute': 960}
        assert _gauntlet_session_spec(OpenOnly) == {
            'tz': NY, 'open_minute': 600, 'close_minute': 960}
        assert _gauntlet_session_spec(Broken) == default

    def test_frames_are_stamped_with_the_declared_session(self):
        session = {'tz': 'Australia/Sydney', 'open_minute': 600, 'close_minute': 960}
        frames = _gauntlet_frames(session, '1 min', True)
        minutes = _local_minutes(frames['session_choppy'], 'Australia/Sydney')
        assert minutes.min() == 600 and minutes.max() == 959
