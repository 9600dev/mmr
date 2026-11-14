"""Regressions for the backtest / evaluation findings of the 2026-11-14 audit
(docs/AUDIT_2026-11-14.md, Part B4).

Each class pins one defect in the form that failed before its fix:

* B4.1  walk-forward reported each test run's WARM-UP as out-of-sample
* B4.2  ``close_by_time`` was compared in New York time for every venue
* B4.4  Sharpe / Sortino annualised with 98,280 on extended-hours curves
* B4.7c the panel path computed a different Sortino from ``run``
* B4.6  a float override on an int-typed parameter was truncated silently
* B4.7a ``bt-sweep --live-semantics`` rows carried no ``_execution_mode``

Everything here runs on in-memory frames or temp DuckDB files.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import trader.mmr_cli as cli
from trader.data.backtest_store import BacktestStore
from trader.data.data_access import TickStorage
from trader.data.duckdb_store import DuckDBDataStore
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation import synthetic_markets
from trader.simulation.backtester import BacktestConfig, Backtester
from trader.simulation.walk_forward import (
    plan_fold_offsets, resolve_folds, run_walk_forward,
)
from trader.strategy import strategy_runtime
from trader.strategy.parameters import (
    _coerce_param, apply_param_overrides, session_bar_ts,
)
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState

NY = 'America/New_York'
SYDNEY = 'Australia/Sydney'


# --------------------------------------------------------------------------
# Shared fixtures
# --------------------------------------------------------------------------

class _Tick:
    def __init__(self, frames):
        self._frames = frames

    def read(self, conid, date_range=None):
        return self._frames[int(conid)].copy()


class _Store:
    """In-memory ``TickStorage`` stand-in: hands back the frame as given."""

    def __init__(self, frames):
        self._frames = frames

    def get_tickdata(self, bar_size):
        return _Tick(self._frames)


def _install(strategy, conids, bar_size=BarSize.Days1, params=None):
    ctx = StrategyContext(
        name=type(strategy).__name__, bar_size=bar_size, conids=conids,
        universe=None, historical_days_prior=0, paper_only=False, storage=None,
        universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
        logger=logging.getLogger('audit-b4'),
    )
    strategy.install(ctx)
    strategy.state = StrategyState.RUNNING
    if params:
        apply_param_overrides(strategy, params)
    return strategy


def _daily(closes, start='2024-01-02', tz=NY):
    closes = np.asarray(closes, dtype=float)
    idx = pd.bdate_range(start, periods=len(closes), tz=tz)
    opens = np.r_[closes[0], closes[:-1]]
    return pd.DataFrame({
        'open': opens, 'high': np.maximum(opens, closes),
        'low': np.minimum(opens, closes), 'close': closes, 'volume': 1e6,
    }, index=idx).rename_axis('date')


def _cfg(**kw):
    base = dict(start_date=dt.datetime(2020, 1, 1), end_date=dt.datetime(2030, 1, 1),
                bar_size=BarSize.Days1, slippage_bps=0.0, commission_per_share=0.0,
                execution_mode='live', trade_notional=50_000.0, cooldown_seconds=0)
    base.update(kw)
    return BacktestConfig(**base)


class _Scripted(Strategy):
    """BUY / SELL on the bar indexes it is told to (fills land one bar later)."""

    buys: tuple = ()
    sells: tuple = ()

    def on_bar(self, prices, state, index):
        if index in self.buys:
            return Signal(source_name='scripted', action=Action.BUY, probability=0.9, risk=0.1)
        if index in self.sells:
            return Signal(source_name='scripted', action=Action.SELL, probability=0.9, risk=0.1)
        return None


def _scripted(buys, sells=()):
    s = _Scripted()
    s.buys, s.sells = tuple(buys), tuple(sells)
    return _install(s, [1])


# --------------------------------------------------------------------------
# B4.1 — measure the window, not the run
# --------------------------------------------------------------------------

class TestMeasurementWindow:
    """``BacktestConfig.measure_from`` / ``measure_to``: the run is unchanged
    (indicators and the book are warm at the window start), but every reported
    number describes ``[measure_from, measure_to)`` only."""

    CLOSES = 100.0 * 1.01 ** np.arange(120)      # +1% a bar, every bar

    def _run(self, strategy, **kw):
        frames = {1: _daily(self.CLOSES)}
        return Backtester(_Store(frames), _cfg(**kw)).run(strategy, [1]), frames[1].index

    def test_return_is_the_windows_own_and_starts_from_the_last_warmup_mark(self):
        full, idx = self._run(_scripted(buys=[5]))
        lo = idx[60]
        windowed, _ = self._run(_scripted(buys=[5]), measure_from=lo.to_pydatetime())

        eq = full.equity_curve
        honest = eq.iloc[-1] / eq[eq.index < lo].iloc[-1] - 1
        assert windowed.total_return == pytest.approx(honest, rel=1e-12)
        # The whole-run figure — what walk-forward used to report — is not it.
        assert full.total_return > windowed.total_return * 1.5
        # The curve is the window's slice of the same run, not a different run.
        assert windowed.equity_curve.index[0] == lo
        pd.testing.assert_series_equal(windowed.equity_curve, eq[eq.index >= lo])

    def test_a_fill_before_the_window_is_executed_but_never_counted(self):
        idx = _daily(self.CLOSES).index
        windowed, _ = self._run(_scripted(buys=[5]),
                                measure_from=idx[60].to_pydatetime())
        assert windowed.trades == [] and windowed.total_trades == 0
        assert windowed.signals == []
        d = windowed.diagnostics
        assert d['warmup_fills'] == 1 and d['warmup_bars'] == 60
        # ... and the position it opened is why the window has a return at all.
        assert d['window_bars_in_market'] == d['window_bars'] == 60
        assert windowed.time_in_market_pct == 1.0

    def test_a_round_trip_opened_in_warmup_is_not_a_window_trade(self):
        """Opened on bars the window does not judge => not one of its trades,
        however it ended. Its mark-to-market inside the window stays in the
        equity curve."""
        # Round trip A: bought in warm-up (fills bar 6), sold inside (bar 71) — a WINNER.
        # Round trip B: bought inside (bar 81) and sold inside (bar 91)     — a LOSER.
        closes = self.CLOSES.copy()
        closes[80:] = closes[79] * 0.99 ** np.arange(1, 41)
        frames = {1: _daily(closes)}

        def run(**kw):
            return Backtester(_Store(frames), _cfg(**kw)).run(
                _scripted(buys=[5, 80], sells=[70, 90]), [1])

        full = run()
        assert (full.win_rate, full.total_trades) == (0.5, 4)

        windowed = run(measure_from=frames[1].index[60].to_pydatetime())
        assert [str(t.action) for t in windowed.trades] == ['SELL', 'BUY', 'SELL']
        assert windowed.win_rate == 0.0            # only B counts, and B lost
        assert windowed.profit_factor == 0.0
        assert windowed.expectancy_bps < 0

    def test_a_window_holding_the_whole_run_changes_nothing(self):
        full, idx = self._run(_scripted(buys=[5], sells=[40]))
        windowed, _ = self._run(
            _scripted(buys=[5], sells=[40]),
            measure_from=(idx[0] - pd.Timedelta(days=30)).to_pydatetime(),
            measure_to=(idx[-1] + pd.Timedelta(days=30)).to_pydatetime())
        for field in ('total_return', 'sharpe_ratio', 'sortino_ratio', 'max_drawdown',
                      'win_rate', 'profit_factor', 'expectancy_bps', 'calmar_ratio',
                      'time_in_market_pct', 'total_trades'):
            assert getattr(windowed, field) == getattr(full, field), field
        pd.testing.assert_series_equal(windowed.equity_curve, full.equity_curve)

    def test_consecutive_windows_partition_the_run(self):
        """Half-open: the boundary bar belongs to the LATER window only, so the
        windows' returns compound to exactly the run's and no bar is judged
        twice. An inclusive end double-counted one daily bar per fold."""
        full, idx = self._run(_scripted(buys=[5]))
        a, b, c = idx[30], idx[70], idx[110]
        first, _ = self._run(_scripted(buys=[5]), measure_from=a.to_pydatetime(),
                             measure_to=b.to_pydatetime())
        second, _ = self._run(_scripted(buys=[5]), measure_from=b.to_pydatetime(),
                              measure_to=c.to_pydatetime())

        assert not set(first.equity_curve.index) & set(second.equity_curve.index)
        assert first.equity_curve.index[-1] < b == second.equity_curve.index[0]
        eq = full.equity_curve
        whole = eq[eq.index < c].iloc[-1] / eq[eq.index < a].iloc[-1] - 1
        assert (1 + first.total_return) * (1 + second.total_return) - 1 == pytest.approx(
            whole, rel=1e-12)

    def test_drawdown_counts_a_fall_on_the_windows_first_bar(self):
        closes = self.CLOSES.copy()
        closes[60] = closes[59] * 0.90             # the window's first bar gaps down 10%
        closes[61:] = closes[60]
        frames = {1: _daily(closes)}
        windowed = Backtester(
            _Store(frames),
            _cfg(measure_from=frames[1].index[60].to_pydatetime()),
        ).run(_scripted(buys=[5]), [1])
        assert windowed.max_drawdown < -0.03       # measured from the last warm-up mark
        assert windowed.total_return == pytest.approx(windowed.max_drawdown, rel=1e-9)

    def test_a_naive_bound_is_read_on_the_frames_own_clock(self):
        """Stored frames are tz-aware; fold boundaries are plain dates."""
        full, idx = self._run(_scripted(buys=[5]))
        windowed, _ = self._run(_scripted(buys=[5]),
                                measure_from=dt.datetime.combine(idx[60].date(), dt.time.min))
        assert windowed.equity_curve.index[0] == idx[60]


class _HoldTwo(Strategy):
    """Panel book: half in each of two names from bar 2, then hold."""

    def on_panel(self, panel, state, index):
        return {1: 0.5, 2: 0.5} if index == 2 else None


class _RebalanceInside(Strategy):
    """Panel book that opens in warm-up, then rotates fully INSIDE the window."""

    def on_panel(self, panel, state, index):
        if index == 2:
            return {1: 1.0, 2: 0.0}
        if index == 30:
            return {1: 0.0, 2: 1.0}
        if index == 40:
            return {1: 0.0, 2: 0.0}
        return None


class TestMeasurementWindowPanel:
    """The cross-sectional path (``run_panel`` -> ``_summarise``) honours the
    same window — it is the path the audit's reproduction ran on."""

    def _frames(self):
        up = 100.0 * 1.01 ** np.arange(60)
        flat = np.full(60, 50.0)
        return {1: _daily(up), 2: _daily(flat), 3: _daily(up[::-1])}

    def _run(self, strategy, **kw):
        frames = self._frames()
        kw.setdefault('execution_mode', 'accumulate')
        bt = Backtester(_Store(frames), _cfg(**kw))
        return bt.run_panel(_install(strategy, [1, 2, 3]), [1, 2, 3]), frames[1].index

    def test_panel_return_is_the_windows_own(self):
        full, idx = self._run(_HoldTwo())
        lo = idx[20]
        windowed, _ = self._run(_HoldTwo(), measure_from=lo.to_pydatetime())

        eq = full.equity_curve
        honest = eq.iloc[-1] / eq[eq.index < lo].iloc[-1] - 1
        assert windowed.total_return == pytest.approx(honest, rel=1e-12)
        assert full.total_return > windowed.total_return * 1.2
        pd.testing.assert_series_equal(windowed.equity_curve, eq[eq.index >= lo])
        assert windowed.trades == [] and windowed.total_trades == 0
        assert windowed.diagnostics['warmup_fills'] == 2
        assert windowed.diagnostics['window_bars_in_market'] == 40

    def test_panel_lot_opened_in_warmup_is_not_a_window_trade(self):
        full, idx = self._run(_RebalanceInside())
        assert full.total_trades == 2              # lot in 1 (won), lot in 2 (flat)
        windowed, _ = self._run(_RebalanceInside(), measure_from=idx[20].to_pydatetime())
        # Name 1 was bought on bar 3 (warm-up) and sold on bar 31: not counted.
        # Name 2 was bought on bar 31 and sold on bar 41: the window's one lot.
        assert windowed.total_trades == 1
        assert {int(t.conid) for t in windowed.trades} == {1, 2}
        assert all(t.timestamp >= idx[20] for t in windowed.trades)


class TestWalkForwardMeasuresTheWindow:
    def test_the_runner_is_told_where_measurement_starts(self):
        """Data start and window start are different dates whenever there is
        warm-up; a runner handed only the first reports the warm-up."""
        folds = resolve_folds(dt.date(2024, 1, 1), plan_fold_offsets(200, 100, 50),
                              warmup_days=30)
        calls = []

        def run(cell, start, end, measure_start):
            calls.append((cell['A'], start, end, measure_start))
            return {'score': float(cell['A'])}

        results = run_walk_forward(folds, [{'A': 1}, {'A': 2}], run,
                                   lambda m: m['score'], pass_measure_start=True)

        assert [r.chosen for r in results] == [{'A': 2}, {'A': 2}]
        expected = []
        for f in folds:
            expected += [(1, f.train_data_start, f.train_end, f.train_start),
                         (2, f.train_data_start, f.train_end, f.train_start),
                         (2, f.test_data_start, f.test_end, f.test_start)]
        assert calls == expected
        for _, start, end, measure_start in calls:
            assert start == measure_start - dt.timedelta(days=30) < measure_start < end

    def test_three_argument_runners_are_unaffected(self):
        folds = resolve_folds(dt.date(2024, 1, 1), plan_fold_offsets(200, 100, 50))
        calls = []

        def run(cell, start, end):
            calls.append((start, end))
            return {'score': 1.0}

        run_walk_forward(folds, [{'A': 1}], run, lambda m: m['score'])
        assert calls == [x for f in folds for x in
                         ((f.train_data_start, f.train_end), (f.test_data_start, f.test_end))]

    def test_a_fold_that_only_held_a_carried_position_is_still_a_tested_fold(self):
        """Once only the window is measured, 'no fills' no longer means 'not
        tested': a book opened in warm-up and held has a return in the window
        and no fills there. Dropping it would shorten the compounded result
        without saying so."""
        assert cli._walk_forward_evaluated({'trades': 0, 'exposed': True})
        assert cli._walk_forward_evaluated({'trades': 3, 'exposed': False})
        assert not cli._walk_forward_evaluated({'trades': 0, 'exposed': False})
        assert not cli._walk_forward_evaluated({})
        assert not cli._walk_forward_evaluated(None)


def _json_payload(capsys):
    """The handler's JSON document. Under pytest the Rich console handler also
    writes the backtester's INFO lines to stdout, so take the document line."""
    lines = [line for line in capsys.readouterr().out.splitlines()
             if line.startswith('{"data"')]
    assert len(lines) == 1, lines
    return json.loads(lines[0])['data']


WARM_HOLD = '''
from trader.objects import Action
from trader.trading.strategy import Signal, Strategy


class WarmHold(Strategy):
    """Buys once, LOOKBACK bars into whatever frame it is given, and holds."""
    LOOKBACK = 20

    def on_bar(self, prices, state, index):
        if index == self.LOOKBACK:
            return Signal(source_name='warm_hold', action=Action.BUY,
                          probability=0.9, risk=0.1)
        return None
'''


class TestWalkForwardCli:
    """End to end through ``mmr walk-forward`` (the reviewer's reproduction,
    /tmp/mmr-review-sim/t_wf.py, on one rising instrument).

    The strategy buys during every run's warm-up — the grid key contains
    ``LOOKBACK``, so each run starts 39 days before its window — and holds.
    Before the fix every fold reported its whole run, warm-up included.
    """

    NOW = dt.datetime(2026, 6, 30, 12, 0)
    DAYS, TRAIN, TEST = 420, 180, 60
    CONID = 4391
    GROWTH = 1.002

    @pytest.fixture
    def stack(self, tmp_path, monkeypatch):
        history = str(tmp_path / 'history.duckdb')
        main = str(tmp_path / 'main.duckdb')
        idx = pd.bdate_range(end=self.NOW.date(), periods=400, tz=NY)
        close = 100.0 * self.GROWTH ** np.arange(len(idx))
        open_ = np.r_[close[0] / self.GROWTH, close[:-1]]
        bars = pd.DataFrame({
            'open': open_, 'high': close, 'low': open_, 'close': close,
            'volume': 1e6, 'average': close, 'bar_count': 100,
        }, index=idx.tz_convert('UTC')).rename_axis('date')
        DuckDBDataStore(history).write(str(self.CONID), bars)

        strategy = tmp_path / 'warm_hold.py'
        strategy.write_text(WARM_HOLD)

        from trader.container import Container
        monkeypatch.setattr(Container, 'instance', classmethod(
            lambda cls: SimpleNamespace(config=lambda: {
                'duckdb_path': main, 'history_duckdb_path': history})))
        monkeypatch.setattr(cli, '_json_mode', True)
        return SimpleNamespace(history=history, strategy=str(strategy),
                               close=pd.Series(close, index=idx))

    def _args(self, stack, grid):
        return argparse.Namespace(
            strategy=stack.strategy, class_name='WarmHold', conids=[self.CONID],
            grid=json.dumps(grid), days=self.DAYS, train_days=self.TRAIN,
            test_days=self.TEST, anchored=False, metric='total_return',
            capital=100_000.0, bar_size='1 day', panel=False, no_trade_band=0.0,
            borrow_bps=0.0, live_semantics=True, trade_notional=90_000.0,
            slippage_bps=0.0)

    def _folds(self, warmup):
        start = (self.NOW - dt.timedelta(days=self.DAYS)).date()
        return resolve_folds(start, plan_fold_offsets(self.DAYS, self.TRAIN, self.TEST),
                             warmup_days=warmup)

    def _whole_run(self, stack, fold, cell):
        """The test run exactly as the handler configures it, unwindowed."""
        config = BacktestConfig(
            start_date=dt.datetime.combine(fold.test_data_start, dt.time.min),
            end_date=dt.datetime.combine(fold.test_end, dt.time.min),
            initial_capital=100_000.0, bar_size=BarSize.Days1, slippage_bps=0.0,
            execution_mode='live', trade_notional=90_000.0)
        return Backtester(TickStorage(stack.history), config).run_from_module(
            stack.strategy, 'WarmHold', [self.CONID], params=dict(cell))

    def test_each_fold_reports_its_test_window_only(self, stack, capsys):
        cli._handle_walk_forward(self._args(stack, {'LOOKBACK': [10, 20]}), now=self.NOW)
        payload = _json_payload(capsys)

        warmup = int(20 * 1.45) + 10
        assert payload['warmup_days'] == warmup == 39
        folds = self._folds(warmup)
        assert payload['folds'] == len(folds) == 4
        assert payload['oos_folds_tested'] == 4       # held, not traded: still tested

        compounded = 1.0
        for row, fold in zip(payload['per_fold'], folds):
            whole = self._whole_run(stack, fold, row['chosen'])
            eq = whole.equity_curve
            lo = pd.Timestamp(fold.test_start, tz=eq.index.tz)
            hi = pd.Timestamp(fold.test_end, tz=eq.index.tz)
            inside, before = eq[(eq.index >= lo) & (eq.index < hi)], eq[eq.index < lo]
            honest = inside.iloc[-1] / before.iloc[-1] - 1

            assert row['total_return'] == pytest.approx(honest, rel=1e-9)
            # What used to be reported: the run from 39 days before the window.
            assert whole.total_return > honest * 1.1
            assert row['total_return'] < whole.total_return
            # The warm-up BUY is executed and disclosed, never counted.
            assert (row['trades'], row['warmup_fills'], row['exposed']) == (0, 1, True)
            compounded *= 1 + honest
        assert payload['oos_compounded_return'] == pytest.approx(compounded - 1, rel=1e-9)

    def test_a_long_only_book_cannot_out_earn_the_asset_over_the_test_span(self, stack, capsys):
        """Needs no second backtest to state: the test windows are disjoint, so
        a book that is at most 90% long a rising asset cannot compound to more
        than the asset made between the first test start and the last test
        end. With warm-up counted, every fold added ~5 weeks it did not own."""
        cli._handle_walk_forward(self._args(stack, {'LOOKBACK': [20]}), now=self.NOW)
        payload = _json_payload(capsys)

        folds = self._folds(39)
        px = stack.close
        first = pd.Timestamp(folds[0].test_start, tz=NY)
        last = pd.Timestamp(folds[-1].test_end, tz=NY)
        asset = px[px.index < last].iloc[-1] / px[px.index < first].iloc[-1] - 1
        assert 0 < payload['oos_compounded_return'] <= asset

    def test_the_report_names_its_execution_semantics(self, stack, capsys):
        cli._handle_walk_forward(self._args(stack, {'LOOKBACK': [20]}), now=self.NOW)
        live = _json_payload(capsys)['execution']
        assert live == {'_execution_mode': 'live', '_trade_notional': 90_000.0}

        args = self._args(stack, {'LOOKBACK': [20]})
        args.live_semantics, args.trade_notional = False, None
        cli._handle_walk_forward(args, now=self.NOW)
        legacy = _json_payload(capsys)['execution']
        assert legacy == {'_execution_mode': 'accumulate'}


# --------------------------------------------------------------------------
# B4.2 — close_by_time is session wall time
# --------------------------------------------------------------------------

def _session_minutes(tz, open_time, close_time, days=3, start='2026-03-02', out_tz=NY):
    """Flat 1-min bars for ``days`` sessions in ``tz``, indexed in ``out_tz`` —
    the store hands every venue's history back New York-indexed."""
    parts = []
    for d in pd.bdate_range(start, periods=days):
        day = d.strftime('%Y-%m-%d')
        parts.append(pd.date_range(f'{day} {open_time}', f'{day} {close_time}',
                                   freq='1min', inclusive='left', tz=tz))
    idx = parts[0].append(parts[1:]).tz_convert(out_tz)
    return pd.DataFrame({'open': 100.0, 'high': 100.0, 'low': 100.0, 'close': 100.0,
                         'volume': 1e4}, index=idx).rename_axis('date')


class _BuyHalfHourIn(Strategy):
    """BUY on the 31st bar of every session, flatten at 15:45 session time."""
    BARS_PER_SESSION = 360

    def on_bar(self, prices, state, index):
        if index % self.BARS_PER_SESSION == 30:
            return Signal(source_name='eod', action=Action.BUY, probability=0.9, risk=0.1,
                          close_by_time=dt.time(15, 45))
        return None


class TestCloseByTimeIsSessionTime:
    def _fills(self, frame, params, bars_per_session):
        strategy = _BuyHalfHourIn()
        strategy.BARS_PER_SESSION = bars_per_session
        _install(strategy, [1], bar_size=BarSize.Mins1, params=params)
        result = Backtester(_Store({1: frame}), _cfg(bar_size=BarSize.Mins1)).run(strategy, [1])
        tz = (params or {}).get('SESSION_TZ', NY)
        return [(str(t.action), pd.Timestamp(t.timestamp).tz_convert(tz).strftime('%H:%M'))
                for t in result.trades]

    def test_asx_position_is_flattened_at_1545_sydney(self):
        """The reviewer's case (t_tz.py). A 10:31 Sydney entry is 18:31 the
        previous evening in New York — already 'after 15:45' on the clock the
        backtester used to read — so the exit fired on the next bar."""
        frame = _session_minutes(SYDNEY, '10:00', '16:00')
        assert str(frame.index.tz) == NY                       # as the store returns it
        fills = self._fills(frame, {'SESSION_TZ': SYDNEY}, 360)
        assert fills == [('BUY', '10:31'), ('SELL', '15:46')] * 3

    def test_us_default_is_unchanged(self):
        frame = _session_minutes(NY, '09:30', '16:00')
        assert self._fills(frame, None, 390) == [('BUY', '10:01'), ('SELL', '15:46')] * 3

    def test_a_utc_indexed_frame_is_still_read_on_the_session_clock(self):
        """Same instants, different index zone => same exits. (The pin fixtures
        feed UTC frames; their time exits used to fire on UTC wall time.)"""
        frame = _session_minutes(NY, '09:30', '16:00', out_tz='UTC')
        assert self._fills(frame, None, 390) == [('BUY', '10:01'), ('SELL', '15:46')] * 3

    def test_a_naive_frame_is_taken_as_session_wall_time(self):
        frame = _session_minutes(NY, '09:30', '16:00')
        frame.index = frame.index.tz_localize(None)
        strategy = _install(_BuyHalfHourIn(), [1], bar_size=BarSize.Mins1)
        strategy.BARS_PER_SESSION = 390
        result = Backtester(_Store({1: frame}), _cfg(bar_size=BarSize.Mins1)).run(strategy, [1])
        assert [pd.Timestamp(t.timestamp).strftime('%H:%M') for t in result.trades] == [
            '10:01', '15:46'] * 3

    def test_live_and_backtest_convert_through_the_same_function(self):
        instant = pd.Timestamp('2026-03-02 15:45', tz=SYDNEY)
        params = {'SESSION_TZ': SYDNEY}
        live = strategy_runtime._session_bar_ts(instant.tz_convert('UTC'), params)
        stored = session_bar_ts(instant.tz_convert(NY), params, naive_is_utc=False)
        assert live == stored == instant
        assert live.time() == stored.time() == dt.time(15, 45)
        # Live frames may be naive UTC; a naive backtest frame is left alone.
        naive = instant.tz_convert('UTC').tz_localize(None)
        assert strategy_runtime._session_bar_ts(naive, params).time() == dt.time(15, 45)
        assert session_bar_ts(naive, params, naive_is_utc=False) is naive


# --------------------------------------------------------------------------
# B4.4 — annualise with the bars the curve actually has
# --------------------------------------------------------------------------

def _with_extended_hours(rth):
    """The same RTH price path with the 04:00-09:29 and 16:00-19:59 minutes
    added as flat bars — what a stored extended-hours series looks like."""
    parts = []
    for _, g in rth.groupby(rth.index.date):
        d0 = g.index[0].normalize()
        full = pd.date_range(d0 + pd.Timedelta(hours=4),
                             d0 + pd.Timedelta(hours=19, minutes=59), freq='1min')
        x = g.reindex(full)
        x['close'] = x['close'].ffill().fillna(g['open'].iloc[0])
        for col in ('open', 'high', 'low'):
            x[col] = x[col].fillna(x['close'])
        x['volume'] = x['volume'].fillna(0.0)
        parts.append(x)
    return pd.concat(parts).rename_axis('date')


class _HoldRth(Strategy):
    """Long from 09:35 to 15:55 New York every day, flat otherwise."""

    def on_bar(self, prices, state, index):
        t = prices.index[index]
        minute = t.hour * 60 + t.minute
        if minute == 575:
            return Signal(source_name='hold', action=Action.BUY, probability=0.6, risk=0.4)
        if minute == 955:
            return Signal(source_name='hold', action=Action.SELL, probability=0.6, risk=0.4)
        return None


class TestAnnualisationFromObservedBars:
    def _idx(self, tz, open_time, close_time, days, start='2026-03-02'):
        return _session_minutes(tz, open_time, close_time, days=days, start=start,
                                out_tz=NY).index

    def test_a_curve_with_390_bars_a_day_keeps_exactly_the_old_factor(self):
        idx = self._idx(NY, '09:30', '16:00', days=10)
        assert Backtester._observed_bars_per_year(idx, BarSize.Mins1) == 98_280.0
        assert Backtester._bars_per_year(BarSize.Mins1) == 98_280.0

    def test_extended_hours_are_counted(self):
        idx = self._idx(NY, '04:00', '20:00', days=10)
        assert Backtester._observed_bars_per_year(idx, BarSize.Mins1) == 252 * 960

    def test_partial_edge_days_and_a_half_day_do_not_move_it(self):
        idx = self._idx(NY, '09:30', '16:00', days=12)
        day = idx.normalize()
        days = day.unique()
        keep = np.ones(len(idx), dtype=bool)
        keep[(day == days[0]) & (np.arange(len(idx)) < 200)] = False         # window opens mid-session
        keep[(day == days[-1]) & (idx.hour >= 11)] = False                   # ... and closes mid-session
        keep[(day == days[5]) & (idx.hour >= 13)] = False                    # a 13:00 half day
        assert Backtester._observed_bars_per_year(idx[keep], BarSize.Mins1) == 98_280.0

    def test_daily_and_coarser_keep_the_nominal_mapping(self):
        daily = pd.bdate_range('2024-01-02', periods=300, tz=NY)
        assert Backtester._observed_bars_per_year(daily, BarSize.Days1) == 252
        weekly = pd.date_range('2024-01-05', periods=80, freq='7D', tz=NY)
        assert Backtester._observed_bars_per_year(weekly, BarSize.Weeks1) == 52

    def test_five_minute_bars_scale_the_same_way(self):
        idx = self._idx(NY, '09:30', '16:00', days=6)[::5]
        assert Backtester._observed_bars_per_year(idx, BarSize.Mins5) == 98_280.0 / 5

    def test_an_asx_session_is_counted_on_its_own_date(self):
        """In New York time a Sydney session straddles midnight for half the
        year (20:00-02:00 EDT), so New York dates hold part-sessions."""
        idx = self._idx(SYDNEY, '10:00', '16:00', days=2, start='2026-06-01')
        assert len(set(idx.date)) == 3                                # 2 sessions, 3 NY dates
        assert Backtester._observed_bars_per_year(idx, BarSize.Mins1, SYDNEY) == 252 * 360
        assert Backtester._observed_bars_per_year(idx, BarSize.Mins1) != 252 * 360

    def test_without_timestamps_it_falls_back_to_the_bar_size(self):
        assert Backtester._observed_bars_per_year(pd.RangeIndex(500), BarSize.Mins1) == 98_280.0
        assert Backtester._observed_bars_per_year(
            pd.DatetimeIndex([], tz=NY), BarSize.Mins1) == 98_280.0

    def _run(self, frame):
        config = BacktestConfig(
            start_date=dt.datetime(2025, 1, 1), end_date=dt.datetime(2027, 1, 1),
            bar_size=BarSize.Mins1, execution_mode='live', trade_notional=50_000,
            slippage_bps=0.0, commission_per_share=0.0, cooldown_seconds=0)
        return Backtester(_Store({1: frame}), config).run(
            _install(_HoldRth(), [1], bar_size=BarSize.Mins1), [1])

    def test_sharpe_does_not_depend_on_flat_extended_hours_bars(self):
        """The reviewer's case (t_sharpe.py): one strategy, one price path,
        stored with and without the flat extended-hours minutes. The curve
        with 960 bars a day used to report sqrt(390/960) = 0.64 of the other's
        Sharpe and Sortino for identical trading."""
        rth = synthetic_markets.driftless_session_bars(days=40, seed=5, annual_drift=0.60)
        a, b = self._run(rth), self._run(_with_extended_hours(rth))

        assert a.total_trades == b.total_trades == 80
        assert a.total_return == pytest.approx(b.total_return, rel=1e-9)
        assert abs(a.sharpe_ratio) > 0.5
        # Not exactly equal: the flat bars change the sample mean/variance a
        # little, not the annualisation.
        assert b.sharpe_ratio == pytest.approx(a.sharpe_ratio, rel=0.02)
        assert b.sortino_ratio == pytest.approx(a.sortino_ratio, rel=0.02)

    def test_an_rth_curve_reports_the_number_it_always_did(self):
        rth = synthetic_markets.driftless_session_bars(days=40, seed=5, annual_drift=0.60)
        result = self._run(rth)
        returns = result.equity_curve.pct_change().dropna()
        legacy = (returns.mean() / returns.std()) * np.sqrt(98_280.0)
        assert result.sharpe_ratio == legacy                    # bit for bit


# --------------------------------------------------------------------------
# B4.7c — one Sortino
# --------------------------------------------------------------------------

class TestOneSortino:
    def test_the_panel_summary_uses_downside_rms_like_run(self):
        rng = np.random.default_rng(2)
        returns = rng.normal(0.0004, 0.01, 2000)
        curve = pd.Series(100_000.0 * np.cumprod(1 + returns),
                          index=pd.bdate_range('2015-01-02', periods=2000, tz=NY))
        bt = Backtester(_Store({}), _cfg())
        summary = bt._summarise(curve, [], [], {})

        r = curve.pct_change().dropna()
        rms = r.mean() / math.sqrt((r.clip(upper=0.0) ** 2).mean()) * math.sqrt(252)
        assert summary.sortino_ratio == pytest.approx(rms, rel=1e-12)
        # What it used to be: the std of the negative returns alone.
        old = r.mean() / r[r < 0].std(ddof=1) * math.sqrt(252)
        assert abs(old - rms) > 0.05 * abs(rms)

    def test_run_and_panel_agree_on_the_same_curve(self):
        closes = 100.0 + np.cumsum(np.random.default_rng(7).normal(0.05, 1.0, 300))
        frames = {1: _daily(closes), 2: _daily(np.full(300, 50.0))}

        class AllInOne(Strategy):
            def on_panel(self, panel, state, index):
                return {1: 1.0} if index == 0 else None

        cfg = dict(execution_mode='accumulate', trade_notional=None)
        panel = Backtester(_Store(frames), _cfg(**cfg)).run_panel(
            _install(AllInOne(), [1, 2]), [1, 2])
        bt = Backtester(_Store(frames), _cfg(**cfg))
        r = panel.equity_curve.pct_change().dropna()
        assert panel.sortino_ratio == pytest.approx(
            r.mean() / bt._downside_deviation(r) * math.sqrt(252), rel=1e-12)
        assert panel.sortino_ratio != 0.0


# --------------------------------------------------------------------------
# B4.6 — a parameter is used as given or refused
# --------------------------------------------------------------------------

class _Bands(Strategy):
    BB_ALPHA = 2
    RSI_OVERSOLD = 30
    WIDTH = 1.5
    MAX_HOLD = None

    def on_prices(self, prices):
        return None


class TestParameterCoercion:
    def test_a_fractional_float_for_an_int_parameter_is_refused_by_name(self):
        with pytest.raises(ValueError) as refused:
            _coerce_param(1.5, 2, 'BB_ALPHA')
        message = str(refused.value)
        assert 'BB_ALPHA' in message and '1.5' in message and 'int' in message

    def test_the_grid_from_the_audit_no_longer_runs_as_1_2_2(self):
        applied, refused = [], []
        for value in (1.5, 2.0, 2.5):
            instance = _install(_Bands(), [1])
            try:
                applied.append(apply_param_overrides(instance, {'BB_ALPHA': value})['BB_ALPHA'])
            except ValueError as ex:
                assert 'BB_ALPHA' in str(ex)
                refused.append(value)
        assert applied == [2] and refused == [1.5, 2.5]

    def test_an_integral_float_is_the_same_number(self):
        value = _coerce_param(2.0, 2, 'BB_ALPHA')
        assert value == 2 and type(value) is int
        assert _coerce_param(np.float64(30.0), 30, 'RSI_OVERSOLD') == 30

    @pytest.mark.parametrize('bad', [27.5, np.float64(27.5), float('nan'), float('inf')])
    def test_values_no_int_can_hold_are_refused(self, bad):
        with pytest.raises(ValueError, match='RSI_OVERSOLD'):
            _coerce_param(bad, 30, 'RSI_OVERSOLD')

    @pytest.mark.parametrize('current', [20, 1.5, True, 'x'])
    def test_none_is_refused_for_a_typed_parameter(self, current):
        with pytest.raises(ValueError, match='EMA_PERIOD'):
            _coerce_param(None, current, 'EMA_PERIOD')

    def test_none_over_a_none_default_is_still_none(self):
        assert _coerce_param(None, None, 'MAX_HOLD') is None
        instance = _install(_Bands(), [1])
        assert apply_param_overrides(instance, {'MAX_HOLD': None}) == {'MAX_HOLD': None}

    def test_the_accepted_forms_are_unchanged(self):
        assert _coerce_param('15', 20, 'EMA_PERIOD') == 15
        assert _coerce_param(3, 1.5, 'WIDTH') == 3.0
        assert _coerce_param('2.5', 1.5, 'WIDTH') == 2.5
        assert _coerce_param(2.5, 1.5, 'WIDTH') == 2.5
        assert _coerce_param('false', True, 'FLAG') is False
        with pytest.raises(ValueError, match='EMA_PERIOD'):
            _coerce_param('2.5', 20, 'EMA_PERIOD')


# --------------------------------------------------------------------------
# B4.7a — every persisted run says which semantics produced it
# --------------------------------------------------------------------------

SWEEP_STRATEGY = '''
from trader.objects import Action
from trader.trading.strategy import Signal, Strategy


class Flip(Strategy):
    HOLD = 2

    def on_bar(self, prices, state, index):
        if index == 1:
            return Signal(source_name='flip', action=Action.BUY, probability=0.9, risk=0.1)
        if index == 1 + self.HOLD:
            return Signal(source_name='flip', action=Action.SELL, probability=0.9, risk=0.1)
        return None
'''


class TestExecutionModeStamp:
    def test_one_helper_stamps_every_mode(self):
        live = BacktestConfig(execution_mode='live', initial_capital=50_000.0)
        assert cli._stamp_execution_params({'A': 1}, live) == {
            'A': 1, '_execution_mode': 'live', '_trade_notional': 1_000.0}

        pyramid = BacktestConfig(execution_mode='pyramid_fixed', trade_notional=1_888.0,
                                 pyramid_max_adds=3)
        assert cli._stamp_execution_params({}, pyramid) == {
            '_execution_mode': 'pyramid_fixed', '_trade_notional': 1_888.0,
            '_pyramid_max_adds': 3}

        # The legacy default stays unstamped, as it always was.
        assert cli._stamp_execution_params({'A': 1}, BacktestConfig()) == {'A': 1}

    @pytest.fixture
    def stack(self, tmp_path, monkeypatch):
        history = str(tmp_path / 'history.duckdb')
        main = str(tmp_path / 'main.duckdb')
        idx = pd.bdate_range(end=dt.datetime.now().date(), periods=30, tz=NY)
        close = 100.0 + np.arange(len(idx), dtype=float)
        bars = pd.DataFrame({
            'open': close - 0.5, 'high': close + 1.0, 'low': close - 1.0, 'close': close,
            'volume': 1e6, 'average': close, 'bar_count': 100,
        }, index=idx.tz_convert('UTC')).rename_axis('date')
        DuckDBDataStore(history).write('4391', bars)
        strategy = tmp_path / 'flip.py'
        strategy.write_text(SWEEP_STRATEGY)

        from trader.container import Container
        monkeypatch.setattr(Container, 'instance', classmethod(
            lambda cls: SimpleNamespace(config=lambda: {
                'duckdb_path': main, 'history_duckdb_path': history,
                'universe_library': 'Universes'})))
        monkeypatch.setattr(cli, '_json_mode', True)
        return SimpleNamespace(main=main, strategy=str(strategy))

    def _sweep(self, stack, live):
        cli._handle_backtest_sweep(argparse.Namespace(
            strategy=stack.strategy, class_name='Flip', conids=[4391], universe=None,
            days=60, capital=100_000.0, bar_size='1 day', grid='{"HOLD": [2, 3]}',
            top=10, note='', slippage_bps=1.0, live_semantics=live, save_trades=False))
        return BacktestStore(stack.main).list(limit=10)

    def test_bt_sweep_live_semantics_rows_are_stamped(self, stack, capsys):
        rows = self._sweep(stack, live=True)
        capsys.readouterr()
        assert len(rows) == 2 and all(r.total_trades == 2 for r in rows)
        for row in rows:
            assert row.params['_execution_mode'] == 'live'
            assert row.params['_trade_notional'] == 2_000.0
        assert sorted(r.params['HOLD'] for r in rows) == [2, 3]

    def test_bt_sweep_accumulate_rows_are_unchanged(self, stack, capsys):
        rows = self._sweep(stack, live=False)
        capsys.readouterr()
        assert len(rows) == 2
        assert all(set(r.params) == {'HOLD'} for r in rows)
