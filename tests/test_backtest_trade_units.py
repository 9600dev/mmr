"""Three counts called "trades", and which one a significance test may use
(audit 2026-11-14, B4.5).

``BacktestResult.total_trades`` counts FILLS (every BUY and SELL; a pyramid
add is a fill). Win rate / profit factor / expectancy are per EXIT (per SELL).
``statistical_confidence.n_trades`` counted FIFO LOTS. With pyramiding one
exit closes several lots; they share the exit price and most of the holding
period, so their P&Ls move together — and the per-trade t-test took each lot
as an independent draw. On driftless random walks (no edge by construction)
a strategy that stacks three lots per exit was "significant at p < 0.05" in
roughly one run in four.

The unit of every per-trade statistic is now the EXIT: P&L summed over the
lots one closing order realised. The three counts have their own names.
"""
import datetime as dt
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import trader.mmr_cli as cli
from trader.objects import Action, BarSize
from trader.simulation.backtest_stats import (
    bootstrap_ci, compute_all, exit_pnls, round_trip_pnls, t_test_mean_zero, trade_counts)
from trader.simulation.backtester import Backtester, BacktestConfig
from trader.trading.strategy import Signal, Strategy

CYCLE, EXIT_BAR = 30, 25


def _fill(bar, action, quantity, price, conid=1, commission=0.0):
    stamp = pd.Timestamp('2026-01-05 09:30', tz='America/New_York') + pd.Timedelta(minutes=bar)
    return {'timestamp': str(stamp), 'conid': conid, 'action': f'Action.{action}',
            'quantity': quantity, 'price': price, 'commission': commission}


def _driftless_fills(seed, adds=(0, 5, 10), cycles=70, shares=10.0):
    """A zero-drift random walk; BUY on bars ``adds`` of every 30-bar cycle and
    one SELL of the whole stack on bar 25. No edge by construction."""
    rng = np.random.default_rng(seed)
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, cycles * CYCLE)))
    fills = []
    for cycle in range(cycles):
        base = cycle * CYCLE
        fills += [_fill(base + k, 'BUY', shares, float(close[base + k])) for k in adds]
        fills.append(_fill(base + EXIT_BAR, 'SELL', shares * len(adds), float(close[base + EXIT_BAR])))
    return fills


def _false_positive_rate(pnl_series, seeds, **kwargs):
    rejected = 0
    for seed in range(seeds):
        _, p_value = t_test_mean_zero(np.array(pnl_series(_driftless_fills(seed, **kwargs))))
        rejected += p_value is not None and p_value < 0.05
    return rejected / seeds


# --------------------------------------------------------------------------
# exit_pnls / trade_counts
# --------------------------------------------------------------------------

class TestExitPnls:
    def test_one_exit_closing_three_lots_is_one_pnl(self):
        fills = [_fill(0, 'BUY', 10, 100.0), _fill(5, 'BUY', 10, 101.0), _fill(10, 'BUY', 10, 102.0),
                 _fill(25, 'SELL', 30, 103.0)]
        assert round_trip_pnls(fills) == pytest.approx([30.0, 20.0, 10.0])
        assert exit_pnls(fills) == pytest.approx([60.0])

    def test_commissions_are_in_the_exit_pnl(self):
        fills = [_fill(0, 'BUY', 10, 100.0, commission=1.0), _fill(5, 'BUY', 10, 101.0, commission=1.0),
                 _fill(25, 'SELL', 20, 103.0, commission=2.0)]
        assert exit_pnls(fills) == pytest.approx([30.0 + 20.0 - 4.0])
        assert sum(exit_pnls(fills)) == pytest.approx(sum(round_trip_pnls(fills)))

    def test_single_lot_round_trips_are_unchanged(self):
        fills = [_fill(0, 'BUY', 10, 100.0), _fill(5, 'SELL', 10, 101.0),
                 _fill(10, 'BUY', 10, 102.0), _fill(15, 'SELL', 10, 100.0)]
        assert exit_pnls(fills) == pytest.approx([10.0, -20.0])
        assert exit_pnls(fills) == pytest.approx(round_trip_pnls(fills))

    def test_scaling_out_of_one_lot_is_two_exits(self):
        fills = [_fill(0, 'BUY', 20, 100.0), _fill(5, 'SELL', 10, 101.0), _fill(9, 'SELL', 10, 99.0)]
        assert exit_pnls(fills) == pytest.approx([10.0, -10.0])

    def test_exits_of_different_instruments_at_one_instant_stay_separate(self):
        fills = [_fill(0, 'BUY', 10, 100.0, conid=1), _fill(0, 'BUY', 10, 50.0, conid=2),
                 _fill(5, 'SELL', 10, 101.0, conid=1), _fill(5, 'SELL', 10, 49.0, conid=2)]
        assert sorted(exit_pnls(fills)) == pytest.approx([-10.0, 10.0])

    def test_one_closing_order_filled_in_two_parts_is_one_exit(self):
        """Partial fills of one closing order carry the same instrument and
        instant: one decision, one P&L."""
        fills = [_fill(0, 'BUY', 10, 100.0), _fill(3, 'BUY', 10, 101.0),
                 _fill(25, 'SELL', 12, 103.0), _fill(25, 'SELL', 8, 103.5)]
        assert len(round_trip_pnls(fills)) == 3
        assert exit_pnls(fills) == pytest.approx([sum(round_trip_pnls(fills))])

    def test_a_sell_with_nothing_to_close_is_not_an_exit(self):
        assert exit_pnls([_fill(0, 'SELL', 10, 100.0)]) == []
        assert exit_pnls([_fill(0, 'BUY', 10, 100.0)]) == []
        assert exit_pnls([]) == []

    def test_fills_without_a_timestamp_are_not_merged_into_one_exit(self):
        fills = [{'conid': 1, 'action': 'BUY', 'quantity': 10, 'price': 100.0},
                 {'conid': 1, 'action': 'SELL', 'quantity': 5, 'price': 101.0},
                 {'conid': 1, 'action': 'SELL', 'quantity': 5, 'price': 99.0}]
        assert exit_pnls(fills) == pytest.approx([5.0, -5.0])


class TestTradeCounts:
    def test_the_three_counts_of_a_pyramided_run(self):
        counts = trade_counts(_driftless_fills(0, cycles=7))
        assert (counts.n_fills, counts.n_lots, counts.n_exits) == (28, 21, 7)

    def test_single_lot_run(self):
        counts = trade_counts(_driftless_fills(0, adds=(0,), cycles=7))
        assert (counts.n_fills, counts.n_lots, counts.n_exits) == (14, 7, 7)

    def test_open_position_is_a_fill_and_nothing_else(self):
        counts = trade_counts([_fill(0, 'BUY', 10, 100.0)])
        assert (counts.n_fills, counts.n_lots, counts.n_exits) == (1, 0, 0)

    def test_unusable_records_are_not_fills(self):
        counts = trade_counts([_fill(0, 'BUY', 0, 100.0), _fill(1, 'BUY', 10, -1.0),
                               {'conid': 1, 'action': 'HOLD', 'quantity': 1, 'price': 1.0},
                               {'conid': 'x', 'action': 'BUY', 'quantity': 1, 'price': 1.0}])
        assert (counts.n_fills, counts.n_lots, counts.n_exits) == (0, 0, 0)
        assert trade_counts(None) == trade_counts([])


# --------------------------------------------------------------------------
# the inflation, and its removal
# --------------------------------------------------------------------------

class TestSignificanceUnit:
    def test_lot_level_t_test_rejects_a_true_null_far_too_often(self):
        """The control that shows why: lots of one exit are not independent."""
        assert _false_positive_rate(round_trip_pnls, seeds=400) > 0.15

    def test_exit_level_t_test_holds_its_nominal_rate(self):
        assert _false_positive_rate(exit_pnls, seeds=400) < 0.08

    def test_five_lots_per_exit_is_worse_at_lot_level_and_still_nominal_per_exit(self):
        adds = (0, 3, 6, 9, 12)
        assert _false_positive_rate(round_trip_pnls, seeds=300, adds=adds) > 0.25
        assert _false_positive_rate(exit_pnls, seeds=300, adds=adds) < 0.08

    def test_compute_all_does_not_call_driftless_pyramids_significant(self):
        """What ``backtests show`` / ``backtests confidence`` report. Before:
        about 22% of these no-edge runs had p < 0.05."""
        seeds = 120
        rejected = sum(compute_all(_driftless_fills(seed), None).p_value < 0.05 for seed in range(seeds))
        assert rejected / seeds < 0.10

    @pytest.mark.parametrize('seed', [0, 1, 2])
    def test_compute_all_tests_and_bootstraps_exit_pnls(self, seed):
        fills = _driftless_fills(seed)
        per_exit = np.array(exit_pnls(fills))
        stats = compute_all(fills, None)

        t_stat, p_value = t_test_mean_zero(per_exit)
        assert (stats.t_stat, stats.p_value) == pytest.approx((t_stat, p_value))
        lot_t, _ = t_test_mean_zero(np.array(round_trip_pnls(fills)))
        assert stats.t_stat != pytest.approx(lot_t)

        low, high = bootstrap_ci(per_exit, lambda s: np.mean(s, axis=-1))
        assert (stats.return_ci_lo, stats.return_ci_hi) == pytest.approx((low, high))
        assert stats.return_ci_n_boot == len(per_exit)

    def test_a_losing_exit_is_one_loss_in_the_streak_not_one_per_lot(self):
        """Five exits, each closing three lots: lose, win, lose, win, win."""
        fills = []
        for cycle, exit_price in enumerate((99.0, 102.0, 99.0, 102.0, 102.0)):
            base = cycle * CYCLE
            fills += [_fill(base + k, 'BUY', 10, 100.0) for k in (0, 5, 10)]
            fills.append(_fill(base + EXIT_BAR, 'SELL', 30, exit_price))
        stats = compute_all(fills, None)
        assert stats.losing_streak_actual == 1, 'was 3: the three lots of one losing exit'


class TestCountsInTheStatsBundle:
    def test_pyramided_run_names_all_three(self):
        stats = compute_all(_driftless_fills(0), None)
        assert (stats.n_fills, stats.n_lots, stats.n_exits) == (280, 210, 70)
        assert stats.n_trades == stats.n_exits == 70, 'n_trades is the sample size of the per-trade tests'

    def test_single_lot_run_is_as_before(self):
        fills = _driftless_fills(0, adds=(0,))
        stats = compute_all(fills, None)
        assert stats.n_trades == stats.n_exits == stats.n_lots == 70
        assert stats.n_fills == 140
        t_stat, p_value = t_test_mean_zero(np.array(round_trip_pnls(fills)))
        assert (stats.t_stat, stats.p_value) == pytest.approx((t_stat, p_value))

    def test_no_trades(self):
        stats = compute_all(None, None)
        assert (stats.n_fills, stats.n_lots, stats.n_exits, stats.n_trades) == (0, 0, 0, 0)


# --------------------------------------------------------------------------
# against the real backtester (pyramid_fixed), and the CLI payload
# --------------------------------------------------------------------------

class _Stack3(Strategy):
    """BUY on bars 0, 5, 10 of every 30-bar cycle; SELL on bar 25."""

    def on_bar(self, prices, state, index):
        k = index % CYCLE
        if k in (0, 5, 10):
            return Signal('s', Action.BUY, 0.6, 0.4)
        if k == EXIT_BAR:
            return Signal('s', Action.SELL, 0.6, 0.4)
        return None


class _Store:
    def __init__(self, frame):
        self.frame = frame

    def get_tickdata(self, bar_size):
        return SimpleNamespace(read=lambda conid, date_range=None: self.frame)


def _walk(seed, cycles=20):
    rng = np.random.default_rng(seed)
    n = cycles * CYCLE
    index = pd.date_range('2026-01-05 09:30', periods=n, freq='1min', tz='America/New_York')
    close = 100 * np.exp(np.cumsum(rng.normal(0, 0.001, n)))
    open_ = np.r_[100.0, close[:-1]]
    return pd.DataFrame({'open': open_, 'high': np.maximum(open_, close), 'low': np.minimum(open_, close),
                         'close': close, 'volume': 1e4}, index=index).rename_axis('date')


def _backtest(mode, seed=0):
    config = BacktestConfig(start_date=dt.datetime(2026, 1, 1), end_date=dt.datetime(2026, 3, 1),
                            bar_size=BarSize.Mins1, execution_mode=mode, slippage_bps=0.0,
                            commission_per_share=0.0, cooldown_seconds=0)
    result = Backtester(_Store(_walk(seed)), config).run(_Stack3(), [1])
    fills = [{'timestamp': str(t.timestamp), 'conid': int(t.conid), 'action': str(t.action),
              'quantity': float(t.quantity), 'price': float(t.price), 'commission': float(t.commission)}
             for t in result.trades]
    return result, fills


class TestAgainstTheBacktester:
    def test_pyramided_run_has_three_different_counts(self):
        result, fills = _backtest('pyramid_fixed')
        sells = sum(1 for trade in result.trades if trade.action == Action.SELL)
        counts = trade_counts(fills)
        assert sells >= 15 and counts.n_lots > sells, 'the run must actually pyramid'
        assert counts.n_fills == result.total_trades == len(result.trades)
        assert counts.n_exits == sells
        assert counts.n_lots == len(round_trip_pnls(fills)) == counts.n_fills - sells

        stats = compute_all(fills, None)
        assert stats.n_trades == sells
        assert (stats.n_fills, stats.n_lots, stats.n_exits) == (counts.n_fills, counts.n_lots, sells)

    def test_single_lot_run_lots_equal_exits(self):
        result, fills = _backtest('live')
        counts = trade_counts(fills)
        assert counts.n_exits == counts.n_lots > 0
        assert counts.n_fills == result.total_trades


class TestCliPayload:
    def _record(self, fills):
        return SimpleNamespace(trades_json=json.dumps(fills), equity_curve_json='', bar_size='1 min')

    def test_count_fields_name_the_three_and_keep_the_old_keys(self):
        stats, _ = cli._compute_bt_stats(self._record(_driftless_fills(0)))
        fields = cli._bt_trade_count_fields(stats)
        assert fields == {'n_trades': 70, 'n_round_trips': 70, 'n_exits': 70, 'n_lots': 210,
                          'n_fills': 280, 'trade_unit': 'exit'}

    def test_summary_counts_from_a_backtest_result(self):
        result, _ = _backtest('pyramid_fixed')
        sells = sum(1 for trade in result.trades if trade.action == Action.SELL)
        fields = cli._bt_result_count_fields(result)
        assert fields == {'n_fills': len(result.trades), 'n_exits': sells,
                          'n_lots': len(result.trades) - sells}
