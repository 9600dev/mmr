"""D06: a panel rebalance never overdraws cash.

``Backtester.run_panel`` sizes a target off the DECISION bar's close and fills
at the NEXT bar's open. When the open gaps up - or when the book was sized off
a holding it could not sell - the buys used to cost more than the cash on hand
and the book ran on implicit, cost-free leverage. Now the rebalance's sells
settle first and its buys are scaled pro rata to the cash then available,
truncated to whole shares; the guard's activity is recorded in
``BacktestResult.diagnostics``. Pinned here (outside tests/invariants, per D06)
with the cash ledger rebuilt from the fill log, which is how the harness's
``min_cash_fraction`` reads it.
"""
from __future__ import annotations

import datetime as dt
import math

import numpy as np
import pandas as pd
import pytest

from trader.objects import BarSize
from trader.simulation.backtester import BacktestConfig, Backtester
from trader.trading.strategy import Strategy


class _NoStorage:
    def get_tickdata(self, bar_size):
        raise AssertionError('frames are injected')


def _frames(n=12, conids=(1, 2, 3), gap=1.20, seed=0):
    """Every open is ``gap`` x the PRIOR close: a buy sized off yesterday's
    close costs ``gap`` x its budget at the fill."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range('2024-01-01', periods=n, freq='D')
    out = {}
    for k, c in enumerate(conids):
        close = 100.0 + k * 10 + np.arange(n, dtype=float)
        opn = np.concatenate([[close[0]], close[:-1] * gap])
        out[c] = pd.DataFrame({'open': opn, 'high': np.maximum(opn, close) + 1.0,
                               'low': np.minimum(opn, close) - 1.0, 'close': close,
                               'volume': rng.integers(1000, 5000, n).astype(float)}, index=idx)
    return out


class _Weights(Strategy):
    """Returns ``schedule[index]`` when present, else None (hold)."""
    def __init__(self, schedule):
        super().__init__()
        self.schedule = schedule

    def precompute_panel(self, panel):
        return {}

    def on_panel(self, panel, state, index):
        return self.schedule.get(index)


def _run(strategy, frames, capital=100_000.0, slippage_bps=0.0, commission=0.0, borrow=0.0):
    cfg = BacktestConfig(start_date=dt.datetime(2024, 1, 1), end_date=dt.datetime(2024, 2, 1),
                         initial_capital=capital, bar_size=BarSize.Days1, slippage_bps=slippage_bps,
                         commission_per_share=commission, short_borrow_bps_annual=borrow)
    bt = Backtester(_NoStorage(), cfg)
    return bt.run_panel(strategy, list(frames), frames=frames)


def _cash_path(trades, capital):
    """End-of-fill-day cash rebuilt from the trade log (the harness's view)."""
    cash, out = capital, []
    for ts, grp in pd.Series(trades, index=[t.timestamp for t in trades]).groupby(level=0):
        for t in grp:
            sgn = 1.0 if str(t.action).endswith('BUY') else -1.0
            cash -= sgn * t.quantity * t.price + t.commission
        out.append((ts, cash))
    return out


def test_a_gapping_open_scales_the_buy_to_the_cash_available_and_is_counted():
    frames = _frames(gap=1.20)
    res = _run(_Weights({2: {1: 1.0, 2: 0.0, 3: 0.0}}), frames)
    (t,) = res.trades
    sized = math.trunc(100_000.0 / float(frames[1]['close'].iloc[2]))     # the pre-fix quantity
    fill = float(frames[1]['open'].iloc[3])
    assert sized * fill > 100_000.0, 'fixture must make the unscaled buy overdraw'
    assert t.quantity == math.floor(sized * (100_000.0 / (sized * fill)))
    assert t.quantity * fill <= 100_000.0
    assert all(c >= 0.0 for _, c in _cash_path(res.trades, 100_000.0))
    assert res.diagnostics['panel_scaled_buy_fills'] == 1
    assert res.diagnostics['panel_scaled_rebalances'] == 1
    assert res.diagnostics['panel_dropped_buy_fills'] == 0
    assert res.diagnostics['panel_min_cash'] >= 0.0
    # equity still adds up: cash + shares x last close
    cash = 100_000.0 - t.quantity * fill
    assert float(res.equity_curve.iloc[-1]) == pytest.approx(cash + t.quantity * float(frames[1]['close'].iloc[-1]))


def test_without_a_gap_nothing_is_scaled_and_the_diagnostics_say_so():
    frames = _frames(gap=0.90)
    res = _run(_Weights({2: {1: 1.0, 2: 0.0, 3: 0.0}}), frames)
    (t,) = res.trades
    assert t.quantity == math.trunc(100_000.0 / float(frames[1]['close'].iloc[2]))
    assert res.diagnostics == {'panel_scaled_buy_fills': 0, 'panel_dropped_buy_fills': 0,
                               'panel_scaled_rebalances': 0, 'panel_min_cash': res.diagnostics['panel_min_cash']}
    assert res.diagnostics['panel_min_cash'] >= 0.0


def test_sells_settle_before_buys_so_a_rotation_needs_no_scaling():
    """Rotate 100% of 1 into 100% of 2 on a flat market: the sale proceeds fund
    the purchase, so the buy fills at its full sized quantity."""
    frames = _frames(gap=1.0)
    res = _run(_Weights({2: {1: 1.0, 2: 0.0, 3: 0.0}, 6: {1: 0.0, 2: 1.0, 3: 0.0}}), frames)
    by = {(t.conid, str(t.action).split('.')[-1]): t for t in res.trades}
    assert (1, 'BUY') in by and (1, 'SELL') in by and (2, 'BUY') in by
    assert by[(1, 'SELL')].timestamp == by[(2, 'BUY')].timestamp
    equity_at_decision = float(res.equity_curve.loc[frames[1].index[6]])
    assert by[(2, 'BUY')].quantity == math.trunc(equity_at_decision / float(frames[2]['close'].iloc[6]))
    assert res.diagnostics['panel_scaled_buy_fills'] == 0
    assert all(c >= 0.0 for _, c in _cash_path(res.trades, 100_000.0))


def test_a_pro_rata_scale_applies_to_every_buy_of_the_rebalance():
    frames = _frames(gap=1.25)
    res = _run(_Weights({2: {1: 0.5, 2: 0.5, 3: 0.0}}), frames)
    buys = {t.conid: t for t in res.trades}
    assert set(buys) == {1, 2}
    sized = {c: math.trunc(50_000.0 / float(frames[c]['close'].iloc[2])) for c in (1, 2)}
    fills = {c: float(frames[c]['open'].iloc[3]) for c in (1, 2)}
    scale = 100_000.0 / sum(sized[c] * fills[c] for c in (1, 2))
    assert 0.0 < scale < 1.0
    for c in (1, 2):
        assert buys[c].quantity == math.floor(sized[c] * scale)
    assert sum(buys[c].quantity * fills[c] for c in (1, 2)) <= 100_000.0
    assert res.diagnostics['panel_scaled_buy_fills'] == 2 and res.diagnostics['panel_scaled_rebalances'] == 1


def test_short_sale_proceeds_count_as_cash_for_the_same_rebalance():
    frames = _frames(gap=1.0)
    res = _run(_Weights({2: {1: -0.5, 2: 0.5, 3: 0.0}}), frames)
    by = {t.conid: t for t in res.trades}
    assert str(by[1].action).endswith('SELL') and str(by[2].action).endswith('BUY')
    assert by[2].quantity == math.trunc(50_000.0 / float(frames[2]['close'].iloc[2]))
    assert res.diagnostics['panel_scaled_buy_fills'] == 0
    # equity = cash + long - short, marked at the last close
    cash = 100_000.0 + by[1].quantity * by[1].price - by[2].quantity * by[2].price
    expect = cash - by[1].quantity * float(frames[1]['close'].iloc[-1]) + by[2].quantity * float(frames[2]['close'].iloc[-1])
    assert float(res.equity_curve.iloc[-1]) == pytest.approx(expect)


def test_slippage_and_commission_are_inside_the_budget():
    frames = _frames(gap=1.0)
    res = _run(_Weights({2: {1: 1.0, 2: 0.0, 3: 0.0}}), frames, slippage_bps=50.0, commission=0.01)
    (t,) = res.trades
    assert t.quantity * t.price + t.commission <= 100_000.0
    assert res.diagnostics['panel_scaled_buy_fills'] == 1     # flat open + costs > budget by a hair
    assert res.diagnostics['panel_min_cash'] >= 0.0


@pytest.mark.parametrize('seed', range(6))
def test_cash_rebuilt_from_the_fill_log_never_goes_negative_on_a_long_only_book(seed):
    """The D06 property itself, over random long-only monthly books on gapping
    markets with costs: end-of-fill-day cash >= 0 at every fill date."""
    rng = np.random.default_rng(seed)
    n, conids = 60, (1, 2, 3, 4)
    idx = pd.date_range('2024-01-01', periods=n, freq='D')
    frames = {}
    for k, c in enumerate(conids):
        close = 100.0 * (1 + k) * np.cumprod(1 + rng.normal(0.0, 0.03, n))
        gap = 1 + rng.normal(0.0, 0.05, n)
        opn = np.concatenate([[close[0]], close[:-1] * gap[1:]])
        frames[c] = pd.DataFrame({'open': opn, 'high': np.maximum(opn, close), 'low': np.minimum(opn, close),
                                  'close': close, 'volume': np.full(n, 1e6)}, index=idx)
    schedule = {}
    for i in range(1, n, 5):
        w = rng.dirichlet(np.ones(len(conids)))
        schedule[i] = {c: float(x) for c, x in zip(conids, w)}
    res = _run(_Weights(schedule), frames, slippage_bps=10.0, commission=0.005)
    path = _cash_path(res.trades, 100_000.0)
    assert path, 'the book must have traded'
    assert min(c for _, c in path) >= -1e-9
    assert res.diagnostics['panel_min_cash'] >= -1e-9
    assert res.diagnostics['panel_scaled_buy_fills'] >= 1, 'fixture must exercise the guard at least once'


def test_sells_execute_and_log_before_buys_whatever_order_the_strategy_used():
    """Rotation listed as {new: 1.0, old: 0.0}: the SELL of the old name must be
    executed (and appear in the log) before the BUY, so cash after every single
    fill stays >= 0 and the buy is not scaled for want of proceeds."""
    frames = _frames(gap=1.0)
    res = _run(_Weights({2: {1: 1.0, 2: 0.0, 3: 0.0}, 6: {2: 1.0, 1: 0.0, 3: 0.0}}), frames)
    same_day = [t for t in res.trades if t.timestamp == frames[1].index[7]]
    assert [(t.conid, str(t.action).split('.')[-1]) for t in same_day] == [(1, 'SELL'), (2, 'BUY')]
    cash = 100_000.0
    for t in res.trades:                       # cash after EVERY fill, in log order
        cash -= (1.0 if str(t.action).endswith('BUY') else -1.0) * t.quantity * t.price + t.commission
        assert cash >= -1e-9, (t, cash)
    equity_at_decision = float(res.equity_curve.loc[frames[1].index[6]])
    assert same_day[1].quantity == math.trunc(equity_at_decision / float(frames[2]['close'].iloc[6]))
    assert res.diagnostics['panel_scaled_buy_fills'] == 0
    assert res.diagnostics['panel_min_cash'] >= 0.0
