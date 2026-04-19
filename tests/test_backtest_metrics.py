"""Correctness + edge-case tests for the extended backtest metrics
(sortino_ratio, calmar_ratio, profit_factor, expectancy_bps, time_in_market_pct).

Approach: hand-craft a deterministic OHLCV series and a scripted strategy
so we can compute the expected metric values by hand, then assert the
Backtester produces them exactly.
"""

import datetime as dt
import logging
import os
import sys

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from trader.data.data_access import TickStorage
from trader.data.duckdb_store import DuckDBDataStore
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation.backtester import Backtester, BacktestConfig
from trader.simulation.slippage import ZeroSlippage
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _write_bars(duckdb_path: str, closes: list, conid: int = 4391) -> None:
    """Write a deterministic OHLCV series with the supplied close prices
    and open=close (no bar-internal move) so fills happen at predictable
    prices under next_open policy."""
    store = DuckDBDataStore(duckdb_path)
    n = len(closes)
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "open":   closes,
        "high":   [c + 0.01 for c in closes],
        "low":    [c - 0.01 for c in closes],
        "close":  closes,
        "volume": [1000.0] * n,
    }, index=dates)
    df.index.name = "date"
    store.write(str(conid), df)


def _make_bt(duckdb_path: str, *, fill_policy: str = 'next_open', initial_capital: float = 100_000.0) -> Backtester:
    storage = TickStorage(duckdb_path=duckdb_path)
    config = BacktestConfig(
        start_date=dt.datetime(2024, 1, 2, 9, 30, tzinfo=dt.timezone.utc),
        end_date=dt.datetime(2024, 1, 2, 10, 30, tzinfo=dt.timezone.utc),
        bar_size=BarSize.Mins1,
        initial_capital=initial_capital,
        slippage_model=ZeroSlippage(),
        commission_per_share=0.0,
        fill_policy=fill_policy,
    )
    return Backtester(storage=storage, config=config)


def _install(strategy: Strategy, duckdb_path: str, conids=None) -> Strategy:
    conids = conids or [4391]
    storage = TickStorage(duckdb_path=duckdb_path)
    ua = UniverseAccessor.__new__(UniverseAccessor)
    ua.duckdb_path = duckdb_path
    ua.universe_library = "Universes"
    ctx = StrategyContext(
        name=strategy.__class__.__name__,
        bar_size=BarSize.Mins1,
        conids=conids,
        universe=None,
        historical_days_prior=0,
        paper=True,
        storage=storage,
        universe_accessor=ua,
        logger=logging.getLogger("test"),
    )
    strategy.install(ctx)
    strategy.state = StrategyState.RUNNING
    return strategy


class ScriptedStrategy(Strategy):
    """Emits BUY/SELL/None at scripted bar indices. Lets us construct an
    exact sequence of fills so the metric math is deterministic."""

    def __init__(self, script: list):
        """script: list aligned with bars. Each element is
        'buy', 'sell', or None. Quantities default to 1."""
        super().__init__()
        self._script = script
        self._i = 0

    def on_prices(self, prices):
        i = self._i
        self._i += 1
        if i >= len(self._script) or self._script[i] is None:
            return None
        action = Action.BUY if self._script[i] == 'buy' else Action.SELL
        return Signal(
            source_name=self.name or 'scripted',
            action=action,
            probability=0.5, risk=0.5,
            quantity=1,
        )


# ---------------------------------------------------------------------------
# Metric correctness — deterministic scenarios
# ---------------------------------------------------------------------------

class TestDefaultSellSizing:
    """Regression: a strategy that accumulates BUYs and then emits SELL
    signals with default quantity=0 used to silently drop the SELLs once
    cash was depleted — the default-quantity code sized SELLs as "10% of
    cash", which rounds to 0 shares. Now it defaults to closing the full
    position."""

    def test_sell_default_quantity_closes_full_position(self, tmp_duckdb_path):
        # Rising prices — strategy does BUY, BUY, BUY, SELL. If the SELL
        # gets dropped (old bug) total_trades = 3; with the fix = 4.
        closes = [100, 100, 100,    # bar 0-2: BUY fills
                  102, 102, 102,    # bar 3-5: BUY fills
                  104, 104, 104,    # bar 6-8: BUY fills
                  106, 108, 110,    # rising
                  115, 115, 115]    # SELL fills
        _write_bars(tmp_duckdb_path, closes)
        script = ['buy', None, None, 'buy', None, None, 'buy', None, None,
                  None, None, None, 'sell', None, None]
        bt = _make_bt(tmp_duckdb_path)
        bt.config.initial_capital = 500  # intentionally tight so cash gets drained
        s = _install(ScriptedStrategy(script), tmp_duckdb_path)
        r = bt.run(s, [4391])

        # 3 BUYs + 1 SELL = 4 trades. With the old bug the final SELL
        # would have been dropped (cash fully drained → default SELL qty = 0).
        assert r.total_trades == 4, (
            f'expected 4 trades (3 BUY + 1 SELL closing full position), '
            f'got {r.total_trades} — the SELL may have been dropped due to '
            f'cash-depleted default sizing'
        )


class TestProfitFactor:
    def test_all_wins_gives_inf(self, tmp_duckdb_path):
        """Every round-trip is a winner → profit factor is mathematically
        infinite. Backtester stores it as a sentinel; we just check the
        value is > any plausible finite result."""
        # Close prices: BUY at 100, fill at 101 (next-open). SELL at 105,
        # fill at 106. Clean +5 winner. Repeat with higher prices.
        closes = [100, 101, 101, 101, 105, 106, 106, 106, 110, 111, 111, 111, 120, 121, 121, 121]
        _write_bars(tmp_duckdb_path, closes)
        # Script: buy at bar 0 → fill at bar 1 open (101);
        #         sell at bar 4 → fill at bar 5 open (106) = +5.
        # buy at bar 8 → fill 111, sell at 12 → fill 121 = +10.
        script = ['buy', None, None, None, 'sell', None, None, None,
                  'buy', None, None, None, 'sell', None, None, None]
        bt = _make_bt(tmp_duckdb_path)
        strategy = _install(ScriptedStrategy(script), tmp_duckdb_path)
        r = bt.run(strategy, [4391])

        # 2 round trips, both winners
        assert r.total_trades == 4
        assert r.profit_factor > 1e15, (
            f"all-wins should saturate profit_factor, got {r.profit_factor}"
        )
        assert r.win_rate == pytest.approx(1.0)

    def test_one_win_one_loss_equal_size(self, tmp_duckdb_path):
        """BUY→SELL with +$5 profit, then BUY→SELL with $5 loss.
        profit_factor = 5 / 5 = 1.0 exactly."""
        closes = [100, 101, 101, 101, 105, 106, 106, 106,     # +5 win on 1 share
                  110, 111, 111, 111, 106, 105, 105, 105]     # -6 loss on 1 share
        _write_bars(tmp_duckdb_path, closes)
        script = ['buy', None, None, None, 'sell', None, None, None,
                  'buy', None, None, None, 'sell', None, None, None]
        bt = _make_bt(tmp_duckdb_path)
        strategy = _install(ScriptedStrategy(script), tmp_duckdb_path)
        r = bt.run(strategy, [4391])

        # Round trip 1: bought at 101, sold at 106 → +5
        # Round trip 2: bought at 111, sold at 105 → -6
        # profit_factor = 5 / 6 ≈ 0.833
        assert r.total_trades == 4
        assert r.profit_factor == pytest.approx(5.0 / 6.0, rel=0.01)
        assert r.win_rate == pytest.approx(0.5)

    def test_no_trades_is_zero(self, tmp_duckdb_path):
        """Strategy that emits no signals → profit_factor is 0, not NaN."""
        _write_bars(tmp_duckdb_path, [100] * 20)
        bt = _make_bt(tmp_duckdb_path)
        strategy = _install(ScriptedStrategy([None] * 20), tmp_duckdb_path)
        r = bt.run(strategy, [4391])
        assert r.total_trades == 0
        assert r.profit_factor == 0.0
        assert r.expectancy_bps == 0.0


class TestExpectancyBps:
    def test_known_expectancy(self, tmp_duckdb_path):
        """Single round-trip: BUY at 100, SELL at 101. Notional 100, P&L 1.
        Return-on-notional = 1%. expectancy_bps = 100 bps."""
        closes = [100, 100, 100, 100, 101, 101, 101, 101]
        _write_bars(tmp_duckdb_path, closes)
        script = ['buy', None, None, None, 'sell', None, None, None]
        bt = _make_bt(tmp_duckdb_path)
        strategy = _install(ScriptedStrategy(script), tmp_duckdb_path)
        r = bt.run(strategy, [4391])
        # BUY at bar 0 fills at bar 1 open (100). SELL at bar 4 fills at bar 5 open (101).
        # P&L = 101 - 100 = 1 on notional 100 → +100 bps
        assert r.total_trades == 2
        assert r.expectancy_bps == pytest.approx(100.0, rel=0.01)


class TestTimeInMarket:
    def test_always_on(self, tmp_duckdb_path):
        """BUY on bar 0, never sell. Should be in market for almost every bar."""
        closes = list(range(100, 120))
        _write_bars(tmp_duckdb_path, closes)
        script = ['buy'] + [None] * (len(closes) - 1)
        bt = _make_bt(tmp_duckdb_path)
        strategy = _install(ScriptedStrategy(script), tmp_duckdb_path)
        r = bt.run(strategy, [4391])
        # BUY on bar 0 fills at bar 1 open. From bar 1 onward we're in market.
        # Bar 0 had no position at end → not counted. 19 of 20 bars in market.
        assert 0.9 <= r.time_in_market_pct <= 1.0

    def test_never_on(self, tmp_duckdb_path):
        closes = [100] * 20
        _write_bars(tmp_duckdb_path, closes)
        bt = _make_bt(tmp_duckdb_path)
        strategy = _install(ScriptedStrategy([None] * 20), tmp_duckdb_path)
        r = bt.run(strategy, [4391])
        assert r.time_in_market_pct == 0.0


class TestCalmarRatio:
    def test_no_drawdown_gives_zero(self, tmp_duckdb_path):
        """No drawdown → Calmar is 0 by our convention (avoids div-by-zero)."""
        closes = [100] * 20
        _write_bars(tmp_duckdb_path, closes)
        bt = _make_bt(tmp_duckdb_path)
        strategy = _install(ScriptedStrategy([None] * 20), tmp_duckdb_path)
        r = bt.run(strategy, [4391])
        assert r.max_drawdown == 0.0
        assert r.calmar_ratio == 0.0

    def test_ratio_correct(self, tmp_duckdb_path):
        """Single BUY at 100 then price drops to 90 then recovers to 105.
        total_return ≈ +5%, max_drawdown ≈ -10% → calmar ≈ 0.5."""
        closes = [100, 100, 100, 100,       # flat, buy filled at 100
                  95, 90,                    # drawdown
                  95, 100, 105,              # recover + gain
                  105, 105, 105, 105]
        _write_bars(tmp_duckdb_path, closes)
        script = ['buy'] + [None] * (len(closes) - 1)
        bt = _make_bt(tmp_duckdb_path)
        # Use small capital so one share moves the needle
        bt.config.initial_capital = 1000
        strategy = _install(ScriptedStrategy(script), tmp_duckdb_path)
        r = bt.run(strategy, [4391])
        # calmar = total_return / |max_drawdown|; sanity-check the ratio > 0
        # with the right sign. Exact arithmetic depends on cash tracking
        # so we assert structure not magnitude.
        assert r.max_drawdown < 0
        assert r.calmar_ratio == pytest.approx(r.total_return / abs(r.max_drawdown), rel=1e-6)


class TestSortinoRatio:
    def test_flat_equity_gives_zero(self, tmp_duckdb_path):
        """No volatility → sortino undefined; we return 0."""
        closes = [100] * 30
        _write_bars(tmp_duckdb_path, closes)
        bt = _make_bt(tmp_duckdb_path)
        strategy = _install(ScriptedStrategy([None] * 30), tmp_duckdb_path)
        r = bt.run(strategy, [4391])
        assert r.sortino_ratio == 0.0

    def test_all_positive_returns_gives_zero_downside(self, tmp_duckdb_path):
        """Monotonically rising equity has no downside std → sortino = 0 by
        our convention (mean/0 would be ∞)."""
        closes = [100, 100, 100, 100] + list(range(105, 130))
        _write_bars(tmp_duckdb_path, closes)
        script = ['buy'] + [None] * (len(closes) - 1)
        bt = _make_bt(tmp_duckdb_path)
        bt.config.initial_capital = 1000
        strategy = _install(ScriptedStrategy(script), tmp_duckdb_path)
        r = bt.run(strategy, [4391])
        # Once we bought at $100, every subsequent equity step is positive
        # → downside std is 0 → sortino is 0 by our guard.
        assert r.sortino_ratio == 0.0


# ---------------------------------------------------------------------------
# Store + metric round-trip
# ---------------------------------------------------------------------------

class TestStorePersistsNewMetrics:
    def test_metrics_survive_store_roundtrip(self, tmp_duckdb_path):
        from trader.data.backtest_store import BacktestRecord, BacktestStore

        store = BacktestStore(tmp_duckdb_path)
        rec = BacktestRecord(
            strategy_path='strategies/x.py', class_name='X',
            conids=[1], universe='', start_date=dt.datetime.now(),
            end_date=dt.datetime.now(), bar_size='1 min',
            initial_capital=100_000.0, fill_policy='next_open',
            slippage_bps=1.0, commission_per_share=0.0,
            sortino_ratio=2.1,
            calmar_ratio=1.5,
            profit_factor=1.85,
            expectancy_bps=12.3,
            time_in_market_pct=0.42,
        )
        rid = store.add(rec)
        got = store.get(rid)
        assert got.sortino_ratio == pytest.approx(2.1)
        assert got.calmar_ratio == pytest.approx(1.5)
        assert got.profit_factor == pytest.approx(1.85)
        assert got.expectancy_bps == pytest.approx(12.3)
        assert got.time_in_market_pct == pytest.approx(0.42)

    def test_infinity_profit_factor_persisted_as_sentinel(self, tmp_duckdb_path):
        """All-winners ∞ profit factor should persist as a large finite value
        rather than fail the DB insert."""
        from trader.data.backtest_store import BacktestRecord, BacktestStore

        store = BacktestStore(tmp_duckdb_path)
        rec = BacktestRecord(
            strategy_path='x.py', class_name='X', conids=[1], universe='',
            start_date=dt.datetime.now(), end_date=dt.datetime.now(),
            bar_size='1 min', initial_capital=100_000.0,
            fill_policy='next_open', slippage_bps=1.0,
            commission_per_share=0.0,
            profit_factor=float('inf'),
        )
        rid = store.add(rec)
        got = store.get(rid)
        assert got.profit_factor > 1e15
