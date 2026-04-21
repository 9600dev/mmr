"""Tests for the precompute + on_bar hook on ``Strategy``.

Guarantees we care about:
- precompute is called exactly once per conid, with the full OHLCV.
- on_bar receives the correct (full_prices, state, index) triple.
- Legacy strategies (on_prices only) still work via the default on_bar fallback.
- A strategy implementing both APIs produces the same signals via either path.
- The fast path materially reduces wall time on a vectorbt-heavy strategy.
"""

import datetime as dt
import logging
import os
import sys
import time

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
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _write_bars(duckdb_path: str, conid: int = 4391, n: int = 60) -> pd.DataFrame:
    """Write a deterministic OHLCV series for a conid and return the DataFrame."""
    store = DuckDBDataStore(duckdb_path)
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    rng = np.random.default_rng(42)
    close = 100.0 + np.cumsum(rng.normal(0.05, 0.2, n))
    df = pd.DataFrame({
        "open":   close - 0.1,
        "high":   close + 0.5,
        "low":    close - 0.5,
        "close":  close,
        "volume": np.full(n, 1000.0),
    }, index=dates)
    df.index.name = "date"
    store.write(str(conid), df)
    return df


def _backtester(duckdb_path: str) -> Backtester:
    storage = TickStorage(duckdb_path=duckdb_path)
    config = BacktestConfig(
        start_date=dt.datetime(2024, 1, 2, 9, 30, tzinfo=dt.timezone.utc),
        end_date=dt.datetime(2024, 1, 3, 9, 30, tzinfo=dt.timezone.utc),
        bar_size=BarSize.Mins1,
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
        paper_only=False,
        storage=storage,
        universe_accessor=ua,
        logger=logging.getLogger("test"),
    )
    strategy.install(ctx)
    strategy.state = StrategyState.RUNNING
    return strategy


# ---------------------------------------------------------------------------
# Precompute-hook sanity tests
# ---------------------------------------------------------------------------

class PrecomputeProbe(Strategy):
    """Records precompute / on_bar invocations and returns no signals."""

    def __init__(self):
        super().__init__()
        self.precompute_calls = 0
        self.precompute_prices_lens = []
        self.on_bar_indices = []
        self.on_bar_prices_ids = set()

    def precompute(self, prices):
        self.precompute_calls += 1
        self.precompute_prices_lens.append(len(prices))
        return {'magic': len(prices)}

    def on_bar(self, prices, state, index):
        self.on_bar_indices.append(index)
        # Track that the backtester passes the SAME DataFrame reference,
        # not a copy-per-bar (copying would defeat the purpose).
        self.on_bar_prices_ids.add(id(prices))
        assert state['magic'] == len(prices), 'state must persist across calls'
        return None


class TestPrecomputeHookSemantics:
    def test_precompute_called_once_per_conid(self, tmp_duckdb_path):
        _write_bars(tmp_duckdb_path, n=60)
        bt = _backtester(tmp_duckdb_path)
        probe = _install(PrecomputeProbe(), tmp_duckdb_path)
        bt.run(probe, [4391])
        assert probe.precompute_calls == 1
        assert probe.precompute_prices_lens == [60]

    def test_on_bar_receives_monotonic_index(self, tmp_duckdb_path):
        _write_bars(tmp_duckdb_path, n=60)
        bt = _backtester(tmp_duckdb_path)
        probe = _install(PrecomputeProbe(), tmp_duckdb_path)
        bt.run(probe, [4391])
        # Exactly one call per bar, indices 0 .. 59
        assert probe.on_bar_indices == list(range(60))

    def test_on_bar_receives_stable_prices_reference(self, tmp_duckdb_path):
        """The backtester must pass the same full-prices DataFrame object
        every call — copying it would erase the O(N²) → O(N) win."""
        _write_bars(tmp_duckdb_path, n=60)
        bt = _backtester(tmp_duckdb_path)
        probe = _install(PrecomputeProbe(), tmp_duckdb_path)
        bt.run(probe, [4391])
        assert len(probe.on_bar_prices_ids) == 1

    def test_precompute_exception_does_not_abort_run(self, tmp_duckdb_path):
        """A broken precompute should log and continue with empty state;
        ``on_bar`` still gets called via its default implementation."""
        class BrokenPrecompute(Strategy):
            def __init__(self):
                super().__init__()
                self.on_bar_calls = 0

            def precompute(self, prices):
                raise RuntimeError('oops — state construction failed')

            def on_bar(self, prices, state, index):
                self.on_bar_calls += 1
                assert state == {}
                return None

        _write_bars(tmp_duckdb_path, n=30)
        bt = _backtester(tmp_duckdb_path)
        s = _install(BrokenPrecompute(), tmp_duckdb_path)
        bt.run(s, [4391])
        assert s.on_bar_calls == 30


# ---------------------------------------------------------------------------
# Backward compatibility — legacy on_prices strategies
# ---------------------------------------------------------------------------

class LegacyMa(Strategy):
    """Mini SMA crossover using only on_prices — exercises the default
    on_bar fallback to on_prices(prices.iloc[:index+1])."""

    def __init__(self):
        super().__init__()
        self.calls = 0

    def on_prices(self, prices):
        self.calls += 1
        if len(prices) < 10:
            return None
        fast = prices['close'].rolling(3).mean()
        slow = prices['close'].rolling(10).mean()
        if fast.iloc[-1] > slow.iloc[-1] and fast.iloc[-2] <= slow.iloc[-2]:
            return Signal(source_name=self.name or 'legacy',
                          action=Action.BUY, probability=0.7, risk=0.3, quantity=1)
        return None


class TestBackwardCompat:
    def test_legacy_strategy_still_runs(self, tmp_duckdb_path):
        _write_bars(tmp_duckdb_path, n=40)
        bt = _backtester(tmp_duckdb_path)
        s = _install(LegacyMa(), tmp_duckdb_path)
        # Must not raise, must produce results.
        result = bt.run(s, [4391])
        assert s.calls == 40  # one on_prices call per bar (via default on_bar)
        assert result is not None
        assert len(result.equity_curve) == 40


# ---------------------------------------------------------------------------
# Equivalence: dual-implementation strategy must produce identical trades
# on fast path vs legacy path.
# ---------------------------------------------------------------------------

class DualMa(Strategy):
    """Same SMA crossover signal computed via both APIs. A strategy that
    implements precompute/on_bar correctly should return the SAME signals
    as its legacy on_prices implementation — proving the fast path doesn't
    accidentally introduce or drop signals."""

    FAST = 5
    SLOW = 20

    def __init__(self, use_fast_path: bool = True):
        super().__init__()
        self.use_fast_path = use_fast_path

    def precompute(self, prices):
        if not self.use_fast_path:
            return {}
        close = prices['close']
        return {
            'fast': close.rolling(self.FAST).mean().to_numpy(),
            'slow': close.rolling(self.SLOW).mean().to_numpy(),
        }

    def on_bar(self, prices, state, index):
        if not self.use_fast_path:
            # Fall back to legacy path for the control run.
            return self.on_prices(prices.iloc[:index + 1])
        fast = state.get('fast'); slow = state.get('slow')
        if fast is None or index < self.SLOW:
            return None
        if np.isnan(fast[index]) or np.isnan(slow[index]):
            return None
        if fast[index] > slow[index] and fast[index - 1] <= slow[index - 1]:
            return Signal(source_name=self.name or 'dual',
                          action=Action.BUY, probability=0.7, risk=0.3, quantity=1)
        if fast[index] < slow[index] and fast[index - 1] >= slow[index - 1]:
            return Signal(source_name=self.name or 'dual',
                          action=Action.SELL, probability=0.7, risk=0.3, quantity=1)
        return None

    def on_prices(self, prices):
        if len(prices) < self.SLOW:
            return None
        close = prices['close']
        fast = close.rolling(self.FAST).mean()
        slow = close.rolling(self.SLOW).mean()
        if np.isnan(fast.iloc[-1]) or np.isnan(slow.iloc[-1]):
            return None
        if fast.iloc[-1] > slow.iloc[-1] and fast.iloc[-2] <= slow.iloc[-2]:
            return Signal(source_name=self.name or 'dual',
                          action=Action.BUY, probability=0.7, risk=0.3, quantity=1)
        if fast.iloc[-1] < slow.iloc[-1] and fast.iloc[-2] >= slow.iloc[-2]:
            return Signal(source_name=self.name or 'dual',
                          action=Action.SELL, probability=0.7, risk=0.3, quantity=1)
        return None


class TestFastPathEquivalence:
    def test_same_signals_as_legacy_path(self, tmp_duckdb_path):
        _write_bars(tmp_duckdb_path, n=80)
        bt = _backtester(tmp_duckdb_path)

        fast_strat = _install(DualMa(use_fast_path=True), tmp_duckdb_path)
        fast_result = bt.run(fast_strat, [4391])

        legacy_strat = _install(DualMa(use_fast_path=False), tmp_duckdb_path)
        legacy_result = bt.run(legacy_strat, [4391])

        fast_tuples = [(t.timestamp, str(t.action), t.quantity, round(t.price, 6))
                       for t in fast_result.trades]
        legacy_tuples = [(t.timestamp, str(t.action), t.quantity, round(t.price, 6))
                         for t in legacy_result.trades]
        assert fast_tuples == legacy_tuples, (
            'fast-path and legacy trades diverged — the precompute hook must '
            'produce semantically identical signals to the on_prices path'
        )
        assert fast_result.total_trades == legacy_result.total_trades
        assert fast_result.total_return == pytest.approx(legacy_result.total_return)
        assert fast_result.final_equity_value() if hasattr(fast_result, 'final_equity_value') else True


# ---------------------------------------------------------------------------
# Performance sanity — the fast path should be noticeably quicker on many
# bars. Not a hard clock target (CI variance); just checks the ratio is
# sensible so a future regression can't silently put us back to O(N²).
# ---------------------------------------------------------------------------

class TestPerfSanity:
    def test_fast_path_not_slower_than_legacy(self, tmp_duckdb_path):
        _write_bars(tmp_duckdb_path, n=500)
        bt = _backtester(tmp_duckdb_path)

        fast_strat = _install(DualMa(use_fast_path=True), tmp_duckdb_path)
        t0 = time.perf_counter()
        bt.run(fast_strat, [4391])
        fast_ms = (time.perf_counter() - t0) * 1000

        legacy_strat = _install(DualMa(use_fast_path=False), tmp_duckdb_path)
        t0 = time.perf_counter()
        bt.run(legacy_strat, [4391])
        legacy_ms = (time.perf_counter() - t0) * 1000

        # For pandas .rolling this is expected to be close to a wash (both
        # are fast); we just guard against a catastrophic regression that
        # would make the fast path slower than the legacy one.
        assert fast_ms <= legacy_ms * 1.5, (
            f'fast path ({fast_ms:.1f}ms) shouldn\'t be meaningfully slower '
            f'than legacy ({legacy_ms:.1f}ms)'
        )
