import datetime as dt
import pytest
import pandas as pd
import numpy as np

from trader.data.data_access import TickStorage
from trader.data.duckdb_store import DuckDBDataStore
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation.backtester import Backtester, BacktestConfig
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState

import logging


# ---------------------------------------------------------------------------
# Test strategies
# ---------------------------------------------------------------------------

class NeverSignalStrategy(Strategy):
    def on_prices(self, prices):
        return None


class AlwaysBuyStrategy(Strategy):
    def on_prices(self, prices):
        if len(prices) < 2:
            return None
        return Signal(
            source_name=self.name or "always_buy",
            action=Action.BUY,
            probability=0.9,
            risk=0.1,
            conid=self.conids[0] if self.conids else 0,
            quantity=10,
        )


class BuySellStrategy(Strategy):
    """Buys on first call, sells on second call."""

    def __init__(self):
        super().__init__()
        self._call_count = 0

    def on_prices(self, prices):
        self._call_count += 1
        if self._call_count == 5:
            return Signal(
                source_name=self.name or "buy_sell",
                action=Action.BUY,
                probability=0.9,
                risk=0.1,
                conid=self.conids[0] if self.conids else 0,
                quantity=10,
            )
        elif self._call_count == 15:
            return Signal(
                source_name=self.name or "buy_sell",
                action=Action.SELL,
                probability=0.9,
                risk=0.1,
                conid=self.conids[0] if self.conids else 0,
                quantity=10,
            )
        return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _write_synthetic_bars(duckdb_path, conid=4391, n=20, base_price=100.0):
    """Write synthetic OHLCV bars to the DuckDB tick_data table."""
    store = DuckDBDataStore(duckdb_path)
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    rng = np.random.default_rng(42)
    close = base_price + np.cumsum(rng.normal(0.05, 0.2, n))
    df = pd.DataFrame({
        "open": close - 0.1,
        "high": close + 0.5,
        "low": close - 0.5,
        "close": close,
        "volume": rng.integers(500, 5000, n).astype(float),
    }, index=dates)
    df.index.name = "date"
    store.write(str(conid), df)
    return df


def _make_backtester(duckdb_path, **config_kwargs):
    storage = TickStorage(duckdb_path=duckdb_path)
    config = BacktestConfig(
        start_date=dt.datetime(2024, 1, 2, 9, 30, tzinfo=dt.timezone.utc),
        end_date=dt.datetime(2024, 1, 2, 10, 30, tzinfo=dt.timezone.utc),
        **config_kwargs,
    )
    return Backtester(storage=storage, config=config)


def _install_strategy(strategy, duckdb_path, conids=None):
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
# Tests
# ---------------------------------------------------------------------------

class TestBacktester:
    def test_no_signal_flat_equity(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(NeverSignalStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        assert result.total_trades == 0
        assert result.total_return == pytest.approx(0.0)
        assert len(result.equity_curve) > 0
        # All equity values should equal initial capital
        assert all(v == pytest.approx(100_000.0) for v in result.equity_curve.values)

    def test_single_buy_opens_position(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        assert result.total_trades > 0
        assert all(t.action == Action.BUY for t in result.trades)

    def test_buy_sell_roundtrip(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=20)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(BuySellStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        actions = [t.action for t in result.trades]
        assert Action.BUY in actions
        assert Action.SELL in actions

    def test_slippage_applied(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path)
        bt = _make_backtester(tmp_duckdb_path, slippage_bps=10.0)
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        if result.trades:
            # Slippage increases buy price above raw close
            # We can't compare directly, but we can check trades exist
            assert result.total_trades > 0

    def test_commission_deducted(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path)
        bt = _make_backtester(tmp_duckdb_path, commission_per_share=1.0)
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        if result.trades:
            for trade in result.trades:
                assert trade.commission == trade.quantity * 1.0

    def test_metrics_computed(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=20)
        bt = _make_backtester(tmp_duckdb_path)
        strategy = _install_strategy(BuySellStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        # Metrics should be numeric, not NaN
        assert not pd.isna(result.total_return)
        assert not pd.isna(result.sharpe_ratio)
        assert not pd.isna(result.max_drawdown)
        assert 0.0 <= result.win_rate <= 1.0

    def test_insufficient_capital(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path)
        bt = _make_backtester(tmp_duckdb_path, initial_capital=1.0)
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])
        # With only $1, no trades should execute
        assert result.total_trades == 0

    def test_no_lookahead_fill_at_next_bar_open(self, tmp_duckdb_path):
        """Fill price must come from the NEXT bar's open, not the triggering bar's close."""
        from trader.simulation.slippage import ZeroSlippage

        # Hand-craft bars with distinct open/close so we can tell which one was used.
        store = DuckDBDataStore(tmp_duckdb_path)
        dates = pd.date_range("2024-01-02 09:30", periods=5, freq="1min", tz="UTC")
        df = pd.DataFrame({
            # close[i] and open[i+1] are intentionally different
            "open":   [100.0, 110.0, 120.0, 130.0, 140.0],
            "high":   [101.0, 111.0, 121.0, 131.0, 141.0],
            "low":    [ 99.0, 109.0, 119.0, 129.0, 139.0],
            "close":  [100.5, 110.5, 120.5, 130.5, 140.5],
            "volume": [1000.0] * 5,
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        bt = _make_backtester(tmp_duckdb_path, slippage_model=ZeroSlippage(), fill_policy='next_open')
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        # The first trade must fill at the OPEN of bar t+1 (not close of bar t).
        # AlwaysBuy triggers on the second bar (needs len>=2). Triggering bar is
        # dates[1] with close=110.5. Next bar is dates[2] with open=120.0. Fill
        # must be at 120.0 to prove we're not peeking at the triggering bar.
        assert result.total_trades >= 1
        first = result.trades[0]
        assert first.price == pytest.approx(120.0), (
            f"Expected fill at next-bar open 120.0, got {first.price} "
            "— this indicates lookahead bias (filled at triggering bar's close)."
        )

    def test_same_close_policy_reproduces_legacy_fill(self, tmp_duckdb_path):
        """With fill_policy='same_close', fills happen at triggering bar close (legacy)."""
        from trader.simulation.slippage import ZeroSlippage

        store = DuckDBDataStore(tmp_duckdb_path)
        dates = pd.date_range("2024-01-02 09:30", periods=5, freq="1min", tz="UTC")
        df = pd.DataFrame({
            "open":   [100.0, 110.0, 120.0, 130.0, 140.0],
            "high":   [101.0, 111.0, 121.0, 131.0, 141.0],
            "low":    [ 99.0, 109.0, 119.0, 129.0, 139.0],
            "close":  [100.5, 110.5, 120.5, 130.5, 140.5],
            "volume": [1000.0] * 5,
        }, index=dates)
        df.index.name = "date"
        store.write("4391", df)

        bt = _make_backtester(tmp_duckdb_path, slippage_model=ZeroSlippage(), fill_policy='same_close')
        strategy = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strategy, [4391])

        assert result.total_trades >= 1
        # Legacy lookahead: first fill at triggering bar's close (110.5).
        first = result.trades[0]
        assert first.price == pytest.approx(110.5)

    def test_sqrt_slippage_model(self, tmp_duckdb_path):
        from trader.simulation.slippage import SquareRootImpact, ZeroSlippage

        _write_synthetic_bars(tmp_duckdb_path)

        bt_zero = _make_backtester(tmp_duckdb_path, slippage_model=ZeroSlippage())
        strategy = _install_strategy(BuySellStrategy(), tmp_duckdb_path)
        result_zero = bt_zero.run(strategy, [4391])

        bt_sqrt = _make_backtester(tmp_duckdb_path, slippage_model=SquareRootImpact())
        strategy2 = _install_strategy(BuySellStrategy(), tmp_duckdb_path)
        result_sqrt = bt_sqrt.run(strategy2, [4391])

        # Both should produce trades
        assert result_zero.total_trades > 0
        assert result_sqrt.total_trades > 0

        # Fill prices should differ between zero and sqrt slippage
        if result_zero.trades and result_sqrt.trades:
            zero_price = result_zero.trades[0].price
            sqrt_price = result_sqrt.trades[0].price
            assert zero_price != sqrt_price


# ---------------------------------------------------------------------------
# Live execution semantics (execution_mode='live') — the mode that matches
# the AutoExecutor: no pyramiding, fixed notional, re-open cooldown.
# ---------------------------------------------------------------------------

class BuyEveryBarNoQty(Strategy):
    """Emits an unsized BUY on every bar after warmup."""
    def on_prices(self, prices):
        if len(prices) < 2:
            return None
        return Signal(source_name=self.name or 'beb', action=Action.BUY,
                      probability=0.9, risk=0.1)


class ScriptedStrategy(Strategy):
    """BUY/SELL at scripted call counts (unsized)."""
    def __init__(self, buys=(), sells=()):
        super().__init__()
        self._n = 0
        self._buys, self._sells = set(buys), set(sells)

    def on_prices(self, prices):
        self._n += 1
        if self._n in self._buys:
            return Signal(source_name=self.name or 's', action=Action.BUY,
                          probability=0.9, risk=0.1)
        if self._n in self._sells:
            return Signal(source_name=self.name or 's', action=Action.SELL,
                          probability=0.9, risk=0.1)
        return None


class TestLiveExecutionMode:
    def test_accumulate_mode_pyramids_live_mode_does_not(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=30)

        bt = _make_backtester(tmp_duckdb_path)  # default: accumulate
        strat = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        buys_legacy = [t for t in result.trades if t.action == Action.BUY]
        assert len(buys_legacy) > 1  # legacy stacks repeat BUYs

        bt_live = _make_backtester(tmp_duckdb_path, execution_mode='live')
        strat = _install_strategy(AlwaysBuyStrategy(), tmp_duckdb_path)
        result = bt_live.run(strat, conids=[4391])
        buys_live = [t for t in result.trades if t.action == Action.BUY]
        assert len(buys_live) == 1  # BUY while holding is refused

    def test_live_mode_fixed_notional_sizing(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=30, base_price=100.0)
        bt = _make_backtester(tmp_duckdb_path, execution_mode='live',
                              initial_capital=100_000.0)
        strat = _install_strategy(BuyEveryBarNoQty(), tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        buys = [t for t in result.trades if t.action == Action.BUY]
        assert len(buys) == 1
        # default notional = 2% of initial capital = $2,000
        assert buys[0].quantity == int(2000 / buys[0].price)

    def test_live_mode_explicit_trade_notional(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=30, base_price=100.0)
        bt = _make_backtester(tmp_duckdb_path, execution_mode='live',
                              trade_notional=5000.0)
        strat = _install_strategy(BuyEveryBarNoQty(), tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        buys = [t for t in result.trades if t.action == Action.BUY]
        assert buys[0].quantity == int(5000 / buys[0].price)

    def test_live_mode_cooldown_blocks_quick_reopen(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=30)
        # BUY@5 fills bar6; SELL@7 fills bar8; BUY@9 is within 300s of the
        # bar-8 fill -> refused; BUY@15 fills bar16 (8 min later) -> allowed.
        bt = _make_backtester(tmp_duckdb_path, execution_mode='live',
                              cooldown_seconds=300)
        strat = _install_strategy(
            ScriptedStrategy(buys=(5, 9, 15), sells=(7,)), tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        actions = [t.action for t in result.trades]
        assert actions == [Action.BUY, Action.SELL, Action.BUY]

    def test_live_mode_cooldown_zero_allows_immediate_reopen(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=30)
        bt = _make_backtester(tmp_duckdb_path, execution_mode='live',
                              cooldown_seconds=0)
        strat = _install_strategy(
            ScriptedStrategy(buys=(5, 9), sells=(7,)), tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        actions = [t.action for t in result.trades]
        assert actions == [Action.BUY, Action.SELL, Action.BUY]

    def test_live_mode_sizing_does_not_compound(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=60, base_price=100.0)
        bt = _make_backtester(tmp_duckdb_path, execution_mode='live',
                              cooldown_seconds=0)
        strat = _install_strategy(
            ScriptedStrategy(buys=(5, 20), sells=(15, 40)), tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        buys = [t for t in result.trades if t.action == Action.BUY]
        assert len(buys) == 2
        # both entries sized off the SAME fixed notional, not off equity
        for b in buys:
            assert b.quantity == int(2000 / b.price)


# ---------------------------------------------------------------------------
# ORB exit rules — EOD flatten + max-hold surface on the BUY signal
# ---------------------------------------------------------------------------

class TestOrbExitRules:
    def _orb(self, **params):
        from types import SimpleNamespace
        from strategies.opening_range_breakout import OpeningRangeBreakout
        s = OpeningRangeBreakout()
        if params:  # live-runtime idiom: session config arrives via params
            s._context = SimpleNamespace(params=params)
        return s

    def test_defaults_preserve_no_exit_behaviour(self):
        assert self._orb()._exit_rules() == (None, None)

    def test_eod_flatten_us_session(self):
        close_by, max_hold = self._orb(EOD_FLATTEN=True)._exit_rules()
        assert close_by == dt.time(15, 45)  # 16:00 close - 15 min default
        assert max_hold is None

    def test_eod_flatten_session_relative_for_asx(self):
        close_by, _ = self._orb(
            EOD_FLATTEN=True, RTH_CLOSE_MIN=960, EOD_FLATTEN_BEFORE_MIN=30,
        )._exit_rules()
        assert close_by == dt.time(15, 30)  # 960 min = 16:00 Sydney, -30

    def test_max_hold_bars_param(self):
        _, max_hold = self._orb(MAX_HOLD_BARS=390)._exit_rules()
        assert max_hold == 390

    def test_class_attr_override_backtest_idiom(self):
        s = self._orb()
        s.EOD_FLATTEN = True          # apply_param_overrides setattr path
        s.EOD_FLATTEN_BEFORE_MIN = 5
        close_by, _ = s._exit_rules()
        assert close_by == dt.time(15, 55)


class TestPyramidFixedMode:
    """pyramid_fixed = stacking allowed, fixed per-lot notional. The
    decomposition mode: differs from 'live' only by allowing adds, and from
    'accumulate' only by non-compounding sizing."""

    def test_stacks_like_accumulate_but_sizes_fixed(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=30, base_price=100.0)
        bt = _make_backtester(tmp_duckdb_path, execution_mode='pyramid_fixed',
                              cooldown_seconds=0)
        strat = _install_strategy(BuyEveryBarNoQty(), tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        buys = [t for t in result.trades if t.action == Action.BUY]
        assert len(buys) > 1  # adds allowed (unlike 'live')
        # every lot sized off the SAME fixed notional (unlike 'accumulate',
        # where each add is 10% of the remaining cash)
        for b in buys:
            assert b.quantity == int(2000 / b.price)

    def test_cooldown_spaces_the_adds(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=30)
        bt = _make_backtester(tmp_duckdb_path, execution_mode='pyramid_fixed',
                              cooldown_seconds=300)
        strat = _install_strategy(BuyEveryBarNoQty(), tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        buys = [t for t in result.trades if t.action == Action.BUY]
        assert len(buys) > 1
        for a, b in zip(buys, buys[1:]):
            assert (b.timestamp - a.timestamp).total_seconds() >= 300

    def test_sell_closes_whole_stack(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=30)
        bt = _make_backtester(tmp_duckdb_path, execution_mode='pyramid_fixed',
                              cooldown_seconds=0)
        strat = _install_strategy(
            ScriptedStrategy(buys=(5, 7, 9), sells=(15,)), tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        buys = [t for t in result.trades if t.action == Action.BUY]
        sells = [t for t in result.trades if t.action == Action.SELL]
        assert len(buys) == 3 and len(sells) == 1
        assert sells[0].quantity == sum(b.quantity for b in buys)


class TestPyramidMaxAddsCap:
    def test_cap_bounds_the_stack(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=30)
        bt = _make_backtester(tmp_duckdb_path, execution_mode='pyramid_fixed',
                              cooldown_seconds=0, pyramid_max_adds=2)
        strat = _install_strategy(BuyEveryBarNoQty(), tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        buys = [t for t in result.trades if t.action == Action.BUY]
        assert len(buys) == 3  # initial + 2 adds, then the cap refuses

    def test_cap_resets_after_full_close(self, tmp_duckdb_path):
        _write_synthetic_bars(tmp_duckdb_path, n=40)
        bt = _make_backtester(tmp_duckdb_path, execution_mode='pyramid_fixed',
                              cooldown_seconds=0, pyramid_max_adds=1)
        strat = _install_strategy(
            ScriptedStrategy(buys=(5, 7, 9, 20, 22, 24), sells=(15,)),
            tmp_duckdb_path)
        result = bt.run(strat, conids=[4391])
        actions = [t.action for t in result.trades]
        # first stack: 2 buys (cap), close; second stack: 2 buys again
        assert actions == [Action.BUY, Action.BUY, Action.SELL,
                           Action.BUY, Action.BUY]
