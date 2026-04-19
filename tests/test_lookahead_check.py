"""Tests for ``assert_no_lookahead``.

Three things to prove:

1. The detector catches canonical lookahead bugs (full-series normalization,
   ``shift(-N)``, centered rolling).
2. The detector does NOT false-positive on clean strategies that use
   backward-looking indicators with warmup NaNs.
3. All currently-migrated strategies (SMICrossOver, VbtMacdBB) pass the
   audit.
"""

import numpy as np
import pandas as pd
import pytest

from trader.simulation.lookahead_check import (
    LookaheadDetected,
    assert_no_lookahead,
)
from trader.trading.strategy import Strategy


# ---------------------------------------------------------------------------
# Fixtures: deterministic synthetic OHLCV
# ---------------------------------------------------------------------------

def _ohlcv(n: int = 200, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100.0 + np.cumsum(rng.normal(0.0, 0.5, n))
    idx = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "open":   close - 0.05,
        "high":   close + 0.3,
        "low":    close - 0.3,
        "close":  close,
        "volume": rng.integers(500, 5000, n).astype(float),
    }, index=idx)
    df.index.name = "date"
    return df


# ---------------------------------------------------------------------------
# Clean strategies — detector must NOT false-positive
# ---------------------------------------------------------------------------

class CleanRolling(Strategy):
    """Past-only rolling indicators. Must pass the audit cleanly."""

    def precompute(self, prices):
        close = prices['close']
        return {
            'sma20': close.rolling(20).mean().to_numpy(),
            'ema10': close.ewm(span=10, adjust=False).mean().to_numpy(),
            'ret':   close.pct_change().to_numpy(),
        }


class CleanExpanding(Strategy):
    """Expanding window is fine — each point uses only up-to-current data."""

    def precompute(self, prices):
        close = prices['close']
        return {
            'cummax': close.expanding().max().to_numpy(),
            'cummin': close.expanding().min().to_numpy(),
        }


# ---------------------------------------------------------------------------
# Buggy strategies — detector MUST catch these
# ---------------------------------------------------------------------------

class FullSeriesZScore(Strategy):
    """Classic ML fit-on-train trap: normalize by the full-series mean and
    std. When you hide the last bar, the mean/std change slightly → past
    z-scores shift → lookahead detected."""

    def precompute(self, prices):
        close = prices['close']
        # This mean/std span the ENTIRE series including bars that don't
        # yet exist at any given point in time. Classic bug.
        z = (close - close.mean()) / close.std()
        return {'zscore': z.to_numpy()}


class ShiftMinusOne(Strategy):
    """Blatant future-lookup."""

    def precompute(self, prices):
        close = prices['close']
        return {
            'tomorrow_close': close.shift(-1).to_numpy(),
        }


class CenteredRolling(Strategy):
    """Centered rolling averages peek half-window into the future."""

    def precompute(self, prices):
        close = prices['close']
        return {
            'sma_centered': close.rolling(20, center=True).mean().to_numpy(),
        }


class GlobalMinMaxScale(Strategy):
    """Normalizing price against the full-series min/max — another popular
    ML preprocessing step that's a classic lookahead vector."""

    def precompute(self, prices):
        close = prices['close']
        lo, hi = close.min(), close.max()
        return {'scaled': ((close - lo) / (hi - lo)).to_numpy()}


# ---------------------------------------------------------------------------
# Positive tests: detector accepts clean strategies
# ---------------------------------------------------------------------------

class TestCleanStrategies:
    def test_rolling_indicators_pass(self):
        """Standard backward-looking rolling/ewm indicators must pass."""
        assert_no_lookahead(CleanRolling(), _ohlcv(200))

    def test_expanding_pass(self):
        """Expanding windows are past-only (position i uses bars [0..i])."""
        assert_no_lookahead(CleanExpanding(), _ohlcv(200))

    def test_empty_precompute_skipped(self):
        """A strategy returning empty state is opted out — detector is a no-op."""
        class NoPrecompute(Strategy):
            pass
        assert_no_lookahead(NoPrecompute(), _ohlcv(100))


# ---------------------------------------------------------------------------
# Negative tests: detector rejects each common lookahead pattern
# ---------------------------------------------------------------------------

class TestDetectsLeaks:
    def test_full_series_zscore_caught(self):
        with pytest.raises(LookaheadDetected) as exc:
            assert_no_lookahead(FullSeriesZScore(), _ohlcv(200))
        assert exc.value.key == 'zscore'
        assert 'future' in str(exc.value).lower() or 'lookahead' in str(exc.value).lower()

    def test_shift_minus_one_caught(self):
        with pytest.raises(LookaheadDetected) as exc:
            assert_no_lookahead(ShiftMinusOne(), _ohlcv(200))
        assert exc.value.key == 'tomorrow_close'

    def test_centered_rolling_caught(self):
        with pytest.raises(LookaheadDetected) as exc:
            assert_no_lookahead(CenteredRolling(), _ohlcv(200))
        assert exc.value.key == 'sma_centered'

    def test_full_series_minmax_caught(self):
        """MinMax scaling leaks only when the truncated tail contained an
        extreme. Use a monotonically increasing series so the last bar is
        the global max — truncation guaranteed to shift the max downward,
        which shifts every past scaled value."""
        n = 200
        idx = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
        close = pd.Series(np.linspace(100.0, 200.0, n), index=idx)
        prices = pd.DataFrame({
            "open": close, "high": close, "low": close, "close": close,
            "volume": 1000.0,
        }, index=idx)
        prices.index.name = "date"
        with pytest.raises(LookaheadDetected) as exc:
            assert_no_lookahead(GlobalMinMaxScale(), prices)
        assert exc.value.key == 'scaled'

    def test_error_message_is_actionable(self):
        """The LookaheadDetected message should name the key + index + both
        values + enough context that the author can find the bug."""
        with pytest.raises(LookaheadDetected) as exc:
            assert_no_lookahead(FullSeriesZScore(), _ohlcv(200))
        msg = str(exc.value)
        assert "'zscore'" in msg
        assert "bar" in msg
        assert "hidden" in msg or "hide" in msg.lower()
        # Common-cause hint should appear so the author knows what to look for
        assert any(tok in msg.lower() for tok in ('normalization', 'centered', 'shift', 'fit-on-full'))


# ---------------------------------------------------------------------------
# Repo strategies: the 2 currently using precompute must pass
# ---------------------------------------------------------------------------

class TestCurrentStrategiesAreClean:
    def test_vbt_macd_bb_no_lookahead(self):
        from strategies.vbt_macd_bb import VbtMacdBB
        # Needs enough bars that MACD + BB warm up
        assert_no_lookahead(VbtMacdBB(), _ohlcv(400))

    def test_smi_crossover_no_lookahead(self):
        from strategies.smi_crossover import SMICrossOver
        # SLOW_WINDOW is 50; give it enough headroom above that
        assert_no_lookahead(SMICrossOver(), _ohlcv(400))
