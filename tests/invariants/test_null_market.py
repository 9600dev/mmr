"""SPEC: the negative control's null must actually be null.

`scripts/negative_control.py` measures what the backtest pipeline reports on
data with no edge in it, and uses that as the zero every real candidate has to
clear. The entire argument rests on one claim: that `driftless_session_bars`
contains nothing to find.

If the generator accidentally carried drift, momentum, or mean reversion, the
control would report a "manufactured" Sharpe that was partly real, the zero
would be set too high, and genuine strategies would be rejected against a
rigged benchmark. If it carried structurally impossible bars, it would not be
measuring the same pipeline the real data goes through.

Neither failure announces itself — a control that is quietly wrong looks
exactly like one that is right. So the null is asserted here rather than
assumed, and the properties are the ones an exploitable pattern would have to
violate.
"""

from __future__ import annotations

import numpy as np
import pytest

from trader.data.bar_quality import impossible_mask
from trader.simulation.synthetic_markets import driftless_session_bars


def _log_returns(df):
    return np.diff(np.log(df['close'].to_numpy(dtype=float)))


class TestThereIsNothingToFind:
    """Each property is one way a strategy could make money here. All must
    fail to hold, or the control is not a control."""

    def test_no_drift(self):
        """Drift is free money for anything long-biased. Averaged over 12
        independent instruments so a single lucky walk cannot mask it; the
        bound is the standard error of the mean, not an arbitrary epsilon."""
        means, ns = [], []
        for seed in range(12):
            r = _log_returns(driftless_session_bars(days=60, seed=seed))
            means.append(r.mean())
            ns.append(len(r))
        pooled = float(np.mean(means))
        se = float(np.mean([np.std(_log_returns(
            driftless_session_bars(days=60, seed=s))) for s in range(3)])) \
            / np.sqrt(np.mean(ns) * 12)
        assert abs(pooled) < 4 * se, (
            f'per-minute drift {pooled:.3e} exceeds 4 standard errors '
            f'({4 * se:.3e}) — the null carries a directional edge')

    @pytest.mark.parametrize('seed', [0, 5, 11])
    def test_no_serial_correlation(self, seed):
        """Momentum and mean reversion both show up here. A breakout strategy
        needs positive lag-1 correlation to work; a fade needs negative."""
        r = _log_returns(driftless_session_bars(days=120, seed=seed))
        rho = float(np.corrcoef(r[:-1], r[1:])[0, 1])
        assert abs(rho) < 0.02, f'lag-1 autocorrelation {rho:+.4f} is exploitable'

    @pytest.mark.parametrize('seed', [1, 7])
    def test_no_longer_horizon_predictability(self, seed):
        """Lag-1 alone is not enough: a slow trend shows up at longer lags
        while leaving consecutive minutes uncorrelated."""
        r = _log_returns(driftless_session_bars(days=120, seed=seed))
        for lag in (5, 15, 30, 60):
            rho = float(np.corrcoef(r[:-lag], r[lag:])[0, 1])
            assert abs(rho) < 0.03, f'lag-{lag} autocorrelation {rho:+.4f}'

    def test_instruments_are_independent(self):
        """A shared factor across instruments would let the sweep's
        symbol axis find something real. Each seed must be its own walk."""
        a = _log_returns(driftless_session_bars(days=60, seed=3))
        b = _log_returns(driftless_session_bars(days=60, seed=4))
        n = min(len(a), len(b))
        rho = float(np.corrcoef(a[:n], b[:n])[0, 1])
        assert abs(rho) < 0.05, f'instruments correlate at {rho:+.4f}'


class TestItStillLooksLikeAMarket:
    """The data must be realistic enough that the strategies RUN. A control
    the strategy cannot trade measures nothing — an opening-range breakout
    needs an opening range to exist."""

    def test_bars_are_structurally_possible(self):
        """It goes through the same write chokepoint as real data. Bars the
        quality gate would refuse would never reach a backtest, so a generator
        producing them would silently shrink the control."""
        df = driftless_session_bars(days=30, seed=2)
        assert int(impossible_mask(df).sum()) == 0

    def test_sessions_have_the_shape_of_a_trading_day(self):
        df = driftless_session_bars(days=10, seed=1)
        by_day = df.groupby(df.index.date).size()
        assert set(by_day) == {390}, f'session lengths: {sorted(set(by_day))}'
        assert df.index.tz is not None
        minutes = df.index.hour * 60 + df.index.minute
        assert minutes.min() == 9 * 60 + 30
        assert minutes.max() == 16 * 60 - 1

    def test_no_weekends(self):
        df = driftless_session_bars(days=20, seed=1)
        assert set(d.weekday() for d in df.index) <= {0, 1, 2, 3, 4}

    def test_volatility_is_in_the_right_ballpark(self):
        """Calibrated to a liquid large-cap. Wildly wrong vol would make the
        control easy or impossible to trade rather than merely edge-free."""
        r = _log_returns(driftless_session_bars(days=120, seed=1))
        annualised = float(r.std() * np.sqrt(252 * 390))
        assert 0.2 < annualised < 0.5, f'annualised vol {annualised:.1%}'


class TestReproducibility:
    def test_same_seed_same_data(self):
        """A control that moves between runs cannot be a baseline — the number
        it produces would not be comparable to the one you recorded."""
        a = driftless_session_bars(days=15, seed=42)
        b = driftless_session_bars(days=15, seed=42)
        assert a.equals(b)

    def test_different_seeds_differ(self):
        a = driftless_session_bars(days=15, seed=42)
        b = driftless_session_bars(days=15, seed=43)
        assert not a['close'].equals(b['close'])
