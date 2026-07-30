"""SPEC: the panel research path must be leak-checked, not trusted.

Every cross-sectional result produced today ran through `precompute_panel`, and
nothing checked it for lookahead. The per-instrument checker cannot be reused:
it passes a single OHLCV frame while the panel hook takes a wide (field, conid)
panel, so it fails on shape before it inspects a value.

The leak that matters most here is subtle and was demonstrated live: anything
normalising across TIME rather than across NAMES. A cross-sectional z-score is
safe - each row uses only that row. A time-series z-score divides by a standard
deviation computed from the whole sample including the future, so the signal
knows how volatile the next five years will be, and it produces a large, stable,
entirely fake IC that looks exactly like a discovery.

These tests build strategies whose leak is known by construction, so the checker
is verified to fire rather than merely to pass.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trader.simulation.lookahead_check import assert_no_panel_lookahead


def _panel(rows=120, names=8, seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.date_range('2024-01-01', periods=rows, freq='D')
    cols = [1000 + i for i in range(names)]
    close = pd.DataFrame(100 * np.exp(np.cumsum(
        rng.normal(0, 0.01, size=(rows, names)), axis=0)), index=idx, columns=cols)
    return pd.concat({'close': close, 'open': close * 0.999,
                      'high': close * 1.01, 'low': close * 0.99,
                      'volume': close * 0 + 1000.0}, axis=1)


class _Clean:
    """Cross-sectional standardisation: each row uses only that row."""
    def precompute_panel(self, panel):
        c = panel['close']
        mom = c.shift(21) / c.shift(63) - 1.0
        z = mom.sub(mom.mean(axis=1), axis=0).div(mom.std(axis=1, ddof=1), axis=0)
        return {'signal': z}


class _TimeSeriesZScore:
    """The live failure mode: standardise down the TIME axis, so the divisor is
    computed from the whole sample including the future."""
    def precompute_panel(self, panel):
        c = panel['close']
        mom = c.shift(21) / c.shift(63) - 1.0
        return {'signal': (mom - mom.mean(axis=0)) / mom.std(axis=0, ddof=1)}


class _ReadsForward:
    def precompute_panel(self, panel):
        return {'signal': panel['close'].shift(-1) / panel['close'] - 1.0}


class _CentredWindow:
    def precompute_panel(self, panel):
        return {'signal': panel['close'].rolling(21, center=True).mean()}


class _KeysDependOnFuture:
    def precompute_panel(self, panel):
        out = {'signal': panel['close'].shift(21)}
        if len(panel) > 115:
            out['extra'] = panel['close'].shift(5)
        return out


class TestTheCheckerAcceptsCleanWork:
    def test_cross_sectional_standardisation_passes(self):
        assert_no_panel_lookahead(_Clean(), _panel())

    def test_a_strategy_that_precomputes_nothing_passes(self):
        class _Empty:
            def precompute_panel(self, panel):
                return {}
        assert_no_panel_lookahead(_Empty(), _panel())


class TestTheCheckerFiresOnKnownLeaks:
    """A checker verified only against clean input is indistinguishable from
    one that always passes."""

    def test_time_axis_standardisation_is_caught(self):
        with pytest.raises(AssertionError, match='LOOKAHEAD'):
            assert_no_panel_lookahead(_TimeSeriesZScore(), _panel())

    def test_reading_the_next_bar_is_caught(self):
        with pytest.raises(AssertionError, match='LOOKAHEAD'):
            assert_no_panel_lookahead(_ReadsForward(), _panel())

    def test_a_centred_window_is_caught(self):
        with pytest.raises(AssertionError, match='LOOKAHEAD'):
            assert_no_panel_lookahead(_CentredWindow(), _panel())

    def test_an_output_set_that_depends_on_the_future_is_caught(self):
        with pytest.raises(AssertionError, match='must not depend'):
            assert_no_panel_lookahead(_KeysDependOnFuture(), _panel())


class TestTheShippedStrategiesAreClean:
    """The strategies today's results came from."""

    @pytest.mark.parametrize('module,cls', [
        ('strategies/xs_momentum.py', 'XsMomentum'),
        ('strategies/xs_composite.py', 'XsComposite'),
    ])
    def test_shipped_panel_strategies_have_no_lookahead(self, module, cls):
        import importlib.util
        import pathlib
        spec = importlib.util.spec_from_file_location(
            f'_lc_{cls}', pathlib.Path(module).resolve())
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        inst = getattr(mod, cls)()
        inst.SECTOR_NEUTRAL = 0          # avoids a DB lookup in the checker
        assert_no_panel_lookahead(inst, _panel(rows=300, names=12))
