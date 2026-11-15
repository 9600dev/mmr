"""Tests for the parameter-override mechanism used by `--param`,
`--params`, and `bt-sweep`.

Covers:
  * Type coercion from CLI strings to int/float/bool.
  * Both strategy idioms — class attributes and ``self.params`` dict.
  * Unknown-key rejection (guards against typos silently no-oping).
  * Effective-params round-trip to ``BacktestResult.applied_params`` so
    the CLI can persist them to the history store.
"""

import pytest

from trader.simulation.backtester import (
    Backtester, _coerce_param, _coerce_loose,
)
from trader.trading.strategy import Strategy, StrategyContext


class _FakeClassAttrStrategy(Strategy):
    """Tunables stored as upper-case class attributes — the new idiom
    used by KeltnerBreakout, VwapReversion, OpeningRangeBreakout, etc."""
    EMA_PERIOD = 20
    BAND_MULT = 2.0
    USE_VOLUME = True


class _FakeParamsDictStrategy(Strategy):
    """Tunables read via ``self.params.get(name, default)`` — the legacy
    idiom used by MaCrossover, Momentum, RSIStrategy, etc."""
    def on_prices(self, prices):
        return None


def _make_installed(cls):
    """Build a strategy with a minimal installed context so lower-case
    params go somewhere writable. Matches what the backtester does."""
    from trader.data.universe import UniverseAccessor
    from trader.objects import BarSize
    import logging as _logging
    inst = cls()
    ctx = StrategyContext(
        name='test', bar_size=BarSize.Mins1, conids=[], universe=None,
        historical_days_prior=0, paper_only=False,
        storage=None,
        universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
        logger=_logging.getLogger('test'),
        params={},
    )
    inst.install(ctx)
    return inst


class TestCoerceParam:
    """Type coercion from whatever the CLI handed us to the class attr type."""

    def test_int_from_string(self):
        assert _coerce_param('15', 20, 'X') == 15
        assert isinstance(_coerce_param('15', 20, 'X'), int)

    def test_float_from_string(self):
        assert _coerce_param('1.5', 2.0, 'X') == 1.5

    def test_bool_from_string(self):
        """bool('False') is True in plain Python — coercion must be
        string-aware or we silently invert negated flags."""
        assert _coerce_param('False', True, 'X') is False
        assert _coerce_param('true', False, 'X') is True
        assert _coerce_param('0', True, 'X') is False
        assert _coerce_param('1', False, 'X') is True

    def test_bool_rejects_garbage(self):
        with pytest.raises(ValueError, match='expects a boolean'):
            _coerce_param('maybe', True, 'X')

    def test_int_rejects_non_numeric(self):
        with pytest.raises(ValueError, match='expects int'):
            _coerce_param('abc', 20, 'EMA_PERIOD')

    def test_float_rejects_non_numeric(self):
        with pytest.raises(ValueError, match='expects float'):
            _coerce_param('abc', 2.0, 'BAND_MULT')

    def test_already_native_passthrough(self):
        assert _coerce_param(15, 20, 'X') == 15
        assert _coerce_param(1.5, 2.0, 'X') == 1.5

    def test_error_message_names_key(self):
        """CLI users need the key name in the error so they know which
        --param was malformed."""
        with pytest.raises(ValueError, match="'EMA_PERIOD'"):
            _coerce_param('abc', 20, 'EMA_PERIOD')


class TestLooseCoerce:
    """Lower-case params.get() keys have no type info — best-effort cast."""

    def test_prefers_int_over_float(self):
        """'10' should become int(10), not float(10.0)."""
        result = _coerce_loose('10')
        assert result == 10
        assert isinstance(result, int)

    def test_float_when_not_int(self):
        assert _coerce_loose('1.5') == 1.5

    def test_bool_strings(self):
        assert _coerce_loose('true') is True
        assert _coerce_loose('False') is False

    def test_passthrough_non_numeric(self):
        assert _coerce_loose('hello') == 'hello'

    def test_typed_passthrough(self):
        """Already-typed values (from JSON --params) pass unchanged."""
        assert _coerce_loose(42) == 42
        assert _coerce_loose([1, 2, 3]) == [1, 2, 3]


class TestApplyOverridesClassAttr:
    """The new idiom — class-level UPPER_CASE constants."""

    def test_override_shadows_class_attr(self):
        inst = _make_installed(_FakeClassAttrStrategy)
        applied = Backtester.apply_param_overrides(
            inst, {'EMA_PERIOD': '15', 'BAND_MULT': '1.5'}
        )
        # Coerced to int/float:
        assert applied == {'EMA_PERIOD': 15, 'BAND_MULT': 1.5}
        assert inst.EMA_PERIOD == 15
        assert inst.BAND_MULT == 1.5

    def test_override_does_not_mutate_class(self):
        """Parallel sweeps must not see each other's overrides via the
        class object. setattr on instance should shadow, not write-through."""
        a = _make_installed(_FakeClassAttrStrategy)
        b = _make_installed(_FakeClassAttrStrategy)
        Backtester.apply_param_overrides(a, {'EMA_PERIOD': 5})
        assert a.EMA_PERIOD == 5
        assert b.EMA_PERIOD == 20
        assert _FakeClassAttrStrategy.EMA_PERIOD == 20

    def test_also_writes_to_context_params(self):
        """Strategies that mix idioms (read both self.EMA_PERIOD AND
        self.params['EMA_PERIOD']) should see the override either way."""
        inst = _make_installed(_FakeClassAttrStrategy)
        Backtester.apply_param_overrides(inst, {'EMA_PERIOD': 15})
        assert inst.params['EMA_PERIOD'] == 15

    def test_unknown_upper_key_raises(self):
        inst = _make_installed(_FakeClassAttrStrategy)
        with pytest.raises(ValueError, match="has no parameter 'NONEXISTENT'"):
            Backtester.apply_param_overrides(inst, {'NONEXISTENT': 5})

    def test_error_message_lists_known_tunables(self):
        """Typos are easy (EMAPERIOD vs EMA_PERIOD). The error should
        surface what the actual knobs are."""
        inst = _make_installed(_FakeClassAttrStrategy)
        with pytest.raises(ValueError, match='EMA_PERIOD'):
            Backtester.apply_param_overrides(inst, {'EMAPERIOD': 5})

    def test_empty_overrides_returns_empty_dict(self):
        inst = _make_installed(_FakeClassAttrStrategy)
        assert Backtester.apply_param_overrides(inst, None) == {}
        assert Backtester.apply_param_overrides(inst, {}) == {}



class TestApplyOverridesParamsDict:
    """The legacy idiom — ``self.params.get(name, default)``."""

    def test_lowercase_key_goes_to_params_dict(self):
        inst = _make_installed(_FakeParamsDictStrategy)
        applied = Backtester.apply_param_overrides(
            inst, {'roc_period': '10', 'roc_threshold': '5.0'}
        )
        assert inst.params['roc_period'] == 10
        assert inst.params['roc_threshold'] == 5.0
        # Loose-coercion result reflected in applied dict:
        assert applied == {'roc_period': 10, 'roc_threshold': 5.0}

    def test_lowercase_accepts_unknown_keys(self):
        """``self.params.get('key', default)`` is free-form — we can't
        know what keys are valid without running the strategy. Unlike
        upper-case class attrs, unknown lower-case keys just get stored."""
        inst = _make_installed(_FakeParamsDictStrategy)
        Backtester.apply_param_overrides(inst, {'totally_new_key': 42})
        assert inst.params['totally_new_key'] == 42

    def test_lowercase_numeric_strings_coerced(self):
        inst = _make_installed(_FakeParamsDictStrategy)
        Backtester.apply_param_overrides(inst, {'period': '14'})
        assert inst.params['period'] == 14
        assert isinstance(inst.params['period'], int)

    def test_lowercase_booleans_coerced(self):
        inst = _make_installed(_FakeParamsDictStrategy)
        Backtester.apply_param_overrides(inst, {'use_volume': 'true'})
        assert inst.params['use_volume'] is True


class TestVbtMacdBBFractionalBandWidth:
    """``VbtMacdBB.BB_ALPHA`` is a band width in standard deviations — a real
    number. It was declared ``2`` (an int), so once a fractional float for an
    int-typed tunable was refused instead of truncated (audit 2026-11-14,
    B4.6), the grid ``[1.5, 2.0, 2.5]`` could not be run for this class at
    all (B7). The default is declared ``2.0``."""

    @staticmethod
    def _strategy():
        from strategies.vbt_macd_bb import VbtMacdBB
        return _make_installed(VbtMacdBB)

    @staticmethod
    def _frame(n=200):
        import numpy as np
        import pandas as pd
        rng = np.random.default_rng(3)
        close = 100 + np.cumsum(rng.normal(0, 0.5, n))
        index = pd.date_range('2026-01-05 14:30', periods=n, freq='1min', tz='UTC', name='date')
        return pd.DataFrame({'open': close, 'high': close + 0.1, 'low': close - 0.1,
                             'close': close, 'volume': 1000.0}, index=index)

    def test_default_is_a_float(self):
        from strategies.vbt_macd_bb import VbtMacdBB
        assert type(VbtMacdBB.BB_ALPHA) is float and VbtMacdBB.BB_ALPHA == 2.0

    @pytest.mark.parametrize('raw, expected', [(1.5, 1.5), (2.0, 2.0), (2.5, 2.5), ('1.5', 1.5), (3, 3.0)])
    def test_fractional_override_is_accepted_and_used_as_given(self, raw, expected):
        inst = self._strategy()
        applied = Backtester.apply_param_overrides(inst, {'BB_ALPHA': raw})
        assert applied['BB_ALPHA'] == expected and type(applied['BB_ALPHA']) is float
        assert inst.BB_ALPHA == expected

    def test_the_audit_grid_runs_as_three_different_strategies(self):
        import numpy as np
        frame = self._frame()
        widths = []
        for alpha in (1.5, 2.0, 2.5):
            inst = self._strategy()
            Backtester.apply_param_overrides(inst, {'BB_ALPHA': alpha})
            state = inst.precompute(frame)
            widths.append(float(np.nanmean(state['upper'] - state['lower'])))
        assert widths[0] < widths[1] < widths[2]
        assert widths[0] / widths[1] == pytest.approx(0.75) and widths[2] / widths[1] == pytest.approx(1.25)

    def test_default_bands_are_the_two_sigma_bands(self):
        """Declaring 2.0 instead of 2 changes no number."""
        import numpy as np
        import vectorbt as vbt
        frame = self._frame()
        inst = self._strategy()
        state = inst.precompute(frame)
        bands = vbt.BBANDS.run(frame['close'], window=inst.BB_WINDOW, alpha=2)
        np.testing.assert_array_equal(state['upper'], np.asarray(bands.upper).ravel())
        np.testing.assert_array_equal(state['lower'], np.asarray(bands.lower).ravel())

    def test_other_int_tunables_of_the_class_still_refuse_a_fraction(self):
        with pytest.raises(ValueError, match='BB_WINDOW'):
            Backtester.apply_param_overrides(self._strategy(), {'BB_WINDOW': 20.5})
