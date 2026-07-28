"""SPEC: what must be true of a stored OHLCV bar, and what must not be claimed.

Every gate in this system verifies CODE against a spec. Nothing verified the
DATA. The order path is mutation-tested, contract-checked and symbolically
verified, and it is fed by strategies fitted to bars nobody had audited. The
first run of these rules over the real store found 2,580 structurally
impossible 1-minute bars.

Two properties matter here and they pull against each other:

  * an IMPOSSIBLE bar must always be caught. A high below the close is not an
    unusual market, it is a row that never happened, and a strategy whose
    signal is the high of a range is reading fiction.
  * a LEGITIMATE oddity must never be called an error. Real series are full of
    weekend placeholders, holiday gaps and overnight jumps. A quality gate that
    fires on those is a gate someone switches off, and then the impossible bars
    stop being caught too.

The second is why severity is part of the spec rather than a presentation
detail.
"""

from __future__ import annotations

import math

from hypothesis import assume, given, settings, strategies as st

from trader.data.bar_quality import (
    Bar, check_bar, check_series, spacing_findings, unexplained_jumps)

_SETTINGS = settings(max_examples=400, deadline=None)

_PRICE = st.floats(min_value=0.01, max_value=1e6,
                   allow_nan=False, allow_infinity=False)
_VOL = st.floats(min_value=0.0, max_value=1e10,
                 allow_nan=False, allow_infinity=False)


def _rules(findings):
    return {f.rule for f in findings}


@st.composite
def coherent_bar(draw):
    """A bar that could have happened: high is the highest, low the lowest."""
    o = draw(_PRICE)
    c = draw(_PRICE)
    hi = draw(st.floats(min_value=max(o, c), max_value=max(o, c) * 1.5,
                        allow_nan=False, allow_infinity=False))
    lo = draw(st.floats(min_value=min(o, c) * 0.5, max_value=min(o, c),
                        allow_nan=False, allow_infinity=False))
    assume(lo > 0)
    return Bar(ts=draw(st.floats(min_value=0, max_value=4e9,
                                 allow_nan=False, allow_infinity=False)),
               open=o, high=hi, low=lo, close=c, volume=draw(_VOL))


class TestACoherentBarIsNeverFlagged:
    """The false-alarm half. A gate that fires on good data gets disabled, and
    then it is not protecting anything."""

    @_SETTINGS
    @given(bar=coherent_bar())
    def test_no_finding_on_a_bar_that_could_have_happened(self, bar):
        assert check_bar(bar, 0) == []

    @_SETTINGS
    @given(bar=coherent_bar())
    def test_a_flat_bar_is_fine(self, bar):
        """open == high == low == close is a real, common bar: an instrument
        that did not move. It must not be mistaken for a broken one."""
        flat = bar._replace(open=bar.close, high=bar.close, low=bar.close)
        assert check_bar(flat, 0) == []

    @_SETTINGS
    @given(bar=coherent_bar())
    def test_zero_volume_is_fine(self, bar):
        """23% of stored GOOGL minutes are zero-volume, and they are real:
        extended-hours minutes with no trades. The backtester sees them."""
        assert check_bar(bar._replace(volume=0.0), 0) == []


class TestAnImpossibleBarIsAlwaysCaught:
    """The other half. These are the rows that cannot describe any market."""

    @_SETTINGS
    @given(bar=coherent_bar(), gap=st.floats(min_value=1e-6, max_value=1e3,
                                             allow_nan=False, allow_infinity=False))
    def test_a_high_below_the_body_is_caught(self, bar, gap):
        broken = bar._replace(high=max(bar.open, bar.close) - gap)
        assume(broken.high > 0)
        assert 'high_not_highest' in _rules(check_bar(broken, 0))

    @_SETTINGS
    @given(bar=coherent_bar(), gap=st.floats(min_value=1e-6, max_value=1e3,
                                             allow_nan=False, allow_infinity=False))
    def test_a_low_above_the_body_is_caught(self, bar, gap):
        broken = bar._replace(low=min(bar.open, bar.close) + gap)
        assert 'low_not_lowest' in _rules(check_bar(broken, 0))

    def test_the_real_signature_found_in_the_store(self):
        """The exact shape of the 2,580 bad bars: a 'high' sitting BETWEEN open
        and close, low equal to open, and no trades aggregated. These were
        synthesised from bid/ask midpoints rather than trades, so the high is
        not a traded high at all."""
        bar = Bar(ts=0.0, open=76.81, high=76.825, low=76.80,
                  close=76.83, volume=5994.0)
        assert 'high_not_highest' in _rules(check_bar(bar, 0))

    @_SETTINGS
    @given(bar=coherent_bar())
    def test_a_negative_or_zero_price_is_caught(self, bar):
        assert 'non_positive_price' in _rules(check_bar(bar._replace(open=0.0), 0))
        assert 'non_positive_price' in _rules(check_bar(bar._replace(close=-1.0), 0))

    @_SETTINGS
    @given(bar=coherent_bar())
    def test_negative_volume_is_caught(self, bar):
        assert 'negative_volume' in _rules(check_bar(bar._replace(volume=-1.0), 0))


class TestMissingIsNotTheSameAsCorrupt:
    """A bar with NO prices is a placeholder for a date the source had nothing
    for. A bar with SOME prices and some holes is corrupt. Collapsing the two
    would either bury the corrupt ones or make the gate permanently red — on
    ASX daily series roughly a fifth of rows are weekend placeholders."""

    def test_an_all_missing_bar_is_a_warning_not_an_error(self):
        nan = float('nan')
        found = check_bar(Bar(ts=0.0, open=nan, high=nan, low=nan,
                              close=nan, volume=nan), 0)
        assert _rules(found) == {'empty_bar'}
        assert all(f.severity == 'warn' for f in found)

    def test_a_partially_missing_bar_is_an_error(self):
        found = check_bar(Bar(ts=0.0, open=10.0, high=float('nan'),
                              low=9.0, close=10.5, volume=1.0), 0)
        assert 'ohlc_partially_missing' in _rules(found)
        assert any(f.severity == 'error' for f in found)

    def test_an_infinite_price_is_missing_not_a_price(self):
        found = check_bar(Bar(ts=0.0, open=10.0, high=float('inf'),
                              low=9.0, close=10.5, volume=1.0), 0)
        assert 'ohlc_partially_missing' in _rules(found)


class TestSequenceRules:
    def test_duplicate_timestamps_are_caught(self):
        """49 of these are in the live store. A backtest walks the series in
        order, so a repeated timestamp replays a minute."""
        b = Bar(ts=60.0, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
        found = check_series([b, b])
        assert 'duplicate_timestamp' in _rules(found)

    def test_out_of_order_timestamps_are_caught(self):
        b = Bar(ts=60.0, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
        assert 'timestamps_out_of_order' in _rules(
            check_series([b, b._replace(ts=30.0)]))

    def test_an_ordered_unique_series_is_clean(self):
        b = Bar(ts=0.0, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
        series = [b._replace(ts=float(i * 60)) for i in range(50)]
        assert check_series(series) == []


class TestSpacingIsAWarningAndOnlyForRealGaps:
    """Legitimate gaps are everywhere: overnight, weekends, holidays, halts.
    Only a step that is not a whole number of intervals indicates the
    timestamps themselves are wrong."""

    def test_a_whole_multiple_gap_is_not_flagged(self):
        b = Bar(ts=0.0, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
        for minutes in (1, 2, 60, 1440):
            assert spacing_findings([b, b._replace(ts=60.0 * minutes)], 60.0) == []

    def test_a_fractional_step_is_flagged(self):
        b = Bar(ts=0.0, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
        found = spacing_findings([b, b._replace(ts=90.0)], 60.0)
        assert _rules(found) == {'non_multiple_gap'}
        assert all(f.severity == 'warn' for f in found)

    def test_a_degenerate_interval_reports_nothing(self):
        b = Bar(ts=0.0, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
        for step in (0.0, -60.0, float('nan')):
            assert spacing_findings([b, b._replace(ts=90.0)], step) == []


class TestJumpsAreReportedNeverJudged:
    """A 50% overnight move is the signature of an unadjusted split, and also
    of a crash. Telling them apart needs a corporate-actions feed this module
    does not take, so it reports and lets a human decide."""

    def test_a_split_sized_move_is_a_warning(self):
        b = Bar(ts=0.0, open=100.0, high=100.0, low=100.0, close=100.0, volume=1.0)
        found = unexplained_jumps([b, b._replace(ts=60.0, open=50.0, high=50.0,
                                                 low=50.0, close=50.0)])
        assert _rules(found) == {'large_jump'}
        assert all(f.severity == 'warn' for f in found)

    def test_an_ordinary_move_is_not_flagged(self):
        b = Bar(ts=0.0, open=100.0, high=100.0, low=100.0, close=100.0, volume=1.0)
        assert unexplained_jumps([b, b._replace(ts=60.0, close=103.0)]) == []


class TestTheFastPathAgreesWithTheSpec:
    """The write path cannot afford to build a NamedTuple per bar, so it uses a
    vectorised mask. Two implementations of one rule is precisely the shape
    that produced the flip-residual bug, where a gate and a splitter answered
    different questions about the same order.

    So the mask is not allowed its own opinion: it must select exactly the rows
    the spec calls errors. If they ever disagree, the spec wins and the mask is
    wrong.
    """

    @_SETTINGS
    @given(rows=st.lists(
        st.one_of(
            coherent_bar(),
            st.builds(Bar,
                      ts=st.floats(min_value=0, max_value=4e9,
                                   allow_nan=False, allow_infinity=False),
                      open=st.one_of(_PRICE, st.just(float('nan')), st.just(0.0)),
                      high=st.one_of(_PRICE, st.just(float('nan'))),
                      low=st.one_of(_PRICE, st.just(float('nan'))),
                      close=st.one_of(_PRICE, st.just(float('nan'))),
                      volume=st.one_of(_VOL, st.just(float('nan')),
                                       st.just(-1.0)))),
        min_size=1, max_size=40))
    def test_the_mask_selects_exactly_the_spec_errors(self, rows):
        import pandas as pd
        from trader.data.bar_quality import impossible_mask

        df = pd.DataFrame({
            'open': [b.open for b in rows], 'high': [b.high for b in rows],
            'low': [b.low for b in rows], 'close': [b.close for b in rows],
            'volume': [b.volume for b in rows],
        })
        by_spec = [any(f.severity == 'error' for f in check_bar(b, i))
                   for i, b in enumerate(rows)]
        by_mask = list(impossible_mask(df))
        assert by_mask == by_spec, (
            f'mask and spec disagree at '
            f'{[i for i, (a, b) in enumerate(zip(by_mask, by_spec)) if a != b]}')

    def test_an_empty_frame_is_handled(self):
        import pandas as pd
        from trader.data.bar_quality import impossible_mask
        assert len(impossible_mask(pd.DataFrame())) == 0

    def test_a_frame_without_ohlc_columns_rejects_nothing(self):
        """Refusing here would block legitimate non-OHLC writes rather than
        protect anything."""
        import pandas as pd
        from trader.data.bar_quality import impossible_mask
        df = pd.DataFrame({'something_else': [1, 2, 3]})
        assert not impossible_mask(df).any()

    def test_placeholder_rows_are_never_selected(self):
        """All-NaN rows record 'the source had nothing for this date'. They may
        be load-bearing for the incremental refresh, so dropping them could
        send it re-fetching the same empty range forever."""
        import numpy as np, pandas as pd
        from trader.data.bar_quality import impossible_mask
        df = pd.DataFrame({'open': [np.nan], 'high': [np.nan], 'low': [np.nan],
                           'close': [np.nan], 'volume': [np.nan]})
        assert not impossible_mask(df).any()
