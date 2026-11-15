"""Share counts a market cap can be computed from (trader/data/share_basis.py).

The research path multiplied split-ADJUSTED prices by AS-REPORTED share
counts, so every stock that later split had its market cap understated by the
split ratio - lookahead toward the names that went up - and the retired
vendor's fiscal-year-end "share counts" were not share counts at all
(docs/RESEARCH_LOG.md, 2026-11-15). These pin the pure rules that repair it.
"""

from __future__ import annotations

import datetime as dt
import math

import pytest

from trader.data.share_basis import (
    is_annual_report,
    is_late_filing_notice,
    plausible_share_counts,
    split_events,
    split_factor_after,
    split_reflected_in_prices,
)

D = dt.date


def _split(ticker, day, split_from, split_to):
    return {'ticker': ticker, 'execution_date': day, 'split_from': split_from, 'split_to': split_to}


NVDA = [_split('NVDA', '2021-07-20', 1, 4), _split('NVDA', '2024-06-10', 1, 10)]


class TestSplitEvents:
    def test_events_are_grouped_by_ticker_in_date_order(self):
        events = split_events([_split('NVDA', '2024-06-10', 1, 10), _split('AAPL', '2020-08-31', 1, 4),
                               _split('NVDA', '2021-07-20', 1, 4)])
        assert events == {'NVDA': [(D(2021, 7, 20), 4.0), (D(2024, 6, 10), 10.0)],
                          'AAPL': [(D(2020, 8, 31), 4.0)]}

    def test_a_reverse_split_is_a_ratio_below_one(self):
        assert split_events([_split('GE', '2021-08-02', 8, 1)]) == {'GE': [(D(2021, 8, 2), 0.125)]}

    def test_a_date_object_and_a_timestamp_string_are_read_alike(self):
        events = split_events([_split('A', D(2020, 1, 2), 1, 2), _split('B', '2020-01-02T00:00:00Z', 1, 2),
                               _split('C', dt.datetime(2020, 1, 2, 9, 30), 1, 2)])
        assert {t: e[0][0] for t, e in events.items()} == {t: D(2020, 1, 2) for t in 'ABC'}

    @pytest.mark.parametrize('row', [
        _split('', '2020-01-02', 1, 2),                  # no ticker
        _split(None, '2020-01-02', 1, 2),
        _split('X', None, 1, 2),                         # no date
        _split('X', 'soon', 1, 2),
        _split('X', '2020-01-02', 0, 2),                 # division by zero
        _split('X', '2020-01-02', 1, 0),                 # a ratio of zero
        _split('X', '2020-01-02', 1, -2),
        _split('X', '2020-01-02', 1, float('nan')),
        _split('X', '2020-01-02', 1, float('inf')),
        _split('X', '2020-01-02', 'one', 2),
        _split('X', '2020-01-02', None, 2),
        _split('X', '2020-01-02', 3, 3),                 # 1:1 changes nothing
    ])
    def test_a_row_that_is_not_a_split_is_dropped(self, row):
        assert split_events([row]) == {}

    def test_a_bad_row_does_not_cost_the_good_ones(self):
        events = split_events([_split('X', None, 1, 2), _split('X', '2020-01-02', 1, 2)])
        assert events == {'X': [(D(2020, 1, 2), 2.0)]}

    def test_whitespace_around_a_ticker_is_not_part_of_it(self):
        assert list(split_events([_split(' NVDA ', '2021-07-20', 1, 4)])) == ['NVDA']


class TestSplitFactorAfter:
    """K(f): what a share count filed on f must be multiplied by to sit beside
    prices adjusted for every later split."""

    EVENTS = split_events(NVDA)['NVDA']

    @pytest.mark.parametrize('filed, expected', [
        (D(2021, 5, 26), 40.0),          # before both
        (D(2021, 7, 19), 40.0),          # the day before the first
        (D(2021, 7, 20), 10.0),          # ON the execution day: already in the filing
        (D(2023, 11, 21), 10.0),
        (D(2024, 6, 9), 10.0),
        (D(2024, 6, 10), 1.0),
        (D(2025, 1, 1), 1.0),
    ])
    def test_only_splits_after_the_filing_count(self, filed, expected):
        assert split_factor_after(self.EVENTS, filed) == expected

    @pytest.mark.parametrize('through, expected', [
        (D(2021, 7, 19), 1.0),           # the prices know of neither
        (D(2021, 7, 20), 4.0),           # inclusive: adjusted to that day
        (D(2024, 6, 9), 4.0),
        (D(2024, 6, 10), 40.0),
        (None, 40.0),
    ])
    def test_a_split_the_prices_are_not_adjusted_to_is_left_out(self, through, expected):
        assert split_factor_after(self.EVENTS, D(2021, 1, 1), through) == expected

    def test_no_events_is_a_factor_of_one(self):
        assert split_factor_after([], D(2021, 1, 1)) == 1.0

    def test_a_reverse_split_shrinks_the_count(self):
        assert split_factor_after([(D(2021, 8, 2), 0.125)], D(2021, 1, 1)) == 0.125

    def test_the_market_cap_it_gives_is_the_true_one(self):
        """NVDA on 2021-07-01: traded at 808, 632M diluted shares as filed in
        May. The stored close is 808 / 40. Unrepaired: $12.8B. True: $511B."""
        adjusted_close = 808.0 / 40
        reported = 632e6
        factor = split_factor_after(self.EVENTS, D(2021, 5, 26))
        assert adjusted_close * reported == pytest.approx(12.77e9, rel=1e-3)
        assert adjusted_close * reported * factor == pytest.approx(808.0 * reported)


class TestSplitReflectedInPrices:
    def test_an_adjusted_series_moves_like_an_ordinary_day(self):
        assert split_reflected_in_prices(45.72, 45.53, 2.0) is True              # MNST 2026-08-11

    def test_an_unadjusted_series_shows_the_raw_jump(self):
        assert split_reflected_in_prices(24.5, 11.08, 2.2) is False              # CLBK 2026-07-21
        assert split_reflected_in_prices(138.49, 63.31, 2.0) is False            # CGNX 2017-12-04

    def test_an_unadjusted_reverse_split_jumps_up(self):
        assert split_reflected_in_prices(6.61, 27.9, 0.25) is False              # WLL 2017-11-09
        assert split_reflected_in_prices(6.61, 6.9, 0.25) is True

    def test_exactly_half_way_counts_as_adjusted(self):
        """The default is what an adjusted series is 98% of the time."""
        ratio = 4.0
        assert split_reflected_in_prices(100.0, 100.0 / math.sqrt(ratio), ratio) is True
        assert split_reflected_in_prices(100.0, 100.0 / math.sqrt(ratio) * 0.99, ratio) is False

    @pytest.mark.parametrize('before, on, ratio', [
        (None, 10.0, 2.0), (10.0, None, 2.0),
        (0.0, 10.0, 2.0), (10.0, 0.0, 2.0), (-1.0, 10.0, 2.0),
        (float('nan'), 10.0, 2.0), (10.0, float('inf'), 2.0),
        ('x', 10.0, 2.0),
        (10.0, 10.0, 0.0), (10.0, 10.0, float('nan')),
    ])
    def test_a_price_or_ratio_that_cannot_be_used_is_unknown(self, before, on, ratio):
        assert split_reflected_in_prices(before, on, ratio) is None

    @pytest.mark.parametrize('ratio', [1.012, 1.1, 1.2, 0.9])
    def test_a_ratio_an_ordinary_day_could_explain_is_unknown(self, ratio):
        assert split_reflected_in_prices(194.66, 194.48, ratio) is None

    def test_the_smallest_ratio_that_can_be_told(self):
        assert split_reflected_in_prices(100.0, 100.0, 1.25) is True
        assert split_reflected_in_prices(100.0, 80.0, 1.25) is False


M = 1e6


class TestShareCountReadings:
    """Filers print share counts in units, thousands or millions."""

    @pytest.mark.parametrize('printed, natural', [
        (725.9, 725.9e6),            # MCD 2024-03-31: millions
        (105_926.0, 105.926e6),      # OKE 2012-03-31: thousands
        (211_784_000.0, 211_784_000.0),
        (999.9, 999.9e6),
        (1_000.0, 1.0e6),            # the boundary: a thousand is in thousands
        (999_999.0, 999.999e6),
        (1_000_000.0, 1_000_000.0),
    ])
    def test_the_natural_reading_comes_first(self, printed, natural):
        from trader.data.share_basis import share_count_readings
        assert share_count_readings(printed)[0] == pytest.approx(natural)

    def test_the_other_scales_follow_when_they_are_possible_counts(self):
        from trader.data.share_basis import share_count_readings
        assert share_count_readings(1_194.0) == pytest.approx([1.194e6, 1.194e9])       # thousands, then millions
        assert share_count_readings(725.9) == pytest.approx([725.9e6])                  # x1000 is below a million
        assert share_count_readings(1_084_561.0) == pytest.approx([1_084_561.0, 1.084561e9])   # COP: units, or thousands
        assert share_count_readings(3.119977e14) == pytest.approx([3.119977e8])         # a million times too large
        assert share_count_readings(4.2364e11) == pytest.approx([4.2364e8])             # a thousand times too large
        assert share_count_readings(424e6) == pytest.approx([424e6])                    # x1000 is beyond any company

    @pytest.mark.parametrize('printed', [0.01, 0.15, 0.9])
    def test_a_number_no_scale_makes_sense_of_has_no_reading(self, printed):
        from trader.data.share_basis import share_count_readings
        assert share_count_readings(printed) == []

    def test_the_bounds_on_a_share_count(self):
        from trader.data import share_basis
        assert (share_basis.MIN_SHARES, share_basis.MAX_SHARES) == (1e6, 2e11)
        assert share_basis.share_count_readings(2e11) == [2e11]                          # the bound is inclusive
        assert share_basis.share_count_readings(2.0000001e11) == pytest.approx([2.0000001e8])
        assert share_basis.share_count_readings(1e6) == pytest.approx([1e6, 1e9])
        assert share_basis.share_count_readings(200e6) == pytest.approx([200e6, 2e11])   # ...at both ends
        assert share_basis.share_count_readings(999_999.0) == pytest.approx([999.999e6])  # read as thousands

    @pytest.mark.parametrize('printed', [1e6, 4_000_000.0, 1_495_700.0, 4.729e9, 2e11])
    def test_a_number_that_can_be_a_share_count_is_never_read_down(self, printed):
        """XOM's first stored count is a wrong 4,000,000. Allowing "divide by
        a thousand" as a reading made every correct 4.7 billion after it fit
        that reference as 4.7 million. A printed number is the count or
        SMALLER than it (thousands, millions), so the readings only go up."""
        from trader.data.share_basis import share_count_readings
        readings = share_count_readings(printed)
        assert readings[0] == printed
        assert all(reading >= printed for reading in readings)


class TestPlausibleShareCounts:
    def test_a_level_is_believed_when_two_filings_agree(self):
        """The first figure of a series has nothing to be compared with, and
        it is as likely to be wrong as any other."""
        assert plausible_share_counts([100 * M]) == [None]
        assert plausible_share_counts([100 * M, 101 * M]) == [None, 101 * M]

    def test_ordinary_drift_is_kept(self):
        counts = [100 * M, 101 * M, 99 * M, 98 * M, 97 * M]
        assert plausible_share_counts(counts) == [None] + counts[1:]

    def test_a_count_printed_in_millions_is_read_in_units(self):
        """MCD from 2024: 725.9, 722.0, 720.0 - every quarter, so "the next
        filing agrees" would have confirmed the wrong level."""
        assert plausible_share_counts([730 * M, 728 * M, 725.9, 722.0, 720.0])[1:] == pytest.approx(
            [728 * M, 725.9 * M, 722 * M, 720 * M])

    def test_a_count_printed_in_thousands_is_read_in_units(self):
        """OKE 2012: 105,926 then 211,784 after its 2:1."""
        assert plausible_share_counts([105_926.0, 105_970.0, 211_784.0, 208e6])[1:] == pytest.approx(
            [105.97 * M, 211.784 * M, 208 * M])

    def test_the_reading_that_fits_the_level_wins_over_the_natural_one(self):
        """TXN 2011: 1,194 among counts of 1.2 billion. Naturally thousands
        (1.2M shares); it is millions."""
        assert plausible_share_counts([1_210 * M, 1_200 * M, 1_194.0, 1_185 * M])[1:] == pytest.approx(
            [1_200 * M, 1_194 * M, 1_185 * M])

    def test_thousands_that_could_be_a_count_as_they_stand(self):
        """COP 2018-2020: 1,084,561 among counts of 1.1 billion. It can be a
        share count as printed; the level says it is thousands."""
        assert plausible_share_counts([1.1e9, 1.09e9, 1_084_561.0, 1.08e9])[1:] == pytest.approx(
            [1.09e9, 1.084561e9, 1.08e9])

    def test_a_count_a_thousand_times_too_large_is_read_down(self):
        assert plausible_share_counts([420 * M, 424 * M, 423.64e9, 425 * M])[1:] == pytest.approx(
            [424 * M, 423.64 * M, 425 * M])
        assert plausible_share_counts([190 * M, 3.119977e14])[1] == pytest.approx(311.9977 * M)

    def test_a_filer_who_always_prints_thousands_is_read_in_units_throughout(self):
        assert plausible_share_counts([28_900.0, 29_100.0, 29_400.0])[1:] == pytest.approx([29.1 * M, 29.4 * M])

    def test_a_real_change_of_level_is_accepted_at_the_second_filing(self):
        """A merger: 100M becomes 400M and stays there. The first 400M could
        not be told from an error when it arrived, and stays rejected."""
        assert plausible_share_counts([100 * M, 101 * M, 400 * M, 405 * M, 410 * M]) == [
            None, 101 * M, None, 405 * M, 410 * M]

    def test_a_wrong_first_count_is_never_used(self):
        assert plausible_share_counts([5 * M, 722 * M, 721 * M, 720 * M]) == [None, None, 721 * M, 720 * M]

    def test_a_first_count_printed_in_another_scale_confirms_the_second(self):
        """XOM as stored: 4,000,000 (four billion, in thousands), then
        4.729e9, 5.089e9. TEVA: 2,000,000, then 1,018,000,000. COP:
        1,495,700, then 1,503,565,000. The first figure is never used, and it
        does not delay the second: read in thousands, it agrees."""
        assert plausible_share_counts([4_000_000.0, 4.729e9, 5.089e9, 5.031e9]) == [
            None, 4.729e9, 5.089e9, 5.031e9]
        assert plausible_share_counts([2_000_000.0, 1.018e9, 1.018e9]) == [None, 1.018e9, 1.018e9]
        assert plausible_share_counts([1_495_700.0, 1.503565e9, 1.501257e9]) == [None, 1.503565e9, 1.501257e9]

    def test_what_it_cannot_see(self):
        """A series printed in thousands from its first row, every number
        large enough to be a count as it stands, is believed as printed - a
        thousand times too small - until a figure in units arrives and the
        one after confirms it. ``unbelievable_cap`` is the check for this."""
        assert plausible_share_counts([1_084_561.0, 1_090_000.0, 1_095_000.0, 1.1e9, 1.101e9]) == [
            None, 1_090_000.0, 1_095_000.0, None, 1.101e9]

    def test_two_different_errors_in_a_row_do_not_confirm_each_other(self):
        assert plausible_share_counts([100 * M, 100 * M, 7 * M, 2_000 * M, 101 * M]) == [
            None, 100 * M, None, None, 101 * M]

    def test_the_boundary_is_inclusive(self):
        assert plausible_share_counts([100 * M, 100 * M, 300 * M]) == [None, 100 * M, 300 * M]
        assert plausible_share_counts([100 * M, 100 * M, 300.01 * M]) == [None, 100 * M, None]
        assert plausible_share_counts([300 * M, 300 * M, 100 * M]) == [None, 300 * M, 100 * M]
        assert plausible_share_counts([300 * M, 300 * M, 99.99 * M]) == [None, 300 * M, None]
        # ...and for the two filings that establish a level
        assert plausible_share_counts([100 * M, 300 * M]) == [None, 300 * M]
        assert plausible_share_counts([100 * M, 300.01 * M]) == [None, None]
        assert plausible_share_counts([300 * M, 100 * M]) == [None, 100 * M]
        assert plausible_share_counts([300 * M, 99.99 * M]) == [None, None]

    def test_the_reference_is_the_median_of_the_last_four_kept(self):
        # kept 10, 10, 10 (millions), then four 25s (within 3x of the median 10)...
        counts = [10 * M, 10 * M, 10 * M, 10 * M, 25 * M, 25 * M, 25 * M, 25 * M, 70 * M]
        # ...so the reference is now 25 and 70 is within 3x of it.
        assert plausible_share_counts(counts)[-1] == 70 * M
        # With only two 25s the median of (10, 10, 25, 25) is 17.5: 70 is not.
        assert plausible_share_counts([10 * M, 10 * M, 10 * M, 10 * M, 25 * M, 25 * M, 70 * M])[-1] is None

    def test_an_even_window_uses_the_mean_of_the_middle_two(self):
        # kept 20, 30: median 25. 75 is exactly 3x of it, 75.1 is beyond.
        assert plausible_share_counts([20 * M, 20 * M, 30 * M, 75 * M]) == [None, 20 * M, 30 * M, 75 * M]
        assert plausible_share_counts([20 * M, 20 * M, 30 * M, 75.1 * M]) == [None, 20 * M, 30 * M, None]

    @pytest.mark.parametrize('bad', [None, float('nan'), float('inf'), 0.0, -47_029_000.0, 'n/a', 0.01])
    def test_what_cannot_be_a_share_count_never_is(self, bad):
        assert plausible_share_counts([100 * M, 100 * M, bad, 101 * M]) == [None, 100 * M, None, 101 * M]

    @pytest.mark.parametrize('bad', [None, float('nan'), 0.0, 'n/a', 0.01])
    def test_what_cannot_be_a_share_count_neither_confirms_nor_contradicts(self, bad):
        """A 10-K between two 10-Qs carries no count: the second 10-Q still
        confirms the first."""
        assert plausible_share_counts([100 * M, bad, 101 * M]) == [None, None, 101 * M]

    def test_a_numeric_string_is_a_number(self):
        assert plausible_share_counts(['100000000', '110000000']) == [None, 110 * M]

    def test_an_unusable_value_does_not_reset_a_held_count(self):
        """400M is held; a blank follows; the next 405M still confirms it."""
        assert plausible_share_counts([100 * M, 100 * M, 400 * M, None, 405 * M]) == [
            None, 100 * M, None, None, 405 * M]

    def test_a_held_count_is_confirmed_by_the_next_ones_fitting_reading(self):
        """The new level arrives printed in units, then in millions."""
        out = plausible_share_counts([100 * M, 100 * M, 400 * M, 405.0])
        assert out[:3] == [None, 100 * M, None] and out[3] == pytest.approx(405 * M)

    def test_a_count_that_returns_to_the_old_level_drops_the_held_one(self):
        """100M, an odd 400M, 101M (the 400M was the error), then 405M: that
        is a first 405M, not a confirmation of the 400M two filings back."""
        assert plausible_share_counts([100 * M, 100 * M, 400 * M, 101 * M, 405 * M, 99 * M]) == [
            None, 100 * M, None, 101 * M, None, 99 * M]

    def test_it_is_causal(self):
        """The verdict on a filing never changes when later filings arrive."""
        series = [100 * M, 101 * M, 725.9, 99 * M, 400 * M, 405 * M, 410 * M, 1_194.0, 415 * M, 0.01, 3e14,
                  1_084_561.0, 4_000_000.0, 4.1e9]
        full = plausible_share_counts(series)
        for n in range(1, len(series) + 1):
            assert plausible_share_counts(series[:n]) == full[:n]

    def test_empty_in_empty_out(self):
        assert plausible_share_counts([]) == []

    def test_a_split_is_one_level_on_the_common_basis(self):
        """NVDA: 632M, then 2,532M after its 4:1. Without the factors that is a
        change of level (the first is held back); with them it is one level."""
        printed = [630 * M, 632 * M, 2_532 * M, 2_540 * M]
        assert plausible_share_counts(printed) == [None, 632 * M, None, 2_540 * M]
        assert plausible_share_counts(printed, basis=[40, 40, 10, 10])[1:] == pytest.approx(
            [25_280 * M, 25_320 * M, 25_400 * M])

    def test_the_scale_is_read_from_the_number_as_printed(self):
        """725.9 (millions) with a factor of 2 is 1,451.8M shares. Multiplied
        first, 1,451.8 would be read as thousands."""
        assert plausible_share_counts([725.9, 725.9], basis=[2.0, 2.0])[1] == pytest.approx(1_451.8 * M)
        assert plausible_share_counts([730 * M, 725.9, 722.0], basis=[2.0, 2.0, 1.0])[1:] == pytest.approx(
            [1_451.8 * M, 722 * M])

    @pytest.mark.parametrize('multiplier', [0.0, -1.0, float('nan'), float('inf')])
    def test_a_factor_that_is_not_a_positive_number_gives_no_count(self, multiplier):
        assert plausible_share_counts([100 * M, 100 * M, 101 * M], basis=[1.0, 1.0, multiplier]) == [
            None, 100 * M, None]

    def test_the_factor_and_window_are_parameters(self):
        assert plausible_share_counts([100 * M, 100 * M, 250 * M], factor=2.0) == [None, 100 * M, None]
        assert plausible_share_counts([100 * M, 250 * M], factor=2.0) == [None, None]
        assert plausible_share_counts([10 * M, 10 * M, 10 * M, 25 * M, 25 * M, 70 * M], reference=2) == [
            None, 10 * M, 10 * M, 25 * M, 25 * M, 70 * M]


class TestUnbelievableCap:
    """A market cap smaller than two typical days of the name's own trading."""

    def test_the_threshold(self):
        from trader.data.share_basis import MAX_DAILY_TURNOVER, unbelievable_cap
        assert MAX_DAILY_TURNOVER == 0.5
        assert unbelievable_cap(1e9, 0.5e9) is False            # exactly half: not beyond it
        assert unbelievable_cap(1e9, 0.5e9 + 1) is True
        assert unbelievable_cap(1e9, 0.03e9) is False           # a busy liquid name

    def test_the_cases_it_exists_for(self):
        from trader.data.share_basis import unbelievable_cap
        # TEVA 2018: a "count" of 2,000,000 at $24 is a $48M company trading $246M a day.
        assert unbelievable_cap(2_000_000 * 24.02, 246e6) is True
        # ...and the real one: 1,018M shares, the same trading.
        assert unbelievable_cap(1.018e9 * 24.02, 246e6) is False

    @pytest.mark.parametrize('cap, volume', [
        (None, 1e6), (1e9, None), (float('nan'), 1e6), (1e9, float('nan')), (float('inf'), 1e6),
        (0.0, 1e6), (-1e9, 1e6), (1e9, 0.0), (1e9, -1e6), ('n/a', 1e6),
    ])
    def test_an_unknown_number_contradicts_nothing(self, cap, volume):
        from trader.data.share_basis import unbelievable_cap
        assert unbelievable_cap(cap, volume) is False

    def test_the_threshold_is_a_parameter(self):
        from trader.data.share_basis import unbelievable_cap
        assert unbelievable_cap(1e9, 0.3e9, max_turnover=0.25) is True
        assert unbelievable_cap(1e9, 0.2e9, max_turnover=0.25) is False


class TestFormTypes:
    @pytest.mark.parametrize('form', ['NT 10-K', 'NT 10-Q', 'NT 10-Q/A', 'nt 10-k', ' NT 10-K '])
    def test_a_late_filing_notice(self, form):
        assert is_late_filing_notice(form) is True
        assert is_annual_report(form) is False

    @pytest.mark.parametrize('form', ['10-K', '10-K/A', '10-k', ' 10-K ', '10-KT'])
    def test_an_annual_report(self, form):
        assert is_annual_report(form) is True
        assert is_late_filing_notice(form) is False

    @pytest.mark.parametrize('form', ['10-Q', '10-Q/A', '', None, '8-K'])
    def test_neither(self, form):
        assert is_annual_report(form) is False
        assert is_late_filing_notice(form) is False
