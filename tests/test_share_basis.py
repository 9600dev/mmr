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


class TestPlausibleShareCounts:
    def test_ordinary_drift_is_kept(self):
        counts = [100e6, 101e6, 99e6, 98e6, 97e6]
        assert plausible_share_counts(counts) == counts

    def test_a_count_in_millions_among_counts_in_units_is_dropped(self):
        """MCD 2024-03-31: 725.9."""
        assert plausible_share_counts([730e6, 728e6, 725.9, 722e6]) == [730e6, 728e6, None, 722e6]

    def test_a_count_a_thousand_times_too_large_is_dropped(self):
        assert plausible_share_counts([420e6, 424e6, 423.64e9, 425e6]) == [420e6, 424e6, None, 425e6]

    def test_a_real_change_of_level_is_accepted_at_the_second_filing(self):
        """A merger: 100M becomes 400M and stays there. The first 400M could
        not be told from an error when it arrived, and stays rejected."""
        assert plausible_share_counts([100e6, 101e6, 400e6, 405e6, 410e6]) == [
            100e6, 101e6, None, 405e6, 410e6]

    def test_a_wrong_first_count_is_corrected_by_the_next_two(self):
        assert plausible_share_counts([725.9, 722e6, 721e6, 720e6]) == [725.9, None, 721e6, 720e6]

    def test_two_different_errors_in_a_row_do_not_confirm_each_other(self):
        assert plausible_share_counts([100e6, 100.0, 100e9, 101e6]) == [100e6, None, None, 101e6]

    def test_the_boundary_is_inclusive(self):
        assert plausible_share_counts([100.0, 300.0]) == [100.0, 300.0]
        assert plausible_share_counts([100.0, 300.01]) == [100.0, None]
        assert plausible_share_counts([300.0, 100.0]) == [300.0, 100.0]
        assert plausible_share_counts([300.0, 99.99]) == [300.0, None]

    def test_the_reference_is_the_median_of_the_last_four_kept(self):
        # kept 10, 10, 10, 10, then four 25s (within 3x of the median 10)...
        counts = [10.0, 10.0, 10.0, 10.0, 25.0, 25.0, 25.0, 25.0, 70.0]
        # ...so the reference is now 25 and 70 is within 3x of it.
        assert plausible_share_counts(counts)[-1] == 70.0
        # With only two 25s the median of (10, 10, 25, 25) is 17.5: 70 is not.
        assert plausible_share_counts([10.0, 10.0, 10.0, 10.0, 25.0, 25.0, 70.0])[-1] is None

    def test_an_even_window_uses_the_mean_of_the_middle_two(self):
        # median(20, 30) = 25: 75 is exactly 3x of it, 75.1 is beyond.
        assert plausible_share_counts([20.0, 30.0, 75.0]) == [20.0, 30.0, 75.0]
        assert plausible_share_counts([20.0, 30.0, 75.1]) == [20.0, 30.0, None]

    @pytest.mark.parametrize('bad', [None, float('nan'), float('inf'), 0.0, -47_029_000.0, 'n/a'])
    def test_what_is_not_a_positive_number_is_never_a_share_count(self, bad):
        assert plausible_share_counts([100e6, bad, 101e6]) == [100e6, None, 101e6]

    def test_a_numeric_string_is_a_number(self):
        assert plausible_share_counts(['100', '110']) == [100.0, 110.0]

    def test_an_unusable_value_does_not_reset_a_held_count(self):
        """400M is held; a blank follows; the next 405M still confirms it."""
        assert plausible_share_counts([100e6, 400e6, None, 405e6]) == [100e6, None, None, 405e6]

    def test_it_is_causal(self):
        """The verdict on a filing never changes when later filings arrive."""
        series = [100e6, 101e6, 725.9, 99e6, 400e6, 405e6, 410e6, 1e3, 415e6]
        full = plausible_share_counts(series)
        for n in range(1, len(series) + 1):
            assert plausible_share_counts(series[:n]) == full[:n]

    def test_empty_in_empty_out(self):
        assert plausible_share_counts([]) == []

    def test_the_factor_and_window_are_parameters(self):
        assert plausible_share_counts([100.0, 250.0], factor=2.0) == [100.0, None]
        assert plausible_share_counts([10.0, 10.0, 25.0, 25.0, 70.0], reference=2) == [10.0, 10.0, 25.0, 25.0, 70.0]


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
