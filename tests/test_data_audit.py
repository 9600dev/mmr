"""The audit's large-jump classification against `corporate_splits`.

`unexplained_jumps` (pure, in bar_quality) reports every large daily move and
declines to judge. `explained_by_split` (pure, spec'd in
tests/invariants/test_bar_quality.py) judges one move against one ratio. The
piece under test here is the WIRING between them —
`mmr_cli._classify_jumps_against_splits` — which owns the two decisions the
pure kernel does not: which splits are in the (previous bar, this bar] window,
and that bad reference rows are skipped rather than raised on (deal's
precondition on `explained_by_split` would raise, so the filter is
load-bearing, not defensive decoration).
"""
import datetime

import pytest

from trader.data.bar_quality import Bar, Finding, unexplained_jumps
from trader.mmr_cli import _classify_jumps_against_splits


def _series(closes, start=datetime.date(2024, 3, 4)):
    """Daily bars with the given closes on consecutive dates."""
    bars, dates = [], []
    d = start
    for c in closes:
        dates.append(d)
        ts = datetime.datetime.combine(d, datetime.time()).timestamp()
        bars.append(Bar(ts=ts, open=c, high=c, low=c, close=c, volume=1000.0))
        d += datetime.timedelta(days=1)
    return bars, dates


def _jumps(bars):
    found = unexplained_jumps(bars, threshold=0.30)
    assert found, 'test series must actually contain a large jump'
    return found


class TestSplitExplainsJump:
    def test_forward_split_on_jump_date_is_reclassified(self):
        # 4:1 split: split_to=4, split_from=1, expected move -75%
        bars, dates = _series([100.0, 25.0])
        out = _classify_jumps_against_splits(
            _jumps(bars), bars, dates, [(dates[1], 4.0, 1.0)])
        assert [f.rule for f in out] == ['split_explained_jump']
        assert '4:1 split' in out[0].detail
        assert str(dates[1]) in out[0].detail

    def test_reverse_split_is_reclassified(self):
        # 1:10 reverse split: expected move +900%
        bars, dates = _series([1.0, 10.0])
        out = _classify_jumps_against_splits(
            _jumps(bars), bars, dates, [(dates[1], 1.0, 10.0)])
        assert [f.rule for f in out] == ['split_explained_jump']

    def test_split_on_non_bar_day_between_bars_still_explains(self):
        # Friday bar, Monday bar, split executed Saturday: the move from
        # Friday's close to Monday's close carries it.
        friday = datetime.date(2024, 3, 1)
        monday = datetime.date(2024, 3, 4)
        bars, _ = _series([100.0, 25.0])
        out = _classify_jumps_against_splits(
            _jumps(bars), bars, [friday, monday],
            [(datetime.date(2024, 3, 2), 4.0, 1.0)])
        assert [f.rule for f in out] == ['split_explained_jump']

    def test_refresh_seam_days_before_execution_still_explains(self):
        # The CRWD shape: incremental refresh puts the discontinuity at the
        # last download BEFORE the split, so the jump bar precedes the
        # execution date. Observed live: seam 2026-06-28, execution 2026-07-02.
        bars, dates = _series([100.0, 25.0])
        exec_date = dates[1] + datetime.timedelta(days=4)
        out = _classify_jumps_against_splits(
            _jumps(bars), bars, dates, [(exec_date, 4.0, 1.0)])
        assert [f.rule for f in out] == ['split_explained_jump']


class TestJumpStaysUnexplained:
    def test_no_split_keeps_large_jump(self):
        bars, dates = _series([100.0, 25.0])
        out = _classify_jumps_against_splits(_jumps(bars), bars, dates, [])
        assert [f.rule for f in out] == ['large_jump']

    def test_wrong_direction_keeps_large_jump(self):
        # A 4:1 split predicts -75%; the price went UP 75%.
        bars, dates = _series([100.0, 175.0])
        out = _classify_jumps_against_splits(
            _jumps(bars), bars, dates, [(dates[1], 4.0, 1.0)])
        assert [f.rule for f in out] == ['large_jump']

    def test_split_outside_window_keeps_large_jump(self):
        bars, dates = _series([100.0, 100.0, 25.0, 25.0])
        splits = [(dates[1], 4.0, 1.0),                              # at the previous bar, not after it
                  (dates[2] + datetime.timedelta(days=8), 4.0, 1.0)]  # beyond the 7-day seam slack
        out = _classify_jumps_against_splits(
            _jumps(bars), bars, dates, splits)
        assert [f.rule for f in out] == ['large_jump']

    def test_wrong_magnitude_keeps_large_jump(self):
        # -90% is far outside a 2:1 split's -50% even with tolerance.
        bars, dates = _series([100.0, 10.0])
        out = _classify_jumps_against_splits(
            _jumps(bars), bars, dates, [(dates[1], 2.0, 1.0)])
        assert [f.rule for f in out] == ['large_jump']


class TestBadReferenceData:
    """A nonsense split row must be skipped, not raised on — deal's
    precondition on `explained_by_split` raises for non-positive ratios, so
    reaching it with one would take the whole audit down on one bad row."""

    @pytest.mark.parametrize('to,frm', [
        (0.0, 1.0), (4.0, 0.0), (-4.0, 1.0),
        (float('nan'), 1.0), (4.0, float('inf'))])
    def test_nonsense_ratio_is_skipped_silently(self, to, frm):
        bars, dates = _series([100.0, 25.0])
        out = _classify_jumps_against_splits(
            _jumps(bars), bars, dates, [(dates[1], to, frm)])
        assert [f.rule for f in out] == ['large_jump']

    def test_bad_row_does_not_mask_a_good_one(self):
        bars, dates = _series([100.0, 25.0])
        splits = [(dates[1], 0.0, 1.0), (dates[1], 4.0, 1.0)]
        out = _classify_jumps_against_splits(_jumps(bars), bars, dates, splits)
        assert [f.rule for f in out] == ['split_explained_jump']


class TestPassThrough:
    def test_other_findings_untouched(self):
        bars, dates = _series([100.0, 25.0])
        other = Finding('empty_bar', 'warn', 0, 'placeholder')
        out = _classify_jumps_against_splits(
            [other] + _jumps(bars), bars, dates, [(dates[1], 4.0, 1.0)])
        assert out[0] == other
        assert out[1].rule == 'split_explained_jump'

    def test_indexless_finding_untouched(self):
        f = Finding('large_jump', 'warn', None, 'no index')
        bars, dates = _series([100.0, 25.0])
        out = _classify_jumps_against_splits(
            [f], bars, dates, [(dates[1], 4.0, 1.0)])
        assert out == [f]
