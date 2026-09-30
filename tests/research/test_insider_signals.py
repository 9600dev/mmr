"""Pure builders of ``trader.research.lowfreq.insider_signals`` (C04; D14):
event timing (first session >= usable_from, never earlier), joint filings,
insider scope, mis-keyed prices, the routine classification (prior purchases
must already be USABLE at the classified purchase's close - the leak the
round-1 critic reproduced), distinct-buyer windows, and the memo's prefix
equality."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from trader.research.lowfreq import insider_signals as ins

_spec = importlib.util.spec_from_file_location('insider_fixtures', Path(__file__).with_name('insider_fixtures.py'))
assert _spec is not None and _spec.loader is not None
FIX = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(FIX)


@pytest.fixture(autouse=True)
def _events():
    ins.set_events_override(FIX.make_events())
    yield
    ins.set_events_override(None)


def _routine_by_year(prepared: pd.DataFrame, ticker: str) -> dict:
    r = prepared[prepared['ticker'] == ticker]
    return {int(y): bool(v) for y, v in zip(r['trans_date'].dt.year, r['routine'])}


def test_prepare_events_refuses_an_event_usable_on_or_before_its_filing_date():
    df = FIX.make_events()
    df.loc[0, 'usable_from'] = df.loc[0, 'filing_date']
    with pytest.raises(ValueError, match='D14'):
        ins.prepare_events(df)


def test_event_enters_on_the_first_session_at_or_after_usable_from_never_earlier():
    prepared = ins.prepared_events()
    idx = FIX.SESSIONS
    u = FIX.marker_usable_from()
    pos = idx.get_loc(u)
    frames = ins.daily_frames(prepared, idx, FIX.TICKERS)
    col = frames['p_value'][FIX.MARKER]
    assert col.iloc[pos] == pytest.approx(FIX.MARKER_VALUE)
    assert (col.iloc[:pos] == 0).all() and (col.iloc[pos + 1:] == 0).all()
    assert frames['p_filings'][FIX.MARKER].iloc[pos] == 1 and frames['p_filings'][FIX.MARKER].sum() == 1
    # usable_from falls on a day the panel has no session for: the NEXT session, not the previous
    idx2 = idx.delete(pos)
    col2 = ins.daily_frames(prepared, idx2, FIX.TICKERS)['p_value'][FIX.MARKER]
    assert col2.loc[idx2[pos]] == pytest.approx(FIX.MARKER_VALUE) and idx2[pos] > u
    assert (col2.iloc[:pos] == 0).all()


def test_joint_filing_splits_value_and_is_one_filing_and_one_buyer():
    prepared = ins.prepared_events()
    frames = ins.daily_frames(prepared, FIX.SESSIONS, [FIX.JOINT])
    assert frames['p_value'][FIX.JOINT].sum() == pytest.approx(200_000.0)      # 2 x 100k, 1/n_owners each
    assert frames['p_filings'][FIX.JOINT].sum() == 1
    assert ins.distinct_buyers(prepared, FIX.SESSIONS, [FIX.JOINT], 63)[FIX.JOINT].max() == 1


def test_ten_percent_only_buyers_are_out_and_miskeyed_prices_count_but_carry_no_value():
    frames = ins.daily_frames(ins.prepared_events(), FIX.SESSIONS, [FIX.TENPCT, FIX.MISKEYED])
    assert frames['p_value'][FIX.TENPCT].sum() == 0 and frames['p_filings'][FIX.TENPCT].sum() == 0
    assert frames['p_value'][FIX.MISKEYED].sum() == 0 and frames['p_filings'][FIX.MISKEYED].sum() == 1


def test_filter_frames_are_subsets_of_the_purchase_count_frame():
    frames = ins.daily_frames(ins.prepared_events(), FIX.SESSIONS, FIX.TICKERS)
    for suffix in ('_opp', '_officer', '_director', '_conv'):
        assert (frames['p_filings' + suffix] <= frames['p_filings']).all().all()
        assert (frames['p_value' + suffix] <= frames['p_value'] + 1e-9).all().all()
    assert set(frames) == set(ins.MEASURES)
    officer_plus_director = frames['p_filings_officer'] + frames['p_filings_director']
    assert (officer_plus_director == frames['p_filings']).all().all()   # every buyer is one or the other


def test_routine_flags_need_three_prior_same_month_years():
    by_year = _routine_by_year(ins.prepared_events(), FIX.ROUTINE)
    assert by_year == {2013: False, 2014: False, 2015: False, 2016: True, 2017: True, 2018: True, 2019: True}
    assert not ins.prepared_events().loc[lambda d: d['ticker'] == FIX.MARKER, 'routine'].any()


def test_a_late_filed_prior_purchase_cannot_make_an_earlier_purchase_routine():
    """Owner 901 bought T011 every March 2014-2018, but the March-2016 purchase
    was filed in May 2017. At the March-2017 purchase's close that prior was
    NOT yet known, so 2017 is not routine; by March 2018 it is known, so 2018
    is routine. Backdating (trans_date) must not leak through."""
    by_year = _routine_by_year(ins.prepared_events(), FIX.ROUTINE_LATE)
    assert by_year == {2014: False, 2015: False, 2016: False, 2017: False, 2018: True}
    late = ins.prepared_events()
    late = late[(late['ticker'] == FIX.ROUTINE_LATE) & (late['trans_date'].dt.year == 2016)]
    assert late['usable_from'].iloc[0] > pd.Timestamp('2017-03-13')            # the prior really is late
    # remove the late filing: nothing before its usable_from may change; 2018 loses its third prior
    without = ins.prepare_events(FIX.make_events(with_late_prior=False))
    assert _routine_by_year(without, FIX.ROUTINE_LATE) == {2014: False, 2015: False, 2017: False, 2018: False}


def test_distinct_buyers_counts_distinct_groups_inside_the_trailing_window():
    b = ins.distinct_buyers(ins.prepared_events(), FIX.SESSIONS, [FIX.CLUSTER], 63)[FIX.CLUSTER]
    assert b.iloc[100] == 0 and b.iloc[101] == 1 and b.iloc[105] == 2 and b.iloc[109] == 3
    assert b.iloc[163] == 3 and b.iloc[164] == 2 and b.iloc[171] == 1 and b.iloc[172] == 0


def test_memo_prefix_and_subset_equal_a_fresh_computation():
    idx, cols = FIX.SESSIONS, FIX.TICKERS
    ins.signal_frames(idx, cols, 63)                                  # fills the memo
    memo = ins.signal_frames(idx[:200], cols[:50], 63)                # prefix + subset: served from it
    ins.clear_caches()
    fresh = ins.signal_frames(idx[:200], cols[:50], 63)               # rebuilt from scratch
    assert set(memo) == set(fresh)
    for k in fresh:
        assert_frame_equal(memo[k], fresh[k], check_dtype=False)
